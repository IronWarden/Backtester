#!/usr/bin/env python3
"""Load company identity and industry from SEC EDGAR into `company_profile`.

The docs said for years that this database has no sector data. It does not have
to be that way and it costs nothing: SEC EDGAR publishes the industry
classification of every company that files with it, free, with no key and no
signup. Two endpoints:

    https://www.sec.gov/files/company_tickers.json       ticker -> CIK
    https://data.sec.gov/submissions/CIK##########.json   sic, sicDescription,
                                                          formerNames, exchange

One request per company, so a full run is ~7,000 requests. SEC's fair-use policy
asks for a real User-Agent with a contact address and no more than 10 requests a
second; this script sends both and paces itself, and it checkpoints so an
interrupted run resumes instead of starting the 7,000 over.

Usage:
    python3 add_company_info.py --dry-run --limit 20   # probe, writes nothing
    python3 add_company_info.py --dry-run              # full fetch, no write
    python3 add_company_info.py                        # fetch and replace table
    python3 add_company_info.py --no-fetch             # write from the cache only
Requires the UI app closed for a real write (DuckDB is single-writer).

WHAT THIS COVERS, WHICH IS NOT EVERYTHING:

  * `company_tickers.json` lists companies with a CURRENT ticker. A company that
    delisted years ago is still in EDGAR — its submissions history does not go
    away — but it has no row in that file, so it cannot be found by symbol.
    Pass --cik-list FILE (one CIK per line) to fetch those directly once you
    have their CIKs from somewhere else. Filling that list from the delisting
    registry is a separate job; see T42 in TASKS.md.
  * SIC is the SEC's own classification: coarser and older than GICS, and its
    divisions are broad. 3571 "Electronic Computers" puts Apple in
    Manufacturing. That is correct SIC and will still surprise someone.
  * A foreign issuer trading here on an ADR may file little or nothing, in
    which case EDGAR has no industry for it.

The sector column mirrors `SectorForSIC` in src/data/company_profile.go. The two
tables of division boundaries must agree; there is a test on the Go side and the
boundaries are written out below so a reader can compare them by eye.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import duckdb
import pandas as pd

import loader_provenance

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = os.environ.get("BACKTESTER_DB", str(ROOT / "stock_data.db"))
TABLE = "company_profile"
CACHE = ROOT / "company_info_cache.json"

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"

# SEC fair-use: identify yourself, and stay under 10 requests/second.
CONTACT = os.environ.get("SEC_CONTACT", "gadhiarishi@gmail.com")
USER_AGENT = f"Backtester research {CONTACT}"
REQUEST_PAUSE = 0.12  # ~8 req/s, comfortably inside the limit

# SIC division boundaries, inclusive, from the SEC's published structure. The
# gaps between them (1800-1999, 6800-6999, 9000-9099, 9730+) are real gaps in
# the scheme, not omissions here. Mirror of sicDivisions in
# src/data/company_profile.go -- keep the two in step.
SIC_DIVISIONS = [
    (100, 999, "Agriculture, Forestry & Fishing"),
    (1000, 1499, "Mining"),
    (1500, 1799, "Construction"),
    (2000, 3999, "Manufacturing"),
    (4000, 4999, "Transportation & Public Utilities"),
    (5000, 5199, "Wholesale Trade"),
    (5200, 5999, "Retail Trade"),
    (6000, 6799, "Finance, Insurance & Real Estate"),
    (7000, 8999, "Services"),
    (9100, 9729, "Public Administration"),
]
UNCLASSIFIED = "Unclassified"


def sector_for_sic(sic: str) -> str:
    """The SIC division a code falls in, or UNCLASSIFIED."""
    try:
        code = int(str(sic).strip())
    except (TypeError, ValueError):
        return UNCLASSIFIED
    if code <= 0:
        return UNCLASSIFIED
    for low, high, sector in SIC_DIVISIONS:
        if low <= code <= high:
            return sector
    return UNCLASSIFIED


def get(url: str, attempts: int = 4) -> bytes:
    """One polite GET, retrying on the throttling and transient codes."""
    # No Accept-Encoding: urllib does not decompress, and SEC honours the
    # header, so asking for gzip means decoding a gzip stream as UTF-8.
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    })
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            # 403 is what SEC returns for a missing/blocked User-Agent, and
            # retrying it will not help; 429/5xx are worth backing off for.
            if e.code in (429, 500, 502, 503, 504) and attempt < attempts - 1:
                time.sleep(2 ** attempt)
                continue
            raise
        except (urllib.error.URLError, TimeoutError):
            if attempt < attempts - 1:
                time.sleep(2 ** attempt)
                continue
            raise
    raise RuntimeError(f"gave up on {url}")


def ticker_map() -> dict[str, int]:
    """ticker -> CIK for every company with a current ticker."""
    raw = json.loads(get(TICKERS_URL))
    out: dict[str, int] = {}
    # The file is {"0": {"cik_str": 320193, "ticker": "AAPL", "title": ...}, ...}
    for row in raw.values():
        ticker = str(row.get("ticker", "")).strip().upper()
        if ticker:
            out[ticker] = int(row["cik_str"])
    return out


def profile_from_submissions(ticker: str, cik: int, doc: dict) -> dict:
    """One `company_profile` row from a submissions document."""
    sic = str(doc.get("sic") or "").strip()
    former = [f.get("name", "") for f in (doc.get("formerNames") or [])
              if f.get("name")]
    # EDGAR repeats the exchange once per listed class of stock, so a company
    # with three share classes reports "Nasdaq, Nasdaq, Nasdaq".
    seen: dict[str, None] = {}
    for e in doc.get("exchanges") or []:
        if e:
            seen[str(e)] = None
    exchanges = list(seen)
    return {
        "ticker": ticker,
        "cik": f"{cik:010d}",
        "name": (doc.get("name") or "").strip(),
        "sic": sic,
        "sic_description": (doc.get("sicDescription") or "").strip(),
        "sector": sector_for_sic(sic),
        "exchange": ", ".join(str(e) for e in exchanges),
        "former_names": json.dumps(former),
        "state_of_incorporation": (doc.get("stateOfIncorporation") or "").strip(),
    }


def load_cache() -> dict[str, dict]:
    if not CACHE.exists():
        return {}
    try:
        return json.loads(CACHE.read_text())
    except (json.JSONDecodeError, OSError):
        print(f"!! {CACHE.name} is unreadable; starting over", file=sys.stderr)
        return {}


def save_cache(rows: dict[str, dict]) -> None:
    CACHE.write_text(json.dumps(rows, indent=0, sort_keys=True))


def wanted_tickers(con: duckdb.DuckDBPyConnection, every: bool) -> set[str]:
    """The tickers worth fetching: those with price data, unless --all."""
    if every:
        return set()
    rows = con.execute(
        'SELECT DISTINCT "Ticker" FROM stock_data_optimized').fetchall()
    return {str(r[0]).strip().upper() for r in rows}


def fetch_profiles(
    targets: list[tuple[str, int]], cache: dict[str, dict], limit: int | None,
) -> dict[str, dict]:
    """Fetch each target's submissions document, resuming from the cache."""
    todo = [(t, c) for t, c in targets if t not in cache]
    if limit:
        todo = todo[:limit]
    print(f"{len(cache)} cached, {len(todo)} to fetch "
          f"(~{len(todo) * REQUEST_PAUSE / 60:.0f} min at {1/REQUEST_PAUSE:.0f}/s)")

    failures = 0
    for i, (ticker, cik) in enumerate(todo, start=1):
        try:
            doc = json.loads(get(SUBMISSIONS_URL.format(cik=cik)))
            cache[ticker] = profile_from_submissions(ticker, cik, doc)
        except Exception as e:  # one bad filer must not end a 7,000-row run
            failures += 1
            print(f"!! {ticker} (CIK {cik}): {e}", file=sys.stderr)
        if i % 250 == 0:
            save_cache(cache)
            print(f"  {i}/{len(todo)} fetched")
        time.sleep(REQUEST_PAUSE)

    save_cache(cache)
    if failures:
        print(f"{failures} companies could not be fetched; "
              f"re-run to retry only those")
    return cache


def report(df: pd.DataFrame, con: duckdb.DuckDBPyConnection) -> None:
    """Print what was gathered and how much of the price table it covers."""
    print(f"\n{len(df)} company profiles")
    if df.empty:
        return

    print("\nby sector:")
    for sector, n in df["sector"].value_counts().items():
        print(f"  {sector:36} {n:6}")

    unclassified = int((df["sector"] == UNCLASSIFIED).sum())
    print(f"\nunclassified: {unclassified} "
          f"({unclassified / len(df) * 100:.1f}%) -- no SIC code, or a code "
          "in one of the scheme's gaps")

    renamed = int((df["former_names"] != "[]").sum())
    print(f"renamed at least once: {renamed} "
          "-- part of the evidence for recycled tickers")

    con.register("prof", df[["ticker"]])
    covered = con.execute("""
        SELECT COUNT(DISTINCT s."Ticker")
        FROM stock_data_optimized s JOIN prof p ON p.ticker = s."Ticker"
    """).fetchone()
    total = con.execute(
        'SELECT COUNT(DISTINCT "Ticker") FROM stock_data_optimized').fetchone()
    con.unregister("prof")
    if covered and total and total[0]:
        print(f"\nprice-table coverage: {covered[0]} of {total[0]} tickers "
              f"({covered[0] / total[0] * 100:.1f}%)")
        print("  the shortfall is mostly $-prefixed benchmark series (not "
              "companies), ADRs that file little, and delisted symbols that "
              "no longer appear in company_tickers.json")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--dry-run", action="store_true",
                    help="fetch and report, write nothing")
    ap.add_argument("--limit", type=int, default=None,
                    help="fetch at most N new companies (for probing)")
    ap.add_argument("--all", action="store_true",
                    help="fetch every SEC filer, not only tickers with prices")
    ap.add_argument("--no-fetch", action="store_true",
                    help="use the cache as-is; make no network requests")
    ap.add_argument("--cik-list", default=None,
                    help="extra CIKs to fetch, one per line (for delisted "
                         "companies, which have no current ticker)")
    args = ap.parse_args()

    try:
        con = duckdb.connect(args.db, read_only=args.dry_run)
    except duckdb.IOException as e:
        print(f"Could not open {args.db}: {e}\n"
              "If a lock is held, close the UI app (DuckDB is single-writer).",
              file=sys.stderr)
        return 1

    try:
        cache = load_cache()

        if not args.no_fetch:
            try:
                mapping = ticker_map()
            except Exception as e:
                print(f"Could not fetch {TICKERS_URL}: {e}", file=sys.stderr)
                return 1
            print(f"{len(mapping)} tickers in company_tickers.json")

            keep = wanted_tickers(con, args.all)
            targets = sorted(
                (t, c) for t, c in mapping.items() if not keep or t in keep)
            print(f"{len(targets)} of them have price data here"
                  if keep else f"{len(targets)} targets (--all)")

            if args.cik_list:
                extra = [int(line) for line in
                         Path(args.cik_list).read_text().split()
                         if line.strip().isdigit()]
                # Keyed by CIK, since these have no ticker to key on.
                targets += [(f"CIK{c:010d}", c) for c in extra]
                print(f"plus {len(extra)} CIKs from {args.cik_list}")

            cache = fetch_profiles(targets, cache, args.limit)

        if not cache:
            print("nothing to write: the cache is empty and no fetch was made",
                  file=sys.stderr)
            return 1

        df = pd.DataFrame(sorted(cache.values(), key=lambda r: r["ticker"]))
        df["fetched_at"] = pd.Timestamp.now("UTC").tz_localize(None)
        report(df, con)

        if args.dry_run:
            print("\n(dry run -- nothing written)")
            return 0

        con.register("incoming", df)
        con.execute("BEGIN")
        # Replaced wholesale rather than merged: a company's SIC code, name and
        # exchange all change over time, and the fetch is a full snapshot.
        con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
        con.execute(f"""
            CREATE TABLE "{TABLE}" (
                ticker                 VARCHAR NOT NULL,
                cik                    VARCHAR,
                name                   VARCHAR,
                sic                    VARCHAR,
                sic_description        VARCHAR,
                sector                 VARCHAR,
                exchange               VARCHAR,
                former_names           VARCHAR,
                state_of_incorporation VARCHAR,
                fetched_at             TIMESTAMP_NS
            )
        """)
        con.execute(f"""
            INSERT INTO "{TABLE}"
            SELECT ticker, cik, name, sic, sic_description, sector, exchange,
                   former_names, state_of_incorporation,
                   CAST(fetched_at AS TIMESTAMP_NS)
            FROM incoming
        """)
        con.execute("COMMIT")
        con.unregister("incoming")

        written = con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()
        loader_provenance.record(
            con, table=TABLE, source="SEC EDGAR submissions + company_tickers",
            endpoint=SUBMISSIONS_URL.format(cik=0),
            licence="US government work, public domain; SEC fair-use headers",
            rows=written[0] if written else 0,
            note="SIC divisions, not GICS; only companies with a current "
                 "ticker unless --cik-list was used")
        print(f"\n{TABLE} written: {written[0] if written else 0} rows")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
