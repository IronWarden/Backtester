#!/usr/bin/env python3
"""Work out WHY each company delisted, from SEC EDGAR. Free, no key.

Alpha Vantage's free delisting feed (add_delistings.py) gives the date a symbol
stopped trading and nothing about the cause. That single missing field is what
has blocked applying a delisting return: an acquisition at a premium is a
positive outcome for the holder and a Chapter 11 is roughly -100%, and the
literature's blanket haircut (Shumway & Warther 1999: -55% Nasdaq; Shumway 1997:
-30% NYSE/AMEX) overcorrects every acquisition when applied blind.

EDGAR answers it, per company, for free, in two stages:

  1. WHO delisted, and when. The quarterly filing indexes
     (https://www.sec.gov/Archives/edgar/full-index/YYYY/QTRn/form.idx) list
     every filing by form type, with the filer's name and CIK. The delisting
     forms are:
        25-NSE   filed BY THE EXCHANGE -- an involuntary delisting
        25       filed by the company -- usually a merger or a voluntary move
        15-12B / 15-12G / 15F-12B / 15F-12G   deregistration, which normally
                 follows an acquisition or a going-private transaction
     This stage is also how a DEAD company's CIK is discovered at all:
     company_tickers.json lists only current tickers, which is the gap
     add_company_info.py documents. Use --emit-cik-list to feed it.

  2. WHY. Each filer's submissions document
     (https://data.sec.gov/submissions/CIK##########.json) carries every filing
     with its 8-K item numbers, and two items settle it:
        item 1.03  Bankruptcy or Receivership
        item 2.01  Completion of Acquisition or Disposition of Assets
     Verified on real cases 2026-08-17: SUNEDISON, INC. (CIK 945436) filed an
     8-K with item 1.03 on 2016-04-27 and the NYSE filed 25-NSE on 2016-05-06
     -> bankruptcy. CoreSite Realty (1490892) and Activision Blizzard (718877)
     each filed an 8-K with item 2.01 on the same day as their 25-NSE
     -> acquired.

Usage:
    python3 add_delisting_reasons.py --dry-run --from-year 2016 --limit-ciks 25
    python3 add_delisting_reasons.py --dry-run          # full harvest, no write
    python3 add_delisting_reasons.py                    # replace the table
    python3 add_delisting_reasons.py --emit-cik-list ciks.txt --no-classify
Requires the UI app closed for a real write (DuckDB is single-writer).

COSTS AND LIMITS, because both are real:

  * Stage 1 downloads ~31 MB per quarter. 2010 to now is ~66 quarters, so ~2 GB
    of transfer for a full harvest -- fast (a couple of seconds each) but not
    free of bandwidth. Quarters are cached in the cache file, so a re-run costs
    nothing. --from-year is there to keep it smaller.
  * Stage 2 is one request per company at ~8/s, inside SEC's fair-use limit.
  * A submissions document holds the most recent 1,000 filings inline and older
    ones in separate files, which this does NOT fetch. For a company that
    stopped filing when it died, its last filings are the relevant ones and they
    are inline. A company that filed heavily for decades could in principle push
    its delisting forms out of the inline window; the `evidence` column says
    which signals were found, so such a row reads as thin rather than as clean.
  * The 8-K items are the filer's own characterisation. A company acquired out of
    bankruptcy can plausibly file both 1.03 and 2.01; bankruptcy wins, because
    the shareholder outcome is the one that matters for a backtest.
  * Nothing here maps CIKs back to TICKERS reliably. EDGAR's submissions
    document lists `tickers` only for companies with a current listing, which by
    construction these do not have. The symbol join belongs with the delisting
    registry (name matching against `delistings.name`) and is deliberately not
    guessed at here.

The classification mirrors src/data/delisting_reasons.go, which is the authority
-- the Go side recomputes the reason from the stored evidence flags.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

import loader_provenance

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = os.environ.get("BACKTESTER_DB", str(ROOT / "stock_data.db"))
TABLE = "delisting_reasons"
CACHE = ROOT / "delisting_reasons_cache.json"

INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/form.idx"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"

CONTACT = os.environ.get("SEC_CONTACT", "gadhiarishi@gmail.com")
USER_AGENT = f"Backtester research {CONTACT}"
REQUEST_PAUSE = 0.12  # ~8 req/s, inside SEC's 10/s fair-use limit

# Forms that mark the end of a listing. Split by who files them, because that
# distinction is most of the signal: an exchange filing 25-NSE is throwing a
# company off, a company filing 25 is usually leaving on its own terms.
FORM_EXCHANGE_DELIST = {"25-NSE"}
FORM_COMPANY_DELIST = {"25"}
FORM_DEREGISTER = {"15-12B", "15-12G", "15F-12B", "15F-12G"}
DELISTING_FORMS = FORM_EXCHANGE_DELIST | FORM_COMPANY_DELIST | FORM_DEREGISTER

ITEM_BANKRUPTCY = "1.03"   # Bankruptcy or Receivership
ITEM_ACQUISITION = "2.01"  # Completion of Acquisition or Disposition of Assets

# How far from the delisting an 8-K may sit and still be about it. CoreSite's was
# the same day; SunEdison's was nine days before. A quarter each way is generous
# enough for a slow deregistration without reaching back to an unrelated deal.
EVENT_WINDOW_DAYS = 120


def get(url: str, attempts: int = 4) -> bytes:
    """One polite GET, retrying on throttling and transient failures."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
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


def load_cache() -> dict[str, Any]:
    if not CACHE.exists():
        return {"quarters": {}, "companies": {}}
    try:
        c = json.loads(CACHE.read_text())
        c.setdefault("quarters", {})
        c.setdefault("companies", {})
        return c
    except (json.JSONDecodeError, OSError):
        print(f"!! {CACHE.name} unreadable; starting over", file=sys.stderr)
        return {"quarters": {}, "companies": {}}


def save_cache(cache: dict[str, Any]) -> None:
    CACHE.write_text(json.dumps(cache, sort_keys=True))


# form.idx *looks* fixed-width and is not: its header columns do not line up
# with its data rows (a 25-NSE row puts the company name at offset 17 where the
# header says 12). Parsing by offset silently produced truncated dates -- which
# read as "no 8-K near the delisting" and misclassified SunEdison's bankruptcy
# as a compliance delisting. Hence a pattern: two or more spaces separate the
# fields, and a company name may contain single spaces.
INDEX_ROW = re.compile(
    r"^(?P<form>\S+)\s{2,}(?P<name>.+?)\s{2,}(?P<cik>\d+)\s+"
    r"(?P<filed>\d{4}-\d{2}-\d{2})\s+(?P<path>\S+)\s*$")


def harvest_quarter(year: int, qtr: int) -> list[dict[str, str]]:
    """The delisting-form rows of one quarterly index."""
    raw = get(INDEX_URL.format(year=year, qtr=qtr)).decode("latin-1")
    out = []
    for line in raw.splitlines():
        if not line[:12].strip() in DELISTING_FORMS:
            continue
        m = INDEX_ROW.match(line)
        if not m:
            continue
        out.append({"cik": str(int(m.group("cik"))), "name": m.group("name"),
                    "form": m.group("form"), "filed": m.group("filed")})
    return out


def harvest(from_year: int, to_year: int, cache: dict[str, Any],
            limit_quarters: int | None) -> list[dict[str, str]]:
    """Stage 1: every delisting-form filing in the year range."""
    # Quarters that have not happened yet exist only for the current year; a
    # past to_year has all four, which an unconditional check would truncate.
    this_year = date.today().year
    this_quarter = (date.today().month - 1) // 3 + 1
    quarters = [(y, q) for y in range(from_year, to_year + 1)
                for q in (1, 2, 3, 4)
                if not (y == this_year and q > this_quarter)]
    if limit_quarters:
        quarters = quarters[:limit_quarters]

    events: list[dict[str, str]] = []
    fetched = 0
    for year, qtr in quarters:
        key = f"{year}Q{qtr}"
        if key in cache["quarters"]:
            events.extend(cache["quarters"][key])
            continue
        try:
            rows = harvest_quarter(year, qtr)
        except Exception as e:
            print(f"!! {key}: {e}", file=sys.stderr)
            continue
        cache["quarters"][key] = rows
        events.extend(rows)
        fetched += 1
        print(f"  {key}: {len(rows)} delisting filings")
        if fetched % 8 == 0:
            save_cache(cache)
    save_cache(cache)
    print(f"stage 1: {len(events)} filings across {len(quarters)} quarters "
          f"({fetched} newly downloaded)")
    return events


def evidence_for(cik: int, events: list[dict[str, str]]) -> dict[str, Any]:
    """Stage 2: the flags that decide one company's reason.

    Reads the filer's submissions document and looks for the 8-K items that
    explain the delisting, within EVENT_WINDOW_DAYS of it.
    """
    doc = json.loads(get(SUBMISSIONS_URL.format(cik=cik)))
    recent = doc.get("filings", {}).get("recent", {})
    forms = recent.get("form", []) or []
    dates = recent.get("filingDate", []) or []
    items = recent.get("items", []) or [""] * len(forms)

    # The delisting date is the earliest exchange/company Form 25 if there is
    # one, else the earliest deregistration.
    def earliest(kinds: set[str]) -> str:
        got = sorted(e["filed"] for e in events if e["form"] in kinds)
        return got[0] if got else ""

    delist_25 = earliest(FORM_EXCHANGE_DELIST | FORM_COMPANY_DELIST)
    delist_15 = earliest(FORM_DEREGISTER)
    event_date = delist_25 or delist_15

    def near(day: str) -> bool:
        if not event_date or not day:
            return False
        try:
            a = datetime.strptime(event_date, "%Y-%m-%d").date()
            b = datetime.strptime(day, "%Y-%m-%d").date()
        except ValueError:
            return False
        return abs((b - a).days) <= EVENT_WINDOW_DAYS

    bankruptcy = acquisition = ""
    for i, form in enumerate(forms):
        if form != "8-K" or i >= len(dates):
            continue
        item = (items[i] if i < len(items) else "") or ""
        day = dates[i]
        if ITEM_BANKRUPTCY in item and near(day) and not bankruptcy:
            bankruptcy = day
        if ITEM_ACQUISITION in item and near(day) and not acquisition:
            acquisition = day

    return {
        "cik": f"{cik:010d}",
        "company_name": doc.get("name", "") or "",
        "sic": str(doc.get("sic") or ""),
        "tickers": json.dumps(doc.get("tickers") or []),
        "delisting_date": event_date,
        "exchange_filed_25": any(e["form"] in FORM_EXCHANGE_DELIST for e in events),
        "company_filed_25": any(e["form"] in FORM_COMPANY_DELIST for e in events),
        "deregistered": bool(delist_15),
        "bankruptcy_8k": bankruptcy,
        "acquisition_8k": acquisition,
    }


def classify(ev: dict[str, Any]) -> tuple[str, str, str]:
    """(reason, confidence, evidence) — mirror of ClassifyReason in Go.

    Bankruptcy outranks acquisition: a company acquired out of Chapter 11 can
    file both, and the shareholder outcome — wiped out — is the one a backtest
    needs.
    """
    notes = []
    if ev["bankruptcy_8k"]:
        notes.append(f"8-K item {ITEM_BANKRUPTCY} on {ev['bankruptcy_8k']}")
    if ev["acquisition_8k"]:
        notes.append(f"8-K item {ITEM_ACQUISITION} on {ev['acquisition_8k']}")
    if ev["exchange_filed_25"]:
        notes.append("25-NSE (exchange filed)")
    if ev["company_filed_25"]:
        notes.append("25 (company filed)")
    if ev["deregistered"]:
        notes.append("Form 15 deregistration")
    evidence = "; ".join(notes) if notes else "no delisting filings found"

    if ev["bankruptcy_8k"]:
        return "bankruptcy", "high", evidence
    if ev["acquisition_8k"]:
        return "acquired", "high", evidence
    if ev["company_filed_25"] or ev["deregistered"]:
        # Left of its own accord, with no 8-K saying why. Usually a merger or a
        # going-private deal, but the filings do not say so.
        return "voluntary", "low", evidence
    if ev["exchange_filed_25"]:
        # Thrown off by the exchange with no explanatory 8-K nearby: a listing
        # standards failure (price, float, filing delinquency).
        return "compliance", "medium", evidence
    return "unknown", "none", evidence


def report(df: pd.DataFrame) -> None:
    print(f"\n{len(df)} companies classified\n")
    print("by reason:")
    for reason, n in df["reason"].value_counts().items():
        print(f"  {reason:12} {n:6}  ({n / len(df) * 100:5.1f}%)")
    print("\nby confidence:")
    for conf, n in df["confidence"].value_counts().items():
        print(f"  {conf:8} {n:6}")

    if "delisting_date" in df:
        years = pd.to_datetime(df["delisting_date"], errors="coerce").dt.year
        known = years.dropna()
        if not known.empty:
            print(f"\ndelisting dates: {int(known.min())} .. {int(known.max())}")

    print("\nspot checks (public knowledge, so these are falsifiable):")
    for needle, expect in (("SUNEDISON", "bankruptcy"),
                           ("CoreSite", "acquired"),
                           ("Activision", "acquired"),
                           ("SVB FINANCIAL", "bankruptcy")):
        hit = df[df["company_name"].str.contains(needle, case=False, na=False)]
        if hit.empty:
            print(f"  {needle:16} not in this harvest window")
            continue
        for _, r in hit.iterrows():
            mark = "ok " if r["reason"] == expect else "!! "
            print(f"  {mark}{r['company_name'][:34]:34} {r['reason']:11} "
                  f"expected {expect:11} [{r['evidence'][:60]}]")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--dry-run", action="store_true",
                    help="harvest, classify and report, write nothing")
    ap.add_argument("--from-year", type=int, default=2010)
    ap.add_argument("--to-year", type=int, default=date.today().year)
    ap.add_argument("--limit-quarters", type=int, default=None,
                    help="stop after N quarterly indexes (for probing)")
    ap.add_argument("--limit-ciks", type=int, default=None,
                    help="classify at most N new companies (for probing)")
    ap.add_argument("--cik", action="append", type=int, default=None,
                    help="classify only these CIKs (repeatable); for spot "
                         "checks against cases whose answer is public")
    ap.add_argument("--emit-cik-list", default=None,
                    help="write every delisted company's CIK here, one per "
                         "line, for add_company_info.py --cik-list")
    ap.add_argument("--no-classify", action="store_true",
                    help="stage 1 only; useful with --emit-cik-list")
    args = ap.parse_args()

    cache = load_cache()
    events = harvest(args.from_year, args.to_year, cache, args.limit_quarters)
    if not events:
        print("no delisting filings found", file=sys.stderr)
        return 1

    by_cik: dict[int, list[dict[str, str]]] = {}
    for e in events:
        by_cik.setdefault(int(e["cik"]), []).append(e)
    print(f"{len(by_cik)} distinct companies")

    if args.emit_cik_list:
        Path(args.emit_cik_list).write_text(
            "\n".join(str(c) for c in sorted(by_cik)) + "\n")
        print(f"CIK list written to {args.emit_cik_list}")

    if args.no_classify:
        return 0

    todo = [c for c in sorted(by_cik) if str(c) not in cache["companies"]]
    if args.cik:
        missing = [c for c in args.cik if c not in by_cik]
        if missing:
            print(f"not in this harvest window: {missing}", file=sys.stderr)
        todo = [c for c in args.cik if c in by_cik]
    if args.limit_ciks:
        todo = todo[:args.limit_ciks]
    print(f"stage 2: {len(cache['companies'])} cached, {len(todo)} to fetch "
          f"(~{len(todo) * REQUEST_PAUSE / 60:.0f} min)")

    failures = 0
    for i, cik in enumerate(todo, start=1):
        try:
            cache["companies"][str(cik)] = evidence_for(cik, by_cik[cik])
        except Exception as e:
            failures += 1
            print(f"!! CIK {cik}: {e}", file=sys.stderr)
        if i % 250 == 0:
            save_cache(cache)
            print(f"  {i}/{len(todo)}")
        time.sleep(REQUEST_PAUSE)
    save_cache(cache)
    if failures:
        print(f"{failures} companies could not be fetched; re-run to retry")

    rows = []
    for cik, ev in sorted(cache["companies"].items(), key=lambda kv: int(kv[0])):
        reason, confidence, evidence = classify(ev)
        rows.append({**ev, "reason": reason, "confidence": confidence,
                     "evidence": evidence})
    df = pd.DataFrame(rows)
    if df.empty:
        print("nothing classified", file=sys.stderr)
        return 1
    df["fetched_at"] = pd.Timestamp.now("UTC").tz_localize(None)
    report(df)

    if args.dry_run:
        print("\n(dry run -- nothing written)")
        return 0

    try:
        con = duckdb.connect(args.db)
    except duckdb.IOException as e:
        print(f"Could not open {args.db}: {e}\n"
              "If a lock is held, close the UI app (DuckDB is single-writer).",
              file=sys.stderr)
        return 1
    try:
        con.register("incoming", df)
        con.execute("BEGIN")
        con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
        con.execute(f"""
            CREATE TABLE "{TABLE}" (
                cik               VARCHAR NOT NULL,
                company_name      VARCHAR,
                sic               VARCHAR,
                tickers           VARCHAR,
                delisting_date    DATE,
                reason            VARCHAR,
                confidence        VARCHAR,
                evidence          VARCHAR,
                exchange_filed_25 BOOLEAN,
                company_filed_25  BOOLEAN,
                deregistered      BOOLEAN,
                bankruptcy_8k     DATE,
                acquisition_8k    DATE,
                fetched_at        TIMESTAMP_NS
            )
        """)
        con.execute(f"""
            INSERT INTO "{TABLE}"
            SELECT cik, company_name, sic, tickers,
                   TRY_CAST(delisting_date AS DATE), reason, confidence, evidence,
                   exchange_filed_25, company_filed_25, deregistered,
                   TRY_CAST(bankruptcy_8k AS DATE), TRY_CAST(acquisition_8k AS DATE),
                   CAST(fetched_at AS TIMESTAMP_NS)
            FROM incoming
        """)
        con.execute("COMMIT")
        con.unregister("incoming")
        written = con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()
        loader_provenance.record(
            con, table=TABLE,
            source="SEC EDGAR full-index (Forms 25/25-NSE/15) + submissions "
                   "8-K items",
            endpoint=INDEX_URL.format(year="YYYY", qtr="n"),
            licence="US government work, public domain; SEC fair-use headers",
            rows=written[0] if written else 0,
            note="keyed by CIK, not ticker; bankruptcy outranks acquisition")
        print(f"\n{TABLE} written: {written[0] if written else 0} rows")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
