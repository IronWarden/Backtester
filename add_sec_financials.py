#!/usr/bin/env python3
"""Load SEC bulk XBRL fundamentals into `sec_financials`. Free, no key.

The existing `financials` table covers 2020-07 onward and only tickers that
still exist, so any factor study is confined to five and a half years of
survivors -- one macro regime, chosen by who lived. SEC's Financial Statement
Data Sets fix both halves:

  * 2009Q1 to the present, quarterly ZIPs of parsed XBRL from every filer. The
    early quarters are nearly empty (the XBRL mandate phased in by filer size),
    and SEC publishes each quarter a few months in arrears -- 2026Q1 was the
    newest available on 2026-08-17, and a missing quarter is reported and
    skipped rather than fatal.
  * Companies that later DIED are in it, because SEC keeps what was filed.
  * Every row carries `filed` -- the actual publication date. That is the field
    `PointInTimeFundamentals` currently has to approximate as "period end + 90
    days" whenever `earnings_calendar` has no entry (see T29), and approximating
    it is the difference between a point-in-time factor backtest and a
    look-ahead-biased one.

Written to its own table rather than into `financials`, deliberately: the
existing table's numbers cannot then move, and the overlapping window (2020-07+)
becomes a free correctness check on both sources.

Usage:
    python3 add_sec_financials.py --dry-run --limit-quarters 1   # probe
    python3 add_sec_financials.py --from-year 2020               # smaller
    python3 add_sec_financials.py                                # 2009 -> now
Requires the UI app closed for a real write (DuckDB is single-writer).

WHAT IT COSTS, measured 2026-08-17: each quarterly ZIP is ~124 MB and takes ~7 s
to fetch and ~8 s to parse, and it yields ~102,000 rows for the tags below. A
full 2009->2026 run is therefore ~69 quarters, ~7 GB of transfer, ~20 minutes,
and ~7 million rows. Quarters are processed and inserted one at a time and the
ZIP is discarded, so the disk cost is the table, not the archive. --from-year
bounds it.

LIMITS worth knowing before trusting a row:

  * Only 10-K and 10-Q filings are read. Amendments (10-K/A) are skipped, so a
    restatement filed as an amendment is not captured.
  * Rows with a `segments` or `coreg` value are skipped: those are per-segment or
    per-subsidiary breakdowns, and summing them double-counts the parent.
  * `qtrs` says what a duration covers -- 0 is an instant (balance sheet), 1 a
    single quarter, 4 a full year. Rows with 2 or 3 are year-to-date partials and
    are skipped rather than mislabelled.
  * The same (company, metric, period) legitimately appears more than once with
    different `filed` dates: an original figure and a later restatement. Both are
    kept, because which one was knowable on a given date is the entire point.
  * `ticker` is filled from SEC's company_tickers.json where the CIK has a
    CURRENT listing, and left NULL otherwise -- which is exactly the delisted
    companies. Joining those to price history needs the name match filed as T51.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = os.environ.get("BACKTESTER_DB", str(ROOT / "stock_data.db"))
TABLE = "sec_financials"

ZIP_URL = ("https://www.sec.gov/files/dera/data/financial-statement-data-sets/"
           "{year}q{qtr}.zip")
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
CONTACT = os.environ.get("SEC_CONTACT", "gadhiarishi@gmail.com")
USER_AGENT = f"Backtester research {CONTACT}"

# The data sets begin in 2009Q1, but the XBRL mandate phased in by filer size,
# so the first quarters are nearly empty: 2009Q1 is a 13 KB archive and 2009Q2 is
# 145 KB, against ~124 MB for a modern quarter. They are included for
# completeness and cost nothing.
FIRST_YEAR, FIRST_QUARTER = 2009, 1

# XBRL tag -> the metric vocabulary `financials` already uses, so the two tables
# can be compared over their overlap without a translation layer. Several tags
# map to one metric because the standard tag for revenue changed with ASC 606;
# whichever a filer used lands under the same name.
TAG_TO_METRIC = {
    "Revenues": "Total Revenue",
    "RevenueFromContractWithCustomerExcludingAssessedTax": "Total Revenue",
    "RevenueFromContractWithCustomerIncludingAssessedTax": "Total Revenue",
    "SalesRevenueNet": "Total Revenue",
    "NetIncomeLoss": "Net Income",
    "OperatingIncomeLoss": "Operating Income",
    "StockholdersEquity": "Stockholders Equity",
    "Assets": "Total Assets",
    "Liabilities": "Total Liabilities Net Minority Interest",
    "CashAndCashEquivalentsAtCarryingValue": "Cash And Cash Equivalents",
    "CommonStockSharesOutstanding": "Ordinary Shares Number",
    "WeightedAverageNumberOfDilutedSharesOutstanding": "Diluted Average Shares",
}

# qtrs -> frequency. 2 and 3 are year-to-date partials and are dropped.
QTRS_TO_FREQUENCY = {"0": "instant", "1": "quarterly", "4": "annual"}

FORMS = {"10-K", "10-Q"}


def get(url: str, attempts: int = 4) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=600) as resp:
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


def ticker_map() -> dict[str, str]:
    """CIK -> ticker, for the companies that still have a listing."""
    raw = json.loads(get(TICKERS_URL))
    out: dict[str, str] = {}
    for row in raw.values():
        ticker = str(row.get("ticker", "")).strip().upper()
        cik = str(int(row["cik_str"]))
        # A CIK with several share classes appears more than once; the first
        # (alphabetically lowest, as the file is ordered by size) is the primary
        # listing and good enough for a join hint.
        if ticker and cik not in out:
            out[cik] = ticker
    return out


def as_date(yyyymmdd: str) -> date | None:
    s = (yyyymmdd or "").strip()
    if len(s) != 8 or not s.isdigit():
        return None
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:]))
    except ValueError:
        return None


def quarter_rows(year: int, qtr: int, tickers: dict[str, str]) -> pd.DataFrame:
    """One quarter's rows, joined from sub.txt and num.txt."""
    blob = get(ZIP_URL.format(year=year, qtr=qtr))
    z = zipfile.ZipFile(io.BytesIO(blob))

    subs: dict[str, dict[str, Any]] = {}
    with z.open("sub.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="latin-1"),
                                  delimiter="\t"):
            if row["form"] not in FORMS:
                continue
            subs[row["adsh"]] = {
                "cik": str(int(row["cik"])),
                "company_name": row["name"].strip(),
                "sic": (row["sic"] or "").strip(),
                "form": row["form"],
                "fy": (row["fy"] or "").strip(),
                "fp": (row["fp"] or "").strip(),
                "filed": as_date(row["filed"]),
                # The filing's OWN period end. A 10-K also restates two prior
                # years, so most rows in a filing are comparatives whose
                # period_end is years before `filed`; comparing the two is only
                # a publication lag when they refer to the same period.
                "filing_period": as_date(row["period"]),
            }

    out: list[dict[str, Any]] = []
    with z.open("num.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="latin-1"),
                                  delimiter="\t"):
            metric = TAG_TO_METRIC.get(row["tag"])
            if metric is None:
                continue
            sub = subs.get(row["adsh"])
            if sub is None:
                continue
            # Segment and subsidiary breakdowns would double-count the parent.
            if row.get("segments") or row.get("coreg"):
                continue
            if row["uom"] not in ("USD", "shares"):
                continue
            frequency = QTRS_TO_FREQUENCY.get(row["qtrs"])
            if frequency is None:
                continue
            period_end = as_date(row["ddate"])
            if period_end is None or sub["filed"] is None:
                continue
            try:
                value = float(row["value"])
            except (TypeError, ValueError):
                continue

            out.append({
                "cik": sub["cik"],
                "ticker": tickers.get(sub["cik"]),
                "company_name": sub["company_name"],
                "sic": sub["sic"],
                "metric": metric,
                "tag": row["tag"],
                "period_end": period_end,
                "filed": sub["filed"],
                "filing_period": sub["filing_period"],
                "value": value,
                "form": sub["form"],
                "fy": sub["fy"],
                "fp": sub["fp"],
                "frequency": frequency,
                "accession": row["adsh"],
            })
    return pd.DataFrame(out)


def quarters(from_year: int, to_year: int) -> list[tuple[int, int]]:
    today = date.today()
    out = []
    for year in range(max(from_year, FIRST_YEAR), to_year + 1):
        for qtr in (1, 2, 3, 4):
            if year == FIRST_YEAR and qtr < FIRST_QUARTER:
                continue
            # Only COMPLETED quarters: SEC publishes a data set weeks after
            # the quarter ends, so the quarter in progress has no ZIP and the
            # one just finished may not either (a 404 is tolerated below).
            if year == today.year and qtr >= (today.month - 1) // 3 + 1:
                continue
            out.append((year, qtr))
    return out


def create_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
    con.execute(f"""
        CREATE TABLE "{TABLE}" (
            cik          VARCHAR NOT NULL,
            ticker       VARCHAR,
            company_name VARCHAR,
            sic          VARCHAR,
            metric       VARCHAR NOT NULL,
            tag          VARCHAR,
            period_end   DATE NOT NULL,
            filed        DATE NOT NULL,
            -- The filing's own period end. period_end == filing_period marks the
            -- figure the filing is actually reporting; the rest are the
            -- comparatives every 10-K carries for prior years.
            filing_period DATE,
            value        DOUBLE,
            form         VARCHAR,
            fy           VARCHAR,
            fp           VARCHAR,
            frequency    VARCHAR,
            accession    VARCHAR
        )
    """)


def report(seen: dict[str, Any]) -> None:
    print(f"\n{seen['rows']:,} rows from {seen['quarters']} quarters")
    print(f"{len(seen['ciks']):,} distinct companies, "
          f"{len(seen['tickers']):,} of them with a current ticker "
          f"({len(seen['ciks']) - len(seen['tickers']):,} have none — those are "
          "the delisted ones, which is the point)")
    print("\nrows per metric:")
    for metric, n in sorted(seen["metrics"].items(), key=lambda kv: -kv[1]):
        print(f"  {metric:44} {n:9,}")
    print(f"\n{seen.get('comparatives', 0):,} of those rows are comparatives "
          "(a prior period restated inside a later filing, so period_end != "
          "filing_period). They are kept: their `filed` date is honest, and the "
          "earliest `filed` for a period is when that figure first became "
          "public.")
    print("\npublication lag for the figures each filing is actually reporting "
          "(filed minus period end, period_end == filing_period), which is what "
          "this table exists to supply:")
    lags = seen["lags"]
    if lags:
        s = pd.Series(lags)
        print(f"  median {s.median():.0f} days, mean {s.mean():.0f}, "
              f"p10 {s.quantile(0.1):.0f}, p90 {s.quantile(0.9):.0f}, "
              f"max {s.max():.0f}")
        print("  compare with the 90-day fallback PointInTimeFundamentals uses "
              "when earnings_calendar has no entry")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--dry-run", action="store_true",
                    help="fetch and report, write nothing")
    ap.add_argument("--from-year", type=int, default=FIRST_YEAR)
    ap.add_argument("--to-year", type=int, default=date.today().year)
    ap.add_argument("--limit-quarters", type=int, default=None,
                    help="stop after N quarters (for probing)")
    args = ap.parse_args()

    todo = quarters(args.from_year, args.to_year)
    if args.limit_quarters:
        todo = todo[-args.limit_quarters:]
    if not todo:
        print("no quarters in range", file=sys.stderr)
        return 1
    print(f"{len(todo)} quarters: {todo[0][0]}Q{todo[0][1]} .. "
          f"{todo[-1][0]}Q{todo[-1][1]}")

    try:
        tickers = ticker_map()
    except Exception as e:
        print(f"Could not fetch {TICKERS_URL}: {e}", file=sys.stderr)
        return 1
    print(f"{len(tickers)} CIKs have a current ticker")

    con = None
    if not args.dry_run:
        try:
            con = duckdb.connect(args.db)
        except duckdb.IOException as e:
            print(f"Could not open {args.db}: {e}\n"
                  "If a lock is held, close the UI app (DuckDB is single-writer).",
                  file=sys.stderr)
            return 1
        create_table(con)

    seen: dict[str, Any] = {"rows": 0, "quarters": 0, "ciks": set(),
                            "tickers": set(), "metrics": {}, "lags": []}
    try:
        for year, qtr in todo:
            started = time.time()
            try:
                df = quarter_rows(year, qtr, tickers)
            except Exception as e:
                print(f"!! {year}Q{qtr}: {e}", file=sys.stderr)
                continue
            if df.empty:
                print(f"  {year}Q{qtr}: no rows")
                continue

            seen["rows"] += len(df)
            seen["quarters"] += 1
            seen["ciks"].update(df["cik"])
            seen["tickers"].update(t for t in df["ticker"] if t)
            for metric, n in df["metric"].value_counts().items():
                seen["metrics"][metric] = seen["metrics"].get(metric, 0) + int(n)
            # Sampled rather than exhaustive: the lag distribution is a summary
            # statistic for the report, and keeping every one of ~7M would cost
            # more memory than the table itself.
            # Only the figures the filing is actually reporting: a comparative
            # from three years back would report a three-year "lag".
            own = df[df["period_end"] == df["filing_period"]]
            if not own.empty:
                lag = (pd.to_datetime(own["filed"])
                       - pd.to_datetime(own["period_end"]))
                seen["lags"].extend(lag.dt.days.head(2000).tolist())
            seen["comparatives"] = seen.get("comparatives", 0) + len(df) - len(own)

            if con is not None:
                con.register("incoming", df)
                con.execute(f'INSERT INTO "{TABLE}" SELECT cik, ticker, '
                            "company_name, sic, metric, tag, "
                            "CAST(period_end AS DATE), CAST(filed AS DATE), "
                            "CAST(filing_period AS DATE), "
                            "value, form, fy, fp, frequency, accession "
                            "FROM incoming")
                con.unregister("incoming")

            print(f"  {year}Q{qtr}: {len(df):7,} rows "
                  f"({time.time() - started:.0f}s)")

        report(seen)

        if args.dry_run:
            print("\n(dry run -- nothing written)")
            return 0

        assert con is not None
        written = con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()
        print(f"\n{TABLE} written: {written[0] if written else 0:,} rows")
        return 0
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    sys.exit(main())
