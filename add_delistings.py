#!/usr/bin/env python3
"""Load the delisting registry from Alpha Vantage into the `delistings` table.

A ticker symbol is not a company -- it is a lease. When a company dies the
exchange re-issues its symbol, and a price loader keyed on the symbol alone
splices the two companies into one unbroken series. `stock_data_optimized` has
269 such series (DD, SUNE, COR, DRS and friends); this table is what lets the
Go engine find them. See `src/data/delistings.go`.

Source: Alpha Vantage LISTING_STATUS, free tier. One HTTP call returns the
whole delisted universe as CSV, so the free tier's ~25 calls/day is not a
constraint. Needs ALPHA_VANTAGE_KEY in .env or the environment.

COVERAGE, WHICH IS THE IMPORTANT CAVEAT: this feed effectively begins in 2013
and is only dense from 2015. Eight rows predate 2009, so Enron, WorldCom,
Lehman, WaMu and Bear Stearns are simply not in it. This closes the recycled-
ticker hole; it does NOT close the pre-2013 survivorship hole, which still
needs paid data (see T20).

Rows are classified rather than filtered, so the table keeps everything and
the caller decides:
  security_class = 'operating'  real operating companies (the useful ones)
                   'etf'        assetType == ETF
                   'derivative' SPAC units, warrants, rights, preferreds --
                                symbols that never were an operating company

Usage:
    python3 add_delistings.py --dry-run     # fetch, classify, print, write nothing
    python3 add_delistings.py               # replace the table
Requires the UI app closed (DuckDB is single-writer).
"""
from __future__ import annotations

import argparse
import io
import os
import sys
import urllib.request
from pathlib import Path

import duckdb
import pandas as pd

import loader_provenance

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = os.environ.get("BACKTESTER_DB", str(ROOT / "stock_data.db"))
TABLE = "delistings"
ENDPOINT = ("https://www.alphavantage.co/query"
            "?function=LISTING_STATUS&state=delisted&apikey={key}")

# Symbols that were never an operating company. Matched against the vendor's
# name field, which is where the instrument type actually shows up -- the
# assetType column calls a SPAC warrant a "Stock".
DERIVATIVE_PATTERN = (
    r"- Units|- Warrant|Warrants|Contingent Value|- Rights|\bRight\b|"
    r"Preferred|When Issued|Trust Preferred"
)


def read_key() -> str:
    """ALPHA_VANTAGE_KEY from the environment, falling back to .env."""
    key = os.environ.get("ALPHA_VANTAGE_KEY")
    if key:
        return key.strip()
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            name, _, value = line.partition("=")
            if name.strip() == "ALPHA_VANTAGE_KEY":
                return value.strip().strip("'\"")
    raise RuntimeError(
        "ALPHA_VANTAGE_KEY not found in the environment or .env. "
        "A free key: https://www.alphavantage.co/support/#api-key"
    )


def fetch(key: str) -> pd.DataFrame:
    """The whole delisted universe, as returned by Alpha Vantage."""
    with urllib.request.urlopen(ENDPOINT.format(key=key), timeout=120) as resp:
        body = resp.read().decode("utf-8")
    # The API reports quota exhaustion and bad keys as a JSON body with HTTP
    # 200, which would otherwise land in pandas as a one-column frame.
    if not body.lstrip().startswith("symbol,"):
        raise RuntimeError(f"unexpected response from Alpha Vantage: {body[:300]}")
    return pd.read_csv(io.StringIO(body))


def classify(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalise columns and tag each row with its security_class."""
    df = pd.DataFrame({
        "symbol": raw["symbol"].astype(str).str.strip(),
        "name": raw["name"].fillna("").astype(str).str.strip(),
        "exchange": raw["exchange"].fillna("").astype(str).str.strip(),
        "asset_type": raw["assetType"].fillna("").astype(str).str.strip(),
        "ipo_date": pd.to_datetime(raw["ipoDate"], errors="coerce"),
        "delisting_date": pd.to_datetime(raw["delistingDate"], errors="coerce"),
    })

    is_etf = df["asset_type"].str.upper() == "ETF"
    is_derivative = df["name"].str.contains(
        DERIVATIVE_PATTERN, case=False, na=False, regex=True)

    df["security_class"] = "operating"
    df.loc[is_derivative, "security_class"] = "derivative"
    df.loc[is_etf, "security_class"] = "etf"

    # A row with no delisting date carries no information, and the Go side
    # drops it anyway; dropping it here keeps the table honest about its size.
    return df.dropna(subset=["delisting_date"]).reset_index(drop=True)


def report(df: pd.DataFrame, con: duckdb.DuckDBPyConnection) -> None:
    """Print what was fetched and what it implies for the price table."""
    print(f"fetched {len(df)} delisted symbols")
    print("\nby security_class:")
    for cls, n in df["security_class"].value_counts().items():
        print(f"  {cls:12} {n:6}")

    years = df["delisting_date"].dt.year
    print(f"\ndelisting dates: {years.min()} .. {years.max()}")
    print("  pre-2009 rows:", int((years < 2009).sum()),
          "  <- why this does not fix pre-2013 survivorship")

    operating = df[df["security_class"] == "operating"]
    con.register("op", operating[["symbol", "delisting_date"]])
    overlap = con.execute("""
        SELECT date_diff('day', o.delisting_date::date, MAX(s."Date")::date) AS past
        FROM op o JOIN stock_data_optimized s ON s."Ticker" = o.symbol
        GROUP BY o.symbol, o.delisting_date
    """).df()
    con.unregister("op")

    if overlap.empty:
        print("\nno operating delistings overlap the price table")
        return

    past = overlap["past"]
    print(f"\n{len(overlap)} priced tickers appear on the delisted list:")
    print(f"  {int((past <= 5).sum()):5}  clean      bars end at/before delisting")
    print(f"  {int(past.between(6, 89).sum()):5}  stale_tail bars run days past")
    print(f"  {int(past.between(90, 730).sum()):5}  suspect    bars run months past")
    print(f"  {int((past > 730).sum()):5}  RECYCLED   bars run years past "
          "-- two companies in one series")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    try:
        key = read_key()
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 1

    try:
        rows = classify(fetch(key))
    except Exception as e:
        print(f"Could not fetch the delisting registry: {e}", file=sys.stderr)
        return 1

    try:
        con = duckdb.connect(args.db, read_only=args.dry_run)
    except duckdb.IOException as e:
        print(f"Could not open {args.db}: {e}\n"
              "If a lock is held, close the UI app (DuckDB is single-writer).",
              file=sys.stderr)
        return 1

    try:
        report(rows, con)

        if args.dry_run:
            print("\n(dry run -- nothing written)")
            return 0

        con.register("incoming", rows)
        con.execute("BEGIN")
        # Replaced wholesale rather than merged: the registry is a full
        # snapshot every time, and a delisting date can be revised.
        con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
        con.execute(f"""
            CREATE TABLE "{TABLE}" (
                symbol          VARCHAR NOT NULL,
                name            VARCHAR,
                exchange        VARCHAR,
                asset_type      VARCHAR,
                security_class  VARCHAR,
                ipo_date        TIMESTAMP_NS,
                delisting_date  TIMESTAMP_NS NOT NULL
            )
        """)
        con.execute(f"""
            INSERT INTO "{TABLE}"
            SELECT symbol, name, exchange, asset_type, security_class,
                   CAST(ipo_date AS TIMESTAMP_NS),
                   CAST(delisting_date AS TIMESTAMP_NS)
            FROM incoming
        """)
        con.execute("COMMIT")
        con.unregister("incoming")

        written = con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()
        loader_provenance.record(
            con, table=TABLE, source="Alpha Vantage LISTING_STATUS (delisted)",
            endpoint=ENDPOINT.format(key="<key>"),
            licence="Alpha Vantage free tier, personal use",
            rows=written[0] if written else 0,
            note="coverage effectively begins 2013, dense from 2015; does not "
                 "close pre-2013 survivorship")
        print(f"\n{TABLE} written: {written[0] if written else 0} rows")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
