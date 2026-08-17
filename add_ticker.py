#!/usr/bin/env python3
"""Add or refresh ticker data in stock_data.db using yfinance (free).

Fetches OHLCV history and writes it into `stock_data_optimized`, matching the
table's conventions: TIMESTAMP_NS dates at midnight, split/dividend-adjusted
prices, Volume as DOUBLE, and "Adj Close" left NULL.

The operation is an idempotent upsert per ticker: rows for that ticker within
the fetched date range are deleted, then re-inserted. So re-running is safe,
and topping up recent data (e.g. --start last-week) won't disturb history.

Examples:
    python3 add_ticker.py BRK-A BRK-B          # full coverage of existing table
    python3 add_ticker.py NVDA --full          # all history yfinance has
    python3 add_ticker.py SPY --start 2024-01-01 --end 2024-12-31
    python3 add_ticker.py AAPL --dry-run       # fetch + preview, no write

Note: DuckDB is single-writer. Close the UI app before running, or the open
will fail with a lock error.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

import loader_provenance
import yfinance as yf

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = os.environ.get("BACKTESTER_DB", str(ROOT / "stock_data.db"))
TABLE = "stock_data_optimized"

# Used when the table is empty/new and no --start is given, matching the
# existing data's earliest coverage.
FALLBACK_START = "2010-06-17"

CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    Date        TIMESTAMP_NS,
    Ticker      VARCHAR,
    Open        DOUBLE,
    High        DOUBLE,
    Low         DOUBLE,
    Close       DOUBLE,
    Volume      DOUBLE,
    "Adj Close" DOUBLE
)
"""


def fetch(symbol: str, start: str | None, end: str | None, adjusted: bool,
          full: bool = False) -> pd.DataFrame:
    """Download one symbol's OHLCV from yfinance, normalized to the table shape.

    Returns an empty DataFrame if yfinance has no data for the symbol/range.
    Dates are midnight, tz-naive; Volume is float to match the DOUBLE column.

    full=True pulls the entire available history via period="max". yfinance
    returns only ~1 month when start is None, so --full must not rely on an
    open-ended start/end range.
    """
    if full:
        df = yf.download(
            symbol, period="max",
            auto_adjust=adjusted, actions=False, progress=False,
        )
    else:
        df = yf.download(
            symbol, start=start, end=end,
            auto_adjust=adjusted, actions=False, progress=False,
        )
    if df is None or df.empty:
        return pd.DataFrame()

    # Single-symbol downloads come back with a (Price, Ticker) MultiIndex.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    out = df.reset_index().loc[:, ["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
    # yfinance dates may be tz-aware; flatten to tz-naive midnight to match the
    # table's TIMESTAMP_NS column.
    dates = out["Date"]
    if dates.dt.tz is not None:
        dates = dates.dt.tz_localize(None)
    out["Date"] = dates.dt.normalize()
    out["Volume"] = out["Volume"].astype("float64")
    out["Ticker"] = symbol
    return out


def upsert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, has_adj: bool) -> None:
    """Replace the ticker's rows within the fetched range, in one transaction."""
    symbol = df["Ticker"].iloc[0]
    lo, hi = df["Date"].min(), df["Date"].max()

    cols = 'Date, Ticker, Open, High, Low, Close, Volume'
    sel = ('CAST(Date AS TIMESTAMP_NS), Ticker, Open, High, Low, Close, Volume')
    if has_adj:
        cols += ', "Adj Close"'
        sel += ', NULL'

    con.register("incoming", df)
    con.execute("BEGIN")
    con.execute(
        f"DELETE FROM {TABLE} WHERE Ticker = ? AND Date BETWEEN ? AND ?",
        [symbol, lo, hi],
    )
    con.execute(f"INSERT INTO {TABLE} ({cols}) SELECT {sel} FROM incoming")
    con.execute("COMMIT")
    con.unregister("incoming")


def resolve_start(con: duckdb.DuckDBPyConnection, table_exists: bool) -> str:
    """Default --start: the table's earliest date, so new tickers line up."""
    if table_exists:
        row = con.execute(f"SELECT MIN(Date) FROM {TABLE}").fetchone()
        if row and row[0] is not None:
            return row[0].date().isoformat()
    return FALLBACK_START


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Add or refresh tickers in stock_data.db via yfinance.",
    )
    parser.add_argument("tickers", nargs="+", help="Symbols, e.g. BRK-A NVDA SPY")
    parser.add_argument("--db", default=DEFAULT_DB, help=f"DuckDB path (default: {DEFAULT_DB})")
    parser.add_argument("--start", help="YYYY-MM-DD (default: table's earliest date)")
    parser.add_argument("--end", help="YYYY-MM-DD, exclusive (default: today)")
    parser.add_argument("--full", action="store_true", help="Fetch all available history")
    parser.add_argument("--raw", action="store_true", help="Store unadjusted prices")
    parser.add_argument("--dry-run", action="store_true", help="Fetch + preview, don't write")
    args = parser.parse_args()

    try:
        con = duckdb.connect(args.db)
    except duckdb.IOException as e:
        print(f"Could not open {args.db}: {e}\n"
              "If a lock is held, close the UI app (DuckDB is single-writer).",
              file=sys.stderr)
        return 1

    try:
        con.execute(CREATE_TABLE)
        cols = [r[1] for r in con.execute(f"PRAGMA table_info('{TABLE}')").fetchall()]
        has_adj = "Adj Close" in cols
        count_row = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()
        table_exists = bool(count_row[0]) if count_row else False

        start = None if args.full else (args.start or resolve_start(con, table_exists))
        end = args.end or date.today().isoformat()
        adjusted = not args.raw

        errors = 0
        for symbol in (t.upper() for t in args.tickers):
            df = fetch(symbol, start, end, adjusted, full=args.full)
            if df.empty:
                print(f"!! {symbol}: no data from yfinance — skipped")
                errors += 1
                continue

            print(f"== {symbol}: {len(df)} rows, "
                  f"{df.Date.min().date()} -> {df.Date.max().date()}, "
                  f"close {df.Close.min():.2f}..{df.Close.max():.2f}"
                  f"{' [adjusted]' if adjusted else ' [raw]'}")

            if args.dry_run:
                continue

            upsert(con, df, has_adj)
            stored = con.execute(
                f"SELECT COUNT(*), MIN(Date), MAX(Date) FROM {TABLE} WHERE Ticker = ?",
                [symbol],
            ).fetchone()
            if stored:
                n, mn, mx = stored
                print(f"   stored: {n} rows in DB, {mn.date()} -> {mx.date()}")

        if args.dry_run:
            print("\n(dry run — nothing written)")
        else:
            total = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()
            loader_provenance.record(
                con, table=TABLE,
                source="Yahoo Finance via yfinance (auto-adjusted OHLCV)",
                endpoint="yfinance Ticker.history / yf.download",
                licence="Yahoo terms of service; personal use only",
                rows=total[0] if total else 0,
                note="real companies; the $-prefixed benchmark series in the "
                     "same table come from add_collections.py. Yahoo serves no "
                     "history for delisted symbols and returns one unbroken "
                     "series for a recycled one")
        return 1 if errors else 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
