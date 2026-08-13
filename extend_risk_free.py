#!/usr/bin/env python3
"""Backfill 3MTreasuryYields with pre-2010 risk-free rates from the $CASH asset.

The Go engine's Sharpe/Sortino use per-day risk-free rates from the
3MTreasuryYields table, which FRED only covers from 2010-06-17. Any backtest
starting earlier therefore scored risk-adjusted metrics over just the 2010+
tail. This fills the gap using the $CASH collection asset already in the DB:
its daily return IS the daily risk-free rate (it compounds the T-bill rate),
and it is aligned to the NYSE trading calendar back to 1927 -- so the two stay
perfectly consistent (a 100% $CASH portfolio has ~0 excess return).

By default it inserts only dates earlier than the table's current minimum, so
the existing FRED-sourced rows are left untouched. --full rebuilds every row
from $CASH.

Usage:
    python3 extend_risk_free.py            # backfill pre-2010 from $CASH
    python3 extend_risk_free.py --full     # rebuild the whole table from $CASH
    python3 extend_risk_free.py --dry-run
Requires the UI app closed (DuckDB is single-writer) and $CASH present
(run add_collections.py first).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = os.environ.get("BACKTESTER_DB", str(ROOT / "stock_data.db"))
TABLE = "3MTreasuryYields"
TRADING_DAYS = 252.0


def build_rates(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Daily risk-free rows derived from the $CASH asset's returns."""
    df = con.execute(
        "SELECT Date, Close FROM stock_data_optimized "
        "WHERE Ticker = '$CASH' ORDER BY Date"
    ).df()
    if df.empty:
        raise RuntimeError("$CASH not found -- run add_collections.py first")

    close = df["Close"].astype(float)
    daily = close.pct_change()          # daily_risk_free_rate_decimal
    # Annualize the daily rate back to the table's percent convention:
    # (1 + daily)^252 - 1, the inverse of how the daily rate was derived.
    annual_pct = ((1.0 + daily) ** TRADING_DAYS - 1.0) * 100.0

    out = pd.DataFrame({
        "Date": pd.DatetimeIndex(df["Date"]).normalize(),
        "annual_yield_percent": annual_pct.values,
        "daily_risk_free_rate_decimal": daily.values,
    })
    return out.dropna(subset=["daily_risk_free_rate_decimal"])


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--full", action="store_true",
                    help="rebuild every row from $CASH (default: backfill pre-min only)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    try:
        con = duckdb.connect(args.db, read_only=args.dry_run)
    except duckdb.IOException as e:
        print(f"Could not open {args.db}: {e}\n"
              "If a lock is held, close the UI app (DuckDB is single-writer).",
              file=sys.stderr)
        return 1

    try:
        rows = build_rates(con)
        cur_min = con.execute(f'SELECT MIN(Date) FROM "{TABLE}"').fetchone()[0]

        if args.full or cur_min is None:
            incoming = rows
            where = None
        else:
            cutoff = pd.Timestamp(cur_min).normalize()
            incoming = rows[rows["Date"] < cutoff]
            where = cutoff

        print(f"$CASH-derived rates: {rows['Date'].min().date()} .. "
              f"{rows['Date'].max().date()} ({len(rows)} days)")
        print(f"table current min: {cur_min}")
        print(f"to insert: {len(incoming)} rows"
              + ("" if where is None else f" (before {where.date()})")
              + (" [FULL REBUILD]" if args.full else ""))

        if args.dry_run:
            print("\n(dry run -- nothing written)")
            return 0

        con.register("incoming", incoming)
        con.execute("BEGIN")
        if args.full:
            con.execute(f'DELETE FROM "{TABLE}"')
        con.execute(
            f'INSERT INTO "{TABLE}" '
            "(Date, annual_yield_percent, daily_risk_free_rate_decimal) "
            "SELECT CAST(Date AS TIMESTAMP_NS), annual_yield_percent, "
            "daily_risk_free_rate_decimal FROM incoming"
        )
        con.execute("COMMIT")
        con.unregister("incoming")

        new_min, new_max, n = con.execute(
            f'SELECT MIN(Date), MAX(Date), COUNT(*) FROM "{TABLE}"'
        ).fetchone()
        print(f"\n{TABLE} now: {new_min} .. {new_max} ({n} rows)")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
