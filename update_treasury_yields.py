#!/usr/bin/env python3
"""Refresh 3MTreasuryYields and 10YrTreasuryYields from FRED.

Appends rows newer than each table's current max Date. The
daily_risk_free_rate_decimal column matches the historical convention:
    daily = (1 + annual_yield_percent/100) ** (1/252) - 1
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

import loader_provenance
from dotenv import load_dotenv
from fredapi import Fred

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "stock_data.db"

TABLES = {
    "3MTreasuryYields": "DGS3MO",
    "10YrTreasuryYields": "DGS10",
}

TRADING_DAYS = 252


def annual_pct_to_daily_decimal(pct: float) -> float:
    return (1.0 + pct / 100.0) ** (1.0 / TRADING_DAYS) - 1.0


def refresh(con: duckdb.DuckDBPyConnection, fred: Fred, table: str, series_id: str) -> int:
    current_max = con.execute(f'SELECT MAX(Date) FROM "{table}"').fetchone()[0]
    if current_max is None:
        start = date(2010, 1, 1)
    else:
        start = (current_max + timedelta(days=1)).date()

    today = date.today()
    if start > today:
        print(f"{table}: up to date (max={current_max})")
        return 0

    print(f"{table}: fetching {series_id} {start} -> {today}")
    series = fred.get_series(
        series_id,
        observation_start=start.isoformat(),
        observation_end=today.isoformat(),
    )
    series = series.dropna()
    if series.empty:
        print(f"{table}: no new observations")
        return 0

    df = pd.DataFrame({
        "Date": pd.to_datetime(series.index).floor("D"),
        "annual_yield_percent": series.values.astype(float),
    })
    df["daily_risk_free_rate_decimal"] = df["annual_yield_percent"].map(annual_pct_to_daily_decimal)

    con.register("incoming", df)
    con.execute(
        f'INSERT INTO "{table}" (Date, annual_yield_percent, daily_risk_free_rate_decimal) '
        'SELECT CAST(Date AS TIMESTAMP_NS), annual_yield_percent, daily_risk_free_rate_decimal '
        'FROM incoming '
        f'WHERE Date > (SELECT COALESCE(MAX(Date), TIMESTAMP \'1900-01-01\') FROM "{table}")'
    )
    con.unregister("incoming")

    new_max = con.execute(f'SELECT MAX(Date) FROM "{table}"').fetchone()[0]
    print(f"{table}: appended {len(df)} candidate rows, new max={new_max}")
    return len(df)


def main() -> int:
    load_dotenv(ROOT / ".env")
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        print("FRED_API_KEY not set", file=sys.stderr)
        return 1

    fred = Fred(api_key=api_key)
    con = duckdb.connect(str(DB_PATH))
    try:
        for table, series_id in TABLES.items():
            refresh(con, fred, table, series_id)
            total = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
            loader_provenance.record(
                con, table=table, source=f"FRED series {series_id}",
                endpoint="https://api.stlouisfed.org/fred/series/observations",
                licence="FRED terms; most series are public domain",
                rows=total[0] if total else 0,
                note="daily_risk_free_rate_decimal = "
                     "(1 + annual/100) ** (1/252) - 1")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
