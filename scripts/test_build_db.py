#!/usr/bin/env python3
"""Tests for build_db.py and fred_public.py.

Run with:  python3 -m pytest scripts/test_build_db.py -q

Nothing here downloads anything or opens a file on disk: the writers run
against an in-memory DuckDB, and the parsers are fed text shaped like the real
files (NASDAQ's symbol directories, FRED's CSV download).
"""
from __future__ import annotations

import duckdb
import pandas as pd
import pytest

import build_db
from fred_public import parse_fred_csv

NASDAQ = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N
QQQ|Invesco QQQ Trust, Series 1|G|N|N|100|Y|N
ZXZZT|NASDAQ TEST STOCK|G|Y|N|100|N|N
ABCDW|Some Acquisition Corp - Warrant|S|N|N|100|N|N
ABCDU|Some Acquisition Corp - Units|S|N|N|100|N|N
File Creation Time: 0929202609:46|||||||
"""

OTHER = """ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
BRK.B|Berkshire Hathaway Inc. Class B Common Stock|N|BRK.B|N|100|N|BRK.B
ABR$D|Arbor Realty Trust 6.375% Series D Preferred Stock|N|ABRpD|N|100|N|ABR-D
SPY|SPDR S&P 500 ETF Trust|P|SPY|Y|100|N|SPY
File Creation Time: 0929202609:46||||||
"""


def test_symbol_directories_split_stocks_from_funds():
    assert build_db.parse_symbol_directory(NASDAQ, "stocks") == ["AAPL"]
    assert build_db.parse_symbol_directory(NASDAQ, "etfs") == ["QQQ"]
    # Share classes take Yahoo's dash; preferreds ($) never make it in.
    assert build_db.parse_symbol_directory(OTHER, "all") == ["BRK-B", "SPY"]


def test_test_issues_warrants_units_and_the_trailer_are_dropped():
    got = build_db.parse_symbol_directory(NASDAQ, "all")
    assert got == ["AAPL", "QQQ"]


def test_universe_combines_presets_files_and_symbols(tmp_path):
    f = tmp_path / "mine.txt"
    f.write_text("# my list\nko\n\nbrk.a\n")
    got = build_db.resolve_universe([f"@{f}", "msft", "KO"])
    # SPY rides along whatever is asked for: the benchmarks need its calendar.
    assert got == ["BRK-A", "KO", "MSFT", "SPY"]
    starter = build_db.resolve_universe(["starter"])
    assert "SPY" in starter and "AAPL" in starter
    assert len(starter) == len(set(starter))


def test_fred_csv_drops_missing_observations():
    text = "observation_date,DGS3MO\n2024-01-02,5.40\n2024-01-15,.\n2024-01-16,5.38\n"
    s = parse_fred_csv(text)
    assert list(s.index.strftime("%Y-%m-%d")) == ["2024-01-02", "2024-01-16"]
    assert s.iloc[-1] == pytest.approx(5.38)


def test_rates_rewrite_keeps_the_back_filled_history():
    """extend_risk_free.py fills the years before FRED's series from $CASH.
    Re-running the rates step must replace FRED's span and leave that alone."""
    con = duckdb.connect()
    first = pd.Series([5.0, 5.1], index=pd.to_datetime(["1981-09-01", "1981-09-02"]))
    build_db.write_rates(con, "3MTreasuryYields", first)
    con.execute("INSERT INTO \"3MTreasuryYields\" VALUES "
                "(TIMESTAMP '1960-01-04', 3.0, 0.0001)")
    again = pd.Series([5.2, 5.3], index=pd.to_datetime(["1981-09-01", "1981-09-02"]))
    build_db.write_rates(con, "3MTreasuryYields", again)
    rows = con.execute('SELECT CAST(Date AS DATE), annual_yield_percent '
                       'FROM "3MTreasuryYields" ORDER BY Date').fetchall()
    assert [(str(d), v) for d, v in rows] == [
        ("1960-01-04", 3.0), ("1981-09-01", 5.2), ("1981-09-02", 5.3)]
    daily = con.execute('SELECT daily_risk_free_rate_decimal FROM "3MTreasuryYields" '
                        "WHERE annual_yield_percent = 5.2").fetchone()
    assert daily is not None
    assert daily[0] == pytest.approx(1.052 ** (1 / 252) - 1)


def test_macro_series_replace_themselves_only():
    con = duckdb.connect()
    idx = pd.to_datetime(["2024-01-01", "2024-02-01"])
    build_db.write_macro(con, "CPIAUCSL", "CPI", pd.Series([300.0, 301.0], index=idx))
    build_db.write_macro(con, "UNRATE", "Unemployment", pd.Series([3.7, 3.9], index=idx))
    build_db.write_macro(con, "CPIAUCSL", "CPI", pd.Series([310.0], index=idx[:1]))
    got = con.execute("SELECT series_id, COUNT(*) FROM economic_indicators "
                      "GROUP BY 1 ORDER BY 1").fetchall()
    assert got == [("CPIAUCSL", 1), ("UNRATE", 2)]


def test_bars_replace_a_ticker_whole_and_leave_the_others():
    from add_ticker import CREATE_TABLE
    con = duckdb.connect()
    con.execute(CREATE_TABLE)

    def bars(sym: str, days: list[str]) -> pd.DataFrame:
        df = pd.DataFrame({"Date": pd.to_datetime(days), "Open": 1.0, "High": 1.0,
                           "Low": 1.0, "Close": 1.0, "Volume": 0.0})
        df["Ticker"] = sym
        return df

    build_db.write_bars(con, {"AAA": bars("AAA", ["2024-01-02", "2024-01-03"]),
                              "BBB": bars("BBB", ["2024-01-02"])})
    build_db.write_bars(con, {"AAA": bars("AAA", ["2024-01-04"])})
    got = con.execute("SELECT Ticker, COUNT(*) FROM stock_data_optimized "
                      "GROUP BY 1 ORDER BY 1").fetchall()
    assert got == [("AAA", 1), ("BBB", 1)]
    assert build_db.write_bars(con, {}) == 0
