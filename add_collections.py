#!/usr/bin/env python3
"""Add long-history "collection" benchmark assets to stock_data.db.

Populates stock_data_optimized with index/asset-class series like S&P 500,
Russell 2000, Total U.S. Market, etc. (the kind testfol.io exposes), plus a
Cash / T-bill asset -- so portfolios can hold and backtest against them.

Each collection is ONE daily series stored under a '$'-prefixed pseudo-ticker
(e.g. $SP500) so it can never collide with a real stock symbol. The Go engine
only reads stock_data_optimized and treats every ticker as a daily price, so
these are built to look exactly like a stock: daily rows, Open=Close, Volume=0.

Return convention -- every series is TOTAL RETURN (dividends reinvested):
  * Modern era: a real ETF's dividend-adjusted close (yfinance auto_adjust),
    which already reinvests dividends.
  * Pre-ETF era: the real index PRICE level (from yfinance ^INDEX or, for the
    S&P 500 back to 1927, the local index_levels table -- no re-fetch) with an
    ESTIMATED dividend added from Shiller's monthly S&P 500 dividend yield.
    So price history is 100% real; only pre-ETF dividend reinvestment is
    approximated. This is flagged per-asset in the printed coverage summary.

$CASH compounds the FRED 3-month T-bill rate (DTB3) back to 1954, matching the
daily-compounding convention already used for the risk-free-rate tables.

Usage:
    python3 add_collections.py                 # build + write all
    python3 add_collections.py --dry-run       # build + print coverage, no write
    python3 add_collections.py --only '$SP500' '$CASH'
    python3 add_collections.py --shiller path/to/ie_data.xls

Notes:
  * DuckDB is single-writer -- close the UI app first, or the open will fail.
  * Re-running is an idempotent full replace per symbol (delete + insert).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = os.environ.get("BACKTESTER_DB", str(ROOT / "stock_data.db"))
TABLE = "stock_data_optimized"
TRADING_DAYS = 252.0
# Each reconstructed series is scaled so its close on ANCHOR_DATE equals
# ANCHOR_PRICE. A fixed EARLY anchor (e.g. 100 in 1927) compounds to a ~$1.4M
# per-share price today, which makes share-sizing strategies misbehave. A fixed
# recent-ish date keeps prices realistic (~$100-600 in the modern era) AND
# stable across data refreshes -- appending newer bars never moves a historical
# anchor. Returns are scale-invariant, so metrics and results are unchanged. The
# date must exist in every series; 2010 predates none of them (youngest is
# $WORLD, 2008).
ANCHOR_DATE = pd.Timestamp("2010-01-04")
ANCHOR_PRICE = 100.0

SHILLER_URL = (
    "https://img1.wsimg.com/blobby/go/e5e77e0b-59d1-44d9-ab25-4763ac982e53/"
    "downloads/ie_data.xls"
)
DEFAULT_SHILLER = ROOT / "scratch_collections" / "ie_data.xls"

# A leg is one segment of a series' history.
#   kind="etf": yfinance symbol, auto-adjusted close -> real total return.
#   kind="index": yfinance ^symbol, price level -> price return + est. dividend.
#   kind="local": index_levels table Ticker, price level -> price + est. dividend.
#   kind="ref":  reuse another already-built collection's daily returns.
# Legs are listed oldest-first; where two legs overlap in time, the LATER leg
# wins (so a real ETF's total return supersedes the estimated-dividend index).
COLLECTIONS = {
    "$SP500": {
        "name": "S&P 500",
        "legs": [
            {"kind": "local", "src": "SP500"},
            {"kind": "etf", "src": "SPY"},
        ],
    },
    "$USTOT": {
        "name": "Total U.S. Market",
        "legs": [
            {"kind": "ref", "src": "$SP500"},
            {"kind": "etf", "src": "VTI"},
        ],
    },
    "$SP100": {
        "name": "S&P 100",
        "legs": [
            {"kind": "index", "src": "^OEX"},
            {"kind": "etf", "src": "OEF"},
        ],
    },
    "$SP400": {
        "name": "S&P 400 MidCap",
        "legs": [
            {"kind": "index", "src": "^SP400"},
            {"kind": "etf", "src": "MDY"},
        ],
    },
    "$SP600": {
        "name": "S&P 600 SmallCap",
        "legs": [
            {"kind": "index", "src": "^SP600"},
            {"kind": "etf", "src": "IJR"},
        ],
    },
    "$RUS2K": {
        "name": "Russell 2000",
        "legs": [
            {"kind": "index", "src": "^RUT"},
            {"kind": "etf", "src": "IWM"},
        ],
    },
    "$EXUS": {
        "name": "Total ex-US Market",
        "legs": [{"kind": "etf", "src": "VEU"}],
    },
    "$EM": {
        "name": "Emerging Markets",
        "legs": [{"kind": "etf", "src": "VWO"}],
    },
    "$WORLD": {
        "name": "Total World Market",
        "legs": [{"kind": "etf", "src": "VT"}],
    },
    "$CASH": {
        "name": "Cash (T-bill)",
        "legs": [
            {"kind": "cash_ff", "src": "RF"},   # 1-mo bill, 1926-1953 (Ibbotson/French)
            {"kind": "cash", "src": "DTB3"},    # 3-mo bill, 1954+ (FRED, daily)
        ],
    },
}

# Build order matters: $USTOT references $SP500, so $SP500 must build first.
BUILD_ORDER = [
    "$SP500", "$USTOT", "$SP100", "$SP400", "$SP600", "$RUS2K",
    "$EXUS", "$EM", "$WORLD", "$CASH",
]


# --------------------------------------------------------------------------- #
# Data sources
# --------------------------------------------------------------------------- #
def load_shiller_yield(path: Path) -> pd.Series:
    """Monthly S&P 500 annual dividend yield (D/P), indexed by month Period.

    Used as the dividend proxy for pre-ETF index price legs. Downloads the
    workbook to `path` if it isn't already present.
    """
    if not path.exists():
        import urllib.request
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f"   downloading Shiller data -> {path}")
        urllib.request.urlretrieve(SHILLER_URL, path)

    df = pd.read_excel(path, sheet_name="Data", skiprows=7)
    df = df[["Date", "P", "D"]].copy()
    df["Date"] = pd.to_numeric(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "P", "D"])
    yr = df["Date"].astype(int)
    mo = (df["Date"] * 100).round().astype(int) - yr * 100
    keep = (mo >= 1) & (mo <= 12)
    df, yr, mo = df[keep], yr[keep], mo[keep]
    stamps = pd.to_datetime(dict(year=yr, month=mo, day=1))
    period = pd.PeriodIndex(stamps, freq="M")
    yield_annual = (df["D"] / df["P"]).to_numpy(dtype=float)
    return pd.Series(yield_annual, index=period).sort_index()


def yf_prices(symbol: str, adjusted: bool) -> pd.Series:
    """Daily close for `symbol` (adjusted=total return, else raw price level)."""
    df = yf.download(
        symbol, period="max",
        auto_adjust=adjusted, actions=False, progress=False,
    )
    if df is None or df.empty:
        raise RuntimeError(f"yfinance returned no data for {symbol}")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    s = df["Close"].copy()
    s.index = pd.DatetimeIndex(s.index).tz_localize(None).normalize()
    return s.dropna()


def local_index_prices(con: duckdb.DuckDBPyConnection, ticker: str) -> pd.Series:
    """Daily close for `ticker` from the local index_levels table."""
    df = con.execute(
        "SELECT Date, Close FROM index_levels WHERE Ticker = ? ORDER BY Date",
        [ticker],
    ).df()
    if df.empty:
        raise RuntimeError(f"index_levels has no rows for {ticker}")
    s = pd.Series(df["Close"].values, index=pd.DatetimeIndex(df["Date"]).normalize())
    return s.dropna()


def fred_tbill_returns(series_id: str) -> pd.Series:
    """Daily total return of cash from a FRED annualized T-bill percent series."""
    from dotenv import load_dotenv
    from fredapi import Fred
    load_dotenv(ROOT / ".env")
    key = os.environ.get("FRED_API_KEY")
    if not key:
        raise RuntimeError("FRED_API_KEY not set (needed for $CASH)")
    raw = Fred(api_key=key).get_series(series_id).dropna()
    raw.index = pd.DatetimeIndex(raw.index).normalize()
    daily = (1.0 + raw.astype(float) / 100.0) ** (1.0 / TRADING_DAYS) - 1.0
    return daily


FF_FACTORS_URL = (
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
    "F-F_Research_Data_Factors_CSV.zip"
)
DEFAULT_FF = ROOT / "scratch_collections" / "F-F_Research_Data_Factors.csv"
# FRED's daily 3-month bill (DTB3) starts 1954-01-04; the French 1-month series
# fills everything strictly before that, so the two legs don't overlap.
DTB3_START = pd.Timestamp("1954-01-01")


def famafrench_rf_returns(path: Path) -> pd.Series:
    """Daily cash return before 1954 from Ken French's monthly 1-month T-bill.

    RF is a monthly return in percent from 1926-07; each month's return is spread
    across ~21 trading days (252/12) so it accrues smoothly, indexed at the month
    start. `align_to_calendar` then forward-fills it across the month. Truncated
    to before DTB3_START so it splices cleanly under the FRED daily series.
    """
    import io
    import zipfile
    if not path.exists():
        import urllib.request
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f"   downloading Fama-French factors -> {path}")
        raw = urllib.request.urlopen(FF_FACTORS_URL, timeout=60).read()
        member = zipfile.ZipFile(io.BytesIO(raw)).namelist()[0]
        text = zipfile.ZipFile(io.BytesIO(raw)).read(member).decode("latin-1")
        path.write_text(text)

    stamps, rates = [], []
    for line in path.read_text().splitlines():
        parts = [c.strip() for c in line.split(",")]
        if len(parts) >= 5 and len(parts[0]) == 6 and parts[0].isdigit():
            ts = pd.Timestamp(int(parts[0][:4]), int(parts[0][4:6]), 1)
            if ts < DTB3_START:
                stamps.append(ts)
                rates.append(float(parts[4]) / 100.0)  # RF percent -> monthly decimal
    monthly = pd.Series(rates, index=pd.DatetimeIndex(stamps))
    return (1.0 + monthly) ** (12.0 / TRADING_DAYS) - 1.0  # -> per-trading-day rate


def canonical_nyse_dates(con: duckdb.DuckDBPyConnection) -> pd.DatetimeIndex:
    """The NYSE trading-day calendar, from local data (no network).

    The Go engine indexes every ticker in a portfolio by the same day position,
    so all collections must share one calendar to be held together. Equity
    series already ride this calendar (index_levels / yfinance); $CASH (FRED
    bank calendar) is realigned onto it. Union of the S&P 500 index level dates
    (1927+) and SPY's rows (recent tail) spans the full range.
    """
    df = con.execute(
        "SELECT DISTINCT d FROM ("
        "  SELECT CAST(Date AS DATE) d FROM index_levels WHERE Ticker = 'SP500'"
        "  UNION SELECT CAST(Date AS DATE) d FROM stock_data_optimized WHERE Ticker = 'SPY'"
        ") ORDER BY d"
    ).df()
    return pd.DatetimeIndex(pd.to_datetime(df["d"])).normalize()


def align_to_calendar(
    returns: pd.Series, calendar: pd.DatetimeIndex, is_cash: bool,
) -> pd.Series:
    """Reindex a return series onto `calendar`, restricted to its own span.

    Equities fill absent days with a 0 return (no price change). Cash carries
    its rate forward (ffill) so it keeps accruing on days FRED didn't quote.
    """
    span = calendar[(calendar >= returns.index.min()) & (calendar <= returns.index.max())]
    out = returns.reindex(span.union(returns.index))
    out = out.ffill() if is_cash else out.fillna(0.0)
    return out.reindex(span)


# --------------------------------------------------------------------------- #
# Return construction
# --------------------------------------------------------------------------- #
def price_returns(prices: pd.Series) -> pd.Series:
    """Simple daily returns; first day is 0 so it anchors the rebuilt level."""
    r = prices.pct_change()
    r.iloc[0] = 0.0
    return r


def add_dividend_proxy(price_ret: pd.Series, yield_annual: pd.Series) -> pd.Series:
    """Add an estimated daily dividend to price returns using a monthly yield.

    daily_div = annual_yield / TRADING_DAYS, mapped by calendar month. Months
    with no yield value (outside Shiller coverage) contribute no dividend.
    """
    months = price_ret.index.to_period("M")
    ann = yield_annual.reindex(months).to_numpy(dtype=float)
    ann = np.where(np.isnan(ann), 0.0, ann)
    return price_ret + ann / TRADING_DAYS


def leg_returns(
    leg: dict,
    con: duckdb.DuckDBPyConnection,
    shiller: pd.Series,
    built: dict[str, pd.Series],
) -> pd.Series:
    """Daily total-return series for one leg."""
    kind, src = leg["kind"], leg["src"]
    if kind == "etf":
        return price_returns(yf_prices(src, adjusted=True))
    if kind == "index":
        return add_dividend_proxy(price_returns(yf_prices(src, adjusted=False)), shiller)
    if kind == "local":
        return add_dividend_proxy(price_returns(local_index_prices(con, src)), shiller)
    if kind == "ref":
        return built[src].copy()
    if kind == "cash":
        return fred_tbill_returns(src)
    if kind == "cash_ff":
        return famafrench_rf_returns(DEFAULT_FF)
    raise ValueError(f"unknown leg kind: {kind}")


def splice(legs_returns: list[pd.Series]) -> pd.Series:
    """Combine leg return series by date; later legs win on overlapping dates.

    Legs are listed oldest-first, so overwriting in order means a real ETF's
    total return supersedes the estimated-dividend index leg where they overlap.
    """
    combined = pd.Series(dtype=float)
    for r in legs_returns:
        combined = combined.reindex(combined.index.union(r.index))
        combined.loc[r.index] = r.values
    return combined.sort_index()


def returns_to_ohlcv(returns: pd.Series, symbol: str) -> pd.DataFrame:
    """Rebuild a synthetic price level from daily returns as OHLCV rows.

    The level is scaled so its value on ANCHOR_DATE is ANCHOR_PRICE (see the
    ANCHOR_DATE comment), keeping per-share prices realistic. Scaling is a
    constant factor, so daily returns -- and every metric -- are unaffected.
    """
    returns = returns.sort_index()
    returns.iloc[0] = 0.0
    level = (1.0 + returns).cumprod()
    ref = level.asof(ANCHOR_DATE)
    if pd.isna(ref):
        ref = level.iloc[0]  # series starts after ANCHOR_DATE; anchor its start
    level = level / ref * ANCHOR_PRICE
    return pd.DataFrame({
        "Date": level.index.normalize(),
        "Ticker": symbol,
        "Open": level.values,
        "High": level.values,
        "Low": level.values,
        "Close": level.values,
        "Volume": 0.0,
    })


def build_collection(
    symbol: str,
    con: duckdb.DuckDBPyConnection,
    shiller: pd.Series,
    calendar: pd.DatetimeIndex,
    built: dict[str, pd.Series],
) -> pd.DataFrame:
    spec = COLLECTIONS[symbol]
    legs = [leg_returns(leg, con, shiller, built) for leg in spec["legs"]]
    returns = align_to_calendar(splice(legs), calendar, is_cash=(symbol == "$CASH"))
    built[symbol] = returns  # allow later collections to reference this one
    return returns_to_ohlcv(returns, symbol)


# --------------------------------------------------------------------------- #
# Write
# --------------------------------------------------------------------------- #
def upsert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, has_adj: bool) -> None:
    symbol = df["Ticker"].iloc[0]
    cols = "Date, Ticker, Open, High, Low, Close, Volume"
    sel = "CAST(Date AS TIMESTAMP_NS), Ticker, Open, High, Low, Close, Volume"
    if has_adj:
        cols += ', "Adj Close"'
        sel += ", NULL"
    con.register("incoming", df)
    con.execute("BEGIN")
    con.execute(f"DELETE FROM {TABLE} WHERE Ticker = ?", [symbol])
    con.execute(f"INSERT INTO {TABLE} ({cols}) SELECT {sel} FROM incoming")
    con.execute("COMMIT")
    con.unregister("incoming")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--only", nargs="+", metavar="SYMBOL",
                    help="build only these symbols (e.g. '$SP500' '$CASH')")
    ap.add_argument("--shiller", type=Path, default=DEFAULT_SHILLER,
                    help="path to Shiller ie_data.xls (downloaded if absent)")
    ap.add_argument("--dry-run", action="store_true",
                    help="build + print coverage; do not write to the DB")
    args = ap.parse_args()

    wanted = args.only or BUILD_ORDER
    unknown = [s for s in wanted if s not in COLLECTIONS]
    if unknown:
        print(f"unknown symbols: {unknown}\nknown: {list(COLLECTIONS)}", file=sys.stderr)
        return 2
    # Preserve dependency-correct order and pull in refs ($USTOT needs $SP500).
    need = set(wanted)
    for s in list(need):
        for leg in COLLECTIONS[s]["legs"]:
            if leg["kind"] == "ref":
                need.add(leg["src"])
    order = [s for s in BUILD_ORDER if s in need]

    needs_shiller = any(
        leg["kind"] in ("index", "local")
        for s in order for leg in COLLECTIONS[s]["legs"]
    )
    shiller = load_shiller_yield(args.shiller) if needs_shiller else pd.Series(dtype=float)

    try:
        con = duckdb.connect(args.db, read_only=args.dry_run)
    except duckdb.IOException as e:
        print(f"Could not open {args.db}: {e}\n"
              "If a lock is held, close the UI app (DuckDB is single-writer).",
              file=sys.stderr)
        return 1

    try:
        has_adj = "Adj Close" in [
            r[1] for r in con.execute(f"PRAGMA table_info('{TABLE}')").fetchall()
        ]
        calendar = canonical_nyse_dates(con)
        built: dict[str, pd.Series] = {}
        summary = []
        for symbol in order:
            df = build_collection(symbol, con, shiller, calendar, built)
            first, last = df["Date"].min().date(), df["Date"].max().date()
            if symbol in wanted and not args.dry_run:
                upsert(con, df, has_adj)
            summary.append((symbol, COLLECTIONS[symbol]["name"], first, last, len(df)))
            tag = "" if (symbol in wanted and not args.dry_run) else " (not written)"
            print(f"== {symbol:8} {COLLECTIONS[symbol]['name']:20} "
                  f"{first} -> {last}  {len(df):6d} rows{tag}")

        print("\nCoverage summary:")
        for symbol, name, first, last, n in summary:
            print(f"  {symbol:8} {name:22} {first} .. {last}  ({n} days)")
        if args.dry_run:
            print("\n(dry run -- nothing written)")
        else:
            print("\nWritten to", args.db)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
