#!/usr/bin/env python3
"""Build a backtesting database from scratch, from free sources, in one command.

stock_data.db was assembled by hand over months, one loader at a time, and it
cannot be shared: its prices come from Yahoo, whose terms forbid redistribution
(see DATA-LICENSE.md). So anyone else who wants to use the app has to build
their own -- and this is the one command that does it, running the same
loaders in the order the database needs them and filling the gaps that used to
be done by hand:

  prices       daily OHLCV for a chosen universe of tickers (Yahoo, via
               yfinance), into stock_data_optimized. Batched, and RESUMABLE:
               tickers already in the database are skipped, so an interrupted
               build picks up where it stopped.
  rates        3-month and 10-year Treasury yields from FRED, 1981+ -- the
               risk-free rate behind Sharpe and Sortino.
  macro        CPI, the fed funds rate, GDP, industrial production, M2 and
               unemployment from FRED, into economic_indicators (CPI is what
               the real-return metric deflates by).
  benchmarks   the $-prefixed index series ($SP500 back to 1927, $CASH, ...)
               via add_collections.py, then extend_risk_free.py to carry the
               risk-free rate back before 1981.
  sectors      SEC industry codes for the tickers you have (add_company_info.py).
  index        point-in-time S&P 500 membership (add_index_membership.py).
  quality      the per-ticker defect audit (add_data_quality.py).
  fundamentals SEC quarterly fundamentals, 2009+ (add_sec_financials.py).
               NOT run by default: ~7 GB of downloads and ~30-40 minutes.

No API keys are needed for any of it. FRED is read through its public CSV
download (fred_public.py) rather than the keyed API.

Usage:
    python3 scripts/build_db.py --db my.db --dry-run                 # show the plan
    python3 scripts/build_db.py --db my.db                           # starter universe
    python3 scripts/build_db.py --db my.db --universe sp500
    python3 scripts/build_db.py --db my.db --universe us-etfs --start 2005-01-01
    python3 scripts/build_db.py --db my.db --universe AAPL MSFT KO
    python3 scripts/build_db.py --db my.db --universe @tickers.txt   # one per line
    python3 scripts/build_db.py --db my.db --steps fundamentals      # add a step later

Universes: starter (~80 well-known stocks and ETFs -- a few minutes), sp500
(today's members), us-stocks, us-etfs, us-all (every US-listed symbol, from
NASDAQ's official symbol directory -- hours, and Yahoo will rate-limit you).

Every step is safe to re-run. Close the app first if it has THIS database open
(DuckDB is single-writer); open the result in the app with its database picker.

WHAT A BUILT DATABASE CANNOT DO, which is the same as the original: Yahoo keeps
no history for companies that no longer trade, so the universe contains only
survivors, and a backtest that picks among them is choosing among known
winners. The app's checks say so; this script cannot fix it.
"""
from __future__ import annotations

import argparse
import io
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import duckdb
import pandas as pd

import loader_provenance

ROOT = Path(__file__).resolve().parent.parent  # the repo root; scripts live in scripts/
PRICES = "stock_data_optimized"

STEPS = ["prices", "rates", "macro", "benchmarks", "sectors", "index",
         "quality", "fundamentals"]
DEFAULT_STEPS = [s for s in STEPS if s != "fundamentals"]

# The app bundles DuckDB 1.5. A newer writer can leave storage an older reader
# cannot scan -- that has happened to this project once already (T137), and
# the symptom was every cross-ticker query failing while single-ticker reads
# worked. So the writer's version line is checked, not assumed.
REQUIRED_DUCKDB = "1.5."

# Small enough to finish in minutes, broad enough to try every kind of idea the
# app's examples use: the largest US companies across sectors, the broad index
# and bond funds, the sector SPDRs, and a few commodities.
STARTER = [
    # Broad market, size and international
    "SPY", "VOO", "VTI", "QQQ", "DIA", "IWM", "MDY", "VEA", "VWO", "EFA",
    # Bonds and cash-like
    "AGG", "BND", "TLT", "IEF", "SHY", "LQD", "HYG", "TIP", "BIL",
    # Real assets
    "GLD", "SLV", "VNQ", "DBC", "USO",
    # Sectors
    "XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLU", "XLB", "XLRE", "XLC",
    # Factor funds
    "MTUM", "QUAL", "VLUE", "USMV", "SCHD", "VIG",
    # Large companies, across sectors
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "BRK-B", "TSLA", "AVGO",
    "JPM", "V", "MA", "BAC", "WFC", "GS", "UNH", "JNJ", "LLY", "PFE", "MRK",
    "ABBV", "XOM", "CVX", "PG", "KO", "PEP", "COST", "WMT", "HD", "MCD", "NKE",
    "DIS", "NFLX", "ADBE", "CRM", "ORCL", "INTC", "AMD", "CSCO", "IBM", "CAT",
    "BA", "GE", "T", "VZ", "NEE",
]

# SPY is required whatever the universe: add_collections.py builds the NYSE
# trading calendar every $ series rides on from index_levels plus SPY's rows.
ALWAYS = ["SPY"]

NASDAQ_LISTED = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
USER_AGENT = "Mozilla/5.0 (Backtester build_db)"

# Securities that are not a company's common stock or a fund: warrants, rights
# and units (SPAC leftovers, mostly), and preferred shares.
NOT_COMMON = ("warrant", "right", " unit", "units", "preferred", "depositary share")

FRED_RATES = {"3MTreasuryYields": "DGS3MO", "10YrTreasuryYields": "DGS10"}
FRED_MACRO = {
    "CPIAUCSL": "Consumer Price Index for All Urban Consumers",
    "FEDFUNDS": "Federal Funds Effective Rate",
    "GDP": "Gross Domestic Product",
    "INDPRO": "Industrial Production Index",
    "M2SL": "M2 Money Stock",
    "UNRATE": "Unemployment Rate",
}
TRADING_DAYS = 252


# --------------------------------------------------------------------------- #
# Universe
# --------------------------------------------------------------------------- #
def yahoo_symbol(symbol: str) -> str:
    """Exchange notation -> Yahoo's: share classes use a dash (BRK.B -> BRK-B)."""
    return symbol.strip().upper().replace(".", "-")


def parse_symbol_directory(text: str, want: str) -> list[str]:
    """NASDAQ's pipe-delimited symbol files -> Yahoo symbols.

    `want` is "stocks", "etfs" or "all". Both files carry an ETF flag and a
    Test Issue flag; the first column is the symbol in either. Test issues,
    preferreds (a `$` in the symbol) and the non-common securities named in
    NOT_COMMON are dropped; the trailer line ("File Creation Time") is not a
    symbol and has no ETF flag, so it falls out on its own.
    """
    df = pd.read_csv(io.StringIO(text), sep="|", dtype=str).fillna("")
    sym = df.columns[0]
    out = []
    for rec in df.to_dict("records"):
        s = str(rec[sym]).strip()
        if not s or "$" in s or rec.get("Test Issue", "N") != "N":
            continue
        flag = rec.get("ETF", "")
        if flag not in ("Y", "N"):
            continue  # the trailer line
        etf = flag == "Y"
        if want == "stocks" and etf or want == "etfs" and not etf:
            continue
        name = str(rec.get("Security Name", "")).lower()
        if not etf and any(k in name for k in NOT_COMMON):
            continue
        out.append(yahoo_symbol(s))
    return out


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read().decode("utf-8", errors="replace")


def resolve_universe(spec: list[str]) -> list[str]:
    """--universe arguments -> a sorted, de-duplicated list of Yahoo symbols."""
    out: set[str] = set(ALWAYS)
    for item in spec:
        key = item.lower()
        if key == "starter":
            out.update(STARTER)
        elif key == "sp500":
            from add_index_membership import fetch_current
            out.update(yahoo_symbol(t) for t in fetch_current()["ticker"])
        elif key in ("us-stocks", "us-etfs", "us-all"):
            want = {"us-stocks": "stocks", "us-etfs": "etfs", "us-all": "all"}[key]
            for url in (NASDAQ_LISTED, OTHER_LISTED):
                out.update(parse_symbol_directory(fetch_text(url), want))
        elif item.startswith("@"):
            lines = Path(item[1:]).read_text().splitlines()
            out.update(yahoo_symbol(t) for t in lines
                       if t.strip() and not t.lstrip().startswith("#"))
        else:
            out.add(yahoo_symbol(item))
    return sorted(out)


# --------------------------------------------------------------------------- #
# Steps written here (the rest run the existing loaders)
# --------------------------------------------------------------------------- #
def download_batch(symbols: list[str], start: str | None) -> dict[str, pd.DataFrame]:
    """One yfinance request for a batch -> {symbol: bars}; failures absent.

    Normalisation is sync_prices.py's, which the app's own refresh uses, so a
    built database and a synced one hold identical bars.
    """
    import yfinance as yf
    from sync_prices import frame_for, rows_for

    if start:
        df = yf.download(symbols, start=start, auto_adjust=True, actions=False,
                         progress=False, group_by="ticker", threads=True)
    else:
        df = yf.download(symbols, period="max", auto_adjust=True, actions=False,
                         progress=False, group_by="ticker", threads=True)
    out: dict[str, pd.DataFrame] = {}
    if df is None or df.empty:
        return out
    for s in symbols:
        rows = rows_for(frame_for(df, s))
        if rows:
            bars = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low",
                                               "Close", "Volume"])
            bars["Date"] = pd.to_datetime(bars["Date"])
            bars["Ticker"] = s
            out[s] = bars
    return out


def write_bars(con: duckdb.DuckDBPyConnection, bars: dict[str, pd.DataFrame]) -> int:
    """Replace each symbol's rows, in one transaction per batch."""
    if not bars:
        return 0
    df = pd.concat(bars.values(), ignore_index=True)
    con.register("incoming", df)
    con.execute("BEGIN")
    con.execute(f"DELETE FROM {PRICES} WHERE Ticker IN (SELECT DISTINCT Ticker FROM incoming)")
    con.execute(f"INSERT INTO {PRICES} (Date, Ticker, Open, High, Low, Close, Volume) "
                "SELECT CAST(Date AS TIMESTAMP_NS), Ticker, Open, High, Low, Close, "
                "Volume FROM incoming")
    con.execute("COMMIT")
    con.unregister("incoming")
    return len(df)


def step_prices(db: str, universe: list[str], args: argparse.Namespace) -> bool:
    from add_ticker import CREATE_TABLE

    con = duckdb.connect(db)
    try:
        con.execute(CREATE_TABLE)
        have = {r[0] for r in con.execute(f"SELECT DISTINCT Ticker FROM {PRICES}").fetchall()}
        todo = universe if args.refresh else [s for s in universe if s not in have]
        print(f"   {len(universe)} symbols in the universe, {len(universe) - len(todo)} "
              f"already here, {len(todo)} to download")

        failed: list[str] = []
        for i in range(0, len(todo), args.batch):
            batch = todo[i:i + args.batch]
            try:
                got = download_batch(batch, args.start)
            except Exception as e:
                print(f"   !! batch starting {batch[0]}: {e}")
                got = {}
            n = write_bars(con, got)
            failed += [s for s in batch if s not in got]
            print(f"   [{min(i + args.batch, len(todo))}/{len(todo)}] "
                  f"{len(got)}/{len(batch)} symbols, {n:,} bars")
            time.sleep(args.pause)

        # One retry, one at a time: a batch-level hiccup or a rate limit fails
        # symbols that download fine on their own.
        if failed:
            print(f"   retrying {len(failed)} one at a time")
            still = []
            for s in failed:
                try:
                    got = download_batch([s], args.start)
                except Exception:
                    got = {}
                write_bars(con, got)
                if s not in got:
                    still.append(s)
                time.sleep(args.pause)
            if still:
                shown = ", ".join(still[:20]) + (" ..." if len(still) > 20 else "")
                print(f"   {len(still)} symbols have no data from Yahoo "
                      f"(delisted, renamed or mistyped): {shown}")

        total = con.execute(f"SELECT COUNT(*) FROM {PRICES}").fetchone()
        loader_provenance.record(
            con, table=PRICES,
            source="Yahoo Finance via yfinance (auto-adjusted OHLCV)",
            endpoint="yf.download, batched (build_db.py)",
            licence="Yahoo terms of service; personal use only",
            rows=total[0] if total else 0,
            note=f"universe: {' '.join(args.universe)}; survivors only -- "
                 "Yahoo keeps no history for delisted symbols")
        return True
    finally:
        con.close()


def daily_rate(annual_pct: pd.Series) -> pd.Series:
    """The engine's convention: (1 + annual/100) ** (1/252) - 1."""
    return (1.0 + annual_pct / 100.0) ** (1.0 / TRADING_DAYS) - 1.0


def write_rates(con: duckdb.DuckDBPyConnection, table: str, annual: pd.Series) -> int:
    """Replace the rows the series covers; rows BEFORE it are left alone.

    That is what keeps a re-run from erasing the pre-1981 rows
    extend_risk_free.py back-fills from $CASH.
    """
    con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ('
                "Date TIMESTAMP_NS, annual_yield_percent DOUBLE, "
                "daily_risk_free_rate_decimal DOUBLE)")
    df = pd.DataFrame({"Date": annual.index, "annual_yield_percent": annual.values,
                       "daily_risk_free_rate_decimal": daily_rate(annual).values})
    con.register("incoming", df)
    con.execute("BEGIN")
    con.execute(f'DELETE FROM "{table}" WHERE Date >= CAST(? AS TIMESTAMP_NS)',
                [annual.index.min()])
    con.execute(f'INSERT INTO "{table}" SELECT CAST(Date AS TIMESTAMP_NS), '
                "annual_yield_percent, daily_risk_free_rate_decimal FROM incoming")
    con.execute("COMMIT")
    con.unregister("incoming")
    return len(df)


def step_rates(db: str, universe: list[str], args: argparse.Namespace) -> bool:
    from fred_public import fred_series
    con = duckdb.connect(db)
    try:
        for table, series in FRED_RATES.items():
            s = fred_series(series)
            n = write_rates(con, table, s)
            print(f"   {table}: {n:,} days, {s.index.min().date()} -> {s.index.max().date()}")
            total = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
            loader_provenance.record(
                con, table=table, source=f"FRED series {series}",
                endpoint=f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}",
                licence="FRED terms; most series are public domain",
                rows=total[0] if total else 0,
                note="daily_risk_free_rate_decimal = (1 + annual/100) ** (1/252) - 1")
        return True
    finally:
        con.close()


def write_macro(con: duckdb.DuckDBPyConnection, series_id: str, name: str,
                values: pd.Series) -> int:
    con.execute("CREATE TABLE IF NOT EXISTS economic_indicators ("
                "Date TIMESTAMP_NS, series_id VARCHAR, indicator_name VARCHAR, "
                "value DOUBLE)")
    df = pd.DataFrame({"Date": values.index, "value": values.values})
    df["series_id"], df["indicator_name"] = series_id, name
    con.register("incoming", df)
    con.execute("BEGIN")
    con.execute("DELETE FROM economic_indicators WHERE series_id = ?", [series_id])
    con.execute("INSERT INTO economic_indicators (Date, series_id, indicator_name, value) "
                "SELECT CAST(Date AS TIMESTAMP_NS), series_id, indicator_name, value "
                "FROM incoming")
    con.execute("COMMIT")
    con.unregister("incoming")
    return len(df)


def step_macro(db: str, universe: list[str], args: argparse.Namespace) -> bool:
    from fred_public import fred_series
    con = duckdb.connect(db)
    try:
        for series_id, name in FRED_MACRO.items():
            s = fred_series(series_id)
            n = write_macro(con, series_id, name, s)
            print(f"   {series_id:9} {n:5,} observations from {s.index.min().date()}")
        total = con.execute("SELECT COUNT(*) FROM economic_indicators").fetchone()
        loader_provenance.record(
            con, table="economic_indicators", source="FRED",
            endpoint="https://fred.stlouisfed.org/graph/fredgraph.csv",
            licence="FRED terms; most series are public domain",
            rows=total[0] if total else 0,
            note="Date is the PERIOD, not the release date: "
                 + ", ".join(FRED_MACRO))
        return True
    finally:
        con.close()


def step_index_levels(db: str) -> None:
    """index_levels' S&P 500 row set, from Yahoo's ^GSPC (1927+).

    add_collections.py builds $SP500's pre-ETF history and the whole NYSE
    calendar from this table. The original database got it by hand; here it is
    the price index level, unadjusted, which is what that script expects.
    """
    import yfinance as yf
    con = duckdb.connect(db)
    try:
        con.execute("CREATE TABLE IF NOT EXISTS index_levels ("
                    "Date TIMESTAMP_NS, Ticker VARCHAR, Close DOUBLE)")
        have = con.execute(
            "SELECT COUNT(*) FROM index_levels WHERE Ticker = 'SP500'").fetchone()
        if have and have[0] > 0:
            print(f"   index_levels: SP500 already present ({have[0]:,} days)")
            return
        df = yf.download("^GSPC", period="max", auto_adjust=False,
                         actions=False, progress=False)
        if df is None or df.empty:
            raise RuntimeError("Yahoo returned no ^GSPC history")
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        close = df["Close"].dropna()
        idx = pd.DatetimeIndex(close.index)
        if idx.tz is not None:
            idx = idx.tz_localize(None)
        out = pd.DataFrame({"Date": idx.normalize(), "Close": close.values})
        out["Ticker"] = "SP500"
        con.register("incoming", out)
        con.execute("INSERT INTO index_levels (Date, Ticker, Close) "
                    "SELECT CAST(Date AS TIMESTAMP_NS), Ticker, Close FROM incoming")
        con.unregister("incoming")
        print(f"   index_levels: SP500 {len(out):,} days from {out['Date'].min().date()}")
        loader_provenance.record(
            con, table="index_levels", source="Yahoo Finance ^GSPC (price level)",
            endpoint="yf.download ^GSPC period=max", licence="Yahoo terms of service; personal use only",
            rows=len(out), note="S&P 500 price index, no dividends; read by add_collections.py")
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# Running the existing loaders
# --------------------------------------------------------------------------- #
def run_loader(script: str, db: str, *extra: str) -> bool:
    """A loader as a child process, with this build's database.

    The builder holds no connection while it runs: DuckDB is single-writer, so
    an open handle here would lock the child out.
    """
    cmd = [sys.executable, str(Path(__file__).resolve().parent / script), "--db", db, *extra]
    print(f"   $ {' '.join(cmd[1:])}")
    return subprocess.run(cmd, cwd=ROOT).returncode == 0


def step_benchmarks(db: str, universe: list[str], args: argparse.Namespace) -> bool:
    step_index_levels(db)
    return (run_loader("add_collections.py", db)
            and run_loader("extend_risk_free.py", db))


def step_sectors(db: str, universe: list[str], args: argparse.Namespace) -> bool:
    return run_loader("add_company_info.py", db)


def step_index(db: str, universe: list[str], args: argparse.Namespace) -> bool:
    return run_loader("add_index_membership.py", db)


def step_quality(db: str, universe: list[str], args: argparse.Namespace) -> bool:
    return run_loader("add_data_quality.py", db)


def step_fundamentals(db: str, universe: list[str], args: argparse.Namespace) -> bool:
    return run_loader("add_sec_financials.py", db)


RUNNERS = {
    "prices": step_prices, "rates": step_rates, "macro": step_macro,
    "benchmarks": step_benchmarks, "sectors": step_sectors, "index": step_index,
    "quality": step_quality, "fundamentals": step_fundamentals,
}

ESTIMATE = {
    "prices": "~1-3 s per symbol", "rates": "seconds", "macro": "seconds",
    "benchmarks": "~1 minute", "sectors": "~0.15 s per symbol (SEC's rate limit)",
    "index": "seconds", "quality": "seconds",
    "fundamentals": "~30-40 minutes, ~7 GB of downloads",
}


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #
def check_duckdb(allow_any: bool) -> bool:
    if duckdb.__version__.startswith(REQUIRED_DUCKDB) or allow_any:
        return True
    print(f"Python has DuckDB {duckdb.__version__}, but the app reads its database "
          f"with DuckDB {REQUIRED_DUCKDB}x.\nA different version can write storage "
          "the app cannot read. Install the matching one:\n"
          f"    pip install 'duckdb=={REQUIRED_DUCKDB}*'\n"
          "or pass --any-duckdb if you know the app was built against yours.",
          file=sys.stderr)
    return False


def verify(db: str) -> None:
    """Scan every ticker, the way the app does.

    A single-ticker read proves little: the one real storage failure this
    project has had only showed up on a query that touched every block of the
    price table. So the check is a GROUP BY over the whole of it.
    """
    con = duckdb.connect(db, read_only=True)
    try:
        tables = [r[0] for r in con.execute(
            "SELECT table_name FROM duckdb_tables() ORDER BY 1").fetchall()]
        print("\nDatabase contents:")
        for t in tables:
            n = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()
            print(f"  {t:28} {n[0] if n else 0:>12,} rows")
        if PRICES in tables:
            per = con.execute(
                f"SELECT Ticker, COUNT(*), MIN(Date), MAX(Date) FROM {PRICES} "
                "GROUP BY Ticker").fetchall()
            real = [p for p in per if not p[0].startswith("$")]
            print(f"\nPrice scan OK: {len(real)} tickers and "
                  f"{len(per) - len(real)} $ benchmark series readable.")
            if real:
                first = min(p[2] for p in real).date()
                last = max(p[3] for p in real).date()
                print(f"Stock and ETF history runs {first} -> {last}.")
    finally:
        con.close()


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", required=True,
                    help="database file to create or extend, e.g. my_data.db")
    ap.add_argument("--universe", nargs="+", default=["starter"],
                    help="starter | sp500 | us-stocks | us-etfs | us-all | "
                         "@file | TICKER ... (combine freely; default starter)")
    ap.add_argument("--steps", default=",".join(DEFAULT_STEPS),
                    help=f"comma-separated, from: {','.join(STEPS)} "
                         f"(default: all but fundamentals)")
    ap.add_argument("--with-fundamentals", action="store_true",
                    help="add the SEC fundamentals step to the default steps")
    ap.add_argument("--start", default=None,
                    help="first date for prices, YYYY-MM-DD (default: all history)")
    ap.add_argument("--refresh", action="store_true",
                    help="re-download tickers already in the database")
    ap.add_argument("--batch", type=int, default=50,
                    help="symbols per Yahoo request (default 50)")
    ap.add_argument("--pause", type=float, default=1.0,
                    help="seconds between Yahoo requests (default 1)")
    ap.add_argument("--dry-run", action="store_true",
                    help="resolve the universe and print the plan; download nothing")
    ap.add_argument("--any-duckdb", action="store_true",
                    help="skip the DuckDB version check")
    args = ap.parse_args()
    # The loaders run as child processes writing to the same terminal; without
    # this, a piped run prints this script's lines after theirs.
    sys.stdout.reconfigure(line_buffering=True)

    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    if args.with_fundamentals and "fundamentals" not in steps:
        steps.append("fundamentals")
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        print(f"unknown steps {unknown}; choose from {STEPS}", file=sys.stderr)
        return 2
    steps = [s for s in STEPS if s in steps]  # the order the data needs

    if not check_duckdb(args.any_duckdb):
        return 1

    db = str(Path(args.db).expanduser().resolve())
    universe: list[str] = []
    if "prices" in steps:
        try:
            universe = resolve_universe(args.universe)
        except Exception as e:
            print(f"Could not resolve the universe: {e}", file=sys.stderr)
            return 1

    print(f"Database: {db}{'' if Path(db).exists() else ' (new)'}")
    if universe:
        shown = ", ".join(universe[:12]) + (" ..." if len(universe) > 12 else "")
        print(f"Universe: {len(universe)} symbols ({shown})")
    print("Steps:")
    for s in steps:
        print(f"  {s:13} {ESTIMATE[s]}")
    if args.dry_run:
        print("\n(dry run -- nothing downloaded or written)")
        return 0

    try:
        duckdb.connect(db).close()
    except duckdb.IOException as e:
        print(f"Could not open {db}: {e}\nIf the app has it open, close the app "
              "(DuckDB is single-writer).", file=sys.stderr)
        return 1

    results: dict[str, str] = {}
    for s in steps:
        print(f"\n== {s}")
        started = time.time()
        try:
            ok = RUNNERS[s](db, universe, args)
        except KeyboardInterrupt:
            print("\ninterrupted -- re-run the same command to resume")
            return 130
        except Exception as e:
            print(f"   !! {s} failed: {e}")
            ok = False
        results[s] = f"{'ok' if ok else 'FAILED'} ({time.time() - started:.0f}s)"

    print("\nSummary:")
    for s, r in results.items():
        print(f"  {s:13} {r}")
    verify(db)
    failed = [s for s, r in results.items() if r.startswith("FAILED")]
    if failed:
        print(f"\nRe-run with --steps {','.join(failed)} to retry what failed.")
    print(f"\nOpen {db} in the app with the database picker.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
