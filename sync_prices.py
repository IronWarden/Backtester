#!/usr/bin/env python3
"""Fetch one batch of symbols' recent OHLCV from yfinance and print it as JSON.

Companion to `ui --sync <db>`. Like fetch_history.py -- and unlike
add_ticker.py -- this script never opens the DuckDB, because the Go side
holds the write lock; it only downloads and normalizes, printing one JSON
object to stdout. The Go side performs the upsert through its own handle.

What makes it different from fetch_history.py is the batching: yfinance takes
a whole list of symbols in ONE request but only one date range for all of them,
so a refresh is affordable exactly to the extent that its symbols share a start
date. In a database that has been synced before, nearly all of them do -- which
is why src/data.SyncBatches groups by start date before calling this.

Usage:
    python3 sync_prices.py --start 2026-07-03 AAPL MSFT NVDA
    python3 sync_prices.py --start 2026-07-03 --end 2026-09-08 AAPL MSFT

Output: {"results": [...]} with one entry per symbol, in the order given; each
entry is {"symbol", "rows": [["YYYY-MM-DD", open, high, low, close, volume]]}
or {"symbol", "error": "..."}. Prices are split/dividend-adjusted, matching
stock_data_optimized's conventions. A symbol with no new sessions in the range
comes back with an empty "rows" list, which is a normal, non-error outcome:
a ticker that has not traded since its last bar has nothing to add.

A symbol yfinance FAILED to download comes back as an error, not as empty
rows. yf.download does not raise for a per-symbol failure -- it records it in
yf.shared._ERRORS and hands back a frame without that symbol -- so the two
outcomes are indistinguishable in the frame alone. Conflating them is not a
cosmetic problem: a rate-limited batch then reports every symbol in it as
"has not traded", and the caller records a clean sync that fetched nothing.

Exit codes: 0 when the JSON contract was printed (even if every symbol
failed); 2 for usage errors or missing dependencies.
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings

warnings.simplefilter("ignore")

try:
    import pandas as pd
    import yfinance as yf
except ImportError as e:
    print(f"missing dependency: {e} (pip install --user yfinance pandas)",
          file=sys.stderr)
    sys.exit(2)

FIELDS = ["Open", "High", "Low", "Close", "Volume"]


def frame_for(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Extract one symbol's OHLCV columns from a download of any size.

    A download comes back with a two-level column index whenever group_by is
    set -- for a single symbol as well as for a batch -- and which level holds
    the ticker depends on group_by, which yfinance has changed the default of
    more than once. So both orders are tried rather than assumed, and a
    single-symbol frame is NOT special-cased: assuming it came back flat is
    what silently produced zero rows for every one-symbol batch.
    """
    if not isinstance(df.columns, pd.MultiIndex):
        return df.loc[:, [c for c in FIELDS if c in df.columns]]

    for level in (0, 1):
        if symbol in df.columns.get_level_values(level):
            sub = df.xs(symbol, axis=1, level=level)
            return sub.loc[:, [c for c in FIELDS if c in sub.columns]]

    # The symbol is in neither level: flatten to whichever level names the
    # OHLCV fields, which is the shape a single-symbol download can take.
    for level in (0, 1):
        if set(FIELDS) & set(df.columns.get_level_values(level)):
            sub = df.copy()
            sub.columns = df.columns.get_level_values(level)
            return sub.loc[:, [c for c in FIELDS if c in sub.columns]]
    return pd.DataFrame()


def rows_for(sub: pd.DataFrame) -> list[list]:
    """Normalize one symbol's frame into the wire rows, dropping empty days.

    Rows where the whole bar is missing are yfinance padding a symbol out to
    the union of the batch's trading days -- a symbol that was suspended, or
    that listed midway through the range. They are not data and must not be
    written as zeros.
    """
    if sub.empty:
        return []
    sub = sub.dropna(how="all", subset=[c for c in FIELDS if c in sub.columns])
    if sub.empty:
        return []

    out = sub.reset_index()
    dates = out[out.columns[0]]
    if getattr(dates.dt, "tz", None) is not None:
        dates = dates.dt.tz_localize(None)
    dates = dates.dt.normalize()

    rows = []
    for i in range(len(out)):
        bar = [float(out[f].iloc[i]) if f in out.columns else 0.0
               for f in FIELDS]
        # A bar missing a price is unusable; volume alone is not a session.
        if any(pd.isna(v) for v in bar[:4]):
            continue
        if pd.isna(bar[4]):
            bar[4] = 0.0
        rows.append([dates.iloc[i].strftime("%Y-%m-%d")] + bar)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch one batch of symbols' adjusted OHLCV as JSON.")
    parser.add_argument("tickers", nargs="+")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--end", help="YYYY-MM-DD, exclusive (default: today)")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.tickers if s.strip()]
    if not symbols:
        print(json.dumps({"results": []}))
        return 0

    # Cleared first: _ERRORS is module-level state that survives across
    # calls, and a stale entry would condemn a symbol this batch fetched fine.
    getattr(yf.shared, "_ERRORS", {}).clear()

    try:
        df = yf.download(
            symbols,
            start=args.start, end=args.end,
            auto_adjust=True, actions=False, progress=False,
            group_by="ticker", threads=True,
        )
    except Exception as e:
        # One failed request must not lose the batch: report the failure
        # against every symbol in it, so the caller can retry or record it.
        print(json.dumps({"results": [
            {"symbol": s, "error": f"fetch failed: {e}"} for s in symbols
        ]}))
        return 0

    # Per-symbol failures yfinance swallowed: rate limits, delisted symbols,
    # transient HTTP errors. Reported so the caller can retry them rather than
    # filing them as tickers that simply did not trade.
    errors = dict(getattr(yf.shared, "_ERRORS", {}) or {})

    results = []
    for symbol in symbols:
        rows = []
        if df is not None and not df.empty:
            try:
                rows = rows_for(frame_for(df, symbol))
            except Exception as e:
                results.append({"symbol": symbol, "error": f"parse failed: {e}"})
                continue
        # Rows win over a recorded error: yfinance files some non-fatal
        # complaints against symbols it nonetheless returned data for, and
        # data in hand is not a failed fetch. An error only speaks when there
        # is nothing to show for the symbol.
        if not rows and symbol in errors:
            results.append({
                "symbol": symbol,
                "error": f"download failed: {errors[symbol]}",
            })
            continue
        results.append({"symbol": symbol, "rows": rows})

    print(json.dumps({"results": results}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
