#!/usr/bin/env python3
"""Fetch daily OHLCV history from yfinance and print it as JSON.

Usage:
    python3 fetch_history.py TSLA NVDA                # full available history
    python3 fetch_history.py SPY --start 2024-01-01 --end 2024-12-31

Companion to the UI's fetch_history chat tool: unlike add_ticker.py this
script never opens the DuckDB (which the running UI holds a write lock on)
— it only downloads and normalizes, printing one JSON object to stdout.
The Go side performs the actual upsert through its own database handle.

Output: {"results": [...]} with one entry per symbol, in order; each entry
is {"symbol", "rows": [["YYYY-MM-DD", open, high, low, close, volume], ...]}
or {"symbol", "error": "..."}. Prices are split/dividend-adjusted, matching
stock_data_optimized's conventions.

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
    from add_ticker import fetch
except ImportError as e:
    print(f"missing dependency: {e} (pip install --user yfinance pandas)",
          file=sys.stderr)
    sys.exit(2)


def history(symbol: str, start: str | None, end: str | None) -> dict:
    full = start is None
    try:
        df = fetch(symbol, start, end, adjusted=True, full=full)
    except Exception as e:
        return {"symbol": symbol, "error": f"fetch failed: {e}"}
    if df.empty:
        return {"symbol": symbol, "error": "no data from yfinance "
                "(invalid or delisted symbol, or empty range?)"}
    rows = [
        [d.strftime("%Y-%m-%d"), o, h, l, c, v]
        for d, o, h, l, c, v in zip(
            df["Date"], df["Open"], df["High"], df["Low"],
            df["Close"], df["Volume"],
        )
    ]
    return {"symbol": symbol, "rows": rows}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch adjusted OHLCV history from yfinance as JSON.")
    parser.add_argument("tickers", nargs="+")
    parser.add_argument("--start", help="YYYY-MM-DD (omit for full history)")
    parser.add_argument("--end", help="YYYY-MM-DD, exclusive (default: today)")
    args = parser.parse_args()

    results = [
        history(sym.strip().upper(), args.start, args.end)
        for sym in args.tickers if sym.strip()
    ]
    print(json.dumps({"results": results}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
