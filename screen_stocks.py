#!/usr/bin/env python3
"""Screen the US stock market by fundamentals via Yahoo Finance.

Usage:
    python3 screen_stocks.py --pe-max 20 --cap-min 10000000000
    python3 screen_stocks.py --sector Technology --div-yield-min 2 --limit 50

Companion to the UI's screen_stocks chat tool. Builds a Yahoo screener
query (yfinance EquityQuery) over US equities and prints one JSON object
to stdout: {"total": N, "quotes": [{symbol, name, pe, market_cap, ...}]}.
Missing fields are omitted, never null. At least one filter is required.

Exit codes: 0 when the JSON contract was printed; 2 for usage errors or
missing dependencies.
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings

warnings.simplefilter("ignore")

try:
    import yfinance as yf
except ImportError:
    print("yfinance not installed: pip install --user yfinance", file=sys.stderr)
    sys.exit(2)

# Screener quote key -> output key. Values are copied via .get() and
# omitted when absent (ETFs/holdcos lack many of these).
QUOTE_FIELDS = {
    "symbol": "symbol",
    "trailingPE": "pe",
    "forwardPE": "forward_pe",
    "marketCap": "market_cap",
    "regularMarketPrice": "price",
    "dividendYield": "dividend_yield",
}

SORT_FIELDS = {"cap": "intradaymarketcap", "pe": "peratio.lasttwelvemonths"}


def _q(op: str, operand: object) -> "yf.EquityQuery":
    # yfinance's EquityQuery type hints demand tuples but the runtime
    # requires lists; this untyped funnel keeps type checkers quiet.
    return yf.EquityQuery(op, operand)  # type: ignore[arg-type]


def build_query(args: argparse.Namespace) -> "yf.EquityQuery":
    clauses = [_q("eq", ["region", "us"])]
    if args.pe_max is not None:
        clauses.append(_q("lt", ["peratio.lasttwelvemonths", args.pe_max]))
    if args.pe_min is not None:
        clauses.append(_q("gt", ["peratio.lasttwelvemonths", args.pe_min]))
    if args.cap_min is not None:
        clauses.append(_q("gt", ["intradaymarketcap", args.cap_min]))
    if args.div_yield_min is not None:
        clauses.append(_q("gt", ["forward_dividend_yield", args.div_yield_min]))
    if args.sector:
        clauses.append(_q("eq", ["sector", args.sector]))
    return _q("and", clauses)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Screen US stocks by fundamentals via Yahoo Finance.")
    parser.add_argument("--pe-max", type=float)
    parser.add_argument("--pe-min", type=float)
    parser.add_argument("--cap-min", type=float, help="Market cap floor, USD")
    parser.add_argument("--div-yield-min", type=float, help="Percent")
    parser.add_argument("--sector")
    parser.add_argument("--sort", choices=sorted(SORT_FIELDS), default="cap")
    parser.add_argument("--limit", type=int, default=25)
    args = parser.parse_args()

    if all(v is None for v in
           (args.pe_max, args.pe_min, args.cap_min, args.div_yield_min)) \
            and not args.sector:
        print("at least one filter is required", file=sys.stderr)
        return 2
    limit = max(1, min(args.limit, 100))

    result = yf.screen(
        build_query(args),
        size=limit,
        sortField=SORT_FIELDS[args.sort],
        # Market cap reads best largest-first; a PE sort means "cheapest first".
        sortAsc=(args.sort == "pe"),
    )

    quotes = []
    for item in result.get("quotes", []):
        out: dict[str, object] = {}
        for src, dst in QUOTE_FIELDS.items():
            val = item.get(src)
            if isinstance(val, float):
                val = round(val, 4)
            if val is not None:
                out[dst] = val
        name = item.get("longName") or item.get("shortName")
        if name:
            out["name"] = name
        sector = item.get("sector")
        if sector:
            out["sector"] = sector
        quotes.append(out)

    print(json.dumps({"total": result.get("total", len(quotes)),
                      "quotes": quotes}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
