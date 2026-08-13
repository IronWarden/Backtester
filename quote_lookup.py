#!/usr/bin/env python3
"""Live quote & fundamentals lookup for the backtester's chat assistant.

Usage:
    python3 quote_lookup.py AAPL MSFT BRK-B

Prints one JSON object to stdout: {"quotes": [...]} with exactly one entry
per requested symbol, in order. Each entry is either a data dict or
{"symbol": ..., "error": "..."}. Missing fields are omitted, never null.
Read-only — never touches the DuckDB, so it is safe to run while the UI
app holds the database lock.

Exit codes: 0 when the JSON contract was printed (even if every symbol
failed); 2 for usage errors or a missing yfinance install.
"""
from __future__ import annotations

import json
import sys
import warnings

warnings.simplefilter("ignore")

try:
    import yfinance as yf
except ImportError:
    print("yfinance not installed: pip install --user yfinance", file=sys.stderr)
    sys.exit(2)

PROFILE_MAX_CHARS = 300

# fast_info attribute -> output key. Attributes are lazy properties that can
# raise individually, so each is read through _get below.
FAST_INFO_FIELDS = {
    "last_price": "price",
    "previous_close": "previous_close",
    "currency": "currency",
    "market_cap": "market_cap",
    "year_high": "year_high",
    "year_low": "year_low",
    "fifty_day_average": "fifty_day_avg",
    "two_hundred_day_average": "two_hundred_day_avg",
    "last_volume": "last_volume",
}

INFO_FIELDS = {
    "sector": "sector",
    "industry": "industry",
    "trailingPE": "trailing_pe",
    "forwardPE": "forward_pe",
    "trailingEps": "eps_ttm",
    "beta": "beta",
}


def _get(obj, attr):
    try:
        return getattr(obj, attr)
    except Exception:
        return None


def lookup(symbol: str) -> dict:
    out: dict[str, object] = {"symbol": symbol}
    ticker = yf.Ticker(symbol)

    try:
        fi = ticker.fast_info
        for attr, key in FAST_INFO_FIELDS.items():
            val = _get(fi, attr)
            if val is not None:
                out[key] = val
    except Exception as e:
        return {"symbol": symbol, "error": f"quote fetch failed: {e}"}

    if out.get("price") is None:
        return {"symbol": symbol, "error": "no data found (invalid or delisted symbol?)"}

    price, prev = out.get("price"), out.get("previous_close")
    if isinstance(price, (int, float)) and isinstance(prev, (int, float)) and prev:
        out["change_pct"] = round((price - prev) / prev * 100, 2)

    # Fundamentals are best-effort: .info is slow and flaky, and a failure
    # here should not discard the quote half.
    try:
        info = ticker.info or {}
        name = info.get("longName") or info.get("shortName")
        if name:
            out["name"] = name
        for key, out_key in INFO_FIELDS.items():
            val = info.get(key)
            if isinstance(val, (int, float)):
                out[out_key] = round(float(val), 4)
            elif isinstance(val, str) and val:
                out[out_key] = val
        # yfinance >= 0.2.38 reports dividendYield already in percent
        # (e.g. 0.45 for 0.45%); older releases reported a fraction.
        # Values under 0.005 are treated as fractions and scaled.
        dy = info.get("dividendYield")
        if isinstance(dy, (int, float)):
            out["dividend_yield"] = round(dy * 100 if dy < 0.005 else dy, 2)
        profile = info.get("longBusinessSummary")
        if profile:
            if len(profile) > PROFILE_MAX_CHARS:
                profile = profile[: PROFILE_MAX_CHARS - 1].rstrip() + "…"
            out["profile"] = profile
    except Exception:
        out["note"] = "fundamentals unavailable; quote fields only"

    return out


def main() -> int:
    symbols = [s.strip().upper() for s in sys.argv[1:] if s.strip()]
    if not symbols:
        print("usage: quote_lookup.py SYMBOL [SYMBOL ...]", file=sys.stderr)
        return 2
    print(json.dumps({"quotes": [lookup(s) for s in symbols]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
