#!/usr/bin/env python3
"""Read a FRED series without an API key.

FRED serves every public series as a plain CSV at fredgraph.csv, the same file
behind the "Download" button on each series page. No key, no signup -- which is
what lets a new user build a database without first registering with the St.
Louis Fed. The keyed API (`fredapi`) is still used where a key is configured;
this is the fallback, and the only path build_db.py takes.

    from fred_public import fred_series
    s = fred_series("DGS3MO")   # pd.Series of floats indexed by date
"""
from __future__ import annotations

import io
import time
import urllib.error
import urllib.request

import pandas as pd

CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"
USER_AGENT = "Mozilla/5.0 (Backtester build_db)"


def parse_fred_csv(text: str) -> pd.Series:
    """The CSV's two columns -> a float series by date, gaps dropped.

    FRED marks a missing observation with "." (a market holiday on a daily
    series), so values are coerced and the blanks dropped rather than trusted
    to parse. The header names vary ("DATE" in older files,
    "observation_date" now), so columns are taken by position.
    """
    df = pd.read_csv(io.StringIO(text))
    if df.shape[1] < 2:
        raise ValueError("FRED CSV has fewer than two columns")
    out = pd.DataFrame({
        "date": pd.to_datetime(df.iloc[:, 0], errors="coerce"),
        "value": pd.to_numeric(df.iloc[:, 1], errors="coerce"),
    }).dropna()
    return pd.Series(out["value"].to_numpy(dtype=float),
                     index=pd.DatetimeIndex(out["date"]).normalize())


def fred_series(series_id: str, attempts: int = 4) -> pd.Series:
    req = urllib.request.Request(CSV_URL.format(series=series_id),
                                 headers={"User-Agent": USER_AGENT})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return parse_fred_csv(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError):
            if attempt == attempts - 1:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError(f"gave up on FRED {series_id}")
