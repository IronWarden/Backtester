#!/usr/bin/env python3
"""Where every table in stock_data.db came from, and when.

The database is assembled from sources with wildly different reliability,
licensing and staleness -- Yahoo for prices, Alpha Vantage for delistings, SEC
for fundamentals and industry codes, Wikipedia for index membership, FRED for
yields -- and until this existed nothing recorded which was which. That makes
the result unauditable ("is this figure Yahoo's or SEC's?"), un-reproducible
("which snapshot produced this backtest?") and impossible to correct selectively
when one source turns out to be wrong, which analysis/free_data_sources.md shows
is the normal case rather than the exception.

Every loader calls `record()` as its final step. One row per (table, loader) --
`stock_data_optimized` genuinely has two contributors, add_ticker.py for real
companies and add_collections.py for the $-prefixed benchmark series, and both
deserve a row -- replaced on each load, in a `data_sources` table:

    table_name, source, endpoint, licence, row_count, loader, loader_git_sha,
    fetched_at, note

Two rules, both load-bearing:

  * **Recording provenance must never fail a load.** Every failure here is
    swallowed and reported, because a missing provenance row is a documentation
    gap and a failed load is lost work.
  * **It cannot be done retroactively.** Once a loader has run without it, the
    fetch date and the code version that produced the rows are gone. Hence the
    swallowing above: there is no reason for any loader to skip the call.

Usage, from a loader that has just written its table:

    import loader_provenance
    loader_provenance.record(
        con, table="delistings", source="Alpha Vantage LISTING_STATUS",
        endpoint=ENDPOINT_WITHOUT_THE_KEY, licence="free tier, personal use",
        rows=written, note="coverage effectively begins 2013")

Read it from Go with src/data.LoadDataSources.
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

TABLE = "data_sources"
ROOT = Path(__file__).resolve().parent

_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS "{TABLE}" (
    table_name     VARCHAR NOT NULL,
    source         VARCHAR NOT NULL,
    endpoint       VARCHAR,
    licence        VARCHAR,
    row_count      BIGINT,
    loader         VARCHAR,
    loader_git_sha VARCHAR,
    fetched_at     TIMESTAMP_NS NOT NULL,
    note           VARCHAR
)
"""


def git_sha() -> str:
    """The engine version that produced the rows, with -dirty when the tree has
    uncommitted changes -- which matters, since a loader run from a modified
    tree is not reproducible from the sha alone."""
    def run(*args: str) -> str:
        return subprocess.run(args, cwd=ROOT, capture_output=True, text=True,
                              timeout=10, check=True).stdout.strip()
    try:
        sha = run("git", "rev-parse", "--short", "HEAD")
    except Exception:
        return "unknown"
    try:
        if run("git", "status", "--porcelain"):
            return f"{sha}-dirty"
    except Exception:
        pass
    return sha


def record(
    con: Any,
    *,
    table: str,
    source: str,
    endpoint: str = "",
    licence: str = "",
    rows: int | None = None,
    loader: str | None = None,
    note: str = "",
) -> None:
    """Record where `table` came from. Never raises."""
    try:
        con.execute(_SCHEMA)
        # Keyed by (table, loader): two loaders legitimately contribute to one
        # table, and keying on the table alone would have the second erase the
        # first's provenance.
        con.execute(f'DELETE FROM "{TABLE}" WHERE table_name = ? AND loader = ?',
                    [table, loader or Path(sys.argv[0]).name])
        con.execute(
            f'INSERT INTO "{TABLE}" (table_name, source, endpoint, licence, '
            "row_count, loader, loader_git_sha, fetched_at, note) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [table, source, endpoint, licence, rows,
             loader or Path(sys.argv[0]).name, git_sha(),
             datetime.now(timezone.utc).replace(tzinfo=None), note],
        )
    except Exception as e:  # provenance is documentation; never fail a load
        print(f"!! could not record provenance for {table}: {e}",
              file=sys.stderr)


def show(con: Any) -> None:
    """Print the provenance table, for a --provenance flag."""
    try:
        rows = con.execute(
            f'SELECT table_name, source, row_count, loader_git_sha, fetched_at '
            f'FROM "{TABLE}" ORDER BY table_name, loader').fetchall()
    except Exception:
        print("no data_sources table yet: no loader has recorded provenance")
        return
    if not rows:
        print("data_sources is empty")
        return
    print(f"{'table':22} {'source':38} {'rows':>10}  sha        fetched")
    for name, source, count, sha, fetched in rows:
        print(f"{name:22} {str(source)[:38]:38} "
              f"{(count if count is not None else 0):10,}  {str(sha):10} "
              f"{str(fetched)[:19]}")
