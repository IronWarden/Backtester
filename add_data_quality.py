#!/usr/bin/env python3
"""Audit every price series in stock_data.db and write the `data_quality` table.

"The best possible data set" cannot be claimed without a defect inventory.
Survivorship was audited (analysis/survivorship.md) and symbol recycling was
audited (add_delistings.py); the bars themselves never were, and the first pass
found this, over 34,988,956 bars across 10,434 tickers:

  * FOUR TICKERS HAVE NEGATIVE CLOSES -- VHI (5,429 bars, to -639.30),
    SVA (5,478, to -78.46), SAFE (1,024, to -28.79) and DEC (691). Yahoo's
    back-adjustment for a large special distribution can drive an adjusted
    close below zero, and every return computed from such a bar is
    meaningless. 11,931 bars in the table are priced below zero.
  * 8,737 bars have High < Low, and 9,439 have a Close outside [Low, High].
  * HUBB's 1972-1984 history is a flat synthetic series: Open = High = Low =
    Close, zero volume, and 1,862 CONSECUTIVE identical closes.
  * EMP has 526 daily moves over 50%, the largest +10,297%.

Verdicts on the first pass: 7,193 good (68.9%), 1,915 usable (18.4%),
1,053 suspect (10.1%), 273 unusable (2.6%).

Everything here is read-only SELECTs except the final table write. The audit is
one query over all 35M bars and takes a few seconds -- there is no sampling and
no approximation, so every number can be re-derived exactly.

Usage:
    python3 add_data_quality.py --dry-run    # audit and report, write nothing
    python3 add_data_quality.py              # audit and replace the table
Requires the UI app closed for a real write (DuckDB is single-writer).

The verdict thresholds mirror src/data/data_quality.go, which is the authority
-- the Go side recomputes the verdict from the stored statistics, so a threshold
change there takes effect without re-running this. They are judgement calls, and
the point of storing the evidence rather than only the verdict is that they can
be argued with later.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = os.environ.get("BACKTESTER_DB", str(ROOT / "stock_data.db"))
TABLE = "data_quality"
PRICES = "stock_data_optimized"

# Mirror of the constants in src/data/data_quality.go. Keep the two in step.
MIN_BARS = 20
IMPOSSIBLE_SHARE_UNUSABLE = 0.01
FLAT_RUN_SUSPECT = 60
FLAT_RUN_USABLE = 20
MISSING_SHARE_SUSPECT = 0.05
MISSING_SHARE_USABLE = 0.01
BIG_JUMP_RETURN = 0.50
BIG_JUMPS_SUSPECT = 5
BIG_JUMPS_USABLE = 1
MAX_JUMP_UNUSABLE = 5.0
ZERO_VOLUME_USABLE = 0.50

# The market calendar. The $-prefixed benchmark series are built on the NYSE
# calendar by add_collections.py, so $SP500's dates ARE that calendar for every
# purpose here -- and using data already in the table keeps the audit free of a
# dependency on a calendar library.
CALENDAR_TICKER = "$SP500"

AUDIT_SQL = f"""
WITH cal AS (
    SELECT DISTINCT "Date" AS d FROM "{PRICES}" WHERE "Ticker" = '{CALENDAR_TICKER}'
),
basics AS (
    SELECT "Ticker" AS ticker,
           COUNT(*)                                        AS bars,
           COUNT(*) - COUNT(DISTINCT "Date")               AS duplicate_bars,
           SUM(CASE WHEN "Close" <= 0 THEN 1 ELSE 0 END)   AS non_positive_closes,
           SUM(CASE WHEN "High" < "Low"
                      OR "Close" > "High" + 1e-9
                      OR "Close" < "Low" - 1e-9
                    THEN 1 ELSE 0 END)                     AS impossible_bars,
           SUM(CASE WHEN "Volume" = 0 THEN 1 ELSE 0 END)   AS zero_volume_bars,
           MIN("Date")                                     AS first_bar,
           MAX("Date")                                     AS last_bar,
           COUNT(DISTINCT "Date")                          AS distinct_days
    FROM "{PRICES}"
    GROUP BY 1
),
-- Gaps are counted against the calendar only INSIDE each series' own span: a
-- series that starts in 2015 is not missing 1990.
expected AS (
    SELECT b.ticker, COUNT(*) AS expected_days
    FROM basics b JOIN cal ON cal.d BETWEEN b.first_bar AND b.last_bar
    GROUP BY 1
),
returns AS (
    SELECT "Ticker" AS ticker, "Close" AS c,
           LAG("Close") OVER (PARTITION BY "Ticker" ORDER BY "Date") AS prev
    FROM "{PRICES}"
),
jumps AS (
    SELECT ticker,
           SUM(CASE WHEN ABS(c / prev - 1) > {BIG_JUMP_RETURN}
                    THEN 1 ELSE 0 END)   AS big_jumps,
           MAX(ABS(c / prev - 1))        AS max_jump
    FROM returns
    WHERE prev IS NOT NULL AND prev > 0
    GROUP BY 1
),
-- Gaps-and-islands: consecutive rows sharing a close land in the same group,
-- so the group sizes are the lengths of the flat runs.
islands AS (
    SELECT "Ticker" AS ticker, "Close" AS c,
           ROW_NUMBER() OVER (PARTITION BY "Ticker" ORDER BY "Date")
             - ROW_NUMBER() OVER (PARTITION BY "Ticker", "Close" ORDER BY "Date")
             AS grp
    FROM "{PRICES}"
),
flat AS (
    SELECT ticker, MAX(run) AS longest_flat_run
    FROM (SELECT ticker, c, grp, COUNT(*) AS run FROM islands GROUP BY 1, 2, 3)
    GROUP BY 1
)
SELECT b.ticker, b.bars, b.duplicate_bars, b.non_positive_closes,
       b.impossible_bars, b.zero_volume_bars,
       COALESCE(f.longest_flat_run, 0)                    AS longest_flat_run,
       COALESCE(j.big_jumps, 0)                           AS big_jumps,
       COALESCE(j.max_jump, 0.0)                          AS max_jump,
       GREATEST(COALESCE(e.expected_days, b.distinct_days) - b.distinct_days, 0)
                                                          AS missing_days,
       COALESCE(e.expected_days, b.distinct_days)         AS expected_days,
       b.first_bar, b.last_bar
FROM basics b
LEFT JOIN expected e USING (ticker)
LEFT JOIN jumps    j USING (ticker)
LEFT JOIN flat     f USING (ticker)
ORDER BY b.ticker
"""


def classify(row: Any) -> str:
    """The verdict, worst finding first. Mirror of ClassifyQuality in Go."""
    bars = int(row["bars"])
    impossible_share = row["impossible_bars"] / bars if bars else 0.0
    missing_share = (row["missing_days"] / row["expected_days"]
                     if row["expected_days"] else 0.0)
    zero_vol_share = row["zero_volume_bars"] / bars if bars else 0.0

    if bars < MIN_BARS:
        return "unusable"
    if row["non_positive_closes"] > 0:
        return "unusable"
    if impossible_share > IMPOSSIBLE_SHARE_UNUSABLE:
        return "unusable"
    if row["max_jump"] > MAX_JUMP_UNUSABLE:
        return "unusable"

    if row["impossible_bars"] > 0:
        return "suspect"
    if row["longest_flat_run"] >= FLAT_RUN_SUSPECT:
        return "suspect"
    if missing_share > MISSING_SHARE_SUSPECT:
        return "suspect"
    if row["big_jumps"] >= BIG_JUMPS_SUSPECT:
        return "suspect"
    if row["duplicate_bars"] > 0:
        return "suspect"

    if (row["longest_flat_run"] >= FLAT_RUN_USABLE
            or missing_share > MISSING_SHARE_USABLE
            or row["big_jumps"] >= BIG_JUMPS_USABLE
            or zero_vol_share > ZERO_VOLUME_USABLE):
        return "usable"
    return "good"


def report(df: pd.DataFrame) -> None:
    """Print the inventory: how bad, how widespread, and the worst offenders."""
    total = len(df)
    print(f"audited {total} tickers, {int(df['bars'].sum()):,} bars\n")

    print("verdicts:")
    for verdict in ("good", "usable", "suspect", "unusable"):
        n = int((df["verdict"] == verdict).sum())
        print(f"  {verdict:9} {n:6}  ({n / total * 100:5.1f}%)")

    print("\nfindings, by how many tickers each affects:")
    checks = [
        ("negative or zero closes", df["non_positive_closes"] > 0),
        ("impossible bars (H<L, C outside [L,H])", df["impossible_bars"] > 0),
        ("duplicate (ticker, date) rows", df["duplicate_bars"] > 0),
        (f"flat run >= {FLAT_RUN_SUSPECT} bars", df["longest_flat_run"] >= FLAT_RUN_SUSPECT),
        (f"missing > {MISSING_SHARE_SUSPECT:.0%} of its calendar",
         df["missing_days"] / df["expected_days"].clip(lower=1) > MISSING_SHARE_SUSPECT),
        (f">= {BIG_JUMPS_SUSPECT} moves over {BIG_JUMP_RETURN:.0%}",
         df["big_jumps"] >= BIG_JUMPS_SUSPECT),
        (f"a single move over {MAX_JUMP_UNUSABLE:.0%}", df["max_jump"] > MAX_JUMP_UNUSABLE),
        ("majority of bars have zero volume",
         df["zero_volume_bars"] / df["bars"].clip(lower=1) > ZERO_VOLUME_USABLE),
        (f"fewer than {MIN_BARS} bars", df["bars"] < MIN_BARS),
    ]
    for label, mask in checks:
        n = int(mask.sum())
        print(f"  {n:6}  {label}")

    def worst(label: str, column: str, n: int = 10, fmt: str = "{:,.0f}") -> None:
        top = df.nlargest(n, column)
        if top[column].max() <= 0:
            print(f"\n{label}: none")
            return
        print(f"\n{label}:")
        for _, r in top.iterrows():
            if r[column] <= 0:
                continue
            print(f"  {r['ticker']:8} {fmt.format(r[column]):>14}"
                  f"   {str(r['first_bar'])[:10]} -> {str(r['last_bar'])[:10]}"
                  f"   {r['verdict']}")

    worst("worst negative-close series", "non_positive_closes")
    worst("worst impossible-bar series", "impossible_bars")
    worst("longest flat runs (a stalled feed looks like a flat stock)",
          "longest_flat_run")
    worst("most days missing from its own calendar span", "missing_days")
    worst("most one-day moves over 50%", "big_jumps")
    worst("largest single-day move", "max_jump", fmt="{:,.1f}x")

    dollar = df[df["ticker"].str.startswith("$")]
    if not dollar.empty:
        print("\nthe $-prefixed benchmark series specifically "
              "(these are the survivorship-free ones, so their quality "
              "matters more than any single stock's):")
        for _, r in dollar.iterrows():
            print(f"  {r['ticker']:8} {r['verdict']:9} bars={int(r['bars']):6} "
                  f"missing={int(r['missing_days']):5} "
                  f"flat_run={int(r['longest_flat_run']):5} "
                  f"jumps={int(r['big_jumps']):4}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--dry-run", action="store_true",
                    help="audit and report, write nothing")
    ap.add_argument("--csv", default=None,
                    help="also write the per-ticker rows to this CSV")
    args = ap.parse_args()

    try:
        con = duckdb.connect(args.db, read_only=args.dry_run)
    except duckdb.IOException as e:
        print(f"Could not open {args.db}: {e}\n"
              "If a lock is held, close the UI app (DuckDB is single-writer).",
              file=sys.stderr)
        return 1

    try:
        cal = con.execute(
            f'SELECT COUNT(DISTINCT "Date") FROM "{PRICES}" '
            f"WHERE \"Ticker\" = '{CALENDAR_TICKER}'").fetchone()
        if not cal or not cal[0]:
            print(f"No {CALENDAR_TICKER} series, so there is no market calendar "
                  "to count missing days against. Load the benchmark "
                  "collections first (add_collections.py).", file=sys.stderr)
            return 1
        print(f"market calendar: {cal[0]} trading days from {CALENDAR_TICKER}")

        df = con.execute(AUDIT_SQL).df()
        df["verdict"] = df.apply(classify, axis=1)
        report(df)

        if args.csv:
            df.to_csv(args.csv, index=False)
            print(f"\nper-ticker rows written to {args.csv}")

        if args.dry_run:
            print("\n(dry run -- nothing written)")
            return 0

        con.register("incoming", df)
        con.execute("BEGIN")
        # Replaced wholesale: the audit is a full recomputation every time, and
        # a stale row is worse than no row.
        con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
        con.execute(f"""
            CREATE TABLE "{TABLE}" (
                ticker              VARCHAR NOT NULL,
                bars                BIGINT,
                duplicate_bars      BIGINT,
                non_positive_closes BIGINT,
                impossible_bars     BIGINT,
                zero_volume_bars    BIGINT,
                longest_flat_run    BIGINT,
                big_jumps           BIGINT,
                max_jump            DOUBLE,
                missing_days        BIGINT,
                expected_days       BIGINT,
                first_bar           TIMESTAMP_NS,
                last_bar            TIMESTAMP_NS,
                verdict             VARCHAR,
                audited_at          TIMESTAMP_NS
            )
        """)
        con.execute(f"""
            INSERT INTO "{TABLE}"
            SELECT ticker, bars, duplicate_bars, non_positive_closes,
                   impossible_bars, zero_volume_bars, longest_flat_run,
                   big_jumps, max_jump, missing_days, expected_days,
                   CAST(first_bar AS TIMESTAMP_NS), CAST(last_bar AS TIMESTAMP_NS),
                   verdict, CAST(NOW() AS TIMESTAMP_NS)
            FROM incoming
        """)
        con.execute("COMMIT")
        con.unregister("incoming")

        written = con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()
        print(f"\n{TABLE} written: {written[0] if written else 0} rows")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
