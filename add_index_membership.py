#!/usr/bin/env python3
"""Reconstruct point-in-time S&P 500 membership into `index_membership`.

The larger half of survivorship bias is SELECTION, not missing prices: the
default config backtests AAPL/MSFT/GOOGL/AMZN/NVDA over 2015-2025, a basket
chosen with knowledge of who won. Knowing who was in the index ON A GIVEN DATE
fixes that half, and it does not need the dead names' prices to be useful.

Source, free: two Wikipedia tables.

  * "List of S&P 500 companies" -- the 503 current constituents, each with a
    Date added, a CIK, and (usefully) a real GICS sector.
  * "Historical components of the S&P 500" -- 408 dated add/remove events,
    1976-07-01 to 2026-08-18, with the reason.

Membership is reconstructed BACKWARDS: start from today's members and undo each
change in reverse chronological order. A ticker that was added on date D was not
a member the day before; a ticker that was removed on D was one.

HOW FAR BACK THIS IS TRUSTWORTHY, which is the whole caveat. The S&P 500 changes
roughly 20-25 constituents a year. Measured against the table on 2026-08-17:

    2011-2026   16-30 changes/year   dense; consistent with reality
    2007-2010    8-13 changes/year   partial
    1976-2006    0-7  changes/year   effectively absent

So the reconstruction is good back to about 2011, approximate to 2007, and
fiction before that -- a ticker whose real removal in 2003 is not in the table
stays "a member" all the way back to the horizon. Every row therefore carries a
`confidence`, derived from how many changes the source records for that year, and
the report prints the member count per year so the drift is visible rather than
implied: a reconstructed year that does not hold ~500 members is a year whose
changes are missing.

Usage:
    python3 add_index_membership.py --dry-run          # reconstruct and report
    python3 add_index_membership.py                    # replace the table
    python3 add_index_membership.py --from-year 2011   # only the dense era
Requires the UI app closed for a real write (DuckDB is single-writer).

This is a scrape of a community-maintained page, not a vendor feed. Every row
records `source`, and nothing here should ever be presented as authoritative
index data.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import urllib.request
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

import loader_provenance

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = os.environ.get("BACKTESTER_DB", str(ROOT / "stock_data.db"))
TABLE = "index_membership"
INDEX_NAME = "SP500"

CONTACT = os.environ.get("SEC_CONTACT", "gadhiarishi@gmail.com")
USER_AGENT = f"Backtester research {CONTACT}"
API = ("https://en.wikipedia.org/w/api.php?action=parse&page={page}"
       "&prop=text&format=json&formatversion=2")
CURRENT_PAGE = "List_of_S%26P_500_companies"
CHANGES_PAGE = "Historical_components_of_the_S%26P_500"
SOURCE = "wikipedia:List_of_S&P_500_companies+Historical_components"

# How many recorded changes a year needs before its reconstruction is worth
# trusting. The index really changes 20-25 times a year, so a year with 15+ is
# plausibly complete and a year with under 5 is a year of missing history.
CONFIDENCE_HIGH = 15
CONFIDENCE_MEDIUM = 5

# Every span open at the present is stored with this end date rather than NULL,
# so a BETWEEN query needs no special case for current members.
OPEN_END = date(9999, 12, 31)


def wiki_tables(page: str) -> list[pd.DataFrame]:
    """Every table on a Wikipedia page, via the parse API."""
    req = urllib.request.Request(API.format(page=page),
                                 headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        doc = json.loads(resp.read())
    return pd.read_html(io.StringIO(doc["parse"]["text"]))


def parse_day(value: Any) -> date | None:
    """Wikipedia writes 'August 18, 2026'; pandas handles the rest."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        parsed = pd.to_datetime(str(value).strip(), errors="coerce",
                                format="mixed")
    except (ValueError, TypeError):
        return None
    if pd.isna(parsed):
        return None
    return parsed.date()


def fetch_current() -> pd.DataFrame:
    """Today's constituents: symbol, security, sector, CIK, date added."""
    raw = wiki_tables(CURRENT_PAGE)[0]
    df = pd.DataFrame({
        "ticker": raw["Symbol"].astype(str).str.strip().str.upper(),
        "security": raw["Security"].astype(str).str.strip(),
        "gics_sector": raw.get("GICS Sector", pd.Series(dtype=str)),
        "cik": raw.get("CIK", pd.Series(dtype=str)),
        "date_added": raw["Date added"].map(parse_day),
    })
    return df[df["ticker"] != ""].reset_index(drop=True)


def fetch_changes() -> pd.DataFrame:
    """Dated add/remove events, newest first.

    Columns are read by POSITION, not by name: the table's header is two levels
    ("Added Ticker", "Removed Ticker") and both collapse to "Ticker", so
    selecting by name returns two columns and silently breaks everything
    downstream.
    """
    raw = wiki_tables(CHANGES_PAGE)[0]
    date_col, added_col, removed_col, reason_col = 0, 1, 3, 5

    out = pd.DataFrame({
        "day": [parse_day(v) for v in raw.iloc[:, date_col]],
        "added": [str(v).strip().upper() for v in raw.iloc[:, added_col]],
        "removed": [str(v).strip().upper() for v in raw.iloc[:, removed_col]],
        "reason": [str(v).strip() for v in raw.iloc[:, reason_col]],
    })
    blank = ("", "NAN", "NONE", "\u2014", "-")
    out["added"] = [t if t not in blank else "" for t in out["added"]]
    out["removed"] = [t if t not in blank else "" for t in out["removed"]]
    out = out[[d is not None for d in out["day"]]]
    return out.sort_values("day", ascending=False).reset_index(drop=True)


def confidence_by_year(changes: pd.DataFrame) -> dict[int, str]:
    """How much to trust each year, from how many changes the source records."""
    counts = pd.Series([d.year for d in changes["day"]]).value_counts()
    out: dict[int, str] = {}
    for year in range(1970, date.today().year + 1):
        n = int(counts.get(year, 0))
        if n >= CONFIDENCE_HIGH:
            out[year] = "high"
        elif n >= CONFIDENCE_MEDIUM:
            out[year] = "medium"
        else:
            out[year] = "low"
    return out


def reconstruct(current: pd.DataFrame, changes: pd.DataFrame,
                horizon: date) -> list[dict[str, Any]]:
    """Membership spans, built by undoing each change from today backwards.

    A ticker added on D was not a member on D-1, so undoing an addition closes
    that ticker's span at D. A ticker removed on D was a member up to D, so
    undoing a removal opens a span ending at D. A ticker can do both more than
    once -- rejoining the index is common -- which is why spans are a list per
    ticker rather than one interval.
    """
    spans: list[dict[str, Any]] = []
    # ticker -> the end date of the span currently open going backwards.
    open_end: dict[str, date] = {t: OPEN_END for t in current["ticker"]}
    security = dict(zip(current["ticker"], current["security"]))

    for _, ch in changes.iterrows():
        day: date = ch["day"]
        if day < horizon:
            break

        added, removed = ch["added"], ch["removed"]
        if added and added in open_end:
            spans.append({"ticker": added, "start_date": day,
                          "end_date": open_end.pop(added)})
        if removed:
            # Already open? Then the source has two removals with no addition
            # between them, which is a gap in the table rather than a real
            # event. Keep the earlier (outer) span and do not nest.
            if removed not in open_end:
                open_end[removed] = day
                security.setdefault(removed, "")

    # Whatever is still open at the horizon has been a member since at least
    # then. Current constituents have a real Date added on the source page; use
    # it when it is earlier than the horizon, since it is better evidence than
    # "at least since".
    added_on = dict(zip(current["ticker"], current["date_added"]))
    for ticker, end in open_end.items():
        start = added_on.get(ticker)
        spans.append({
            "ticker": ticker,
            "start_date": start if start and start <= horizon else horizon,
            "end_date": end,
            # A span we could not find a beginning for is marked, so a caller
            # can tell "joined here" from "was already there".
            "start_is_horizon": not (start and start <= horizon),
        })

    for s in spans:
        s.setdefault("start_is_horizon", False)
        s["security"] = security.get(s["ticker"], "")
    return spans


def members_on(spans: list[dict[str, Any]], when: date) -> set[str]:
    """The membership set on one day. Mirror of MembersOn in Go."""
    return {s["ticker"] for s in spans
            if s["start_date"] <= when <= s["end_date"]}


def report(spans: list[dict[str, Any]], changes: pd.DataFrame,
           conf: dict[int, str], horizon: date) -> None:
    print(f"\n{len(spans)} membership spans for "
          f"{len({s['ticker'] for s in spans})} distinct tickers")

    print("\nmember count at each year end (should be ~500; a year that is far "
          "off is a year whose changes the source is missing):")
    for year in range(horizon.year, date.today().year + 1):
        when = min(date(year, 12, 31), date.today())
        n = len(members_on(spans, when))
        flag = "" if 495 <= n <= 510 else "   <-- off"
        print(f"  {year}  {n:4} members   confidence {conf.get(year, 'low'):6}"
              f"{flag}")

    print("\nrecorded changes per year:")
    counts = pd.Series([d.year for d in changes["day"]]).value_counts()
    for year in sorted(y for y in counts.index if y >= horizon.year):
        print(f"  {year}  {int(counts[year]):3}")

    print("\nspot checks -- companies whose exit is public knowledge:")
    for ticker, gone_by, what in (
        ("ATVI", date(2024, 1, 1), "Activision, acquired by Microsoft 2023-10"),
        ("SIVB", date(2023, 6, 1), "SVB Financial, failed 2023-03"),
        ("BBBY", date(2023, 6, 1), "Bed Bath & Beyond, dropped on market cap 2017-07-26, years before its 2023 bankruptcy"),
        ("TWTR", date(2023, 6, 1), "Twitter, taken private 2022-10"),
    ):
        held = [s for s in spans if s["ticker"] == ticker]
        if not held:
            print(f"  {ticker:6} not in the reconstruction "
                  f"(its exit predates the dense era) -- {what}")
            continue
        still_in = ticker in members_on(spans, gone_by)
        mark = "!! " if still_in else "ok "
        print(f"  {mark}{ticker:6} member on {gone_by}: {still_in}  "
              f"spans={[(str(s['start_date']), str(s['end_date'])) for s in held]}"
              f"  -- {what}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--dry-run", action="store_true",
                    help="reconstruct and report, write nothing")
    ap.add_argument("--from-year", type=int, default=2007,
                    help="reconstruction horizon; before 2007 the source has "
                         "too few changes for the result to mean anything")
    args = ap.parse_args()

    horizon = date(args.from_year, 1, 1)
    try:
        current = fetch_current()
        changes = fetch_changes()
    except Exception as e:
        print(f"Could not fetch the Wikipedia tables: {e}", file=sys.stderr)
        return 1
    print(f"{len(current)} current constituents, {len(changes)} recorded changes "
          f"({changes['day'].min()} .. {changes['day'].max()})")

    conf = confidence_by_year(changes)
    spans = reconstruct(current, changes, horizon)
    report(spans, changes, conf, horizon)

    rows = pd.DataFrame([{
        "index_name": INDEX_NAME,
        "ticker": s["ticker"],
        "security": s["security"],
        "start_date": s["start_date"],
        "end_date": s["end_date"],
        "start_is_horizon": s["start_is_horizon"],
        "confidence": conf.get(s["start_date"].year, "low"),
        "source": SOURCE,
        "fetched_at": datetime.utcnow(),
    } for s in spans])

    if args.dry_run:
        print("\n(dry run -- nothing written)")
        return 0

    try:
        con = duckdb.connect(args.db)
    except duckdb.IOException as e:
        print(f"Could not open {args.db}: {e}\n"
              "If a lock is held, close the UI app (DuckDB is single-writer).",
              file=sys.stderr)
        return 1
    try:
        con.register("incoming", rows)
        con.execute("BEGIN")
        con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
        con.execute(f"""
            CREATE TABLE "{TABLE}" (
                index_name       VARCHAR NOT NULL,
                ticker           VARCHAR NOT NULL,
                security         VARCHAR,
                start_date       DATE NOT NULL,
                end_date         DATE NOT NULL,
                start_is_horizon BOOLEAN,
                confidence       VARCHAR,
                source           VARCHAR,
                fetched_at       TIMESTAMP_NS
            )
        """)
        con.execute(f"""
            INSERT INTO "{TABLE}"
            SELECT index_name, ticker, security,
                   CAST(start_date AS DATE), CAST(end_date AS DATE),
                   start_is_horizon, confidence, source,
                   CAST(fetched_at AS TIMESTAMP_NS)
            FROM incoming
        """)
        con.execute("COMMIT")
        con.unregister("incoming")
        written = con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()
        loader_provenance.record(
            con, table=TABLE, source=SOURCE,
            endpoint=API.format(page=CHANGES_PAGE),
            licence="Wikipedia CC BY-SA 4.0; community-maintained, not vendor "
                    "data",
            rows=written[0] if written else 0,
            note=f"reconstructed backwards from {horizon}; trustworthy from "
                 "~2011, approximate 2007-2010")
        print(f"\n{TABLE} written: {written[0] if written else 0} rows")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
