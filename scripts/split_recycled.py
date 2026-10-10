#!/usr/bin/env python3
"""Split the price series that carry two companies under one symbol.

A ticker symbol is not a company, it is a lease. When a company dies the
exchange re-issues its symbol, and a price loader keyed on the symbol alone
splices the two occupants into one unbroken series: `COR` is CoreSite Realty
until 2021-12-28 and Cencora afterwards, with no gap, break or marker at the
seam. `src/data/delistings.go` classifies those series as `recycled`; this
script is what actually separates them.

THE CONVENTION: the company trading today keeps the bare symbol and the
previous occupant takes a generation suffix, numbering from the oldest -- so
`COR` keeps meaning what every existing config already meant by it, and the
dead company becomes addressable as `COR.1` rather than deleted. The separator
is `src/data.GenerationSeparator` on the Go side and SEPARATOR here; the two
must agree.

THE JOIN IS THE HARD PART, NOT THE SPLIT. A delisting date alone does not prove
the later bars are a different company: a symbol can be delisted from one
exchange and relisted on another as the same company, and a company can simply
change its name. So every candidate is corroborated against `company_profile`
(SEC EDGAR, loaded by add_company_info.py) before anything is rewritten:

  split         the delisted name matches neither the current filer's name nor
                any of its former names -- two different companies
  same_company  it matches one of them -- a rename or a relisting, NOT a
                recycled symbol, and splitting it would be the error this
                check exists to prevent
  review        no `company_profile` row for the symbol, so there is nothing to
                corroborate against. Printed and left alone.

Under-splitting is recoverable; wrongly splitting a continuous company is not,
so the matching deliberately errs toward `same_company`. The known cost of that
choice: when an ACQUIRER takes its target's name, the two names match and the
series is left spliced. `COHR` is the live example -- Coherent Inc was bought by
II-VI in 2022 and II-VI renamed itself Coherent Corp, so "Coherent Inc" matches
"COHERENT CORP." at the suffix tier and COHR stays unsplit although it really
does carry two companies. Read the `same_company` list; it is not a proof.

SCOPE: rewrites `stock_data_optimized`, which is the only price table the Go
engine reads. The legacy `stock_data` table is deliberately left untouched --
say so out loud rather than half-migrating a table nothing reads.

Usage:
    python3 scripts/split_recycled.py                # dry run, writes nothing
    python3 scripts/split_recycled.py --apply        # rewrite the bars
Requires the UI app closed (DuckDB is single-writer).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd

import loader_provenance

ROOT = Path(__file__).resolve().parent.parent  # the repo root; scripts live in scripts/
DEFAULT_DB = os.environ.get("BACKTESTER_DB", str(ROOT / "stock_data.db"))
PRICES = "stock_data_optimized"

# Must equal src/data.GenerationSeparator.
SEPARATOR = "."

# Must equal src/data.RecycledStartDays. Bars more than two years past a
# delisting cannot belong to the delisted company.
RECYCLED_START_DAYS = 730

# The corporate-suffix set from src/data/symbol_match.go, so that "CoreSite
# Realty Corp" and "CoreSite Realty Corporation" are one name in both languages.
SUFFIXES = {
    "INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY", "LTD",
    "LIMITED", "LLC", "LP", "PLC", "HOLDINGS", "HOLDING", "GROUP", "SA", "NV",
    "AG", "TRUST", "REIT", "CLASS", "COM", "NEW",
}


def normalise(name: str) -> str:
    """Upper-case, strip punctuation and the state-of-incorporation suffix that
    EDGAR appends (`COMMUNICATIONS SYSTEMS INC/MN`), collapse whitespace."""
    n = name.upper().split("/")[0]
    n = re.sub(r"[^A-Z0-9 ]+", " ", n)
    return " ".join(n.split())


def strip_suffixes(name: str) -> str:
    """`normalise` with the corporate suffixes removed, which is the weaker of
    the two tiers below."""
    return " ".join(w for w in name.split() if w not in SUFFIXES)


def same_company(
    delisted_name: str, profile_name: str, formers: list[str],
) -> tuple[str, str]:
    """Which tier, if any, says the delisted name IS the filer trading today,
    and WHICH of that filer's names it matched.

    Returns ("exact"|"suffix"|"core", matched name), or ("", "") -- the same
    precedence as src/data/symbol_match.go, so that a rename is recognised by
    the same rules that join a delisting to its cause.

    The matched name is returned rather than the current one because they are
    routinely different, and that difference is the whole finding: `DD` is not
    a recycled symbol because "DowDuPont Inc" is a FORMER name of the filer now
    called DuPont de Nemours. Reporting the current name as the evidence would
    describe a match that did not happen.
    """
    target = normalise(delisted_name)
    candidates = [c for c in [profile_name] + list(formers) if c]
    if not target:
        return "", ""
    for c in candidates:
        if normalise(c) == target:
            return "exact", c
    bare = strip_suffixes(target)
    # A name that is nothing but suffixes cannot corroborate anything.
    if not bare:
        return "", ""
    for c in candidates:
        if strip_suffixes(normalise(c)) == bare:
            return "suffix", c
    # `Ads-Tec Energy Plc` against `Ads-Tec Energy Public Ltd Co`: one name's
    # words are a prefix of the other's once the suffixes are gone. Two words
    # minimum, so a single common word like AMERICAN corroborates nothing.
    words = bare.split()
    for c in candidates:
        ow = strip_suffixes(normalise(c)).split()
        n = min(len(words), len(ow))
        if n >= 2 and words[:n] == ow[:n]:
            return "core", c
    return "", ""


def load(con: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, dict[str, int]]:
    """Every priced symbol whose bars run more than RECYCLED_START_DAYS past its
    delisting date AND begin before it, with whatever EDGAR knows about the
    symbol today. Also returns the counts this script does NOT act on, so its
    numbers can be reconciled against the Go audit."""
    profiles_exist = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'company_profile'").fetchone()[0] > 0

    context = {
        "begins_after": con.execute(f"""
            SELECT COUNT(*) FROM (
              SELECT d.symbol FROM delistings d
              JOIN "{PRICES}" s ON s."Ticker" = d.symbol
              WHERE d.security_class = 'operating'
              GROUP BY d.symbol, d.delisting_date
              HAVING date_diff('day', d.delisting_date::date, MAX(s."Date")::date)
                     > {RECYCLED_START_DAYS}
                 AND MIN(s."Date")::date > d.delisting_date::date)
        """).fetchone()[0],
        "non_operating": con.execute(f"""
            SELECT COUNT(*) FROM (
              SELECT d.symbol FROM delistings d
              JOIN "{PRICES}" s ON s."Ticker" = d.symbol
              WHERE d.security_class <> 'operating'
              GROUP BY d.symbol, d.delisting_date
              HAVING date_diff('day', d.delisting_date::date, MAX(s."Date")::date)
                     > {RECYCLED_START_DAYS})
        """).fetchone()[0],
    }

    df = con.execute(f"""
        SELECT d.symbol,
               d.name                       AS delisted_name,
               d.delisting_date::date       AS delisting_date,
               MIN(s."Date")::date          AS first_bar,
               MAX(s."Date")::date          AS last_bar,
               COUNT(*)                     AS bars,
               COUNT(*) FILTER (WHERE s."Date" <= d.delisting_date) AS bars_before,
               COUNT(*) FILTER (WHERE s."Date" >  d.delisting_date) AS bars_after
        FROM delistings d
        JOIN "{PRICES}" s ON s."Ticker" = d.symbol
        WHERE d.security_class = 'operating'
        GROUP BY d.symbol, d.name, d.delisting_date
        HAVING date_diff('day', d.delisting_date::date, MAX(s."Date")::date)
               > {RECYCLED_START_DAYS}
           -- A series that BEGINS after the death cannot contain the dead
           -- company: the price table simply has no bars for it, and there is
           -- nothing to split. Mirrors the same rule in ClassifyCoverage.
           AND MIN(s."Date")::date <= d.delisting_date::date
        ORDER BY d.symbol
    """).df()

    if not profiles_exist:
        df["profile_name"] = None
        df["former_names"] = None
        df["cik"] = None
        return df, context

    prof = con.execute("""
        SELECT ticker, cik, name AS profile_name, former_names
        FROM company_profile
    """).df()
    return df.merge(prof, left_on="symbol", right_on="ticker", how="left"), context


def bucket(df: pd.DataFrame) -> pd.DataFrame:
    """Tag each candidate split / same_company / review, with the reason."""
    verdicts, reasons = [], []
    for row in df.itertuples():
        profile = getattr(row, "profile_name", None)
        if not isinstance(profile, str) or not profile:
            verdicts.append("review")
            reasons.append("no company_profile row -- nothing to corroborate against")
            continue
        raw = getattr(row, "former_names", None)
        try:
            formers = json.loads(raw) if isinstance(raw, str) else list(raw or [])
        except (TypeError, ValueError):
            formers = []
        tier, matched = same_company(row.delisted_name, profile, formers)
        if tier:
            verdicts.append("same_company")
            via = "" if matched == profile else f", a former name of {profile!r}"
            reasons.append(
                f"{tier} name match: {row.delisted_name!r} is {matched!r}{via}")
        else:
            verdicts.append("split")
            reasons.append(f"{row.delisted_name!r} is not {profile!r} nor a former name")
    out = df.copy()
    out["verdict"] = verdicts
    out["reason"] = reasons
    return out


def report(df: pd.DataFrame, context: dict[str, int]) -> None:
    print(f"{len(df)} priced symbols are genuinely spliced: their bars run "
          f"> {RECYCLED_START_DAYS} days past the delisting AND begin before "
          f"it.\n")
    # These three lines exist so this script's numbers reconcile with
    # src/data.DelistingCoverage, which classifies every asset class and has no
    # notion of "operating". A silent disagreement between the audit and the
    # repair is how one of them stops being believed.
    print(f"  {context['begins_after']:5}  more would be recycled on the "
          "last bar alone, but begin after the delisting -- one company, "
          "nothing to split")
    print(f"  {context['non_operating']:5}  recycled symbols are ETFs or "
          "derivatives, out of scope here\n")
    for verdict in ("split", "same_company", "review"):
        rows = df[df["verdict"] == verdict]
        print(f"{verdict:13} {len(rows):5}")
    print()

    to_split = df[df["verdict"] == "split"]
    if len(to_split):
        print("TO SPLIT -- the dead occupant moves to the generation symbol:")
        for r in to_split.itertuples():
            gen = f"{r.symbol}{SEPARATOR}1"
            print(f"  {r.symbol:8} {r.bars:6} bars {r.first_bar}..{r.last_bar}")
            print(f"    -> {gen:10} {r.bars_before:6} bars {r.first_bar}"
                  f"..{r.delisting_date}   ({r.delisted_name})")
            print(f"    -> {r.symbol:10} {r.bars_after:6} bars "
                  f"(> {r.delisting_date})..{r.last_bar}   ({r.profile_name})")

    kept = df[df["verdict"] == "same_company"]
    if len(kept):
        print("\nNOT SPLIT -- corroboration says these are one company:")
        for r in kept.itertuples():
            print(f"  {r.symbol:8} {r.reason}")

    review = df[df["verdict"] == "review"]
    if len(review):
        print("\nREVIEW -- untouched, no evidence either way:")
        for r in review.itertuples():
            print(f"  {r.symbol:8} {r.delisted_name!r} delisted {r.delisting_date}")


def apply_splits(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Move each dead occupant's bars to its generation symbol. One transaction:
    a half-applied split is a third kind of wrong series."""
    to_split = df[df["verdict"] == "split"]
    moved = 0
    con.execute("BEGIN")
    try:
        for r in to_split.itertuples():
            gen = f"{r.symbol}{SEPARATOR}1"
            con.execute(
                f'UPDATE "{PRICES}" SET "Ticker" = ? '
                f'WHERE "Ticker" = ? AND "Date" <= ?',
                [gen, r.symbol, str(r.delisting_date)])
            moved += int(r.bars_before)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return moved


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--apply", action="store_true",
                    help="rewrite the bars; without it nothing is written")
    args = ap.parse_args()

    try:
        con = duckdb.connect(args.db, read_only=not args.apply)
    except duckdb.IOException as e:
        print(f"Could not open {args.db}: {e}\n"
              "If a lock is held, close the UI app (DuckDB is single-writer).",
              file=sys.stderr)
        return 1

    try:
        if not con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'delistings'").fetchone()[0]:
            print("No `delistings` table. Run add_delistings.py first.",
                  file=sys.stderr)
            return 1

        raw, context = load(con)
        df = bucket(raw)
        report(df, context)

        if not args.apply:
            print("\n(dry run -- nothing written; pass --apply to rewrite)")
            return 0

        moved = apply_splits(con, df)
        n = int((df["verdict"] == "split").sum())
        print(f"\nsplit {n} symbols, {moved} bars moved to generation symbols")
        loader_provenance.record(
            con, table=PRICES, source="split_recycled.py over the delisting registry",
            endpoint="(local: delistings x stock_data_optimized x company_profile)",
            licence="derived from the existing price table",
            rows=moved,
            note=f"{n} recycled symbols split at their delisting date; the dead "
                 f"occupant moved to <symbol>{SEPARATOR}1. Corroborated against "
                 "company_profile; symbols with no profile row were left alone")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
