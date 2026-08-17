# Backtester

A concurrent Go backtesting engine that simulates one or more portfolios over historical equity data stored in DuckDB. Portfolios, date ranges, tickers, and strategies are defined in a TOML config; each portfolio runs in parallel on a worker pool sized to the host's CPU count. Strategies are written in Lua, and a Wails desktop UI (with a built-in AI assistant) drives the same engine.

## How it works

- **`src/main.go`** — CLI entry point. Opens the DuckDB file, loads `config.toml`, converts each TOML portfolio entry into a `Portfolio`, and hands them to the runner.
- **`src/data/database.go`** — DuckDB access layer. Reads OHLCV bars from `stock_data_optimized`, daily risk-free rates from `3MTreasuryYields`, and serves the read-only `RunQuery` used by the UI and the assistant.
- **`src/backtest/runner.go`** — orchestrates the simulation. Validates that every portfolio's window is covered by its tickers' data, pre-fetches history for every unique ticker in one union-range query, aligns each portfolio onto the trading days common to its own tickers, then fans the portfolios out across `runtime.NumCPU()` workers. Each worker runs on a `Clone()` of the portfolio, so strategy state is never shared. Results come back as a `[]Result` (metrics plus the equity curve and its dates) and are additionally written to a file when the config has an `[Output]` block.
- **`src/backtest/strategy.go`** — the `Strategy` interface and `NewStrategy`, which builds one from a spec string (`greedy`, `equalWeights`, `buyAndHold:<type>`, `smaCross:<short>:<long>:<type>`, `lua:<path>`). `BuyAndHold` and `SMACross` both implement it; the package-level `SMA` and `RSI` helpers are used by `SMACross` and are also exposed to Lua as `sma()` / `rsi()`.
- **`src/backtest/strategy_lua.go`** — the Lua strategy host. Loads a `.lua` file, injects the price/account/order API, and calls its `step(day)` once per trading day.
- **`src/backtest/portfolio.go`** — portfolio state, `Buy` / `Sell` / `Deposit` / `Withdraw`, and end-of-day mark-to-market via `AdjustPortfolioParameters`.
- **`src/backtest/metrics.go`** — Sharpe, Sortino, max drawdown, annualized return, and standard deviation, all annualized over a 252-trading-day year.
- **`src/backtest/reporter.go`** — writes results as txt / csv / json, with optional filtering, sorting, and a row limit.
- **`src/backtest/correlation.go`** — cross-asset correlation over the fetched history.
- **`ui/`** — the Wails desktop app: backtest runs, a SQL console, quotes, screening, and the AI assistant.

## Prerequisites

- Go 1.24+ (module pinned to `go 1.24.4`).
- A DuckDB file named `stock_data.db` in the repository root containing:
  - `stock_data_optimized(Date, Ticker, Open, High, Low, Close, Volume)`
  - `"3MTreasuryYields"(Date, daily_risk_free_rate_decimal)`
- A `config.toml` in the repository root (see below).

Dependencies (`github.com/marcboeker/go-duckdb`, `gonum.org/v1/gonum`, `github.com/BurntSushi/toml`, `github.com/yuin/gopher-lua`, and the Wails runtime) are pulled via `go mod`.

### Building from a fresh clone

`go build ./...` fails on a clean checkout with:

```
pattern all:frontend/dist: no matching files found
```

This is expected. `ui/main.go` embeds `frontend/dist`, which is a build output ignored by `ui/.gitignore`, so the frontend has to be built before the Go build can embed it — normal for a Wails project. Either build the whole app:

```bash
wails build -tags webkit2_41   # the webkit tag is required on this setup
```

or just the frontend, if you only want `go build ./...` to succeed:

```bash
cd ui/frontend && npm install && npm run build
```

Do not "fix" this by committing `dist`. The engine itself needs no such step: `go build ./src/...` works on a bare clone.

## Long-history benchmark assets

`add_collections.py` populates `stock_data_optimized` with index / asset-class
benchmarks (the kind testfol.io exposes) so portfolios can hold and test against
them. Each is one daily **total-return** series under a `$`-prefixed pseudo-ticker
so it never collides with a real stock, and all share the NYSE trading calendar
so they can be mixed with each other and with real stocks in one portfolio.

| Ticker | Collection | Coverage |
| --- | --- | --- |
| `$SP500` | S&P 500 | 1927– |
| `$USTOT` | Total U.S. Market | 1927– |
| `$SP400` | S&P 400 MidCap | 1981– |
| `$SP100` | S&P 100 | 1982– |
| `$RUS2K` | Russell 2000 | 1987– |
| `$SP600` | S&P 600 SmallCap | 1989– |
| `$EM` | Emerging Markets | 2005– |
| `$EXUS` | Total ex-US Market | 2007– |
| `$WORLD` | Total World Market | 2008– |
| `$CASH` | Cash (T-bill) | 1927– |

Modern history uses a real ETF's dividend-adjusted close (true total return);
pre-ETF history uses the real index price level plus an estimated dividend from
Shiller's monthly S&P 500 yield (so only the early *dividend* is approximated,
never the price).

**ETF legs are grossed back up by the fund's expense ratio**, because a fund's
adjusted close is a *fund* return — already net of its fee — and these series
are meant to represent the index. Without that correction every series would
silently become "the index minus fees" from its ETF splice date onward (1993
for `$SP500`, and 1995–2008 for the others), which compounds to roughly 3% of
cumulative return over thirty years of SPY. The ratio is stored per leg in
`COLLECTIONS` in `add_collections.py`; re-check the figures when you rebuild,
since fee cuts are common and nothing detects a stale value. Two limits remain
by design: the ratio is a single constant applied to the whole leg even though
several of these funds have cut fees over time, and tracking error is not
corrected. Both are far smaller than ignoring the fee.

`$CASH` compounds the FRED 3-month T-bill rate (daily, 1954+)
spliced onto Ken French's 1-month T-bill return (monthly, 1926–1953, since a
daily 3-month series doesn't exist that far back). Rebuild or
refresh anytime (idempotent per symbol; close the UI first — DuckDB is
single-writer):

```bash
python3 add_collections.py            # build/refresh all
python3 add_collections.py --dry-run  # preview coverage, no write
python3 add_collections.py --only '$SP500' '$CASH'
```

## Fundamentals, macro and the publication lag

Beyond prices and the risk-free rate, `stock_data.db` carries several tables
the engine does not yet read:

| Table | Rows | Coverage |
| --- | --- | --- |
| `financials` | 8.5M | ~6,900 tickers, 2020-07 → 2026-03, quarterly |
| `earnings_calendar` | 21k | ~1,000 tickers, 1999 → 2026, real report timestamps |
| `economic_indicators` | 5k | CPI, FEDFUNDS, GDP, INDPRO, M2, UNRATE, 1927+ |
| `"10YrTreasuryYields"` | 4k | 2010 → 2026 |
| `crypto_ohlcv` | 15k | 5 tickers, 2014+, **not** on the NYSE calendar |
| `company_info` | 0 | empty, and its `(Ticker, Date, Value)` shape was never company metadata — ignore it |
| `company_profile` | optional | ticker, CIK, SIC code, **sector**, former names — loaded from SEC EDGAR, see below |

`financials` is long-format (`metric, date, value, ticker, frequency`) and
includes Total Revenue, Net Income, Stockholders Equity, Total Assets,
Tangible Book Value, Cash And Cash Equivalents and Ordinary Shares Number — so
P/E, P/B, P/S, ROE, ROA and market cap are all derivable. About 6,600 of those
tickers also have price history.

> **`financials.date` is the fiscal period end, not the publication date.**
> Its dates are overwhelmingly 12-31, 03-31, 06-30 and 09-30. Joining it to
> prices on that date uses Q4 figures on December 31, weeks before they were
> published — look-ahead bias that flatters any factor backtest built on it.

Read it through `src/data.PointInTimeFundamentals`, which dates every figure
with the day it became knowable, by three rules in order of preference:

1. **The SEC filing date** from `sec_financials.filed` (see below), taking the
   *earliest* filing that carried the period, usable from the following session.
   A primary-source fact rather than an estimate — and the reason for taking the
   earliest is that a period restated in a later filing must not be dated by a
   filing that did not exist yet.
2. **The `earnings_calendar` report date** where one exists, plus one day, since
   those timestamps land after the close.
3. **Period end + `FixedReportLagDays` (90)** otherwise.

Each point records which rule applied (`LagSource`), so a result resting mostly
on the fallback can be read accordingly. The helper also deduplicates — the raw
table contains exact duplicate rows.

Rules 1 and 2 disagree often, and rule 1 wins. A company announces headline
results days or weeks before it files, so for revenue and earnings the
announcement really is the first knowable date — but the balance-sheet metrics
here are frequently not in the press release, and this helper's principle is to
be late rather than early. If you are studying earnings surprises specifically,
read the announcement date from `earnings_calendar` directly.

**The 90-day fallback is deliberately not retuned**, even though `sec_financials`
now shows the real median is 48 days. A median puts half of all figures earlier
than they were published, which is precisely the bias the whole mechanism exists
to prevent; the fallback has to clear essentially every filer, so it stays at the
worst case. The fix for the lost signal is rule 1, not a smaller constant.

### Fundamentals back to 2009, dead companies included

`financials` starts 2020-07 and holds only tickers that still exist. SEC's
Financial Statement Data Sets — free, no key — go back to 2009, include the
companies that later died, and carry **`filed`, the real publication date**.

```bash
python3 add_sec_financials.py --dry-run --limit-quarters 1   # probe
python3 add_sec_financials.py --from-year 2020               # smaller
python3 add_sec_financials.py                                # 2009 -> now
```

It lands in its own `sec_financials` table rather than into `financials`, so the
existing numbers cannot move and the overlapping window becomes a free
correctness check on both sources. Metrics use the same vocabulary (`Total
Revenue`, `Net Income`, `Stockholders Equity`, `Total Assets`, `Total Liabilities
Net Minority Interest`, `Cash And Cash Equivalents`, `Operating Income`,
`Ordinary Shares Number`, `Diluted Average Shares`). Read it with
`src/data.LoadSECFundamentals`, and reduce it with `FirstKnown` / `KnownOn`.

**The measurement that matters**, from the 2026Q1 quarter (96,713 rows, 5,109
companies, 509 of them with no current ticker — the delisted ones):

> The publication lag for the figures a filing actually reports is a **median of
> 48 days** (p10 31, p90 62, max 90).

So `PointInTimeFundamentals`' 90-day fallback is not a typical lag — it is the
worst case, and it delays every fundamental by about six extra weeks. That is
conservative rather than biased, but it throws away real signal, and having the
true dates is the fix.

Two shapes of row live in the table and the difference is load-bearing:
`period_end == filing_period` is the figure the filing is reporting, and
`period_end < filing_period` is a **comparative** — every 10-K restates prior
years, so ~60% of rows are these. Both are kept, because a comparative's `filed`
date is honest evidence that the figure was public by then. `FirstKnown` takes the
*earliest* filing of each figure, which is when it became knowable; using a later
restatement at the original filing's date would be look-ahead bias in disguise.

Costs and limits, measured: ~124 MB per quarterly ZIP, ~7 s to fetch and ~8 s to
parse, ~102,000 rows kept. A full run is ~69 quarters, ~7 GB of transfer, ~20
minutes, ~7 M rows; quarters are streamed and inserted one at a time so the disk
cost is the table, not the archive. Only 10-K and 10-Q are read (amendments are
skipped); segment and subsidiary rows are skipped to avoid double-counting the
parent; year-to-date partials are skipped rather than mislabelled. SEC publishes
each quarter a few months in arrears — 2026Q1 was the newest available on
2026-08-17 — and a missing quarter is reported and skipped.

Two further caveats before building anything on this:

- **`economic_indicators` has the same trap.** `Date` is the period, and CPI
  for January is published in mid-February and revised for years. Lag it
  before using it in a decision. Regime signals computed from prices
  (drawdown depth, realised volatility) need no lag.
- **Fundamentals begin 2020-07** — five and a half years containing one
  inflation shock and one hiking cycle, which is a single macro regime. Treat
  factor results over that window as hypothesis-generating, not evidence.

### Sector data, from SEC EDGAR

There *is* sector data available, and it is free. SEC EDGAR publishes the
industry classification of every company that files with it — no key, no
signup — and `add_company_info.py` loads it into the optional
`company_profile` table:

```bash
python3 add_company_info.py --dry-run --limit 20   # probe, writes nothing
python3 add_company_info.py --dry-run              # full fetch, no write
python3 add_company_info.py                        # fetch and replace the table
```

Columns: `ticker`, `cik`, `name`, `sic`, `sic_description`, `sector`,
`exchange`, `former_names` (a JSON array), `state_of_incorporation`,
`fetched_at`. A full run is about 5,600 requests — one per company, paced at
~8/s to stay inside SEC's fair-use limit — and it checkpoints to
`company_info_cache.json`, so an interrupted run resumes rather than starting
over. Read it from Go with `src/data.LoadCompanyProfiles` or the narrower
`SectorsForTickers`; both return nil when the table is absent, so nothing
changes until you load it.

`CIK` is worth knowing about: it is the SEC's permanent filer identifier and,
unlike a ticker symbol, it is never reassigned. It is the only stable way to
say "the same company" across a rename or a symbol change — which is exactly
the evidence the recycled-ticker problem below needs. `former_names` is the
rename trail for the same reason.

Three limits, stated up front:

- **SIC is not GICS.** It is the SEC's own scheme, coarser and older: code 3571
  is "Electronic Computers", which puts Apple in Manufacturing. `sector` is the
  SIC *division* the code falls in (`src/data.SectorForSIC`, mirrored in the
  loader), and the divisions have genuine gaps — 1800–1999 and 6800–6999 belong
  to nothing, so those companies come back `Unclassified`.
- **Coverage is about 5,600 of the 10,434 tickers with prices.** The rest are
  `$`-prefixed benchmark series (not companies at all), ADRs that file little,
  and delisted symbols.
- **Delisted companies are missing, for a fixable reason.**
  `company_tickers.json` lists only companies with a *current* ticker. EDGAR
  keeps a dead company's filing history indefinitely, so the data exists — it
  just cannot be found by symbol. Pass `--cik-list FILE` to fetch those
  directly once you have their CIKs.

## Delisted companies, and recycled tickers

A ticker symbol is not a company — it is a lease. When a company dies the
exchange re-issues its symbol, and because `stock_data_optimized` is keyed on
the symbol alone, every re-use splices **two companies into one unbroken price
series** with no marker at the seam. 269 tickers in the database are affected:

| Ticker | Series in the database | What it actually is |
| ------ | ---------------------- | ------------------- |
| `DD`   | 1972 → 2026, unbroken  | DowDuPont was delisted 2019-05-31; today's `DD` is DuPont de Nemours |
| `SUNE` | 1981 → 2026, unbroken  | SunEdison went **bankrupt** in 2016 |
| `COR`  | 1995 → 2026, unbroken  | CoreSite Realty until 2021, then Cencora |
| `DRS`  | 1985 → 2026, unbroken  | DRS Technologies died 2009; Leonardo DRS re-listed in 2022 |

This is not survivorship bias, and it is worse. Survivorship bias makes a
result optimistic in a direction you can reason about; a spliced series makes
it arbitrary, because the strategy holds one company and is paid by another.

### The `delistings` table

Load the registry that makes these findable:

```bash
python3 add_delistings.py --dry-run   # fetch, classify, report, write nothing
python3 add_delistings.py             # replace the table
```

It needs `ALPHA_VANTAGE_KEY` in `.env` (a free key covers it — the whole
universe arrives in one call) and the UI closed, since DuckDB is single-writer.
Columns are `symbol, name, exchange, asset_type, security_class, ipo_date,
delisting_date`, where `security_class` is `operating`, `etf` or `derivative`
(SPAC units, warrants, rights and preferreds, which never were companies).

**The table is optional.** With it absent every query below returns
`unknown` and the engine behaves exactly as it did before this shipped.

### Reading it from Go

`src/data.DelistingCoverage` joins the registry to the price table and returns
a verdict per ticker, based on how far the bars run past the delisting date:

| Verdict | Bars run past delisting by | Tradable |
| ------- | -------------------------- | -------- |
| `clean` | ≤ 5 days (or stop earlier) | yes |
| `stale_tail` | 6–89 days — vendor disagreement about the final session | yes |
| `suspect` | 90–730 days — the death date is wrong, or a second company took the symbol | **no** |
| `recycled` | > 730 days — two companies in one series | **no** |
| `unknown` | no registry entry | yes |

`Coverage.Trustworthy()` collapses that to a boolean, and
`UntrustworthyTickers` names the offenders. `unknown` is the absence of
evidence, not a clean bill of health — see the coverage limit below.

### What this does *not* fix

The registry's coverage effectively **begins in 2013** and is only dense from
2015; eight rows predate 2009. Enron, WorldCom, Lehman, Washington Mutual and
Bear Stearns are absent from it and from the price table alike.

So the broader survivorship problem stands: the database contains no company
that stopped trading before 2025, and of the 1,792 tickers trading in 2000 all
1,792 are still present — against a real twenty-five-year survival rate nearer
40–50%. Long multi-asset backtests on real tickers are choosing among known
survivors and overstate returns by roughly 1–4 points a year. Closing that gap
needs paid data. Until then, prefer the `$`-benchmark series for long horizons:
they are reconstructed index series and are survivorship-free by construction.

### Provenance: which source produced which table

The database is assembled from Yahoo (prices), Alpha Vantage (delistings), SEC
(fundamentals, industry codes, delisting reasons), Wikipedia (index membership)
and FRED (yields). Every loader records what it wrote, as its last step, into a
`data_sources` table:

| Column | |
| --- | --- |
| `table_name`, `source`, `endpoint`, `licence` | what it is and where it came from |
| `row_count` | how much of it there is |
| `loader`, `loader_git_sha` | which script, at which commit — suffixed `-dirty` when the tree had uncommitted changes |
| `fetched_at`, `note` | when, plus the caveat that belongs with that source |

One row per **(table, loader)**, because `stock_data_optimized` genuinely has two
contributors: `add_ticker.py` for real companies and `add_collections.py` for the
`$`-prefixed benchmark series, which are built completely differently. Read it
with `src/data.LoadDataSources`, `SourcesFor`, or `SummarizeSources` for a
one-line-per-source footer.

Two rules in `loader_provenance.py` worth knowing. Recording provenance **never
fails a load** — every error there is swallowed and reported, because a missing
provenance row is a documentation gap while a failed load is lost work. And it
**cannot be reconstructed afterwards**: once a loader has run without it, the
fetch date and the code version that produced those rows are gone, which is why
every loader calls it rather than only the interesting ones.

### Who was in the index on a given date

The larger half of survivorship bias is **selection**, not missing prices. The
default config backtests AAPL/MSFT/GOOGL/AMZN/NVDA over 2015–2025 — a basket
chosen in 2026 with knowledge of who won — and no amount of walk-forward
machinery repairs a universe picked that way. Point-in-time membership fixes that
half, and it works *without* the dead names' prices: a run that knows `SIVB` was
an S&P 500 member until 2023-03-15 is asking an honest question even with no bars
for what came after.

```bash
python3 add_index_membership.py --dry-run          # reconstruct and report
python3 add_index_membership.py                    # replace the table
python3 add_index_membership.py --from-year 2011   # only the dense era
```

Two free Wikipedia tables — today's 503 constituents, and 408 dated add/remove
events — reconstructed **backwards**: a ticker added on date D was not a member
the day before, and one removed on D was. The result is `index_membership(index_name,
ticker, security, start_date, end_date, start_is_horizon, confidence, source,
fetched_at)`; read it with `src/data.IndexMembersOn` or the pure `MembersOn`.
Current members carry `end_date = 9999-12-31`, so a date comparison needs no
special case.

**How far back it means anything**, which is the whole caveat. The index really
changes 20–25 constituents a year. Measured against the source on 2026-08-17:

| Era | Changes recorded per year | Verdict |
| --- | --- | --- |
| 2011–2026 | 16–30 | dense; consistent with reality |
| 2007–2010 | 8–13 | partial |
| 1976–2006 | 0–7 | effectively absent |

So the reconstruction is good back to ~2011, approximate to 2007, and fiction
before that — a ticker whose real removal in 2003 is missing from the table stays
"a member" all the way to the horizon. Hence the per-row `confidence`, the
`--from-year` horizon, and `start_is_horizon`, which marks "was already a member
when reliable history begins" as distinct from "joined on this date".

The loader's own check is the member count per year, which should hold near 500.
On the verification run it held **501–510 across 2007–2026**, and the four spot
checks came out right: `ATVI` 2015-08-28 → 2023-10-18, `SIVB` 2018-03-19 →
2023-03-15, `TWTR` 2018-06-07 → 2022-11-01, and `BBBY` dropped on market cap in
2017 — years before its 2023 bankruptcy, which is itself a useful reminder that
leaving the index and dying are different events.

This is a scrape of a community-maintained page, not a vendor feed. Every row
records its `source`, and it should not be presented as authoritative index data.

### Why a company delisted — acquired, or wiped out

The registry above knows the date and not the cause, and the two causes are
opposite: an acquisition usually pays a premium, a Chapter 11 usually pays
nothing. That single missing field is what makes a delisting return a guess, and
it is why the literature falls back to a blanket haircut (Shumway & Warther 1999:
−55% for Nasdaq; Shumway 1997: nearer −30% for NYSE/AMEX) that overcorrects every
acquisition.

SEC EDGAR answers it, free and without a key. `add_delisting_reasons.py` works in
two stages:

```bash
# probe: one year of filings, four known companies, writes nothing
python3 add_delisting_reasons.py --dry-run --from-year 2023 --cik 718877
python3 add_delisting_reasons.py --dry-run          # full harvest, no write
python3 add_delisting_reasons.py                    # replace the table
python3 add_delisting_reasons.py --emit-cik-list ciks.txt --no-classify
```

1. **Who delisted, and when** — the quarterly filing indexes
   (`full-index/YYYY/QTRn/form.idx`) list every filing with its filer and CIK.
   `25-NSE` is filed *by the exchange* (involuntary), `25` by the company, and
   `15-12B`/`15-12G` are deregistrations. This stage is also the only free way to
   learn a **dead company's CIK**, which is the gap `add_company_info.py`
   documents — hence `--emit-cik-list`, which feeds its `--cik-list`.
2. **Why** — each filer's submissions document carries every filing's 8-K item
   numbers, and two settle it: **item 1.03** (Bankruptcy or Receivership) and
   **item 2.01** (Completion of Acquisition or Disposition of Assets).

Reasons are `bankruptcy`, `acquired`, `compliance` (the exchange removed the
listing and nothing explains it — a price, float or filing-delinquency failure,
after which the stock usually keeps trading over the counter), `voluntary` (left
quietly, usually a merger or going-private, but the filings do not say), and
`unknown`. `src/data.ClassifyReason` is pure and recomputes the reason from the
stored evidence, and `Reason.TerminalForHolders()` is true only for bankruptcy.

Verified 2026-08-17 against cases whose answers are public:

| Company | Evidence found | Reason |
| --- | --- | --- |
| SunEdison (CIK 945436) | 8-K item 1.03 on 2016-04-27; 25-NSE nine days later | `bankruptcy` |
| CoreSite Realty (1490892) | 8-K item 2.01 on 2021-12-28, same day as 25-NSE | `acquired` |
| Activision Blizzard (718877) | 8-K item 2.01 on 2023-10-13; 25-NSE; Form 15 | `acquired` |
| SVB Financial (719739) | item 2.01 on 2023-03-14 **and** item 1.03 on 2023-03-17 | `bankruptcy` |

SVB is why bankruptcy outranks acquisition: a company sold out of Chapter 11
files both, and its shareholders were still wiped out.

Two costs and one gap, all real. Stage 1 downloads ~31 MB per quarter (~2 GB for
2010→now, cached afterwards, and `--from-year` bounds it). Stage 2 is one request
per company at ~8/s. And the table is keyed by **CIK, not ticker** — EDGAR lists
a filer's tickers only while it has a current listing, which these by definition
do not, so this answers "what happened to this company" and not yet "what
happened to the symbol my backtest held". Closing that needs name-matching
against `delistings.name`, and is deliberately not guessed at.

### What is wrong with the bars themselves

Survivorship and symbol recycling are audited above. The bars needed auditing
too, and `add_data_quality.py` does it — one query over all 34,988,956 of them,
no sampling, a few seconds:

```bash
python3 add_data_quality.py --dry-run   # audit and report, write nothing
python3 add_data_quality.py             # audit and replace the data_quality table
```

Verdicts from the 2026-08-17 pass: **7,193 `good` (68.9%), 1,915 `usable`
(18.4%), 1,053 `suspect` (10.1%), 273 `unusable` (2.6%)**. So about one ticker in
eight has a defect large enough to move a metric by itself. The four findings
worth knowing before you trust a result:

- **Four tickers have negative prices** — `VHI` down to −639.30, `SVA` to
  −78.46, plus `SAFE` and `DEC`; 11,931 bars below zero. This is Yahoo's
  back-adjustment for a large special distribution exceeding the historical
  price. Every return computed across a sign change is meaningless and looks
  like an ordinary extreme move.
- **Some early history is synthetic.** `HUBB` has 1,862 *consecutive identical
  closes* (1977–1984), and its 1972 bars have `Open = High = Low = Close` with
  zero volume on non-consecutive dates. 281 tickers have a flat run of 60+. A
  moving average over one of those produces confident nonsense. The check cannot
  tell a stalled feed from a genuine trading halt, which is why the verdict is
  `suspect` rather than `unusable`.
- **263 tickers have a single-day move over 500%**, the largest 12,499× — mostly
  warrants and units moving in multiples off a near-zero base.
- **Gaps matter more than they look.** `MTRA` has 269 bars where the calendar has
  2,613. Because a day is only simulated when every ticker has a bar for it (see
  below), one gappy holding deletes those days for every other holding.

Also worth knowing: the price table has **no duplicate rows**, unlike
`financials`. And the `$` benchmark series are clean on every check — they report
`usable` only because they have zero volume, which is correct for a
reconstructed index rather than a defect.

The full audit, with every query and the twenty worst tickers per category, is in
`analysis/data_quality.md`. Read it from Go with `src/data.LoadDataQuality` or
`UntradableTickers`; both return nil when the table is absent. The verdict is
recomputed from the stored statistics by `src/data.ClassifyQuality`, so the
thresholds — which are judgement calls, named and reasoned about in
`src/data/data_quality.go` — can be changed without re-running the audit.
**Nothing currently refuses to trade an `unusable` series**; wiring that in
changes existing results, so it is a decision rather than a fix.

### Every ticker must cover the whole window

**This is a design rule, not an accident.** A portfolio's tickers all share one
window: the UI will not accept dates that conflict with a ticker's coverage, and
the engine enforces the same thing from two directions.

- `validateCoverage` **rejects** a portfolio outright when an explicit `EndDate`
  runs past any ticker's last bar (or `StartDate` precedes its first), naming the
  ticker and its real range.
- `alignToWindow` simulates a day **only when every ticker has a bar for it**, so
  the days that do run are directly comparable across holdings.

The consequence worth stating plainly: a stock that stopped trading cannot be
held in a portfolio whose window extends past its death. Give it a window that
ends at the delisting instead. This is why loading delisted price history would
not, by itself, let you mix dead and live names in one long run.

Where the rule can still bite quietly is an **open-ended window** or a **gap
inside one ticker's history** — neither is caught by `validateCoverage`, and both
shorten the run for every holding. That is no longer silent:

```
portfolio "Tech Giants": simulated 1,258 of the 2,517 trading days its window
covers (through 2025-03-31) — a day is only simulated when every ticker has a
bar for it; "SIVB" ends 2023-03-10, so the run was truncated there
```

The message names the ticker and the date, and distinguishes a truncated tail
from a gap in the middle. Nothing liquidates the short holding and carries the
survivors onward — that would break the shared-window invariant, which is the
property that makes two holdings' numbers comparable in the first place.

## Configuration

Define one `[[portfolio]]` block per portfolio in `config.toml`. Each block names exactly one strategy; to compare strategies, write one block per strategy. Every portfolio runs as its own job.

```toml
[[portfolio]]
Name        = "Tech Giants"
BuyingPower = 20000.0
StartDate   = "2015-03-31"   # YYYY-MM-DD
EndDate     = "2025-03-31"
Tickers     = ["AAPL", "MSFT", "GOOGL", "AMZN"]
Strategy    = "greedy"

[[portfolio]]
Name        = "Momentum"
BuyingPower = 25000.0
StartDate   = "2015-01-02"
EndDate     = "2024-01-01"
Tickers     = ["AAPL", "MSFT", "JNJ", "$CASH"]
Strategy    = "lua:strategies/momentum_rotation.lua"

  [portfolio.Params]
  lookback       = 126
  top_n          = 2
  rebalance_days = 21
```

Field reference:

| Field | Type | Notes |
| --- | --- | --- |
| `Name` | string | Identifier shown in metric output and results files. |
| `BuyingPower` | float | Starting cash. |
| `StartDate` / `EndDate` | string | `YYYY-MM-DD`. Both must be fully covered by every ticker's data or the run is rejected with an error naming the offending ticker and its real range. |
| `Tickers` | []string | Must exist in `stock_data_optimized` for the date range. |
| `Strategy` | string | One strategy spec (below). Required by the CLI; the UI falls back to the Lua script open in its editor. |
| `Params` | table | Optional, strategy-specific. Passed to a Lua strategy as the global `params`. |
| `Costs` | table | Optional trading-cost model (below). Absent means frictionless, which is how the engine behaved before costs existed. |
| `Benchmark` | string | Optional ticker to score against, e.g. `"$SP500"`. Never traded — see below. |

Strategy specs:

| Spec | Meaning |
| --- | --- |
| `greedy` | Pour all available buying power into each ticker in order. |
| `equalWeights` | Split buying power evenly across the portfolio's tickers. |
| `buyAndHold:<greedy\|equalWeights>` | The explicit form of the two above. |
| `smaCross:<short>:<long>:<buyType>` | Built-in moving-average crossover. |
| `lua:<path>` | Run a Lua strategy file, e.g. `lua:strategies/sma_cross.lua`. |

Note that config loading does no validation of its own: an omitted key simply zero-values, and the error surfaces later — a bad date or an unknown strategy spec fails in `ToPortfolio`, an uncovered date range in the runner's coverage check.

### `[portfolio.Costs]`

Trading is free unless you say otherwise. Every field defaults to `0.0`, so
omitting the block reproduces the frictionless results the engine produced
before this existed.

```toml
  [portfolio.Costs]
  commission_per_trade = 1.0   # flat charge per filled order
  commission_bps       = 2.0   # plus this many basis points of notional
  slippage_bps         = 5.0   # fills move against you by this much
```

Slippage worsens the fill price on both sides — buys fill above the quoted
close, sells below it — and the position's cost basis records the slipped
price. Commission is charged per filled order and treated as a cash expense
rather than capitalized into the basis.

Neither can overdraw the account. An order whose commission the balance
cannot cover does not fill at all. An order sized against the whole balance
(what `buy_max` and most Lua strategies do) still fills: it is clamped down
to the largest size whose fee the balance also covers, rather than being
rejected for the fee alone.

Costs make high-turnover strategies look worse, which is the point — the
`Turnover` metric reports how much trading each strategy is doing, so the two
can be read together.

### `Benchmark`

Set `Benchmark = "$SP500"` on a portfolio to score it against that series.
The benchmark is **never traded**: it does not receive capital, does not
affect which days are simulated, and is not valued as a holding. Adding it
changes nothing about what the portfolio does — only what gets measured.
A portfolio may hold and benchmark against the same ticker.

With it set, six extra metrics are reported: `Alpha`, `Beta`,
`TrackingError`, `InformationRatio`, `UpCapture` and `DownCapture`. Beta and
alpha are computed on returns excess of the risk-free rate; the rest on raw
returns.

The benchmark is also reported **on its own terms**, so "did I beat it" does
not have to be inferred from alpha. Each result carries a `BenchmarkCurve` —
the benchmark's value over the same trading days, compounded from the
portfolio's own starting capital so the two share one axis and one origin —
and `BenchmarkStats`, its own `AnnualReturn`, `SharpeRatio`, `MaxDrawdown` and
`StandardDev`, computed with the same functions the portfolio uses. The
desktop UI draws the curve as a muted dashed line on the equity chart and adds
one row per distinct benchmark to the metrics table.

These are nested values rather than scalars, so — like `Drawdowns` and the
calendar returns — they are not available as `[Output] fields`, which is a
flat table. They deliberately live outside `Metrics`: that struct describes
the portfolio, and mixing the benchmark's own figures into it would make every
consumer report two different subjects in one row.

If the benchmark's data does not cover every trading day of the window, the
six metrics are left at zero, the curve and stats are left empty, and a line
is logged. The backtest itself still runs — a benchmark is a measurement, and
a missing one is not a reason to lose the result.

### Automatic baseline

Every result now carries a **baseline**: what an equal-weight buy-and-hold of
**the same tickers** would have returned over the same days, with the same
starting capital and the same costs. No configuration — it is always computed.

```
Baseline (equal-weight buy & hold):  final 14,230.11  CAGR 7.3%  Sharpe 0.61
Strategy beat it by 1,120.44 (+2.1%/yr)
```

Most strategies do not beat buy-and-hold, and until this existed nothing said
so unless you built the comparison by hand — which tends to happen when you are
sceptical and not when you are excited. It is the fastest way to kill a bad
idea.

Two deliberate choices:

- **The same universe, not the market.** Beating the S&P 500 by holding five
  megacaps is a statement about the universe, not about the strategy. Holding
  the universe fixed isolates what the strategy actually contributed. The
  configured `Benchmark` answers the other question and is reported separately.
- **The same costs.** The baseline is a real simulation through the same order
  path, paying the same commission and slippage. An analytic curve would compare
  a costed strategy against a frictionless hold and flatter every result.

Read it on `Result.Baseline`: `FinalValue`, `AnnualReturn`, `SharpeRatio`,
`MaxDrawdown`, `StandardDev`, plus `ExcessFinalValue`, `ExcessAnnualReturn` and
a `Beat` flag. `Computed` is false when there was no baseline to build (no
tickers, or fewer than two days) — which is distinct from a strategy that
matched the baseline exactly.

### Trade blotter and per-ticker attribution

Every fill is now recorded, and every result carries `TradeStats`: trade counts,
win rate, average win and loss, profit factor, share-weighted average holding
period, total fees, and — the most decision-changing view in the app —
**per-ticker P&L attribution**.

```
Trades 34 (18 buys, 16 sells) · win rate 56% · profit factor 1.8
avg hold 27d · fees 412.50

  NVDA   realised +8,240   unrealised     0   12 trades
  MSFT   realised +1,110   unrealised  +430    8 trades
  INTC   realised -2,905   unrealised     0    9 trades
```

A Sharpe cannot tell you whether an edge was broad or was one lucky position;
that table can, at a glance. `Trades == 0` is worth its own attention: a strategy
that never traded is not a strategy, and until now it reported a 0% return like
any other.

Two accounting choices, made to agree with the engine rather than with
convention:

- **Realised P&L is average-cost**, because `Position.AveragePrice` is how the
  engine already values a holding. FIFO lot-matching would be equally defensible
  and would disagree with every other number in the app.
- **Holding periods are FIFO**, because "how long was this held" has no
  average-cost answer — shares bought on different days are indistinguishable
  once averaged. So money is average-cost and time is FIFO, deliberately.

Fees are charged to the trade that paid them and realised P&L is net of them, so
a strategy that is gross-profitable and net-negative shows up as exactly that
rather than as a winner.

### Is this result better than chance?

The deflated Sharpe below corrects the best of N trials for the size of the
search. It says nothing about a single strategy run once — which is the question
users ask first, because a Sharpe of 1.4 over ten years might be skill or might
be what a coin flip looks like on this particular price path.

Every result now carries `Significance`, with no configuration:

```
Sharpe 1.42 · p = 0.03 (97th percentile of random timing)
null mean 0.71, 95th pct 1.38 · bootstrap 90% CI [0.88, 1.94]
```

The null model is **random timing**. The strategy's daily exposure — how much of
the book was invested each day — is shuffled in time and applied to the market's
actual returns, a thousand times. That holds everything fixed except the thing
being tested: the same total exposure, the same price path, the same number of
days invested, and only *when* it happened is destroyed. If the strategy's
Sharpe sits inside that distribution, the timing added nothing.

**Why not simply permute the strategy's own returns**, which is the obvious first
idea: the Sharpe ratio is order-independent, so shuffling a return series leaves
its Sharpe exactly unchanged and the test would report p = 1 for everything.

`BootstrapLow`/`BootstrapHigh` answer a different question — not "is it real" but
"how precise is this number" — by resampling the strategy's own returns in
20-day blocks. Blocks rather than single days because daily resampling destroys
the autocorrelation every real return series has, giving an interval that is far
too narrow.

Both are seeded (`SignificanceSeed`) and deterministic: an unreproducible p-value
is worse than none, because nobody can check it. A run shorter than 60 days, or a
strategy that never invested, reports `Computed: false` rather than a number.

Note what this says about buy-and-hold: it is fully invested every day, so
shuffling its exposure changes nothing and it scores p ≈ 1. That is correct — it
has no timing to test — and it is the calibration case the implementation is
checked against.

### `[portfolio.Sweep]`

Turns one portfolio block into many runs. Every key maps to a **list**, and
the block expands to the cartesian product of them:

```toml
[[Portfolio]]
Name        = "RSI"
BuyingPower = 10000.0
StartDate   = "2015-01-01"
EndDate     = "2025-01-01"
Tickers     = ["AAPL"]
Strategy    = "lua:strategies/rsi.lua"
[Portfolio.Params]
  period     = 14        # base values
  buy_thresh = 30
[Portfolio.Sweep]
  period     = [7, 14, 21]
  buy_thresh = [20, 30]
```

That is 3 × 2 = 6 backtests, executed concurrently by the same worker pool
that runs multiple portfolios. Each takes `Params` with its own sweep values
written over the top, so a swept key overrides its base value and an unswept
key is inherited. Runs are named for their parameters — `RSI [buy_thresh=20
period=7]` — since the name is the only place the parameter set reaches the
results table and `[Output]`.

Expansion order is deterministic (keys sorted, values in written order), so
the same config always produces the same runs in the same sequence and two
runs can be diffed. The product is capped at 1000 runs; an oversized sweep is
rejected with an error naming the count rather than started.

Omit the block and the portfolio is exactly one run with exactly `Params`,
identical to every config written before sweeps existed.

Pair it with `[Output]` to rank the results:

```toml
[Output]
sort_by = "SharpeRatio"
order   = "desc"
limit   = 10
```

> **A sweep makes overfitting easy.** Two hundred parameter combinations over
> one window will always yield a flattering best-of-N: that winner was chosen
> using the same data it is being scored on. Treat a swept result as a
> hypothesis, and confirm it on a period it was not selected on before
> believing it.

### `[portfolio.Validation]`

Splits the run into a segment the parameters were chosen on and a segment
they were not.

```toml
[Portfolio.Validation]
  in_sample_end = "2021-01-01"   # strictly inside [StartDate, EndDate]
```

Each result then carries two `SegmentStats` — in-sample first — with each
segment's own compounded return, CAGR, Sharpe, Sortino, max drawdown and
standard deviation, plus the trading days it actually spans. The full-window
metrics are unchanged.

Both segments are **sliced out of the one simulation that already ran**, not
produced by running the backtest twice. A second run starting at the split
date would begin out-of-sample with a flat balance and no positions, which is
a different experiment: every strategy that holds across the boundary would be
measured as if forced to liquidate and re-enter there.

Each segment's max drawdown is measured within itself, so the in-sample half's
peak never anchors the out-of-sample half — a segment's drawdown is what
someone starting on its first day would have lived through.

A split date outside the window, or on either boundary, is rejected with an
error naming the dates. A split that is legal on the calendar but leaves fewer
than 30 trading days on either side is skipped with a log line, keeping the
backtest result: ratio metrics over a handful of days are noise quoted to two
decimals.

Turnover, `AvgCorrelation` and `CointegratedPairs` are deliberately not split
— the first needs per-segment traded notional, which is not tracked, and the
other two describe the tickers rather than the run.

> Use this with `[portfolio.Sweep]`. A sweep picks its winner using every day
> in the window, so the in-sample figure for that winner is the number that
> was optimised, not evidence. The out-of-sample column is the one to read.

### Overfitting statistics

Every result carries `Trials` — how many parameter sets the config block it
came from expanded to — plus two figures derived from it. They are computed
after the whole run, because the correction needs the spread of Sharpe ratios
across the trial group, and results are grouped by config block so two
unrelated sweeps never deflate each other.

| Field | Scale | Reading |
| --- | --- | --- |
| `Trials` | count | How many candidates the winner was chosen from. |
| `ExpectedMaxSharpe` | Sharpe | The Sharpe the *best* of those trials would be expected to show with no edge at all — the bar to clear. |
| `DeflatedSharpe` | probability, 0–1 | The chance the result's true Sharpe beats that bar. |

`DeflatedSharpe` is a **confidence, not a Sharpe**: 0.95 means the result
survives the correction, 0.10 means the same headline number is most likely
selection bias. Read it next to `ExpectedMaxSharpe`, which is on the familiar
Sharpe scale.

Why it matters, from the test suite: two hundred pure random walks with zero
expected return produce a best-of-N Sharpe of **1.69** — a number most people
would call a real strategy. Its `ExpectedMaxSharpe` is 1.98, so the winner
does not even clear the bar luck alone sets, and its `DeflatedSharpe` is 0.34.
Nothing in the raw Sharpe tells you that; the trial count is the missing
input.

An unswept portfolio is one trial in a group of one. Its bar is 0 and its
`DeflatedSharpe` is the plain probabilistic Sharpe — the chance its true
Sharpe is above zero given how many days and how fat-tailed the returns are.
No existing config's numbers change.

All three are available in `[Output] fields`, `filter` and `sort_by`. Sorting
a sweep by `SharpeRatio` and reading the top row is precisely the mistake
these exist to catch; sort by `DeflatedSharpe` instead.

Source: Bailey & López de Prado, *The Deflated Sharpe Ratio* (Journal of
Portfolio Management, 2014). The implementation lives in `metrics.go` as new
functions — no existing metric formula was touched.

### `[portfolio.WalkForward]`

Rolls the in-sample/out-of-sample split across the whole history instead of
splitting it once.

```toml
[Portfolio.Sweep]
  period = [7, 14, 21, 28]
[Portfolio.WalkForward]
  train_days = 252     # selection window, in TRADING days
  test_days  = 63      # scored window that follows it
  step_days  = 63      # defaults to test_days
  objective  = "SharpeRatio"   # or SortinoRatio, AnnualReturn
```

On each window the sweep's candidates are simulated over the training days and
the best by `objective` is selected; that winner is then run over the test days
it has never seen. The test segments are concatenated into one equity curve and
scored, and that curve is the result — it is the honest answer to "what would
this have done", because no day in it was ever used to choose anything.

**It requires a `[portfolio.Sweep]` with at least two combinations.** Nothing
in this engine is *fitted* — strategies take parameters rather than estimating
them — so selection is the fitting step, and with one candidate there is
nothing to select.

Windows are counted in trading days, so a schedule means the same thing across
periods with different holiday counts. `step_days` defaults to `test_days`,
the only value that tiles the scored segments with neither overlap nor gap.

Each window is reported: its train and test ranges, the parameter set selected,
that set's training score, what it then returned out of sample, and the capital
the window opened with. **The gap between training score and test return is the
diagnostic** — a schedule that scores brilliantly in training and poorly out of
sample is describing overfitting, and that is only visible with both.

Two properties worth knowing before reading the numbers:

- **Capital carries between windows; positions do not.** The curve is a real
  running balance, but each window starts flat, because a newly selected
  parameter set is not entitled to the previous one's holdings. That boundary
  liquidation is inherent to walk-forward and inflates turnover.
- **Each test window's first bar is the entry** and contributes no return, the
  same convention every run here uses. A 63-day test window therefore scores
  62 days.

`Trials` on the result counts every simulation the schedule performed —
candidates × windows — so the deflated Sharpe discounts it by the size of the
whole search, not just one window's.

### `[Output]`

An optional block that writes results to a file. Omit it and results are returned in memory only (the UI's path).

```toml
[Output]
path    = "results.csv"
format  = "csv"                              # "txt" (default), "csv", "json"
fields  = ["PortfolioName", "SharpeRatio"]   # emitted in this order
filter  = "SharpeRatio > 0.5 && AnnualReturn > 5"
sort_by = "SharpeRatio"
order   = "desc"                             # "asc" or "desc" (default)
limit   = 20                                 # 0 means unlimited
```

`filter` is a Go-style boolean expression over the same field names as `fields`.

## Running

The binary expects to be launched from `src/` because it resolves `../stock_data.db` and `../config.toml` relative to the working directory.

```bash
cd src
go run main.go                          # quiet run; logs are discarded
go run main.go -debug                   # writes backtester.log + transactions.log, and serves pprof on :6060
go run main.go -config ../strategy_library.toml
go run main.go -scan-signals             # screen signals instead of backtesting
```

To build a binary:

```bash
cd src
go build -o backtester
./backtester -debug
```

### Finding a strategy: screen signals first

A backtest is an expensive way to learn that a signal contains no information.
`-scan-signals` asks the cheap question first — does this signal rank tomorrow's
winners above tomorrow's losers, across the universe? — and only the ones that do
are worth writing a strategy around.

```bash
cd src && go run main.go -scan-signals
```

It reads the tickers and window from your config, screens a built-in library of
twelve signals at 5-, 21- and 63-day horizons, and prints them strongest first:

```
Signal scan: 30 tickers, 2637 trading days

mom_252               5d  IC +0.0338  t  +2.34  hit  55.3%  Q5-Q1   0.253%  n=476
mom_126               5d  IC +0.0276  t  +2.02  hit  53.3%  Q5-Q1   0.173%  n=501
sma_50_200           21d  IC +0.0502  t  +1.77  hit  57.4%  Q5-Q1   1.065%  n=115
lowvol_21            21d  IC -0.0374  t  -1.45  hit  42.7%  Q5-Q1  -1.062%  n=124
```

- **IC** is the cross-sectional Spearman rank correlation between the signal and
  the forward return, averaged over sampled days. **Real ICs are small** — 0.02
  to 0.05 with |t| > 2 is a genuinely usable signal, and anyone expecting a
  correlation will misread that as nothing.
- **hit** is the share of days with a positive IC. A good mean IC with a hit rate
  near 50% is a few lucky days, not an edge, and the two columns together catch
  that where either alone would not.
- **Q5-Q1** is the top-fifth minus bottom-fifth forward return: the IC in money.
  Withheld below 10 tickers, because a "quintile" of eight names is one stock
  wearing a portfolio's clothes.
- **n** is the number of **non-overlapping** samples. This is the honesty knob:
  scoring every day against a 21-day forward return reuses each return 21 times
  and inflates the t-statistic by roughly √21, so the scan steps by the horizon
  instead. It is why `n` is 115 rather than 2,600 at the 21-day horizon.

Signs are chosen so a **positive IC always means "high signal predicted high
return"** — hence `lowvol_21` is *negated* volatility and `rev_5` is *negated*
one-week return. The example above reads as: 12-month momentum and the 50/200
trend ratio had modest predictive power on those 30 large caps, and low
volatility did not — high-volatility names outperformed over that window.

Three refusals, all deliberate: fewer than 5 usable tickers on a day is not a
cross-section and the day is skipped; fewer than 12 non-overlapping samples is an
anecdote and no summary is printed at all; and a signal that is constant or
undefined produces no row rather than a `NaN`.

The signal library covers trailing return over 21/63/126/252 days, 12-1 momentum,
one-week reversal, realised volatility at 21 and 63 days, distance from the
52-week high, the 50/200 SMA ratio, a volume trend, and volatility-scaled
momentum. Each is a pure function of one ticker's bars **up to and including the
scored day** — a signal is handed an index and can never see the forward window
it is being judged against, which is asserted by a test rather than by
convention.

**A screen is not a backtest.** It ignores costs, position sizing and when you
could actually trade. A signal that survives here still has to survive a real run
with `[portfolio.Costs]`, an out-of-sample split and walk-forward.

### What happens after X? Event studies

"Buy after a 10% drawdown", "buy the breakout", "fade a three-sigma day" — a
large class of strategy ideas, answerable in seconds without writing one:

```bash
cd src && go run main.go -event-study
```

It finds every occurrence of each built-in event across your universe and reports
what followed, at 5, 21 and 63 days. Real output, 30 large caps over 2016–2026:

```
drawdown_20  63d  n= 214  abnormal  +0.97% (median +0.39%)  raw  +8.12%  t +0.87  hit 51.4%
drop_3sigma  21d  n= 760  abnormal  -0.24% (median -0.59%)  raw  +1.41%  t -0.96  hit 45.9%
high_52w      5d  n=2016  abnormal  +0.09% (median +0.06%)  raw  +0.29%  t +1.16  hit 51.2%
```

Read the first row carefully, because it is the whole point of the feature.
"Buy the 20% dip" returned **+8.1% over the next quarter** — and **+0.97% net of
what the rest of the universe did over the same days**, with a t of 0.87 and a
hit rate barely over half. The strategy is almost entirely the market recovering,
not the dip. A raw-return study would have called that an edge.

- **abnormal** is the return after the event net of the equal-weight return of
  the universe over the same window. It is the number that matters; `raw` is
  shown beside it so the gap is visible.
- **n** counts **non-overlapping** occurrences. A drawdown that persists fires
  on twenty consecutive days, and counting each as independent evidence inflates
  everything — one occurrence per ticker per horizon.
- **median** is reported because one 400% recovery can carry a mean on its own.
- Below 10 occurrences nothing is reported at all: a confident mean over a dozen
  events is how a backtest lies.

Built-in events: `drawdown_10`, `drawdown_20` (crossing 10%/20% below the 1-year
high — the *crossing*, not the state), `high_52w`, `drop_3sigma`, `jump_3sigma`
(a day beyond three times its 1-month volatility) and `volume_spike`.

Earnings and macro-release events are deliberately absent for now. They need a
database read inside the run path and, more importantly, the publication-lag
rules — an earnings event dated at the fiscal period end rather than the report
date would make every study of it look wonderful and mean nothing.

### Desktop UI

```bash
wails dev -tags webkit2_41              # live-reloading development build
wails build -tags webkit2_41 && cp build/bin/ui ui/ui
```

## AI assistant (UI) — Claude or local Ollama models

The Wails UI has a built-in AI chat, toggled with the **✦ Assistant**
button in the topbar. It is specialized for this app: its system prompt
carries the TOML config schema, the strategy spec strings, the full Lua
strategy API, the metric definitions, the live schema of the chosen DuckDB
(every table, bar counts, date span, available `$`-benchmarks), the saved
`*.toml` configs and `strategies/*.lua` library, and whatever TOML/Lua is
open (even unsaved) in the editors.

- **Models**: the header dropdown lists the Claude models (Sonnet 5 /
  Opus 4.8 / Haiku 4.5) plus every model your local Ollama server has
  pulled (labelled "(local)", discovered live from `/api/tags`;
  `OLLAMA_HOST` overrides the default `localhost:11434`). The choice is
  remembered across restarts.
- It can query the database itself through a read-only SQL tool
  (SELECT-family statements only, capped at 100 rows) — e.g. to check
  ticker coverage before proposing a date range. Queries it runs are shown
  inline in the chat. Local models get the tool too if they advertise the
  `tools` capability (qwen3.5 does); models that don't simply chat over
  the embedded context.
- Fenced ` ```toml `/` ```lua ` blocks in its replies get an
  **Insert into … editor** button, so a generated config or strategy lands
  directly in the corresponding editor tab.
- Setup: Claude models need an Anthropic API key — set
  `ANTHROPIC_API_KEY` in the repo-root `.env` (or the environment), or
  paste it into the panel's first-run form, which writes that `.env`
  entry for you. Local Ollama models need no key at all.
- Local-model notes: the request pins `num_ctx` to 16384 (Ollama's default
  is too small for the embedded context) and sends a leaner system prompt
  (the saved-config dump is skipped); "thinking" models have thinking
  disabled for snappier replies.

Backend: `ui/chat.go` (shared prompt/tool/dispatch), `ui/claude.go`
(streaming Anthropic client), `ui/ollama.go` (streaming Ollama client),
`data.RunQuery` in `src/data/database.go`; frontend:
`ui/frontend/src/ChatPanel.tsx`. Live tests: `go test ./ui` (the Ollama
ones skip when no server is running).

## Output

- **stdout / `backtester.log`** — query timings, debug info, and per-portfolio metrics when `PrintMetrics` is invoked.
- **`transactions.log`** (debug only) — every `BUY` / `SELL` and the day's percentage change.
- **the `[Output]` file** — one record per portfolio in txt, csv, or json, honouring `filter`, `fields`, `sort_by`, `order`, and `limit`. Without an `[Output]` block nothing is written to disk.
- **pprof** (debug only) — `http://localhost:6060/debug/pprof/` for CPU and heap profiling.

Reported metrics per run:

- `SharpeRatio` — annualized, using the per-day risk-free rate from `3MTreasuryYields`.
- `SortinoRatio` — annualized, downside-deviation denominator.
- `MaxDrawdown` — peak-to-trough drawdown of the daily close-value series, as a percent.
- `AnnualReturn` — CAGR derived from the compounded daily return series.
- `StandardDev` — annualized stdev of daily returns.
- `Turnover` — annualized gross traded notional as a multiple of the portfolio's average value; `1.0` means it traded its own value once over a year. Both sides of a round trip count. Reported whether or not `[portfolio.Costs]` is set.

With a `Benchmark` configured, additionally:

- `Alpha` — annualized return in percent beyond what the portfolio's beta exposure explains.
- `Beta` — slope against the benchmark; `1.0` means it moved one-for-one.
- `TrackingError` — annualized stdev of the return difference, as a decimal.
- `InformationRatio` — annualized mean active return over tracking error.
- `UpCapture` / `DownCapture` — percent of the benchmark's move captured on its up days and its down days. For down-capture, lower is better.

Each result additionally carries time-sliced views of the same run, available to the UI (they are nested, so the table-shaped file reporter does not carry them):

- **Drawdowns** — the ten deepest peak-to-trough declines, with the peak, trough and recovery dates and the days spent in each. A decline still underwater on the final day is reported as ongoing rather than omitted.
- **Rolling Sharpe** — annualized Sharpe over each trailing 252-day window, dated at the window's last day.
- **Calendar returns** — compounded return per year and per month. The first and last periods are usually partial; they are marked as such and reported as the plain return over the days present, never annualized.

## Adding a strategy

Lua is the intended route — no rebuild, and the engine handles the day loop.

1. Copy a file from `strategies/` and edit it. A strategy is one global `step(day)` function, called once per trading day with `day` counting from 0; the engine provides `price()`, `sma()`, `rsi()`, `cash()`, `position()`, `buy()`, `buy_max()`, `sell()`, `sell_all()`, and the `tickers` / `params` globals. Full API, patterns, and gotchas: [`strategies/README.md`](strategies/README.md).
2. Point a portfolio at it with `Strategy = "lua:strategies/your_file.lua"` and pass any settings in a `[portfolio.Params]` block.
3. Add it to `shippedStrategies()` in `src/backtest/strategies_library_test.go`, which runs every shipped strategy against synthetic data and fails if it errors, overspends, or never trades.

The Go path is the lower-level alternative — worth it only for something that needs engine internals or has to be fast enough to matter in a large sweep:

1. Implement the `Strategy` interface (`Name()` and `Step(p, hist, day)`) in `src/backtest/strategy.go`, as `BuyAndHold` and `SMACross` do. `Step` calls `Buy` / `Sell`; the runner's day loop takes care of `AdjustPortfolioParameters` and the metrics.
2. Add a case for its spec string to `NewStrategy`. The runner is not hard-coded to any strategy — it builds whatever `NewStrategy` returns for the portfolio's `Strategy` field, so nothing in `runner.go` needs to change.
3. Set `Strategy = "<your spec>"` on a portfolio in `config.toml`.

Per-strategy state lives on the implementing struct, so a `Strategy` instance is single-use; the runner clones the portfolio (rebuilding the strategy from its spec) for each pass.

## Project layout

```
.
├── config.toml              # portfolio definitions consumed at runtime
├── stock_data.db            # DuckDB file (OHLCV + risk-free rates)
├── go.mod / go.sum
├── *.py                     # data loaders (add_collections, fetch_history,
│                            #   add_ticker, update_treasury_yields, …)
├── strategies/              # Lua strategy library + its API reference
│   ├── README.md
│   └── *.lua
├── src/
│   ├── main.go              # CLI entry point
│   ├── backtest/
│   │   ├── config.go        # TOML loading + Portfolio construction
│   │   ├── portfolio.go     # Portfolio / Position state and trade execution
│   │   ├── runner.go        # worker pool, data prefetch, result collection
│   │   ├── strategy.go      # Strategy interface, NewStrategy, BuyAndHold,
│   │   │                    #   SMACross, SMA/RSI helpers
│   │   ├── strategy_lua.go  # Lua strategy host and API surface
│   │   ├── metrics.go       # Sharpe, Sortino, drawdown, CAGR
│   │   ├── reporter.go      # txt/csv/json output, filtering and sorting
│   │   └── correlation.go   # cross-asset correlation
│   └── data/
│       └── database.go      # DuckDB queries and read-only RunQuery
└── ui/                      # Wails desktop app
    ├── app.go               # bound methods (run backtests, load/save configs)
    ├── chat.go              # assistant prompt, tools, dispatch
    ├── claude.go            # streaming Anthropic client
    ├── ollama.go            # streaming Ollama client
    ├── query.go             # SQL console backend
    ├── quote.go / fetch.go / screen.go
    └── frontend/src/        # React: App, ChatPanel, QueryConsole,
                             #   ResultsView, SimpleForm
```
