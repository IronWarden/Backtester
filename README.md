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
with the day it became knowable: the real `earnings_calendar` report date
where one exists (plus one day, since those land after the close), and period
end + `FixedReportLagDays` (90) otherwise. Each point records which rule
applied, so a result resting mostly on the fallback can be read accordingly.
The helper also deduplicates — the raw table contains exact duplicate rows.

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

### A holding whose data ends before the window does

Worth knowing before you load any delisted history, because it is the rule that
decides what such a backtest means. **A day is simulated only when every one of
the portfolio's tickers has a bar for it.** So a holding whose series ends
mid-window — a delisting, an acquisition, or just a stale feed — ends the run
for *every* holding on its last bar, and the reported metrics describe that
shorter window rather than the one the config asked for. The same rule applies
to a single missing day inside one ticker's history: that day is dropped for all
of them.

The run now says so, naming the ticker and the date:

```
portfolio "Tech Giants": simulated 1,258 of the 2,517 trading days its window
covers (through 2025-03-31) — a day is only simulated when every ticker has a
bar for it; "SIVB" ends 2023-03-10, so the run was truncated there
```

Two things it does *not* do, both deliberate: it does not liquidate the dead
holding and carry the survivors to the end of the window, and it does not fail
the run. Either would change the numbers of every existing config that mixes
histories of different lengths. Note also that `validateCoverage` rejects a
portfolio outright when an explicit `EndDate` runs past a ticker's last bar, so
today a delisted name is unusable rather than merely truncating — the message
names the ticker and its real range.

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
```

To build a binary:

```bash
cd src
go build -o backtester
./backtester -debug
```

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
