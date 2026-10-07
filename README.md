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

## Version, platform, and what a release contains

The build names itself: `Backtester 0.1.0-beta.1 · linux/amd64` beside the app's
name in the header, and from the CLI:

```bash
cd src && go run . -version
```

Quote it in a bug report — a report that cannot name a build is very hard to
act on. The constant lives in `src/version/version.go` and nowhere else;
`package.sh` reads it, and the release script stamps the commit in with
`-ldflags "-X my-backtester/src/version.Commit=$(git rev-parse --short HEAD)"`.
`CHANGELOG.md` says what each release changed.

**The beta is Linux only.** The desktop build needs `-tags webkit2_41`; the
`ui/build/darwin` and `ui/build/windows` directories are Wails scaffolding that
has never been built or tested, and a report from either is answered
differently from one that says `linux/amd64`.

**If the window feels sluggish, try GPU rendering.** Wails paints the Linux
webview on the CPU by default, because acceleration leaves a blank or crashing
window on some NVIDIA/Wayland stacks. `BACKTESTER_GPU=on ./backtester` (or
`auto` for WebKit's on-demand policy) turns it on for one launch; unset or
`off` keeps the safe default. If the window comes up blank with it on, also try
`WEBKIT_DISABLE_DMABUF_RENDERER=1`.

`./package.sh` assembles a release directory. What ships and why:

| file | why it has to be there |
|---|---|
| `backtester` | the binary, with the frontend and the Lua library embedded |
| `LICENSE` | the software's terms, which end at 1.0 |
| `DATA-LICENSE.md` | the data's terms, deliberately stricter than the software's, plus the defect inventory |
| `THIRD-PARTY-NOTICES.txt` | **required**: shipping the binary without it breaks the MIT and BSD terms of everything it links |
| `CHANGELOG.md`, `README.md` | what changed, and how to use it |
| `strategies/` | readable, editable copies — the binary carries its own, and an on-disk script of the same name wins |
| `stock_data.db` | only with `--with-db`, since it is ~2.9 GB and a package is usually assembled several times before it is right |

## Prerequisites

- Go 1.25+ (module pinned to `go 1.25.0`).
- A DuckDB file named `stock_data.db` in the repository root (or any file you
  open with the app's database picker) containing:
  - `stock_data_optimized(Date, Ticker, Open, High, Low, Close, Volume)`
  - `"3MTreasuryYields"(Date, daily_risk_free_rate_decimal)`

  Don't have one? [Build your own](#build-your-own-database) with one command.
- A `config.toml` in the repository root (see below).

Dependencies (`github.com/duckdb/duckdb-go/v2`, `gonum.org/v1/gonum`, `github.com/BurntSushi/toml`, `github.com/yuin/gopher-lua`, and the Wails runtime) are pulled via `go mod`.

**Keep every DuckDB that writes the file on the same version line.** The app
bundles DuckDB **1.5.x** (`duckdb-go` v2.10505). The Python loaders use
whatever `pip` installed, and a newer DuckDB can write storage an older one
cannot read — on 2026-09-27 a loader run under Python DuckDB 1.5.0 left a
database that the app's previous engine (1.1.3) and a 1.3.2 CLI could read one
ticker at a time but failed on any scan across tickers, with an `INTERNAL
Error: Attempted to access index 9223372036854775807`. The data was intact;
only the older readers were stuck. So: use `pip install "duckdb>=1.5,<1.6"`
for the loaders, a 1.5 CLI for inspection, and upgrade all three together.

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

### Where the app looks for its files

Nothing has to be launched from a particular directory. Both binaries resolve
the strategy library, the database and `config.toml` through `src/apppath`,
which searches in this order and takes the first hit:

1. the working directory, then its parent, then its parent — the walk this
   project has always done, so a checkout behaves exactly as it did (the CLI
   runs from `src/`, the desktop app from `ui/`, and `../strategies` is right
   for both);
2. `~/.config/backtester` — the per-user directory, where a packaged app saves
   a script and the API key;
3. the executable's own directory and its parent, with symlinks resolved, so a
   binary installed under `/opt` or reached through `$PATH` finds what was
   installed beside it.

The **strategy library is embedded in the binary** (`strategies/embed.go`, the
way `ui` embeds `frontend/dist`), so `Strategy = "lua:strategies/rsi.lua"`
resolves even when no `strategies/` directory exists anywhere. The embedded
copy is always the last answer: a script on disk with the same name wins.
Saving a script from the Lua tab writes to the on-disk library when there is
one and to `~/.config/backtester/strategies/` when there is not — where the
engine then finds it. **The shipped scripts themselves are never written**:
the Lua tab opens them read-only, and `WriteFile` refuses one outright
(`strategies.IsLibrary`). Your own scripts are the ones named `my_…`.

The database is the one thing that can genuinely be missing. The UI asks Go
where it is (`DefaultDBPath`) instead of assuming `../stock_data.db`; when
nothing is found the header reads **Choose DB…** rather than naming a file that
is not there.

### How the database is opened

DuckDB is single-writer *and* single-opener, and a shipped database is often
not writable at all, so `data.InitDB` tries three things in order and
`data.Status()` reports which one happened:

| what it finds | what it does | `Status().Mode` |
|---|---|---|
| a file it can write (every checkout) | opens it in place — unchanged | `read-write`, no notice |
| a file it cannot write (`/usr/share`, `/opt`, an AppImage mount, `chmod 444`) | copies it once to `~/.local/share/backtester/` and opens the copy | `read-write`, with a notice naming the copy |
| that, but the copy is impossible too | opens the original with `access_mode=read_only` | `read-only`, with a notice saying why |

Nothing is ever copied out from under a database you own: the copy happens
only on the branch where the file refused a read-write open. An existing copy
is reused whenever the source it was made from is unchanged — tracked by a
`<name>.db.source` stamp beside it, so bars you fetch *into* the copy are not
wiped by the next launch.

Read-only is a real mode, not a failure: everything reads normally and only
writing stops. `UpsertBars` refuses up front with the reason rather than
letting DuckDB complain about the attach mode halfway through.

If another program already holds the database — a second window, a CLI run, a
Python loader — DuckDB refuses the attach in both modes. The app turns its
message into a sentence naming the program and PID to close, instead of
showing `IO Error: Could not set lock on file …: Conflicting lock is held in
…` at the user. In the desktop app the notice arrives through
`DatabaseNotice`, which the header calls whenever the database changes.

## Build your own database

The database this project was developed against cannot be shared — its prices
come from Yahoo, whose terms forbid redistribution (see `DATA-LICENSE.md`). So
anyone else builds their own, from the same free sources, with one command:

```bash
pip install -r requirements.txt            # needs DuckDB 1.5.x, like the app
python3 build_db.py --db my.db --dry-run   # show the plan, download nothing
python3 build_db.py --db my.db             # build it (a few minutes)
```

Then open `my.db` with the app's database picker, or name it `stock_data.db`
and put it in the repository root. **No API keys are needed**: prices come from
Yahoo via `yfinance`, rates and macro series from FRED's public CSV download
(`fred_public.py`), and everything else from SEC EDGAR and Wikipedia.

`--universe` chooses what to download, and the choices combine:

| universe | what | how long |
|---|---|---|
| `starter` (default) | ~80 well-known stocks and ETFs: the broad index, bond, sector and factor funds, gold, and large companies across sectors | a few minutes |
| `sp500` | today's S&P 500 members | ~15–30 minutes |
| `us-stocks`, `us-etfs`, `us-all` | every US-listed common stock and/or ETF, from NASDAQ's official symbol directory (~5,800 of each) | hours — Yahoo will rate-limit you; re-run to resume |
| `@file.txt` | your own list, one symbol per line (`#` comments allowed) | |
| `AAPL KO BRK.B` | just these | |

`SPY` is always included: the benchmark step builds the market calendar from it.

The build runs these steps in order; `--steps` picks a subset, and every one is
safe to re-run:

| step | what it writes | source |
|---|---|---|
| `prices` | `stock_data_optimized` — daily split- and dividend-adjusted OHLCV. Batched (`--batch`, default 50 symbols per request), and **resumable**: tickers already present are skipped unless `--refresh`. `--start` limits history. Symbols Yahoo has no data for are listed at the end, not fatal. | Yahoo |
| `rates` | `"3MTreasuryYields"` and `"10YrTreasuryYields"`, 1981+ / 1962+ | FRED `DGS3MO`, `DGS10` |
| `macro` | `economic_indicators` — CPI, fed funds, GDP, industrial production, M2, unemployment | FRED |
| `benchmarks` | `index_levels` (the S&P 500 price index back to 1927, from `^GSPC`), then the [`$` series](#long-history-benchmark-assets) via `add_collections.py`, then `extend_risk_free.py` to carry the risk-free rate back to 1928 | Yahoo, FRED, Shiller, Ken French |
| `sectors` | `company_profile` for the tickers you have | SEC EDGAR |
| `index` | `index_membership` — point-in-time S&P 500 membership | Wikipedia |
| `quality` | `data_quality` — the per-ticker defect audit | computed |
| `fundamentals` | `sec_financials`, 2009+ — **not in the default set**: `--with-fundamentals` or `--steps fundamentals` (~7 GB of downloads, ~30–40 minutes) | SEC EDGAR |

A failed step is reported and the rest carry on; the summary names what to
re-run. The build ends by scanning every ticker the way the app does — a
cross-ticker `GROUP BY`, which is the query that exposed the one real storage
incompatibility this project has had — and prints what the database holds.

Two limits carry over unchanged from the original database. The universe holds
**only companies still trading** (Yahoo serves no history for delisted
symbols), so a backtest that picks among them is picking among survivors. And
the delisting tables (`delistings`, `delisting_reasons`) are not built: the
first needs an Alpha Vantage key, so run `add_delistings.py` yourself if you
have one.

`build_db.py` refuses to write with a Python DuckDB outside 1.5.x
(`--any-duckdb` overrides), for the reason given under
[Prerequisites](#prerequisites).

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
correctness check on both sources. Metrics use the same vocabulary — 30 of
them, listed in `src/data.SECMetrics`: the income statement (`Total Revenue`,
`Cost Of Revenue`, `Gross Profit`, `Operating Income`, `Net Income`, EPS, …), the
cash flow statement (`Operating Cash Flow`, `Capital Expenditure`, dividends,
buybacks) and the balance sheet (`Total Assets`, `Current Assets`, `Current
Liabilities`, `Stockholders Equity`, `Long Term Debt`, `Goodwill`, …). A strategy
reads it by setting `FundamentalSource = "sec"` — see [Fundamentals in a
strategy](#fundamentals-in-a-strategy). From Go, read it with
`src/data.LoadSECFundamentals` and reduce it with `FirstKnown` / `KnownOn`, or
take point-in-time figures straight from `SECPointInTimeFundamentals`.

Where filers disagree on which XBRL tag carries a figure, several tags map to one
metric and each row records its `tag_rank` (0 = preferred). Two fallbacks matter
more than the rest: companies with noncontrolling interests — PG, CAT, VZ, JNJ
among them — tag balance-sheet equity only as total equity *including* those
interests, so that stands in for `Stockholders Equity` when nothing better was
filed (it reads somewhat high where minority interests are large); and IBM and
CAT tag net income as `ProfitLoss` or `NetIncomeLossAvailableToCommonStockholdersBasic`
for years at a time, which stand in for `Net Income` the same way.

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
parse. A full run (measured 2026-10-03) is 69 quarters, ~7 GB of transfer,
~15–20 minutes and ~20 M rows, which adds ~0.55 GB to the database; quarters are
streamed and inserted one at a time so the disk cost is the table, not the
archive. Only 10-K and 10-Q are read (amendments are skipped); segment and
subsidiary rows are skipped to avoid double-counting the parent; year-to-date
partials are kept under their own `year_to_date` frequency, never labelled as a
quarter. SEC publishes
each quarter a few months in arrears — 2026Q1 was the newest available on
2026-08-17 — and a missing quarter is reported and skipped.

Two further caveats before building anything on this:

- **`economic_indicators` has the same trap.** `Date` is the period, and CPI
  for January is published in mid-February and revised for years. Lag it
  before using it in a decision. Regime signals computed from prices
  (drawdown depth, realised volatility) need no lag.
- **`financials` begins 2020-07** — five and a half years containing one
  inflation shock and one hiking cycle, which is a single macro regime. Treat
  factor results over that window as hypothesis-generating, not evidence.
  `FundamentalSource = "sec"` is the way past it: 2010 onward for most
  companies, since XBRL filing phased in through 2009–2011.

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
| `renamed` | the dates look recycled, but the delisted name is one the filer trading today used to have | yes |
| `unknown` | no registry entry | yes |

A series that **begins after** its delisting date is `clean` whatever its last
bar says: the price table simply has no bars for the dead company, so there is
nothing spliced to worry about. That rule alone reclassifies 149 of the symbols
a last-bar-only test called `recycled`.

`renamed` exists because **the registry cannot tell a death from a name change**.
Alpha Vantage records a symbol leaving an exchange the same way whether somebody
else took it over or the company simply renamed itself, so on dates alone `DD`
looks identical to a recycling — when "DowDuPont Inc" is a *former name* of the
filer now called DuPont de Nemours, same CIK. `src/data.MatchRename` corroborates
a condemning verdict against `company_profile`'s current and former names, using
the same normalisation and tier order as `symbol_match.go` so that the audit and
`split_recycled.py` never disagree about which symbols are recycled. On the
loaded database that clears **74 symbols** that would otherwise be untradable.

With no `company_profile` table nothing is corroborated and every verdict is
what it was before — the same inert-when-absent rule as every other optional
dataset here. The matching errs toward `renamed`, because wrongly condemning a
continuous series costs a usable ticker while the opposite leaves things as they
already are; its known cost is an acquirer that takes its target's name, and
`COHR` is the live example (Coherent Inc was bought by II-VI, which renamed
itself Coherent Corp).

`Coverage.Trustworthy()` collapses that to a boolean, and
`UntrustworthyTickers` names the offenders. `unknown` is the absence of
evidence, not a clean bill of health — see the coverage limit below.

### Splitting a recycled series

A recycled series is repaired by separating the companies that actually traded
under the symbol. The convention: **the company trading today keeps the bare
symbol, and the previous occupant takes a generation suffix**, numbering from
the oldest. `COR` keeps meaning Cencora, which is what every existing config
already meant by it, and CoreSite Realty becomes addressable as `COR.1` rather
than deleted.

`src/data.ParseGeneration` splits `COR.1` into (`COR`, 1) and leaves a bare
symbol at generation 0; `GenerationSymbol` is its inverse. A suffix that is not
a positive integer is not a generation, so `BRK.B` is never mangled into `BRK`.
The separator is the single constant `src/data.GenerationSeparator`.
`DelistingCoverage` resolves a generation symbol to its base company's death
date, so both halves of a split pair read `clean` while the unsplit series
still reads `recycled`.

```bash
python3 split_recycled.py            # dry run: classify and print, write nothing
python3 split_recycled.py --apply    # rewrite the bars
```

**The join is the hard part, not the split.** A delisting date alone does not
prove the later bars are a different company — a symbol can be delisted from one
exchange and relisted as the same company, and a company can simply change its
name. So every candidate is corroborated against `company_profile` before
anything is rewritten, into three buckets:

| Bucket | Meaning | Action |
| ------ | ------- | ------ |
| `split` | the delisted name matches neither the current filer's name nor any former name | rewritten |
| `same_company` | it matches one of them — a rename or a relisting | left alone |
| `review` | no `company_profile` row, so nothing to corroborate against | left alone |

As of 2026-08-20 that is 48 / 57 / 15 of the 120 genuinely spliced symbols.
`DD` is in `same_company`, not `split`: "DowDuPont Inc" is a *former name* of
the filer now called DuPont de Nemours, so DD is one company that renamed
itself, not two companies sharing a symbol.

The matching errs toward `same_company` on purpose, because under-splitting is
recoverable and wrongly splitting a continuous company is not. Its known cost:
when an acquirer takes its target's name the two names match and the series is
left spliced — `COHR` is the live example. The `same_company` list is evidence,
not proof; read it.

The script rewrites `stock_data_optimized`, the only price table the engine
reads. The legacy `stock_data` table is deliberately left untouched.

### Shorting

Off by default. With `AllowShort` absent the engine is long-only exactly as it
was, and every config written before shorting existed produces identical
numbers.

```toml
[[portfolio]]
AllowShort = true
```

With it on, `sell(ticker, n, price)` past the holding closes the long first and
shorts the remainder, and `short(ticker, n, price, day)` says it directly —
a **target**, not a delta, so after the call the position is exactly `-n`,
reached from a long as readily as from flat, and `short(t, 0, …)` goes flat.
`cover(ticker, price, day)` buys the whole short back and is a no-op on a long;
`sell_all` closes a position whichever way it points. **Market-neutral needs no
new API**: a negative weight in `target_weights(day, {A = 0.5, B = -0.5})` is a
short leg. A short order on a portfolio without `AllowShort` is refused *and says
so*, including a negative `target_weights` leg — which would otherwise leave the
book at half the size the strategy asked for with nothing to show for it.
A short's `Amount` is negative and its `AveragePrice` is the price it was
shorted at; valuation adds `Amount * close`, which subtracts the cost of buying
back, while the proceeds sit in cash. Covering realises `AveragePrice - fill`,
the mirror of a long, and only the closing portion of an oversized sell realises
anything.

Exposure becomes **gross** — long plus short, absolute — rather than
everything-that-is-not-cash, because a short raises cash while adding exposure.
A market-neutral book reports its real exposure instead of zero.

#### Borrow cost

Shorts are not free to hold.

```toml
[[portfolio]]
AllowShort = true

  [portfolio.Costs]
  borrow_bps_annual = 50
```

`borrow_bps_annual` is the annual borrow rate in basis points of the short's
**marked** value, accrued once per trading day over a 252-day year and charged
straight to cash *before* the strategy acts — the borrow is owed on what was
held overnight, not on what today's orders leave behind. It is charged only on
negative positions, so it is inert for a long-only book whatever it is set to,
and `0` (the default) charges nothing.

It never appears in the blotter: no shares change hands, and a trade record
would corrupt the trade count, win rate and holding period. The `BorrowPaid`
metric is the total charged over the run. **A short-side edge that `BorrowPaid`
eats is not an edge**, and nothing else in the result would show it.

One flat rate is a deliberate simplification. Real borrow is per-security and
moves daily — general collateral is a few tens of basis points, a hard-to-borrow
name can be hundreds or thousands — so a strategy shorting crowded or small
names is under-charged here, possibly by an order of magnitude.

#### Margin

A short must be backed by equity, checked once to open the position and every
day to keep it.

```toml
[[portfolio]]
AllowShort = true

  [portfolio.Margin]
  initial_pct     = 0.50
  maintenance_pct = 0.30
```

Both are fractions of the short's **current marked value**, not of the price it
was opened at — which is what makes a breach possible with the strategy doing
nothing at all: the short moves against you, so the requirement grows while the
equity backing it shrinks.

**Zero means "use the default", not "no requirement."** This is the one knob in
this config where an unset value is not the inert value, because a zero margin
requirement is unbounded leverage. The defaults are Regulation T's 50% initial
(`backtest.RegTInitialMargin`) and 30% maintenance
(`backtest.DefaultMaintenanceMargin`) — exchange minimums are nearer 25–30% and
brokers commonly set 30–35%, so 30 is a conservative middle and a *default*, not
a fact about any broker. A maintenance requirement above the initial one is
rejected: every short would be called the day it was opened.

A breach **forces a cover** whether the strategy wanted it or not, and covers the
minimum that restores the requirement rather than liquidating the book. Covering
is equity-neutral, so covering notional `x` leaves equity `E` alone and cuts
gross short `S` to `S - x`; the requirement `E ≥ m·S` is met exactly when
`x ≥ S − E/m`. If the book is *still* in breach afterwards — costs come out of
equity — it is covered entirely. A forced cover may overdraw the account: a
margin call is not a discretionary order, and a book whose equity has gone
negative goes into debit rather than staying in breach.

The `MarginCalls` metric counts the days this fired. **A non-zero value means
the result contains trades the strategy never chose**, which is usually the most
important thing about it.

### What a holding is worth when the company dies

The engine marks every position at its last printed close. For an acquisition
that is about right — the tape's final price is roughly what the holder was
paid. For a bankruptcy it is fiction: the last trade before a Chapter 11
delisting is not what the shareholder got, which was usually nothing.

```toml
[[portfolio]]
DelistingReturn = -0.30
```

`DelistingReturn` is the fraction of a position **lost** on the delisting day.
It must be in `[-1, 0]`; anything else is rejected by name rather than clamped.
**Zero is the default and applies nothing at all**, so every config written
before this produces identical numbers. The researched values are exported as
`backtest.ShumwayNYSEReturn` (−0.30, Shumway 1997) and
`backtest.ShumwayNasdaqReturn` (−0.55, Shumway & Warther 1999).

**It applies only to performance-related delistings** — a bankruptcy, or a
compliance removal by the exchange. An acquisition keeps its last close, because
haircutting acquisitions would be the larger error: of the delistings whose
reason is known, the majority *are* acquisitions. The reason comes from
`delisting_reasons` joined to the registry by `src/data.MatchDelistingSymbols`,
which resolves about a quarter of symbols.

A delisting whose reason could **not** be identified is deliberately left alone
and counted instead:

| Metric | Meaning |
| ------ | ------- |
| `DelistedHoldings` | positions liquidated at the haircut because the company failed |
| `UnclassifiedDelistings` | positions still open at a delisting whose reason is unknown — **not** haircut, so a non-zero value means the result is optimistic by however many of them were failures |

The liquidation runs through the normal sell path, so it lands in the blotter,
the realised P&L and the per-ticker attribution rather than being a silent
mark-down. It fires only on the final simulated day, because that is the only
reachable case: `validateCoverage` refuses a portfolio whose window runs past a
ticker's last bar, so a holding cannot die mid-run — it can only still be held
when the run ends on or after the day it died.

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
do not, so the table alone answers "what happened to this company" and not
"what happened to the symbol my backtest held".

`src/data.MatchDelistingSymbols` closes that gap by name-matching
`delisting_reasons.company_name` against `delistings.name`. It is pure — both
sides go in as slices, no database — and it refuses to guess in two ways that
matter more than the matching does:

- **The dates must agree.** A candidate whose delisting date is more than
  `MaxDateGapDays` (30) from the registry's is rejected at any similarity. Two
  companies can share a name; the same company leaving on the same date is much
  stronger evidence than the name alone.
- **Ambiguity is not a match.** When two filers tie at the same tier the result
  is `ambiguous`, recording every contender and choosing none. A wrong join
  silently attributes a bankruptcy to the wrong symbol, which is worse than no
  join — an unmatched symbol simply degrades to `unknown`.

Each match reports which rule fired, so callers can weigh them:

| `MatchedBy` | Rule |
| --- | --- |
| `exact` | normalised names identical, suffixes intact |
| `suffix` | identical after stripping INC/CORP/CO/LTD/LLC/PLC/HOLDINGS/GROUP |
| `fuzzy` | edit-distance similarity ≥ `MinSimilarity` (0.90) |
| `ambiguous` | two or more filers tied — recorded, not resolved |
| `none` | nothing cleared both the name and the date gate |

`MatchedBy.Resolved()` is true only for the first three.
`ReasonsByMatchedSymbol` then produces the payoff — symbol → reason — dropping
everything unresolved. Prefer `ReasonsBySymbol` where EDGAR listed a ticker
itself; that path is exact but empty for most delisted filers, which is why
this one exists.

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
**A run now says so.** Before the workers start, every holding is looked up in
the audit and each `unusable` or `suspect` one produces a warning diagnostic
naming the defect — *"VHI cannot support a backtest according to the data
audit: 5,429 bars priced at or below zero, so any return computed across the
sign change is arithmetic nonsense that reads as an ordinary extreme move.
Nothing is excluded — this run includes it, and its numbers include whatever
the bars do."*

**Nothing refuses to trade an `unusable` series**, and that is deliberate:
excluding one changes the numbers every existing config produces, and an
engine that silently drops a holding is worse than either alternative.
Refusing is a 1.0 decision, to be made once there is feedback from people who
are not the author. Benchmarks are not checked — nobody chose the bars behind
`$SP500`, and a warning you cannot act on is noise. A ticker the audit has
never seen is unexamined rather than condemned, and a database with no
`data_quality` table says so once per session and carries on.

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

## Keeping the price data current

A backtest is only as current as its last bar, and a database that has sat for
a month quietly costs you the most recent window of every run. Refreshing it
used to mean asking the assistant, which drives `fetch_history` three symbols
at a time — fine for one ticker, hopeless for ten thousand — or running
`add_ticker.py` in a loop with the app closed.

`ui --sync <db>` refreshes every tradable ticker in one command. It is a
terminal step, run with the app **closed** — DuckDB is single-writer, and the
app holds the database while it is open. There is no sync button in the app
and no sync tool for the assistant; both were removed so refreshing the data
is a deliberate step rather than something that happens mid-session.

```bash
ui --sync /path/to/stock_data.db --dry-run   # how far behind is it?
ui --sync /path/to/stock_data.db             # bring it up to date
```

It prints one progress line per batch and a summary, exits non-zero if the run
was cut short, and treats Ctrl-C as a clean stop — the batch in flight is
lost, everything already stored is kept.

Two things make a full refresh affordable:

- **Only the missing tail is fetched.** Each ticker starts the day after its
  last stored bar, so nothing already held is downloaded twice.
- **Tickers sharing a start date go in one request.** After any previous sync
  almost all of them do, so a full refresh is roughly a hundred requests
  rather than ten thousand.

What it skips, and says so in the summary:

| Skipped | Why |
| --- | --- |
| `$`-prefixed series | Built locally by `add_collections.py` from other rows in the same table; there is no Yahoo symbol to fetch. |
| Generation symbols (`COR.1`) | By construction the earlier occupant of a recycled symbol — it stopped trading when the symbol moved on. |
| Delisted symbols | The `delistings` registry records a death date. Yahoo serves nothing for them, so attempting them would mean ~400 guaranteed failures every run. |

A **renamed** symbol is *not* skipped. The registry files a name change the
same way it files a death (see [A rename is not a recycling](#delisted-companies-and-recycled-tickers)),
and the company is still trading — skipping it would freeze 74 live tickers.

Each batch is written as it arrives, so **an interrupted sync keeps its work**.
Because the next run derives its plan from the database's own last bars, it
resumes exactly where the previous one stopped; Ctrl-C or losing the network
costs you the batch in flight and nothing else.

**Timeouts.** Each batch's download has three minutes. A batch that exceeds it
is split in half and each half retried once, so one slow symbol or a slow
stretch of network does not fail a hundred tickers; a half that times out again
is reported as failed and the run carries on. A killed download is given two
seconds to release its output before it is abandoned, so a hung worker thread
cannot stall the run past its timeout.

The target session is the latest weekday session that has **finished**, on the
New York clock: today's only from 6 pm ET, when Yahoo's daily bar has settled,
and the previous weekday before that. Syncing during market hours used to store
today's half-formed bar as the day's close, and because the ticker then looked
current, no later sync ever replaced it. Exchange holidays
are deliberately not modelled: getting one wrong costs a single no-op pass,
and the alternative is a holiday calendar that must be maintained forever.

**Rate limiting.** A full refresh is well over a hundred requests, and Yahoo
throttles a run that fires them back to back. Two seconds separate the
batches; a throttled batch is retried after 30 seconds, doubling, four times;
and a batch still refused after that **stops the run** rather than spending the
remaining batches confirming it — the report comes back `INCOMPLETE`, and a
later run resumes from the last bar written. Nothing about this is silent: a
symbol yfinance failed to download is reported as a failure, never as a ticker
that simply had not traded. That distinction was worth the plumbing, because
`yf.download` does not raise for a per-symbol failure — it records it in
`yf.shared._ERRORS` and returns a frame without that symbol, so conflating the
two once made a fully rate-limited sync of 7,403 tickers report itself as a
clean run in which 5,202 tickers had nothing new.

### What sync does not cover

`ui --sync` refreshes the **price table only**. These still need their Python
loaders, also run with the app closed:

| Stale thing | Loader |
| --- | --- |
| `$SP500`, `$CASH` and the other benchmark series | `python3 add_collections.py` |
| `3MTreasuryYields`, `10YrTreasuryYields` | `python3 update_treasury_yields.py` |
| `economic_indicators` (CPI, FEDFUNDS, …) | see [Fundamentals, macro and the publication lag](#fundamentals-macro-and-the-publication-lag) |

This matters more than it sounds: the Backtest form defaults `Benchmark` to
`"$SP500"`, and benchmark-relative metrics — alpha, beta, tracking error,
information ratio, up/down capture — are dropped entirely for any window the
benchmark does not fully cover. A stale `$SP500` silently removes the
comparison from every default run. Real (inflation-adjusted) return goes blank
past the last CPI reading the same way.

Implementation: `src/data/sync.go` (the pure plan — what to fetch, from when,
and what to leave alone), `sync_prices.py` (the batched download; like
`fetch_history.py` it never opens the DuckDB; the Go side holds the lock),
`ui/sync.go` (orchestration, pacing and retries), `ui/synccmd.go` (the
`--sync` flag).

## Configuration

Define one `[[portfolio]]` block per portfolio in `config.toml`. Each block names exactly one strategy; to compare strategies, write one block per strategy. Every portfolio runs as its own job.

In the app, the **Config** tab opens on a template that lists every setting
below — the two example portfolios live, every other key and section commented
out at its zero value with a one-line note on what it takes. Delete the leading
`# ` to turn one on; left alone, the template runs exactly the two portfolios.
`ui/default_toml_test.go` fails when a config key is added to the engine but not
to that template.

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
| `BuyingPower` | float | Starting cash. Must not be negative; `0` is allowed, since `Deposit` can fund a portfolio after it starts. |
| `StartDate` / `EndDate` | string | `YYYY-MM-DD`. `EndDate` must not precede `StartDate` — a reversed window is rejected by name rather than crashing the run. Both must be fully covered by every ticker's data or the run is rejected with an error naming the offending ticker and its real range. |
| `Tickers` | []string | Must exist in `stock_data_optimized` for the date range. No duplicates — a repeated symbol is rejected by name. To hold more of something, use `[portfolio.Params.weights]` with `buy_and_hold.lua`, not a second entry. |
| `Strategy` | string | One strategy spec (below). Required by the CLI; the UI falls back to the Lua script open in its editor. |
| `Params` | table | Optional, strategy-specific. Passed to a Lua strategy as the global `params`. **Built-in strategies ignore it entirely** — only `lua:` specs receive it, and a warning diagnostic says so if you set it on a built-in. `smaCross` takes its periods from the spec string (`smaCross:<short>:<long>:<buyType>`), so it cannot be swept through `Params` at all; use one `[[portfolio]]` block per setting. |
| `Costs` | table | Optional trading-cost model (below). Absent means frictionless, which is how the engine behaved before costs existed. |
| `Benchmark` | string | Optional ticker to score against, e.g. `"$SP500"`. Never traded — see below. |
| `Rules` | table | Optional. A strategy written as choices from a fixed menu, used **instead of** `Strategy` (setting both is an error). See [`[portfolio.Rules]`](#portfoliorules) below. |

Strategy specs:

| Spec | Meaning |
| --- | --- |
| `greedy` | Pour all available buying power into each ticker in order. |
| `equalWeights` | Split buying power evenly across the portfolio's tickers. |
| `buyAndHold:<greedy\|equalWeights>` | The explicit form of the two above. |
| `smaCross:<short>:<long>:<buyType>` | Built-in moving-average crossover. |
| `lua:<path>` | Run a Lua strategy file, e.g. `lua:strategies/sma_cross.lua`. |

Note that config loading does no validation of its own: an omitted key simply zero-values, and the error surfaces later — a bad date, a reversed window, negative capital or an unknown strategy spec fails in `ToPortfolio`, an uncovered date range in the runner's coverage check.

**`-check` brings all of that forward.** It runs the same construction path a
run does and stops there, so it opens no database and simulates no days — a
whole config is validated in milliseconds against a 2.9 GB file it never
touches:

```bash
cd src && go run main.go -check -config ../config.toml
```

It reports exactly what a run would have reported, only sooner: reversed dates,
duplicate tickers, negative capital, unknown strategy specs, walk-forward
schedules with nothing to select between, `Params` a strategy cannot read,
swept keys nothing reads, duplicate portfolio names, and an unusable `[Output]`
block. That last one is worth the flag on its own — the output file is opened
*after* the simulation, so a typo in `fields` is otherwise discovered at the
end of a run that is now wasted.

Errors exit `1`; warnings alone exit `0`, because a config with warnings still
runs — it just may not do what was meant.

### `[portfolio.Rules]`

A strategy does not have to be a script. A rule set is a handful of choices
from a fixed menu; the engine implements each entry once, in Go, and the app
reads the whole set back as **one sentence**, so you check what the strategy
does by reading English rather than code.

```toml
[[portfolio]]
Name      = "Top 3 by momentum"
BuyingPower = 100000.0
StartDate = "2012-01-03"
EndDate   = "2024-12-31"
Tickers   = ["AAPL", "MSFT", "JNJ", "XOM", "JPM", "PG"]
Benchmark = "$SP500"

  [portfolio.Rules]
  Rebalance = "monthly"
  Rank      = "return_126"
  Pick      = 3
  Keep      = 5
  Filter    = "above_sma_200"
  RiskOff   = "benchmark_below_sma_200"
```

reads as:

> Every 21 trading days (about monthly), hold the 3 stocks that rose most over
> the last 126 trading days, counting only those trading above their 200-day
> average, equally weighted; keep a holding while it stays in the top 5. Move
> everything to cash whenever the S&P 500 is below its 200-day average. If none
> qualify, the money waits in cash. The first decision waits for 200 trading
> days of history; until then it holds cash.

`DescribeRules` in `src/backtest/rules.go` is the only thing that writes that
sentence, so the app, the assistant and the results cannot describe one rule set
three different ways.

| Key | Takes | Notes |
| --- | --- | --- |
| `Rebalance` | `daily` `weekly` `monthly` `quarterly` `annually` | **Required.** Counted in trading days: 1, 5, 21, 63, 252. |
| `Rank` | a rank signal | Optional. Orders the candidates, strongest first. Without it, every ticker that passes `Filter` is held. |
| `Pick` | int | Required with `Rank`: how many of the top-ranked to hold. At most the number of `Tickers`. |
| `Keep` | int | Optional; `0` means `Pick`. A holding stays while it is still in the top `Keep`, so a name slipping from 3rd to 4th is not sold and bought back. Never below `Pick`. |
| `Filter` | a filter | Optional. A ticker must pass it to be held at all. |
| `Weight` | `equal` `inverse_vol_N` `allocations` | Optional; `equal` by default. `allocations` reads `[portfolio.Rules.Allocations]` (relative weights, each a listed ticker), rescaled over whatever is held. |
| `RiskOff` | a risk-off test | Optional. The whole book goes to cash while it is true. It reads the portfolio's `Benchmark`, so it needs one. |

The menu (`N`, `S`, `L` are trading days):

| Kind | Entry | Meaning |
| --- | --- | --- |
| Rank | `return_N` | Price change over the last N days; highest first. |
| Rank | `momentum_12_1` | Return from 273 to 21 trading days ago (12-1 momentum). |
| Rank | `low_vol_N` | Standard deviation of daily returns over N days; **lowest** first. |
| Rank | `drawdown_N` | How far the close sits below its N-day highest close; closest first. |
| Filter | `above_sma_N` / `below_sma_N` | Close above / below the average of the **previous** N closes. |
| Filter | `sma_cross_S_L` | The S-day average is above the L-day one (S < L). |
| Filter | `rising_N` | Higher than N trading days ago. |
| Filter | `rsi_below_X` / `rsi_above_X` | 14-day RSI below / above X. |
| RiskOff | `benchmark_below_sma_N` | The benchmark's close is below the average of its previous N closes. |
| RiskOff | `benchmark_drawdown_over_X` | The benchmark is more than X% below its 252-day highest close. |
| Weight | `inverse_vol_N` | Weight each holding by 1 / its N-day volatility. |

Nothing in the menu reads past the decision day, so a rule set **cannot have a
look-ahead bug** — nobody writing one writes code. `rules_test.go` pins that
directly (altering every bar after day *t* must not change the decision on *t*),
pins each signal against the Lua primitive it mirrors (`roc`, `sma`, `rsi`,
`stdev`) on every day and ticker, and pins whole rule sets to the cent against
`momentum_rotation.lua` and `rebalance.lua`, and against hand-written Lua twins
for the ideas no library script trades by target weight (`trend_following`'s
"hold what is above its average", inverse-volatility, low-volatility, RSI). Orders go
through the same code as Lua's `target_weights`, so costs, cash, the blotter and
delisting behave identically.

Until the longest lookback has history, the book holds cash and an `info`
diagnostic says so once. Refused up front, by `-check` and the app's ✓ Check, in
plain words: an unknown entry (with a "did you mean"), `Pick` above the ticker
count, `Keep` below `Pick`, `RiskOff` without a `Benchmark`, `Allocations` naming
a ticker not in `Tickers`, `Rules` together with `Strategy`, and — not supported
yet — a `[portfolio.Sweep]` or `[portfolio.WalkForward]` on a rules portfolio.
The universe is the portfolio's own `Tickers`, and the menu is price-only:
fundamental signals still need a Lua strategy.

#### Rules in the Backtest tab: describe it, read it back, run it

You do not have to write the block above by hand. On the **Backtest** tab, a
card's *What should it do?* dropdown has **Describe my own idea…** just under
buy-and-hold:

1. Choosing it opens the assistant with the card's holdings, window and
   comparison already in the message — you finish the sentence with your idea.
   The assistant answers with a rule set it has checked with `check_rules`,
   and its toml block carries a **Use these rules in the form** button.
2. That button puts the rules on the card (your holdings and dates stay). The
   card shows the engine's sentence large, with each rule beneath it as a
   control — dropdowns and number boxes filled from the engine's menu, so there
   is no free text to mistype, every number is clamped to its range, and
   *Keep while in the top* cannot go below *Hold the top*. Change a control and
   the sentence is read again. You can also build the rules with the controls
   alone and never open the assistant.
3. The Run button carries the sentence's promise — **Run: hold the 2 stocks
   that rose most over the last 126 trading days…** — so pressing it is the
   confirmation. With more than one portfolio it reads *Run backtest*. A rule
   set the engine refuses disables Run and says why, in the engine's words.

*Split the money* → *In fixed proportions* uses the card's own percentage
boxes as `[portfolio.Rules.Allocations]`. The rules travel into *Edit as TOML*
and the session like everything else on the card. There is no "open as Lua"
yet: a rule set that outgrows the menu has to be rewritten as a script.

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

Neither can overdraw the account, and neither stops a strategy trading.
Orders are sized against the price they will actually fill at, so a strategy
spending its whole balance asks for a quantity the cash covers even at high
slippage — `slippage_bps = 100` buys about 1% fewer shares, it does not buy
zero. On top of that, an order sized against the whole balance (what
`buy_max` and most Lua strategies do) is clamped down to the largest size
whose commission the balance also covers, rather than being rejected for the
fee alone. An order that genuinely overshoots the balance still does not
fill.

Costs make high-turnover strategies look worse, which is the point — the
`Turnover` metric reports how much trading each strategy is doing, so the two
can be read together.

### `[Portfolio.Cashflow]` — recurring deposits and withdrawals

Contributing on payday, or drawing down in retirement. Optional; **absent means
no flows and byte-identical results**, so nothing written before this existed
is affected.

```toml
[[Portfolio]]
Name        = "Contributing"
BuyingPower = 10000.0
StartDate   = "2015-01-01"
EndDate     = "2025-01-01"
Tickers     = ["AAPL", "MSFT"]

  [Portfolio.Cashflow]
  Amount    = 500.0          # SIGNED: negative withdraws
  Frequency = "monthly"      # monthly | quarterly | annually
  Start     = "2015-06-01"   # optional; defaults to StartDate
```

**The sub-table name must match the array header's case.** TOML paths are
case-sensitive, so `[[Portfolio]]` pairs with `[Portfolio.Cashflow]` and
`[[portfolio]]` with `[portfolio.Cashflow]`. Mixing the two makes the decoder
see two different top-level keys and fail with *"incompatible types: TOML value
has type map; destination has type slice"* — which names neither the line nor
the real problem. The same applies to `[portfolio.Costs]` and
`[portfolio.Margin]`.

**The first flow lands one whole period after `Start`, never on it.** The
opening capital is already in the book, so contributing on day one would
double-count it: a January start with a monthly schedule contributes in
February.

Flows are applied at the top of each day, **before the strategy acts**, so a
strategy can invest a contribution the day it arrives rather than a day late.
The built-in `buyAndHold` and the library's `buy_and_hold.lua` both do: each
deposit is bought at that day's close, split at the weights the opening
purchase used. Only the
deposit is spent, never other idle cash. A Lua script reads the day's flow with
`cashflow()` (positive on a deposit day, negative on a withdrawal day, 0
otherwise) and can size an order against it with `buy_max`'s `budget` argument.

#### A contribution is not a return

The day's return is measured against the book's value at the previous close, so
money added in between would post as performance the market never delivered —
deposit £500 into a £10,000 book and the day shows a 5% gain. Every ratio the
engine reports is built on that daily series, so the error would propagate into
Sharpe, Sortino, CAGR, the rolling Sharpe and the significance nulls alike.

The day's baseline is therefore raised by the flow before the return is taken.
The reported figures answer **"how did the strategy do"**, which is the only
question a backtest can answer about a book whose size you were changing by
hand.

Note the deliberate contrast with `borrow_bps_annual`, which is charged on the
adjacent line of the same loop and gets the *opposite* treatment: borrow cost is
a real cost of holding the position and *should* depress the day's return.

#### Drawdowns are measured on a flow-adjusted series

With a schedule set, `MaxDrawdown`, `CalmarRatio`, `LongestUnderwaterDays` and
the Deepest Drawdowns panel are computed from an index that compounds the
flow-neutralised returns, not from the raw book value. Against the raw value a
withdrawal reads as a decline that never happened, and a deposit refills the
peak and **erases** one that did — plausibly and invisibly.

The equity **curve** stays the raw book value, because the chart should show the
account actually growing when money is added. Two series, two questions: what
the account was worth, and how the strategy performed.

#### Withdrawals that cannot be funded

A withdrawal larger than the cash balance **sells holdings pro-rata** to fund
itself — pro-rata rather than largest-first, so funding a withdrawal does not
itself rebalance the book. The sales go through the normal path, so commission
and slippage are charged and the trades appear in the blotter.

One that cannot be met even after liquidating everything is **counted, not
skipped**: `Metrics.WithdrawalsMissed` increments and a warning names the date
and the shortfall. A decumulation plan with a non-zero count has **failed**,
however good its other numbers look — every figure after the first miss
describes a spent account.

#### `NetContributions` and profit

`Metrics.NetContributions` is the signed total moved: deposits positive,
withdrawals negative. **It is not profit.** Contributing £500 a month for ten
years grows the account by £60,000 that was never a gain, so profit is
`FinalValue − BuyingPower − NetContributions`.

#### Contributions are invested on the day they arrive

`buyAndHold` normally buys once on the first day and never acts again, which
would have left every deposit sitting as idle cash — 37.5% of the book, in a
six-deposit test — and made the feature useless in the strategy the Backtest
tab generates by default. It now puts each contribution to work on the day it
lands, at the same weights the opening purchase used.

Only the **new** money is spent. A cash balance can be high for reasons that
have nothing to do with a contribution — a forced sale to fund a withdrawal, a
strategy deliberately holding cash — and sweeping all of it into the market
would be a different strategy from the one you configured. Withdrawals are
ignored here too: the pro-rata sale has already happened, and buying back into
the position it just sold would defeat the point.

Without a schedule nothing changes: buy-and-hold buys once and holds, exactly as
before.

Other built-ins deploy new cash whenever their own signal next fires. A Lua
strategy can read the flow directly through the portfolio's pending cashflow.

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
portfolio's own starting capital so the two share one axis and one origin,
and **paid the same `[portfolio.Cashflow]` deposits and withdrawals on the same
days** as the portfolio (each lands after that day's return, as a contribution
is invested at the close). Without that, a portfolio fed $1,000 a month
appears to beat the index on the chart by roughly what was paid in. The
equal-weight baseline has always received the contributions; the benchmark
line now does too. Then
`BenchmarkStats`, its own `AnnualReturn`, `RealAnnualReturn`,
`SharpeRatio`, `MaxDrawdown` and `StandardDev`, computed with the same
functions the portfolio uses. The benchmark's real return is deflated over the
same window by the same CPI ratio, so the two `Real %` figures answer the same
question — a real portfolio return beside a nominal benchmark one would be
worse than showing neither. The
desktop UI draws the curve as a muted dashed line on the equity chart and adds
one row per distinct benchmark line to the metrics table. `BenchmarkStats`
stay time-weighted (computed on the curve WITHOUT the flows), exactly as the
portfolio's own metrics are, so a deposit never reads as index performance;
the row's Profit nets the contributions out, as the portfolio's does. Two
portfolios against the same benchmark with different deposit schedules get
two lines, each named after its portfolio (`$SP500 · Monthly`).

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

### Which environment did it actually work in?

A single Sharpe blends every market the window contained. A strategy that made
all its money in one calm bull stretch and gave some back whenever volatility
arrived reports the same headline as one that worked throughout — and the
difference is the whole question when deciding whether to trade it now.

Every result carries `Regimes`, with no configuration:

```
  by market regime (labels from the traded universe):
    bull-calm       892 days (54.1%)  CAGR    18.44%  Sharpe   1.62
    bull-volatile   412 days (25.0%)  CAGR     6.10%  Sharpe   0.41
    bear-volatile   243 days (14.7%)  CAGR   -21.30%  Sharpe  -0.88
    bear-calm       102 days ( 6.2%)  CAGR    -3.90%  Sharpe  -0.22  (too few days to conclude from)
```

Two axes, both from the traded universe's own price history: how far it sits
below its **running peak** (below −10% is a bear), and whether trailing
volatility is above the median of every trailing window **seen so far**.

Both are causal by construction — day *i*'s label uses returns up to *i* and
nothing after — which is what makes them safe to condition a strategy on. The
expanding volatility median matters here: comparing against the whole run's
median would decide what "high volatility" means using the future, and truncating
the series would then change past labels. There is a test that it does not.

The first 63 days are `warmup` and are excluded rather than lumped in with
whatever they resemble, and any regime holding under 10% of the days is marked as
too thin to conclude from.

**Macro regimes are deliberately not here** — rate direction, the 10y−3m curve,
CPI and industrial-production terciles. Every macro series is published with a
lag and revised afterwards (`economic_indicators.Date` is the period, not the
release), so those labels need the same publication-lag treatment
`PointInTimeFundamentals` applies. A regime label that peeks is the same bug as a
fundamental that peeks and considerably harder to notice.

### Validating a candidate before believing it

A strategy that peeks at future data, or never trades, or quietly loses cash to a
typo'd ticker produces a perfectly plausible-looking result. None of those
failures announces itself — the equity curve is smooth, the metrics are finite,
and the Sharpe is often excellent.

```bash
cd src && go run main.go -validate lua:strategies/rsi.lua
```

```
validating lua:strategies/rsi.lua
  ok    look-ahead             identical behaviour across 9 forked futures (108 trade comparisons)
  ok    placed trades          36 fills over 400 days
  ok    cash conserved         final value matches cash plus book (99671.43)
  ok    degenerate inputs      survived 4 degenerate paths
  passed: nothing here invalidates a result from this strategy
```

It runs on synthetic data generated in-process, so it needs neither the database
nor a config, and exits non-zero on failure.

**The look-ahead detector is the piece worth having.** Run the strategy twice on
histories that are *identical up to day k* and different after it. A strategy
reading only the past must behave identically up to k in both runs — the future
it cannot see changed and it did not notice. A strategy whose trades differ
before k has read the future, and there is no way to fake passing: it is a
property of behaviour, not of source text.

The forks are spread deliberately, and both details were learned by watching the
detector miss things:

- **One fork is not enough.** Forking at day k makes the histories differ from
  k+1 onward, so a strategy peeking a *single* day ahead differs only on day k
  itself — a coin flip on whether that day's decision changes. Nine forks turn
  that into a near-certainty.
- **The forks must start early.** A strategy that peeks twenty days ahead, buys
  once and holds makes its only future-dependent decision in the first week;
  every fork placed after it compares two identical trade lists and sees nothing.

The other three checks: a strategy that placed **no trades** is not a strategy
(and that is exactly what the T13 slippage bug produces); **cash conservation**
catches a position held under a ticker outside the portfolio's list, which is
never valued and so silently removes the cash that bought it (T9); and
**degenerate inputs** — flat prices, a series that only falls, one bar, two bars
— must not panic or produce a NaN.

### One command that asks the hard questions first

Every habit that produces trustworthy research is now built — a baseline on every
result, a test against chance, per-ticker attribution, regime slicing, a
robustness battery — and every one of them is something you have to *remember* to
look at. `-research` makes them the default reading order instead:

```bash
cd src && go run main.go -research
```

```
== My Portfolio (lua:strategies/buy_and_hold.lua)
  5 fills, none closed — nothing was sold, so there is no win rate to report
  MATCHED equal-weight buy-and-hold exactly (50.41%/yr) — it is doing the same thing
  Sharpe 1.26, p = 0.000 against random timing with the same exposure
  90% confidence interval on that Sharpe: 0.74 to 1.76
  Best contributor: NVDA (521353.52 of 580363.58 total)
  One position produced most of the profit — that is a bet on that name,
  not evidence the rule works.
  by market regime (labels from the traded universe):
    bull-calm       996 days (40.6%)  CAGR  88.56%  Sharpe  2.61
    bull-volatile   805 days (32.8%)  CAGR 155.07%  Sharpe  2.59
    bear-volatile   612 days (25.0%)  CAGR -44.90%  Sharpe -0.86
```

That is the shipped default config, and the report says three uncomfortable
things about it in six lines: the strategy is doing exactly what buy-and-hold
does, **90% of the profit is one ticker**, and a quarter of the days were spent
in a regime that lost 45% annualized.

The order is the point — each question can stop you before the next matters:

1. **Did it trade at all?** No trades is not a 0% return, and the report stops there.
2. **Did it beat buy-and-hold?** Most strategies do not.
3. **Is it better than random timing?** A Sharpe is not evidence.
4. **Where did the money come from?** One lucky name, or broadly?
5. **Which environment did it work in?**
6. **What kills it?** Friction, or a different start date.

It invents no statistic and passes no verdict. Deciding what "good enough" means —
a pre-registered criterion, a trial budget, survival thresholds — is a
research-design choice with real consequences, and it belongs to the person doing
the research. The report closes by naming what it cannot answer and which flag
answers it.

### Robustness: how hard can you push before it breaks?

A Sharpe from one window with one cost assumption is a point estimate dressed as
a finding. `-robustness` re-runs each portfolio under more friction and from
later start dates — same strategy, same window, no new data:

```bash
cd src && go run main.go -robustness    # ~9x the simulation time; opt-in
```

```
My Portfolio
  cost sensitivity (round-trip slippage):
       0 bps  CAGR   50.41%  Sharpe 1.21  trades 5  final 590363.58
       5 bps  CAGR   21.34%  Sharpe 0.85  trades 4  final  69006.06
      20 bps  CAGR   21.35%  Sharpe 0.85  trades 4  final  68994.06
      50 bps  CAGR   21.37%  Sharpe 0.85  trades 4  final  68970.06
    trade count CHANGES at 5 bps (4 fills against 5 when free) — costs are
    altering behaviour, not just returns, so the drop below is not a cost
  start-date sensitivity:
      +5d CAGR 49.76%   +20d CAGR 50.56%   +60d CAGR 50.27%   +120d CAGR 47.71%
    spread 2.85 points (worst 47.71%, best 50.56%)
```

**"Dies above 7bps" is a complete review in three words.** A strategy whose edge
evaporates at realistic friction is not a strategy, and a result that only exists
for one start date is a calendar artifact.

Two things the report is careful about, because both mislead by default:

- **A flat cost curve can mean the strategy stopped trading**, not that its edge
  is cost-proof. Fill counts are printed at every level, and a change in them is
  called out: costs are supposed to change returns, not behaviour. The example
  above is the shipped default config, where one order of five is rejected at 5
  bps and the final value falls from 590k to 69k — that is [T13], not friction.
- **A later start SLICES the window** rather than skipping days into it.
  Strategies key off the absolute day index (`buy_and_hold` buys on day 0, every
  cadence rule is `day % n == 0`), so a loop that merely began at day 5 would
  leave buy-and-hold in cash and report 0% as though the start date had destroyed
  it.

Not included yet: the parameter-neighbourhood check (a swept winner on a plateau
is plausible; a spike between two bad neighbours is overfit) and universe
subsampling. Both need to know about the sweep that produced a result, which is
different machinery from re-running one portfolio.

### Monthly returns, in the app

A year-by-month matrix per portfolio, with a compounded Year column. The
calendar-year table says whether a year was good; this says whether it was
steady or was one good month — the distinction a single annual figure hides
best.

The Year column **compounds** rather than sums, because returns are
multiplicative: a year of −50% then +50% reads −25%, not 0. Partial months
(the run does not span them) are starred and shown as the plain return over the
days present, never annualized.

### Trading and attribution, in the app

When a run traded, the results view adds a **Trading** panel (trades, round
trips, win rate, average win and loss, profit factor, average holding days,
realized P&L, fees) and an **Attribution** panel listing per-ticker total P&L,
realized, unrealized, fees, trades and shares held — **sorted by absolute
total**, so the position that carried the result is first. That is the whole
point of the view: it distinguishes an edge that worked broadly from one lucky
position, which no scalar metric does.

Both read `TradeStats`, which the blotter has produced since T31 and which the
frontend did not carry until now. One sign convention worth knowing:
`AvgLoss` is a **positive magnitude** — the blotter accumulates `-RealizedPnL`
for losers — so the UI writes the minus sign itself rather than relying on the
field to be signed.

### The same loop, in the app

The config pane carries a collapsible **Pre-register a hypothesis** box holding
the same TOML the CLI reads. It sits *above* the run button deliberately: a
criterion typed after seeing a result is not a pre-registration. An empty box
runs the backtest unjudged — which is not the same as judging it against a
default nobody chose.

Results then show a **Hypothesis verdict** panel above the standing objections,
with the verdict colour-coded and every condition listed as PASS / FAIL / n/a
alongside the numbers behind it. A criterion the parser refuses shows as an
error in the box rather than a run that quietly went unjudged.

### Hypothesis in, verdict out

A backtest lies mainly through one mechanism: deciding what counts as success
*after* seeing the results. The fix is to write the criterion down first and
evaluate it mechanically.

```toml
# hypothesis.toml
ID          = "H-1"
Claim       = "12-1 momentum beats buy-and-hold on the benchmark series"
Campaign    = "trend"
Criterion   = "sharpe >= 1.2"
TrialBudget = 200
```

```bash
cd src
go run main.go -hypothesis ../hypothesis.toml
```

A `Criterion` is one metric, one operator (`>` `>=` `<` `<=`) and one number —
deliberately not an expression language, because a criterion complex enough to
need parentheses is one nobody can check was written in advance. It is parsed
at *load*, so an unparseable criterion fails before the run rather than after
the numbers exist.

**Three verdicts, and four conditions that must *all* pass to survive:**

| Condition | Source |
| --- | --- |
| The pre-registered criterion | your `Criterion` |
| Campaign-deflated confidence ≥ 0.90 | T17/T23 |
| Beats the baseline **out of sample** | T32 + `[portfolio.Validation]` |
| Robustness ≥ 0.6, parameter-neighbourhood non-zero | T24 |

`rejected` is a claim about the **strategy** — something was evaluated and
failed. `inconclusive` is a claim about the **evidence** — something could not
be evaluated at all. They are not degrees of the same thing.

**Nothing can survive today, on purpose.** Condition 4 needs the
parameter-neighbourhood check that T24 did not build, so it always reports
unavailable and every otherwise-good result reads inconclusive with that named
as the reason. Unevaluable must never read as a pass; finishing T24 turns this
on without changing the verdict engine.

Two more refusals worth knowing: condition 3 will not substitute a whole-run
baseline when no split is configured, because that would score parameter fitting
as a finding; and a run that declared `Fundamentals` but scored under 50%
coverage is disqualified as evidence rather than rejected as a strategy.

The trial budget is reported when spent but never decides a verdict — it says
the campaign is over and further searching is not evidence.

### Fundamentals in a strategy

`PointInTimeFundamentals` has been correct since T29 and nothing called it, so
value, quality, leverage and size were unreachable from a strategy. A portfolio
now declares what it needs:

```toml
[[Portfolio]]
Fundamentals = ["Net Income", "Stockholders Equity", "Total Revenue"]
```

They are fetched **once per run**, before the worker pool starts — the raw table
is 8.5M rows, so naming the metrics is what keeps it to the handful a strategy
reads. Absent or empty, nothing is fetched and the run is bit-identical to
before.

Two Lua globals, and deliberately no more:

```lua
fundamental(ticker, metric, day)                  --> number, or nil if not yet published
fundamental(ticker, metric, day, "annual")        --> the newest full fiscal year only
fundamental(ticker, metric, day, "quarterly")     --> the newest single quarter only
fundamental(ticker, metric, day, "annual", 1)     --> the fiscal year before that
market_cap(ticker, day)                           --> shares x close, or nil
```

**Name a frequency for any flow** (net income, revenue, cash flow). The bare
three-argument form returns the newest figure published, whichever frequency
it was, so over `financials` it can hand back a quarter one day and a year the
next — every ratio built on it steps by about 4x at each company's own fiscal
year end (T108). The bare form is kept exactly as it was so existing configs do
not move; the fourth argument is the fix. An unknown frequency is an error, not
a silent "any". The fifth, `periods_back`, steps back through earlier periods of
that frequency *as each was known on `day`* — `fundamental(t, "Total Assets",
day, "annual", 1)` is last year's year-end balance sheet, so year-over-year
change needs no extra window of price history.

#### `FundamentalSource = "sec"`: 2010 onward

```toml
[[portfolio]]
FundamentalSource = "sec"
Fundamentals = ["Net Income", "Stockholders Equity"]
```

reads values from `sec_financials` instead of `financials` — the T138 loader's
30 metrics, back to 2009 (2010 in practice), each figure **as first filed**
and usable from the day after its filing. A later restatement is never
substituted back. Three things differ from the default source:

- **Frequencies are SEC's own.** `"annual"` is a full fiscal year and
  `"quarterly"` a single quarter, from the filing's period length rather than a
  label, so the T108 mislabelling cannot happen. A balance-sheet figure answers
  to both: `"quarterly"` is the newest balance sheet of any kind, `"annual"`
  the fiscal-year-end one from the 10-K, which lines up with an annual flow.
  The bare form never mixes: it means `"annual"` for a flow and the newest
  balance sheet for a level.
- **Values are as reported.** SEC does not restate share counts for a later
  split, so `Ordinary Shares Number`, the EPS metrics and `Diluted Average
  Shares` jump at every split, and **`market_cap()` always returns `nil`** —
  an as-reported share count times a split-adjusted close would be off by every
  split since. Ratios of two dollar figures (ROE, margins, debt / assets) are
  unaffected, and they are what the shipped fundamentals strategies use.
- **Only listed companies are reachable.** Delisted filers are in the table
  with no ticker, and joining them to a price series is T51. A hand-typed
  `Tickers` list is survivors by construction anyway; read results with that
  in mind.

**A script can declare what it reads**, so picking it by name is enough. A
top-level table in the script itself

```lua
fundamentals = {
    source  = "sec",
    metrics = { "Net Income", "Stockholders Equity" },
}
```

is applied to any portfolio running the script whose config declares no
`Fundamentals` of its own (a config `FundamentalSource` still wins over the
table's `source`). It is code rather than a comment on purpose — it used to be
a `-- @fundamentals:` header line, and deleting a comment while tidying a copy
switched the strategy off without a word. The engine **reads** the table from
the script's syntax tree before the run; it does not run the script to get it,
because the figures are fetched before any Lua executes. So it must be a plain
table of quoted names: a computed name, an unknown field or a second
assignment is refused with the line it is on. A config that names metrics means exactly those. And a script
that asks for a metric nobody loaded gets a warning in the run's diagnostics,
once per metric, instead of a silent `nil` on every day — which is how a
fundamentals strategy chosen from the gallery used to hold cash for a whole run
without a word.

The config is checked when it loads: a metric `sec_financials` does not carry
(`EBITDA`, say) is refused with the list of the ones it does, rather than
returning `nil` forever; so is `"sec"` with no `Fundamentals`, and any other
value. Without the key, or with `"financials"`, nothing changes.

Five strategies in `strategies/` use it — `quality_roe.lua`,
`gross_profitability.lua`, `piotroski_fscore.lua`, `low_asset_growth.lua` and
`strong_balance_sheet.lua`, each declaring its figures in a `fundamentals`
table, so naming one as the `Strategy` is all a portfolio needs. Each has a block in
`strategy_library.toml`.
See [strategies/README.md](strategies/README.md#fundamentals-strategies).

**They return `nil`, not 0, which breaks the convention `price()` follows.** A
price of zero is impossible, so 0 safely means "absent". A company can genuinely
report zero net income, zero cash or negative equity, so 0 here would be
indistinguishable from "not published yet" — precisely the ambiguity that makes
a strategy trade on a figure it never had.

The look-ahead guarantee is enforced in Go, upstream of Lua: every figure
carries the day it became public (real SEC filing date where known, then the
earnings-calendar date, then period end + 90 days as the worst case), and a
lookup only ever returns what was public that morning. `market_cap` also fetches
`Ordinary Shares Number` whether or not you named it, and returns `nil` rather
than a plausible-looking number when the share count is missing or the close is
≤ 0.

**Coverage is measured rather than enforced.** `financials` begins in 2020, so
a strategy run on it from 2010 sees `nil` for the first decade, trades only in the
back half, and looks like it found an edge. Refusing that window would be too
blunt — a strategy may legitimately act only when fundamentals exist — so the
`FundamentalCoverage` metric reports the share of (day × ticker × declared
metric) cells that had a figure, shown as **Fund Cov %** in the results table
and as an em dash for runs that declared nothing. A low figure beside a good
Sharpe is the thing to look at first.

### The same verdict, in the app

The results view carries the same three cards under the portfolio summaries, in
a panel titled **Does this survive the obvious objections?** — beat the
baseline, better than random timing, and how many trials the result was picked
from. Both surfaces read the same fields and apply the same rules, so a result
reviewed on screen and the same result read from an exported page cannot
disagree.

`Baseline` and `Significance` are computed on every run. They used to be dropped
on the way to the frontend, which meant the two questions that most often kill
an idea were answerable only from the CLI.

### One page per candidate

Deciding whether a result is real means reading a table row, squinting at a
chart, and re-running with different dates. `-pages` collapses that into one
self-contained HTML file per result:

```bash
cd src
go run main.go -pages ../pages    # one <portfolio-name>.html per result
```

Each page carries the equity curve against its benchmark, the underwater plot,
rolling 12-month Sharpe, the in/out-of-sample split, the walk-forward windows,
calendar-year and monthly returns, the deepest drawdowns, trade statistics and
every metric.

What makes it more than a prettier table is the strip directly under the
headline, **Does this survive the obvious objections?** — three cards in the
same visual weight as the return itself:

| Card | What it answers |
| --- | --- |
| Beat *&lt;baseline&gt;*? | the T32 equal-weight buy-and-hold of the same tickers, net of the same costs |
| Better than random timing? | the significance p-value against the random-timing null |
| How many trials was this picked from? | the trial count, the deflated Sharpe, and the Sharpe a no-edge best-of-N would show anyway |

The third card turns red when a result was selected from more than one trial and
its deflated Sharpe is below 0.90. A candidate that looks good because it was
the best of two hundred tries should not be able to hide the two hundred, and
those are exactly the numbers that get skipped when they sit in a footnote.

The file is self-contained by design: inline CSS, inline SVG, no scripts and no
external requests, so it still renders years later with no network. It follows
the reader's light or dark preference and prints without a black background.

### Run bundles: hand a result to someone else

A result lives in a table and cannot be checked by anyone who was not at this
machine on the day it ran. Sharing a Sharpe is an assertion; sharing a bundle is
evidence.

```bash
cd src
go run main.go -bundle ../momentum-2026-08.json   # capture this run
go run main.go -verify ../momentum-2026-08.json   # re-run it and compare
```

```
verifying ../momentum-2026-08.json
  recorded on engine 4fd0b47, re-run on 9c1e02a
  DRIFT    My Portfolio: sharpe was 1.268690805, now 1.208276957 (-4.762%)
  NOT reproduced — either the engine changed, the strategy changed, or the data did
```

A bundle carries the fully expanded config, the **strategy's source**, the
engine's commit, and every figure a verification checks. `-verify` exits non-zero
when the numbers no longer reproduce.

Three details that make it worth having:

- **The strategy source is embedded, not referenced.** A path is not a strategy:
  `lua:strategies/rsi.lua` names a file anyone can edit. A changed script is
  reported as its own finding, first — every number below it is then expected to
  differ, and listing them individually would bury the reason.
- **The tolerance is float noise, not a modelling allowance.** The engine is
  deterministic over identical data, so anything past the last few bits is a real
  change. A relative difference of 1e-6 is reported; 1e-12 is not.
- **A run that could not be re-run is `MISSING`, not reproduced.** Silence must
  never read as success.

The commit is recorded with a `-dirty` suffix when the tree had uncommitted
changes, because a bundle produced from a modified tree cannot be reproduced from
its sha alone.

Re-running an old bundle is the only real check that the engine has not drifted —
and it catches it from a different direction than the correctness suite, which
can only test what it was written to expect.

### The research log: what have I already tried?

Nothing persisted before this: `output.txt` is truncated on every run, so the app
had no memory. You could not compare today's idea with last week's, could not
tell whether you had already tested a hypothesis, and — the one that matters most
— could not count how many things you had tried. That count is the denominator
every overfitting correction needs.

```bash
cd src
go run main.go -record                      # append this run to ../research.db
go run main.go -record -campaign momentum   # group runs under a campaign
go run main.go -history 20                  # the last 20 runs, newest first
```

Off by default, so existing usage is untouched. When it is on, a repeat is called
out at the moment it is still actionable:

```
note: "My Portfolio" has been run 1 time(s) before, last on 2026-08-17 [9be4ecc86759]
campaign "momentum" has now spent 2 trials
```

Three properties, each deliberate:

- **A separate database file.** `stock_data.db` is 2.9 GB and single-writer, and
  the UI holds it open. A log you cannot write while the market data is open is
  a log nobody writes to.
- **Append-only.** Never updated, never deleted. A re-run is a new row sharing
  its config hash, and that repetition is the signal — it is what stops you
  quietly retrying until something works.
- **A failed write never fails a backtest.** Recording happens strictly after
  the run and its errors are reported, not fatal.

The config hash covers strategy, tickers, window, capital, benchmark, costs and
every parameter, so the same experiment always hashes the same. Ticker **order**
is part of it: `buyAndHold:greedy` spends everything on the first ticker, so
reordering the list is a different experiment, not a repeat.

### Campaign-level deflation

The deflated Sharpe below corrects the best of N trials for the size of **one
sweep**. That is the right correction for one config block and the wrong one for
a research programme: 40 hypotheses at 50 parameter sets each is 2,000 trials,
and deflating the winner by 50 overstates it enormously.

With `-campaign`, the registry knows the real count and reports it:

```
campaign "momentum" has now spent 214 trials; luck alone would produce a
Sharpe of 0.63 across that many, so treat anything below it as noise
```

Each recorded row also stores `campaign_trials` and
`campaign_deflated_sharpe` — the figures as they stood when the row was written,
since the log is append-only and a row records what was known then.

One property worth understanding: the bar responds to the **variety** of what
you tried, not the raw count. Running the identical config four times produces
no spread of outcomes and so no bar, which is right — that is one experiment
repeated, not four tries at a search. A sweep across genuinely different
parameters produces a spread, and the bar rises with it.

This is the difference between a research tool and a slot machine. Every
strategy-search product that has produced garbage produced it by deflating
against the wrong denominator.

`research.db` is git-ignored — it is your experiment history, not code.

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
`fields`, `filter` and `sort_by` all draw on one list — every metric above,
plus `PortfolioName`, `Strategy`, `InitialCapital`, `FinalValue`, `Profit`,
`Trials`, `ExpectedMaxSharpe`, `DeflatedSharpe`, and the trade blotter's
summary: `Trades`, `Buys`, `Sells`, `RoundTrips`, `Wins`, `Losses`, `WinRate`,
`AvgWin`, `AvgLoss`, `ProfitFactor`, `TotalRealized`, `TotalFees` and
`AvgHoldingDays`. An unrecognised name is rejected when the config loads, and
the error names the valid alternatives rather than leaving you to guess.

`Trades > 0` is worth putting in almost every sweep filter. A candidate that
never traded does not look like a failure — it reports a flat equity curve
rather than an error, so a filter written in terms of returns or drawdown
keeps it, and it can even sort near the top on drawdown for never having
risked anything:

```toml
filter  = "Trades > 0 && ProfitFactor > 1.5"
sort_by = "ProfitFactor"
```

The baseline comparison and the chance test are addressable too:
`BaselineFinalValue`, `BaselineAnnualReturn`, `BaselineSharpe`,
`BaselineMaxDrawdown`, `BaselineStandardDev`, `ExcessFinalValue`,
`ExcessAnnualReturn`, `BeatBaseline`, `BaselineComputed`, and `PValue`,
`Percentile`, `NullMeanSharpe`, `NullP95Sharpe`, `BootstrapLow`,
`BootstrapHigh`, `SignificanceComputed`. The baseline names are prefixed
because `BaselineStats` has its own `FinalValue`, `SharpeRatio`, `MaxDrawdown`
and `StandardDev`.

**Always pair these with their `Computed` flag.** When a figure was not
computed it is not absent, it is **zero** — and zero is a passing value:

```toml
filter = "PValue < 0.05"                          # WRONG
filter = "SignificanceComputed && PValue < 0.05"  # right
```

An uncomputed `Significance` reports `PValue` 0, which sails through
`PValue < 0.05` and reads as maximally significant when nothing was tested at
all. `Baseline` has the same shape: uncomputed means `BeatBaseline` false and
the excess figures 0, which reads as "tied with buy-and-hold" rather than "no
baseline existed". The filter that says what most people mean is:

```toml
filter = "BaselineComputed && BeatBaseline && Trades > 0"
```

## Running

The binary expects to be launched from `src/` because it resolves `../stock_data.db` and `../config.toml` relative to the working directory.

```bash
cd src
go run main.go                          # quiet run; logs are discarded
go run main.go -debug                   # writes backtester.log + transactions.log, and serves pprof on :6060
go run main.go -config ../strategy_library.toml
go run main.go -scan-signals             # screen signals instead of backtesting
go run main.go -event-study              # what happened after drawdowns, breakouts
go run main.go -list-strategies          # the gallery: what each script needs and how it fails
go run main.go -check                    # validate the config and exit; opens no database
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
- **Coverage** is reported when it matters. A day is skipped when fewer than 5
  tickers have a value (a warm-up or a short history) or when every ticker scores
  the same (no ordering to correlate). Past a quarter of candidate days, the row
  says so and names the reason — the two are different problems, one fixed by
  adding tickers and one a property of the signal:

  ```
  mom_252  5d  IC +0.0338  t +2.34  n=476  (312 of 500 days skipped: fewer than 5 tickers had a value)
  ```

  It is context, not a refusal: a scan with too few samples is already refused.
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

#### Checking a config before running it

The **✓ Check** button next to Run validates the config in the editor without
running it — the UI half of `-check`. It opens no database and simulates
nothing, so it answers immediately even for a config whose run would take
minutes. The first error it finds is shown in the status bar (see *Where a
run's problems show up* below). Errors
mean the config will not run as written; warnings mean it will run but may not
do what was meant.

#### Saying what you are hoping for

Pre-registering a claim is the most useful habit the app can teach and the
hardest one to start: the criterion is a metric, an operator and a number, and
`sharpe >= 1.2` means very little on a first run. So under the criterion row
there are presets:

| preset | the criterion it fills |
|---|---|
| Better than a coin flip | `p_value <= 0.05` |
| Steady enough to hold | `sharpe >= 1` |
| Never fell more than 30% | `max_drawdown <= 30` |
| Made at least 8% a year | `annual_return >= 8` |

They **fill** the existing controls rather than replacing them, so one click
also shows you what the phrase meant and leaves you free to change the number.
A preset only appears if the engine actually serves its metric, so the form
cannot offer a criterion the run would reject. If the claim box is empty a
preset fills that too; if you have written your own words, they are left alone.

#### Keeping bad input out of the form

The Backtest tab refuses input that would make a run fail, rather than
reporting it afterwards. Every rule is a pure function in
`ui/frontend/src/portfolioCheck.ts`, pinned by the smoke run:

- **Holdings** come only from the database's own list. Enter and **Add** take
  the highlighted suggestion, else an exact symbol, else the best match — never
  free text — and a symbol whose history does not overlap what is already held
  is not offered at all.
- **Dates** accept only digits (dashes are inserted as you type) and are
  disabled until there is a holding to take a window from. On leaving the box a
  typo falls back to the last good date, anything outside the window every
  holding covers is pulled inside it, and a start after the end moves the
  *other* date so what you typed is what you keep. The window also stops at the
  benchmark's last bar when the two overlap, because a window past it silently
  drops every comparison metric. The **Compare it against** list disables a
  benchmark this database does not hold.
- **Names** are made unique when you leave the box (`Value` → `Value (2)`), and
  a blank one becomes `Portfolio N`.
- **Money** boxes take digits only, up to $999 billion; a starting capital left
  at zero goes back to $10,000.
- **The criterion value** accepts only a signed decimal.

#### Where a run's problems show up

There is no console panel. What the form cannot prevent — a Lua script that
raises, a hand-written TOML config — surfaces in two places: the **first
error** of a run or ✓ Check appears as one line in the status bar (`+N more`
when there are several), and a Lua error that names a line is drawn as a
marker in the editor's gutter. Warnings and `log()` notes are not shown in the
app; a CLI run prints every diagnostic to its log (stderr, or
`backtester.log` under `-debug`), and `-check` prints the config-level ones.
The diagnostics the engine produces:

| level | what produces it |
|---|---|
| `info` | a strategy calling `log()` — the script talking, not a defect |
| `warn` | a `[portfolio.Params]` key the strategy never reads — usually a typo, and in a sweep it means every trial ran identical settings |
| `warn` | `Params` or `Sweep` keys on a **built-in** strategy, which cannot read either — including on a `[portfolio.WalkForward]` block, where identical candidates make the selection meaningless |
| `error` | a Lua strategy that failed to load, or raised on a given day |
| `error` | a portfolio that simulated nothing — no tickers, or no trading day covered by all of them, with each ticker's real range named |
| `warn` | fewer days simulated than the window covers, and which ticker is short |
| `warn` | a benchmark with no data, or one not covering every trading day |
| `warn` | a holding the data audit calls `unusable` or `suspect`, naming the defect — see *What is wrong with the bars themselves*. Nothing is excluded; the run includes it |
| `info` | this database has no `data_quality` table, so no holding was checked (said once per session) |
| `warn` | an in-sample/out-of-sample split with too few days either side to score |

Each carries the level, the portfolio it belongs to, and the message. The
engine's own logging — query timings, row-scan warnings, the `PrintMetrics`
dump — is not a diagnostic; the split is decided in Go at each call site, by
`backtest.diagf`, not filtered afterwards.

At most 200 diagnostics are kept per run; beyond that a final one says how many
were suppressed. A strategy that raises on every trading day is the case this
bounds.

## The Research tab (UI)

A company lookup that works like a terminal's command line, in limited scope
and on purpose without noise: **no headlines, no adverts, no live quotes**.
Everything it shows is primary data — SEC filings, official statistics and the
database's own prices — and says where it came from and as of when.

Type a ticker or a company name, then a function, and press Enter
(`Ctrl+4` jumps here with the cursor in the box; the command palette's *Look up
a company* does the same):

| command | shows | from |
|---|---|---|
| `AAPL` or `AAPL DES` | what the company is: name, industry (SIC), exchange, state of incorporation, former names, SEC CIK with a link to its filings on EDGAR; S&P 500 membership spans; last close and 52-week closing range; the data-quality verdict on its price series | `company_profile`, `index_membership`, `stock_data_optimized`, `data_quality` |
| `AAPL FA` | reported financials — revenue through shares outstanding — for the last 5 fiscal years, or 8 quarters with *Quarterly*. Each figure **as first filed**: hover a cell for the filing date and form. Later restatements are not shown, so this is what the market could know at the time | `sec_financials` |
| `AAPL ERN` | every report date (US Eastern) with whether it came before the open or after the close, and the price move from the last close before it to the first close after it | `earnings_calendar`, `stock_data_optimized` |
| `AAPL CN` | the company's **own news**: its recent SEC filings, newest first, each in plain words — an 8-K by what it reports (*Earnings results*, *Directors or officers changed*, *Material agreement*, *Bankruptcy*, …), 10-K/10-Q, 13D/13G stakes, proxies, insider forms — timed to the minute EDGAR accepted it. Click one to read it in the app as clean text; an earnings 8-K opens on its press release (EX-99.1) | SEC EDGAR, live (keyless) |
| `ECO` | the **economy**, no ticker needed: the latest official reading of CPI, core CPI, nonfarm payrolls, unemployment, real GDP growth, industrial production, M2 and the fed funds rate — each with the period it describes, its change from the previous reading and over a year, when it was published and when the next release comes out — and the FOMC's last three and next four meetings, with links to each statement and its minutes | FRED (keyless CSV; release dates need `FRED_API_KEY`), federalreserve.gov, live |
| `MY` | **your watchlist**, no ticker needed: the last 90 days of every company on it, merged and newest first — its SEC filings as CN labels them (events, reports and proxies) and its earnings reports with ERN's price move — plus the earnings dates still to come. Click a filing to read it. Add and remove tickers in the view | `watchlist.json` in the config directory, SEC EDGAR, `earnings_calendar` |

The function may come first (`FA AAPL`), case does not matter, and a name
resolves to the best-matching symbol (`apple fa` → `AAPL FA`) the same way the
Backtest tab's ticker search does. The function buttons under the box switch
view without retyping the ticker. A lone `ECO` or `MY` is that view; the
tickers ECO and MY themselves are still `ECO DES` and `MY DES`.

Honest edges, each said in the view rather than discovered:

- The price line is **history, not a quote**: it names the session its close
  comes from. A 52-week range is of daily closes, not intraday highs and lows.
- A table the database lacks is named together with the loader that fills it
  (`add_company_info.py`, `add_index_membership.py`, `add_data_quality.py`,
  `add_sec_financials.py`); the rest of the view still renders.
- SEC EDGAR lists only current tickers, so a delisted symbol has prices but no
  profile. `company_profile` pads CIKs to ten digits and `sec_financials` does
  not; FA matches them either way.
- `earnings_calendar` covers about 1,000 tickers. A report during the session
  is measured from the prior close, so it includes that morning's trading, and
  the view says so.

**CN reads SEC EDGAR live**, because filings are where company news starts: an
earnings headline is a rewrite of the 8-K's exhibit 99.1, an executive
departure is an 8-K item 5.02, an insider sale is a Form 4. Reading the filing
skips the rewrite, the adverts and the spin.

- **Filters.** *Events & reports* is the default and leaves out insider forms
  (3/4/5/144), which for a large company are most of what it files and bury
  the events; *Insiders* and *All* bring them back.
- **The reader** fetches the document and strips it to text — no scripts,
  styles, trackers or the hidden inline-XBRL header — with *On sec.gov ↗* for
  the original. It fetches only documents under `www.sec.gov/Archives/`, so a
  link cannot point it anywhere else. Very long documents (a 10-K) are cut at
  400,000 characters and say so.
- **Polite by construction.** Every request carries `SEC_CONTACT` in its
  User-Agent (from the environment or `.env`, else the same default as the
  Python loaders), the whole app stays under ~8 requests a second, 429s and
  5xx back off and retry, and a filing list is cached for five minutes and a
  document for a day — in memory only; nothing is written to the database.
- **EDGAR's API mis-states acceptance times.** `acceptanceDateTime` in the
  submissions JSON is not the UTC instant its `Z` claims: it is the Eastern
  time shifted by *twice* the UTC offset (+8 hours in summer, +10 in winter).
  Apple's July 2026 earnings 8-K reads `2026-07-31T00:30:28Z` there and
  *Accepted 2026-07-30 16:30:28* on EDGAR's own filing page. The app corrects
  it; `TestAcceptedTimesMatchEDGARsFilingPages` pins four filings, summer and
  winter, against those pages.
- CN finds a company by its SEC CIK from `company_profile`, so it needs that
  table, and works for current filers only (delisted symbols have no profile).
- Wire-service press releases and media headlines are deliberately absent.

**ECO reads FRED and the Federal Reserve live**, and is honest about time,
because an economic number is only knowable from the day it is published:

- **Period and publication are separate columns.** *Sep 2026* is the month the
  unemployment rate describes; *Published* is when it came out. With a
  `FRED_API_KEY` (environment or `.env`, the key `update_treasury_yields.py`
  already uses) both *Published* and *Next release* come from FRED's release
  calendar. Without one, *Published* is an estimate from the release's usual
  timing, marked `≈`, and *Next release* is blank — the view says so rather
  than guess. *Next release* is when the **next reading** is due — the first
  release after the next period ends — so the fed funds average, which rides
  on the daily H.15, shows the day after the month closes rather than tomorrow.
- **Changes read the way each series is quoted**: an index (CPI, production,
  M2) by percent, a rate (unemployment, GDP growth, fed funds) by points,
  payrolls by jobs. *Change* is against the previous reading and names its
  month, because FRED leaves a period it never collected blank — October 2025's
  CPI and unemployment, cancelled by the shutdown — and the previous reading
  then is two months back, not one.
- **One source failing never blanks the view.** A series FRED does not answer
  for falls back to the database's `economic_indicators` copy, badged
  *Database copy* and noted with where that copy ends (it is months behind
  unless the loader was re-run). A failed meeting calendar is a note beside
  readings that still show.
- **FOMC meetings** are read off the Fed's own calendar page: the last three
  and next four, *with economic projections* where the Fed marks one, notation
  votes as such, and links to each statement and its minutes once published.
  A date ahead is tentative until the meeting before it confirms it.
- Nothing is written to the database; responses are cached in memory, a
  reading for an hour and a calendar for twelve.

**MY is CN and ERN across a watchlist.**

- **The list is yours, not the database's**: `watchlist.json` in the per-user
  config directory (`~/.config/backtester` on Linux), beside the API key — a
  shipped database may be read-only, and a watchlist is a preference, not
  data. The first time you open MY with no list saved, it is seeded from the
  Backtest form's holdings; after that, *Add the form's holdings* offers any
  you have added since. Emptying the list is kept as a choice — it does not
  re-seed. Up to 50 tickers, upper-cased, repeats dropped.
- **One quarter, newest first**: 90 days, so every company's latest earnings
  and quarterly report are in it, at most 300 items. Only events, reports and
  proxies are shown. Insider forms and securities offerings would bury the
  rest (JPMorgan alone files thousands of structured-note prospectuses a
  quarter). Ownership stakes (13D/13G) are left out because a company's EDGAR
  list mixes stakes in it with stakes it holds elsewhere — most of
  JPMorgan's are the latter — and the record does not say which. An
  earnings report appears twice on purpose: once from `earnings_calendar`
  with the price's answer, once as the 8-K that carries the release.
- A ticker with no SEC filer (a fund, a delisted symbol) or that EDGAR does
  not answer for is named in a note; the rest of the feed still shows. Each
  company is one EDGAR request, paced like CN's.

Code: `src/data/research.go` (the pure shaping — `FinancialsTable`,
`EarningsReactions`, `SummarizePrices` — plus thin readers), `ui/research.go`
(the `CompanyOverview` / `CompanyFinancials` / `CompanyEarnings` bindings), and
`ui/frontend/src/Research.tsx`; CN is `src/edgar` (the client, parsing and the
text reader, tested against SEC responses recorded in `src/edgar/testdata`) and
`ui/filings.go`; ECO is `src/econ` (FRED CSV, release calendar and the Fed's
meeting page, tested against responses recorded in `src/econ/testdata`) and
`ui/eco.go`; MY is `ui/watchlist.go`. Full-text search is T142e.

## The Backtest tab (UI)

The topbar has four tabs, named for what they operate on rather than for how
advanced they are: **Backtest** (the form below), **Config** (the raw TOML and
Lua editors), **Data** (a SQL console over the database) and **Research**
(look a company up — see [The Research tab](#the-research-tab-ui)).

The desktop app opens on the **Backtest** tab: a form for the things every
backtest needs — name, starting capital, a date window with `Max`/`YTD`/`1Y`/`5Y`
presets, and a list of tickers with allocation percentages. It generates the
TOML for you, and **Edit as TOML →** hands that config to the **Config** tab's editor
when you want the full schema.

### The Lua editor knows the strategy API

The app's Lua tab completes and explains all 34 globals a strategy can call:
type `sm` and get `sma(ticker, day, period)` with its documentation, accept it
and the cursor lands on the first argument. Hovering any name shows the same
card. On an empty file the first completion is `step`, the one function every
strategy defines.

The descriptions are served by the engine (`backtest.LuaAPI`), not copied into
the frontend, and `TestLuaAPICoversEveryGlobal` parses the package for its own
`SetGlobal` calls and fails if the two sets differ in either direction. A
global added to the engine cannot ship without its documentation, and the
editor can never offer one that does not exist — which would be worse than
offering nothing, since you would only find out on day 0 of a run.

### Reading a library strategy, and editing your own copy

Every strategy picked on the Backtest tab opens in the Lua tab in a buffer of
its own, and its card there has a **Read the script →** link that goes
straight to it — so what a strategy actually does is one click from where it
was chosen. Each open script is a chip above the editor; 🔒 marks a library
script and • unsaved work.

A library script is **read-only**. It is what the gallery describes and what
every config naming it runs, so an edit would quietly change other runs. To
change one, **Edit a copy**: it opens `strategies/my_<name>.lua` — your copy,
first line relabelled, created on Save — or your existing copy if there
already is one, so a second click never overwrites work. Run it with **▶ Run
this script**, or name it in a config as `Strategy =
"lua:strategies/my_<name>.lua"`. The Assistant's "Insert into Lua editor"
lands in the scratch file when a library script is showing.

A portfolio that omits `Strategy` runs the script the config names (or the
scratch file), not whichever chip is open — reading a library script never
changes what a run does.

### Starting a script, and running it

The Lua tab opens on `strategies/my_strategy.lua`, your scratch file — a blank
`step(day)` until you first save it, so Save never changes a library strategy.
A blank file says nothing about how a strategy is shaped, though. Above the editor, while the buffer holds no
unsaved work, are three **starting shapes**:

| shape | for |
|---|---|
| A signal, per ticker | decide about each holding on its own — a moving average, an RSI level, a breakout |
| Rebalance on a schedule | hold everything and put the weights back every so often; no signal, no timing |
| Rank and hold the top few | score every ticker against the others and hold the best — momentum, value, low vol |

Each runs as written and has one `TODO` where the idea goes, and each is put
through the app's own ✓ Check by the engine's tests — a skeleton that does not
run teaches a beginner that they broke it. Picking one moves the path to
`strategies/my_strategy.lua`, so it can never overwrite the script that was
open, and they are only offered when the buffer is unmodified: an unmodified
file is still on disk, so replacing it loses nothing and there is no "are you
sure" to get wrong.

**▶ Run this script** runs the open script against the holdings already on the
Backtest tab — every portfolio there, pointed at this file instead of whatever
it was going to run. Without it, trying an idea means going to the Config tab,
writing a portfolio around it in TOML, and coming back, which is a lot to learn
before finding out whether the idea does anything at all.

### Seeing what the strategy saw: `plot()`

A strategy can record a number per day with `plot(name, value)`, and each name
becomes a small chart under **What the strategy plotted**, drawn against the
same days as the equity curve. It is how a person finds out *why* a run did
what it did — the moving average being crossed, the z-score being waited on —
without reading it out of a thousand `log()` lines.

Separate charts rather than extra lines on the equity curve, deliberately:
that axis is in dollars, and an RSI between 0 and 100 drawn on a $184,000 axis
is a flat line along the bottom.

One value per name per day (the last call wins), at most 8 names, series start
where you start plotting them, and NaN/infinity are dropped. Both bounds report
themselves once as a diagnostic rather than silently losing data. Nothing here
reaches a metric: it is an instrument, not a measurement.

### An error marked on its line

Every diagnostic that carries a Lua position — a raised error, a syntax error,
anything the script check found — arrives with the script and line parsed out
of it (`backtest.ScriptPosition`, done once in `diagf` so no consumer parses
messages itself). Those positions are drawn as markers in the editor's gutter
for the script that is open.

Both formats are recognised, which matters more than it sounds: a raised error
reads `mine.lua:41:` and a *syntax* error reads `mine.lua line:3(column:3)`,
sharing no punctuation — and the syntax error is the one whose line is
certain.

### Checking a script before running it

The Lua tab's **✓ Check script** compiles the script, requires its `step(day)`,
and then *runs that function* for three days against a two-ticker synthetic
history built in memory. It opens no database and simulates no market, so it
answers on a keystroke.

Running it is the part that matters. Anything that only compiles the file will
never execute the body, and the body is where the mistakes are — a misspelled
global, a `nil` compared with a number, arguments in the wrong order. Those are
reported as **warnings**, not errors, and the message says what they ran
against: a script that needs fundamentals gets `nil` from this history and may
raise here while being perfectly correct in a real run. Only the unambiguous
failures are errors — a syntax error with its line, no `step` at all, a `step`
that is not a function, an empty file.

**A name nothing defines is an error, not `nil`.** While the check runs, the
globals table carries a metatable that raises on reading a name the engine
never registered and the script never assigned, with the closest real name
offered: *unknown name "smaa" — did you mean "sma"?* This is Lua's worst trap
for a beginner. `smaa(t, day, 20)` is a call on nil and at least raises; `local
n = my_thresold` is simply nil, forever, and a strategy that quietly never
trades looks exactly like one that decided not to.

It is installed **in the check only**. A real run is untouched: a config that
ran yesterday produces identical numbers today, and a script that relies on
reading an undefined global as nil still does. Assigning a global declares it,
so a script keeping its own state (`held = false`, then reading `held`) is
unaffected — as is the whole standard library, since the metatable only ever
sees names the table does not have.

It also reports a `[portfolio.Params]` key the script never reads, which in a
sweep means every trial ran identical settings. The reverse is deliberately not
reported: `params.period or 20` is ordinary Lua.

An endless loop at the top of a script is bounded rather than fatal — the check
runs under a two-second deadline, because hanging the app is the one failure a
checker must not have. Every one of the twelve shipped strategies passes it,
which is what `TestCheckPassesEveryShippedStrategy` exists to keep true.

### Basics, or everything

The metrics table has two views, chosen in its header:

- **Basics** — final value, profit, annual %, max drawdown, Sharpe, and the
  strategy and portfolio names. Plus, whenever the run produced them,
  *Contributed*, *Missed W/D*, *Confidence* and *Alpha %*.
- **Everything** — the full table, exactly as it has always been.

The four extras in Basics are not a judgement call about importance. They are
there because hiding them makes a visible column lie: Profit without
*Contributed* reads as investment return when part of it is money paid in; a
swept Sharpe without *Confidence* reads as edge when it may be selection; and
a withdrawal plan that ran out of money says so in *Missed W/D* and nowhere
else.

The choice is remembered, and Basics is what a first run shows. Nothing is
removed by it — every hidden column is one click away, with its definition
still on hover.

### The form is remembered between launches

Everything on the Backtest tab — the portfolio cards, holdings, allocations,
window, strategy, benchmark, contribution schedule and the pre-registered
claim — is saved as you edit it and restored next launch. A first session is
exploratory, and closing the app to think about a result should not cost the
portfolio you built to get it.

Three things are deliberately *not* remembered:

- **the results.** A stale result beside a fresh form reads as today's numbers.
- **the database and config paths.** They may have moved, and the app resolves
  them properly on launch (see *Where the app looks for its files*).
- **anything that no longer matches the current form.** A stored session is
  input like any other: it survives upgrades, so it is validated field by
  field and dropped *whole* if any part of it fails. Half a restored form —
  holdings with no allocations, a criterion with no metric — is worse than an
  empty one, because you cannot see what is missing.

Stored under `backtester-session-v1` in the app's local storage; the version
is in the key, so a shape change re-defaults rather than restoring a form the
app no longer understands.

### Starting from a question

An empty form asks for holdings, two dates, a strategy, an allocation and a
success criterion before anything has happened — five decisions a first-time
user has no basis to make. So while no portfolio has holdings, the tab offers
**starter examples**, each labelled with the question it asks:

| example | the question it asks |
|---|---|
| Hold the big five | What would simply holding the five biggest tech names have done? |
| Chase the winners | Does rotating into whatever has been rising beat holding all of them? |
| Step out of falls | Does selling when the market drops below its average beat sitting through it? |
| Drip money in | Is paying in a bit each month better than investing it all at once? |

One click fills the holdings, the window, the strategy, the benchmark and a
matching pre-registered hypothesis — a complete run you are then meant to
**edit**, which is a much easier first act than assembling one from nothing.

*Chase the winners* is written as [rules](#portfoliorules), so its button and
its card read as a sentence — "Every 21 trading days (about monthly), hold the 2
stocks that rose most over the last 126 trading days, counting only those up
over the last 126 trading days…" — rather than as a script you would have to
open. It is the exact twin of `momentum_rotation.lua` (see below). *Step out of
falls* and *Drip money in* stay scripts and say why on their buttons: one buys
and sells on the crossing itself, which a target-weight rule set does not
reproduce, and the other is driven by deposits, which no rule describes.

**Library scripts with a rules twin.** Two shipped scripts can be said exactly
by the menu, at their default parameters:

| script | the same strategy as rules |
|---|---|
| `momentum_rotation.lua` | `Rebalance = "monthly"`, `Rank = "return_126"`, `Pick = 2`, `Filter = "rising_126"` (the script only funds winners with positive momentum) |
| `rebalance.lua` | `Rebalance = "quarterly"`; its optional `weights` are `Weight = "allocations"` |

Pick either on the Backtest tab and its card shows the sentence under the
works/fails text, with **Use as rules →** to switch the card over.
`TestLibraryTwinsMatchTheirScripts` (`src/backtest/rules_test.go`) runs each twin
against its script and requires the same equity curve to the cent, so the two
cannot drift apart; the Lua originals stay in `strategies/` unchanged. Nothing
else in the library has an exact twin — `trend_following.lua` was tried as a
daily `above_sma_200` filter and differs from the first trade, because the
script sizes with `buy_max` rather than to a target weight.

Every example is built against the database you have loaded: symbols it does
not carry are dropped, the window is clamped to what the remaining holdings
**and** the benchmark both cover, and an example that cannot be made runnable
is not offered at all. So clicking one always produces a run rather than a red
date error. They vanish once a portfolio has holdings, and return if you clear
it.

### When you do not know what to hold

The ticker box searches company names as well as symbols, which helps if you
know you want Apple. If you do not know what you want at all, an empty holdings
box offers **starter baskets** — big tech, the whole market as one fund, or a
spread across five sectors — and one click adds the lot at equal weight.

Like the examples, a basket is filtered to symbols your database actually has
and is withdrawn entirely if too few survive, so "spread across sectors" never
resolves to two names. They appear only once something else in the form has
holdings: on a wholly empty form the starter examples above are the better
answer, and offering both would be two answers to one question.

### Copying a famous investor

Every portfolio card has a **Copy a famous investor** dropdown in its
*Holdings & allocation* box, so one can run next to your own; an empty form
also lists them as cards under the starter examples. Picking one **replaces**
the card's holdings, weights, dates and strategy (capital and any cashflow
schedule are kept), and the card then shows that portfolio's caveat. One your
database cannot build stays in the list, disabled and marked *not in this
database*. The portfolios:

| portfolio | holdings | held how |
|---|---|---|
| Buffett 90/10 | `$SP500` 90, `$CASH` 10 — his instructions for his own estate (2013 letter) | `rebalance.lua`, 30 years |
| Berkshire 13F, Q4 2015 | the filing's 25 largest holdings at its dollar weights (WFC, KHC, KO, IBM, AXP, …), less any your database cannot price | bought 2016-02-17 and held |
| Dalio All Weather | VTI 30, TLT 40, IEF 15, GLD 7.5, DBC 7.5 | `rebalance.lua`, 20 years |
| Browne Permanent Portfolio | VTI, TLT, GLD, `$CASH` at 25 each | `rebalance.lua`, 20 years |
| Swensen portfolio | VTI 30, VEA 15, VWO 5, VNQ 20, TIP 15, IEF 15 | `rebalance.lua`, 15 years |

**There is deliberately no "Buffett's current holdings".** Today's portfolio
run back over ten years is a list of what worked, chosen with the ending
already known — it says nothing about what copying him would have earned. So
every entry is something a person could have bought on the window's first day:
a published recipe, or a **13F bought the day after it was filed** (Berkshire's
Q4 2015 report went public on 16 Feb 2016, SEC accession
0000950123-16-015025). A database whose data does not cover that day withdraws
it rather than starting it elsewhere.

Each shows its caveat: the recipes rebalance quarterly where their
authors gave a band or nothing; years before a recipe was published (All
Weather, 2014) test the recipe, not anyone following it; and the 13F leaves out
Precision Castparts (bought outright before the filing) and anything your
database cannot price, scaling the rest to 100%. Weights are filled as exact
percentages, and picking one also registers a hypothesis, as the examples do.
On the bundled database the 13F copy made 11.2%/yr against the S&P 500's 15.7%
over the same days.

`rebalance.lua` now takes a custom split from the form as its `weights`
Param, the same table buy-and-hold reads; every other library strategy still
sizes its own positions. A `[[Portfolio]]` block for each lives under
*Famous investors* in `strategy_library.toml`.

### Importing the portfolio you already hold

**Import…**, beside *Add* in the holdings box, fills the card from a file of
what you own: a **CSV** (comma, tab or semicolon), an **Excel `.xlsx`** (first
sheet), or plain text with the columns separated by spaces — a table pasted out
of a spreadsheet or a web page works as-is:

```
Asset    Value(USD)
VOO      11397
BRK-B    3530
Cash     60569
```

- **Columns are found, not assumed.** The header is the early row naming the
  most recognisable columns (`Symbol`/`Ticker`, else `Asset`/`Holding`/`Name`…,
  and `Value`/`Market Value`/`Amount`/`Balance`, else `Weight`/`%`), so a
  brokerage export with account details above its header still reads. Without
  a header, the first column is the asset and the rightmost numeric column the
  amount. `$8,889`, `(1,200)` and `4.5%` all parse.
- **Amounts become exact percentages** of what matched, to two decimals and
  summing to exactly 100 — and the allocation boxes now accept decimals. Dollar
  amounts also set the **starting capital** to their sum, so the backtest starts
  from your real book; a file of weights leaves capital alone. One symbol on two
  rows (two accounts) is merged. Total lines and zero or negative amounts are
  skipped and listed.
- **A `Cash` line becomes `$CASH`**, the T-bill series — never the stock `CASH`,
  which is Pathward Financial. `USD`, `Money Market`, `Cash & Equivalents` and
  a few broker spellings are treated the same.
- **Spellings are reconciled**: `BRK.B` / `BRK/B` / `BRK-B` resolve to whichever
  the database has, and `BTC` to `BTC-USD` when only that exists.
- **Nothing disappears silently.** A note under the box says what was mapped,
  and which symbols the database cannot price were left out and what share of
  the file they were; the rest is scaled to 100%.

The import **replaces** the card's holdings and switches it to custom weights.
Legacy binary `.xls` is refused with a message to save as `.xlsx` or `.csv`.
Parsed in Go (`ui/holdings.go`, `App.ImportHoldings`) with the standard library
only. Holdings that started trading recently (an IPO in 2024) pull the window
forward like any other ticker — the dates re-snap to what every holding covers.

### Several portfolios in one run

**+ Add portfolio** appends another card, and the run carries all of them: one
`[[Portfolio]]` block each, one line per portfolio on the equity chart, one row
each in the metrics table. That is what makes a result a comparison rather than
a number — and the runner and results view have always supported it. Until now
the form emitted exactly one block, so the only way to compare two ideas was to
hand-write TOML, which is a strange thing to ask of the tab the app opens on.

A new portfolio inherits the previous one's window and starting capital (two
portfolios over different date ranges are not a comparison) with no holdings
and a fresh name. Each card carries its own tickers, dates, strategy, benchmark
and coverage verdict. **Run is enabled only when every portfolio is runnable** — and when it is
not, a line beside it says what to fix first (*To run, B: add at least one
holding.*), from the same check that disables the button (`runBlocker` in
`portfolioCheck.ts`), so the two cannot disagree —
a run carries all of them, so one broken card stops the run rather than quietly
contributing a bad block — and **two portfolios may not share a name**, because
the name is how every result, trade and verdict is labelled.

The pre-registered hypothesis stays **one per run**, not one per portfolio: the
engine registers a single claim and judges every result against it, so a box per
card would invent a concept the engine does not have.

A single portfolio generates byte-for-byte the config it always did.

### Recurring contributions, on the form

The **Portfolio** card carries a *Deposit / Withdraw* dropdown, an amount, and a
frequency (Never / Monthly / Quarterly / Annually). **Never** is the default and
emits no `[portfolio.Cashflow]` block at all, so a form you have not touched
generates exactly the config it always did — which is also why there is no
separate enable checkbox.

Direction is a dropdown rather than a minus sign. The TOML field is a signed
`Amount`, but a currency box that means the opposite thing when you type `-` is
a trap, and a mistyped sign turns a contribution plan into a decumulation one.

A line under the row says what it means in words — *"$500 added on the first
trading day of each month, beginning one month after 2015-06-01"* — because
"monthly" hides both the first-trading-day-on-or-after rule and the one-period
delay, and those are exactly what someone reconciling against a spreadsheet
will trip on.

It is **per portfolio**, so lump-sum against monthly-contributions is a
two-card comparison on one chart.

### The ticker picker knows what a symbol is

Typing in the holdings box searches **symbols and company names** and shows
both, so `apple` finds `AAPL` and `AAPL` reads as "Apple Inc." rather than as
four letters you have to already recognise. Matches rank symbol before name and
prefix before substring, so `AA` still gives you `AA` and `AAPL` first. Arrow
keys move, Enter takes the highlighted row (or whatever you typed, if none is),
Escape closes. The names also sit beside each selected holding in the
allocation list.

Names come from `App.TickerNames`: the optional `company_profile` table
(SEC EDGAR, loaded by `add_company_info.py`) plus built-in labels for the ten
`$`-prefixed benchmark series, which are synthetic and have no filer behind
them. **Both sources are optional and coverage is partial** — about 5,600 of
the priced symbols have a name, because `company_tickers.json` lists only
current filers. A database with no `company_profile` table shows bare symbols
and works exactly as it did before. Symbols carrying a generation suffix
(`COR.1`, from `split_recycled.py`) are deliberately left unnamed: that series
is the symbol's *previous* occupant, a different company, and the current
filer's name would be wrong rather than missing.

**Strategy** is a picker fed by the shipped library's gallery headers
(`App.ListStrategies` over `backtest.LoadGallery`). Choosing one shows what it
does, what it needs to work, and — the useful part — how it fails, before the
run rather than after a bad result:

> **Works when** trends run far beyond the point they look extended.
> **Fails when** range-bound markets: every breakout fails back into the
> range, and the win rate is low by design even when it works.

The default is buy-and-hold, which is the mode's original behaviour: it runs
`strategies/buy_and_hold.lua`, splitting equally, or passing your custom
percentages as `[portfolio.Params] weights`. Deposits from a cashflow schedule
are invested the day they arrive, split the same way. Picking a library strategy emits
`Strategy = "lua:strategies/<file>"` and runs it on its own defaults —
the strategy sizes its own positions, so the allocation percentages are not
consulted and the form says so — except `rebalance.lua`, which holds them as
its target weights. To tune its parameters, switch to Advanced and
add a `[portfolio.Params]` block.

**Compare it against** sets `Benchmark`, defaulting to `$SP500`. This is the
one simple-mode default that is *not* a zero value, and it is deliberate: with
`Benchmark` unset, Alpha, Beta, TrackingError, InformationRatio and
Up/DownCapture are all zero and "did this beat just buying the market?" — the
question a user actually has — is unanswerable. The portfolio's own numbers do
not change; the benchmark is scored against, never traded, and stays out of
`Tickers` so it receives no capital and does not constrain the trading
calendar. Choose *Nothing — skip the comparison* to get the previous output
back exactly.

The engine skips every benchmark-relative metric unless the benchmark has a bar
for **every trading day in the window**, and says so only in a warning diagnostic
(the form keeps the window inside the benchmark's for this reason).
The Backtest tab checks it up front instead: the `$`-series currently end weeks
earlier than the priced tickers (`$SP500` to 2026-07-02 against 2026-08-07 for
real symbols), so the date presets snap to the earlier of the two ends, and a
hand-typed end date past it gets a warning with a one-click fix.

**Before you run** is pre-registration, and it is the reason the mode exists in
this order: you say what you think is true, and what would count as being
right, *before* seeing a number. The engine has judged hypotheses mechanically
for a while, but it wanted hand-written TOML, so nobody on the Backtest tab ever saw
a verdict. The card builds the same document from a claim and a
metric/operator/number triple — which is the whole grammar `ParseCriterion`
accepts:

```toml
ID          = "H-11eyu7q"
Claim       = "12-1 momentum beats buy-and-hold"
Criterion   = "sharpe >= 1.2"
```

The metric list is served by the engine (`App.CriterionMetrics` over
`backtest.CriterionMetricNames`), so the picker cannot offer a criterion the
parser would reject. The `ID` is derived from the question itself, which makes
*"a changed question is a new ID"* structural rather than a rule to remember:
edit the claim or the threshold and the next run registers a new id, while
re-running the same question keeps it. Leaving the claim empty runs the
backtest unjudged — different from judging it against a default nobody chose.
**Edit as TOML →** carries the question across to the Config tab's raw box.

`my_strategy.lua` is deliberately absent from the picker: it is the Lua tab's
scratch file, not a strategy. `buy_and_hold_weighted.lua` and `example.lua`
were folded into `buy_and_hold.lua`; a saved config that still names either
runs `buy_and_hold.lua`.

### Reading the results

The results view leads with what decides whether to keep looking: the
hypothesis verdict, the "does this survive the obvious objections?" strip, the
equity curve, and the metrics table. Everything below those — walk-forward
windows, in-sample vs out-of-sample, the ten deepest drawdowns, trading,
attribution, calendar-year and monthly returns — is **collapsed behind its own
title** and opens on click, with the choice remembered across runs.

A panel that has nothing to report is hidden rather than collapsed, so an
absent title means absent data (no `[portfolio.Sweep]`, no
`[portfolio.Validation]`, no trades) rather than a closed section.

**Any panel can leave the strip for a window of its own.** Double-click its
header, or press **⤢** at its right edge. The window drags by its header,
resizes from its bottom-right corner, maximises on another double-click (or
**□**), and goes back with **×** or **Esc**; clicking a window brings it to
the front. The equity curve grows to the window's height, so popping it out is
how to read it at a real size. The strip keeps a one-line stub — *in its own
window* — with **Show** and **Dock** buttons, and which panels are out, and
where, is remembered across runs and launches. A window is always pulled back
on screen, whatever the display did since.

The bar between the tab and the results drags to resize and remembers its
height; so does the assistant panel's left edge (double-click that edge to
reset its width). Its **▁** button minimises the results down to the bar,
giving the tab the whole window, and **□** maximises them over the tab;
pressing the same button again (**▴** / **❐**) restores, and dragging the bar
restores from either. **Double-clicking the bar** steps through the same
states like a window's title bar: minimised → normal → maximised → normal.

**The results start minimised** at every launch — results are never restored,
so an open panel would only be an empty box — and open by themselves the
first time a run produces results. Maximised never survives a restart, so
the app never opens with its tabs hidden.

The **assistant** maximises the same way: double-click its header bar (not
its model picker or buttons), or its **□** button, and it takes the whole
row while the tabs step aside; again (or **❐**, or Esc) restores.

### Keyboard shortcuts

**F1** (or the **⌨** chip in the top bar) lists them. **On a Mac every one is
pressed with Cmd instead of Ctrl** — Cmd+Shift+P, Cmd+Enter, Cmd+1 — and the
list, the palette and the button tooltips print Cmd there. Zoom follows the
browser convention, Ctrl/Cmd with **=**, **-** and **0**; Ctrl+↑/↓ and
Ctrl+scroll still work too, but not with Cmd, since Cmd+arrow moves the caret
on a Mac.

| keys | does |
|---|---|
| Ctrl+Shift+P | **Command palette** (Ctrl+K also opens it): every action by name — type any words of it, ↑/↓, Enter. Includes the actions with no key (Check config/script, Open config file…, Choose database…, zoom), and shows each one's shortcut, so it also teaches them. An action that does not apply here (Save on the Backtest tab) is listed greyed out with the reason. |
| / | Jump to the ticker search on the Backtest tab (only when you are not typing in a field — there it types a `/`) |
| Ctrl+Enter | Run the backtest — the visible Run button, so it is refused exactly when the button is disabled, and then the status line says why (*Can't run yet: add at least one holding.*). On the Data tab it runs the query, as before. |
| Ctrl+1 / 2 / 3 / 4 | Backtest / Config / Data / Research tab (Ctrl+4 also puts the cursor in the Research command line) |
| Ctrl+J | Show or hide the assistant |
| Ctrl+Shift+J | Maximise or restore the assistant |
| Ctrl+M | Minimise or restore the results |
| Ctrl+Shift+M | Maximise or restore the results |
| Ctrl+S | Save the config and scripts (Config tab); never the webview's "save page" |
| Esc | Close the palette or the shortcut list, else restore a maximised pane; otherwise it reaches whatever has focus |
| Ctrl+= / Ctrl+- / Ctrl+0 | Zoom in / out / reset (also Ctrl+↑ / ↓ and Ctrl+scroll) |

**While a run is going** the status line counts up beside *Running…*
(`7s`, `1m 05s`), so a long run reads as working rather than frozen. The
timer is its own tiny component: its once-a-second tick redraws that span
and nothing else.

**Startup.** The code editor (Monaco) is most of the app's JavaScript and the
Backtest tab never shows it, so it is not in the startup bundle: it loads in
the background just after the first screen draws (`monacoSetup.ts`,
`LazyEditor.tsx`), and is ready long before the Config or Data tab is opened.
The startup bundle went from 4.0 MB to 263 KB.

The table lives in `ui/frontend/src/shortcuts.tsx`; the overlay, the palette
(`commandPalette.tsx`) and the button tooltips read it, and a key and a palette
pick run the same code, so they cannot drift from what the keys do.

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
- It cannot refresh the whole database — that is `ui --sync <db>`, run from
  a terminal with the app closed, and the assistant will tell you so. See
  [Keeping the price data current](#keeping-the-price-data-current).
- It can query the database itself through a read-only SQL tool
  (SELECT-family statements only, capped at 100 rows) — e.g. to check
  ticker coverage before proposing a date range. Queries it runs are shown
  inline in the chat. Local models get the tool too if they advertise the
  `tools` capability (qwen3.5 does); models that don't simply chat over
  the embedded context.
- **It writes strategies as rules first.** Asked to turn an idea into a
  strategy, it proposes a [`[portfolio.Rules]`](#portfoliorules) block and
  writes Lua only when the menu cannot say what you mean — and then says
  what the menu was missing. Before showing a rule set it calls the
  `check_rules` tool, which runs the rules through the same construction a
  backtest does and returns either the errors or the engine's own sentence
  for them (`DescribeRules`) plus the window the database covers for those
  tickers. The assistant shows you that sentence verbatim, so what you
  confirm is the engine's reading of the strategy, not the model's
  paraphrase. When your idea leaves out how often to re-decide or what to
  hold when the market turns, it proposes a default and says so ("checked
  monthly — say if you meant weekly") rather than choosing silently.
  Small local models follow this less reliably than Claude: in testing,
  qwen3.5:4b called `check_rules` on about half of attempts and otherwise
  wrote rules from memory, once with an entry that is not on the menu — the
  app's ✓ Check catches those before a run.
- Fenced ` ```toml `/` ```lua ` blocks in its replies get an
  **Insert into … editor** button, so a generated config or strategy lands
  directly in the corresponding editor tab.
- Setup: Claude models need an Anthropic API key — set
  `ANTHROPIC_API_KEY` in the environment or a `.env`, or paste it into the
  panel's first-run form, which writes it for you. The form writes the
  repo-root `.env` when there is one (a checkout, alongside the other project
  keys) and `~/.config/backtester/.env` when there is not — an installed
  binary may sit somewhere unwritable, and silently failing to save a key
  someone just typed is the worst available behaviour. Reads go the other way
  round: environment, then `~/.config/backtester/.env`, then the project's,
  so the last key you saved is the one in use. Local Ollama models need no
  key at all.
- Local-model notes: the request pins `num_ctx` to 16384 (Ollama's default
  is too small for the embedded context) and sends a leaner system prompt;
  "thinking" models have thinking disabled for snappier replies.
  The lean prompt is a *different, condensed reference* (`leanReference` in
  `ui/chat.go`), not the full one with the saved-config dump removed: the
  full reference is ~19k tokens on its own and does not fit `num_ctx` with
  a conversation and tool results on top, so a local model was silently
  losing the tail of its own prompt — which is where the tool routing
  lived. The lean one runs ~4.5k tokens with the live database context
  included, and keeps the config schema, the Lua surface, every metric
  name, the tool routing and the data traps; it drops narrative the model
  can recover with `query_db`. `TestLeanPromptFitsOllamaContext` budgets it
  at a third of the window so it cannot drift back over.

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
- `RealAnnualReturn` — `AnnualReturn` with inflation divided out: the same CAGR expressed in constant money, so a 10% year in 2021 and a 10% year in 2013 stop looking identical. Deflated by **`CPIAUCSL`** (CPI for All Urban Consumers, All Items, seasonally adjusted) from the optional `economic_indicators` table, endpoint to endpoint over the run's own window.

  It uses the **compounding** form — `(1 + nominal) / (1 + inflation)` — not `nominal − inflation`. The subtraction is an approximation that drifts as both the return and the window grow, which is exactly the decade-long case this figure exists for: 10% nominal against 3% inflation is **6.80% real, not 7%**.

  **Zero means *not available*, not "no real growth".** It is reported only when CPI has a reading for the months at *both* ends of the window. CPI for a month is published mid-way through the next one, so a run ending in the last few weeks will not have it — and deflating by a shorter span than the return covers would overstate the real return silently, in the flattering direction. The engine reports nothing instead, and the results panel shows an em dash. A database with no `economic_indicators` table behaves identically to before this metric existed.

  **Only the return is deflated.** The equity curve, `MaxDrawdown`, `CalmarRatio` and `LongestUnderwaterDays` stay nominal on purpose: a drawdown is a *nominal* experience — you watched the account fall by that much — and restating it in today's money would corrupt the figure users trust most to answer "how bad was it". `SharpeRatio` is already quoted on excess-of-risk-free returns, which prices the time value of money from a different direction; deflating on top would double-count.
- `StandardDev` — annualized stdev of daily returns.
- `NetContributions` — signed total moved by a `[portfolio.Cashflow]` schedule; deposits positive, withdrawals negative, `0` without one. Not profit — see above.
- `WithdrawalsMissed` — scheduled withdrawals that could not be funded even after selling every holding. Non-zero means the portfolio ran out of money partway through, which for a decumulation backtest is the answer rather than a footnote.
- `LongestUnderwaterDays` — the most calendar days the equity curve ever spent below a previous peak, measured from that peak until it was regained, and to the end of the run for a decline never recovered. `MaxDrawdown` says how *deep* the worst hole was; this says how *long* you would have had to sit in one. A 40% drawdown recovered in a month and a 15% one that took three years are not the same experience, and only the second gets abandoned. Computed over **every** drawdown, not the deepest few the results panel lists — the longest is frequently not the deepest.
- `CalmarRatio` — `AnnualReturn / MaxDrawdown`. Return per unit of worst-case decline, and a different question from Sharpe: Sharpe penalises volatility in both directions, Calmar counts only the deepest hole the account was ever in — which is what actually decides whether a strategy gets abandoned partway through. **A portfolio that never declined reports 0, not infinity**: an unbounded ratio is undefined rather than excellent, and reporting it would sort a strategy that never traded above every real one.
- `Turnover` — annualized gross traded notional as a multiple of the portfolio's average value; `1.0` means it traded its own value once over a year. Both sides of a round trip count. Reported whether or not `[portfolio.Costs]` is set.
- `AvgExposure` — the mean invested fraction of the book, as a percent.
- `TimeInMarket` — the percent of simulated days holding any position at all.

The last two are shown side by side because **neither is interpretable alone**.
An `AvgExposure` of 50% means "half invested every day" when `TimeInMarket` is
100%, and "fully invested half the days" when `TimeInMarket` is 50% — the same
average return per day, but very different strategies, and only the second is
sitting in cash waiting for something. Both read 0 for a portfolio that never
traded, which is the honest reading of a flat equity curve and is easy to
mistake for a 0% return.

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

1. Copy a file from `strategies/` and edit it. A strategy is one global `step(day)` function, called once per trading day with `day` counting from 0; the engine provides `price()`, `sma()`, `rsi()`, `cash()`, `cashflow()`, `position()`, `buy()`, `buy_max()`, `sell()`, `sell_all()`, and the `tickers` / `params` globals. Orders may only name a ticker from the `tickers` global — a typo is refused and reported as a run error rather than silently burning the cash. Full API, patterns, and gotchas: [`strategies/README.md`](strategies/README.md).
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
│                            #   add_ticker, sync_prices, update_treasury_yields, …)
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

## Licence

Three files, because three different things ship together and they are not
under the same terms:

| File | Covers |
|---|---|
| [`LICENSE`](LICENSE) | The software. A **beta evaluation licence** — free to use, including commercially, but not open source and not redistributable. The source is published to be read; reading it grants nothing further. Beta terms end at 1.0. |
| [`DATA-LICENSE.md`](DATA-LICENSE.md) | The bundled `stock_data.db`. **More restrictive than the software licence** — some of the data comes from providers whose terms forbid redistribution. Also the honest inventory of what is wrong with the data, which anyone acting on a result should read first. |
| [`THIRD-PARTY-NOTICES.txt`](THIRD-PARTY-NOTICES.txt) | The 14 Go modules and 11 npm packages compiled into the binary, including the DuckDB C++ library, which `duckdb-go` statically links from the per-platform `duckdb-go-bindings/lib/*` module. All permissive (MIT, BSD, ISC, Apache-2.0), and all of them require their notice to travel with the binary. |

`THIRD-PARTY-NOTICES.txt` is generated — regenerate it whenever a dependency
changes, because a stale notices file asserts something untrue:

```bash
python3 gen_notices.py
```

It reads `go list -deps` and `npm ls --omit=dev`, so it lists what is actually
linked and bundled rather than everything in `go.mod` and `package.json`.
