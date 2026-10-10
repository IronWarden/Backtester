# Data

Building a database, the benchmark series, fundamentals and macro data, delisted and recycled tickers, and keeping prices current.

[← README](../README.md) · [Installation and setup](installation.md) · **Data** · [Configuration](configuration.md) · [Command line](cli.md) · [The desktop app](app.md) · [Architecture](architecture.md)

## Build your own database

The database this project was developed against cannot be shared — its prices
come from Yahoo, whose terms forbid redistribution (see `DATA-LICENSE.md`). So
anyone else builds their own, from the same free sources, with one command:

```bash
pip install -r requirements.txt            # needs DuckDB 1.5.x, like the app
python3 scripts/build_db.py --db my.db --dry-run   # show the plan, download nothing
python3 scripts/build_db.py --db my.db             # build it (a few minutes)
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
[Prerequisites](installation.md#prerequisites).

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
python3 scripts/add_collections.py            # build/refresh all
python3 scripts/add_collections.py --dry-run  # preview coverage, no write
python3 scripts/add_collections.py --only '$SP500' '$CASH'
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
python3 scripts/add_sec_financials.py --dry-run --limit-quarters 1   # probe
python3 scripts/add_sec_financials.py --from-year 2020               # smaller
python3 scripts/add_sec_financials.py                                # 2009 -> now
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
strategy](configuration.md#fundamentals-in-a-strategy). From Go, read it with
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
python3 scripts/add_company_info.py --dry-run --limit 20   # probe, writes nothing
python3 scripts/add_company_info.py --dry-run              # full fetch, no write
python3 scripts/add_company_info.py                        # fetch and replace the table
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
python3 scripts/add_delistings.py --dry-run   # fetch, classify, report, write nothing
python3 scripts/add_delistings.py             # replace the table
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
python3 scripts/split_recycled.py            # dry run: classify and print, write nothing
python3 scripts/split_recycled.py --apply    # rewrite the bars
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
python3 scripts/add_index_membership.py --dry-run          # reconstruct and report
python3 scripts/add_index_membership.py                    # replace the table
python3 scripts/add_index_membership.py --from-year 2011   # only the dense era
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
python3 scripts/add_delisting_reasons.py --dry-run --from-year 2023 --cik 718877
python3 scripts/add_delisting_reasons.py --dry-run          # full harvest, no write
python3 scripts/add_delisting_reasons.py                    # replace the table
python3 scripts/add_delisting_reasons.py --emit-cik-list ciks.txt --no-classify
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
python3 scripts/add_data_quality.py --dry-run   # audit and report, write nothing
python3 scripts/add_data_quality.py             # audit and replace the data_quality table
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
| `$SP500`, `$CASH` and the other benchmark series | `python3 scripts/add_collections.py` |
| `3MTreasuryYields`, `10YrTreasuryYields` | `python3 scripts/update_treasury_yields.py` |
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
