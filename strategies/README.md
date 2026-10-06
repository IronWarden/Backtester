# Strategy library

Ready-to-run Lua strategies, plus the API for writing your own. Run them
all side by side with `strategy_library.toml` in the project root: open it
in Advanced mode and hit **Run backtest**.

**This is the only strategies directory.** A `Strategy = "lua:strategies/x.lua"`
spec is resolved by walking up from the working directory, so it finds this one
whether the process started at the repo root (CLI) or in `ui/` (the desktop
app). There used to be a second copy under `ui/`, and because the walk checks
the working directory first, the two files it held **shadowed** the library —
including an older weighted buy-and-hold script that floored to whole shares.
Do not recreate it: a script placed there silently wins over the one here.

## Using one

Point a portfolio's `Strategy` at the file and pass its parameters in a
`[portfolio.Params]` block:

```toml
[[portfolio]]
Name = "Momentum"
BuyingPower = 100000.0
StartDate = "2015-01-02"
EndDate = "2024-01-01"
Tickers = ["AAPL", "MSFT", "JNJ", "$CASH"]
Strategy = "lua:strategies/momentum_rotation.lua"

[portfolio.Params]
lookback = 126
top_n = 2
rebalance_days = 21
```

Every parameter has a default, so `Params` is optional. There are also two
built-ins that need no script: `buyAndHold:equalWeights` (split capital
evenly on day 0) and `buyAndHold:greedy` (spend it all on the first ticker).

## The gallery: what each one needs, and how it fails

Every script carries four tagged lines in its header — `@works`, `@fails`,
`@sweep` and `@baseline` — and a test requires all four of every shipped
script, so a strategy cannot join the library undocumented. They are
documentation for the gallery and nothing reads them at run time: delete them
from your own copy and it runs exactly the same. What a script needs in order
to *work* is code, never a comment — the `fundamentals` table below is the one
example. List them from the CLI, which needs no database
and no config:

```bash
cd src && go run main.go -list-strategies
```

**`@fails` is the one that earns its place.** Every strategy here works
somewhere; the useful question is where it does not, and that is what a library
of examples usually omits.

| Strategy | Works when | Fails when | Sweep |
|---|---|---|---|
| `bollinger_reversion.lua` | the market oscillates around a stable average, so a band touch is a stretch rather than a new trend | a sustained downtrend, where it buys each new low as the band keeps moving down with the price | `period = [10, 20, 50], k = [1.5, 2.0, 2.5]` |
| `buy_and_hold.lua` | the asset rises over your window — the default answer, and the one most strategies fail to beat | you cannot sit through the drawdown it takes on the way; it has no exit and never goes to cash. With weights, one name can run away and quietly become the whole book | `nothing numeric to sweep; vary the UNIVERSE instead, which is what actually decides this one. rebalance.lua is the version that holds its weights` |
| `dca.lua` | the market falls before it recovers, so the later buys land cheaper | the market rises in a straight line: every day held in cash is a day of lost return, and it will lose to lump-sum roughly two thirds of the time | `every_days = [5, 21, 63], amount = [2500.0, 5000.0, 10000.0]` |
| `donchian_breakout.lua` | trends run far beyond the point they look extended — it takes many small losses for a few large winners | range-bound markets: every breakout fails back into the range, and the win rate is low by design even when it works | `entry_period = [20, 55, 100], exit_period = [10, 20, 40]` |
| `gross_profitability.lua` | gross profit is the cleanest measure of how much a business earns from what it sells, before accounting choices lower down the income statement; Novy-Marx (2013) found it predicts returns about as strongly as book-to-market | a high gross margin spent on overhead — it ignores SG&A and R&D, so a company burning cash below the gross line still ranks well; and asset-light companies win by construction, whatever their prospects | `top_n = [5, 10, 15], rebalance_days = [63, 126, 252]` |
| `low_asset_growth.lua` | companies that grow their balance sheets fastest — acquisitions, capex binges, issuing stock — go on to underperform (Cooper, Gulen and Schill 2008); it is the investment factor in the Fama-French five-factor model | growth that pays off — it avoids every company compounding assets because its investments work, and a shrinking balance sheet can be write-downs and distress rather than discipline | `top_n = [5, 10, 15], rebalance_days = [63, 126, 252]` |
| `momentum_rotation.lua` | leadership persists for months at a time, which is the historically documented effect | a sharp reversal: momentum crashes are its signature failure, and it is fully invested in last quarter's winners when one arrives | `lookback = [63, 126, 252], top_n = [1, 2, 3], rebalance_days = [21, 63]` |
| `piotroski_fscore.lua` | cheap or neglected companies whose fundamentals are quietly improving — Piotroski (2000) built it to separate winners from losers among value stocks, where it does most of its work | on a list of large, healthy companies most score 5-7 and it sorts noise; and the signals are year-over-year changes, so a company that is excellent but stable earns fewer points than a mediocre one recovering | `min_score = [6, 7, 8], max_names = [5, 10], rebalance_days = [63, 252]` |
| `quality_roe.lua` | highly profitable businesses keep earning high returns for years and the market underprices that persistence — the quality factor | ROE is high because equity is tiny: leverage and heavy buybacks shrink the denominator, so it drifts toward indebted companies; and in junk rallies (2009, 2020-21) the least profitable names lead | `top_n = [5, 10, 15], rebalance_days = [63, 126, 252]` |
| `rebalance.lua` | the holdings are volatile and take turns leading, so trimming the winner funds the laggard before it recovers | one asset trends far ahead of the rest: rebalancing sells the winner all the way up, which is the classic drag against a runaway leader | `rebalance_days = [21, 63, 126, 252]` |
| `risk_parity.lua` | volatility is a stable predictor of risk and the low-vol assets are genuinely safer | a low-volatility asset breaks — quiet things get the biggest weights, so the one that surprises you does the most damage | `lookback = [21, 63, 126], rebalance_days = [21, 63]` |
| `rsi.lua` | the market is range-bound and oversold readings mean stretched rather than falling | a sustained downtrend: oversold gets more oversold, and this buys the whole way down | `period = [7, 14, 21], buy_thresh = [20, 30, 40], sell_thresh = [60, 70, 80]` |
| `sma_cross.lua` | the market makes sustained multi-month trends in one direction | a choppy sideways market whipsaws it: every false crossing is a round trip paid for in costs | `short = [10, 20, 50], long = [50, 100, 200]` |
| `strong_balance_sheet.lua` | credit stress — selloffs that punish borrowers (2015-16 energy debt, the 2022 rate shock) hit lightly-indebted companies least | long bull markets with cheap money, where leverage is rewarded and these lag; and banks, insurers and utilities run on borrowed money by design, so they never qualify — partly a sector bet | `top_n = [5, 10, 15], rebalance_days = [63, 126, 252]` |
| `trend_following.lua` | prices trend persistently above or below a long average | chop around the average, where the buffer is crossed repeatedly and each crossing is a trade | `period = [50, 100, 200], buffer_pct = [0.0, 0.01, 0.03]` |

Paste a `@sweep` line into `[portfolio.Sweep]` to search it — that is what the
ranges are for. Run `-scan-signals` first to see which signals have any
predictive power on your universe before choosing, and remember that every
result is already compared against equal-weight buy-and-hold automatically.

## The catalog

Each file documents its own parameters at the top. Copy one and edit it —
that is the intended way to build something custom.

| Strategy | Idea | Key params |
|---|---|---|
| `buy_and_hold.lua` | Buy on day 0 — equally, or at `weights` — and hold to the end, never rebalancing. Scheduled deposits are invested the day they arrive, split the same way. | `weights`, `buyType` |
| `dca.lua` | Invest a fixed dollar amount on a cadence, never sell. | `amount`, `every_days` |
| `rebalance.lua` | Hold target weights, rebalance on a cadence — trims winners, tops up losers. | `rebalance_days`, `weights` |
| `risk_parity.lua` | Weight by 1/volatility so each holding contributes similar risk. | `lookback`, `rebalance_days` |
| `momentum_rotation.lua` | Hold the top N by trailing return, rotate on a cadence, cash out when nothing has positive momentum. | `lookback`, `top_n`, `rebalance_days`, `skip_days` |
| `trend_following.lua` | Hold each ticker only while it is above its own moving average. | `period`, `buffer_pct` |
| `sma_cross.lua` | Golden/death cross: fast average over slow. | `short`, `long` |
| `donchian_breakout.lua` | Buy N-day highs, exit on M-day lows (turtle style). | `entry_period`, `exit_period` |
| `rsi.lua` | Buy oversold, sell overbought. | `period`, `buy_thresh`, `sell_thresh` |
| `bollinger_reversion.lua` | Buy below the lower band, exit at the average. | `period`, `k` |
| `quality_roe.lua` | Hold the top N by return on equity (SEC filings). | `top_n`, `rebalance_days`, `min_names` |
| `gross_profitability.lua` | Hold the top N by gross profit / total assets (SEC filings). | `top_n`, `rebalance_days`, `min_names` |
| `piotroski_fscore.lua` | Score each company 0–8 on whether its last annual report shows it getting healthier; hold the high scorers (SEC filings). | `min_score`, `max_names`, `rebalance_days` |
| `low_asset_growth.lua` | Hold the N whose total assets grew least last year (SEC filings). | `top_n`, `rebalance_days`, `min_names` |
| `strong_balance_sheet.lua` | Hold the top N by equity / assets among cash-generative companies (SEC filings). | `top_n`, `rebalance_days`, `min_names` |

**Pick a universe that matches the family.** Per-ticker signal strategies
(trend, breakout, RSI, Bollinger) judge each ticker independently, so give
them a basket of individual names. Allocation strategies (rotation, risk
parity, rebalance) divide capital across the universe, so they are most
interesting across asset classes — e.g. `["$SP500", "$CASH"]`.

### Fundamentals strategies

The five marked *SEC filings* rank companies on their reported figures. Each
declares what it reads in a `fundamentals` table at its top, and the engine loads
exactly that whenever the portfolio's config declares no `Fundamentals` of its
own — so choosing one as the strategy is all it takes, from the Simple form or
the Config tab alike. `strategy_library.toml` has a ready block for each, over one list of large
companies from 2015-06. Things to know before reading their results:

- **They need `sec_financials`** (`python3 add_sec_financials.py`). Without
  it every `fundamental()` is `nil`, and they hold nothing at all — by
  design, not a bug; the `Fund Cov %` column says so.
- **Every ratio is two dollar figures from the same filing** — ROE, gross
  profit / assets, equity / assets. None divides by a price: SEC share counts
  are not split-adjusted, so `market_cap()` is `nil` under the SEC source, and
  the closes are dividend-adjusted, which biases price multiples anyway
  (T112).
- **Give them a cross-section.** Ranking three companies is not a screen; the
  ranking ones leave the book alone below `min_names`. Twenty names or more is
  where the idea starts to mean something.
- **A hand-typed list is survivors.** Every company you can type today lived
  through your window, which flatters any long-only screen. Delisted filers
  are in `sec_financials` but cannot be traded until T51.
- **Banks and insurers mostly drop out**: no gross profit, no current
  assets/liabilities, balance sheets that are mostly debt by design.

## Writing your own

**Start from a shape.** On a blank (or unmodified) Lua tab the app offers
three starting shapes — a per-ticker signal, a scheduled rebalance, and a
cross-sectional ranking — each running as written with one `TODO` where the
idea goes. **▶ Run this script** then runs it against the holdings already on
the Backtest tab, so trying an idea does not require writing a config around it
first.

**Check it before you run it.** The Lua tab's ✓ Check script compiles the
script, requires `step(day)`, and runs it for three days against synthetic
prices — no database, no backtest. A typo inside the body is found there
instead of on day 0 of a real run. It reports a syntax error, a missing or
non-function `step`, anything the dry run raises, and a `[portfolio.Params]`
key the script never reads — and, because it runs the body with strict
globals, a name nothing defines: *unknown name "smaa" — did you mean "sma"?*
Reading an undefined global is silently `nil` in ordinary Lua, which is how a
strategy ends up quietly never trading. Only the check is strict; a real run
behaves exactly as it always has.

**The editor knows all of this.** In the app's Lua tab, typing offers every
global with its signature and a one-line explanation, and hovering a name shows
the same thing — the list is served by the engine itself (`backtest.LuaAPI`),
and a test pins it to the `SetGlobal` calls that register them, so what the
editor offers is exactly what a script can call. `step` is the first
completion on an empty file. The table below is the same surface, for reading
away from the app.


A strategy is a Lua file defining one global function, `step(day)`, called
once per trading day with `day` counting from 0. Everything below is a
global provided by the engine; there is nothing to import.

### Context

| Global | Description |
|---|---|
| `tickers` | 1-indexed table of the portfolio's ticker strings. |
| `params` | Table of the `[portfolio.Params]` values from the config. |

### Prices

All take `(ticker, day)` and return 0 when the day is out of range.

| Function | Returns |
|---|---|
| `price(t, day)` / `close_at(t, day)` | Closing price. |
| `open_at`, `high_at`, `low_at` | That day's open / high / low. |
| `volume_at(t, day)` | Volume. |
| `date_at(t, day)` | `"YYYY-MM-DD"` string (`""` when out of range). |

### Fundamentals

Reported figures, dated by the day they became **public** — never by the period
they describe. Declare what you need in the config first; nothing is fetched
otherwise:

```toml
[[Portfolio]]
Fundamentals = ["Net Income", "Stockholders Equity", "Total Revenue"]
```

| Function | Returns |
|---|---|
| `fundamental(t, metric, day)` | The newest figure published on or before that day, or **`nil`**. |
| `fundamental(t, metric, day, freq)` | Only figures of that frequency: `"annual"` (a full fiscal year) or `"quarterly"` (one quarter). Anything else is an error. **Use this for every flow** — see below. |
| `fundamental(t, metric, day, freq, n)` | The period `n` before the newest of that frequency, as it was known on `day`. `n = 1` with `"annual"` is last fiscal year — the year-over-year comparison, with no extra price history needed. |
| `market_cap(t, day)` | Shares × close, or **`nil`** when the share count is missing or the close is ≤ 0. Always `nil` with `FundamentalSource = "sec"`. |

**Name the frequency for net income, revenue, cash flow — any flow.** The bare
form returns whichever figure was published most recently, so it can return a
quarter one day and a full year the next, and a ratio built on it jumps about
4x at each company's fiscal year end. Balance-sheet figures (equity, assets,
debt) are levels, so the bare form is fine for them.

**Where the figures come from.** By default, the `financials` table: 2020-07
onward. Add `FundamentalSource = "sec"` to the portfolio to read SEC filings
instead — 2010 onward, each figure exactly as first filed, from the day after
the filing. Under `"sec"` the bare form never mixes frequencies (a flow means
its newest full year, a level its newest balance sheet), and a metric SEC does
not carry is refused when the config loads. Its 30 metrics are listed in
`src/data.SECMetrics`; per-share figures and share counts there are as
reported, so they jump at every split.

**Declare what your script reads in the script** and it works wherever it is
picked, with no config changes:

```lua
fundamentals = {
    source  = "sec",                                  -- leave out for `financials`
    metrics = { "Net Income", "Stockholders Equity" },
}
```

It must be a top-level assignment of a plain table of quoted names: the engine
reads it before the run, without running the script, because the figures are
loaded before any Lua executes. A computed name, an unknown field or a second
assignment is an error naming the line, and ✓ Check script reports it. It
applies when the config declares no `Fundamentals`; a config that names
metrics means exactly those. Asking `fundamental()` for a metric that was not
loaded logs a warning once, rather than returning `nil` in silence.

**These return `nil`, not 0 — the opposite of `price()`.** That is deliberate.
A price of zero is impossible, so `price()` can safely use 0 to mean "absent".
A company can genuinely report zero net income, zero cash, or negative equity,
so a 0 here would be indistinguishable from "not published yet" — the exact
ambiguity that makes a strategy trade on a figure it never had. Test for `nil`:

```lua
local ni = fundamental(t, "Net Income", day, "annual")
local eq = fundamental(t, "Stockholders Equity", day)
if ni ~= nil and eq ~= nil and eq > 0 then
  local roe = ni / eq
  ...
end
```

You cannot see the future here even if you try: the publication date is applied
in Go, upstream of Lua, so a lookup only ever returns what was public that
morning. Ratios, rankings and cross-sectional sorts are arithmetic you do
yourself — there is no wider API on purpose.

**Coverage is measured, not enforced.** The default source starts in 2020, so
a strategy run on it from 2010 sees `nil` for the first decade, trades only in the back
half, and can look like it found an edge. The engine will not refuse that
window; it reports a `Fund Cov %` metric instead — the share of
(day × ticker × declared metric) cells that had a figure. Read it before
believing the Sharpe.

### Indicators

Computed in Go, so they are cheap to call in a loop.

| Function | Returns |
|---|---|
| `sma(t, day, period)` | Mean close over `[day-period, day)`; 0 without enough history. |
| `rsi(t, day, period)` | Wilder-style RSI; 50 without enough history, 100 when there were no losses. |
| `roc(t, day, n)` | Trailing return over the `n` days ending at `day`. The building block of momentum and trend. |
| `stdev(t, day, n)` | Realised volatility: standard deviation of the `n` daily returns ending at `day`. **Not annualized** — multiply by `math.sqrt(252)`. |
| `zscore(t, day, n)` | How far today's close sits from its `n`-day mean, in standard deviations of the close. Strongly negative is stretched to the downside. |
| `atr(t, day, n)` | Average true range in price units, for volatility-scaled sizing and stops that widen with the instrument. |
| `high_n(t, day, n)` / `low_n(t, day, n)` | The extreme High/Low over the `n` days ending at `day`, inclusive. Breakout channels without the loop. |
| `corr(a, b, day, n)` | Pearson correlation of two tickers' daily returns, for pairs and for checking a "diversified" book is not one bet in five costumes. |

**The newer functions return a second value saying whether the answer is real:**

```lua
local r, ok = roc(t, day, 21)
if not ok then return end          -- not enough history; do not treat it as 0
```

`sma` and `rsi` predate that convention and return a neutral number instead (0
and 50), which silently turns "unknown" into a value. Ignoring the second return
is allowed and behaves the old way, but a strategy that checks it will not open a
position on a warm-up artifact.

Everything is computed in Go, so calling these in a loop over every ticker is
cheap, and nothing reads past `day` — a signal cannot see the future even by
accident.

### Account and orders

| Function | Effect |
|---|---|
| `cash()` | Uninvested buying power. |
| `cashflow()` | The signed amount a `[portfolio.Cashflow]` schedule moved at the top of today: positive on a deposit day, negative on a withdrawal day, `0` otherwise. Read this, not `cash()`, to invest contributions — cash can be high for reasons that are not a deposit. |
| `position(t)` | `{amount, avg_price, current_price}`, or `nil` when flat. |
| `buy(t, shares, price, day)` | Buy an exact share count. Fractional shares are allowed. |
| `buy_max(t, price, buyType, day, [budget])` | Size the order automatically; `buyType` is `"equalWeights"` (`budget / #tickers`) or `"greedy"` (all of `budget`). `budget` defaults to `cash()`; pass `cashflow()` to invest just a deposit, or a balance snapshotted before the first leg so equal-weight legs don't shrink in turn. The order never exceeds what `cash()` buys. |
| `sell(t, shares, price, day)` | Sell an exact share count. With `AllowShort`, a sell past the holding closes the long and **shorts the remainder**; without it, such an order is refused. |
| `sell_all(t, price, day)` | Close the whole position, whichever way it points — a short is closed by buying the shares back. |
| `short(t, shares, price, day)` | Be short exactly `shares` after this call. A **target**, not a delta — see below. `short(t, 0, …)` goes flat. Needs `AllowShort`. |
| `cover(t, price, day)` | Buy the whole short back. A no-op on a long. |
| `equity(day)` | Cash plus every position marked at that day's close. |
| `weight_of(t, day)` | The fraction of equity a position currently represents, for checking drift. |
| `target_weights(day, {T = fraction, …})` | Move the whole book to those fractions of equity: sells run first so the proceeds fund the buys, and buys are clamped to available cash. A ticker absent from the table is a target of **zero**, so `target_weights(day, {})` goes to cash. |
| `rank(day, fn)` | Score every ticker with `fn(ticker, day)` and return `{ {ticker=…, value=…}, … }` **sorted strongest first**. Tickers whose `fn` returns `nil` are left out rather than ranked last. |
| `log(...)` | Record a note in the run's diagnostics (printed by a CLI run; not shown in the desktop app). Any number of values, rendered as `tostring` would and joined with spaces, like `print`. |
| `plot(name, value)` | Record a number for today. Each name becomes a small chart on the results panel, drawn against the same days as the equity curve — the way to *see* what the strategy is reacting to rather than reading it out of a thousand log lines. |
| `num_days()` | Trading days in this run. `day` is 0-based, so **the last day is `num_days() - 1`**. |

### Seeing what the strategy saw

```lua
function step(day)
  local t = tickers[1]
  local fast, slow = sma(t, day, 20), sma(t, day, 50)
  plot("fast", fast)
  plot("slow", slow)
  plot("spread", fast - slow)
  ...
end
```

Three charts appear under **What the strategy plotted**, on the same days as
the equity curve, each on its own vertical scale — which is why they are
separate charts and not extra lines on the equity chart: an RSI between 0 and
100 drawn on a $184,000 axis is a flat line along the bottom.

Rules worth knowing:

- **One value per name per day**, and the last call wins. A `plot()` inside a
  loop over tickers therefore records the loop's final value, not one point
  per ticker — give each ticker its own name if that is what you want.
- **At most 8 names.** Past that the extra names are ignored and a warning
  diagnostic says so once.
- **A series starts where you start plotting it.** An indicator with 200 days
  of warm-up simply has no line before day 200, rather than a line at zero.
- `nil`, NaN and infinities are dropped rather than drawn.
- It changes no number the engine reports. It is an instrument, not a metric.

### Shorting

With `AllowShort = true` on the portfolio, `sell` past the holding *is* a short,
and `short()` says it directly:

```lua
short(t, 100, price(t, day), day)   -- be short 100, from wherever you are
cover(t, price(t, day), day)        -- buy it all back
```

**`short` is a target, not a delta**, which is the opposite of `buy` and `sell`
and is deliberate. "Short 30" said as a delta is ambiguous the moment a position
already exists — 30 more, or 30 in total? — and the ambiguity is invisible until
a backtest is wrong. As a target it is correct without reading the current
position first, it reaches the target from a long as readily as from flat, and
`short(t, 0, …)` is "go flat" with no extra function.

**Market-neutral is a target weight away**, and needs no new API at all: a
negative weight is a short leg, because the rebalancer's sell pass sells past
what is held and its buy pass covers.

```lua
target_weights(day, { LONG_ONE = 0.5, SHORT_ONE = -0.5 })
```

A short order on a portfolio without `AllowShort` is refused **and says so** —
including a negative `target_weights` leg, which would otherwise leave the book
at half the size the strategy asked for with nothing to show that it had.

`position(t).amount` is **negative** for a short and `avg_price` is the price it
was shorted at. Buying covers it, and the gain is `avg_price - fill` — the
mirror of a long. Without `AllowShort` the engine is long-only and the same call
is refused, so a strategy that shorts must say so in its config.

Borrow cost is charged if the config asks for it — `borrow_bps_annual` under
`[portfolio.Costs]`, accrued daily on the short's marked value and visible as
the `BorrowPaid` metric. It defaults to zero, so a short-side result is
optimistic by the borrow it never paid unless the config sets a rate.

Margin is enforced: `[portfolio.Margin]` sets `initial_pct` (default 50%, how
much equity is needed to open a short) and `maintenance_pct` (default 30%, how
much to keep it). A maintenance breach **covers your short for you** — the
`MarginCalls` metric counts the days it happened, and any strategy whose result
has a non-zero `MarginCalls` contains trades it never asked for.

`num_days()` is what makes "do this once, at the end" expressible — `step` is
handed `day` with nothing to compare it against:

```lua
if day == num_days() - 1 then
    sell_all(t, price(t, day), day)   -- close the book on the final bar
end
```

Closing out is worth knowing about for a reason that is easy to miss: realised
P&L, win rate and profit factor are computed from **sells**. A strategy that
buys and holds realises nothing, so those three read as zero however well it
did. Selling on the last day makes them describe something — at the cost of one
final round of fees, which is real and will show in `TotalFees`.

**A misspelled `Params` key is reported, not silently ignored.** Scripts read
settings as `params.lookback or 126`, so a key spelled wrong falls through to
the default and the run proceeds with settings you did not choose. A warning
diagnostic names any key you passed that the script never reads (✓ Check and
`-check` report it before a run). In a **sweep** this
matters more than it looks: sweep values are merged into `Params`, so a
mistyped swept key expands to N runs that differ only in a value nothing reads
— every trial produces identical numbers, and the deflated Sharpe still
discounts the result for a search that never happened. The warning says so.

The check reads the script's source, so a key composed at runtime
(`params[name]`) can be reported even though it is used. It is a warning for
that reason, never a rejection.

**Use `log()`, not `print()`.** Lua's `print` writes to stdout, which is fine
from the CLI and **invisible in the desktop app** — debugging a strategy there
with `print` shows you nothing. `log()` records a diagnostic tagged with the
script name, which a CLI run prints to its log (stderr, or `backtester.log`
under `-debug`); the desktop app has no console and does not show it. It is not
deduplicated: a `log()` inside the day loop fires every day, which is usually
what you wanted, and a run keeps the first 200 diagnostics.

Every order function requires a ticker from the `tickers` global. A ticker
the portfolio does not list — a typo, or a `nil` that reads as the empty
string — is **refused**, and an error diagnostic says which function asked for
what alongside the list it should have come from. It used to fill: the
position went in under the unlisted key, was never valued, and the cash
disappeared from the equity curve for the rest of the run.

Always pass `day` as the last argument — it timestamps the trade in the
transaction log.

### Patterns worth copying

**Warm-up guard.** Indicators need history; return early until you have it:

```lua
function step(day)
    if day < period then return end
    ...
end
```

**Act on a cadence** rather than every day:

```lua
if day % rebalance_days ~= 0 then return end
```

**A cross-sectional strategy in six lines.** `rank` plus `target_weights` is
the whole shape of "rank the universe by X, hold the best N":

```lua
function step(day)
    if day % 21 ~= 0 then return end
    local ranked = rank(day, function(t) return (roc(t, day, 126)) end)

    local targets = {}
    for i = 1, math.min(3, #ranked) do
        targets[ranked[i].ticker] = 1.0 / 3
    end
    target_weights(day, targets)
end
```

Note the parentheses around `roc(...)`: they discard its second return value, so
the ranking function returns exactly one number. Without them Lua passes both,
and `rank` would score on the first while ignoring the rest — harmless here, but
worth knowing when a function returns a pair.

Swap the signal for `zscore` (negated, for mean reversion), `stdev` (negated, to
tilt toward calm names), or your own expression, and you have a different
strategy without touching the plumbing. `-scan-signals` on the CLI will tell you
which signals are worth ranking on before you write any of it.

**Total account value** is `equity(day)`, and **rebalancing to target weights**
is `target_weights(day, targets)`. `momentum_rotation.lua` and `rebalance.lua`
used to carry their own copy of both — around forty duplicated lines each — and
now call the primitives; their output is unchanged, which is what proved the
extraction correct. `risk_parity.lua` still carries its own copy, deliberately
left alone for now so there is an example of the long-hand form to copy from.

### Gotchas

- **Shorting is opt-in; there is no leverage on the long side.** A buy larger
  than `cash()` is clamped, so size against `cash()` or `equity()` rather than
  assuming credit. Selling past the holding is refused unless the portfolio
  sets `AllowShort = true`, which also enables `short()` (a *target*, not a
  delta), `cover()`, and negative legs in `target_weights`. See the main
  README's shorting section for borrow cost and margin.
- **Transaction costs are off by default.** Set `[portfolio.Costs]` in the
  config (`commission_per_trade`, `commission_bps`, `slippage_bps`) to charge
  them; omitting the block keeps trading frictionless, so high-turnover
  strategies will look better than in reality. Costs apply inside `buy`/`sell`
  and are invisible to strategy code — `cash()` already reflects them, and a
  `buy_max` sized against the whole balance is clamped down to cover its fee
  rather than rejected. See the main README's `[portfolio.Costs]` section.
- **`fundamental()` and `market_cap()` return `nil`, while `price()` returns 0.** The two conventions genuinely differ, because 0 is an impossible price but a possible fundamental. Guard prices with `> 0` and fundamentals with `~= nil`.
- **`price()` returns 0, not `nil`, out of range** — guard with `> 0`
  rather than `~= nil`.
- **Only tickers in the `tickers` global can be traded.** Anything else is
  refused and reported; see "Account and orders" above.
- **Errors are logged, not raised.** If a strategy misbehaves, the app's status
  bar shows the first error and the editor marks its line; a CLI run (or
  `backtester.log` under `-debug`) has them all. A runtime
  error means `step` stops for that day but the backtest continues.
- **`then` is a Lua keyword** — do not name a variable `then`.

### Testing a new strategy

`src/backtest/strategies_library_test.go` runs every file in this directory
against synthetic data and fails if it logs an error, spends cash it does
not have, or silently never trades. Add your file to `shippedStrategies()`
there — the test also fails if a strategy ships without an entry.

```sh
go test ./src/backtest -run Shipped -v
```
