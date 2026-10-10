# Configuration

Every `config.toml` key, cost, cash-flow, rule, and validation option, and the `[Output]` block.

[← README](../README.md) · [Installation and setup](installation.md) · [Data](data.md) · **Configuration** · [Command line](cli.md) · [The desktop app](app.md) · [Architecture](architecture.md)

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
See [strategies/README.md](../strategies/README.md#fundamentals-strategies).

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
