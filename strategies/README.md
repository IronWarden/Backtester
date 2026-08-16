# Strategy library

Ready-to-run Lua strategies, plus the API for writing your own. Run them
all side by side with `strategy_library.toml` in the project root: open it
in Advanced mode and hit **Run backtest**.

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

## The catalog

Each file documents its own parameters at the top. Copy one and edit it —
that is the intended way to build something custom.

| Strategy | Idea | Key params |
|---|---|---|
| `buy_and_hold.lua` | Buy on day 0, hold to the end. | `buyType` |
| `buy_and_hold_weighted.lua` | Buy a fixed weighted allocation on day 0, never rebalance. | `weights` |
| `dca.lua` | Invest a fixed dollar amount on a cadence, never sell. | `amount`, `every_days` |
| `rebalance.lua` | Hold target weights, rebalance on a cadence — trims winners, tops up losers. | `rebalance_days`, `weights` |
| `risk_parity.lua` | Weight by 1/volatility so each holding contributes similar risk. | `lookback`, `rebalance_days` |
| `momentum_rotation.lua` | Hold the top N by trailing return, rotate on a cadence, cash out when nothing has positive momentum. | `lookback`, `top_n`, `rebalance_days`, `skip_days` |
| `trend_following.lua` | Hold each ticker only while it is above its own moving average. | `period`, `buffer_pct` |
| `sma_cross.lua` | Golden/death cross: fast average over slow. | `short`, `long` |
| `donchian_breakout.lua` | Buy N-day highs, exit on M-day lows (turtle style). | `entry_period`, `exit_period` |
| `rsi.lua` | Buy oversold, sell overbought. | `period`, `buy_thresh`, `sell_thresh` |
| `bollinger_reversion.lua` | Buy below the lower band, exit at the average. | `period`, `k` |

**Pick a universe that matches the family.** Per-ticker signal strategies
(trend, breakout, RSI, Bollinger) judge each ticker independently, so give
them a basket of individual names. Allocation strategies (rotation, risk
parity, rebalance) divide capital across the universe, so they are most
interesting across asset classes — e.g. `["$SP500", "$CASH"]`.

## Writing your own

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

### Indicators

Computed in Go, so they are cheap to call in a loop.

| Function | Returns |
|---|---|
| `sma(t, day, period)` | Mean close over `[day-period, day)`; 0 without enough history. |
| `rsi(t, day, period)` | Wilder-style RSI; 50 without enough history, 100 when there were no losses. |

Anything else — standard deviation, EMA, momentum, channels — is a few
lines of Lua over `price()`. See `bollinger_reversion.lua` for a standard
deviation and `risk_parity.lua` for return volatility.

### Account and orders

| Function | Effect |
|---|---|
| `cash()` | Uninvested buying power. |
| `position(t)` | `{amount, avg_price, current_price}`, or `nil` when flat. |
| `buy(t, shares, price, day)` | Buy an exact share count. Fractional shares are allowed. |
| `buy_max(t, price, buyType, day)` | Size the order automatically; `buyType` is `"equalWeights"` or `"greedy"`. |
| `sell(t, shares, price, day)` | Sell an exact share count. |
| `sell_all(t, price, day)` | Close the whole position. |

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

**Total account value** (cash plus marked-to-market positions):

```lua
local function equity(day)
    local total = cash()
    for i = 1, #tickers do
        local pos = position(tickers[i])
        if pos then total = total + pos.amount * price(tickers[i], day) end
    end
    return total
end
```

**Rebalancing to target weights** — sell first so the proceeds fund the
buys. `momentum_rotation.lua`, `risk_parity.lua`, and `rebalance.lua` each
carry a copy of this helper; they are deliberately self-contained so you
can copy a single file and edit it.

### Gotchas

- **No shorting and no leverage.** A buy larger than `cash()` is clamped;
  size against `cash()` or `equity()` rather than assuming credit.
- **Transaction costs are off by default.** Set `[portfolio.Costs]` in the
  config (`commission_per_trade`, `commission_bps`, `slippage_bps`) to charge
  them; omitting the block keeps trading frictionless, so high-turnover
  strategies will look better than in reality. Costs apply inside `buy`/`sell`
  and are invisible to strategy code — `cash()` already reflects them, and a
  `buy_max` sized against the whole balance is clamped down to cover its fee
  rather than rejected. See the main README's `[portfolio.Costs]` section.
- **`price()` returns 0, not `nil`, out of range** — guard with `> 0`
  rather than `~= nil`.
- **Errors are logged, not raised.** If a strategy misbehaves, check the
  app's console output; a runtime error means `step` stops for that day but
  the backtest continues.
- **`then` is a Lua keyword** — do not name a variable `then`.

### Testing a new strategy

`src/backtest/strategies_library_test.go` runs every file in this directory
against synthetic data and fails if it logs an error, spends cash it does
not have, or silently never trades. Add your file to `shippedStrategies()`
there — the test also fails if a strategy ships without an entry.

```sh
go test ./src/backtest -run Shipped -v
```
