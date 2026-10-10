# Architecture

How the engine fits together, the project layout, and adding a strategy.

[← README](../README.md) · [Installation and setup](installation.md) · [Data](data.md) · [Configuration](configuration.md) · [Command line](cli.md) · [The desktop app](app.md) · **Architecture**

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

## Project layout

```
.
├── config.toml              # portfolio definitions consumed at runtime
├── stock_data.db            # DuckDB file (OHLCV + risk-free rates)
├── go.mod / go.sum
├── strategy_library.toml    # the strategy cards the app and assistant offer
├── requirements.txt         # Python deps for the scripts (DuckDB 1.5.x)
├── docs/                    # this reference
├── scripts/                 # Python data loaders (build_db, add_*, sync_prices,
│                            #   fetch_history, …), their tests, and package.sh
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

## Adding a strategy

Lua is the intended route — no rebuild, and the engine handles the day loop.

1. Copy a file from `strategies/` and edit it. A strategy is one global `step(day)` function, called once per trading day with `day` counting from 0; the engine provides `price()`, `sma()`, `rsi()`, `cash()`, `cashflow()`, `position()`, `buy()`, `buy_max()`, `sell()`, `sell_all()`, and the `tickers` / `params` globals. Orders may only name a ticker from the `tickers` global — a typo is refused and reported as a run error rather than silently burning the cash. Full API, patterns, and gotchas: [`strategies/README.md`](../strategies/README.md).
2. Point a portfolio at it with `Strategy = "lua:strategies/your_file.lua"` and pass any settings in a `[portfolio.Params]` block.
3. Add it to `shippedStrategies()` in `src/backtest/strategies_library_test.go`, which runs every shipped strategy against synthetic data and fails if it errors, overspends, or never trades.

The Go path is the lower-level alternative — worth it only for something that needs engine internals or has to be fast enough to matter in a large sweep:

1. Implement the `Strategy` interface (`Name()` and `Step(p, hist, day)`) in `src/backtest/strategy.go`, as `BuyAndHold` and `SMACross` do. `Step` calls `Buy` / `Sell`; the runner's day loop takes care of `AdjustPortfolioParameters` and the metrics.
2. Add a case for its spec string to `NewStrategy`. The runner is not hard-coded to any strategy — it builds whatever `NewStrategy` returns for the portfolio's `Strategy` field, so nothing in `runner.go` needs to change.
3. Set `Strategy = "<your spec>"` on a portfolio in `config.toml`.

Per-strategy state lives on the implementing struct, so a `Strategy` instance is single-use; the runner clones the portfolio (rebuilding the strategy from its spec) for each pass.
