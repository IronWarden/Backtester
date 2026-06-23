# Backtester

A concurrent Go backtesting engine that simulates one or more portfolios over historical equity data stored in DuckDB. Portfolios, date ranges, tickers, and strategies are defined in a TOML config; each `(portfolio, strategy)` pair runs in parallel on a worker pool sized to the host's CPU count.

## How it works

The runtime is structured around four components:

- **`src/main.go`** — entry point. Opens the DuckDB file, loads `config.toml`, converts each TOML portfolio entry into a `Portfolio` struct, and hands them to the runner.
- **`src/data/database.go`** — DuckDB access layer. Reads OHLCV bars from `stock_data_optimized` and daily risk-free rates from `3MTreasuryYields`.
- **`src/backtest/runner.go`** — orchestrates the simulation. Pre-fetches historical data for every unique ticker once, then fans out `(portfolio, strategy)` jobs across `runtime.NumCPU()` workers. Results with `SharpeRatio > 0.5` are written to `worthy_tickers.txt`.
- **`src/backtest/strategy.go`** — strategy implementations. Currently exercises `BuyAndHold`; `SMACross` and `RSI` helpers are defined for extension.
- **`src/backtest/portfolio.go`** — portfolio state, `Buy` / `Sell` / `Deposit` / `Withdraw`, and end-of-day mark-to-market via `AdjustPortfolioParameters`.
- **`src/backtest/metrics.go`** — Sharpe, Sortino, max drawdown, annualized return, and standard deviation, all annualized over a 252-trading-day year.

## Prerequisites

- Go 1.24+ (module pinned to `go 1.24.4`).
- A DuckDB file named `stock_data.db` in the repository root containing:
  - `stock_data_optimized(Date, Ticker, Open, High, Low, Close, Volume)`
  - `"3MTreasuryYields"(Date, daily_risk_free_rate_decimal)`
- A `config.toml` in the repository root (see below).

Dependencies (`github.com/marcboeker/go-duckdb`, `gonum.org/v1/gonum`, `github.com/BurntSushi/toml`) are pulled via `go mod`.

## Configuration

Define one `[[Portfolio]]` block per portfolio in `config.toml`. The runner will execute every strategy listed for every portfolio.

```toml
[[Portfolio]]
Name        = "Tech Giants"
BuyingPower = 20000.0
StartDate   = "2015-03-31"   # YYYY-MM-DD
EndDate     = "2025-03-31"
Tickers     = ["AAPL", "MSFT", "GOOGL", "AMZN"]
Strategies  = ["greedy"]      # "greedy" or "equalWeights"

[[Portfolio]]
Name        = "Multi Strategy"
BuyingPower = 25000.0
StartDate   = "2023-01-01"
EndDate     = "2023-03-31"
Tickers     = ["MSFT", "GOOGL"]
Strategies  = ["greedy", "equalWeights"]
```

Field reference:

| Field | Type | Notes |
| --- | --- | --- |
| `Name` | string | Identifier shown in metric output and `worthy_tickers.txt`. |
| `BuyingPower` | float | Starting cash. |
| `StartDate` / `EndDate` | string | `YYYY-MM-DD`. |
| `Tickers` | []string | Must exist in `stock_data_optimized` for the date range. |
| `Strategies` | []string | Allocation modes consumed by `BuyAndHold`. Each runs as a separate job. |

Built-in allocation modes (selected via the `Strategies` list):

- `greedy` — pour all available buying power into each ticker in order.
- `equalWeights` — split buying power evenly across the portfolio's tickers.

## Running

The binary expects to be launched from `src/` because it resolves `../stock_data.db` and `../config.toml` relative to the working directory.

```bash
cd src
go run main.go              # quiet run; logs are discarded
go run main.go -debug       # writes backtester.log + transactions.log, and serves pprof on :6060
```

To build a binary:

```bash
cd src
go build -o backtester
./backtester -debug
```

## Output

- **stdout / `backtester.log`** — query timings, debug info, and per-portfolio metrics when `PrintMetrics` is invoked.
- **`transactions.log`** (debug only) — every `BUY` / `SELL` and the day's percentage change.
- **`worthy_tickers.txt`** — one line per `(portfolio, strategy)` whose annualized Sharpe ratio exceeds 0.5, with Sharpe / Sortino / Max Drawdown / Annual Return.
- **pprof** (debug only) — `http://localhost:6060/debug/pprof/` for CPU and heap profiling.

Reported metrics per run:

- `SharpeRatio` — annualized, using the per-day risk-free rate from `3MTreasuryYields`.
- `SortinoRatio` — annualized, downside-deviation denominator.
- `MaxDrawdown` — peak-to-trough drawdown of the daily close-value series, as a percent.
- `AnnualReturn` — CAGR derived from the compounded daily return series.
- `StandardDev` — annualized stdev of daily returns.

## Adding a strategy

1. Add a new method on `*Portfolio` in `src/backtest/strategy.go` that walks `historicalData` day-by-day, calls `Buy` / `Sell`, and finishes each day with `AdjustPortfolioParameters` so daily returns and close values are recorded.
2. Either extend `BuyAndHold` to dispatch on `strategyType`, or wire the new strategy into the worker loop in `src/backtest/runner.go` (currently hard-coded to `BuyAndHold`).
3. Add the strategy name to a portfolio's `Strategies` list in `config.toml`.

## Project layout

```
.
├── config.toml              # portfolio definitions consumed at runtime
├── stock_data.db            # DuckDB file (OHLCV + risk-free rates)
├── go.mod / go.sum
└── src/
    ├── main.go              # entry point
    ├── backtest/
    │   ├── config.go        # TOML loading + Portfolio construction
    │   ├── portfolio.go     # Portfolio / Position state and trade execution
    │   ├── runner.go        # worker pool, data prefetch, result collection
    │   ├── strategy.go      # BuyAndHold, SMACross, RSI helpers
    │   └── metrics.go       # Sharpe, Sortino, drawdown, CAGR
    └── data/
        └── database.go      # DuckDB queries
```
