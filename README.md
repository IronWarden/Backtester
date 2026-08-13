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
never the price). `$CASH` compounds the FRED 3-month T-bill rate (daily, 1954+)
spliced onto Ken French's 1-month T-bill return (monthly, 1926–1953, since a
daily 3-month series doesn't exist that far back). Rebuild or
refresh anytime (idempotent per symbol; close the UI first — DuckDB is
single-writer):

```bash
python3 add_collections.py            # build/refresh all
python3 add_collections.py --dry-run  # preview coverage, no write
python3 add_collections.py --only '$SP500' '$CASH'
```

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
