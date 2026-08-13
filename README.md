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

If the benchmark's data does not cover every trading day of the window, the
six metrics are left at zero and a line is logged. The backtest itself still
runs — a benchmark is a measurement, and a missing one is not a reason to
lose the result.

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
