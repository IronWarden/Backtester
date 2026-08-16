package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"my-backtester/src/data"

	wruntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

// The assistant is provider-agnostic: the frontend sends the visible chat
// history plus a model id, and Chat dispatches to the Anthropic API
// (claude.go) or a local Ollama server (ollama.go). Both providers share
// the system prompt specialized to this app (TOML schema, Lua strategy
// API, live DB schema, the user's config files) and the same tools: the
// read-only query_db tool and the lookup_quote yfinance tool (quote.go).
// Text streams to the frontend via "chat:delta" events and tool calls are
// announced via "chat:tool"; the final text is also returned so the
// frontend can replace its streamed buffer with the authoritative
// content.

const (
	maxToolIterations   = 8
	maxToolRows         = 100
	maxToolCellChars    = 300
	maxFileContextChars = 16_000
	maxConfigFileChars  = 4_000
	// Local models get a leaner prompt: editor buffers are clipped harder
	// and the saved-config dump is skipped, to fit small context windows.
	maxLeanContextChars = 4_000
)

// ChatMessage is one visible turn of the conversation as the frontend
// stores it. Tool calls/results live only inside a single Chat invocation
// and are never round-tripped through the frontend.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ModelOption is one entry of the frontend's model dropdown.
type ModelOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

// ollamaPrefix marks model ids that route to the local Ollama server; the
// rest of the id is the Ollama model name (which may itself contain ':').
const ollamaPrefix = "ollama:"

var claudeModels = []ModelOption{
	{ID: "claude-sonnet-5", Label: "Claude Sonnet 5"},
	{ID: "claude-opus-4-8", Label: "Claude Opus 4.8"},
	{ID: "claude-haiku-4-5-20251001", Label: "Claude Haiku 4.5"},
}

// chatMu serializes chats: the streamed events carry no request id, so two
// concurrent chats would interleave their deltas. The frontend disables
// Send while busy; this is the backend-side guarantee.
var chatMu sync.Mutex

// repoRoot resolves the project root. The app runs with cwd ui/ (dev and
// packaged alike — DEFAULT_DB_PATH is ../stock_data.db), but fall back to
// "." so a binary launched from the repo root still finds its files.
func repoRoot() string {
	for _, dir := range []string{"..", "."} {
		if _, err := os.Stat(filepath.Join(dir, "strategies")); err == nil {
			return dir
		}
	}
	return ".."
}

// ---------------------------------------------------------------------------
// Context assembly

// dbOverviewCache memoizes the per-database schema summary; keyed by path
// since the user can switch DBs. Invalidated only by restart, which is fine
// because the schema and ticker universe change via external scripts.
var dbOverviewCache sync.Map

// dbOverview builds a live description of the database at dbPath: every
// table's columns, the size/date-span of the price table, and which
// $-benchmark series are present.
func dbOverview(dbPath string) string {
	if dbPath == "" {
		return "No database chosen yet — ask the user to pick one before querying."
	}
	if cached, ok := dbOverviewCache.Load(dbPath); ok {
		return cached.(string)
	}
	if _, err := data.InitDB(dbPath); err != nil {
		return fmt.Sprintf("Database at %q could not be opened: %v", dbPath, err)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "DuckDB file: %s\n\nTables and columns:\n", dbPath)
	cols, rows, _, err := data.RunQuery(
		`SELECT table_name, column_name, data_type
		 FROM information_schema.columns
		 ORDER BY table_name, ordinal_position`, 500)
	if err != nil || len(cols) < 3 {
		fmt.Fprintf(&b, "(schema query failed: %v)\n", err)
	} else {
		byTable := map[string][]string{}
		var order []string
		for _, r := range rows {
			if _, ok := byTable[r[0]]; !ok {
				order = append(order, r[0])
			}
			byTable[r[0]] = append(byTable[r[0]], r[1]+" "+r[2])
		}
		for _, t := range order {
			fmt.Fprintf(&b, "- %q(%s)\n", t, strings.Join(byTable[t], ", "))
		}
	}

	if _, rows, _, err := data.RunQuery(
		`SELECT COUNT(*), COUNT(DISTINCT Ticker), MIN(Date), MAX(Date)
		 FROM stock_data_optimized`, 1); err == nil && len(rows) == 1 {
		fmt.Fprintf(&b,
			"\nstock_data_optimized holds %s daily bars for %s tickers, %s to %s.\n",
			rows[0][0], rows[0][1], rows[0][2], rows[0][3])
	}
	if _, rows, _, err := data.RunQuery(
		`SELECT DISTINCT Ticker FROM stock_data_optimized
		 WHERE Ticker LIKE '$%' ORDER BY Ticker`, 50); err == nil && len(rows) > 0 {
		syms := make([]string, len(rows))
		for i, r := range rows {
			syms[i] = r[0]
		}
		fmt.Fprintf(&b, "Benchmark pseudo-tickers present: %s\n",
			strings.Join(syms, ", "))
	}

	overview := b.String()
	dbOverviewCache.Store(dbPath, overview)
	return overview
}

// clipText caps s at n bytes for prompt inclusion, marking the cut.
func clipText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "\n… (truncated)"
}

// userConfigsContext inlines every user-defined config the project holds:
// the saved TOML portfolio configs in the repo root and the Lua strategy
// library. The live editor buffers are passed separately since they may be
// unsaved.
func userConfigsContext() string {
	root := repoRoot()
	var b strings.Builder

	tomls, _ := filepath.Glob(filepath.Join(root, "*.toml"))
	sort.Strings(tomls)
	if len(tomls) > 0 {
		b.WriteString("Saved TOML configs in the project root:\n")
		for _, p := range tomls {
			raw, err := os.ReadFile(p)
			if err != nil {
				continue
			}
			fmt.Fprintf(&b, "\n--- %s ---\n%s\n",
				filepath.Base(p), clipText(string(raw), maxConfigFileChars))
		}
	}

	luas, _ := filepath.Glob(filepath.Join(root, "strategies", "*.lua"))
	sort.Strings(luas)
	if len(luas) > 0 {
		b.WriteString("\nLua strategy library (strategies/):\n")
		for _, p := range luas {
			raw, err := os.ReadFile(p)
			if err != nil {
				continue
			}
			fmt.Fprintf(&b, "\n--- strategies/%s ---\n%s\n",
				filepath.Base(p), clipText(string(raw), maxConfigFileChars))
		}
	}
	return b.String()
}

// appReference is the static portion of the system prompt: what this app
// is and how its config, strategies, and metrics work. Kept in one place
// so the assistant's knowledge tracks the engine.
const appReference = `You are the built-in AI assistant of "Backtester", a desktop app for
backtesting equity portfolio strategies over historical daily OHLCV data
stored in DuckDB. You are an expert in quantitative finance: portfolio
construction, trading strategies, risk/return metrics, market history, and
factor investing. Stay practical and concise; use finance terminology
precisely but explain it when the question suggests the user is learning.

## How the app works
The user edits a TOML config (one [[portfolio]] block per portfolio) and
optionally a Lua strategy script, then runs the backtest. Every portfolio
runs concurrently; results (metrics + equity curve) appear in a table and
chart. Simulation is daily: for each trading day the strategy's step runs,
then the portfolio is marked to market on that day's Close.

## TOML config format
[[portfolio]]
Name        = "My Portfolio"      # shown in results
BuyingPower = 100000.0            # starting cash
StartDate   = "2015-01-01"        # YYYY-MM-DD
EndDate     = "2025-01-01"
Tickers     = ["AAPL", "MSFT"]    # must all have data covering the window
Strategy    = "buyAndHold:equalWeights"   # optional; see specs below
Benchmark   = "$SP500"            # optional; scored against, NOT traded
[portfolio.Params]                # optional, passed to Lua as 'params'
  # arbitrary keys; e.g. for buy_and_hold_weighted.lua:
  # [portfolio.Params.weights]
  # AAPL = 60
  # MSFT = 40
[portfolio.Costs]                 # optional; every field defaults to 0
  commission_per_trade = 1.0      # flat fee per filled order
  commission_bps       = 5.0      # fee as basis points of order notional
  slippage_bps         = 2.0      # fills worsen by bps: buys higher, sells lower

Benchmark supplies a return series for the benchmark-relative metrics and is
deliberately kept out of Tickers, so it does not receive capital and does not
affect the trading calendar. Omit it and those metrics are simply absent.
An absent [portfolio.Costs] block is identical to all-zero: trading is
frictionless. When a user asks whether a strategy survives real-world costs,
propose a Costs block rather than telling them the engine cannot model it.

Strategy spec strings:
- "greedy" or "equalWeights"            -> buy-and-hold with that sizing
- "buyAndHold:<greedy|equalWeights>"    -> same, explicit form
- "smaCross:<short>:<long>:<buyType>"   -> SMA crossover (e.g. "smaCross:50:200:equalWeights")
- "lua:<path>"                          -> Lua script (path relative to app cwd, e.g. "lua:strategies/rsi.lua")
- Omitting Strategy uses the Lua script currently open in the app's Lua tab.
Sizing: "greedy" pours all remaining cash into each ticker in order;
"equalWeights" splits cash evenly across the portfolio's tickers.

An optional [Output] block writes results to a file:
path, format ("txt"|"csv"|"json"), fields (list of result fields), filter
(Go-style expression like "SharpeRatio > 0.5 && AnnualReturn > 5"),
sort_by, order ("asc"|"desc"), limit.
Result fields usable in fields/filter/sort_by: PortfolioName, Strategy,
SharpeRatio, SortinoRatio, MaxDrawdown, AnnualReturn, StandardDev,
AvgCorrelation, CointegratedPairs, Turnover, Alpha, Beta, TrackingError,
InformationRatio, UpCapture, DownCapture, InitialCapital, FinalValue, Profit.

## Lua strategy API
A strategy script must define a global function step(day). day is a
0-based index into the backtest window (trading days only, all tickers
aligned to their common dates). step runs once per day, day 0 first.
Globals available:
- tickers            -- 1-indexed table of the portfolio's symbols
- params             -- table from the TOML Params block (may be empty)
- cash()             -- current buying power
- position(t)        -- nil, or {amount, avg_price, current_price}
- price(t, day) / close_at(t, day) / open_at / high_at / low_at / volume_at(t, day)
- date_at(t, day)    -- "YYYY-MM-DD" for that bar
- sma(t, day, period)-- mean Close over [day-period, day); 0 if not enough history
- rsi(t, day, period)-- RSI over trailing period ending at day; 50 if not enough history
- buy(t, amount, price, [day])      -- caller-sized buy (fractional shares OK)
- buy_max(t, price, [buyType="equalWeights"], [day]) -- auto-sized buy
- sell(t, amount, price, [day])
- sell_all(t, price, [day])
Pass the current day index as the optional [day] so transactions get dated.
Buys silently no-op if cash is insufficient; guard with cash() if needed.
There is no shorting and no leverage. Look-ahead warning: only use data at
indices <= day, or the backtest is invalid.

## Shipped strategy library (strategies/)
Ready-made, parameterized strategies — their full source is inlined below
under "Lua strategy library". Prefer pointing a config at one of these
(Strategy = "lua:strategies/<file>", parameters in [portfolio.Params])
over writing a script from scratch; write new Lua only when the user wants
behavior none of them cover, and start from the closest file.
- Baselines: buy_and_hold, buy_and_hold_weighted, dca
- Allocation: rebalance, risk_parity, momentum_rotation
- Trend: trend_following, sma_cross, donchian_breakout
- Mean reversion: rsi, bollinger_reversion
strategy_library.toml in the project root runs all of them side by side.
Match the universe to the family: per-ticker signal strategies (trend,
breakout, rsi, bollinger) want a basket of individual stocks; allocation
strategies split capital across the universe, so they are most meaningful
across asset classes (e.g. ["$SP500", "$CASH"]).

## Metrics (all annualized over 252 trading days)
- SharpeRatio: mean daily excess return over the daily risk-free rate
  (from the 3MTreasuryYields table) / stdev, annualized.
- SortinoRatio: same numerator, downside deviation denominator.
- MaxDrawdown: worst peak-to-trough decline of daily portfolio value, in %.
- AnnualReturn: CAGR of the compounded daily return series, in %.
- StandardDev: annualized stdev of daily returns.
- AvgCorrelation / CointegratedPairs: mean pairwise return correlation and
  count of cointegrated ticker pairs in the portfolio.
- Turnover: annualized gross traded notional as a multiple of the portfolio's
  average value. 1.0 means it traded its own value once over a year. Always
  reported, with or without costs — it is what costs are charged against.

Benchmark-relative metrics, computed only when the portfolio sets Benchmark
and that ticker's data covers the whole window (otherwise all zero):
- Beta: slope of portfolio returns against the benchmark's; 1.0 moves with it.
- Alpha: annualized %, the excess return beta does not explain.
- TrackingError: annualized stdev of active (portfolio - benchmark) return.
- InformationRatio: active return per unit of tracking error.
- UpCapture / DownCapture: % of the benchmark's gain captured on its up days,
  and % of its loss taken on its down days. Under 100 down-capture with over
  100 up-capture is the asymmetry most strategies are actually chasing.

Setting Benchmark also draws the benchmark itself. The results panel plots it
as a muted dashed line on the equity chart, rebased to the portfolio's own
starting capital so both share one axis, and adds a row to the metrics table
with the benchmark's OWN return, Sharpe, max drawdown and standard deviation
(BenchmarkStats). So "did I beat the index" is answerable directly, rather
than by inferring it from alpha. Recommend a Benchmark whenever a user asks
how a portfolio compares to the market — "$SP500" for US large-cap, "$USTOT"
for the total US market.

Beyond the scalar metrics, each run also carries time-sliced views that the
results panel renders: the ten deepest drawdowns with their recovery dates, a
252-day rolling Sharpe series, and compounded calendar-year and month returns.
Point users at these when a single Sharpe number hides the shape of a run —
a strategy with a good Sharpe and one four-year underwater stretch is a
different proposition from one with the same Sharpe and shallow drawdowns.

## Database conventions
- Daily bars live in stock_data_optimized(Date, Ticker, Open, High, Low,
  Close, Volume). Prices are dividend-adjusted for the $-benchmarks;
  regular tickers are as loaded by the user's data pipeline.
- "3MTreasuryYields"(Date, daily_risk_free_rate_decimal) holds the daily
  risk-free rate (note: the table name needs double quotes in SQL).
- $-prefixed pseudo-tickers are long-history total-return benchmark series
  ($SP500, $USTOT, $SP400, $SP100, $RUS2K, $SP600, $EM, $EXUS, $WORLD,
  $CASH). They mix freely with real stocks in a portfolio. Their modern
  history comes from a real ETF, grossed back up by that fund's expense
  ratio, so they represent the index rather than a fund tracking it —
  treat them as index returns, not as something a user could have bought.
  Residual tracking error is not corrected.
- IMPORTANT: the ticker "CASH" (no $) is Pathward Financial, a bank stock —
  NOT a cash proxy. For cash / T-bills use $CASH.
- The database has NO fundamentals — no P/E, market cap, sector, earnings
  or dividend data. Questions like "companies with P/E under 20" can never
  be answered with SQL; they need the screen_stocks tool.

## Working style
- When proposing a portfolio config, emit one complete fenced toml code
  block; when proposing a strategy, emit one complete fenced lua code
  block. The app shows an "insert into editor" button on such blocks, so a
  complete, runnable block is far more useful than a fragment.
- Before recommending tickers or date ranges, verify coverage with the
  query_db tool — a backtest fails if any ticker lacks data anywhere in
  the window. MIN(Date)/MAX(Date) GROUP BY Ticker answers this cheaply.
- The lookup_quote tool fetches LIVE quotes and fundamentals (price, P/E,
  market cap, sector) from Yahoo Finance for real symbols. Use it for
  "what is X trading at / worth today" questions; use query_db for
  historical bars. It cannot look up the $-benchmark pseudo-tickers.
- The fetch_history tool downloads daily adjusted OHLCV from Yahoo into
  the database (idempotent upsert). When the user wants to backtest a
  ticker that is missing or stale — check with query_db — offer to fetch
  it, then confirm the stored range before building the config. It cannot
  fetch the $-benchmark series (those are built locally).
- To find or filter companies by fundamentals (P/E, market cap, dividend
  yield, sector), use the screen_stocks tool — the database cannot answer
  these. It screens the whole US market, so before proposing a backtest
  from its results, cross-check coverage with query_db (fetch_history can
  fill gaps).
- Keep SQL result sets small (aggregate, LIMIT); never SELECT * over the
  whole bar table.
- Be honest about backtesting pitfalls when relevant: survivorship bias,
  look-ahead, and overfitting are all real here and unmodelled. Transaction
  costs ARE modelled — see [portfolio.Costs] — so treat a frictionless result
  as a choice the config made, not a limit of the engine.`

// buildSystemPrompt assembles the static app reference plus the live,
// per-request context: today's date, the DB overview, the user's saved
// configs, and the unsaved editor buffers. lean produces a smaller prompt
// for local models: tighter clipping and no saved-config dump.
func buildSystemPrompt(dbPath, cfgText, luaText string, lean bool) string {
	bufCap := maxFileContextChars
	if lean {
		bufCap = maxLeanContextChars
	}
	var b strings.Builder
	b.WriteString(appReference)
	fmt.Fprintf(&b, "\n\nToday's date: %s\n", time.Now().Format("2006-01-02"))
	b.WriteString("\n## Live database\n")
	b.WriteString(dbOverview(dbPath))
	if !lean {
		b.WriteString("\n## User-defined configs on disk\n")
		b.WriteString(userConfigsContext())
	}
	if strings.TrimSpace(cfgText) != "" {
		fmt.Fprintf(&b,
			"\n## TOML config currently open in the editor (may be unsaved)\n%s\n",
			clipText(cfgText, bufCap))
	}
	if strings.TrimSpace(luaText) != "" {
		fmt.Fprintf(&b,
			"\n## Lua script currently open in the editor (may be unsaved)\n%s\n",
			clipText(luaText, bufCap))
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// query_db tool (shared; each provider wraps the schema in its wire format)

const queryDBToolName = "query_db"

const queryDBToolDescription = "Run one read-only SQL statement (DuckDB " +
	"dialect) against the backtester's market database. Allowed statements: " +
	"SELECT, WITH, DESCRIBE, SHOW, SUMMARIZE, EXPLAIN. Main tables: " +
	`stock_data_optimized(Date, Ticker, Open, High, Low, Close, Volume) ` +
	`and "3MTreasuryYields"(Date, daily_risk_free_rate_decimal). ` +
	"Results are capped at 100 rows, so aggregate or LIMIT. The database " +
	"holds ONLY price history and treasury yields — no fundamentals (P/E, " +
	"market cap, sector); use screen_stocks or lookup_quote for those."

func queryDBToolSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"sql": map[string]any{
				"type":        "string",
				"description": "A single read-only DuckDB SQL statement.",
			},
		},
		"required": []string{"sql"},
	}
}

var readOnlySQLPrefixes = []string{
	"select", "with", "describe", "show", "summarize", "explain",
}

// firstSQLKeyword returns the first word of the statement, skipping SQL
// line comments, lowercased.
func firstSQLKeyword(sql string) string {
	for _, line := range strings.Split(sql, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) > 0 {
			return strings.ToLower(strings.TrimLeft(fields[0], "("))
		}
	}
	return ""
}

// runQueryTool executes the model's SQL and formats the result as a
// compact pipe table. Errors come back as text so the model can correct
// its query instead of aborting the chat.
func runQueryTool(dbPath, sql string) string {
	kw := firstSQLKeyword(sql)
	allowed := false
	for _, p := range readOnlySQLPrefixes {
		if kw == p {
			allowed = true
			break
		}
	}
	if !allowed {
		return fmt.Sprintf(
			"ERROR: statement %q is not allowed — only read-only queries "+
				"(SELECT/WITH/DESCRIBE/SHOW/SUMMARIZE/EXPLAIN) are permitted.", kw)
	}
	if _, err := data.InitDB(dbPath); err != nil {
		return fmt.Sprintf("ERROR: open db %q: %v", dbPath, err)
	}
	cols, rows, truncated, err := data.RunQuery(sql, maxToolRows)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}
	var b strings.Builder
	b.WriteString(strings.Join(cols, " | "))
	b.WriteString("\n")
	for _, r := range rows {
		for i, cell := range r {
			if len(cell) > maxToolCellChars {
				r[i] = cell[:maxToolCellChars] + "…"
			}
		}
		b.WriteString(strings.Join(r, " | "))
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "(%d rows", len(rows))
	if truncated {
		fmt.Fprintf(&b, ", truncated at %d", maxToolRows)
	}
	b.WriteString(")")
	return b.String()
}

// ---------------------------------------------------------------------------
// Streaming emitter shared by both providers

// chatEmitter forwards text deltas and tool notices to the frontend while
// accumulating the final message. Emitting is skipped when there is no
// Wails context (tests). startTurn arms a paragraph separator so text from
// a follow-up model turn (after tool calls) doesn't glue onto the last.
type chatEmitter struct {
	app     *App
	final   strings.Builder
	pending bool
}

func (e *chatEmitter) send(event string, payload string) {
	if e.app != nil && e.app.ctx != nil {
		wruntime.EventsEmit(e.app.ctx, event, payload)
	}
}

func (e *chatEmitter) startTurn() {
	e.pending = e.final.Len() > 0
}

func (e *chatEmitter) text(s string) {
	if s == "" {
		return
	}
	if e.pending {
		e.final.WriteString("\n\n")
		e.send("chat:delta", "\n\n")
		e.pending = false
	}
	e.final.WriteString(s)
	e.send("chat:delta", s)
}

// tool announces a tool call with a one-line preview (the SQL for
// query_db, "toolname: args" for the others).
func (e *chatEmitter) tool(previewText string) {
	preview := strings.Join(strings.Fields(previewText), " ")
	if len(preview) > 90 {
		preview = preview[:90] + "…"
	}
	e.send("chat:tool", preview)
}

// dbChanged tells the frontend the database contents changed (the
// fetch_history tool stored new bars) so ticker lists can refresh.
func (e *chatEmitter) dbChanged() {
	e.send("chat:db-changed", "")
}

// ---------------------------------------------------------------------------
// Public API

// ListChatModels returns every model the chat can use: the Claude models
// plus whatever the local Ollama server has pulled (prefixed "ollama:").
// Ollama being down just means no local entries.
func (a *App) ListChatModels() []ModelOption {
	out := make([]ModelOption, 0, len(claudeModels)+4)
	out = append(out, claudeModels...)
	if tags, err := listOllamaTags(); err == nil {
		for _, m := range tags {
			out = append(out, ModelOption{
				ID:    ollamaPrefix + m.Name,
				Label: m.Name + " (local)",
			})
		}
	}
	return out
}

// Chat sends the conversation to the chosen model and returns the
// assistant's final text. While it runs, "chat:delta" events stream text
// chunks and "chat:tool" events announce database queries, so the
// frontend can render progress live.
func (a *App) Chat(
	messages []ChatMessage, model, cfgText, luaText, dbPath string,
) (string, error) {
	chatMu.Lock()
	defer chatMu.Unlock()

	msgs := make([]ChatMessage, 0, len(messages))
	for _, m := range messages {
		if strings.TrimSpace(m.Content) == "" {
			continue
		}
		if m.Role != "user" && m.Role != "assistant" {
			m.Role = "user"
		}
		msgs = append(msgs, m)
	}
	if len(msgs) == 0 {
		return "", fmt.Errorf("empty conversation")
	}

	ctx := a.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	em := &chatEmitter{app: a}
	var err error
	if name, ok := strings.CutPrefix(model, ollamaPrefix); ok {
		system := buildSystemPrompt(dbPath, cfgText, luaText, true)
		err = ollamaChat(ctx, name, system, msgs, dbPath, em)
	} else {
		system := buildSystemPrompt(dbPath, cfgText, luaText, false)
		err = anthropicChat(ctx, model, system, msgs, dbPath, em)
	}
	return em.final.String(), err
}
