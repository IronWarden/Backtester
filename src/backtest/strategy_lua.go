package backtest

import (
	"fmt"
	"my-backtester/src/data"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	lua "github.com/yuin/gopher-lua"
)

// LuaStrategy runs a user-supplied Lua script that handles control flow.
// All numeric heavy-lifting (SMA, RSI, OHLCV lookups, order sizing) stays
// in Go and is exposed to the script as registered globals. The Lua state
// is lazily constructed on the first Step call because Step is when the
// strategy first sees the live portfolio and price history. Each Portfolio
// clone yields its own LuaStrategy with its own lua.LState, so workers do
// not share interpreter state.
//
// Spec format: "lua:<path>". Strategy parameters travel separately as a
// typed map and arrive in the script as the `params` global table
// (numbers as Lua numbers, bools as booleans, arrays as 1-indexed tables,
// sub-tables as nested tables).
type LuaStrategy struct {
	// Path is the resolved, openable location; Spec is the path exactly as
	// the config wrote it, kept for display.
	Path   string
	Spec   string
	Params map[string]any

	L      *lua.LState
	stepFn *lua.LFunction
}

func NewLuaStrategy(
	path string, params map[string]any,
) (*LuaStrategy, error) {
	if path == "" {
		return nil, fmt.Errorf("lua script path required")
	}
	resolved, err := resolveLuaPath(path)
	if err != nil {
		return nil, fmt.Errorf("lua script %q: %w", path, err)
	}
	return &LuaStrategy{Path: resolved, Spec: path, Params: params}, nil
}

// resolveLuaPath locates a script relative to the working directory or to a
// parent of it. The CLI runs from the repo root while the desktop app runs
// from ui/, so a config saying "lua:strategies/foo.lua" must work from
// both without the user hand-editing "../" prefixes per launcher.
func resolveLuaPath(path string) (string, error) {
	if filepath.IsAbs(path) {
		if _, err := os.Stat(path); err != nil {
			return "", err
		}
		return path, nil
	}
	candidates := []string{
		path,
		filepath.Join("..", path),
		filepath.Join("..", "..", path),
	}
	var firstErr error
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c, nil
		} else if firstErr == nil {
			firstErr = err
		}
	}
	return "", firstErr
}

// Name reports the spec as the user wrote it, so results stay labelled the
// same however the process happened to be launched.
func (s *LuaStrategy) Name() string {
	if s.Spec != "" {
		return "lua:" + s.Spec
	}
	return "lua:" + s.Path
}

// Close releases the underlying lua.LState. Safe to call multiple times.
func (s *LuaStrategy) Close() {
	if s.L != nil {
		s.L.Close()
		s.L = nil
	}
}

func (s *LuaStrategy) Step(
	p *Portfolio, hist map[string][]data.AssetData, day int,
) {
	if s.L == nil {
		if err := s.init(p, hist); err != nil {
			diagf("error", p.Pname, "lua strategy %q init: %v", s.Path, err)
			return
		}
	}
	s.L.Push(s.stepFn)
	s.L.Push(lua.LNumber(day))
	if err := s.L.PCall(1, 0, nil); err != nil {
		diagf("error", p.Pname,
			"lua strategy %q step day=%d: %v", s.Path, day, err)
	}
}

func (s *LuaStrategy) init(
	p *Portfolio, hist map[string][]data.AssetData,
) error {
	L := lua.NewState()

	tickersTbl := L.CreateTable(len(p.Tickers), 0)
	for i, t := range p.Tickers {
		tickersTbl.RawSetInt(i+1, lua.LString(t))
	}
	L.SetGlobal("tickers", tickersTbl)

	L.SetGlobal("params", goToLua(L, s.Params))

	registerIndicators(L, hist)
	registerOHLCV(L, hist)
	registerTrading(L, p, hist, s.Path)
	registerLogging(L, p, s.Path)
	registerWindow(L, p, hist)
	// Additive only — see strategy_lua_signals.go. No global registered above
	// is replaced, so every existing script behaves exactly as it did.
	registerSignals(L, hist)
	registerCrossSection(L, p)
	registerFundamentals(L, p, hist)
	registerBook(L, p, hist)

	warnUnreadParams(p, s.Path, s.Params)

	if err := L.DoFile(s.Path); err != nil {
		L.Close()
		return fmt.Errorf("loading %q: %w", s.Path, err)
	}
	fn, ok := L.GetGlobal("step").(*lua.LFunction)
	if !ok {
		L.Close()
		return fmt.Errorf(
			"%q: global 'step' must be a function", s.Path,
		)
	}
	s.L = L
	s.stepFn = fn
	return nil
}

// goToLua converts a value decoded from TOML (or any Go map/slice tree)
// into the equivalent Lua value. TOML integers arrive as int64 and floats
// as float64; both map to lua.LNumber. Arrays become 1-indexed Lua tables.
// Unknown types are stringified as a last resort so scripts still see
// *something* rather than nil.
func goToLua(L *lua.LState, v any) lua.LValue {
	switch x := v.(type) {
	case nil:
		return lua.LNil
	case bool:
		return lua.LBool(x)
	case int:
		return lua.LNumber(x)
	case int64:
		return lua.LNumber(x)
	case float64:
		return lua.LNumber(x)
	case string:
		return lua.LString(x)
	case []any:
		t := L.CreateTable(len(x), 0)
		for i, item := range x {
			t.RawSetInt(i+1, goToLua(L, item))
		}
		return t
	case map[string]any:
		t := L.CreateTable(0, len(x))
		for k, item := range x {
			t.RawSetString(k, goToLua(L, item))
		}
		return t
	default:
		return lua.LString(fmt.Sprintf("%v", x))
	}
}

func registerIndicators(
	L *lua.LState, hist map[string][]data.AssetData,
) {
	// sma(ticker, day, period) — mean Close over [day-period, day).
	L.SetGlobal("sma", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		day := L.ToInt(2)
		period := L.ToInt(3)
		series := hist[ticker]
		if period <= 0 || day < period || day > len(series) {
			L.Push(lua.LNumber(0))
			return 1
		}
		sum := 0.0
		for i := day - period; i < day; i++ {
			sum += series[i].Close
		}
		L.Push(lua.LNumber(sum / float64(period)))
		return 1
	}))

	// rsi(ticker, day, period) — Wilder-lite RSI on Close changes over
	// the trailing `period` days ending at `day`. Returns 50 if there is
	// not enough history yet, 100 if there have been no losses.
	L.SetGlobal("rsi", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		day := L.ToInt(2)
		period := L.ToInt(3)
		series := hist[ticker]
		if period <= 0 || day < period || day >= len(series) {
			L.Push(lua.LNumber(50))
			return 1
		}
		gain, loss := 0.0, 0.0
		for i := day - period + 1; i <= day; i++ {
			change := series[i].Close - series[i-1].Close
			if change >= 0 {
				gain += change
			} else {
				loss -= change
			}
		}
		gain /= float64(period)
		loss /= float64(period)
		if loss == 0 {
			L.Push(lua.LNumber(100))
			return 1
		}
		rs := gain / loss
		L.Push(lua.LNumber(100 - (100 / (1 + rs))))
		return 1
	}))
}

func registerOHLCV(
	L *lua.LState, hist map[string][]data.AssetData,
) {
	field := func(pick func(data.AssetData) float64) lua.LGFunction {
		return func(L *lua.LState) int {
			ticker := L.ToString(1)
			day := L.ToInt(2)
			series, ok := hist[ticker]
			if !ok || day < 0 || day >= len(series) {
				L.Push(lua.LNumber(0))
				return 1
			}
			L.Push(lua.LNumber(pick(series[day])))
			return 1
		}
	}
	L.SetGlobal("open_at", L.NewFunction(
		field(func(a data.AssetData) float64 { return a.Open }),
	))
	L.SetGlobal("high_at", L.NewFunction(
		field(func(a data.AssetData) float64 { return a.High }),
	))
	L.SetGlobal("low_at", L.NewFunction(
		field(func(a data.AssetData) float64 { return a.Low }),
	))
	L.SetGlobal("close_at", L.NewFunction(
		field(func(a data.AssetData) float64 { return a.Close }),
	))
	L.SetGlobal("volume_at", L.NewFunction(
		field(func(a data.AssetData) float64 { return a.Volume }),
	))
	L.SetGlobal("price", L.NewFunction(
		field(func(a data.AssetData) float64 { return a.Close }),
	))

	L.SetGlobal("date_at", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		day := L.ToInt(2)
		series, ok := hist[ticker]
		if !ok || day < 0 || day >= len(series) {
			L.Push(lua.LString(""))
			return 1
		}
		L.Push(lua.LString(series[day].Date.Format("2006-01-02")))
		return 1
	}))
}

// warnUnreadParams reports a Params key the script never mentions, which is
// almost always a misspelling.
//
// The failure it catches is silent and expensive. Scripts read their settings
// as `params.lookback or 126`, so a key spelled wrong does not error — it
// falls through to the default and the run proceeds with settings the user
// did not choose. In a SWEEP it is worse: sweep values are merged into Params
// (see ToPortfolios), so a mistyped sweep key expands to N portfolios that
// differ only in a value nothing reads. Every trial produces identical
// numbers, while Trials is N and the deflated Sharpe duly discounts the
// result for a search that never happened.
//
// Checked against the source text rather than by instrumenting the table.
// Making `params` a recording proxy would fire __index only on missing keys,
// so it would have to be an empty table backed by a metatable — and gopher-lua
// is Lua 5.1, with no __pairs, which would silently break any script iterating
// its own params. A source scan cannot break a script that works.
//
// It can miss a key composed at runtime (`params[name]`), so this warns rather
// than rejects.
func warnUnreadParams(p *Portfolio, path string, params map[string]any) {
	if len(params) == 0 {
		return
	}
	src, err := os.ReadFile(path)
	if err != nil {
		return // DoFile is about to report this properly.
	}
	text := string(src)

	unread := make([]string, 0, len(params))
	for key := range params {
		if !mentionsParam(text, key) {
			unread = append(unread, key)
		}
	}
	if len(unread) == 0 {
		return
	}
	sort.Strings(unread)

	for _, key := range unread {
		msg := fmt.Sprintf(
			"lua strategy %q: Params key %q is never read by the script, so "+
				"its value is being ignored — check the spelling",
			path, key)
		if p.Trials > 1 {
			msg += fmt.Sprintf(
				"; this is a sweep of %d trials, so if %q is a swept key "+
					"every trial is running identical settings",
				p.Trials, key)
		}
		diagf("warn", p.Pname, "%s", msg)
	}
}

// mentionsParam reports whether the source reads this key off the params
// table, in any of the three spellings Lua allows.
func mentionsParam(src, key string) bool {
	return strings.Contains(src, "params."+key) ||
		strings.Contains(src, `params["`+key+`"]`) ||
		strings.Contains(src, "params['"+key+"']")
}

// registerWindow tells a script how long its run is. A strategy is handed
// `day` and had no way to learn what it counts up to, so "do this once at the
// end" — liquidate, take a final reading — was not expressible at all. Every
// ticker has the same number of bars once the window is aligned (see
// alignToWindow and T67), so one series answers it.
func registerWindow(
	L *lua.LState, p *Portfolio, hist map[string][]data.AssetData,
) {
	days := 0
	if len(p.Tickers) > 0 {
		days = len(hist[p.Tickers[0]])
	}
	// num_days() — trading days in this run. day is 0-based, so the last one
	// is num_days() - 1.
	L.SetGlobal("num_days", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LNumber(days))
		return 1
	}))
}

// registerLogging gives a script a way to say something its author will
// actually see. Lua's own print writes to stdout, which is fine from the CLI
// and invisible in the desktop app — a strategy author debugging there got
// nothing at all. log() routes to the run console instead, and to the Go log
// alongside it, so both launchers show it.
//
// Messages are info-level: they are not defects, they are the script talking.
// The console's per-run cap is what stops a log call inside the day loop from
// flooding it, so no deduplication here — a log line is deliberate and
// repeating it on each day is often the point.
func registerLogging(L *lua.LState, p *Portfolio, script string) {
	// log(...) — any number of values, rendered as Lua's tostring would and
	// joined with spaces, like print.
	L.SetGlobal("log", L.NewFunction(func(L *lua.LState) int {
		n := L.GetTop()
		parts := make([]string, 0, n)
		for i := 1; i <= n; i++ {
			parts = append(parts, L.ToStringMeta(L.Get(i)).String())
		}
		diagf("info", p.Pname, "lua strategy %q: %s",
			script, strings.Join(parts, " "))
		return 0
	}))
}

func registerTrading(
	L *lua.LState, p *Portfolio, hist map[string][]data.AssetData,
	script string,
) {
	// resolveTicker is the gate on every order function. The order functions
	// used to read their ticker with L.ToString(1), which yields "" for a nil
	// or non-string argument, and Buy will open a position under any key —
	// including "". GetPortfolioValue iterates p.Tickers, so such a position is
	// never valued: the cash left the equity curve and never came back, and a
	// typo'd ticker read as a permanent drag on returns rather than as an
	// error (T9).
	//
	// Rejecting an unknown ticker breaks nothing that ever worked. A script
	// trading outside its Tickers list was already losing the money silently,
	// so there is no behaviour here worth preserving.
	//
	// Reported once per function-and-ticker pair: a typo inside a day loop
	// would otherwise say the same thing on every trading day.
	valid := make(map[string]bool, len(p.Tickers))
	for _, t := range p.Tickers {
		valid[t] = true
	}
	warned := make(map[string]bool)
	resolveTicker := func(L *lua.LState, fn string) (string, bool) {
		ticker := L.ToString(1)
		if valid[ticker] {
			return ticker, true
		}
		key := fn + "\x00" + ticker
		if !warned[key] {
			warned[key] = true
			// Naming the valid list is the point: a typo is the common case,
			// and seeing what was expected next to what was asked for is what
			// makes it obvious.
			diagf("error", p.Pname,
				"lua strategy %q: %s(%q) — not in Tickers %v; order ignored",
				script, fn, ticker, p.Tickers)
		}
		return "", false
	}

	dateOf := func(ticker string, day int) time.Time {
		if day < 0 {
			return time.Time{}
		}
		series, ok := hist[ticker]
		if !ok || day >= len(series) {
			return time.Time{}
		}
		return series[day].Date
	}

	L.SetGlobal("cash", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LNumber(p.BuyingPower))
		return 1
	}))

	L.SetGlobal("position", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		pos, ok := p.FindPosition(ticker)
		if !ok || pos == nil || pos.Amount == 0 {
			L.Push(lua.LNil)
			return 1
		}
		t := L.CreateTable(0, 3)
		t.RawSetString("amount", lua.LNumber(pos.Amount))
		t.RawSetString("avg_price", lua.LNumber(pos.AveragePrice))
		t.RawSetString("current_price", lua.LNumber(pos.CurrentPrice))
		L.Push(t)
		return 1
	}))

	// buy_max(ticker, price, [buyType="equalWeights"], [day=-1])
	// Sizes the order with generalBuy and submits it. Returns the share
	// count it actually placed (caller can ignore).
	L.SetGlobal("buy_max", L.NewFunction(func(L *lua.LState) int {
		ticker, ok := resolveTicker(L, "buy_max")
		if !ok {
			L.Push(lua.LNumber(0))
			return 1
		}
		price := float64(L.ToNumber(2))
		buyType := L.OptString(3, "equalWeights")
		day := L.OptInt(4, -1)
		amount := generalBuy(p.BuyingPower, price, buyType, p.Tickers, p.Costs)
		p.Buy(ticker, amount, price, dateOf(ticker, day))
		L.Push(lua.LNumber(amount))
		return 1
	}))

	// buy(ticker, amount, price, [day=-1]) — caller-sized buy.
	L.SetGlobal("buy", L.NewFunction(func(L *lua.LState) int {
		ticker, ok := resolveTicker(L, "buy")
		if !ok {
			return 0
		}
		amount := float64(L.ToNumber(2))
		price := float64(L.ToNumber(3))
		day := L.OptInt(4, -1)
		p.Buy(ticker, amount, price, dateOf(ticker, day))
		return 0
	}))

	// sell(ticker, amount, price, [day=-1]) — caller-sized sell.
	L.SetGlobal("sell", L.NewFunction(func(L *lua.LState) int {
		ticker, ok := resolveTicker(L, "sell")
		if !ok {
			return 0
		}
		amount := float64(L.ToNumber(2))
		price := float64(L.ToNumber(3))
		day := L.OptInt(4, -1)
		p.Sell(ticker, amount, price, dateOf(ticker, day))
		return 0
	}))

	// sell_all(ticker, price, [day=-1]) — closes the entire position.
	L.SetGlobal("sell_all", L.NewFunction(func(L *lua.LState) int {
		ticker, ok := resolveTicker(L, "sell_all")
		if !ok {
			return 0
		}
		price := float64(L.ToNumber(2))
		day := L.OptInt(3, -1)
		if pos, _ := p.FindPosition(ticker); pos != nil {
			p.Sell(ticker, pos.Amount, price, dateOf(ticker, day))
		}
		return 0
	}))
}
