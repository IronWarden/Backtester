package backtest

import (
	"fmt"
	"log"
	"my-backtester/src/data"
	"os"
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
	Path   string
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
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("lua script %q: %w", path, err)
	}
	return &LuaStrategy{Path: path, Params: params}, nil
}

func (s *LuaStrategy) Name() string { return "lua:" + s.Path }

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
			log.Printf("lua strategy %q init: %v", s.Path, err)
			return
		}
	}
	s.L.Push(s.stepFn)
	s.L.Push(lua.LNumber(day))
	if err := s.L.PCall(1, 0, nil); err != nil {
		log.Printf("lua strategy %q step day=%d: %v", s.Path, day, err)
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
	registerTrading(L, p, hist)

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

func registerTrading(
	L *lua.LState, p *Portfolio, hist map[string][]data.AssetData,
) {
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
		ticker := L.ToString(1)
		price := float64(L.ToNumber(2))
		buyType := L.OptString(3, "equalWeights")
		day := L.OptInt(4, -1)
		amount := generalBuy(p.BuyingPower, price, buyType, p.Tickers)
		p.Buy(ticker, amount, price, dateOf(ticker, day))
		L.Push(lua.LNumber(amount))
		return 1
	}))

	// buy(ticker, amount, price, [day=-1]) — caller-sized buy.
	L.SetGlobal("buy", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		amount := float64(L.ToNumber(2))
		price := float64(L.ToNumber(3))
		day := L.OptInt(4, -1)
		p.Buy(ticker, amount, price, dateOf(ticker, day))
		return 0
	}))

	// sell(ticker, amount, price, [day=-1]) — caller-sized sell.
	L.SetGlobal("sell", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		amount := float64(L.ToNumber(2))
		price := float64(L.ToNumber(3))
		day := L.OptInt(4, -1)
		p.Sell(ticker, amount, price, dateOf(ticker, day))
		return 0
	}))

	// sell_all(ticker, price, [day=-1]) — closes the entire position.
	L.SetGlobal("sell_all", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		price := float64(L.ToNumber(2))
		day := L.OptInt(3, -1)
		if pos, _ := p.FindPosition(ticker); pos != nil {
			p.Sell(ticker, pos.Amount, price, dateOf(ticker, day))
		}
		return 0
	}))
}
