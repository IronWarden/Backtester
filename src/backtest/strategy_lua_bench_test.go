package backtest

import (
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"my-backtester/src/data"
	"testing"
	"time"

	lua "github.com/yuin/gopher-lua"
)

// Benchmark setup: same SMA-cross logic implemented three ways against the
// same synthetic price series. The only variable is where the work happens
// (native Go / Lua interpreter / Lua orchestration + Go indicators).

const (
	benchTickers = 10
	benchDays    = 1000
	benchShort   = 10
	benchLong    = 50
	benchCash    = 100_000.0
)

func benchInit() {
	if TransactionLogger == nil {
		TransactionLogger = log.New(io.Discard, "", 0)
	}
}

func generateBenchData() ([]string, map[string][]data.AssetData) {
	tickers := make([]string, benchTickers)
	hist := make(map[string][]data.AssetData, benchTickers)
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < benchTickers; i++ {
		t := fmt.Sprintf("T%02d", i)
		tickers[i] = t
		rng := rand.New(rand.NewSource(int64(i + 1)))
		series := make([]data.AssetData, benchDays)
		price := 100.0
		for d := 0; d < benchDays; d++ {
			price += math.Sin(float64(d)/20.0)*0.4 + rng.NormFloat64()*0.5
			if price < 1 {
				price = 1
			}
			series[d] = data.AssetData{
				Date:   base.AddDate(0, 0, d),
				Open:   price,
				High:   price * 1.01,
				Low:    price * 0.99,
				Close:  price,
				Volume: 1_000_000,
			}
		}
		hist[t] = series
	}
	return tickers, hist
}

func newBenchPortfolio(tickers []string, strat Strategy) *Portfolio {
	return &Portfolio{
		Pname:                "bench",
		BuyingPower:          benchCash,
		InitialBuyingPower:   benchCash,
		Positions:            make(map[string]*Position),
		DailyReturns:         make([]DailyReturn, 0, benchDays),
		PortfolioCloseValues: make([]float64, 0, benchDays),
		Tickers:              tickers,
		Strategy:             strat,
	}
}

func runBenchSimulation(p *Portfolio, hist map[string][]data.AssetData) {
	p.Strategy.Step(p, hist, 0)
	prev := p.GetPortfolioValue(p.Tickers, hist, 0)
	for day := 1; day < benchDays; day++ {
		p.Strategy.Step(p, hist, day)
		curr := p.GetPortfolioValue(p.Tickers, hist, day)
		p.AdjustPortfolioParameters(p.Tickers, hist, day, prev, curr)
		prev = curr
	}
}

// === Native Go strategy (Close-based, to match Lua variants exactly) ===

type benchSMACrossClose struct {
	Short, Long int
	prevShort   map[string]float64
	prevLong    map[string]float64
}

func (s *benchSMACrossClose) Name() string { return "benchSMACrossClose" }

func (s *benchSMACrossClose) Step(p *Portfolio, hist map[string][]data.AssetData, day int) {
	if day < s.Long {
		return
	}
	if s.prevShort == nil {
		s.prevShort = make(map[string]float64, len(p.Tickers))
		s.prevLong = make(map[string]float64, len(p.Tickers))
	}
	for _, ticker := range p.Tickers {
		td := hist[ticker]
		if day >= len(td) {
			continue
		}
		ss := SMA(td[day-s.Short : day])
		ll := SMA(td[day-s.Long : day])
		price := td[day].Close
		ps, pl := s.prevShort[ticker], s.prevLong[ticker]
		if ps != 0 && pl != 0 {
			if ss > ll && ps <= pl {
				amount := generalBuy(p.BuyingPower, price, "equalWeights", p.Tickers)
				p.Buy(ticker, amount, price, td[day].Date)
			} else if ss < ll && ps >= pl {
				if pos, _ := p.FindPosition(ticker); pos != nil {
					p.Sell(ticker, pos.Amount, price, td[day].Date)
				}
			}
		}
		s.prevShort[ticker], s.prevLong[ticker] = ss, ll
	}
}

func BenchmarkSMACrossNative(b *testing.B) {
	benchInit()
	tickers, hist := generateBenchData()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := newBenchPortfolio(tickers, &benchSMACrossClose{
			Short: benchShort, Long: benchLong,
		})
		runBenchSimulation(p, hist)
	}
}

// === Lua strategies ===
//
// luaScriptPureLua: SMA computed entirely inside the Lua interpreter.
// luaScriptGoIndicators: Lua only orchestrates; SMA implemented as a Go
// function exposed to Lua. Both share the same crossover logic.

const luaScriptPureLua = `
function step(day)
    if day < long then return end
    for i = 1, #tickers do
        local t = tickers[i]
        local closes = data[t]
        local s, l = 0, 0
        for k = day - short + 1, day do s = s + closes[k+1] end
        for k = day - long  + 1, day do l = l + closes[k+1] end
        s = s / short
        l = l / long
        local ps = prevShort[t] or 0
        local pl = prevLong[t]  or 0
        if ps ~= 0 and pl ~= 0 then
            local price = closes[day+1]
            if s > l and ps <= pl then
                buy(t, price)
            elseif s < l and ps >= pl then
                sell(t, price)
            end
        end
        prevShort[t] = s
        prevLong[t]  = l
    end
end
`

const luaScriptGoIndicators = `
function step(day)
    if day < long then return end
    for i = 1, #tickers do
        local t = tickers[i]
        local s = sma(t, day, short)
        local l = sma(t, day, long)
        local ps = prevShort[t] or 0
        local pl = prevLong[t]  or 0
        if ps ~= 0 and pl ~= 0 then
            local price = price_at(t, day)
            if s > l and ps <= pl then
                buy(t, price)
            elseif s < l and ps >= pl then
                sell(t, price)
            end
        end
        prevShort[t] = s
        prevLong[t]  = l
    end
end
`

type luaBenchStrategy struct {
	L      *lua.LState
	stepFn *lua.LFunction
}

func (s *luaBenchStrategy) Name() string { return "luaBench" }

func (s *luaBenchStrategy) Step(p *Portfolio, hist map[string][]data.AssetData, day int) {
	s.L.Push(s.stepFn)
	s.L.Push(lua.LNumber(day))
	if err := s.L.PCall(1, 0, nil); err != nil {
		panic(err)
	}
}

func (s *luaBenchStrategy) Close() { s.L.Close() }

func newLuaBenchStrategy(
	script string,
	useGoIndicators bool,
	p *Portfolio,
	hist map[string][]data.AssetData,
) (*luaBenchStrategy, error) {
	L := lua.NewState()

	tbl := L.CreateTable(len(p.Tickers), 0)
	for i, t := range p.Tickers {
		tbl.RawSetInt(i+1, lua.LString(t))
	}
	L.SetGlobal("tickers", tbl)
	L.SetGlobal("short", lua.LNumber(benchShort))
	L.SetGlobal("long", lua.LNumber(benchLong))
	L.SetGlobal("prevShort", L.CreateTable(0, len(p.Tickers)))
	L.SetGlobal("prevLong", L.CreateTable(0, len(p.Tickers)))

	if useGoIndicators {
		L.SetGlobal("sma", L.NewFunction(func(L *lua.LState) int {
			ticker := L.ToString(1)
			day := L.ToInt(2)
			period := L.ToInt(3)
			series := hist[ticker]
			if day < period {
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
		L.SetGlobal("price_at", L.NewFunction(func(L *lua.LState) int {
			ticker := L.ToString(1)
			day := L.ToInt(2)
			L.Push(lua.LNumber(hist[ticker][day].Close))
			return 1
		}))
	} else {
		dataTbl := L.CreateTable(0, len(p.Tickers))
		for _, t := range p.Tickers {
			inner := L.CreateTable(len(hist[t]), 0)
			for i, ad := range hist[t] {
				inner.RawSetInt(i+1, lua.LNumber(ad.Close))
			}
			dataTbl.RawSetString(t, inner)
		}
		L.SetGlobal("data", dataTbl)
	}

	L.SetGlobal("buy", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		price := float64(L.ToNumber(2))
		amount := generalBuy(p.BuyingPower, price, "equalWeights", p.Tickers)
		p.Buy(ticker, amount, price, time.Time{})
		return 0
	}))
	L.SetGlobal("sell", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		price := float64(L.ToNumber(2))
		if pos, _ := p.FindPosition(ticker); pos != nil {
			p.Sell(ticker, pos.Amount, price, time.Time{})
		}
		return 0
	}))

	if err := L.DoString(script); err != nil {
		L.Close()
		return nil, err
	}
	fn, ok := L.GetGlobal("step").(*lua.LFunction)
	if !ok {
		L.Close()
		return nil, fmt.Errorf("step not a function")
	}
	return &luaBenchStrategy{L: L, stepFn: fn}, nil
}

func benchLua(b *testing.B, useGoIndicators bool, script string) {
	benchInit()
	tickers, hist := generateBenchData()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := newBenchPortfolio(tickers, nil)
		ls, err := newLuaBenchStrategy(script, useGoIndicators, p, hist)
		if err != nil {
			b.Fatal(err)
		}
		p.Strategy = ls
		runBenchSimulation(p, hist)
		ls.Close()
	}
}

func BenchmarkSMACrossLuaPureLua(b *testing.B)      { benchLua(b, false, luaScriptPureLua) }
func BenchmarkSMACrossLuaGoIndicators(b *testing.B) { benchLua(b, true, luaScriptGoIndicators) }
