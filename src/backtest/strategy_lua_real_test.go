package backtest

import (
	"math"
	"math/rand"
	"my-backtester/src/data"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// Loads each .lua file from the actual strategies/ directory, runs the
// real day loop (runOne) against synthetic OHLCV, and asserts the strategy
// did something sensible. The aim is a smoke test of the full pipeline:
// NewStrategy → LuaStrategy.init → Step loop → metrics → Close.

func strategiesDir(t testing.TB) string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// src/backtest/strategy_lua_real_test.go -> ../../strategies
	return filepath.Join(filepath.Dir(file), "..", "..", "strategies")
}

// generateRealTestData makes a longer, more varied series than the bench
// data so RSI/SMA strategies see real crossings.
func generateRealTestData(tickers []string, days int) (
	map[string][]data.AssetData, map[int64]float64,
) {
	hist := make(map[string][]data.AssetData, len(tickers))
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, ticker := range tickers {
		rng := rand.New(rand.NewSource(int64(i + 1)))
		series := make([]data.AssetData, days)
		price := 100.0 + float64(i*10)
		for d := 0; d < days; d++ {
			// Sine wave plus noise — gives both SMA crossings and RSI
			// extremes inside `days`.
			price += math.Sin(float64(d)/15.0)*1.2 + rng.NormFloat64()*0.6
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
		hist[ticker] = series
	}
	// Empty risk-free rates is fine — Sharpe/Sortino fall through to 0.
	return hist, map[int64]float64{}
}

func newRealTestPortfolio(
	t testing.TB,
	tickers []string,
	cash float64,
	spec string,
	params map[string]any,
) *Portfolio {
	benchInit()
	strat, err := NewStrategy(spec, params)
	if err != nil {
		t.Fatalf("NewStrategy(%q, %v): %v", spec, params, err)
	}
	return &Portfolio{
		Pname:                "lua-real-test",
		BuyingPower:          cash,
		InitialBuyingPower:   cash,
		Positions:            make(map[string]*Position),
		DailyReturns:         make([]DailyReturn, 0, 256),
		PortfolioCloseValues: make([]float64, 0, 256),
		Tickers:              tickers,
		StrategySpec:         spec,
		StrategyParams:       params,
		Strategy:             strat,
	}
}

func TestLuaStrategy_BuyAndHold(t *testing.T) {
	dir := strategiesDir(t)
	tickers := []string{"AAA", "BBB", "CCC"}
	hist, rf := generateRealTestData(tickers, 120)

	// NOTE: using greedy because the pre-existing equalWeights sizing in
	// generalBuy (strategy.go) divides via integer math and yields 0
	// shares for any N>1 tickers.
	p := newRealTestPortfolio(t, tickers, 30_000,
		"lua:"+filepath.Join(dir, "buy_and_hold.lua"),
		map[string]any{"buyType": "greedy"},
	)
	runOne(p, hist, rf)

	if len(p.Positions) == 0 {
		t.Fatalf("expected at least one open position after buy_and_hold")
	}
	totalShares := 0.0
	for _, pos := range p.Positions {
		totalShares += pos.Amount
	}
	if totalShares <= 0 {
		t.Errorf("no shares held after buy_and_hold")
	}
	if p.BuyingPower < 0 {
		t.Errorf("buying power went negative: %.2f", p.BuyingPower)
	}
}

func TestLuaStrategy_SMACross(t *testing.T) {
	dir := strategiesDir(t)
	tickers := []string{"AAA", "BBB"}
	hist, rf := generateRealTestData(tickers, 400)

	p := newRealTestPortfolio(t, tickers, 50_000,
		"lua:"+filepath.Join(dir, "sma_cross.lua"),
		map[string]any{
			"short":   int64(5),
			"long":    int64(20),
			"buyType": "greedy",
		},
	)
	runOne(p, hist, rf)

	if len(p.DailyReturns) == 0 {
		t.Fatal("smaCross produced no daily returns")
	}
	// Synthetic sine-wave data must produce at least one crossing per
	// ticker over 400 days — verifying the script actually traded.
	nonzero := 0
	for _, dr := range p.DailyReturns {
		if dr.Return != 0 {
			nonzero++
		}
	}
	if nonzero == 0 {
		t.Error("smaCross never moved portfolio value — no trades fired")
	}
}

func TestLuaStrategy_RSI(t *testing.T) {
	dir := strategiesDir(t)
	tickers := []string{"AAA", "BBB"}
	hist, rf := generateRealTestData(tickers, 300)

	p := newRealTestPortfolio(t, tickers, 50_000,
		"lua:"+filepath.Join(dir, "rsi.lua"),
		map[string]any{
			"period":      int64(14),
			"buy_thresh":  int64(35),
			"sell_thresh": int64(65),
			"buyType":     "greedy",
		},
	)
	runOne(p, hist, rf)

	if len(p.DailyReturns) == 0 {
		t.Fatal("rsi produced no daily returns")
	}
	nonzero := 0
	for _, dr := range p.DailyReturns {
		if dr.Return != 0 {
			nonzero++
		}
	}
	if nonzero == 0 {
		t.Error("rsi never moved portfolio value — no trades fired")
	}
}

func TestLuaStrategy_CloneIsolation(t *testing.T) {
	// Two clones running in sequence must not share Lua state. We use
	// the SMA cross strategy and confirm each clone gets fresh prevShort
	// / prevLong (i.e. no carry-over of crossings from the prior run).
	dir := strategiesDir(t)
	tickers := []string{"AAA"}
	hist, rf := generateRealTestData(tickers, 200)

	template := newRealTestPortfolio(t, tickers, 25_000,
		"lua:"+filepath.Join(dir, "sma_cross.lua"),
		map[string]any{
			"short":   int64(5),
			"long":    int64(20),
			"buyType": "greedy",
		},
	)

	c1, err := template.Clone()
	if err != nil {
		t.Fatalf("clone 1: %v", err)
	}
	c2, err := template.Clone()
	if err != nil {
		t.Fatalf("clone 2: %v", err)
	}
	if c1.Strategy == c2.Strategy {
		t.Fatal("clones share the same Strategy instance")
	}
	runOne(c1, hist, rf)
	runOne(c2, hist, rf)

	// Same inputs → same deterministic outputs. Comparing the daily
	// portfolio value series is the cleanest equality check; Sharpe etc.
	// can be NaN when the risk-free map is empty.
	if len(c1.PortfolioCloseValues) != len(c2.PortfolioCloseValues) {
		t.Fatalf("clones diverged in series length: %d vs %d",
			len(c1.PortfolioCloseValues), len(c2.PortfolioCloseValues))
	}
	for i := range c1.PortfolioCloseValues {
		if c1.PortfolioCloseValues[i] != c2.PortfolioCloseValues[i] {
			t.Errorf("clones diverged at day %d: %.4f vs %.4f — shared "+
				"Lua state suspected",
				i, c1.PortfolioCloseValues[i], c2.PortfolioCloseValues[i])
			break
		}
	}
	if c1.BuyingPower == c1.InitialBuyingPower {
		t.Error("clone 1 never traded; cannot validate isolation")
	}
}

func TestLuaStrategy_BadSpec(t *testing.T) {
	if _, err := NewStrategy("lua:", nil); err == nil {
		t.Error("expected error for empty path")
	}
	if _, err := NewStrategy(
		"lua:/nonexistent/path/strategy.lua", nil,
	); err == nil {
		t.Error("expected error for missing file")
	}
}

// TestLuaStrategy_TypedParams verifies that TOML-style typed values
// reach Lua with their native types preserved: ints stay ints, floats
// stay floats, bools stay bools, arrays become 1-indexed tables, and
// nested maps become sub-tables. The script writes outcomes back into
// the portfolio via trade calls so we can assert from Go.
func TestLuaStrategy_TypedParams(t *testing.T) {
	script := filepath.Join(t.TempDir(), "typed.lua")
	if err := os.WriteFile(script, []byte(`
function step(day)
  if day ~= 0 then return end
  -- Param types must round-trip from Go to Lua intact.
  assert(type(params.an_int) == "number",
    "an_int type: "..type(params.an_int))
  assert(params.an_int == 7, "an_int value: "..tostring(params.an_int))
  assert(type(params.a_float) == "number",
    "a_float type: "..type(params.a_float))
  assert(math.abs(params.a_float - 1.5) < 1e-9,
    "a_float value: "..tostring(params.a_float))
  assert(type(params.flag) == "boolean",
    "flag type: "..type(params.flag))
  assert(params.flag == true, "flag value: "..tostring(params.flag))
  assert(type(params.label) == "string",
    "label type: "..type(params.label))
  assert(params.label == "hi", "label value: "..tostring(params.label))
  assert(type(params.list) == "table",
    "list type: "..type(params.list))
  assert(#params.list == 3, "list length: "..#params.list)
  assert(params.list[2] == 20, "list[2]: "..tostring(params.list[2]))
  assert(type(params.nested) == "table",
    "nested type: "..type(params.nested))
  assert(params.nested.inner == 42,
    "nested.inner: "..tostring(params.nested.inner))

  -- Use a value to actually trade so we can verify the assert path ran.
  local qty = params.an_int  -- 7
  buy(tickers[1], qty, price(tickers[1], day), day)
end
`), 0644); err != nil {
		t.Fatal(err)
	}

	tickers := []string{"AAA"}
	hist, rf := generateRealTestData(tickers, 10)
	params := map[string]any{
		"an_int":  int64(7),
		"a_float": 1.5,
		"flag":    true,
		"label":   "hi",
		"list":    []any{int64(10), int64(20), int64(30)},
		"nested":  map[string]any{"inner": int64(42)},
	}
	p := newRealTestPortfolio(t, tickers, 100_000, "lua:"+script, params)
	runOne(p, hist, rf)

	pos, ok := p.FindPosition("AAA")
	if !ok || pos.Amount != 7 {
		t.Fatalf("expected 7 shares of AAA (proves assert-and-trade "+
			"path executed); got pos=%+v ok=%v", pos, ok)
	}
}

