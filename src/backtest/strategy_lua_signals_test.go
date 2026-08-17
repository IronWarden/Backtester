package backtest

// The Lua signal library, and the proof that extracting it changed nothing.
//
// The primitives replace hand-written arithmetic inside two shipped strategies.
// The only test that really matters is therefore equivalence: the rewritten
// scripts must produce the same trades and the same daily values as the versions
// that carried their own copy. The legacy sources are preserved verbatim below
// as fixtures for exactly that comparison — do not "tidy" them, they are a
// record of what the output has to keep matching.

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	lua "github.com/yuin/gopher-lua"
)

// legacyRebalance is rebalance.lua as it stood before the primitives existed,
// with its own equity() and rebalance_to().
const legacyRebalance = `
local every   = params.rebalance_days or 63
local weights = params.weights

local function equity(day)
    local total = cash()
    for i = 1, #tickers do
        local pos = position(tickers[i])
        if pos then
            total = total + pos.amount * price(tickers[i], day)
        end
    end
    return total
end

local function rebalance_to(day, targets)
    local eq = equity(day)
    for i = 1, #tickers do
        local t, px = tickers[i], price(tickers[i], day)
        if px > 0 then
            local pos = position(t)
            local held = pos and pos.amount or 0
            local want = (eq * (targets[t] or 0)) / px
            if held - want > 1e-9 then
                sell(t, held - want, px, day)
            end
        end
    end
    for i = 1, #tickers do
        local t, px = tickers[i], price(tickers[i], day)
        if px > 0 then
            local pos = position(t)
            local held = pos and pos.amount or 0
            local want = (eq * (targets[t] or 0)) / px
            local short = want - held
            local affordable = cash() / px
            if short > affordable then short = affordable end
            if short > 1e-9 then
                buy(t, short, px, day)
            end
        end
    end
end

function step(day)
    if day % every ~= 0 then return end

    local targets = {}
    if weights then
        local total = 0.0
        for i = 1, #tickers do
            total = total + (weights[tickers[i]] or 0)
        end
        if total <= 0 then return end
        for i = 1, #tickers do
            targets[tickers[i]] = (weights[tickers[i]] or 0) / total
        end
    else
        for i = 1, #tickers do
            targets[tickers[i]] = 1.0 / #tickers
        end
    end
    rebalance_to(day, targets)
end
`

// legacyMomentum is momentum_rotation.lua before the primitives, including its
// own table.sort ranking.
const legacyMomentum = `
local lookback = params.lookback or 126
local skip     = params.skip_days or 0
local top_n    = params.top_n or 2
local every    = params.rebalance_days or 21

local function equity(day)
    local total = cash()
    for i = 1, #tickers do
        local pos = position(tickers[i])
        if pos then
            total = total + pos.amount * price(tickers[i], day)
        end
    end
    return total
end

local function rebalance_to(day, targets)
    local eq = equity(day)
    for i = 1, #tickers do
        local t, px = tickers[i], price(tickers[i], day)
        if px > 0 then
            local pos = position(t)
            local held = pos and pos.amount or 0
            local want = (eq * (targets[t] or 0)) / px
            if held - want > 1e-9 then
                sell(t, held - want, px, day)
            end
        end
    end
    for i = 1, #tickers do
        local t, px = tickers[i], price(tickers[i], day)
        if px > 0 then
            local pos = position(t)
            local held = pos and pos.amount or 0
            local want = (eq * (targets[t] or 0)) / px
            local short = want - held
            local affordable = cash() / px
            if short > affordable then short = affordable end
            if short > 1e-9 then
                buy(t, short, px, day)
            end
        end
    end
end

function step(day)
    if day < lookback + skip then return end
    if day % every ~= 0 then return end

    local ranked = {}
    for i = 1, #tickers do
        local t = tickers[i]
        local recent = price(t, day - skip)
        local past   = price(t, day - skip - lookback)
        if recent > 0 and past > 0 then
            ranked[#ranked + 1] = { ticker = t, ret = recent / past - 1 }
        end
    end
    table.sort(ranked, function(a, b) return a.ret > b.ret end)

    local n = math.min(top_n, #ranked)
    local winners = 0
    for i = 1, n do
        if ranked[i].ret > 0 then winners = winners + 1 end
    end

    local targets = {}
    if winners > 0 then
        for i = 1, n do
            if ranked[i].ret > 0 then
                targets[ranked[i].ticker] = 1.0 / winners
            end
        end
    end
    rebalance_to(day, targets)
end
`

// runScriptSource writes a script and drives it exactly as runStrategyScript
// drives a shipped one, so the two are comparable day for day.
func runScriptSource(
	t *testing.T, src string, params map[string]any,
) *Portfolio {
	t.Helper()
	path := filepath.Join(t.TempDir(), "legacy.lua")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatalf("write script: %v", err)
	}
	p, logged := runStrategyScript(t, path, params)
	if logged != "" {
		t.Fatalf("legacy script logged: %s", logged)
	}
	return p
}

// assertSameRun compares two runs bar for bar. Anything but an exact match means
// the extracted primitive is not the code it replaced.
func assertSameRun(t *testing.T, label string, want, got *Portfolio) {
	t.Helper()

	if len(want.PortfolioCloseValues) != len(got.PortfolioCloseValues) {
		t.Fatalf("%s: %d daily values vs %d", label,
			len(want.PortfolioCloseValues), len(got.PortfolioCloseValues))
	}
	for i := range want.PortfolioCloseValues {
		w, g := want.PortfolioCloseValues[i], got.PortfolioCloseValues[i]
		if math.Abs(w-g) > 1e-9*math.Max(1, math.Abs(w)) {
			t.Fatalf("%s: day %d value %.10f vs %.10f — the primitive is not "+
				"equivalent to the code it replaced", label, i, w, g)
		}
	}
	if math.Abs(want.BuyingPower-got.BuyingPower) > 1e-9 {
		t.Errorf("%s: cash %.10f vs %.10f", label,
			want.BuyingPower, got.BuyingPower)
	}
	for _, ticker := range libTickers {
		wp, wok := want.FindPosition(ticker)
		gp, gok := got.FindPosition(ticker)
		if wok != gok {
			t.Errorf("%s: %s held=%v vs %v", label, ticker, wok, gok)
			continue
		}
		if !wok {
			continue
		}
		if math.Abs(wp.Amount-gp.Amount) > 1e-9 {
			t.Errorf("%s: %s shares %.10f vs %.10f", label, ticker,
				wp.Amount, gp.Amount)
		}
	}
}

// The equivalence that authorises the whole task: rebalance.lua rewritten on
// target_weights must produce the same run as the version that carried its own
// rebalancer.
func TestRebalanceOnPrimitivesMatchesTheHandRolledVersion(t *testing.T) {
	for _, params := range []map[string]any{
		{"rebalance_days": int64(21)},
		{"rebalance_days": int64(63)},
		{"rebalance_days": int64(21), "weights": map[string]any{
			"AAA": 0.5, "BBB": 0.3, "CCC": 0.2}},
		// Relative weights that do not sum to 1, which the script normalises.
		{"rebalance_days": int64(21), "weights": map[string]any{
			"AAA": 3.0, "BBB": 1.0}},
	} {
		legacy := runScriptSource(t, legacyRebalance, params)
		shipped, logged := runStrategyScript(
			t, filepath.Join("strategies", "rebalance.lua"), params)
		if logged != "" {
			t.Fatalf("rewritten rebalance.lua logged: %s", logged)
		}
		assertSameRun(t, "rebalance", legacy, shipped)
	}
}

// The same for momentum_rotation.lua, which exercises rank() as well as
// target_weights().
func TestMomentumRotationOnPrimitivesMatchesTheHandRolledVersion(t *testing.T) {
	for _, params := range []map[string]any{
		{"lookback": int64(60), "top_n": int64(2), "rebalance_days": int64(21)},
		{"lookback": int64(120), "top_n": int64(1), "rebalance_days": int64(42)},
		{"lookback": int64(60), "skip_days": int64(21), "top_n": int64(2),
			"rebalance_days": int64(21)},
		// top_n larger than the universe: every qualifying name is held.
		{"lookback": int64(60), "top_n": int64(9), "rebalance_days": int64(21)},
	} {
		legacy := runScriptSource(t, legacyMomentum, params)
		shipped, logged := runStrategyScript(
			t, filepath.Join("strategies", "momentum_rotation.lua"), params)
		if logged != "" {
			t.Fatalf("rewritten momentum_rotation.lua logged: %s", logged)
		}
		assertSameRun(t, "momentum_rotation", legacy, shipped)
	}
}

// --- the primitives themselves ---------------------------------------------

// evalOnHostData runs a script that stores `expr` into two globals on day 4 and
// reads them back before the state is closed. hostHist gives AAA closes
// 100..104 and BBB 110..114, High = close+1 and Low = close-1, so every
// expectation below is derived from those numbers rather than captured.
func evalOnHostData(t *testing.T, expr string) (float64, bool) {
	t.Helper()

	src := `
value, value_ok = 0, false
function step(day)
  if day ~= 4 then return end
  value, value_ok = ` + expr + `
end
`
	path := filepath.Join(t.TempDir(), "eval.lua")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatalf("write script: %v", err)
	}

	tickers := []string{"AAA", "BBB"}
	hist := hostHist(tickers)
	p, err := InitializePortfolio(
		10000, hist["AAA"][0].Date, hist["AAA"][hostDays-1].Date,
		"eval", tickers, "lua:"+path, nil,
	)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	strat := p.Strategy.(*LuaStrategy)
	defer strat.Close()

	for day := 0; day < hostDays; day++ {
		strat.Step(p, hist, day)
	}

	// Read the globals while the state is still open — Close() is deferred.
	value, okNum := strat.L.GetGlobal("value").(lua.LNumber)
	if !okNum {
		t.Fatalf("%s: script left a non-number in `value`", expr)
	}
	ok, okBool := strat.L.GetGlobal("value_ok").(lua.LBool)
	if !okBool {
		t.Fatalf("%s: script left a non-boolean in `value_ok`", expr)
	}
	return float64(value), bool(ok)
}

// hostReturns is AAA's daily return series over the fixture, so an expectation
// can be stated as a formula rather than as a constant someone has to trust.
func hostReturns(ticker string, day, n int) []float64 {
	series := hostHist([]string{"AAA", "BBB"})[ticker]
	var out []float64
	for i := day - n + 1; i <= day; i++ {
		out = append(out, series[i].Close/series[i-1].Close-1)
	}
	return out
}

func TestSignalPrimitiveArithmetic(t *testing.T) {
	wantStdev, _ := sampleStdev(hostReturns("AAA", 4, 4))

	cases := []struct {
		name string
		expr string
		want float64
		ok   bool
	}{
		// AAA: close[4]=104, close[0]=100 -> 4%.
		{"roc over the whole window", `roc("AAA", 4, 4)`, 104.0/100.0 - 1, true},
		{"roc over two days", `roc("AAA", 4, 2)`, 104.0/102.0 - 1, true},
		// A window longer than the history is undefined, not zero.
		{"roc past the start", `roc("AAA", 4, 9)`, 0, false},
		{"stdev of four returns", `stdev("AAA", 4, 4)`, wantStdev, true},
		{"stdev needs two returns", `stdev("AAA", 4, 1)`, 0, false},
		// Closes 100..104: mean 102, sample stdev sqrt(2.5).
		{"zscore at the top of the window", `zscore("AAA", 4, 5)`,
			2.0 / math.Sqrt(2.5), true},
		// True range each day is max(H-L, |H-prevC|, |L-prevC|) = max(2, 2, 0).
		{"atr is the two-point range", `atr("AAA", 4, 4)`, 2, true},
		// High = close+1, Low = close-1, so over five days: 105 and 99.
		{"high_n", `high_n("AAA", 4, 5)`, 105, true},
		{"low_n", `low_n("AAA", 4, 5)`, 99, true},
		{"high_n over one day", `high_n("AAA", 4, 1)`, 105, true},
		// An unknown ticker has no bars at all: undefined, not zero.
		{"unknown ticker", `roc("ZZZ", 4, 2)`, 0, false},
		{"zero window", `roc("AAA", 4, 0)`, 0, false},
		{"negative window", `stdev("AAA", 4, -3)`, 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := evalOnHostData(t, tc.expr)
			if ok != tc.ok {
				t.Fatalf("%s: ok = %v, want %v (value %v)", tc.expr, ok, tc.ok, got)
			}
			if !ok {
				return
			}
			if math.Abs(got-tc.want) > 1e-9*math.Max(1, math.Abs(tc.want)) {
				t.Errorf("%s = %.12g, want %.12g", tc.expr, got, tc.want)
			}
		})
	}
}

// Both fixture series rise monotonically with shrinking percentage steps, so
// their return series move together: the correlation must be strongly positive.
// A sign error here would quietly invert every pairs strategy built on it.
func TestCorrIsPositiveForSeriesThatMoveTogether(t *testing.T) {
	got, ok := evalOnHostData(t, `corr("AAA", "BBB", 4, 4)`)
	if !ok {
		t.Fatal("corr over four returns is undefined")
	}
	if got < 0.99 {
		t.Errorf("corr = %v for two series rising in lockstep, want ~1", got)
	}

	self, ok := evalOnHostData(t, `corr("AAA", "AAA", 4, 4)`)
	if !ok || math.Abs(self-1.0) > 1e-12 {
		t.Errorf("corr(x, x) = %v (ok=%v), want exactly 1", self, ok)
	}
}

// --- rank and the book ------------------------------------------------------

// runBookScript drives a script over the fixture and hands back the portfolio,
// for the primitives whose effect is a trade rather than a number.
func runBookScript(t *testing.T, body string) *Portfolio {
	t.Helper()
	p, logged := runHostScript(t, "function step(day)\n"+body+"\nend\n", nil)
	if logged != "" {
		t.Fatalf("script logged: %s", logged)
	}
	return p
}

// Weights that sum above 1 must fill as far as the cash allows rather than
// having the last order rejected outright. Without the clamp, Portfolio.Buy sees
// a notional larger than the buying power and drops the whole order — so the
// clamp is the difference between a 36-share partial fill and holding nothing.
//
// hostHist: AAA opens at 100, BBB at 110, cash 10,000.
func TestTargetWeightsFillsAsFarAsCashAllows(t *testing.T) {
	p := runBookScript(t, `if day == 0 then
	    target_weights(day, { AAA = 0.6, BBB = 0.6 })
	end`)

	aaa, ok := p.FindPosition("AAA")
	if !ok || aaa == nil {
		t.Fatal("AAA not held")
	}
	// 10,000 * 0.6 / 100 = 60 shares, affordable in full.
	if math.Abs(aaa.Amount-60) > 1e-9 {
		t.Errorf("AAA = %v shares, want 60", aaa.Amount)
	}

	bbb, ok := p.FindPosition("BBB")
	if !ok || bbb == nil {
		t.Fatal("BBB not held — the buy was rejected instead of clamped to " +
			"the remaining cash")
	}
	// 6,000 wanted, 4,000 left: 4000/110 = 36.3636… shares.
	if want := 4000.0 / 110.0; math.Abs(bbb.Amount-want) > 1e-9 {
		t.Errorf("BBB = %v shares, want %v (the cash-clamped amount)",
			bbb.Amount, want)
	}
	if p.BuyingPower > 1e-9 {
		t.Errorf("cash left = %v, want ~0", p.BuyingPower)
	}
}

// Rebalancing to the book you already hold must place no orders at all. Without
// an epsilon, floating-point dust would generate a trade every rebalance —
// turnover, and with costs switched on, a slow leak.
func TestTargetWeightsDoesNotChurnWhenAlreadyAtTarget(t *testing.T) {
	once := runBookScript(t, `if day == 0 then
	    target_weights(day, { AAA = 0.5, BBB = 0.5 })
	end`)
	twice := runBookScript(t, `if day == 0 then
	    target_weights(day, { AAA = 0.5, BBB = 0.5 })
	    target_weights(day, { AAA = 0.5, BBB = 0.5 })
	end`)

	assertSamePositions(t, "rebalancing twice", once, twice)
	if math.Abs(once.BuyingPower-twice.BuyingPower) > 1e-9 {
		t.Errorf("cash %v vs %v after a redundant rebalance",
			once.BuyingPower, twice.BuyingPower)
	}
}

// An empty target table means every weight is zero, which must liquidate rather
// than do nothing — that is how a strategy goes to cash.
func TestTargetWeightsEmptyTableGoesToCash(t *testing.T) {
	p := runBookScript(t, `if day == 0 then
	    target_weights(day, { AAA = 0.5, BBB = 0.5 })
	elseif day == 2 then
	    target_weights(day, {})
	end`)

	for _, ticker := range []string{"AAA", "BBB"} {
		if pos, ok := p.FindPosition(ticker); ok && pos != nil && pos.Amount > 1e-9 {
			t.Errorf("%s still holds %v shares after a target of {}",
				ticker, pos.Amount)
		}
	}
	if p.BuyingPower < 9000 {
		t.Errorf("cash = %v after liquidating a 10,000 book, want most of it "+
			"back", p.BuyingPower)
	}
}

// assertSamePositions is the position half of assertSameRun, for comparisons
// where the daily series is not the subject.
func assertSamePositions(t *testing.T, label string, want, got *Portfolio) {
	t.Helper()
	for _, ticker := range want.Tickers {
		wp, wok := want.FindPosition(ticker)
		gp, gok := got.FindPosition(ticker)
		if wok != gok {
			t.Errorf("%s: %s held=%v vs %v", label, ticker, wok, gok)
			continue
		}
		if !wok {
			continue
		}
		if math.Abs(wp.Amount-gp.Amount) > 1e-9 {
			t.Errorf("%s: %s %v vs %v shares", label, ticker,
				wp.Amount, gp.Amount)
		}
	}
}

// rank must order strongest first and leave out the tickers it could not score.
// Ordering backwards turns "hold the winners" into "hold the losers", and
// treating an unscored ticker as zero silently ranks it above everything
// negative — both are sign-level errors that produce a plausible-looking result.
func TestRankOrdersDescendingAndSkipsUnscored(t *testing.T) {
	// BBB scores 9, AAA scores 1, so descending order puts BBB first.
	count, first := evalOnHostData(t, `(function()
	    local r = rank(4, function(t) if t == "AAA" then return 1 end return 9 end)
	    return #r, r[1] ~= nil and r[1].ticker == "BBB"
	end)()`)
	if count != 2 {
		t.Errorf("ranked %v tickers, want 2", count)
	}
	if !first {
		t.Error("rank did not put the highest-scoring ticker first")
	}

	// BBB returns nil: unscored, so it must be absent rather than ranked last.
	count, isAAA := evalOnHostData(t, `(function()
	    local r = rank(4, function(t) if t == "AAA" then return 5 end return nil end)
	    return #r, r[1] ~= nil and r[1].ticker == "AAA"
	end)()`)
	if count != 1 {
		t.Errorf("ranked %v tickers, want 1 — an unscored ticker was included",
			count)
	}
	if !isAAA {
		t.Error("the only scored ticker is not first")
	}

	// A ranking function returning a non-number is the same as no score.
	count, _ = evalOnHostData(t, `(function()
	    local r = rank(4, function(t) return "not a number" end)
	    return #r, true
	end)()`)
	if count != 0 {
		t.Errorf("ranked %v tickers on a non-numeric score, want 0", count)
	}
}

// equity and weight_of are the read-only halves of the book API, and both are
// what a script uses to decide whether it has drifted.
func TestEquityAndWeightOf(t *testing.T) {
	// All cash on day 0: equity is the starting balance and every weight is 0.
	eq, _ := evalOnHostData(t, `equity(0), true`)
	if math.Abs(eq-10000) > 1e-9 {
		t.Errorf("equity with no positions = %v, want 10000", eq)
	}
	w, ok := evalOnHostData(t, `weight_of("AAA", 0)`)
	if !ok || w != 0 {
		t.Errorf("weight of an unheld ticker = %v (ok=%v), want 0", w, ok)
	}

	// After putting half the book into AAA, its weight is a half and equity is
	// unchanged (the trade moved cash into stock at the same price).
	p := runBookScript(t, `if day == 0 then
	    target_weights(day, { AAA = 0.5 })
	end`)
	pos, ok := p.FindPosition("AAA")
	if !ok || pos == nil {
		t.Fatal("AAA not held")
	}
	if math.Abs(pos.Amount-50) > 1e-9 {
		t.Errorf("AAA = %v shares, want 50 (5,000 at 100)", pos.Amount)
	}
	if math.Abs(p.BuyingPower-5000) > 1e-9 {
		t.Errorf("cash = %v, want 5000", p.BuyingPower)
	}
}
