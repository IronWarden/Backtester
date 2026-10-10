package backtest

import (
	"bytes"
	"log"
	"math"
	"strings"
	"testing"
)

// runShortScript is runHostScript with shorting switched on, since the whole
// point of these primitives is what they do when the portfolio permits it.
func runShortScript(t *testing.T, body string, allowShort bool) (*Portfolio, string) {
	t.Helper()
	tickers := []string{"AAA", "BBB"}
	hist := hostHist(tickers)

	src := "function step(day)\n" + body + "\nend\n"
	p, err := InitializePortfolio(
		10000,
		hist["AAA"][0].Date, hist["AAA"][hostDays-1].Date,
		"short-test", tickers, "lua:"+hostScript(t, src), nil,
	)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	p.AllowShort = allowShort
	strat := p.Strategy.(*LuaStrategy)
	defer strat.Close()

	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	for day := 0; day < hostDays; day++ {
		strat.Step(p, hist, day)
	}
	return p, logBuf.String()
}

// short() is a TARGET, not a delta. That is the opposite of buy and sell, and
// it is the reason the function exists: "short 30" as a delta is ambiguous the
// moment a position already exists.
func TestShortIsATargetNotADelta(t *testing.T) {
	p, _ := runShortScript(t, `if day == 0 then
	    short("AAA", 30, price("AAA", day), day)
	elseif day == 1 then
	    short("AAA", 30, price("AAA", day), day)
	end`, true)

	pos, ok := p.FindPosition("AAA")
	if !ok || pos == nil {
		t.Fatal("AAA not held")
	}
	if math.Abs(pos.Amount-(-30)) > 1e-9 {
		t.Errorf("AAA = %v, want -30 — asking twice for the same target must "+
			"not double the position", pos.Amount)
	}
}

// The target is reached from wherever the book happens to be, including from a
// long, which is what saves a script from reading its own position first.
func TestShortReachesTheTargetFromALong(t *testing.T) {
	p, _ := runShortScript(t, `if day == 0 then
	    buy("AAA", 20, price("AAA", day), day)
	elseif day == 1 then
	    short("AAA", 10, price("AAA", day), day)
	end`, true)

	pos, ok := p.FindPosition("AAA")
	if !ok || pos == nil {
		t.Fatal("AAA not held")
	}
	if math.Abs(pos.Amount-(-10)) > 1e-9 {
		t.Errorf("AAA = %v, want -10", pos.Amount)
	}
}

// short(t, 0) is "go flat", which is the reason a target beats a delta: the
// zero case needs no special function.
func TestShortZeroGoesFlat(t *testing.T) {
	p, _ := runShortScript(t, `if day == 0 then
	    short("AAA", 25, price("AAA", day), day)
	elseif day == 2 then
	    short("AAA", 0, price("AAA", day), day)
	end`, true)

	if pos, ok := p.FindPosition("AAA"); ok && pos != nil && pos.Amount != 0 {
		t.Errorf("AAA = %v after short(0), want flat", pos.Amount)
	}
}

func TestCoverClosesTheShort(t *testing.T) {
	p, _ := runShortScript(t, `if day == 0 then
	    short("AAA", 25, price("AAA", day), day)
	elseif day == 2 then
	    cover("AAA", price("AAA", day), day)
	end`, true)

	if pos, ok := p.FindPosition("AAA"); ok && pos != nil && pos.Amount != 0 {
		t.Errorf("AAA = %v after cover(), want flat", pos.Amount)
	}
}

// cover() on a long must do nothing at all: it closes shorts, and silently
// selling a long would be a very expensive surprise.
func TestCoverLeavesALongAlone(t *testing.T) {
	p, _ := runShortScript(t, `if day == 0 then
	    buy("AAA", 20, price("AAA", day), day)
	elseif day == 2 then
	    cover("AAA", price("AAA", day), day)
	end`, true)

	pos, ok := p.FindPosition("AAA")
	if !ok || pos == nil {
		t.Fatal("cover() closed a long position")
	}
	if math.Abs(pos.Amount-20) > 1e-9 {
		t.Errorf("AAA = %v, want 20 untouched", pos.Amount)
	}
}

// The silent-failure guard. Without AllowShort the order cannot fill, and a
// strategy that is quietly not shorting looks exactly like one whose signal
// never fired.
func TestShortWithoutAllowShortSaysSo(t *testing.T) {
	p, logged := runShortScript(t, `if day == 0 then
	    short("AAA", 30, price("AAA", day), day)
	end`, false)

	if len(p.Positions) != 0 {
		t.Errorf("a short filled on a long-only portfolio: %+v", p.Positions)
	}
	if !strings.Contains(logged, "AllowShort") {
		t.Errorf("the refusal never mentioned AllowShort, so the user cannot "+
			"act on it: %q", logged)
	}
}

func TestNegativeShortSizeIsRefused(t *testing.T) {
	p, logged := runShortScript(t, `if day == 0 then
	    short("AAA", -30, price("AAA", day), day)
	end`, true)

	if len(p.Positions) != 0 {
		t.Errorf("a negative short size traded: %+v", p.Positions)
	}
	if !strings.Contains(logged, "magnitude") {
		t.Errorf("the refusal did not explain itself: %q", logged)
	}
}

// The payoff of the whole T27 series: market-neutral in one line, expressed
// through the existing rebalancer rather than a second API.
func TestTargetWeightsGoesMarketNeutral(t *testing.T) {
	p, _ := runShortScript(t, `if day == 0 then
	    target_weights(day, { AAA = 0.5, BBB = -0.5 })
	end`, true)

	aaa, ok := p.FindPosition("AAA")
	if !ok || aaa == nil || aaa.Amount <= 0 {
		t.Fatalf("AAA is not long: %+v", aaa)
	}
	bbb, ok := p.FindPosition("BBB")
	if !ok || bbb == nil || bbb.Amount >= 0 {
		t.Fatalf("BBB is not short: %+v", bbb)
	}
	// 10,000 equity: 5,000 long AAA at 100, 5,000 short BBB at 110.
	if math.Abs(aaa.Amount-50) > 1e-9 {
		t.Errorf("AAA = %v shares, want 50", aaa.Amount)
	}
	if want := -5000.0 / 110.0; math.Abs(bbb.Amount-want) > 1e-9 {
		t.Errorf("BBB = %v shares, want %v", bbb.Amount, want)
	}
}

// The other silent failure: a short leg on a long-only portfolio leaves the
// book at half the size the strategy asked for, and nothing said so.
func TestNegativeTargetWeightWithoutAllowShortSaysSo(t *testing.T) {
	p, logged := runShortScript(t, `if day == 0 then
	    target_weights(day, { AAA = 0.5, BBB = -0.5 })
	end`, false)

	if pos, ok := p.FindPosition("BBB"); ok && pos != nil && pos.Amount < 0 {
		t.Errorf("BBB went short on a long-only portfolio: %v", pos.Amount)
	}
	if !strings.Contains(logged, "AllowShort") {
		t.Errorf("the half-built book was never reported: %q", logged)
	}
}
