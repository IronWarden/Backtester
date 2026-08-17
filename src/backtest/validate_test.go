package backtest

// A validator that never fails anything is worse than none: it launders bad
// candidates. So the tests are mostly deliberately broken strategies, each
// breaking one rule, and each must be caught by the check that exists for it.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// luaSpec writes a script to a temp file and returns a spec pointing at it.
func luaSpec(t *testing.T, src string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "candidate.lua")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return "lua:" + path
}

func findingFor(t *testing.T, report ValidationReport, check string) Finding {
	t.Helper()
	for _, f := range report.Findings {
		if f.Check == check {
			return f
		}
	}
	t.Fatalf("no finding for %q in %+v", check, report.Findings)
	return Finding{}
}

// THE test. A strategy that reads tomorrow's close and buys when it is higher
// must be caught — this is the failure that produces a beautiful equity curve
// and means nothing, and no amount of reading the source proves its absence.
func TestLookAheadIsDetected(t *testing.T) {
	cheat := luaSpec(t, `
function step(day)
    local t = tickers[1]
    local today    = price(t, day)
    local tomorrow = close_at(t, day + 1)   -- reading the future
    if tomorrow > today and not position(t) then
        buy_max(t, today, "equalWeights", day)
    elseif tomorrow < today and position(t) then
        sell_all(t, today, day)
    end
end
`)
	report := ValidateStrategy(cheat, nil)
	f := findingFor(t, report, "look-ahead")
	if !f.Failed {
		t.Errorf("a strategy reading tomorrow's close passed the look-ahead "+
			"check: %s", f.Detail)
	}
	if !f.Fatal {
		t.Error("look-ahead should be fatal")
	}
	if report.Passed {
		t.Error("the report passed a strategy that reads the future")
	}
}

// The mirror image, and just as important: an honest strategy must NOT be
// flagged. A detector with false positives is one people learn to ignore.
func TestHonestStrategiesPassLookAhead(t *testing.T) {
	for _, spec := range []struct {
		name   string
		spec   string
		params map[string]any
	}{
		{"buy and hold", "buyAndHold:equalWeights", nil},
		{"sma cross", "lua:strategies/sma_cross.lua",
			map[string]any{"short": int64(10), "long": int64(30)}},
		{"momentum rotation", "lua:strategies/momentum_rotation.lua",
			map[string]any{"lookback": int64(60), "rebalance_days": int64(21)}},
	} {
		t.Run(spec.name, func(t *testing.T) {
			report := ValidateStrategy(spec.spec, spec.params)
			if f := findingFor(t, report, "look-ahead"); f.Failed {
				t.Errorf("false positive on an honest strategy: %s", f.Detail)
			}
			if !report.Passed {
				t.Errorf("shipped strategy failed validation: %s", report.String())
			}
		})
	}
}

// Reading FURTHER into the future must also be caught — a detector that only
// notices day+1 would miss the common case of peeking at a whole future window.
func TestLookAheadFurtherAheadIsDetected(t *testing.T) {
	cheat := luaSpec(t, `
function step(day)
    local t = tickers[1]
    local later = close_at(t, day + 20)
    if later > price(t, day) * 1.02 and not position(t) then
        buy_max(t, price(t, day), "equalWeights", day)
    end
end
`)
	if f := findingFor(t, ValidateStrategy(cheat, nil), "look-ahead"); !f.Failed {
		t.Errorf("a strategy reading 20 days ahead passed: %s", f.Detail)
	}
}

// A strategy that never trades is not a strategy, and it otherwise reports a
// 0% return like any other result.
func TestNeverTradingIsCaught(t *testing.T) {
	idle := luaSpec(t, "function step(day)\nend\n")
	f := findingFor(t, ValidateStrategy(idle, nil), "placed trades")
	if !f.Failed {
		t.Errorf("a strategy that never trades passed: %s", f.Detail)
	}
	if !strings.Contains(f.Detail, "no trades") {
		t.Errorf("finding does not say what happened: %q", f.Detail)
	}
}

// Buying a ticker outside the portfolio's list makes the cash vanish: the
// position is never valued, so the equity curve simply loses it. That is T9, and
// a typo'd symbol is enough to cause it.
func TestCashLostToAnUnlistedTickerIsCaught(t *testing.T) {
	typo := luaSpec(t, `
function step(day)
    if day == 1 then
        buy("NOTINLIST", 10, price(tickers[1], day), day)
    end
end
`)
	report := ValidateStrategy(typo, nil)
	f := findingFor(t, report, "cash conserved")
	if !f.Failed {
		t.Errorf("cash lost to an unlisted ticker passed: %s", f.Detail)
	}
	if !strings.Contains(f.Detail, "NOTINLIST") {
		t.Errorf("the finding does not name the offending ticker: %q", f.Detail)
	}
	if report.Passed {
		t.Error("a strategy that loses cash passed validation")
	}
}

// Degenerate paths must not panic or produce NaN. A strategy dividing by a
// zero-volatility window is the classic way to get one.
func TestDegenerateInputsAreSurvived(t *testing.T) {
	divider := luaSpec(t, `
function step(day)
    local t = tickers[1]
    local sd = stdev(t, day, 20)          -- exactly 0 on a flat series
    local score = (price(t, day) - sma(t, day, 20)) / sd
    if score > 1 then buy_max(t, price(t, day), "equalWeights", day) end
end
`)
	f := findingFor(t, ValidateStrategy(divider, nil), "degenerate inputs")
	// Dividing by zero in Lua gives inf, not a panic, and the strategy then
	// simply never trades — so the check should pass. What it must never do is
	// let a NaN reach the portfolio values.
	if f.Failed && !strings.Contains(f.Detail, "NaN") {
		t.Errorf("degenerate check failed for an unexpected reason: %s", f.Detail)
	}
}

// The report must distinguish a pass from a failure in its text, since that is
// what a user actually reads.
func TestReportTextStatesTheVerdict(t *testing.T) {
	good := ValidateStrategy("buyAndHold:equalWeights", nil).String()
	if !strings.Contains(good, "passed") {
		t.Errorf("a passing report does not say so: %q", good)
	}
	idle := ValidateStrategy(luaSpec(t, "function step(day)\nend\n"), nil).String()
	if !strings.Contains(idle, "FAILED") {
		t.Errorf("a failing report does not say so: %q", idle)
	}
}
