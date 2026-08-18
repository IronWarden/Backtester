package backtest

import (
	"strings"
	"testing"
)

// NewStrategy passes Params to the "lua" case and nothing else, so a
// [portfolio.Params] block on a built-in spec is discarded in silence. The Lua
// host's per-key check cannot see this case: there is no interpreter to ask.
func TestParamsOnABuiltinAreReported(t *testing.T) {
	collected := captureDiagnostics(t)
	quietLog(t)

	pc := &PortfolioConfig{
		Name: "builtin", BuyingPower: 10000,
		StartTime: "2020-01-01", EndTime: "2020-12-31",
		Tickers: []string{"AAA"}, Strategy: "buyAndHold:equalWeights",
		Params: map[string]any{"lookback": 20},
	}
	if _, err := pc.ToPortfolios(); err != nil {
		t.Fatalf("ToPortfolios: %v", err)
	}

	got := collected()
	if len(got) != 1 {
		t.Fatalf("got %d diagnostics, want 1: %+v", len(got), got)
	}
	if got[0].Level != "warn" {
		t.Errorf("level = %q, want warn", got[0].Level)
	}
	if !strings.Contains(got[0].Message, `"lookback"`) {
		t.Errorf("message does not name the ignored key: %s", got[0].Message)
	}
}

// A Lua strategy must not be reported here — it has its own per-key check, and
// two warnings for one problem is how a warning gets ignored.
func TestParamsOnALuaStrategyAreLeftToTheHost(t *testing.T) {
	collected := captureDiagnostics(t)
	quietLog(t)

	pc := &PortfolioConfig{
		Name: "scripted", BuyingPower: 10000,
		StartTime: "2020-01-01", EndTime: "2020-12-31",
		Tickers:  []string{"AAA"},
		Strategy: "lua:" + hostScript(t, `function step(day) end`),
		Params:   map[string]any{"lookback": 20},
	}
	if _, err := pc.ToPortfolios(); err != nil {
		t.Fatalf("ToPortfolios: %v", err)
	}

	if got := collected(); len(got) != 0 {
		t.Errorf("a Lua strategy was warned about at config time: %+v", got)
	}
}

// The severe case: sweeping a built-in expands to N identical runs while
// Trials is N, so the deflated Sharpe discounts a search that never happened.
// The message has to say that, since N identical rows do not look like a
// config mistake.
func TestSweepingABuiltinSaysTheTrialsAreIdentical(t *testing.T) {
	collected := captureDiagnostics(t)
	quietLog(t)

	pc := &PortfolioConfig{
		Name: "swept-builtin", BuyingPower: 10000,
		StartTime: "2020-01-01", EndTime: "2020-12-31",
		Tickers: []string{"AAA"}, Strategy: "buyAndHold:equalWeights",
		Sweep: map[string]any{"lookback": []any{int64(10), int64(20), int64(30)}},
	}
	if _, err := pc.ToPortfolios(); err != nil {
		t.Fatalf("ToPortfolios: %v", err)
	}

	got := collected()
	if len(got) != 1 {
		t.Fatalf("got %d diagnostics, want exactly 1 for the block: %+v",
			len(got), got)
	}
	for _, want := range []string{"3 trials", "identical", "deflated Sharpe"} {
		if !strings.Contains(got[0].Message, want) {
			t.Errorf("message omits %q: %s", want, got[0].Message)
		}
	}
}

// smaCross is the built-in people most want to sweep, and its periods live in
// the spec string, so a Sweep block on it looks exactly like it should work.
func TestSmaCrossPointsAtTheSpecString(t *testing.T) {
	collected := captureDiagnostics(t)
	quietLog(t)

	pc := &PortfolioConfig{
		Name: "sma", BuyingPower: 10000,
		StartTime: "2020-01-01", EndTime: "2020-12-31",
		Tickers: []string{"AAA"}, Strategy: "smaCross:5:20:equalWeights",
		Params: map[string]any{"short": 5},
	}
	if _, err := pc.ToPortfolios(); err != nil {
		t.Fatalf("ToPortfolios: %v", err)
	}

	got := collected()
	if len(got) != 1 {
		t.Fatalf("got %d diagnostics, want 1: %+v", len(got), got)
	}
	if !strings.Contains(got[0].Message, "smaCross:<short>:<long>:<buyType>") {
		t.Errorf("message does not point at the spec string: %s",
			got[0].Message)
	}
}

// A built-in with no Params is the common case and must stay silent.
func TestBuiltinWithoutParamsIsSilent(t *testing.T) {
	collected := captureDiagnostics(t)
	quietLog(t)

	pc := &PortfolioConfig{
		Name: "plain", BuyingPower: 10000,
		StartTime: "2020-01-01", EndTime: "2020-12-31",
		Tickers: []string{"AAA"}, Strategy: "buyAndHold:equalWeights",
	}
	if _, err := pc.ToPortfolios(); err != nil {
		t.Fatalf("ToPortfolios: %v", err)
	}

	if got := collected(); len(got) != 0 {
		t.Errorf("a plain built-in produced diagnostics: %+v", got)
	}
}
