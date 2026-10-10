package backtest

import (
	"strings"
	"testing"
)

// checkText runs CheckConfig over a TOML string and reports the errors it
// counted alongside everything it said.
func checkText(t *testing.T, toml string) (int, []Diagnostic) {
	t.Helper()
	collected := captureDiagnostics(t)
	quietLog(t)

	cfg, err := LoadConfigText(toml)
	if err != nil {
		t.Fatalf("LoadConfigText: %v", err)
	}
	return CheckConfig(cfg, ""), collected()
}

func TestCheckPassesACleanConfig(t *testing.T) {
	errs, diags := checkText(t, `
[[portfolio]]
Name        = "clean"
BuyingPower = 10000.0
StartDate   = "2015-01-01"
EndDate     = "2020-01-01"
Tickers     = ["AAA", "BBB"]
Strategy    = "buyAndHold:equalWeights"
`)
	if errs != 0 {
		t.Errorf("a clean config reported %d errors: %+v", errs, diags)
	}
	if len(diags) != 0 {
		t.Errorf("a clean config produced diagnostics: %+v", diags)
	}
}

// Every construction failure this session added has to surface here, or the
// check is not actually the same validation a run performs.
func TestCheckCatchesConstructionFailures(t *testing.T) {
	cases := []struct {
		name, toml, want string
	}{
		{"reversed window", `
[[portfolio]]
Name = "p"
BuyingPower = 10000.0
StartDate = "2020-01-01"
EndDate = "2019-01-01"
Tickers = ["AAA"]
Strategy = "buyAndHold:equalWeights"
`, "before StartDate"},
		{"duplicate ticker", `
[[portfolio]]
Name = "p"
BuyingPower = 10000.0
StartDate = "2015-01-01"
EndDate = "2020-01-01"
Tickers = ["AAA", "AAA"]
Strategy = "buyAndHold:equalWeights"
`, "more than once"},
		{"negative capital", `
[[portfolio]]
Name = "p"
BuyingPower = -100.0
StartDate = "2015-01-01"
EndDate = "2020-01-01"
Tickers = ["AAA"]
Strategy = "buyAndHold:equalWeights"
`, "negative"},
		{"unknown strategy", `
[[portfolio]]
Name = "p"
BuyingPower = 10000.0
StartDate = "2015-01-01"
EndDate = "2020-01-01"
Tickers = ["AAA"]
Strategy = "nonsense"
`, "unknown strategy spec"},
	}
	for _, tc := range cases {
		errs, diags := checkText(t, tc.toml)
		if errs != 1 {
			t.Errorf("%s: got %d errors, want 1: %+v", tc.name, errs, diags)
			continue
		}
		if !strings.Contains(diags[0].Message, tc.want) {
			t.Errorf("%s: message %q does not mention %q",
				tc.name, diags[0].Message, tc.want)
		}
	}
}

// A warning is not a failure: the config runs, it just may not do what was
// meant. The exit code depends on this distinction.
func TestCheckSeparatesWarningsFromErrors(t *testing.T) {
	errs, diags := checkText(t, `
[[portfolio]]
Name = "swept builtin"
BuyingPower = 10000.0
StartDate = "2015-01-01"
EndDate = "2020-01-01"
Tickers = ["AAA"]
Strategy = "smaCross:5:20:equalWeights"
  [portfolio.Sweep]
  short = [5, 10, 20]
`)
	if errs != 0 {
		t.Errorf("a warning was counted as an error: %d, %+v", errs, diags)
	}
	if len(diags) != 1 || diags[0].Level != "warn" {
		t.Fatalf("want one warning, got %+v", diags)
	}
	if !strings.Contains(diags[0].Message, "identical") {
		t.Errorf("the sweep consequence is missing: %s", diags[0].Message)
	}
}

// An unusable [Output] block is worth catching here more than anywhere: the
// file is opened after the simulation, so a typo in `fields` is otherwise
// discovered at the very end of a run that is now wasted.
func TestCheckValidatesTheOutputBlock(t *testing.T) {
	errs, diags := checkText(t, `
[[portfolio]]
Name = "p"
BuyingPower = 10000.0
StartDate = "2015-01-01"
EndDate = "2020-01-01"
Tickers = ["AAA"]
Strategy = "buyAndHold:equalWeights"

[Output]
path = "out.csv"
fields = ["PortfolioName", "Sharp"]
`)
	if errs != 1 {
		t.Fatalf("got %d errors, want 1: %+v", errs, diags)
	}
	if !strings.Contains(diags[0].Message, "SharpeRatio") {
		t.Errorf("the error does not suggest the field they meant: %s",
			diags[0].Message)
	}
}

// A walk-forward block never reaches ToPortfolios, so checking only that would
// skip every walk-forward config in the file — the same fork that made T73 a
// bug in the first place.
func TestCheckFollowsTheWalkForwardFork(t *testing.T) {
	errs, diags := checkText(t, `
[[portfolio]]
Name = "wf"
BuyingPower = 10000.0
StartDate = "2015-01-01"
EndDate = "2020-01-01"
Tickers = ["AAA"]
Strategy = "buyAndHold:equalWeights"
  [portfolio.WalkForward]
  train_days = 252
  test_days = 63
`)
	// No Sweep, so there is nothing to select between.
	if errs != 1 {
		t.Fatalf("got %d errors, want 1: %+v", errs, diags)
	}
	if !strings.Contains(diags[0].Message, "at least two") {
		t.Errorf("unexpected message: %s", diags[0].Message)
	}
}

// Two portfolios sharing a name are not rejected, but every result and log
// line is keyed by it, so the pair cannot be told apart afterwards.
func TestCheckWarnsOnDuplicatePortfolioNames(t *testing.T) {
	errs, diags := checkText(t, `
[[portfolio]]
Name = "same"
BuyingPower = 10000.0
StartDate = "2015-01-01"
EndDate = "2020-01-01"
Tickers = ["AAA"]
Strategy = "buyAndHold:equalWeights"

[[portfolio]]
Name = "same"
BuyingPower = 10000.0
StartDate = "2015-01-01"
EndDate = "2020-01-01"
Tickers = ["BBB"]
Strategy = "buyAndHold:equalWeights"
`)
	if errs != 0 {
		t.Errorf("a duplicate name was treated as fatal: %+v", diags)
	}
	if len(diags) != 1 || !strings.Contains(diags[0].Message, "more than once") {
		t.Errorf("want one duplicate-name warning, got %+v", diags)
	}
}

func TestCheckSummaryWording(t *testing.T) {
	cases := []struct {
		errs, warns int
		want        string
	}{
		{0, 0, "config check passed"},
		{0, 2, "passed with 2 warning(s)"},
		{1, 0, "FAILED: 1 error(s)"},
	}
	for _, tc := range cases {
		if got := CheckSummary(tc.errs, tc.warns); !strings.Contains(got, tc.want) {
			t.Errorf("CheckSummary(%d, %d) = %q, want it to contain %q",
				tc.errs, tc.warns, got, tc.want)
		}
	}
}

// -check is only worth having if it validates exactly the config a run would
// execute. The strategy-defaulting rule used to be written out separately in
// RunFromConfigText and CheckConfig; two copies can drift, and the failure
// mode is a check that passes a config the run then rejects. This pins the
// rule itself, which both now call.
func TestResolveStrategyIsOneRule(t *testing.T) {
	cases := []struct {
		name, strategy, deflt, want string
		wantErr                     bool
	}{
		{"explicit strategy wins", "buyAndHold:greedy", "d.lua",
			"buyAndHold:greedy", false},
		{"omitted takes the default", "", "d.lua", "lua:d.lua", false},
		{"whitespace counts as omitted", "   ", "d.lua", "lua:d.lua", false},
		{"omitted with no default is an error", "", "", "", true},
	}
	for _, tc := range cases {
		pc := &PortfolioConfig{Name: "p", Strategy: tc.strategy}
		err := pc.ResolveStrategy(tc.deflt)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s: want an error, got none", tc.name)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if pc.Strategy != tc.want {
			t.Errorf("%s: Strategy = %q, want %q",
				tc.name, pc.Strategy, tc.want)
		}
	}
}

// The observable half of the same property: a config the check accepts only
// because a default was supplied must still be accepted when that default is
// absent — as an error, from both paths, with the same wording.
func TestCheckHonoursTheDefaultLuaPath(t *testing.T) {
	const cfg = `
[[portfolio]]
Name = "no strategy"
BuyingPower = 10000.0
StartDate = "2015-01-01"
EndDate = "2020-01-01"
Tickers = ["AAA"]
`
	collected := captureDiagnostics(t)
	quietLog(t)
	parsed, err := LoadConfigText(cfg)
	if err != nil {
		t.Fatalf("LoadConfigText: %v", err)
	}
	if errs := CheckConfig(parsed, ""); errs != 1 {
		t.Fatalf("no default: got %d errors, want 1: %+v", errs, collected())
	}
	if !strings.Contains(collected()[0].Message, "Strategy is required") {
		t.Errorf("unexpected message: %s", collected()[0].Message)
	}
}
