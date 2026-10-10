package backtest

import (
	"os"
	"strings"
	"testing"
)

// Scripts read settings as `params.lookback or 126`, so a misspelled key does
// not error — it falls through to the default and the run proceeds with
// settings the user did not choose.
func TestMisspelledParamIsReported(t *testing.T) {
	collected := captureDiagnostics(t)
	quietLog(t)

	runHostScript(t, `
local lookback = params.lookback or 126
function step(day) end
`, map[string]any{"lookbak": 252})

	got := collected()
	if len(got) != 1 {
		t.Fatalf("got %d diagnostics, want 1: %+v", len(got), got)
	}
	if got[0].Level != "warn" {
		t.Errorf("level = %q, want warn", got[0].Level)
	}
	if !strings.Contains(got[0].Message, `"lookbak"`) {
		t.Errorf("message does not name the unread key: %s", got[0].Message)
	}
}

// A key the script does read must stay silent, in every spelling Lua allows —
// a false positive here would train the user to ignore the warning.
func TestReadParamsAreNotReported(t *testing.T) {
	for _, access := range []string{
		"params.lookback",
		`params["lookback"]`,
		"params['lookback']",
	} {
		collected := captureDiagnostics(t)
		quietLog(t)

		runHostScript(t, `
local lookback = `+access+` or 126
function step(day) end
`, map[string]any{"lookback": 252})

		if got := collected(); len(got) != 0 {
			t.Errorf("%s was reported as unread: %+v", access, got)
		}
	}
}

// The expensive case. Sweep values are merged into Params, so a mistyped sweep
// key expands to N portfolios differing only in a value nothing reads: every
// trial produces identical numbers while Trials is N, and the deflated Sharpe
// discounts the result for a search that never happened. The message has to
// say that, because the symptom — a sweep where every row matches — is not
// obviously a spelling problem.
func TestMisspelledSweepKeySaysEveryTrialIsIdentical(t *testing.T) {
	collected := captureDiagnostics(t)
	quietLog(t)

	tickers := []string{"AAA", "BBB"}
	hist := hostHist(tickers)
	p, err := InitializePortfolio(10000,
		hist["AAA"][0].Date, hist["AAA"][hostDays-1].Date,
		"swept", tickers,
		"lua:"+hostScript(t, `
local lookback = params.lookback or 126
function step(day) end
`),
		map[string]any{"lookbak": 252},
	)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	p.Trials = 12
	strat := p.Strategy.(*LuaStrategy)
	defer strat.Close()
	strat.Step(p, hist, 0)

	got := collected()
	if len(got) == 0 {
		t.Fatal("no diagnostic for a misspelled swept key")
	}
	if !strings.Contains(got[0].Message, "12 trials") ||
		!strings.Contains(got[0].Message, "identical") {
		t.Errorf("message does not explain the sweep consequence: %s",
			got[0].Message)
	}
}

// A strategy with no Params must not pay for the check, and must not warn.
func TestNoParamsNoWarning(t *testing.T) {
	collected := captureDiagnostics(t)
	quietLog(t)

	runHostScript(t, `function step(day) end`, nil)

	if got := collected(); len(got) != 0 {
		t.Errorf("a paramless strategy produced diagnostics: %+v", got)
	}
}

// Every shipped strategy must be clean under this check, or the warning is
// noise from the first run a user does.
func TestShippedStrategiesReadEveryDocumentedParam(t *testing.T) {
	dir := strategiesDir(t)
	for script, params := range shippedStrategies() {
		if len(params) == 0 {
			continue
		}
		raw, err := os.ReadFile(dir + "/" + script)
		if err != nil {
			t.Fatalf("%s: %v", script, err)
		}
		src := string(raw)
		for key := range params {
			if !mentionsParam(src, key) {
				t.Errorf("%s: the test harness passes %q but the script "+
					"never reads it", script, key)
			}
		}
	}
}
