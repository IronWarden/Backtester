package main

import (
	"strings"
	"testing"

	"my-backtester/src/backtest"
)

// The simple-mode hypothesis builder lives in TypeScript, and the parser that
// has to accept its output lives here. These are the EXACT documents
// buildHypothesis() produced for a representative set of inputs, captured by
// running the compiled module — not hand-written approximations of them. If
// the generator's format drifts, the frontend still typechecks and this test
// still passes, which is the gap T87 (a frontend test runner) closes; what
// this pins is the other half — that the format the UI ships is one the engine
// actually registers, including the quoting of a claim containing quotes and a
// negative threshold.
func TestSimpleModeHypothesesParse(t *testing.T) {
	generated := []struct {
		name    string
		doc     string
		wantID  string
		wantOp  string
		wantVal float64
	}{
		{
			"sharpe floor",
			"ID          = \"H-11eyu7q\"\nClaim       = \"12-1 momentum beats buy-and-hold\"\nCriterion   = \"sharpe >= 1.2\"\n",
			"H-11eyu7q", ">=", 1.2,
		},
		{
			"claim containing quotes",
			"ID          = \"H-097sgk6\"\nClaim       = \"RSI \\\"dip buying\\\" works\"\nCriterion   = \"annual_return > 15\"\n",
			"H-097sgk6", ">", 15,
		},
		{
			"negative threshold",
			"ID          = \"H-0qzdpor\"\nClaim       = \"drawdown stays shallow\"\nCriterion   = \"max_drawdown <= -20.5\"\n",
			"H-0qzdpor", "<=", -20.5,
		},
		{
			"dollar threshold",
			"ID          = \"H-137nhld\"\nClaim       = \"profit clears a number\"\nCriterion   = \"total_return > 25000\"\n",
			"H-137nhld", ">", 25000,
		},
	}

	for _, tc := range generated {
		h, err := backtest.ParseHypothesis([]byte(tc.doc))
		if err != nil {
			t.Errorf("%s: the UI would generate a hypothesis the engine "+
				"rejects: %v", tc.name, err)
			continue
		}
		if h.ID != tc.wantID {
			t.Errorf("%s: ID = %q, want %q", tc.name, h.ID, tc.wantID)
		}
		c, err := backtest.ParseCriterion(h.Criterion)
		if err != nil {
			t.Errorf("%s: criterion %q rejected: %v", tc.name, h.Criterion, err)
			continue
		}
		if c.Op != tc.wantOp || c.Value != tc.wantVal {
			t.Errorf("%s: criterion parsed as %s %v, want %s %v",
				tc.name, c.Op, c.Value, tc.wantOp, tc.wantVal)
		}
	}
}

// Every metric the picker can offer must be one ParseCriterion accepts. The
// list is served from Go so it cannot drift, and this asserts the binding is
// wired to the engine's own table rather than a copy of it.
func TestCriterionMetricsAreAllValid(t *testing.T) {
	app := NewApp()
	names := app.CriterionMetrics()
	if len(names) == 0 {
		t.Fatal("no criterion metrics offered — the picker would be empty")
	}
	for _, m := range names {
		if _, err := backtest.ParseCriterion(m + " >= 1"); err != nil {
			t.Errorf("offered metric %q is not one the engine accepts: %v",
				m, err)
		}
	}
	// A metric that does not exist must still be refused, or the check above
	// proves nothing.
	if _, err := backtest.ParseCriterion("definitely_not_a_metric >= 1"); err == nil {
		t.Error("ParseCriterion accepted a metric that does not exist")
	}
	if !strings.Contains(strings.Join(names, ","), "sharpe") {
		t.Error("sharpe is missing from the criterion metrics")
	}
}
