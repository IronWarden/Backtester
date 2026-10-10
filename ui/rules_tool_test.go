package main

import (
	"strings"
	"testing"
	"time"

	"my-backtester/src/backtest"
	"my-backtester/src/data"
)

func checkRulesInput(rules map[string]any) map[string]any {
	return map[string]any{
		"rules":      rules,
		"tickers":    []any{"AAA", "BBB", "CCC", "DDD"},
		"start_date": "2015-01-02",
		"end_date":   "2020-01-02",
		"benchmark":  "$SP500",
	}
}

// What the user reads must be the engine's sentence, character for
// character — not a near-copy the tool assembled on its own.
func TestCheckRulesReturnsTheEnginesSentence(t *testing.T) {
	in := checkRulesInput(map[string]any{
		"Rebalance": "monthly", "Rank": "return_126", "Pick": float64(2),
		"Filter": "above_sma_200", "RiskOff": "benchmark_below_sma_200",
	})
	out := runCheckRulesTool("", in)
	if strings.HasPrefix(out, "ERROR") {
		t.Fatalf("valid rules refused: %s", out)
	}
	want := backtest.DescribeRules(backtest.RulesConfig{
		Rebalance: "monthly", Rank: "return_126", Pick: 2,
		Filter: "above_sma_200", RiskOff: "benchmark_below_sma_200",
	}, []string{"AAA", "BBB", "CCC", "DDD"}, "$SP500")
	if !strings.Contains(out, want) {
		t.Errorf("tool output does not carry DescribeRules' sentence.\nwant: %s\ngot:  %s", want, out)
	}
	if !strings.Contains(out, "Coverage not checked") {
		t.Errorf("with no database the tool must say coverage went unchecked: %s", out)
	}
}

// Each refusal is the engine's, worded for a person; the tool adds nothing
// a run would not have said, and drops the internal portfolio name.
func TestCheckRulesRefusesWhatARunWould(t *testing.T) {
	cases := []struct {
		name  string
		rules map[string]any
		bench string
		want  string
	}{
		{"typo suggests", map[string]any{"Rebalance": "monthly", "Rank": "retrun_126", "Pick": float64(2)}, "$SP500", "return_126"},
		{"keep below pick", map[string]any{"Rebalance": "monthly", "Rank": "return_126", "Pick": float64(3), "Keep": float64(2)}, "$SP500", "Keep"},
		{"pick above tickers", map[string]any{"Rebalance": "monthly", "Rank": "return_126", "Pick": float64(9)}, "$SP500", "only 4"},
		{"riskoff needs benchmark", map[string]any{"Rebalance": "monthly", "RiskOff": "benchmark_below_sma_200"}, "", "Benchmark"},
		{"no rebalance", map[string]any{"Rank": "return_126", "Pick": float64(2)}, "$SP500", "Rebalance is required"},
		{"warm-up longer than window", map[string]any{"Rebalance": "monthly", "Filter": "above_sma_900"}, "$SP500", "trading days of history"},
		{"misspelt key", map[string]any{"Rebalance": "monthly", "Filtre": "above_sma_200"}, "$SP500", "Filtre"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := checkRulesInput(tc.rules)
			in["benchmark"] = tc.bench
			if tc.name == "warm-up longer than window" {
				in["end_date"] = "2016-01-04"
			}
			out := runCheckRulesTool("", in)
			if !strings.HasPrefix(out, "ERROR: ") {
				t.Fatalf("not refused: %s", out)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("refusal does not mention %q: %s", tc.want, out)
			}
			if strings.Contains(out, `portfolio "rules"`) {
				t.Errorf("refusal leaks the tool's internal portfolio name: %s", out)
			}
		})
	}
}

// Lowercase keys, as a model copying the JSON tags might send, decode too.
func TestCheckRulesAcceptsEitherKeyCase(t *testing.T) {
	out := runCheckRulesTool("", checkRulesInput(map[string]any{
		"rebalance": "weekly", "filter": "rising_63",
	}))
	if strings.HasPrefix(out, "ERROR") || !strings.Contains(out, "up over the last 63") {
		t.Errorf("lowercase keys: %s", out)
	}
}

func TestDescribeCoverage(t *testing.T) {
	d := func(s string) time.Time { t, _ := time.Parse("2006-01-02", s); return t }
	ranges := map[string]data.DateRange{
		"AAA":    {Min: d("2010-01-04"), Max: d("2025-06-30")},
		"BBB":    {Min: d("2014-03-03"), Max: d("2025-06-30")},
		"$SP500": {Min: d("1990-01-02"), Max: d("2025-05-30")},
	}
	want := []string{"AAA", "BBB", "$SP500"}
	if got := describeCoverage(ranges, want, "2015-01-02", "2024-12-31"); !strings.Contains(got, "2014-03-03..2025-05-30") ||
		!strings.Contains(got, "fits") {
		t.Errorf("inside the window: %s", got)
	}
	if got := describeCoverage(ranges, want, "2012-01-03", "2024-12-31"); !strings.HasPrefix(got, "ERROR") ||
		!strings.Contains(got, "2014-03-03") {
		t.Errorf("start before BBB's first bar must be refused with the real window: %s", got)
	}
	if got := describeCoverage(ranges, append(want, "ZZZ"), "2015-01-02", "2024-12-31"); !strings.Contains(got, "ZZZ") {
		t.Errorf("a ticker missing from the DB must be named: %s", got)
	}
}

// Live: a local model asked for an idea should come back with the engine's
// sentence. Gated like the other live tests; model looseness is logged, not
// failed, because a 4B model's wording is not this repo's regression.
func TestOllamaProposesRulesLive(t *testing.T) {
	if testing.Short() {
		t.Skip("live model test")
	}
	requireOllama(t)
	out, err := (&App{}).Chat(
		[]ChatMessage{{Role: "user", Content: "Each month, hold whichever 2 of " +
			"AAPL, MSFT, JNJ and XOM rose most over the last 126 trading days. " +
			"2015-01-02 to 2024-12-31, benchmark $SP500. Propose it as rules."}},
		liveOllamaModel, "", "", "../stock_data.db",
	)
	if err != nil {
		t.Fatalf("chat failed: %v", err)
	}
	t.Logf("reply: %s", out)
	if !strings.Contains(out, "rose most over the last 126 trading days") {
		t.Logf("note: reply does not quote check_rules' sentence; inspect the log above")
	}
}
