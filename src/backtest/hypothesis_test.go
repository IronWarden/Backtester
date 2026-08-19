package backtest

import (
	"strings"
	"testing"
)

// passingResult is a result that clears every condition that CAN currently be
// evaluated. Condition 4 is unavailable by construction (see hypothesis.go), so
// this alone still judges inconclusive — the tests below are explicit about it.
func passingResult() Result {
	return Result{
		PortfolioName:  "candidate",
		InitialCapital: 100000,
		FinalValue:     130000,
		Metrics: Metrics{
			SharpeRatio: 1.4, SortinoRatio: 1.9, CalmarRatio: 0.9,
			AnnualReturn: 16.0, MaxDrawdown: -18.0, Turnover: 3.0,
			AvgExposure: 87, TimeInMarket: 91,
		},
		DeflatedSharpe:    0.95,
		ExpectedMaxSharpe: 0.7,
		Significance:      Significance{Computed: true, PValue: 0.02, Draws: 1000},
		Baseline: BaselineStats{
			Computed: true, Beat: true, Name: BaselineName,
			AnnualReturn:            9.0,
			OutOfSampleAnnualReturn: 8.0,
			OutOfSampleComputed:     true,
		},
		Splits: []SegmentStats{
			{Label: "in-sample", Start: "2020-01-02", End: "2022-12-30", Days: 700, AnnualReturn: 20},
			{Label: "out-of-sample", Start: "2023-01-03", End: "2024-12-31", Days: 500, AnnualReturn: 12},
		},
		Robustness: Robustness{Computed: true, BreakEvenBps: 40, StartSpread: 2.5},
	}
}

func hyp(criterion string) Hypothesis {
	return Hypothesis{
		ID: "H-1", Claim: "momentum works on benchmarks",
		Campaign: "trend", Criterion: criterion,
	}
}

func findCheck(t *testing.T, rep VerdictReport, name string) Check {
	t.Helper()
	for _, c := range rep.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no check named %q in %v", name, rep.Checks)
	return Check{}
}

func TestParseCriterion(t *testing.T) {
	good := map[string]Criterion{
		"sharpe >= 1.2":        {Metric: "sharpe", Op: ">=", Value: 1.2},
		"SHARPE > 1":           {Metric: "sharpe", Op: ">", Value: 1},
		"max_drawdown > -25":   {Metric: "max_drawdown", Op: ">", Value: -25},
		"p_value <= 0.05":      {Metric: "p_value", Op: "<=", Value: 0.05},
		"annual_return >= 8.5": {Metric: "annual_return", Op: ">=", Value: 8.5},
	}
	for in, want := range good {
		got, err := ParseCriterion(in)
		if err != nil {
			t.Errorf("ParseCriterion(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseCriterion(%q) = %+v, want %+v", in, got, want)
		}
	}

	// Anything not fully understood must be refused, not guessed at: a
	// criterion that silently means something else still looks pre-registered.
	for _, bad := range []string{
		"", "sharpe", "sharpe >", "sharpe >= high", "sharpe => 1.2",
		"profit >= 1.2", "sharpe >= 1.2 and calmar > 1", "1.2 <= sharpe",
	} {
		if _, err := ParseCriterion(bad); err == nil {
			t.Errorf("ParseCriterion(%q) accepted a criterion it should refuse", bad)
		}
	}
}

func TestCriterionEvaluate(t *testing.T) {
	r := passingResult()
	cases := []struct {
		criterion string
		want      bool
	}{
		{"sharpe >= 1.2", true},
		{"sharpe >= 2.0", false},
		{"max_drawdown > -25", true}, // -18 is shallower than -25
		{"max_drawdown > -10", false},
		{"p_value <= 0.05", true},
		{"annual_return >= 20", false},
	}
	for _, c := range cases {
		crit, err := ParseCriterion(c.criterion)
		if err != nil {
			t.Fatalf("ParseCriterion(%q): %v", c.criterion, err)
		}
		if got, _ := crit.Evaluate(r); got != c.want {
			t.Errorf("%q evaluated %v, want %v", c.criterion, got, c.want)
		}
	}
}

// A failed criterion rejects outright, and rejection wins over any number of
// unavailable conditions — a claim about the strategy outranks a claim about
// the evidence.
func TestJudgeRejectsOnTheCriterion(t *testing.T) {
	rep := Judge(hyp("sharpe >= 2.0"), passingResult(), 12)
	if rep.Verdict != VerdictRejected {
		t.Errorf("verdict = %q, want rejected", rep.Verdict)
	}
	c := findCheck(t, rep, "pre-registered criterion")
	if c.Status != CheckFail {
		t.Errorf("criterion check = %q, want fail", c.Status)
	}
	if !strings.Contains(c.Detail, "1.4") {
		t.Errorf("detail should name the measured value; got %q", c.Detail)
	}
}

// The headline behaviour, and the one most likely to be "fixed" by someone who
// finds it inconvenient: condition 4 cannot be evaluated, so nothing survives
// yet. Unevaluable must never read as a pass.
func TestJudgeIsInconclusiveWhileRobustnessCannotBeScored(t *testing.T) {
	rep := Judge(hyp("sharpe >= 1.2"), passingResult(), 12)
	if rep.Verdict != VerdictInconclusive {
		t.Fatalf("verdict = %q, want inconclusive", rep.Verdict)
	}
	c := findCheck(t, rep, "robustness")
	if c.Status != CheckUnavailable {
		t.Errorf("robustness check = %q, want unavailable", c.Status)
	}
	if !strings.Contains(c.Detail, "parameter-neighbourhood") {
		t.Errorf("the reason must name what is missing; got %q", c.Detail)
	}
	// Everything else genuinely passed.
	for _, name := range []string{
		"pre-registered criterion", "campaign-deflated confidence",
		"beats the baseline out of sample",
	} {
		if got := findCheck(t, rep, name); got.Status != CheckPass {
			t.Errorf("%s = %q, want pass (%s)", name, got.Status, got.Detail)
		}
	}
}

// Every condition passing is what "survives" means. Proven here by forcing the
// one unevaluable condition to pass, which is also the regression guard for the
// day T24 is finished.
func TestJudgeSurvivesOnlyWhenEveryConditionPasses(t *testing.T) {
	checks := []Check{
		{Name: "a", Status: CheckPass},
		{Name: "b", Status: CheckPass},
	}
	if got := combine(checks); got != VerdictSurvives {
		t.Errorf("all-pass combined to %q, want survives", got)
	}
	if got := combine(append(checks, Check{Status: CheckUnavailable})); got != VerdictInconclusive {
		t.Errorf("one unavailable combined to %q, want inconclusive", got)
	}
	if got := combine(append(checks, Check{Status: CheckFail})); got != VerdictRejected {
		t.Errorf("one failure combined to %q, want rejected", got)
	}
	// A failure outranks an unavailable regardless of order.
	mixed := []Check{{Status: CheckUnavailable}, {Status: CheckFail}}
	if got := combine(mixed); got != VerdictRejected {
		t.Errorf("unavailable+fail combined to %q, want rejected", got)
	}
}

func TestJudgeRejectsWhenTheBaselineWinsOutOfSample(t *testing.T) {
	r := passingResult()
	r.Baseline.OutOfSampleAnnualReturn = 15 // beats the strategy's 12
	rep := Judge(hyp("sharpe >= 1.2"), r, 12)
	if rep.Verdict != VerdictRejected {
		t.Errorf("verdict = %q, want rejected", rep.Verdict)
	}
	if c := findCheck(t, rep, "beats the baseline out of sample"); c.Status != CheckFail {
		t.Errorf("baseline check = %q, want fail", c.Status)
	}
}

// Without a split there is no out-of-sample stretch, so the condition cannot be
// judged — it must not silently fall back to the whole-run comparison, which
// would score the parameter fitting as if it were a finding.
func TestJudgeWillNotSubstituteAWholeRunBaseline(t *testing.T) {
	r := passingResult()
	r.Splits = nil
	r.Baseline.OutOfSampleComputed = false
	rep := Judge(hyp("sharpe >= 1.2"), r, 12)
	c := findCheck(t, rep, "beats the baseline out of sample")
	if c.Status != CheckUnavailable {
		t.Errorf("check = %q, want unavailable without a split", c.Status)
	}
	if !strings.Contains(c.Detail, "Validation") {
		t.Errorf("detail should say how to fix it; got %q", c.Detail)
	}
}

func TestJudgeRejectsAWeakDeflatedSharpe(t *testing.T) {
	r := passingResult()
	r.DeflatedSharpe = 0.62
	rep := Judge(hyp("sharpe >= 1.2"), r, 200)
	if rep.Verdict != VerdictRejected {
		t.Errorf("verdict = %q, want rejected", rep.Verdict)
	}
	if c := findCheck(t, rep, "campaign-deflated confidence"); c.Status != CheckFail {
		t.Errorf("deflation check = %q, want fail", c.Status)
	}
}

// A missing criterion is not a pass. A hypothesis without one was never
// pre-registered, whatever else it carries.
func TestJudgeTreatsAMissingCriterionAsUnjudgeable(t *testing.T) {
	rep := Judge(hyp(""), passingResult(), 12)
	c := findCheck(t, rep, "pre-registered criterion")
	if c.Status != CheckUnavailable {
		t.Errorf("empty criterion = %q, want unavailable", c.Status)
	}
	if rep.Verdict == VerdictSurvives {
		t.Error("a hypothesis with no criterion must never survive")
	}
}

// T33's gate, which could not live in T25: a strategy that declared
// fundamentals and could not see them has not been tested. Inconclusive, not
// rejected — the evidence is disqualified, the strategy is not condemned.
func TestJudgeDisqualifiesARunWithTooLittleFundamentalCoverage(t *testing.T) {
	r := passingResult()
	r.Metrics.FundamentalCoverage = 31.4
	rep := Judge(hyp("sharpe >= 1.2"), r, 12)
	c := findCheck(t, rep, "fundamental coverage")
	if c.Status != CheckUnavailable {
		t.Errorf("coverage check = %q, want unavailable", c.Status)
	}
	if rep.Verdict == VerdictRejected {
		t.Error("low coverage disqualifies the evidence; it does not reject the strategy")
	}

	// Above the bar it is an ordinary pass.
	r.Metrics.FundamentalCoverage = 88
	rep = Judge(hyp("sharpe >= 1.2"), r, 12)
	if c := findCheck(t, rep, "fundamental coverage"); c.Status != CheckPass {
		t.Errorf("88%% coverage = %q, want pass", c.Status)
	}

	// A run that declared no fundamentals gets no check at all, rather than a
	// zero that reads as a failure.
	r.Metrics.FundamentalCoverage = 0
	rep = Judge(hyp("sharpe >= 1.2"), r, 12)
	for _, c := range rep.Checks {
		if c.Name == "fundamental coverage" {
			t.Error("a run declaring no fundamentals should carry no coverage check")
		}
	}
}

func TestBudgetSpentIsReportedButDoesNotDecideTheVerdict(t *testing.T) {
	h := hyp("sharpe >= 1.2")
	h.TrialBudget = 50

	rep := Judge(h, passingResult(), 51)
	if !rep.BudgetSpent {
		t.Error("51 trials against a budget of 50 should report the budget spent")
	}
	if rep.Verdict != VerdictInconclusive {
		t.Errorf("verdict = %q; the budget is a campaign fact, not a condition", rep.Verdict)
	}
	if !strings.Contains(rep.String(), "BUDGET SPENT") {
		t.Error("the report should say the budget is spent")
	}

	rep = Judge(h, passingResult(), 50)
	if rep.BudgetSpent {
		t.Error("exactly at budget is not over it")
	}
}

func TestBudgetDefaults(t *testing.T) {
	if got := (Hypothesis{}).Budget(); got != DefaultTrialBudget {
		t.Errorf("default budget = %d, want %d", got, DefaultTrialBudget)
	}
	if got := (Hypothesis{TrialBudget: 25}).Budget(); got != 25 {
		t.Errorf("explicit budget = %d, want 25", got)
	}
}

// A rejection has to say what it rejected on, or the research log is a list of
// nos with no evidence in it.
func TestVerdictReportNamesItsGrounds(t *testing.T) {
	out := Judge(hyp("sharpe >= 2.0"), passingResult(), 12).String()
	for _, want := range []string{
		"H-1", "rejected", "momentum works on benchmarks",
		"sharpe >= 2", "FAIL", "trend",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report omits %q:\n%s", want, out)
		}
	}
}
