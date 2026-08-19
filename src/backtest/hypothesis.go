package backtest

// The loop: a hypothesis in, a verdict out.
//
// Everything else in the research programme exists so this can be trusted. A
// backtest lies through one mechanism above all others — deciding what counts
// as success AFTER seeing the results — and the cheapest possible fix is to
// write the success criterion down first and then evaluate it mechanically.
// So a Hypothesis is registered before a run, and Judge never takes an opinion
// as an argument.
//
// FOUR CONDITIONS, ALL OF THEM, NOT ANY:
//
//  1. The pre-registered criterion, whatever it said.
//  2. Campaign-deflated confidence >= MinDeflatedSharpe.
//  3. Beats the automatic baseline OUT OF SAMPLE, net of costs.
//  4. Robustness >= MinRobustnessScore, with a non-zero parameter-neighbourhood
//     component.
//
// THREE VERDICTS, AND "INCONCLUSIVE" MUST BE COMMON. A loop that never says
// "I cannot tell" is not testing anything. Two situations produce it: a
// condition that failed nothing but could not be evaluated (no split
// configured, robustness not run), and a run whose inputs disqualify it (a
// fundamental strategy with almost no fundamental coverage). Neither is a
// rejection — a rejection is a claim about the strategy, and these are claims
// about the evidence.
//
// UNEVALUABLE IS NEVER A PASS. Condition 4 currently cannot be evaluated at
// all: T24 shipped two of its five checks, and the parameter-neighbourhood
// component it names is not among them. So a verdict of "survives" is
// unreachable today, by construction, and every otherwise-good result reads
// "inconclusive" with that named as the reason. That is the correct behaviour
// for a system whose job is to not fool you, and finishing T24 turns it on
// without changing anything here.

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// The agreed guardrails. Named constants rather than literals because each one
// is a judgement call someone will want to argue with.
const (
	// DefaultTrialBudget is how many parameter sets one campaign may spend.
	// Large enough to search a two-parameter space properly, small enough that
	// the campaign-deflated Sharpe still has power: past a few thousand trials
	// the correction dominates and nothing can clear it, which is honest but
	// useless.
	DefaultTrialBudget = 200

	// MinDeflatedSharpe is the confidence a result must reach. Not 0.95: with
	// a few hundred trials over a few thousand days that is close to
	// unachievable, and a loop that can only ever say "inconclusive" teaches
	// nothing. 0.90 is demanding without being a wall.
	MinDeflatedSharpe = 0.90

	// MinRobustnessScore is the bar for condition 4.
	MinRobustnessScore = 0.6

	// MinFundamentalCoverage is the share of declared-fundamental cells a run
	// must actually have before its numbers mean anything. Below this the
	// strategy spent most of the window unable to see its own inputs, so the
	// result describes the tail of the window rather than the claim.
	MinFundamentalCoverage = 50.0
)

// Verdict is the mechanical answer. Three values, deliberately.
type Verdict string

const (
	// VerdictSurvives means every condition was evaluated and every one passed.
	VerdictSurvives Verdict = "survives"

	// VerdictRejected means at least one condition was evaluated and failed.
	// A claim about the strategy.
	VerdictRejected Verdict = "rejected"

	// VerdictInconclusive means nothing failed but something could not be
	// judged. A claim about the evidence, not the strategy.
	VerdictInconclusive Verdict = "inconclusive"
)

// CheckStatus is one condition's outcome.
type CheckStatus string

const (
	CheckPass        CheckStatus = "pass"
	CheckFail        CheckStatus = "fail"
	CheckUnavailable CheckStatus = "unavailable"
)

// Check is one condition, its outcome, and the numbers behind it. Detail is
// written for a human reading a rejection and asking "on what grounds".
type Check struct {
	Name   string      `json:"name"`
	Status CheckStatus `json:"status"`
	Detail string      `json:"detail"`
}

// Hypothesis is what gets registered BEFORE a run. Every field is an input;
// none of them is written after a result exists.
type Hypothesis struct {
	// ID names this hypothesis. A changed question is a NEW id, never an edit
	// — see Judge's contract.
	ID string `json:"id" toml:"ID"`
	// Claim is the one-sentence assertion being tested, in plain language.
	Claim string `json:"claim" toml:"Claim"`
	// Campaign groups hypotheses whose trials share a budget and a deflation.
	Campaign string `json:"campaign" toml:"Campaign"`
	// Criterion is the success test, chosen in advance. See ParseCriterion.
	Criterion string `json:"criterion" toml:"Criterion"`
	// TrialBudget caps the parameter sets this campaign may spend. Zero means
	// DefaultTrialBudget.
	TrialBudget  int       `json:"trialBudget" toml:"TrialBudget"`
	RegisteredAt time.Time `json:"registeredAt" toml:"-"`
}

// Budget returns the effective trial budget.
func (h Hypothesis) Budget() int {
	if h.TrialBudget <= 0 {
		return DefaultTrialBudget
	}
	return h.TrialBudget
}

// Criterion is a pre-registered success test: one metric, one comparison, one
// threshold. Deliberately not an expression language — a criterion complex
// enough to need parentheses is one nobody can check was written in advance.
type Criterion struct {
	Metric string
	Op     string
	Value  float64
}

// criterionMetrics maps the names a criterion may use to the figure each reads.
// Restricted on purpose: every name here is a number a Result actually carries,
// so a criterion cannot be registered against something that will never be
// measured.
var criterionMetrics = map[string]func(Result) float64{
	"sharpe":         func(r Result) float64 { return r.Metrics.SharpeRatio },
	"sortino":        func(r Result) float64 { return r.Metrics.SortinoRatio },
	"calmar":         func(r Result) float64 { return r.Metrics.CalmarRatio },
	"annual_return":  func(r Result) float64 { return r.Metrics.AnnualReturn },
	"max_drawdown":   func(r Result) float64 { return r.Metrics.MaxDrawdown },
	"total_return":   func(r Result) float64 { return r.FinalValue - r.InitialCapital },
	"turnover":       func(r Result) float64 { return r.Metrics.Turnover },
	"exposure":       func(r Result) float64 { return r.Metrics.AvgExposure },
	"time_in_market": func(r Result) float64 { return r.Metrics.TimeInMarket },
	"p_value":        func(r Result) float64 { return r.Significance.PValue },
}

// CriterionMetricNames lists the usable metric names, sorted, for error
// messages and documentation.
func CriterionMetricNames() []string {
	names := make([]string, 0, len(criterionMetrics))
	for k := range criterionMetrics {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// ParseCriterion reads "sharpe >= 1.2" and friends. Operators: > >= < <= .
//
// Rejects anything it does not fully understand rather than guessing, because
// a criterion that silently means something other than what was written is
// worse than no criterion: it still looks pre-registered.
func ParseCriterion(s string) (Criterion, error) {
	fields := strings.Fields(s)
	if len(fields) != 3 {
		return Criterion{}, fmt.Errorf(
			"criterion %q must read like \"sharpe >= 1.2\": a metric, an "+
				"operator and a number", s)
	}
	metric, op, raw := strings.ToLower(fields[0]), fields[1], fields[2]

	if _, ok := criterionMetrics[metric]; !ok {
		return Criterion{}, fmt.Errorf(
			"criterion metric %q is not measured; use one of: %s",
			fields[0], strings.Join(CriterionMetricNames(), ", "))
	}
	switch op {
	case ">", ">=", "<", "<=":
	default:
		return Criterion{}, fmt.Errorf(
			"criterion operator %q must be one of > >= < <=", op)
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return Criterion{}, fmt.Errorf("criterion threshold %q: %w", raw, err)
	}
	return Criterion{Metric: metric, Op: op, Value: v}, nil
}

// Evaluate applies the criterion to a result.
func (c Criterion) Evaluate(r Result) (bool, float64) {
	got := criterionMetrics[c.Metric](r)
	switch c.Op {
	case ">":
		return got > c.Value, got
	case ">=":
		return got >= c.Value, got
	case "<":
		return got < c.Value, got
	case "<=":
		return got <= c.Value, got
	}
	return false, got
}

func (c Criterion) String() string {
	return fmt.Sprintf("%s %s %g", c.Metric, c.Op, c.Value)
}

// VerdictReport is the judgement and everything it rests on.
type VerdictReport struct {
	HypothesisID string    `json:"hypothesisId"`
	Claim        string    `json:"claim"`
	Campaign     string    `json:"campaign"`
	Portfolio    string    `json:"portfolio"`
	Criterion    string    `json:"criterion"`
	Verdict      Verdict   `json:"verdict"`
	Checks       []Check   `json:"checks"`
	JudgedAt     time.Time `json:"judgedAt"`
	// TrialsSpent is the campaign's cumulative trial count and Budget what it
	// was allowed. Spent past Budget does not change the verdict — it says the
	// campaign is over and further searching is not evidence.
	TrialsSpent int  `json:"trialsSpent"`
	Budget      int  `json:"budget"`
	BudgetSpent bool `json:"budgetSpent"`
}

// Judge applies the pre-registered criterion and the three standing conditions
// to one result, mechanically.
//
// It takes the hypothesis and the result and nothing else that could carry an
// opinion. campaignTrials is the campaign's cumulative count from the registry
// (T22/T23); pass the result's own Trials when there is no campaign.
//
// THE CONTRACT: this function must never be given a criterion that was chosen
// after seeing r. Nothing in Go can enforce that — it is enforced by
// registering the hypothesis first and by the rule that a changed question
// gets a new ID.
func Judge(h Hypothesis, r Result, campaignTrials int) VerdictReport {
	rep := VerdictReport{
		HypothesisID: h.ID,
		Claim:        h.Claim,
		Campaign:     h.Campaign,
		Portfolio:    r.PortfolioName,
		Criterion:    h.Criterion,
		JudgedAt:     time.Now(),
		TrialsSpent:  campaignTrials,
		Budget:       h.Budget(),
	}
	rep.BudgetSpent = campaignTrials > rep.Budget

	rep.Checks = append(rep.Checks,
		checkCriterion(h, r),
		checkDeflation(r, campaignTrials),
		checkBeatsBaselineOutOfSample(r),
		checkRobustness(r),
	)
	// A run whose inputs disqualify it is not a result. Appended last so it
	// reads as a qualifier on everything above it.
	if c, ok := checkInputsUsable(r); ok {
		rep.Checks = append(rep.Checks, c)
	}

	rep.Verdict = combine(rep.Checks)
	return rep
}

// combine turns the checks into the verdict. Any failure rejects; otherwise
// any unevaluated condition makes it inconclusive; only an all-pass survives.
func combine(checks []Check) Verdict {
	unavailable := false
	for _, c := range checks {
		switch c.Status {
		case CheckFail:
			return VerdictRejected
		case CheckUnavailable:
			unavailable = true
		}
	}
	if unavailable {
		return VerdictInconclusive
	}
	return VerdictSurvives
}

func checkCriterion(h Hypothesis, r Result) Check {
	c := Check{Name: "pre-registered criterion"}
	if strings.TrimSpace(h.Criterion) == "" {
		c.Status = CheckUnavailable
		c.Detail = "no criterion was registered, so there is nothing to judge " +
			"against; a hypothesis without one is not pre-registered"
		return c
	}
	crit, err := ParseCriterion(h.Criterion)
	if err != nil {
		c.Status = CheckUnavailable
		c.Detail = err.Error()
		return c
	}
	ok, got := crit.Evaluate(r)
	c.Detail = fmt.Sprintf("%s — measured %.4f", crit, got)
	if ok {
		c.Status = CheckPass
	} else {
		c.Status = CheckFail
	}
	return c
}

func checkDeflation(r Result, campaignTrials int) Check {
	c := Check{Name: "campaign-deflated confidence"}
	if r.DeflatedSharpe <= 0 {
		c.Status = CheckUnavailable
		c.Detail = "deflated Sharpe was not computed for this run"
		return c
	}
	c.Detail = fmt.Sprintf(
		"%.3f against a bar of %.2f, over %d campaign trial(s); a no-edge "+
			"best-of-%d would show %.2f",
		r.DeflatedSharpe, MinDeflatedSharpe, campaignTrials,
		max(campaignTrials, 1), r.ExpectedMaxSharpe)
	if r.DeflatedSharpe >= MinDeflatedSharpe {
		c.Status = CheckPass
	} else {
		c.Status = CheckFail
	}
	return c
}

// checkBeatsBaselineOutOfSample is condition 3. It needs BOTH a validation
// split and a baseline measured over that same out-of-sample window; whole-run
// baselines do not answer the question, because the parameters were chosen
// with the in-sample half in view.
func checkBeatsBaselineOutOfSample(r Result) Check {
	c := Check{Name: "beats the baseline out of sample"}
	if !r.Baseline.Computed {
		c.Status = CheckUnavailable
		c.Detail = "no baseline could be built for this universe"
		return c
	}
	oos, ok := outOfSample(r.Splits)
	if !ok {
		c.Status = CheckUnavailable
		c.Detail = "no out-of-sample segment; set [portfolio.Validation] so " +
			"there is a stretch the parameters were not chosen on"
		return c
	}
	if !r.Baseline.OutOfSampleComputed {
		c.Status = CheckUnavailable
		c.Detail = "the baseline was not measured over the out-of-sample " +
			"window, so the comparison would mix two periods"
		return c
	}
	c.Detail = fmt.Sprintf(
		"out of sample %s..%s: strategy %+.2f%%/yr vs baseline %+.2f%%/yr",
		oos.Start, oos.End, oos.AnnualReturn, r.Baseline.OutOfSampleAnnualReturn)
	if oos.AnnualReturn > r.Baseline.OutOfSampleAnnualReturn {
		c.Status = CheckPass
	} else {
		c.Status = CheckFail
	}
	return c
}

// checkRobustness is condition 4, and today it can only ever be unavailable:
// T24 shipped cost and start-date sensitivity, and neither the aggregate score
// nor the parameter-neighbourhood component this condition names exists. See
// the file comment — unevaluable must never read as a pass.
func checkRobustness(r Result) Check {
	c := Check{Name: "robustness"}
	if !r.Robustness.Computed {
		c.Status = CheckUnavailable
		c.Detail = "the robustness battery was not run; pass -robustness or " +
			"-research"
		return c
	}
	c.Status = CheckUnavailable
	c.Detail = fmt.Sprintf(
		"cost and start-date sensitivity ran (break-even %.0f bps, start "+
			"spread %.2f pts), but the parameter-neighbourhood component this "+
			"condition requires is not built (T24), so a score against the "+
			"%.1f bar cannot be computed",
		r.Robustness.BreakEvenBps, r.Robustness.StartSpread, MinRobustnessScore)
	return c
}

// checkInputsUsable is the qualifier T33 could not put in T25: a strategy that
// declared fundamentals and then could not see them for most of the window has
// not been tested, whatever its numbers say. Returns false when the run
// declared no fundamentals, so the check does not appear at all.
func checkInputsUsable(r Result) (Check, bool) {
	if r.Metrics.FundamentalCoverage <= 0 {
		return Check{}, false
	}
	c := Check{Name: "fundamental coverage"}
	c.Detail = fmt.Sprintf(
		"%.1f%% of declared-metric cells had a published figure (bar %.0f%%)",
		r.Metrics.FundamentalCoverage, MinFundamentalCoverage)
	if r.Metrics.FundamentalCoverage >= MinFundamentalCoverage {
		c.Status = CheckPass
	} else {
		c.Status = CheckUnavailable
		c.Detail += "; the strategy could not see its own inputs for most of " +
			"the window, so this run describes the tail of it rather than the claim"
	}
	return c, true
}

func outOfSample(splits []SegmentStats) (SegmentStats, bool) {
	for _, s := range splits {
		if s.Label == "out-of-sample" {
			return s, true
		}
	}
	return SegmentStats{}, false
}

// String renders the report as the research log entry it is meant to be —
// including rejections, which are the entries that make the log evidence
// rather than marketing.
func (v VerdictReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "hypothesis %s — %s\n", v.HypothesisID, v.Verdict)
	if v.Claim != "" {
		fmt.Fprintf(&b, "  claim:     %s\n", v.Claim)
	}
	if v.Portfolio != "" {
		fmt.Fprintf(&b, "  portfolio: %s\n", v.Portfolio)
	}
	if v.Campaign != "" {
		fmt.Fprintf(&b, "  campaign:  %s (%d of %d trials spent)\n",
			v.Campaign, v.TrialsSpent, v.Budget)
	}
	for _, c := range v.Checks {
		mark := map[CheckStatus]string{
			CheckPass: "PASS", CheckFail: "FAIL", CheckUnavailable: "n/a ",
		}[c.Status]
		fmt.Fprintf(&b, "  [%s] %s\n         %s\n", mark, c.Name, c.Detail)
	}
	if v.BudgetSpent {
		fmt.Fprintf(&b,
			"  BUDGET SPENT: %d trials against a budget of %d. Further "+
				"searching in this campaign is not evidence.\n",
			v.TrialsSpent, v.Budget)
	}
	return b.String()
}

// LoadHypothesis reads a pre-registration file:
//
//	ID          = "H-1"
//	Claim       = "12-1 momentum beats buy-and-hold on the benchmark series"
//	Campaign    = "trend"
//	Criterion   = "sharpe >= 1.2"
//	TrialBudget = 200
//
// The criterion is parsed HERE, at registration, not at judgement. A criterion
// that turns out to be unparseable only after a run has produced numbers is a
// criterion chosen after seeing them, whatever the file's timestamp says.
func LoadHypothesis(path string) (Hypothesis, error) {
	var h Hypothesis
	raw, err := os.ReadFile(path)
	if err != nil {
		return h, fmt.Errorf("reading hypothesis %s: %w", path, err)
	}
	if err := toml.Unmarshal(raw, &h); err != nil {
		return h, fmt.Errorf("parsing hypothesis %s: %w", path, err)
	}
	if strings.TrimSpace(h.ID) == "" {
		return h, fmt.Errorf("hypothesis %s: ID is required — a changed "+
			"question is a new hypothesis with a new ID, never an edit to "+
			"this one", path)
	}
	if strings.TrimSpace(h.Criterion) == "" {
		return h, fmt.Errorf("hypothesis %s: Criterion is required; without "+
			"one this is not a pre-registration", path)
	}
	if _, err := ParseCriterion(h.Criterion); err != nil {
		return h, fmt.Errorf("hypothesis %s: %w", path, err)
	}
	h.RegisteredAt = time.Now()
	return h, nil
}
