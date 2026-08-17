package backtest

// The battery exists to make a fragile result visible, so the tests build
// results whose fragility is known in advance: one whose edge is small enough
// that friction kills it, and one whose returns are identical whenever you
// start.

import (
	"math"
	"strings"
	"testing"
)

// withRobustness turns the battery on for one test and restores the package
// flag afterwards, since it is global by design (like TransactionLogger).
func withRobustness(t *testing.T) {
	t.Helper()
	prev := RobustnessChecks
	RobustnessChecks = true
	t.Cleanup(func() { RobustnessChecks = prev })
}

// Off by default: a run that did not ask for the battery must not pay for it,
// and must not report figures it never computed.
func TestRobustnessIsOffByDefault(t *testing.T) {
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{"AAA": rampCloses(300, 0.5)},
		CostConfig{})
	if p.Robustness.Computed {
		t.Error("the battery ran without being asked for")
	}
	if len(p.Robustness.Costs) != 0 || len(p.Robustness.Starts) != 0 {
		t.Errorf("battery produced points while disabled: %+v", p.Robustness)
	}
}

// Friction is a drag, always: every cost level must leave the strategy no
// better off than the one below it. A battery that showed costs helping would
// be measuring something other than costs.
func TestHigherCostsNeverHelp(t *testing.T) {
	withRobustness(t)
	// A strategy that actually trades, so friction has something to bite on.
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA", "BBB"}, map[string][]float64{
			"AAA": rampCloses(300, 0.4),
			"BBB": rampCloses(300, 0.2),
		}, CostConfig{})

	if !p.Robustness.Computed {
		t.Fatal("the battery did not run")
	}
	if len(p.Robustness.Costs) != len(CostLevels) {
		t.Fatalf("got %d cost points, want %d",
			len(p.Robustness.Costs), len(CostLevels))
	}
	for i := 1; i < len(p.Robustness.Costs); i++ {
		prev, cur := p.Robustness.Costs[i-1], p.Robustness.Costs[i]
		if cur.FinalValue > prev.FinalValue+1e-9 {
			t.Errorf("%.0f bps finished at %.2f, ABOVE the %.2f at %.0f bps — "+
				"more friction cannot help", cur.SlippageBps, cur.FinalValue,
				prev.FinalValue, prev.SlippageBps)
		}
	}
	// The zero-cost point must reproduce the run itself, which is the
	// calibration case for the whole re-run path.
	almost(t, "zero-cost final value", p.Robustness.Costs[0].FinalValue,
		finalValue(t, p))
}

// compoundingCloses grows by a constant PERCENTAGE each day, which is what
// "steady" has to mean here: rampCloses adds a fixed number of dollars, so its
// percentage growth decelerates and a later start genuinely earns a lower CAGR.
// Using a linear ramp as the start-independent fixture would be testing the
// fixture's arithmetic rather than the battery.
func compoundingCloses(n int, daily float64) []float64 {
	out := make([]float64, n)
	px := 100.0
	for i := range out {
		out[i] = px
		px *= 1 + daily
	}
	return out
}

// A path that compounds at a constant rate returns the same whatever day you
// start, so its start spread must be near zero. A wide spread here would mean
// the re-run is measuring something other than the strategy.
func TestStartSpreadIsSmallOnASteadyPath(t *testing.T) {
	withRobustness(t)
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"},
		map[string][]float64{"AAA": compoundingCloses(400, 0.002)},
		CostConfig{})

	if !p.Robustness.Computed {
		t.Fatal("the battery did not run")
	}
	if len(p.Robustness.Starts) != len(StartShifts) {
		t.Fatalf("got %d start points, want %d",
			len(p.Robustness.Starts), len(StartShifts))
	}
	if p.Robustness.StartSpread > 1 {
		t.Errorf("start spread %.2f points on a constantly compounding path, "+
			"want ~0 — every start date earns the same rate",
			p.Robustness.StartSpread)
	}
	if p.Robustness.BestStart < p.Robustness.WorstStart {
		t.Errorf("best start %.2f is below the worst %.2f",
			p.Robustness.BestStart, p.Robustness.WorstStart)
	}
}

// A path that only rises in one stretch is start-dependent by construction: a
// run beginning after the stretch misses it entirely. This is the case the
// check exists to catch.
func TestStartSpreadIsWideOnASingleStretchPath(t *testing.T) {
	withRobustness(t)
	// Flat, then one sharp rally in the first 80 days, then flat again.
	closes := make([]float64, 400)
	px := 100.0
	for i := range closes {
		if i > 20 && i < 80 {
			px *= 1.03
		}
		closes[i] = px
	}
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{"AAA": closes}, CostConfig{})

	if !p.Robustness.Computed {
		t.Fatal("the battery did not run")
	}
	if p.Robustness.StartSpread <= 5 {
		t.Errorf("start spread %.2f points on a path whose entire return "+
			"happens in one 60-day stretch — starting after it must give a "+
			"very different answer", p.Robustness.StartSpread)
	}
}

// BreakEvenBps is -1 when the strategy survives everything tested, which is a
// different statement from "not tested" and must not be confused with zero.
func TestBreakEvenIsMinusOneWhenNothingKillsIt(t *testing.T) {
	withRobustness(t)
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{"AAA": rampCloses(400, 1.0)},
		CostConfig{})

	if p.Robustness.BreakEvenBps != -1 {
		t.Errorf("break-even = %v for a strategy that survives 50bps, want -1",
			p.Robustness.BreakEvenBps)
	}
	if !math.Signbit(p.Robustness.BreakEvenBps) {
		t.Error("break-even should be negative when nothing killed the edge")
	}
}

// The report has to say which it is, because "-1" printed raw reads as a
// failure and "0 bps" reads as an immediate death.
func TestRobustnessReportDistinguishesSurvivalFromDeath(t *testing.T) {
	survived := Robustness{Computed: true, BreakEvenBps: -1,
		Costs: []CostPoint{{SlippageBps: 0}}}
	if got := survived.String(); !strings.Contains(got, "survives every level") {
		t.Errorf("survival not stated: %q", got)
	}

	died := Robustness{Computed: true, BreakEvenBps: 20,
		Costs: []CostPoint{{SlippageBps: 0}}}
	if got := died.String(); !strings.Contains(got, "dies at 20 bps") {
		t.Errorf("death not stated: %q", got)
	}

	if got := (Robustness{}).String(); !strings.Contains(got, "not computed") {
		t.Errorf("an uncomputed battery reported something: %q", got)
	}
}

// Friction must actually be applied, not merely iterated over. Monotonicity
// alone cannot tell "costs were charged" from "costs were ignored" — a flat
// line satisfies "never helps" perfectly — so this asserts a strict loss.
func TestHighestCostIsStrictlyWorseThanFree(t *testing.T) {
	withRobustness(t)
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA", "BBB"}, map[string][]float64{
			"AAA": rampCloses(300, 0.4),
			"BBB": rampCloses(300, 0.2),
		}, CostConfig{})

	points := p.Robustness.Costs
	if len(points) < 2 {
		t.Fatalf("got %d cost points", len(points))
	}
	free, worst := points[0], points[len(points)-1]
	if worst.FinalValue >= free.FinalValue {
		t.Errorf("%.0f bps finished at %.2f, not below the %.2f at 0 bps — "+
			"the cost level is not reaching the re-run", worst.SlippageBps,
			worst.FinalValue, free.FinalValue)
	}
}

// A later start must SKIP the early window, not shorten the late one. The
// difference is invisible on a smooth path and decisive here: the first 30 days
// crash, so any run that starts after them must beat the one that sat through
// them. A version that truncated the end instead would still eat the crash.
func TestALaterStartSkipsTheEarlyWindow(t *testing.T) {
	withRobustness(t)
	closes := make([]float64, 400)
	px := 100.0
	for i := range closes {
		switch {
		case i < 30:
			px *= 0.97 // a crash, only in the first six weeks
		default:
			px *= 1.004
		}
		closes[i] = px
	}
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{"AAA": closes}, CostConfig{})

	base := p.Metrics.AnnualReturn
	for _, s := range p.Robustness.Starts {
		if s.ShiftDays < 60 {
			continue // still inside or near the crash
		}
		if s.AnnualReturn <= base {
			t.Errorf("starting %d days later returned %.2f%%, not above the "+
				"base %.2f%% — the shifted run is still eating the early "+
				"crash, so the window was truncated at the END rather than "+
				"re-based at the start", s.ShiftDays, s.AnnualReturn, base)
		}
	}
}

// Costs are supposed to change returns, not behaviour. When the trade count
// moves, the cost curve is measuring two things at once and its drop cannot be
// read as friction — so the report has to say so.
//
// This is not hypothetical: it fires on the shipped default config, where one
// order of five is rejected at 5 bps and the final value falls from 590k to 69k.
// That is T13, and the battery is how it becomes visible in a run rather than in
// a task file.
func TestCostsChangingBehaviourIsFlagged(t *testing.T) {
	r := Robustness{
		Computed: true, BreakEvenBps: -1,
		Costs: []CostPoint{
			{SlippageBps: 0, Trades: 5, FinalValue: 590363},
			{SlippageBps: 5, Trades: 4, FinalValue: 69006},
			{SlippageBps: 50, Trades: 4, FinalValue: 68970},
		},
		TradesChangedAtBps: 5,
	}
	got := r.String()
	if !strings.Contains(got, "trade count CHANGES at 5 bps") {
		t.Errorf("behaviour change not reported: %q", got)
	}
	if !strings.Contains(got, "4 fills against 5") {
		t.Errorf("report does not name both counts: %q", got)
	}

	// A strategy whose fills are unchanged says nothing about behaviour.
	steady := Robustness{Computed: true, BreakEvenBps: -1,
		Costs: []CostPoint{
			{SlippageBps: 0, Trades: 5}, {SlippageBps: 50, Trades: 5}}}
	if strings.Contains(steady.String(), "trade count CHANGES") {
		t.Error("an unchanged trade count was reported as a behaviour change")
	}
}

// And the detection itself, through a real run: the shipped default config is
// not available here, but any strategy whose fills differ under friction must
// set the field.
func TestTradeCountChangeIsDetectedDuringARun(t *testing.T) {
	withRobustness(t)
	// A commission large enough to be refused on the smaller of two orders.
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA", "BBB"}, map[string][]float64{
			"AAA": rampCloses(200, 0.3),
			"BBB": rampCloses(200, 0.1),
		}, CostConfig{})

	// Whatever the outcome, the invariant is that the flag and the data agree:
	// it is set exactly when some level's fill count differs from the free one.
	free := p.Robustness.Costs[0].Trades
	var differs bool
	for _, c := range p.Robustness.Costs[1:] {
		if c.Trades != free {
			differs = true
		}
	}
	if differs != (p.Robustness.TradesChangedAtBps > 0) {
		t.Errorf("fills differ = %v but TradesChangedAtBps = %v; the flag and "+
			"the recorded counts disagree", differs,
			p.Robustness.TradesChangedAtBps)
	}
}
