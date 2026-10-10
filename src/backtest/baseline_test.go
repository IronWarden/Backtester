package backtest

// The baseline answers "was this worth doing at all?", so the tests are about
// the comparison being honest rather than about the arithmetic being novel: the
// baseline is a real buy-and-hold run, and buy-and-hold is already pinned by
// correctness_test.go.

import (
	"math"
	"testing"
)

// A buy-and-hold portfolio IS the baseline, so the comparison must come out
// exactly level. This is the calibration case: any drift here means the
// baseline is not simulating what it claims to.
func TestBaselineMatchesAnIdenticalStrategy(t *testing.T) {
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA", "BBB"}, map[string][]float64{
			"AAA": {100, 110, 120, 130},
			"BBB": {50, 55, 45, 60},
		}, CostConfig{})

	if !p.Baseline.Computed {
		t.Fatal("no baseline was computed for a two-ticker run")
	}
	almost(t, "baseline final value", p.Baseline.FinalValue, finalValue(t, p))
	if math.Abs(p.Baseline.ExcessFinalValue) > 1e-9 {
		t.Errorf("a buy-and-hold portfolio beat the buy-and-hold baseline by "+
			"%v, want 0", p.Baseline.ExcessFinalValue)
	}
	if p.Baseline.Beat {
		t.Error("Beat is true for a strategy that exactly matched the baseline")
	}
	if p.Baseline.Name != BaselineName {
		t.Errorf("baseline name = %q, want %q", p.Baseline.Name, BaselineName)
	}
}

// A strategy that concentrates on the winner must beat an equal-weight hold of
// the same universe, and the flag must say so. This is the case the feature
// exists for, and the direction of the comparison is the thing most worth
// pinning: a sign error here would report every good strategy as a failure.
func TestBaselineDetectsAStrategyThatBeatsIt(t *testing.T) {
	closes := map[string][]float64{
		"AAA": {100, 120, 140, 200}, // doubles
		"BBB": {100, 100, 100, 100}, // flat
	}
	// greedy spends everything on the first ticker, so it holds only the winner.
	p := runExact(t, "buyAndHold:greedy", exactCash,
		[]string{"AAA", "BBB"}, closes, CostConfig{})

	if !p.Baseline.Computed {
		t.Fatal("no baseline computed")
	}
	// Equal weight: half doubles, half is flat -> 1.5x. Greedy: all of it
	// doubles -> 2x. Both derived by hand from the path above.
	almost(t, "baseline final value", p.Baseline.FinalValue, exactCash*1.5)
	almost(t, "strategy final value", finalValue(t, p), exactCash*2.0)
	almost(t, "excess", p.Baseline.ExcessFinalValue, exactCash*0.5)
	if !p.Baseline.Beat {
		t.Error("Beat is false for a strategy that finished 50% ahead")
	}
	if p.Baseline.ExcessAnnualReturn <= 0 {
		t.Errorf("excess annual return = %v, want positive",
			p.Baseline.ExcessAnnualReturn)
	}
}

// And the reverse: concentrating on the loser must report Beat false. Worth its
// own case because a flag that is always true is indistinguishable from a
// working one on the test above alone.
func TestBaselineDetectsAStrategyThatLosesToIt(t *testing.T) {
	closes := map[string][]float64{
		"AAA": {100, 90, 80, 50}, // halves
		"BBB": {100, 110, 120, 150},
	}
	p := runExact(t, "buyAndHold:greedy", exactCash,
		[]string{"AAA", "BBB"}, closes, CostConfig{})

	// Equal weight: half halves, half is up 50% -> 1.0x. Greedy holds the
	// loser -> 0.5x.
	almost(t, "baseline final value", p.Baseline.FinalValue, exactCash*1.0)
	almost(t, "excess", p.Baseline.ExcessFinalValue, -exactCash*0.5)
	if p.Baseline.Beat {
		t.Error("Beat is true for a strategy that lost half the book")
	}
}

// The baseline must pay the same costs as the strategy. Comparing a costed
// strategy against a frictionless hold would flatter every result, which is the
// exact failure this feature is supposed to prevent.
func TestBaselinePaysTheSameCosts(t *testing.T) {
	closes := map[string][]float64{
		"AAA": {100, 110, 120, 130},
		"BBB": {100, 105, 110, 115},
	}
	free := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA", "BBB"}, closes, CostConfig{})
	costed := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA", "BBB"}, closes,
		CostConfig{CommissionPerTrade: 25.0})

	if !costed.Baseline.Computed {
		t.Fatal("no baseline computed for the costed run")
	}
	if costed.Baseline.FinalValue >= free.Baseline.FinalValue {
		t.Errorf("costed baseline %v is not below the frictionless one %v — "+
			"the baseline is not paying the strategy's costs",
			costed.Baseline.FinalValue, free.Baseline.FinalValue)
	}
	// Both sides paid, so the comparison itself stays level.
	if math.Abs(costed.Baseline.ExcessFinalValue) > 1e-9 {
		t.Errorf("excess = %v under costs, want 0 for an identical strategy",
			costed.Baseline.ExcessFinalValue)
	}
}

// A single-ticker portfolio has an equal-weight hold of exactly that ticker,
// which is still a useful floor: it says whether the timing added anything over
// simply owning the thing.
func TestBaselineOnASingleTicker(t *testing.T) {
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{"AAA": {100, 150}},
		CostConfig{})

	if !p.Baseline.Computed {
		t.Fatal("no baseline computed for a single-ticker run")
	}
	almost(t, "baseline final value", p.Baseline.FinalValue, exactCash*1.5)
}
