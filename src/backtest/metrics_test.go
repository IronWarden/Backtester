package backtest

import (
	"math"
	"math/rand"
	"testing"

	"gonum.org/v1/gonum/stat"
)

// These four functions produce every number in the results table, so they
// are pinned against hand-computed values rather than golden output: a
// refactor that changes an annualization factor, a sign, or a denominator
// convention should fail here rather than silently shift users' reported
// performance.

func closeTo(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-6 {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func TestGetMaxDrawdown(t *testing.T) {
	cases := []struct {
		name string
		in   []float64
		want float64
	}{
		{"empty", nil, 0},
		{"single value", []float64{100}, 0},
		{"monotonic rise", []float64{100, 110, 120}, 0},
		// Peak 100 -> trough 50 = 50%.
		{"halved", []float64{100, 50}, 50},
		// Peak 200 (after recovery) -> 150 is 25%; the earlier 100->80 dip
		// is 20%, so the later, deeper drawdown wins.
		{"deepest of several", []float64{100, 80, 200, 150}, 25},
		// Recovery after the trough must not erase the recorded drawdown.
		{"recovered", []float64{100, 50, 100}, 50},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			closeTo(t, "GetMaxDrawdown", GetMaxDrawdown(tc.in), tc.want)
		})
	}
}

func TestGetAnnualReturn(t *testing.T) {
	// Two years of total +21% growth -> 10% CAGR (1.1^2 = 1.21).
	got := GetAnnualReturn([]float64{0.1, 0.1}, 2)
	closeTo(t, "CAGR of 1.1^2 over 2y", got, 10)

	// One year, single +50% day -> 50%.
	closeTo(t, "one-year", GetAnnualReturn([]float64{0.5}, 1), 50)

	// A flat series returns 0% regardless of length.
	closeTo(t, "flat", GetAnnualReturn([]float64{0, 0, 0}, 3), 0)

	// Guard against divide-by-zero / negative elapsed time.
	closeTo(t, "zero years", GetAnnualReturn([]float64{0.1}, 0), 0)
	closeTo(t, "negative years", GetAnnualReturn([]float64{0.1}, -1), 0)

	// Losses compound too: 0.5 * 0.5 = 0.25 over 2y -> -50% CAGR.
	closeTo(t, "losses", GetAnnualReturn([]float64{-0.5, -0.5}, 2), -50)
}

func TestGetSharpeRatio(t *testing.T) {
	// Zero dispersion must return 0, not NaN — this is the guard that keeps
	// a constant-return portfolio from poisoning the results table.
	if got := GetSharpeRatio([]float64{0.01, 0.01, 0.01}); got != 0 {
		t.Errorf("constant series Sharpe = %v, want 0", got)
	}
	// Fewer than two samples makes the sample stdev NaN; the guard must
	// convert that to 0 rather than leaking NaN into the results table.
	for _, in := range [][]float64{nil, {}, {0.01}} {
		if got := GetSharpeRatio(in); got != 0 {
			t.Errorf("GetSharpeRatio(%v) = %v, want 0 (guarded)", in, got)
		}
	}

	// mean 0.01, sample stdev 0.01 -> 1.0 * sqrt(252).
	got := GetSharpeRatio([]float64{0.02, 0.0, 0.02, 0.0})
	want := 0.01 / 0.011547005383792516 * math.Sqrt(252)
	if math.Abs(got-want) > 1e-6 {
		t.Errorf("Sharpe = %v, want %v", got, want)
	}
	if got <= 0 {
		t.Errorf("positive-mean series should have positive Sharpe, got %v", got)
	}

	// Sign follows the mean excess return.
	if got := GetSharpeRatio([]float64{-0.02, 0.0, -0.02, 0.0}); got >= 0 {
		t.Errorf("negative-mean series Sharpe = %v, want < 0", got)
	}
}

func TestGetSortinoRatio(t *testing.T) {
	if got := GetSortinoRatio(nil); got != 0 {
		t.Errorf("empty Sortino = %v, want 0", got)
	}
	// No negative periods -> no downside deviation -> guarded 0.
	if got := GetSortinoRatio([]float64{0.01, 0.02}); got != 0 {
		t.Errorf("all-positive Sortino = %v, want 0 (no downside)", got)
	}

	// Downside deviation is RMS over ALL periods (positives contribute 0),
	// not the stdev of only the negative days. For [0.02, -0.01]:
	//   mean = 0.005, downside = sqrt(0.0001/2) = 0.0070710678
	in := []float64{0.02, -0.01}
	want := 0.005 / math.Sqrt(0.0001/2) * math.Sqrt(252)
	if got := GetSortinoRatio(in); math.Abs(got-want) > 1e-6 {
		t.Errorf("Sortino = %v, want %v", got, want)
	}

	// Sortino ignores upside volatility, so it must exceed Sharpe on a
	// series whose big moves are gains.
	series := []float64{0.05, 0.05, -0.01, 0.05}
	if GetSortinoRatio(series) <= GetSharpeRatio(series) {
		t.Errorf("Sortino (%v) should exceed Sharpe (%v) when volatility is upside",
			GetSortinoRatio(series), GetSharpeRatio(series))
	}
}

func TestGetTurnover(t *testing.T) {
	// A portfolio worth 1000 every day for 252 days that traded 1000 of
	// notional turned over exactly 1x annually.
	year := make([]float64, 252)
	for i := range year {
		year[i] = 1000
	}
	closeTo(t, "one full turn", GetTurnover(1000, year), 1)
	closeTo(t, "two full turns", GetTurnover(2000, year), 2)

	// Half a year at the same trading pace annualizes to the same rate:
	// 500 traded over 126 days of a 1000 portfolio is still 1x.
	closeTo(t, "annualized from a half year", GetTurnover(500, year[:126]), 1)

	// Turnover measures against average value, not starting value.
	// avg(1000, 3000) = 2000; 1000 traded over 2 days -> 0.5 * 252/2 = 63.
	closeTo(t, "average value", GetTurnover(1000, []float64{1000, 3000}), 63)

	// Nothing traded, nothing to report.
	closeTo(t, "no trades", GetTurnover(0, year), 0)

	// Guards: no series, and a portfolio that went to zero.
	for _, tc := range []struct {
		name     string
		notional float64
		closes   []float64
	}{
		{"nil series", 100, nil},
		{"empty series", 100, []float64{}},
		{"zero-valued portfolio", 100, []float64{0, 0}},
		{"negative average", 100, []float64{-100, -100}},
		{"negative notional", -100, year},
	} {
		if got := GetTurnover(tc.notional, tc.closes); got != 0 {
			t.Errorf("%s: GetTurnover = %v, want 0", tc.name, got)
		}
	}
}

// No metric may emit NaN or Inf into the results table, whatever the input.
func TestMetricsNeverNaN(t *testing.T) {
	inputs := [][]float64{
		nil,
		{},
		{0},
		{0, 0, 0},
		{1e-18, -1e-18},
		{math.SmallestNonzeroFloat64},
	}
	for _, in := range inputs {
		for name, got := range map[string]float64{
			"Sharpe":   GetSharpeRatio(in),
			"Sortino":  GetSortinoRatio(in),
			"MaxDD":    GetMaxDrawdown(in),
			"Annual":   GetAnnualReturn(in, 1),
			"Turnover": GetTurnover(1000, in),
		} {
			if math.IsNaN(got) || math.IsInf(got, 0) {
				t.Errorf("%s(%v) = %v — must be finite", name, in, got)
			}
		}
	}
}

// --- overfitting statistics -------------------------------------------------

// The bar a swept winner must clear rises with the size of the search: the
// best of 1000 coin-flippers looks better than the best of 10.
func TestExpectedMaxSharpeRisesWithTheSearch(t *testing.T) {
	const spread = 0.5
	prev := ExpectedMaxSharpe(2, spread)
	for _, trials := range []int{5, 10, 50, 200, 1000} {
		got := ExpectedMaxSharpe(trials, spread)
		if got <= prev {
			t.Errorf("bar at %d trials (%.4f) did not exceed the previous (%.4f)",
				trials, got, prev)
		}
		prev = got
	}
	// A search of one has nothing to correct for, and neither does a set of
	// candidates that all behaved identically.
	if got := ExpectedMaxSharpe(1, spread); got != 0 {
		t.Errorf("one trial gave a bar of %v, want 0", got)
	}
	if got := ExpectedMaxSharpe(100, 0); got != 0 {
		t.Errorf("zero spread gave a bar of %v, want 0", got)
	}
}

// The correction is a probability, so it must stay in [0,1] whatever it is
// handed, and must fall as the bar rises.
func TestDeflatedSharpeIsAProbabilityThatFallsWithTheBar(t *testing.T) {
	returns := make([]float64, 500)
	rng := rand.New(rand.NewSource(7))
	for i := range returns {
		returns[i] = 0.0006 + rng.NormFloat64()*0.01
	}
	observed := GetSharpeRatio(returns)

	prev := GetDeflatedSharpe(observed, 0, returns)
	if prev < 0 || prev > 1 {
		t.Fatalf("deflated Sharpe %v is not a probability", prev)
	}
	for _, bar := range []float64{0.25, 0.5, 1.0, 2.0, 5.0} {
		got := GetDeflatedSharpe(observed, bar, returns)
		if got < 0 || got > 1 {
			t.Errorf("bar %v gave %v, which is not a probability", bar, got)
		}
		if got > prev {
			t.Errorf("bar %v gave %.6f, higher than the lower bar's %.6f",
				bar, got, prev)
		}
		prev = got
	}
}

// Degenerate inputs must return 0 rather than NaN, like every other metric.
func TestOverfittingStatsNeverNaN(t *testing.T) {
	cases := map[string][]float64{
		"empty":     {},
		"one value": {0.01},
		"all zero":  {0, 0, 0, 0, 0},
		"identical": {0.01, 0.01, 0.01, 0.01},
	}
	for name, returns := range cases {
		got := GetDeflatedSharpe(1.0, 0.5, returns)
		if math.IsNaN(got) || math.IsInf(got, 0) {
			t.Errorf("%s: deflated Sharpe = %v", name, got)
		}
		if got < 0 || got > 1 {
			t.Errorf("%s: deflated Sharpe = %v, outside [0,1]", name, got)
		}
	}
	if got := ExpectedMaxSharpe(10, math.NaN()); math.IsNaN(got) {
		t.Error("ExpectedMaxSharpe propagated a NaN spread")
	}
}

// The test that decides whether the feature does its job: search a pile of
// genuinely edgeless strategies and the winner still looks good on raw
// Sharpe, but must not survive the correction.
func TestNoEdgeSearchDoesNotSurviveDeflation(t *testing.T) {
	const (
		trials = 200
		days   = 504 // two trading years
	)
	rng := rand.New(rand.NewSource(11))

	// Every candidate is a pure random walk with zero expected return, so by
	// construction not one of them has any edge whatsoever.
	sharpes := make([]float64, trials)
	best, bestIdx := math.Inf(-1), 0
	series := make([][]float64, trials)
	for i := range sharpes {
		r := make([]float64, days)
		for d := range r {
			r[d] = rng.NormFloat64() * 0.01
		}
		series[i] = r
		sharpes[i] = GetSharpeRatio(r)
		if sharpes[i] > best {
			best, bestIdx = sharpes[i], i
		}
	}

	// The winner looks like a real strategy on the headline number.
	if best < 0.5 {
		t.Fatalf("best-of-%d Sharpe was only %.2f; the fixture is not "+
			"exercising selection bias", trials, best)
	}

	bar := ExpectedMaxSharpe(trials, stat.StdDev(sharpes, nil))
	deflated := GetDeflatedSharpe(best, bar, series[bestIdx])

	t.Logf("best raw Sharpe %.3f, bar %.3f, deflated %.4f", best, bar, deflated)
	if deflated > 0.5 {
		t.Errorf("a search over %d edgeless strategies produced a deflated "+
			"Sharpe of %.4f; the correction is not discounting selection bias",
			trials, deflated)
	}

	// And the same series, had it been the only thing tried, would not be
	// discounted nearly as hard.
	alone := GetDeflatedSharpe(best, 0, series[bestIdx])
	if alone <= deflated {
		t.Errorf("an untried strategy (%.4f) scored no better than the same "+
			"one found by searching (%.4f)", alone, deflated)
	}
}
