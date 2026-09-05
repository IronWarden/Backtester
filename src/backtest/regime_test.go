package backtest

// Regime labels decide how a result is read, so the tests build markets whose
// regime is known by construction: one that only rises calmly, one that crashes,
// and one whose volatility arrives late.

import (
	"math"
	"math/rand"
	"testing"
)

func countRegimes(labels []Regime) map[Regime]int {
	out := make(map[Regime]int)
	for _, l := range labels {
		out[l]++
	}
	return out
}

// A market that rises steadily is never in a bear regime — it is never 10%
// below its own running peak.
func TestSteadyRiseIsNeverBearish(t *testing.T) {
	returns := make([]float64, 400)
	for i := range returns {
		returns[i] = 0.001
	}
	counts := countRegimes(LabelRegimes(returns))

	if counts[RegimeBearCalm]+counts[RegimeBearVolatile] > 0 {
		t.Errorf("a monotonically rising market was labelled bearish: %v", counts)
	}
	if counts[RegimeUnknown] != RegimeWarmup {
		t.Errorf("warm-up covered %d days, want %d",
			counts[RegimeUnknown], RegimeWarmup)
	}
}

// A market that falls 40% must spend real time in a bear regime, and the label
// must appear only AFTER the drawdown crosses the threshold — not before.
func TestACrashIsLabelledBearish(t *testing.T) {
	returns := make([]float64, 400)
	for i := range returns {
		switch {
		case i < 100:
			returns[i] = 0.002 // rise first, to establish a peak
		case i < 160:
			returns[i] = -0.01 // then a long fall
		default:
			returns[i] = 0.0005
		}
	}
	labels := LabelRegimes(returns)
	counts := countRegimes(labels)

	bear := counts[RegimeBearCalm] + counts[RegimeBearVolatile]
	if bear == 0 {
		t.Fatalf("a 40%% drawdown produced no bear days: %v", counts)
	}
	// Nothing before the fall begins can be bearish: the index is at its peak.
	for i := RegimeWarmup; i < 100; i++ {
		if labels[i] == RegimeBearCalm || labels[i] == RegimeBearVolatile {
			t.Fatalf("day %d was labelled %s while the market was at its peak",
				i, labels[i])
		}
	}
}

// The label must never use information from after the day it labels. Truncating
// the series must not change any label that survives the truncation — the
// property that makes these labels safe to condition a strategy on.
func TestLabelsAreCausal(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	returns := make([]float64, 400)
	for i := range returns {
		returns[i] = rng.NormFloat64() * 0.01
	}

	full := LabelRegimes(returns)
	for _, cut := range []int{120, 200, 300} {
		truncated := LabelRegimes(returns[:cut])
		for i := 0; i < cut; i++ {
			if truncated[i] != full[i] {
				t.Fatalf("day %d is %s with %d days of data and %s with all "+
					"400 — the label is reading the future",
					i, truncated[i], cut, full[i])
			}
		}
	}
}

// Slicing must partition the labelled days exactly: every non-warmup day lands
// in exactly one regime, and the shares sum to one.
func TestSliceByRegimePartitionsTheDays(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	market := make([]float64, 500)
	strategy := make([]float64, 500)
	for i := range market {
		market[i] = rng.NormFloat64() * 0.012
		strategy[i] = market[i] * 0.5
	}
	labels := LabelRegimes(market)
	breakdown := SliceByRegime(strategy, labels)

	if !breakdown.Computed {
		t.Fatal("no breakdown computed for a 500-day run")
	}
	var days int
	var share float64
	for _, s := range breakdown.Stats {
		days += s.Days
		share += s.Share
	}
	labelled := len(market) - countRegimes(labels)[RegimeUnknown]
	if days != labelled {
		t.Errorf("regimes cover %d days, want the %d labelled ones",
			days, labelled)
	}
	if math.Abs(share-1.0) > 1e-9 {
		t.Errorf("shares sum to %v, want 1", share)
	}
	// Sorted by day count, so the environment the headline mostly measures is
	// the first row.
	for i := 1; i < len(breakdown.Stats); i++ {
		if breakdown.Stats[i].Days > breakdown.Stats[i-1].Days {
			t.Errorf("regimes are not ordered by day count: %v", breakdown.Stats)
		}
	}
}

// The case the feature exists for: a strategy that makes money in calm markets
// and loses it in volatile ones reports one blended Sharpe, and the breakdown
// must separate them.
func TestBreakdownSeparatesGoodAndBadEnvironments(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	const n = 600
	market := make([]float64, n)
	strategy := make([]float64, n)
	for i := range market {
		volatile := i%200 >= 100 // alternating calm and wild stretches
		if volatile {
			market[i] = rng.NormFloat64() * 0.03
			strategy[i] = -0.002 + rng.NormFloat64()*0.001 // loses when wild
		} else {
			market[i] = rng.NormFloat64() * 0.004
			strategy[i] = 0.002 + rng.NormFloat64()*0.001 // makes money when calm
		}
	}

	breakdown := SliceByRegime(strategy, LabelRegimes(market))
	if !breakdown.Computed {
		t.Fatal("no breakdown computed")
	}

	var calm, wild float64
	var sawCalm, sawWild bool
	for _, s := range breakdown.Stats {
		switch s.Regime {
		case RegimeBullCalm, RegimeBearCalm:
			calm += s.AnnualReturn
			sawCalm = true
		case RegimeBullVolatile, RegimeBearVolatile:
			wild += s.AnnualReturn
			sawWild = true
		}
	}
	if !sawCalm || !sawWild {
		t.Fatalf("both environments should appear: %+v", breakdown.Stats)
	}
	if calm <= wild {
		t.Errorf("calm regimes returned %.2f and volatile %.2f; the strategy "+
			"was built to make money only in calm markets, so the breakdown "+
			"is not separating them", calm, wild)
	}
}

// Too little history means no labels and no breakdown, rather than a confident
// slice of thirty days.
func TestShortRunsProduceNoBreakdown(t *testing.T) {
	short := make([]float64, RegimeWarmup-1)
	labels := LabelRegimes(short)
	for i, l := range labels {
		if l != RegimeUnknown {
			t.Errorf("day %d labelled %s before the warm-up finished", i, l)
		}
	}
	if got := SliceByRegime(short, labels); got.Computed {
		t.Errorf("a %d-day run produced a breakdown: %+v", len(short), got.Stats)
	}
}

// End to end through a real run.
func TestRegimesDuringARun(t *testing.T) {
	closes := make([]float64, 400)
	px := 100.0
	for i := range closes {
		if i > 150 && i < 220 {
			px *= 0.99
		} else {
			px *= 1.002
		}
		closes[i] = px
	}
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{"AAA": closes}, CostConfig{})

	if !p.Regimes.Computed {
		t.Fatal("no regime breakdown for a 400-day run")
	}
	var bear bool
	for _, s := range p.Regimes.Stats {
		if s.Regime == RegimeBearCalm || s.Regime == RegimeBearVolatile {
			bear = true
		}
	}
	if !bear {
		t.Errorf("a run containing a 50%% drawdown has no bear regime: %+v",
			p.Regimes.Stats)
	}
}

// Drawdown is measured from the RUNNING PEAK, not from the start of the window.
// The distinguishing case is a correction inside a bull market: the index is
// still far above where it began, and is nonetheless 15% off its high. A version
// measuring from the start would call that a bull, which is exactly backwards
// for a strategy being asked how it behaves in a selloff.
func TestDrawdownIsMeasuredFromTheRunningPeak(t *testing.T) {
	returns := make([]float64, 400)
	for i := range returns {
		switch {
		case i < 200:
			returns[i] = 0.0035 // roughly doubles
		case i < 240:
			returns[i] = -0.005 // then gives back ~18% from the high
		default:
			returns[i] = 0.0
		}
	}
	labels := LabelRegimes(returns)

	// After the selloff the index is still well above its starting value, so
	// only a running-peak drawdown can see the correction.
	var bearAfterSelloff int
	for i := 235; i < 260; i++ {
		if labels[i] == RegimeBearCalm || labels[i] == RegimeBearVolatile {
			bearAfterSelloff++
		}
	}
	if bearAfterSelloff == 0 {
		t.Errorf("an 18%% fall from the high was not labelled bearish while "+
			"the index was still up on the window — drawdown is being "+
			"measured from the start rather than from the running peak: %v",
			labels[235:260])
	}
}

// The expanding-median comparison LabelRegimes used to make by sorting a copy
// of every volatility seen so far. Kept here as the reference the streaming
// median has to agree with exactly: the point of runningMedian is that it is
// faster, not that it is different.
func labelRegimesBySorting(returns []float64) []Regime {
	labels := make([]Regime, len(returns))
	if len(returns) == 0 {
		return labels
	}
	index, peak := 1.0, 1.0
	var vols []float64
	for i, r := range returns {
		index *= 1 + r
		if index > peak {
			peak = index
		}
		labels[i] = RegimeUnknown
		if i < RegimeWarmup {
			continue
		}
		drawdown := index/peak - 1
		vol, ok := sampleStdev(returns[i-RegimeVolWindow+1 : i+1])
		if !ok {
			continue
		}
		vols = append(vols, vol)
		volatile := vol > median(vols)
		switch {
		case drawdown <= BearDrawdown && volatile:
			labels[i] = RegimeBearVolatile
		case drawdown <= BearDrawdown:
			labels[i] = RegimeBearCalm
		case volatile:
			labels[i] = RegimeBullVolatile
		default:
			labels[i] = RegimeBullCalm
		}
	}
	return labels
}

// LabelRegimes sorted the whole volatility history once per day, which made
// every backtest quadratic in its own length -- 80% of the per-portfolio cost
// at ten years, seconds at forty. The streaming median removed that, and this
// is what says it removed nothing else: same labels, day for day, on markets
// with enough structure to exercise every branch.
func TestStreamingMedianLabelsIdenticallyToSorting(t *testing.T) {
	rng := rand.New(rand.NewSource(20260905))
	for _, days := range []int{0, 1, RegimeWarmup, RegimeWarmup + 1, 500, 3000} {
		returns := make([]float64, days)
		vol := 0.008
		for i := range returns {
			// Volatility that drifts and occasionally jumps, so the median
			// comparison flips both ways rather than settling.
			if i%137 == 0 {
				vol = 0.004 + rng.Float64()*0.02
			}
			returns[i] = rng.NormFloat64()*vol + 0.0002
		}
		got := LabelRegimes(returns)
		want := labelRegimesBySorting(returns)
		if len(got) != len(want) {
			t.Fatalf("%d days: got %d labels, want %d", days, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%d days: label %d is %q, the sorting version says %q",
					days, i, got[i], want[i])
			}
		}
	}
}

// The median itself, against the definition rather than against a label: an
// even count averages the two middle values, an odd count takes the middle,
// and an empty set is 0 -- the same answers median() gives.
func TestRunningMedianMatchesTheSortingMedian(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	var m runningMedian
	if m.median() != 0 {
		t.Fatalf("empty running median is %v, want 0", m.median())
	}
	var seen []float64
	for i := 0; i < 400; i++ {
		v := rng.NormFloat64()
		seen = append(seen, v)
		m.push(v)
		if got, want := m.median(), median(seen); got != want {
			t.Fatalf("after %d values: running median %v, sorting median %v",
				i+1, got, want)
		}
	}
}

// Labelling ten years of daily returns. The sorting version took ~120ms here
// and grew with the SQUARE of the run's length; this should stay in the
// microseconds and grow with it linearly, so a regression is visible as an
// order of magnitude rather than as a percentage.
func BenchmarkLabelRegimes(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	returns := make([]float64, 2517)
	for i := range returns {
		returns[i] = rng.NormFloat64() * 0.01
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LabelRegimes(returns)
	}
}
