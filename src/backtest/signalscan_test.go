package backtest

// The signal screen is a machine for deciding what to spend an afternoon on, so
// the thing that matters is that it cannot flatter a worthless signal. Every
// case here is built so the right answer is known before the code runs: a signal
// that IS the forward return must score 1.0, a random one must score nothing,
// and a signal must be structurally unable to see the future it is scored
// against.

import (
	"math"
	"math/rand"
	"my-backtester/src/data"
	"testing"
)

// scanSeries lays n bars of the given closes on consecutive days, reusing the
// exactEpoch convention from correctness_test.go.
func scanHist(closes map[string][]float64) map[string][]data.AssetData {
	return histFrom(closes)
}

// perfectSignal reads the forward return it is about to be scored against —
// which no honest signal can do, and which is exactly why it is the calibration
// case: the scanner must report an IC of 1.0 for it.
func perfectSignal(h int) Signal {
	return Signal{
		Name: "oracle",
		Compute: func(bars []data.AssetData, i int) (float64, bool) {
			return forwardReturn(bars, i, h)
		},
	}
}

// A universe of `n` tickers whose paths differ, so each day has a real
// cross-section to rank rather than a set of identical values.
func spreadUniverse(t *testing.T, n, days int, seed int64) (
	map[string][]data.AssetData, []string,
) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	closes := make(map[string][]float64, n)
	tickers := make([]string, 0, n)
	for k := 0; k < n; k++ {
		name := string(rune('A'+k/26)) + string(rune('A'+k%26))
		tickers = append(tickers, name)
		path := make([]float64, days)
		price := 100.0
		for d := 0; d < days; d++ {
			price *= 1 + (rng.Float64()-0.5)*0.04
			path[d] = price
		}
		closes[name] = path
	}
	return scanHist(closes), tickers
}

// The calibration case. A signal equal to the forward return ranks the
// cross-section perfectly every day, so the mean IC is exactly 1, the hit rate
// is 100%, and the quintile spread is positive.
func TestOracleSignalScoresPerfectIC(t *testing.T) {
	hist, tickers := spreadUniverse(t, 12, 400, 1)
	const h = 21

	scores := ScanSignals(hist, tickers, []Signal{perfectSignal(h)}, []int{h})
	if len(scores) != 1 {
		t.Fatalf("got %d scores, want 1", len(scores))
	}
	s := scores[0]
	if math.Abs(s.MeanIC-1.0) > 1e-9 {
		t.Errorf("mean IC = %v, want 1.0", s.MeanIC)
	}
	if s.HitRate != 1.0 {
		t.Errorf("hit rate = %v, want 1.0", s.HitRate)
	}
	if !s.QuintileOK || s.QuintileSpread <= 0 {
		t.Errorf("quintile spread = %v (ok=%v), want a positive spread",
			s.QuintileSpread, s.QuintileOK)
	}
	// StdErr is zero when every IC is identical, and a zero denominator must
	// not produce an infinite or NaN t-statistic.
	if math.IsInf(s.TStat, 0) || math.IsNaN(s.TStat) {
		t.Errorf("t-statistic = %v on a zero-variance IC series", s.TStat)
	}
}

// The mirror image: negating the oracle must score exactly -1. This is what
// pins the SIGN convention, which is the easiest thing to get backwards and the
// most consequential — a sign error turns "sell these" into "buy these".
func TestNegatedOracleScoresMinusOne(t *testing.T) {
	hist, tickers := spreadUniverse(t, 12, 400, 2)
	const h = 21
	negated := Signal{
		Name: "anti-oracle",
		Compute: func(bars []data.AssetData, i int) (float64, bool) {
			r, ok := forwardReturn(bars, i, h)
			return -r, ok
		},
	}

	scores := ScanSignals(hist, tickers, []Signal{negated}, []int{h})
	if math.Abs(scores[0].MeanIC+1.0) > 1e-9 {
		t.Errorf("mean IC = %v, want -1.0", scores[0].MeanIC)
	}
	if scores[0].QuintileSpread >= 0 {
		t.Errorf("quintile spread = %v, want negative", scores[0].QuintileSpread)
	}
}

// A seeded random signal must come back with nothing to report. If this ever
// fails the screen is manufacturing signal, which is the only failure mode that
// would make the whole feature worse than not having it.
func TestRandomSignalScoresNothing(t *testing.T) {
	hist, tickers := spreadUniverse(t, 20, 1200, 3)
	rng := rand.New(rand.NewSource(99))
	noise := Signal{
		Name: "noise",
		Compute: func(bars []data.AssetData, i int) (float64, bool) {
			return rng.Float64(), true
		},
	}

	scores := ScanSignals(hist, tickers, []Signal{noise}, []int{21})
	if len(scores) != 1 {
		t.Fatalf("got %d scores, want 1", len(scores))
	}
	s := scores[0]
	if math.Abs(s.MeanIC) > 0.15 {
		t.Errorf("mean IC = %v for a random signal, want near zero", s.MeanIC)
	}
	if math.Abs(s.TStat) > 2.5 {
		t.Errorf("t-statistic = %v for a random signal, want |t| < 2.5", s.TStat)
	}
}

// The look-ahead guard, and the reason Signal takes an index rather than a
// window: a signal must never be handed a bar past the day being scored. This
// is structural, so it is worth asserting structurally.
func TestSignalNeverSeesPastTheScoredDay(t *testing.T) {
	hist, tickers := spreadUniverse(t, 8, 300, 4)
	const h = 21

	// Records the highest index the scanner asked about, and what it was asked
	// to score it as.
	maxIndex := -1
	watcher := Signal{
		Name: "watcher",
		Compute: func(bars []data.AssetData, i int) (float64, bool) {
			if i > maxIndex {
				maxIndex = i
			}
			// Reading past i would be the bug; assert the slice we were given
			// still extends there, so the guard is about discipline rather than
			// about the scanner truncating the data.
			if i >= len(bars) {
				t.Errorf("scored index %d beyond %d bars", i, len(bars))
			}
			return bars[i].Close, true
		},
	}

	scores := ScanSignals(hist, tickers, []Signal{watcher}, []int{h})
	if len(scores) != 1 {
		t.Fatalf("got %d scores, want 1", len(scores))
	}
	// The last day that can be scored has a full forward window after it, so no
	// index within h of the end may ever be handed to a signal.
	days := len(hist[tickers[0]])
	if maxIndex >= days-h {
		t.Errorf("signal saw index %d; the last scoreable day is %d "+
			"(days=%d, horizon=%d)", maxIndex, days-h-1, days, h)
	}
}

// Non-overlapping sampling, which is the difference between an honest
// t-statistic and one inflated by sqrt(h). 300 days at a 21-day horizon is
// ~14 samples, not ~279.
func TestSamplingIsNonOverlapping(t *testing.T) {
	hist, tickers := spreadUniverse(t, 8, 300, 5)
	const h = 21

	scores := ScanSignals(hist, tickers, []Signal{perfectSignal(h)}, []int{h})
	got := scores[0].Samples
	want := 0
	for day := 0; day+h < 300; day += h {
		want++
	}
	if got != want {
		t.Errorf("scored %d samples, want %d — overlapping windows reuse each "+
			"day's return %d times and inflate the t-statistic", got, want, h)
	}
	if got > 20 {
		t.Errorf("scored %d samples over 300 days at a %d-day horizon; that is "+
			"daily sampling, not non-overlapping", got, h)
	}
}

// Too small a cross-section must be refused rather than ranked. Three tickers
// produce a rank correlation that is arithmetically valid and completely
// meaningless.
func TestTooSmallACrossSectionIsRefused(t *testing.T) {
	hist, tickers := spreadUniverse(t, 3, 400, 6)
	scores := ScanSignals(hist, tickers, []Signal{perfectSignal(21)}, []int{21})
	if len(scores) != 0 {
		t.Errorf("scored a 3-ticker universe: %+v", scores)
	}

	// And at the threshold it is accepted, so the limit is a boundary rather
	// than a blanket refusal.
	hist, tickers = spreadUniverse(t, MinCrossSection, 400, 7)
	scores = ScanSignals(hist, tickers, []Signal{perfectSignal(21)}, []int{21})
	if len(scores) != 1 {
		t.Fatalf("a %d-ticker universe was refused", MinCrossSection)
	}
	// Below MinQuintileUniverse the spread must be withheld, not approximated.
	if scores[0].QuintileOK {
		t.Errorf("reported a quintile spread for %d tickers", MinCrossSection)
	}
}

// A signal that is undefined everywhere yields no score, and one that is
// constant yields no IC — a constant series has no ordering, and correlating its
// ranks would divide by zero and report NaN.
func TestUndefinedAndConstantSignals(t *testing.T) {
	hist, tickers := spreadUniverse(t, 12, 400, 8)

	undefined := Signal{Name: "undefined",
		Compute: func([]data.AssetData, int) (float64, bool) { return 0, false }}
	if got := ScanSignals(hist, tickers, []Signal{undefined}, []int{21}); len(got) != 0 {
		t.Errorf("an always-undefined signal produced %+v", got)
	}

	constant := Signal{Name: "constant",
		Compute: func([]data.AssetData, int) (float64, bool) { return 1.0, true }}
	got := ScanSignals(hist, tickers, []Signal{constant}, []int{21})
	if len(got) != 0 {
		t.Errorf("a constant signal produced a score: %+v", got)
	}
}

func TestSpearman(t *testing.T) {
	cases := []struct {
		name string
		a, b []float64
		want float64
		ok   bool
	}{
		{"perfect agreement", []float64{1, 2, 3, 4}, []float64{10, 20, 30, 40}, 1, true},
		{"perfect disagreement", []float64{1, 2, 3, 4}, []float64{40, 30, 20, 10}, -1, true},
		// Monotone but not linear: Spearman sees 1, Pearson would not, which is
		// the reason for using ranks at all.
		{"monotone non-linear", []float64{1, 2, 3, 4}, []float64{1, 4, 9, 1000}, 1, true},
		// One tied pair in a otherwise perfect ordering: the average rank keeps
		// the correlation just below 1 rather than breaking it.
		{"a tie", []float64{1, 2, 2, 4}, []float64{1, 2, 3, 4}, 0.9486832980505138, true},
		{"constant left", []float64{5, 5, 5}, []float64{1, 2, 3}, 0, false},
		{"constant right", []float64{1, 2, 3}, []float64{5, 5, 5}, 0, false},
		{"too short", []float64{1}, []float64{1}, 0, false},
		{"mismatched lengths", []float64{1, 2}, []float64{1}, 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := spearman(tc.a, tc.b)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if ok && math.Abs(got-tc.want) > 1e-12 {
				t.Errorf("spearman = %v, want %v", got, tc.want)
			}
		})
	}
}

// The built-in library is only useful if each signal is actually computable and
// signed the way the report claims. A momentum signal on a monotonically rising
// path must be positive; a volatility signal on a calm path must beat a wild one
// (it is negated, so calm scores higher).
func TestBuiltinSignalsAreSignedAsDocumented(t *testing.T) {
	rising := seriesFrom(rampCloses(400, 1.0))
	byName := map[string]Signal{}
	for _, s := range BuiltinSignals() {
		byName[s.Name] = s
	}

	for _, name := range []string{"mom_21", "mom_63", "mom_126", "mom_252",
		"mom_12_1", "sma_50_200", "vol_scaled_mom"} {
		sig, ok := byName[name]
		if !ok {
			t.Fatalf("%s missing from the library", name)
		}
		v, ok := sig.Compute(rising, 399)
		if !ok {
			t.Errorf("%s undefined at day 399 of a 400-day series", name)
			continue
		}
		if v <= 0 {
			t.Errorf("%s = %v on a monotonically rising path, want positive",
				name, v)
		}
	}

	// rev_5 is negated, so a rising path must score negative: it says "this has
	// run up, expect it to give some back".
	if v, ok := byName["rev_5"].Compute(rising, 399); !ok || v >= 0 {
		t.Errorf("rev_5 = %v (ok=%v) on a rising path, want negative", v, ok)
	}

	// Low volatility is negated too: a calm series must score ABOVE a wild one.
	calm := seriesFrom(rampCloses(400, 0.01))
	wild := make([]data.AssetData, len(calm))
	copy(wild, calm)
	for i := range wild {
		if i%2 == 0 {
			wild[i].Close *= 1.15
		}
	}
	calmScore, ok1 := byName["lowvol_21"].Compute(calm, 399)
	wildScore, ok2 := byName["lowvol_21"].Compute(wild, 399)
	if !ok1 || !ok2 {
		t.Fatalf("lowvol_21 undefined: calm=%v wild=%v", ok1, ok2)
	}
	if calmScore <= wildScore {
		t.Errorf("lowvol_21: calm %v is not above wild %v — the sign is "+
			"inverted, so the report would read the low-volatility anomaly "+
			"backwards", calmScore, wildScore)
	}
}

// Every built-in must be defined somewhere on a long enough series, or it is
// dead weight in the report — and must be undefined early on, or it is reading
// history it does not have.
func TestBuiltinSignalsRespectTheirWarmup(t *testing.T) {
	bars := seriesFrom(rampCloses(400, 0.5))
	for _, sig := range BuiltinSignals() {
		if _, ok := sig.Compute(bars, 399); !ok {
			t.Errorf("%s is undefined even at day 399", sig.Name)
		}
		if _, ok := sig.Compute(bars, 0); ok {
			t.Errorf("%s produced a value on day 0 with no history", sig.Name)
		}
		if sig.Description == "" {
			t.Errorf("%s has no description; the report would print a bare name",
				sig.Name)
		}
	}
}

// Results are ordered by |t|, because the user's question is "which of these is
// worth my afternoon" and the answer should be the first row.
func TestScoresAreSortedByStrength(t *testing.T) {
	hist, tickers := spreadUniverse(t, 15, 800, 9)
	rng := rand.New(rand.NewSource(11))
	signals := []Signal{
		{Name: "noise", Compute: func([]data.AssetData, int) (float64, bool) {
			return rng.Float64(), true
		}},
		perfectSignal(21),
	}

	scores := ScanSignals(hist, tickers, signals, []int{21})
	if len(scores) != 2 {
		t.Fatalf("got %d scores, want 2", len(scores))
	}
	if scores[0].Signal != "oracle" {
		t.Errorf("first row is %q, want the oracle — results are not sorted "+
			"by strength", scores[0].Signal)
	}
}

// A window too short for the horizon must produce nothing rather than a summary
// built from three samples. 80 days at a 21-day horizon is three
// non-overlapping windows, and a mean IC over three days is an anecdote with a
// t-statistic attached — the most dangerous shape a number can have here.
func TestTooFewSamplesIsRefused(t *testing.T) {
	hist, tickers := spreadUniverse(t, 12, 80, 12)
	if got := ScanSignals(hist, tickers, []Signal{perfectSignal(21)}, []int{21}); len(got) != 0 {
		t.Errorf("scored %d results from an 80-day window at a 21-day "+
			"horizon: %+v", len(got), got)
	}

	// Just past the threshold it is accepted, so this is a boundary and not a
	// blanket refusal: MinSamples windows of 21 days plus the forward window.
	days := (MinSamples + 1) * 21
	hist, tickers = spreadUniverse(t, 12, days, 13)
	got := ScanSignals(hist, tickers, []Signal{perfectSignal(21)}, []int{21})
	if len(got) != 1 {
		t.Fatalf("a %d-day window produced %d results, want 1", days, len(got))
	}
	if got[0].Samples < MinSamples {
		t.Errorf("reported %d samples, below the %d minimum",
			got[0].Samples, MinSamples)
	}
}
