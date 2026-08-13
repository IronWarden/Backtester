package backtest

import (
	"math"
	"testing"
)

// The benchmark-relative metrics are pinned against cases whose answer is
// forced by the definition rather than by the implementation: a portfolio
// compared with itself, a portfolio that is exactly a multiple of its
// benchmark, and a portfolio that ignores it entirely.

var benchSeries = []float64{0.01, -0.02, 0.03, -0.01, 0.02, 0.00, -0.015}

// scaled returns k * series, i.e. a portfolio with beta exactly k and no
// alpha.
func scaled(series []float64, k float64) []float64 {
	out := make([]float64, len(series))
	for i, v := range series {
		out[i] = v * k
	}
	return out
}

// A portfolio benchmarked against itself is the strongest available check:
// every one of these values is forced by the definitions, with no reference
// implementation needed.
func TestBenchmarkMetricsAgainstItself(t *testing.T) {
	b := benchSeries
	closeTo(t, "beta", GetBeta(b, b), 1)
	closeTo(t, "alpha", GetAlpha(b, b), 0)
	closeTo(t, "tracking error", GetTrackingError(b, b), 0)
	closeTo(t, "information ratio", GetInformationRatio(b, b), 0)
	closeTo(t, "up capture", GetUpCapture(b, b), 100)
	closeTo(t, "down capture", GetDownCapture(b, b), 100)
}

func TestBetaScalesWithExposure(t *testing.T) {
	b := benchSeries
	for _, k := range []float64{0.5, 1, 1.5, 2, 3} {
		closeTo(t, "beta", GetBeta(scaled(b, k), b), k)
	}
	// An inverse portfolio has negative beta.
	closeTo(t, "inverse beta", GetBeta(scaled(b, -1), b), -1)

	// A portfolio uncorrelated with its benchmark has no explanatory slope.
	// This series has zero covariance with benchSeries by construction:
	// it is constant, so it cannot co-move with anything.
	flat := make([]float64, len(b))
	closeTo(t, "flat beta", GetBeta(flat, b), 0)
}

// A pure multiple of the benchmark has no alpha, whatever its beta: all of
// its return is explained by exposure.
func TestAlphaIsZeroForPureExposure(t *testing.T) {
	b := benchSeries
	for _, k := range []float64{0.5, 1, 2} {
		closeTo(t, "alpha of pure exposure", GetAlpha(scaled(b, k), b), 0)
	}

	// Adding a constant daily excess produces exactly that, annualized and
	// in percent: 0.001/day * 252 * 100 = 25.2%.
	lifted := make([]float64, len(b))
	for i, v := range b {
		lifted[i] = v + 0.001
	}
	closeTo(t, "lifted alpha", GetAlpha(lifted, b), 25.2)

	// And a constant drag is negative alpha of the same size.
	dragged := make([]float64, len(b))
	for i, v := range b {
		dragged[i] = v - 0.001
	}
	closeTo(t, "dragged alpha", GetAlpha(dragged, b), -25.2)
}

func TestTrackingErrorAndInformationRatio(t *testing.T) {
	b := benchSeries

	// A portfolio that tracks exactly has no active return to disperse.
	closeTo(t, "perfect tracking", GetTrackingError(b, b), 0)
	// With no dispersion there is nothing to divide by; guarded to 0 rather
	// than NaN.
	closeTo(t, "IR of a perfect tracker", GetInformationRatio(b, b), 0)

	// A constant daily outperformance is pure signal, no noise: the active
	// series has zero standard deviation, so tracking error is 0 and the
	// information ratio is guarded to 0 rather than infinite.
	lifted := make([]float64, len(b))
	for i, v := range b {
		lifted[i] = v + 0.001
	}
	closeTo(t, "TE of a constant lift", GetTrackingError(lifted, b), 0)
	closeTo(t, "IR of a constant lift", GetInformationRatio(lifted, b), 0)

	// A portfolio at 2x its benchmark has active return equal to the
	// benchmark itself, so tracking error is the benchmark's own annualized
	// stdev and the IR matches its Sharpe-like ratio.
	doubled := scaled(b, 2)
	active := activeReturns(doubled, b)
	for i := range active {
		if math.Abs(active[i]-b[i]) > 1e-12 {
			t.Fatalf("active[%d] = %v, want the benchmark itself", i, active[i])
		}
	}
	if GetTrackingError(doubled, b) <= 0 {
		t.Error("a 2x portfolio must have positive tracking error")
	}
	// The sign of the IR follows the sign of the mean active return.
	if mean := meanOf(active); mean > 0 && GetInformationRatio(doubled, b) <= 0 {
		t.Error("positive mean active return must give a positive IR")
	}
}

func meanOf(xs []float64) float64 {
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

func TestCaptureRatios(t *testing.T) {
	b := []float64{0.02, -0.01, 0.04, -0.03}

	// Half the benchmark in both directions.
	half := scaled(b, 0.5)
	closeTo(t, "up capture at half", GetUpCapture(half, b), 50)
	closeTo(t, "down capture at half", GetDownCapture(half, b), 50)

	// A defensive portfolio: full participation up, none down. Up days are
	// indices 0 and 2, down days 1 and 3.
	defensive := []float64{0.02, 0, 0.04, 0}
	closeTo(t, "defensive up capture", GetUpCapture(defensive, b), 100)
	closeTo(t, "defensive down capture", GetDownCapture(defensive, b), 0)

	// A leveraged portfolio captures more of both.
	closeTo(t, "leveraged up capture", GetUpCapture(scaled(b, 2), b), 200)
	closeTo(t, "leveraged down capture", GetDownCapture(scaled(b, 2), b), 200)

	// Up-capture ignores down days entirely: changing them must not move it.
	altered := []float64{0.02, -99, 0.04, -99}
	closeTo(t, "up capture ignores down days", GetUpCapture(altered, b), 100)

	// A benchmark that only rises has no down days to capture.
	onlyUp := []float64{0.01, 0.02, 0.03}
	closeTo(t, "no down days", GetDownCapture(onlyUp, onlyUp), 0)
	// ... and one that only falls has no up days.
	onlyDown := []float64{-0.01, -0.02, -0.03}
	closeTo(t, "no up days", GetUpCapture(onlyDown, onlyDown), 0)
}

// Exactly-zero benchmark days count as neither up nor down, so a benchmark
// that never moves has nothing to capture in either direction.
func TestCaptureIgnoresFlatDays(t *testing.T) {
	flat := []float64{0, 0, 0, 0}
	p := []float64{0.01, -0.01, 0.02, 0.03}
	closeTo(t, "up capture of a flat benchmark", GetUpCapture(p, flat), 0)
	closeTo(t, "down capture of a flat benchmark", GetDownCapture(p, flat), 0)
}

// Misaligned or too-short inputs are a programming error upstream, but they
// must degrade to zeros rather than panicking or emitting garbage.
func TestBenchmarkMetricsDegenerateInputs(t *testing.T) {
	cases := []struct {
		name string
		p, b []float64
	}{
		{"both nil", nil, nil},
		{"both empty", []float64{}, []float64{}},
		{"single sample", []float64{0.01}, []float64{0.01}},
		{"portfolio shorter", []float64{0.01}, []float64{0.01, 0.02}},
		{"benchmark shorter", []float64{0.01, 0.02}, []float64{0.01}},
		{"portfolio nil", nil, []float64{0.01, 0.02}},
		{"benchmark nil", []float64{0.01, 0.02}, nil},
		{"zero-variance benchmark", []float64{0.01, 0.02}, []float64{0.01, 0.01}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for name, got := range map[string]float64{
				"Beta":             GetBeta(tc.p, tc.b),
				"Alpha":            GetAlpha(tc.p, tc.b),
				"TrackingError":    GetTrackingError(tc.p, tc.b),
				"InformationRatio": GetInformationRatio(tc.p, tc.b),
				"UpCapture":        GetUpCapture(tc.p, tc.b),
				"DownCapture":      GetDownCapture(tc.p, tc.b),
			} {
				if math.IsNaN(got) || math.IsInf(got, 0) {
					t.Errorf("%s = %v — must be finite", name, got)
				}
				if name == "Beta" && got != 0 {
					t.Errorf("Beta = %v, want 0 for degenerate input", got)
				}
			}
		})
	}
}

// The same NaN/Inf sweep the other metrics get, over adversarial values.
func TestBenchmarkMetricsNeverNaN(t *testing.T) {
	inputs := [][]float64{
		{0, 0, 0},
		{1e-18, -1e-18, 0},
		{math.SmallestNonzeroFloat64, math.SmallestNonzeroFloat64},
		{math.MaxFloat64, -math.MaxFloat64},
		{1e308, 1e308, -1e308},
	}
	for _, p := range inputs {
		for _, b := range inputs {
			if len(p) != len(b) {
				continue
			}
			for name, got := range map[string]float64{
				"Beta":             GetBeta(p, b),
				"Alpha":            GetAlpha(p, b),
				"TrackingError":    GetTrackingError(p, b),
				"InformationRatio": GetInformationRatio(p, b),
				"UpCapture":        GetUpCapture(p, b),
				"DownCapture":      GetDownCapture(p, b),
			} {
				if math.IsNaN(got) || math.IsInf(got, 0) {
					t.Errorf("%s(%v, %v) = %v — must be finite", name, p, b, got)
				}
			}
		}
	}
}
