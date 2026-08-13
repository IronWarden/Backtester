package backtest

import (
	"math"
	"math/rand"
	"my-backtester/src/data"
	"testing"
	"time"
)

// bars turns a list of closes into a date-sorted AssetData series. Only the
// Close field matters to correlation.go.
func bars(closes ...float64) []data.AssetData {
	out := make([]data.AssetData, len(closes))
	day := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, c := range closes {
		out[i] = data.AssetData{Date: day.AddDate(0, 0, i), Close: c}
	}
	return out
}

// Three synthetic series with hand-checkable returns: up 10%, down 10%,
// up 20% for A; the same for B at a different price level; the exact
// negative for C.
var (
	seriesA = bars(100, 110, 99, 118.8)
	seriesB = bars(50, 55, 49.5, 59.4)
	seriesC = bars(100, 90, 99, 79.2)
	// Flat: zero variance, so its correlation with anything is NaN.
	seriesFlat = bars(100, 100, 100, 100)
)

func TestClosesOnly(t *testing.T) {
	series := bars(1, 2, 3)
	cases := []struct {
		name string
		n    int
		want []float64
	}{
		{"exact length", 3, []float64{1, 2, 3}},
		{"truncates to n", 2, []float64{1, 2}},
		{"n beyond the series", 10, []float64{1, 2, 3}},
		{"n of one", 1, []float64{1}},
		{"zero", 0, nil},
		{"negative", -1, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := closesOnly(series, tc.n)
			if len(got) != len(tc.want) {
				t.Fatalf("closesOnly(_, %d) = %v, want %v", tc.n, got, tc.want)
			}
			for i := range tc.want {
				closeTo(t, "close", got[i], tc.want[i])
			}
		})
	}
	if got := closesOnly(nil, 5); got != nil {
		t.Errorf("closesOnly(nil, 5) = %v, want nil", got)
	}
}

func TestReturnsFromCloses(t *testing.T) {
	// n-1 returns from n closes.
	got := returnsFromCloses(seriesA, 4)
	want := []float64{0.1, -0.1, 0.2}
	if len(got) != len(want) {
		t.Fatalf("returns = %v, want %v", got, want)
	}
	for i := range want {
		closeTo(t, "return", got[i], want[i])
	}

	// Truncating the window truncates the returns with it.
	if got := returnsFromCloses(seriesA, 2); len(got) != 1 {
		t.Errorf("returnsFromCloses(_, 2) = %v, want one return", got)
	}

	// Fewer than two closes cannot produce a return.
	for _, n := range []int{1, 0, -1} {
		if got := returnsFromCloses(seriesA, n); got != nil {
			t.Errorf("returnsFromCloses(_, %d) = %v, want nil", n, got)
		}
	}
	if got := returnsFromCloses(bars(100), 5); got != nil {
		t.Errorf("single-bar series returned %v, want nil", got)
	}

	// A zero previous close would divide by zero; that day contributes a
	// zero return and the rest of the series is still computed.
	got = returnsFromCloses(bars(0, 10, 20), 3)
	if len(got) != 2 {
		t.Fatalf("returns = %v, want 2", got)
	}
	closeTo(t, "return after a zero close", got[0], 0)
	closeTo(t, "return", got[1], 1.0)
}

func TestAvgPairwiseCorrelation(t *testing.T) {
	hist := map[string][]data.AssetData{
		"A": seriesA, "B": seriesB, "C": seriesC, "FLAT": seriesFlat,
	}

	// Identical return series correlate at exactly 1.
	closeTo(t, "identical", AvgPairwiseCorrelation([]string{"A", "B"}, hist, 4), 1)
	// Mirrored return series correlate at exactly -1.
	closeTo(t, "mirrored", AvgPairwiseCorrelation([]string{"A", "C"}, hist, 4), -1)
	// Three tickers = three pairs: (A,B)=1, (A,C)=-1, (B,C)=-1 -> -1/3.
	closeTo(t, "three tickers",
		AvgPairwiseCorrelation([]string{"A", "B", "C"}, hist, 4), -1.0/3.0)

	// A pair whose correlation is undefined (zero variance) is skipped
	// rather than poisoning the average with NaN.
	got := AvgPairwiseCorrelation([]string{"A", "FLAT"}, hist, 4)
	if math.IsNaN(got) || got != 0 {
		t.Errorf("flat pair = %v, want 0", got)
	}
	// ... and skipping it leaves the remaining pairs' average intact.
	closeTo(t, "flat ignored",
		AvgPairwiseCorrelation([]string{"A", "B", "FLAT"}, hist, 4), 1)
}

func TestAvgPairwiseCorrelationDegenerateInputs(t *testing.T) {
	hist := map[string][]data.AssetData{
		"A":     seriesA,
		"SHORT": bars(100, 110, 99), // 2 returns, enough to be included
		"TINY":  bars(100, 110),     // 1 return, dropped
	}
	cases := []struct {
		name    string
		tickers []string
		dataLen int
	}{
		{"no tickers", nil, 4},
		{"single ticker", []string{"A"}, 4},
		{"unknown ticker", []string{"A", "NOPE"}, 4},
		{"too few bars for a return", []string{"A", "TINY"}, 4},
		{"zero dataLen", []string{"A", "B"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := AvgPairwiseCorrelation(tc.tickers, hist, tc.dataLen)
			if got != 0 {
				t.Errorf("AvgPairwiseCorrelation = %v, want 0", got)
			}
		})
	}

	// Series of different lengths pair on their common prefix rather than
	// being dropped: A and SHORT agree over the first two returns.
	closeTo(t, "mismatched lengths",
		AvgPairwiseCorrelation([]string{"A", "SHORT"}, hist, 4), 1)
}

// randomWalk builds a deterministic price series so the cointegration
// tests below are reproducible.
func randomWalk(r *rand.Rand, n int, start, step float64) []float64 {
	out := make([]float64, n)
	out[0] = start
	for i := 1; i < n; i++ {
		out[i] = out[i-1] + r.NormFloat64()*step
	}
	return out
}

func TestCountCointegratedPairs(t *testing.T) {
	const n = 200
	r := rand.New(rand.NewSource(1))

	base := randomWalk(r, n, 100, 1)
	// Tied to base by a stationary (iid) disturbance -> cointegrated.
	tied := make([]float64, n)
	for i := range base {
		tied[i] = 2*base[i] + 5 + r.NormFloat64()*0.5
	}
	// An independent walk -> two unit roots, no cointegrating relation.
	other := randomWalk(r, n, 80, 1)

	hist := map[string][]data.AssetData{
		"BASE":  bars(base...),
		"TIED":  bars(tied...),
		"OTHER": bars(other...),
	}

	if got := CountCointegratedPairs([]string{"BASE", "TIED"}, hist, n); got != 1 {
		t.Errorf("cointegrated pair count = %d, want 1", got)
	}
	if got := CountCointegratedPairs([]string{"BASE", "OTHER"}, hist, n); got != 0 {
		t.Errorf("independent walks counted as cointegrated: %d", got)
	}
	// Across all three only the one genuine relation is found.
	if got := CountCointegratedPairs([]string{"BASE", "TIED", "OTHER"}, hist, n); got != 1 {
		t.Errorf("three-ticker count = %d, want 1", got)
	}
}

func TestCountCointegratedPairsDegenerateInputs(t *testing.T) {
	const n = 200
	r := rand.New(rand.NewSource(2))
	walk := randomWalk(r, n, 100, 1)

	hist := map[string][]data.AssetData{
		"A":     bars(walk...),
		"B":     bars(walk...), // identical: residuals are all zero
		"SHORT": bars(walk[:29]...),
	}

	cases := []struct {
		name    string
		tickers []string
		dataLen int
	}{
		{"no tickers", nil, n},
		{"single ticker", []string{"A"}, n},
		// The test needs 30 observations; a shorter series is excluded
		// rather than pairing on a too-short window.
		{"series below the 30-bar minimum", []string{"A", "SHORT"}, n},
		{"window below the 30-bar minimum", []string{"A", "B"}, 29},
		// Zero residual variance is degenerate, not evidence — the test
		// must decline rather than divide by zero.
		{"identical series", []string{"A", "B"}, n},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CountCointegratedPairs(tc.tickers, hist, tc.dataLen); got != 0 {
				t.Errorf("CountCointegratedPairs = %d, want 0", got)
			}
		})
	}
}
