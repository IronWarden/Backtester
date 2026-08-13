package backtest

import (
	"bytes"
	"log"
	"math"
	"my-backtester/src/data"
	"testing"
	"time"
)

// End-to-end checks for the benchmark wiring, driven through the same
// runOne the worker pool calls.

// benchPortfolio builds a single-ticker buy-and-hold portfolio, optionally
// benchmarked. "greedy" puts the entire balance into the one ticker on day
// 0, so the portfolio's daily returns are that ticker's price returns
// exactly — which is what makes the self-benchmark case exact rather than
// approximate.
func benchPortfolio(t *testing.T, tickers []string, benchmark string) *Portfolio {
	t.Helper()
	p := &Portfolio{
		Pname:                "bench-test",
		BuyingPower:          100000,
		InitialBuyingPower:   100000,
		Positions:            make(map[string]*Position),
		DailyReturns:         make([]DailyReturn, 0, libDays),
		PortfolioCloseValues: make([]float64, 0, libDays),
		Tickers:              tickers,
		Benchmark:            benchmark,
		Strategy:             &BuyAndHold{BuyType: "greedy"},
	}
	return p
}

// A portfolio holding exactly its own benchmark must score as identical to
// it. Every one of these values is forced by the definitions, so this is the
// check that needs no reference implementation.
func TestBenchmarkAgainstTheThingItHolds(t *testing.T) {
	hist := synthHist(42)
	p := benchPortfolio(t, []string{"AAA"}, "AAA")

	runOne(p, hist, map[int64]float64{})

	if len(p.DailyReturns) == 0 {
		t.Fatal("no simulated days")
	}
	closeTo(t, "beta", p.Metrics.Beta, 1)
	closeTo(t, "alpha", p.Metrics.Alpha, 0)
	closeTo(t, "tracking error", p.Metrics.TrackingError, 0)
	closeTo(t, "information ratio", p.Metrics.InformationRatio, 0)
	closeTo(t, "up capture", p.Metrics.UpCapture, 100)
	closeTo(t, "down capture", p.Metrics.DownCapture, 100)
}

// The benchmark must not become a holding. This is the invariant that would
// silently change what a portfolio trades if it broke, so it is checked on
// the object the runner actually simulates.
func TestBenchmarkIsNotTraded(t *testing.T) {
	hist := synthHist(42)
	// Holds AAA only, benchmarked against BBB.
	p := benchPortfolio(t, []string{"AAA"}, "BBB")

	runOne(p, hist, map[int64]float64{})

	if _, held := p.FindPosition("BBB"); held {
		t.Error("the benchmark was bought — it must never be traded")
	}
	if _, held := p.FindPosition("AAA"); !held {
		t.Error("the holding was not bought")
	}
	if len(p.Tickers) != 1 || p.Tickers[0] != "AAA" {
		t.Errorf("Tickers = %v, want only the holding", p.Tickers)
	}
	// Benchmark metrics were still computed against it.
	if p.Metrics.Beta == 0 {
		t.Error("beta not computed against an untraded benchmark")
	}
}

// Running the identical portfolio with and without a benchmark must produce
// identical trading and identical non-benchmark metrics. The benchmark is a
// measurement, not an input.
func TestBenchmarkDoesNotChangeTheSimulation(t *testing.T) {
	plain := benchPortfolio(t, []string{"AAA", "BBB"}, "")
	scored := benchPortfolio(t, []string{"AAA", "BBB"}, "CCC")

	runOne(plain, synthHist(42), map[int64]float64{})
	runOne(scored, synthHist(42), map[int64]float64{})

	if plain.BuyingPower != scored.BuyingPower {
		t.Errorf("cash differs: %v vs %v", plain.BuyingPower, scored.BuyingPower)
	}
	if len(plain.PortfolioCloseValues) != len(scored.PortfolioCloseValues) {
		t.Fatalf("day counts differ: %d vs %d",
			len(plain.PortfolioCloseValues), len(scored.PortfolioCloseValues))
	}
	for i := range plain.PortfolioCloseValues {
		if plain.PortfolioCloseValues[i] != scored.PortfolioCloseValues[i] {
			t.Fatalf("equity curve diverges at day %d: %v vs %v",
				i, plain.PortfolioCloseValues[i], scored.PortfolioCloseValues[i])
		}
	}
	// The seven pre-existing metrics must be untouched, bit for bit.
	if plain.Metrics.SharpeRatio != scored.Metrics.SharpeRatio ||
		plain.Metrics.SortinoRatio != scored.Metrics.SortinoRatio ||
		plain.Metrics.MaxDrawdown != scored.Metrics.MaxDrawdown ||
		plain.Metrics.AnnualReturn != scored.Metrics.AnnualReturn ||
		plain.Metrics.StandardDev != scored.Metrics.StandardDev ||
		plain.Metrics.AvgCorrelation != scored.Metrics.AvgCorrelation ||
		plain.Metrics.CointegratedPairs != scored.Metrics.CointegratedPairs ||
		plain.Metrics.Turnover != scored.Metrics.Turnover {
		t.Errorf("non-benchmark metrics changed:\n plain  %+v\n scored %+v",
			plain.Metrics, scored.Metrics)
	}
	// An unbenchmarked portfolio leaves the benchmark fields at zero.
	if plain.Metrics.Beta != 0 || plain.Metrics.Alpha != 0 ||
		plain.Metrics.TrackingError != 0 || plain.Metrics.InformationRatio != 0 ||
		plain.Metrics.UpCapture != 0 || plain.Metrics.DownCapture != 0 {
		t.Errorf("unbenchmarked portfolio has benchmark metrics: %+v", plain.Metrics)
	}
}

// A benchmark whose data does not cover the window must log and leave the
// fields zeroed — never fail a run that would otherwise succeed.
func TestBenchmarkCoverageGapDegradesGracefully(t *testing.T) {
	cases := map[string]func(map[string][]data.AssetData){
		"benchmark absent from the data": func(h map[string][]data.AssetData) {
			delete(h, "CCC")
		},
		"benchmark starts late": func(h map[string][]data.AssetData) {
			h["CCC"] = h["CCC"][10:]
		},
		"benchmark ends early": func(h map[string][]data.AssetData) {
			h["CCC"] = h["CCC"][:len(h["CCC"])-10]
		},
		"benchmark missing a day in the middle": func(h map[string][]data.AssetData) {
			series := h["CCC"]
			h["CCC"] = append(append([]data.AssetData{}, series[:50]...), series[51:]...)
		},
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			hist := synthHist(42)
			breakIt(hist)
			p := benchPortfolio(t, []string{"AAA"}, "CCC")

			var logBuf bytes.Buffer
			prev := log.Writer()
			log.SetOutput(&logBuf)
			runOne(p, hist, map[int64]float64{})
			log.SetOutput(prev)

			// The run itself still succeeded.
			if len(p.PortfolioCloseValues) == 0 {
				t.Fatal("a benchmark coverage gap broke the backtest")
			}
			if p.Metrics.SharpeRatio == 0 {
				t.Error("ordinary metrics were not computed")
			}
			// The benchmark metrics are absent, not guessed.
			if p.Metrics.Beta != 0 || p.Metrics.TrackingError != 0 ||
				p.Metrics.UpCapture != 0 {
				t.Errorf("partial benchmark produced metrics: %+v", p.Metrics)
			}
			if logBuf.Len() == 0 {
				t.Error("the skipped benchmark was not reported")
			}
		})
	}
}

func TestBenchmarkReturnsAlignment(t *testing.T) {
	day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	dates := []time.Time{day, day.AddDate(0, 0, 1), day.AddDate(0, 0, 2)}
	bars := []data.AssetData{
		{Date: dates[0], Close: 100},
		{Date: dates[1], Close: 110},
		{Date: dates[2], Close: 99},
	}

	got, ok := benchmarkReturns(bars, dates)
	if !ok {
		t.Fatal("full coverage reported as a gap")
	}
	if len(got) != 2 {
		t.Fatalf("got %d returns from 3 days, want 2", len(got))
	}
	closeTo(t, "return 0", got[0], 0.1)
	closeTo(t, "return 1", got[1], -0.1)

	// Extra benchmark history outside the portfolio's days is ignored, not
	// an error: the benchmark is fetched over the union range of every
	// portfolio, so it routinely spans more days than any one of them.
	wider := append([]data.AssetData{
		{Date: day.AddDate(0, 0, -5), Close: 50},
	}, bars...)
	wider = append(wider, data.AssetData{Date: day.AddDate(0, 0, 9), Close: 200})
	got2, ok := benchmarkReturns(wider, dates)
	if !ok {
		t.Fatal("wider benchmark history reported as a gap")
	}
	for i := range got {
		if math.Abs(got[i]-got2[i]) > 1e-12 {
			t.Errorf("extra history changed return %d: %v vs %v", i, got2[i], got[i])
		}
	}

	// Gaps and degenerate inputs.
	for name, tc := range map[string]struct {
		bars  []data.AssetData
		dates []time.Time
	}{
		"missing middle day": {[]data.AssetData{bars[0], bars[2]}, dates},
		"no bars":            {nil, dates},
		"single day":         {bars, dates[:1]},
		"no dates":           {bars, nil},
		"zero previous close": {
			[]data.AssetData{{Date: dates[0], Close: 0}, bars[1], bars[2]}, dates,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := benchmarkReturns(tc.bars, tc.dates); ok {
				t.Error("expected a coverage failure")
			}
		})
	}
}
