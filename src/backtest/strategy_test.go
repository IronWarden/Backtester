package backtest

import (
	"math"
	"my-backtester/src/data"
	"testing"
	"time"
)

// flatSeries is one ticker's history: a starting close that moves to end over
// two days, with OHLC held equal so no strategy can pick a different fill.
func flatSeries(start, end float64) []data.AssetData {
	d0 := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)
	return []data.AssetData{
		{Date: d0, Open: start, High: start, Low: start, Close: start},
		{Date: d0.AddDate(0, 0, 1), Open: end, High: end, Low: end, Close: end},
	}
}

// TestEqualWeightsInvestsFullBalance pins the property that made the bug
// visible: N identical assets held at equal weight must return exactly what
// one of them returns held alone. Sizing each leg against the balance left
// after the previous leg filled instead strands ((N-1)/N)^N of the capital in
// cash, so the pair returned 75% of the solo figure.
func TestEqualWeightsInvestsFullBalance(t *testing.T) {
	const capital = 1000.0
	// Every ticker doubles, so a fully invested portfolio must double too.
	hist := map[string][]data.AssetData{
		"A": flatSeries(100, 200),
		"B": flatSeries(50, 100),
		"C": flatSeries(400, 800),
	}

	for _, tickers := range [][]string{{"A"}, {"A", "B"}, {"A", "B", "C"}} {
		p, err := InitializePortfolio(
			capital, hist["A"][0].Date, hist["A"][1].Date,
			"equal", tickers, "equalWeights", nil,
		)
		if err != nil {
			t.Fatalf("%v: %v", tickers, err)
		}
		p.Strategy.Step(p, hist, 0)

		if p.BuyingPower > 1e-9 {
			t.Errorf("%v: %.2f of %.0f left uninvested, want ~0",
				tickers, p.BuyingPower, capital)
		}
		got := p.GetPortfolioValue(tickers, hist, 1)
		if math.Abs(got-2*capital) > 1e-6 {
			t.Errorf("%v: value %.2f, want %.2f (every holding doubled)",
				tickers, got, 2*capital)
		}
		// Equal weight means equal dollars, not equal share counts.
		for _, tk := range tickers {
			pos, ok := p.FindPosition(tk)
			if !ok {
				t.Fatalf("%v: no position in %s", tickers, tk)
			}
			want := capital / float64(len(tickers))
			if dollars := pos.Amount * hist[tk][0].Close; math.Abs(dollars-want) > 1e-6 {
				t.Errorf("%v: %s allocated %.2f, want %.2f",
					tickers, tk, dollars, want)
			}
		}
	}
}

// TestGreedyPoursIntoFirstTicker guards the other half of the fix: greedy is
// defined as pouring all available buying power into each ticker in order, so
// it must keep reading the live balance and leave later tickers with nothing.
func TestGreedyPoursIntoFirstTicker(t *testing.T) {
	hist := map[string][]data.AssetData{
		"A": flatSeries(100, 200),
		"B": flatSeries(50, 100),
	}
	tickers := []string{"A", "B"}

	p, err := InitializePortfolio(
		1000, hist["A"][0].Date, hist["A"][1].Date,
		"greedy", tickers, "greedy", nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	p.Strategy.Step(p, hist, 0)

	pos, ok := p.FindPosition("A")
	if !ok {
		t.Fatal("no position in A")
	}
	if math.Abs(pos.Amount-10) > 1e-9 {
		t.Errorf("A holds %.4f shares, want 10 (all 1000 at 100)", pos.Amount)
	}
	if _, ok := p.FindPosition("B"); ok {
		t.Error("B holds a position, but greedy spent everything on A")
	}
}
