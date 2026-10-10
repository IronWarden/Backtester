package backtest

// Does a run of several portfolios honour each portfolio's own date window, or
// does one window win?
//
// The runner issues ONE database query covering the union of every portfolio's
// window (dateRange), which is what makes the question worth asking: the shared
// history handed to every worker spans all of them. alignToWindow is what
// narrows it back down per portfolio, and these pin that it actually does.
//
// Nothing tested this before. The behaviour was correct, but it was correct by
// inspection, and a comment is not a guarantee.

import (
	"testing"
	"time"

	"my-backtester/src/data"
)

// runWindow simulates one buy-and-hold portfolio over an explicit window,
// against the SHARED history the runner would have fetched for the union of
// every portfolio's window.
func runWindow(
	t *testing.T, name string, start, end time.Time, hist map[string][]data.AssetData,
) *Portfolio {
	t.Helper()
	p, err := InitializePortfolio(
		exactCash, start, end, name,
		[]string{"AAA"}, "buyAndHold:equalWeights", nil,
	)
	if err != nil {
		t.Fatalf("InitializePortfolio(%s): %v", name, err)
	}
	runOne(p, hist, map[int64]float64{})
	return p
}

// Two portfolios, two windows, one shared history: each must simulate only its
// own days.
func TestEachPortfolioUsesItsOwnDateWindow(t *testing.T) {
	// 40 consecutive days. The price doubles over the first 20 and is flat
	// after, so a portfolio confined to the second half must show no return —
	// and one that leaked into the first half could not.
	closes := make([]float64, 40)
	for i := range closes {
		if i < 20 {
			closes[i] = 100 + float64(i)*5 // 100 -> 195
		} else {
			closes[i] = 195
		}
	}
	hist := histFrom(map[string][]float64{"AAA": closes})

	day := func(i int) time.Time { return exactEpoch.AddDate(0, 0, i) }

	// The union window the runner would query for, spanning both portfolios.
	unionStart, unionEnd := day(0), day(39)

	// Deliberately different LENGTHS as well as different positions: two
	// windows of the same span could both be wrong in the same way and still
	// agree. The simulation loop runs `for day := 1; day < dataLen`, so a
	// window of N bars produces N-1 simulated days.
	first := runWindow(t, "early-short", unionStart, day(9), hist) // 10 bars
	second := runWindow(t, "late-long", day(20), unionEnd, hist)   // 20 bars

	if got := len(first.PortfolioCloseValues); got != 9 {
		t.Errorf("early-short simulated %d days, want 9 (10 bars) — its window "+
			"is being widened by the shared history or by the other portfolio", got)
	}
	if got := len(second.PortfolioCloseValues); got != 19 {
		t.Errorf("late-long simulated %d days, want 19 (20 bars)", got)
	}

	// The substance, not just the day count: the flat half must be flat.
	if second.Metrics.AnnualReturn != 0 {
		t.Errorf("late-long annual return is %v over a flat price path — it is "+
			"seeing the early window's rise", second.Metrics.AnnualReturn)
	}
	if first.Metrics.AnnualReturn <= 0 {
		t.Errorf("early-short annual return is %v over a rising price path",
			first.Metrics.AnnualReturn)
	}
	// Two windows over the same series must not produce the same result, which
	// is what "one window shadows the other" would look like.
	firstEnd := first.PortfolioCloseValues[len(first.PortfolioCloseValues)-1]
	secondEnd := second.PortfolioCloseValues[len(second.PortfolioCloseValues)-1]
	if firstEnd == secondEnd {
		t.Errorf("both portfolios ended at %v — the windows are not distinct",
			firstEnd)
	}
}

// dateRange is what makes the shared query wide enough for every portfolio. If
// it ever narrowed to an intersection, the outer portfolios would silently
// lose bars.
func TestDateRangeSpansEveryPortfolio(t *testing.T) {
	day := func(i int) time.Time { return exactEpoch.AddDate(0, 0, i) }
	mk := func(name string, start, end time.Time) *Portfolio {
		p, err := InitializePortfolio(exactCash, start, end, name,
			[]string{"AAA"}, "buyAndHold:equalWeights", nil)
		if err != nil {
			t.Fatalf("InitializePortfolio(%s): %v", name, err)
		}
		return p
	}
	// Deliberately out of order and overlapping, since neither the earliest
	// start nor the latest end belongs to the same portfolio.
	ps := []*Portfolio{
		mk("middle", day(10), day(20)),
		mk("early", day(0), day(15)),
		mk("late", day(5), day(39)),
	}
	start, end := dateRange(ps)
	if !start.Equal(day(0)) {
		t.Errorf("dateRange start = %v, want %v (the earliest of any portfolio)",
			start.Format("2006-01-02"), day(0).Format("2006-01-02"))
	}
	if !end.Equal(day(39)) {
		t.Errorf("dateRange end = %v, want %v (the latest of any portfolio)",
			end.Format("2006-01-02"), day(39).Format("2006-01-02"))
	}
}
