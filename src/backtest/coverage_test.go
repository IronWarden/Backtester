package backtest

// What happens when a holding's price series ends before the backtest window
// does — a delisting, an acquisition, or a feed that went stale.
//
// Nothing in the engine had ever been asked this, because the database contains
// no company that stopped trading (see analysis/survivorship.md). The moment
// any delisted history is loaded it becomes the normal case, so these tests pin
// what the engine does today, and the shape of the answer is deliberately
// uncomfortable: alignToWindow simulates only the days EVERY ticker has a bar
// for, so one dead holding silently shortens the whole backtest for all the
// others. The numbers are not wrong for the window they describe; they answer a
// shorter question than the one the config asked.
//
// These tests assert that behaviour AND that it is now reported. Changing the
// behaviour — liquidating the dead holding and carrying the survivors to the
// end of the window — moves numbers for every existing config that mixes
// histories of different lengths, so it needs the user's call and is filed as
// T49 in TASKS.md.

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"
)

// runTwoTickers simulates an equal-weight buy-and-hold over two series that may
// be of different lengths, with the window explicitly wide enough for the
// longer of the two, and returns the portfolio plus whatever was logged.
func runTwoTickers(
	t *testing.T, tickers []string, closes map[string][]float64,
) (*Portfolio, string) {
	t.Helper()

	longest := 0
	for _, cs := range closes {
		if len(cs) > longest {
			longest = len(cs)
		}
	}
	p, err := InitializePortfolio(
		exactCash,
		exactEpoch,
		exactEpoch.AddDate(0, 0, longest), // past the last bar of the longer series
		"coverage",
		tickers,
		"buyAndHold:equalWeights",
		nil,
	)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}

	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	runOne(p, histFrom(closes), map[int64]float64{})
	log.SetOutput(prev)

	return p, logBuf.String()
}

// The finding, pinned: a holding that stops trading halfway truncates the whole
// run to its own last bar. Ten days of window, five days of AAA actually
// simulated, and until now not a word about it anywhere.
func TestDelistedHoldingTruncatesTheWholeRun(t *testing.T) {
	closes := map[string][]float64{
		// AAA doubles over ten days. BBB is flat and stops after five.
		"AAA": {100, 110, 120, 130, 140, 150, 160, 170, 180, 200},
		"BBB": {100, 100, 100, 100, 100},
	}

	for _, order := range [][]string{{"AAA", "BBB"}, {"BBB", "AAA"}} {
		t.Run(strings.Join(order, ","), func(t *testing.T) {
			p, logged := runTwoTickers(t, order, closes)

			// Five bars in common: one initial valuation plus four daily
			// records. Ticker order must not change the answer — the
			// intersection walks the first ticker's dates, so a bug there
			// would show as an order-dependent length.
			if got := len(p.PortfolioCloseValues); got != 4 {
				t.Errorf("simulated %d days, want 4 (the 5 common bars, "+
					"less the first)", got)
			}

			// Half the cash rode AAA from 100 to 140 (day 4, the last common
			// bar) and half sat in BBB at 100. Not AAA's 200: the last five
			// days of AAA's history were never simulated.
			almost(t, "final value", finalValue(t, p),
				exactCash/2*1.4+exactCash/2*1.0)

			// And the truncation is no longer silent. The message must name
			// the ticker responsible and the date it ends, or it is not
			// actionable.
			if !strings.Contains(logged, `"BBB" ends 2021-01-08`) {
				t.Errorf("truncation not reported against BBB; log was %q",
					logged)
			}
			// Five bars simulated of the ten the window spans. (The four
			// daily records above are those five bars less the first, which
			// is the initial valuation rather than a return.)
			if !strings.Contains(logged, "simulated 5 of the 10 trading days") {
				t.Errorf("shortfall not quantified; log was %q", logged)
			}
		})
	}
}

// The same mechanism with a hole in the middle instead of a missing tail: one
// ticker's gap deletes that day for every other holding. Worth its own test
// because the fix for the delisting case (carry the dead name at its last
// price) is the wrong fix here, where the name is alive and the data is bad.
func TestInteriorGapDropsThatDayForEveryTicker(t *testing.T) {
	closes := map[string][]float64{
		"AAA": {100, 100, 100, 100, 100, 100},
		"BBB": {100, 100, 100, 100, 100, 100},
	}
	hist := histFrom(closes)
	// BBB is missing day 2 only; its first and last bars still line up with
	// AAA's, so nothing about the window's extent changes.
	hist["BBB"] = append(hist["BBB"][:2], hist["BBB"][3:]...)

	p, err := InitializePortfolio(exactCash, exactEpoch,
		exactEpoch.AddDate(0, 0, 6), "gap", []string{"AAA", "BBB"},
		"buyAndHold:equalWeights", nil)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	runOne(p, hist, map[int64]float64{})
	log.SetOutput(prev)

	// Six calendar bars, five simulated days (day 2 dropped), four records.
	if got := len(p.PortfolioCloseValues); got != 4 {
		t.Errorf("simulated %d days, want 4 (6 bars less the gap, less the "+
			"first)", got)
	}
	if !strings.Contains(logBuf.String(), "gaps inside the tickers' histories") {
		t.Errorf("a dropped interior day was not reported as a gap; log was %q",
			logBuf.String())
	}
	// No ticker ends early, so nothing may be blamed for a truncation.
	if strings.Contains(logBuf.String(), "truncated") {
		t.Errorf("an interior gap was reported as a truncation; log was %q",
			logBuf.String())
	}
}

// The regression guard: every config that exists today has tickers covering
// its whole window, and must stay both silent and numerically untouched.
func TestFullCoverageReportsNothing(t *testing.T) {
	closes := map[string][]float64{
		"AAA": {100, 110, 120, 130, 140},
		"BBB": {100, 100, 100, 100, 100},
	}
	p, logged := runTwoTickers(t, []string{"AAA", "BBB"}, closes)

	if logged != "" {
		t.Errorf("a fully covered portfolio logged %q", logged)
	}
	if got := len(p.PortfolioCloseValues); got != 4 {
		t.Errorf("simulated %d days, want 4", got)
	}
	almost(t, "final value", finalValue(t, p), exactCash/2*1.4+exactCash/2*1.0)
}

// windowCoverage is the pure half, so it can be checked without a simulation.
func TestWindowCoverage(t *testing.T) {
	hist := histFrom(map[string][]float64{
		"AAA": {1, 2, 3, 4, 5, 6},
		"BBB": {1, 2, 3},
	})
	day := func(i int) time.Time { return exactEpoch.AddDate(0, 0, i) }

	t.Run("open window", func(t *testing.T) {
		union, last, early := windowCoverage(
			hist, []string{"AAA", "BBB"}, time.Time{}, time.Time{})
		if union != 6 {
			t.Errorf("union = %d, want 6", union)
		}
		if !last.Equal(day(5)) {
			t.Errorf("last day = %v, want %v", last, day(5))
		}
		if got, ok := early["BBB"]; !ok || !got.Equal(day(2)) {
			t.Errorf("BBB ends %v (present=%v), want %v", got, ok, day(2))
		}
		if _, ok := early["AAA"]; ok {
			t.Error("AAA reported as ending early; it runs to the last day")
		}
	})

	t.Run("window clipped to the short series", func(t *testing.T) {
		// With the window ending on day 2, neither ticker falls short and
		// nothing is missing: the shortfall is a property of the window, not
		// of the ticker.
		union, last, early := windowCoverage(
			hist, []string{"AAA", "BBB"}, day(0), day(2))
		if union != 3 {
			t.Errorf("union = %d, want 3", union)
		}
		if !last.Equal(day(2)) {
			t.Errorf("last day = %v, want %v", last, day(2))
		}
		if len(early) != 0 {
			t.Errorf("reported %v as ending early inside a window they cover",
				early)
		}
	})

	t.Run("single ticker", func(t *testing.T) {
		union, _, early := windowCoverage(
			hist, []string{"BBB"}, time.Time{}, time.Time{})
		if union != 3 || len(early) != 0 {
			t.Errorf("union = %d, early = %v; a lone ticker defines its own "+
				"window and can never end early", union, early)
		}
	})
}
