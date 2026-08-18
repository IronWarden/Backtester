package backtest

import (
	"strings"
	"testing"
	"time"
)

// A window that ends before it starts used to panic: days went negative and
// make([]T, 0, negative) is "makeslice: cap out of range". Swapping two dates,
// or typing 2019 where 2029 was meant, is enough. The CLI died with a stack
// trace and the UI showed the user that stack instead of the typo.
func TestReversedWindowIsRejectedNotPanicked(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a reversed window still panics: %v", r)
		}
	}()

	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	p, err := InitializePortfolio(10000, start, end, "rev",
		[]string{"AAA"}, "buyAndHold:equalWeights", nil)

	if err == nil {
		t.Fatal("a window ending before it starts was accepted")
	}
	if p != nil {
		t.Error("a rejected config still returned a portfolio")
	}
	// The message has to name the dates, or the user is told the config is
	// wrong without being told which line.
	for _, want := range []string{"rev", "2019-01-01", "2020-01-01"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}

// A single-day window is degenerate but not a mistake, and must still build.
// The boundary matters: the check is Before, not !After.
func TestZeroLengthWindowIsAllowed(t *testing.T) {
	day := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := InitializePortfolio(10000, day, day, "same",
		[]string{"AAA"}, "buyAndHold:equalWeights", nil); err != nil {
		t.Errorf("a start == end window was rejected: %v", err)
	}
}

// Negative capital is not a portfolio. Zero is left alone deliberately:
// Deposit exists, so starting empty and funding later is a supported shape.
func TestCapitalBounds(t *testing.T) {
	end := exactEpoch.AddDate(0, 0, 10)

	if _, err := InitializePortfolio(-5000, exactEpoch, end, "neg",
		[]string{"AAA"}, "buyAndHold:equalWeights", nil); err == nil {
		t.Error("negative BuyingPower was accepted")
	}
	if _, err := InitializePortfolio(0, exactEpoch, end, "zero",
		[]string{"AAA"}, "buyAndHold:equalWeights", nil); err != nil {
		t.Errorf("zero BuyingPower was rejected, but Deposit makes it a "+
			"legitimate starting state: %v", err)
	}
}

// The config path has to surface the same rejection rather than translating it
// into something less useful, since that is where a real user meets it.
func TestReversedWindowFromConfig(t *testing.T) {
	pc := &PortfolioConfig{
		Name:        "backwards",
		BuyingPower: 10000,
		StartTime:   "2020-01-01",
		EndTime:     "2019-01-01",
		Tickers:     []string{"AAA"},
		Strategy:    "buyAndHold:equalWeights",
	}
	_, err := pc.ToPortfolio()
	if err == nil {
		t.Fatal("a config with EndDate before StartDate was accepted")
	}
	if !strings.Contains(err.Error(), "backwards") {
		t.Errorf("error does not name the portfolio: %v", err)
	}
}

// A repeated ticker used to panic in the day loop, and the panic was hiding a
// worse problem. alignToWindow keys its output by ticker but appends once per
// INDEX, so a duplicate appended the same bar twice per day and returned a
// series of 2n bars alongside a length of n. GetPortfolioValue iterates
// Tickers too, so the single position would have been counted twice and the
// equity curve inflated.
func TestDuplicateTickersAreRejected(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a duplicate ticker still panics: %v", r)
		}
	}()

	p, err := InitializePortfolio(10000, exactEpoch, exactEpoch.AddDate(0, 0, 10),
		"dupe", []string{"AAA", "BBB", "AAA"}, "buyAndHold:equalWeights", nil)

	if err == nil {
		t.Fatal("a ticker listed twice was accepted")
	}
	if p != nil {
		t.Error("a rejected config still returned a portfolio")
	}
	if !strings.Contains(err.Error(), `"AAA"`) {
		t.Errorf("error does not name the repeated ticker: %v", err)
	}
}

// The underlying defect, pinned directly so it cannot be reintroduced by a
// caller that reaches alignToWindow without going through the constructor:
// one entry per ticker per day, however the ticker list is spelled.
func TestAlignToWindowEmitsOneBarPerDay(t *testing.T) {
	hist := histFrom(map[string][]float64{
		"AAA": {100, 101, 102},
		"BBB": {200, 201, 202},
	})
	aligned, n := alignToWindow(hist, []string{"AAA", "BBB"},
		time.Time{}, time.Time{})

	if n != 3 {
		t.Fatalf("aligned length = %d, want 3", n)
	}
	for ticker, bars := range aligned {
		if len(bars) != n {
			t.Errorf("%s has %d bars against a reported length of %d — "+
				"indexing the series by day will run off the end",
				ticker, len(bars), n)
		}
	}
}
