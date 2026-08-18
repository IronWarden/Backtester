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
