package backtest

import (
	"strings"
	"testing"

	"my-backtester/src/data"
)

func qualityFixture() map[string]data.TickerQuality {
	return map[string]data.TickerQuality{
		// The real one: Yahoo's back-adjustment drove its closes negative.
		"VHI": {
			Verdict: data.QualityUnusable,
			Stats:   data.QualityStats{Bars: 5429, NonPositiveCloses: 5429},
		},
		"HUBB": {
			Verdict: data.QualitySuspect,
			Stats:   data.QualityStats{Bars: 13000, LongestFlatRun: 1862},
		},
		"AAPL": {
			Verdict: data.QualityGood,
			Stats:   data.QualityStats{Bars: 11000, ExpectedDays: 11000},
		},
		"MSFT": {
			Verdict: data.QualityUsable,
			Stats:   data.QualityStats{Bars: 9000, ExpectedDays: 9000, BigJumps: 2},
		},
	}
}

// Only the two verdicts that mean something to a user. Warning about the
// ordinary roughness of free daily data would train them to ignore the channel
// that carries the real findings.
func TestQualityWarningsOnlyCondemnedSeries(t *testing.T) {
	p := &Portfolio{Pname: "P", Tickers: []string{"AAPL", "MSFT", "HUBB", "VHI"}}
	got := qualityWarnings([]*Portfolio{p}, qualityFixture())

	if len(got) != 2 {
		t.Fatalf("reported %d warnings, want 2 (VHI and HUBB):\n%+v", len(got), got)
	}
	joined := got[0].Message + "\n" + got[1].Message
	for _, want := range []string{
		"HUBB", "VHI",
		"cannot support a backtest",    // the unusable wording
		"move a metric on its own",     // the suspect wording
		"priced at or below zero",      // VHI's actual defect
		"consecutive identical closes", // HUBB's actual defect
		"Nothing is excluded",          // warn only, refuse nothing
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("warnings do not mention %q:\n%s", want, joined)
		}
	}
	for _, never := range []string{"AAPL", "MSFT"} {
		if strings.Contains(joined, never) {
			t.Errorf("a clean series was warned about (%s):\n%s", never, joined)
		}
	}
	for _, d := range got {
		if d.Level != "warn" || d.Portfolio != "P" {
			t.Errorf("warning is not a portfolio warning: %+v", d)
		}
	}
}

// A ticker the audit has never seen is unexamined, not condemned. Saying
// anything about it would be inventing a verdict.
func TestQualityWarningsSayNothingAboutAnUnauditedTicker(t *testing.T) {
	p := &Portfolio{Pname: "P", Tickers: []string{"NEWCO"}}
	if got := qualityWarnings([]*Portfolio{p}, qualityFixture()); len(got) != 0 {
		t.Errorf("an unaudited ticker produced %+v", got)
	}
	// And with no audit at all, nothing is said by this function either -- the
	// missing table is reported once, elsewhere.
	if got := qualityWarnings([]*Portfolio{p}, nil); len(got) != 0 {
		t.Errorf("a missing audit produced %+v", got)
	}
}

// One line per portfolio per condemned holding: a portfolio naming three bad
// tickers has three problems, and two portfolios sharing a bad ticker each
// have one -- the warning belongs to the portfolio that has to be fixed.
func TestQualityWarningsAreOnePerPortfolioPerTicker(t *testing.T) {
	a := &Portfolio{Pname: "A", Tickers: []string{"VHI", "HUBB"}}
	b := &Portfolio{Pname: "B", Tickers: []string{"VHI"}}
	got := qualityWarnings([]*Portfolio{a, b}, qualityFixture())
	if len(got) != 3 {
		t.Fatalf("reported %d warnings, want 3:\n%+v", len(got), got)
	}
	// Sorted within a portfolio, so a run's warnings do not shuffle between
	// otherwise identical runs.
	if !strings.HasPrefix(got[0].Message, "HUBB") || !strings.HasPrefix(got[1].Message, "VHI") {
		t.Errorf("warnings are not in ticker order:\n%+v", got)
	}
	if got[2].Portfolio != "B" {
		t.Errorf("the second portfolio's warning is missing:\n%+v", got)
	}
}
