package backtest

// The research report is prose about numbers, so what matters is that it says
// the RIGHT thing about each shape of result — especially the shapes that flatter
// a bad idea if described carelessly.

import (
	"strings"
	"testing"
)

func reportFor(edit func(*Result)) string {
	res := Result{
		PortfolioName: "candidate",
		Strategy:      "lua:strategies/rsi.lua",
		Metrics:       Metrics{AnnualReturn: 12.0, SharpeRatio: 1.1},
		TradeStats: TradeStats{
			Trades: 20, RoundTrips: 10, WinRate: 0.6, AvgHoldingDays: 14,
			PerTicker: []TickerPnL{
				{Ticker: "AAA", Realized: 400}, {Ticker: "BBB", Realized: 350},
			},
		},
		Baseline: BaselineStats{
			Computed: true, Beat: true, AnnualReturn: 8.0,
			ExcessAnnualReturn: 4.0, ExcessFinalValue: 900, FinalValue: 10000,
		},
		Significance: Significance{
			Computed: true, Sharpe: 1.1, PValue: 0.02,
			BootstrapLow: 0.6, BootstrapHigh: 1.6,
		},
	}
	if edit != nil {
		edit(&res)
	}
	return ResearchReport(res)
}

// A strategy that never traded stops the report: everything below would be
// describing an empty book, and a flat 0% is not a finding.
func TestNoTradesStopsTheReport(t *testing.T) {
	got := reportFor(func(r *Result) { r.TradeStats = TradeStats{} })
	if !strings.Contains(got, "NO TRADES") {
		t.Errorf("a strategy with no fills was not called out: %q", got)
	}
	for _, later := range []string{"buy-and-hold", "random timing", "regime"} {
		if strings.Contains(got, later) {
			t.Errorf("report continued past no-trades and discussed %q: %q",
				later, got)
		}
	}
}

// Beating, matching and losing are three different outcomes. A tie printed as
// "LOST by 0.00%/yr" is the kind of line that teaches a reader to stop trusting
// the report — it appeared on the shipped default config.
func TestBaselineHasThreeOutcomes(t *testing.T) {
	beat := reportFor(nil)
	if !strings.Contains(beat, "beat equal-weight") {
		t.Errorf("a winning strategy was not described as beating: %q", beat)
	}

	tied := reportFor(func(r *Result) {
		r.Baseline.Beat = false
		r.Baseline.ExcessFinalValue = 0
		r.Baseline.ExcessAnnualReturn = 0
		r.Baseline.AnnualReturn = r.Metrics.AnnualReturn
	})
	if !strings.Contains(tied, "MATCHED") {
		t.Errorf("an exact tie was not described as a match: %q", tied)
	}
	if strings.Contains(tied, "LOST") {
		t.Errorf("an exact tie was described as a loss: %q", tied)
	}

	lost := reportFor(func(r *Result) {
		r.Baseline.Beat = false
		r.Baseline.ExcessFinalValue = -1500
		r.Baseline.ExcessAnnualReturn = -3.0
		r.Baseline.AnnualReturn = 15.0
	})
	if !strings.Contains(lost, "LOST to equal-weight") {
		t.Errorf("a losing strategy was not described as losing: %q", lost)
	}
	// And the margin is reported as a positive quantity, since "lost by -3%"
	// reads as a gain.
	if !strings.Contains(lost, "by 3.00%/yr") {
		t.Errorf("the losing margin is not stated positively: %q", lost)
	}
}

// A strategy that never sells has no round trips, and "0.0% win rate" reads as
// a failure rather than as the shape of a hold-forever strategy.
func TestNoRoundTripsIsNotAZeroWinRate(t *testing.T) {
	got := reportFor(func(r *Result) {
		r.TradeStats.RoundTrips = 0
		r.TradeStats.WinRate = 0
	})
	if strings.Contains(got, "0.0% win rate") {
		t.Errorf("a hold-forever strategy was given a 0%% win rate: %q", got)
	}
	if !strings.Contains(got, "none closed") {
		t.Errorf("report does not explain the absent win rate: %q", got)
	}
}

// The concentration warning is the one that most often changes a decision: a
// result carried by one position is a bet on that name.
func TestConcentrationIsCalledOut(t *testing.T) {
	concentrated := reportFor(func(r *Result) {
		r.TradeStats.PerTicker = []TickerPnL{
			{Ticker: "NVDA", Realized: 9000}, {Ticker: "AAA", Realized: 200},
		}
	})
	if !strings.Contains(concentrated, "One position produced most of the profit") {
		t.Errorf("a 97%%-from-one-name result was not flagged: %q", concentrated)
	}

	spread := reportFor(nil) // 400 vs 350 — no single name dominates
	if strings.Contains(spread, "One position produced most") {
		t.Errorf("an evenly spread result was flagged as concentrated: %q", spread)
	}
}

// A p-value in the middle of the null must be described as such rather than
// printed bare, since "p = 0.52" invites the reader to see a number as evidence.
func TestWeakSignificanceIsSpelledOut(t *testing.T) {
	noise := reportFor(func(r *Result) { r.Significance.PValue = 0.52 })
	if !strings.Contains(noise, "indistinguishable from being invested at random") {
		t.Errorf("a p of 0.52 was not explained: %q", noise)
	}

	weak := reportFor(func(r *Result) { r.Significance.PValue = 0.2 })
	if !strings.Contains(weak, "Weak evidence") {
		t.Errorf("a p of 0.2 was not qualified: %q", weak)
	}

	strong := reportFor(nil) // p = 0.02
	if strings.Contains(strong, "indistinguishable") ||
		strings.Contains(strong, "Weak evidence") {
		t.Errorf("a p of 0.02 was qualified as weak: %q", strong)
	}
}

// A swept result must be quoted with its deflated figure, and the report has to
// say which number to use.
func TestTrialCountQualifiesTheResult(t *testing.T) {
	swept := reportFor(func(r *Result) {
		r.Trials = 240
		r.DeflatedSharpe = 0.31
	})
	if !strings.Contains(swept, "240 trials") ||
		!strings.Contains(swept, "0.310") {
		t.Errorf("a 240-trial result did not report its deflated figure: %q",
			swept)
	}

	single := reportFor(nil)
	if strings.Contains(single, "trials;") {
		t.Errorf("a single run mentioned a trial correction: %q", single)
	}
}

// The summary must name the questions it cannot answer, so the report does not
// read as a complete verdict.
func TestSummaryNamesWhatItCannotAnswer(t *testing.T) {
	got := ResearchSummary([]Result{{PortfolioName: "x"}})
	for _, flag := range []string{"-validate", "-record", "-scan-signals", "-bundle"} {
		if !strings.Contains(got, flag) {
			t.Errorf("summary does not point at %s: %q", flag, got)
		}
	}
}
