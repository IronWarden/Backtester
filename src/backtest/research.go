package backtest

// One command that asks the questions in the order that kills ideas fastest.
//
// Every habit that produces trustworthy research — comparing against a
// baseline, checking a result against chance, seeing which environment it
// worked in, pushing on it with costs — is now built, and every one of them is
// something a user has to REMEMBER to look at. That is the wrong default: the
// pieces exist but the discipline does not, and discipline that depends on
// remembering does not survive an exciting result.
//
// So this composes them into one narrative, ordered by what should stop you
// soonest:
//
//	1. Did it place any trades at all?      (no trades is not a 0% return)
//	2. Did it beat buy-and-hold?            (most strategies do not)
//	3. Is it better than random timing?     (a Sharpe is not evidence)
//	4. Where did the money come from?       (one lucky name, or broadly?)
//	5. Which environment did it work in?    (one calm stretch, or throughout?)
//	6. What kills it?                       (friction, or a different start date)
//
// It invents no new statistic and passes no verdict. Deciding what "good
// enough" means — the pre-registered criterion, the trial budget, the survival
// thresholds — is a research-design choice with real consequences, and it
// belongs to whoever is doing the research, not to the tool reporting to them.

import (
	"fmt"
	"math"
	"strings"
)

// ResearchReport renders the full narrative for one finished run.
//
// Pure: it reads a Result and says what is in it, so the ordering and the
// wording can be tested without running anything.
func ResearchReport(res Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n== %s (%s)\n", res.PortfolioName, res.Strategy)

	// 1. Did it trade? Everything below is meaningless otherwise, so it comes
	// first and stops the report when the answer is no.
	if res.TradeStats.Trades == 0 {
		b.WriteString("  NO TRADES. This strategy never entered the market, so " +
			"there is no result to read.\n  A flat 0% is not a finding — check " +
			"the warm-up, the parameters, and whether\n  a non-zero " +
			"slippage_bps is rejecting its orders.\n")
		return b.String()
	}
	// A strategy that never sells has no round trips, and printing "0.0% win
	// rate" for it reads as a failure rather than as a shape of strategy.
	if res.TradeStats.RoundTrips == 0 {
		fmt.Fprintf(&b, "  %d fills, none closed — nothing was sold, so there "+
			"is no win rate to report\n", res.TradeStats.Trades)
	} else {
		fmt.Fprintf(&b, "  %d fills, %.1f%% win rate, %.0f days average hold\n",
			res.TradeStats.Trades, res.TradeStats.WinRate*100,
			res.TradeStats.AvgHoldingDays)
	}

	// 2. Against the baseline. The first question worth asking of any result.
	if res.Baseline.Computed {
		// Three outcomes, not two. A strategy that ties the baseline has not
		// lost to it, and "LOST by 0.00%/yr" is the kind of line that teaches a
		// reader to stop trusting the report.
		switch {
		case matchesBaseline(res):
			fmt.Fprintf(&b,
				"  MATCHED equal-weight buy-and-hold exactly (%.2f%%/yr) — it "+
					"is doing the same thing\n", res.Metrics.AnnualReturn)
		case res.Baseline.Beat:
			fmt.Fprintf(&b,
				"  beat equal-weight buy-and-hold by %.2f%%/yr (%.2f vs %.2f)\n",
				res.Baseline.ExcessAnnualReturn,
				res.Metrics.AnnualReturn, res.Baseline.AnnualReturn)
		default:
			fmt.Fprintf(&b,
				"  LOST to equal-weight buy-and-hold by %.2f%%/yr (%.2f vs "+
					"%.2f)\n", -res.Baseline.ExcessAnnualReturn,
				res.Metrics.AnnualReturn, res.Baseline.AnnualReturn)
			b.WriteString("  A strategy that loses to holding its own universe " +
				"has to justify itself\n  on something other than return — " +
				"lower drawdown, say — or not at all.\n")
		}
	}

	// 3. Against chance.
	if res.Significance.Computed {
		fmt.Fprintf(&b,
			"  Sharpe %.2f, p = %.3f against random timing with the same "+
				"exposure\n", res.Significance.Sharpe, res.Significance.PValue)
		switch {
		case res.Significance.PValue > 0.4:
			b.WriteString("  That is indistinguishable from being invested at " +
				"random.\n")
		case res.Significance.PValue > 0.1:
			b.WriteString("  Weak evidence that the timing did anything.\n")
		}
		fmt.Fprintf(&b, "  90%% confidence interval on that Sharpe: %.2f to %.2f\n",
			res.Significance.BootstrapLow, res.Significance.BootstrapHigh)
	}

	// 4. Where the money came from.
	if n := len(res.TradeStats.PerTicker); n > 0 {
		top := res.TradeStats.PerTicker[0]
		var total float64
		for _, row := range res.TradeStats.PerTicker {
			total += row.Total()
		}
		fmt.Fprintf(&b, "  Best contributor: %s (%.2f of %.2f total)\n",
			top.Ticker, top.Total(), total)
		if total > 0 && top.Total() > 0.7*total && n > 1 {
			b.WriteString("  One position produced most of the profit — that " +
				"is a bet on that name,\n  not evidence the rule works.\n")
		}
	}

	// 5. Which environment.
	if res.Regimes.Computed {
		b.WriteString(res.Regimes.String())
	}

	// 6. What kills it.
	if res.Robustness.Computed {
		b.WriteString(res.Robustness.String())
	} else {
		b.WriteString("  (run with -robustness to see how much friction and " +
			"which start dates it survives)\n")
	}

	// The overfitting correction last, because it qualifies everything above.
	if res.Trials > 1 {
		fmt.Fprintf(&b,
			"  Selected from %d trials; deflated Sharpe %.3f — that is the "+
				"figure to quote,\n  not the raw one.\n",
			res.Trials, res.DeflatedSharpe)
	}
	return b.String()
}

// matchesBaseline reports whether a run is indistinguishable from the baseline.
// The tolerance is relative to the baseline's own final value, so it means "the
// same result" rather than "within a fixed number of dollars".
func matchesBaseline(res Result) bool {
	scale := math.Max(1, math.Abs(res.Baseline.FinalValue))
	return math.Abs(res.Baseline.ExcessFinalValue) <= 1e-6*scale
}

// ResearchSummary renders every run plus a closing reminder of the questions
// this report cannot answer.
func ResearchSummary(results []Result) string {
	var b strings.Builder
	b.WriteString("Research report — the questions that kill an idea, in order\n")
	for _, res := range results {
		b.WriteString(ResearchReport(res))
	}
	b.WriteString("\nWhat this does NOT tell you:\n")
	b.WriteString("  - whether the strategy reads the future: run " +
		"-validate <spec>\n")
	b.WriteString("  - whether you have tested this before, or how many " +
		"things you have tried:\n    run with -record -campaign <name>\n")
	b.WriteString("  - whether the signal it trades on predicts anything at " +
		"all: run -scan-signals\n")
	b.WriteString("  - whether it still reproduces next month: keep a " +
		"-bundle and -verify it\n")
	return b.String()
}
