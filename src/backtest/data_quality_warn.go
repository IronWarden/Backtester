package backtest

// Saying that a series cannot support a backtest.
//
// The 2026-08-17 audit found 273 tickers whose bars cannot support one at all
// and 1,053 more whose defects can move a metric by themselves. VHI carries
// closes down to -639.30, so a return computed across the sign change is
// arithmetic nonsense that reads as an ordinary extreme move. Nothing declined
// to trade any of them, and nothing said so.
//
// This warns and refuses nothing. That is the whole decision: the run produces
// exactly the numbers it produced before, no saved config breaks, and the user
// is told which holding the audit condemns and why. Refusing a series is a 1.0
// decision, to be made once there is feedback from people who are not its
// author -- and a strategy of "the engine silently drops your ticker" is worse
// than either.
//
// It reads the optional `data_quality` table. Where that table is absent every
// verdict is absent, and this says so once and carries on.

import (
	"fmt"
	"sort"
	"sync"

	"my-backtester/src/data"
)

// missingAuditOnce keeps "this database has no audit" to one line per process
// rather than one per run. It is a fact about the database, not about the run,
// and a config re-run twenty times should not say it twenty times.
var missingAuditOnce sync.Once

// warnDataQuality reports the holdings the audit calls unusable or suspect.
// Benchmarks are deliberately not checked: a user did not choose the bars
// behind $SP500, and a warning they cannot act on is noise.
func warnDataQuality(portfolios []*Portfolio) {
	tickerSet := make(map[string]bool)
	for _, p := range portfolios {
		for _, t := range p.Tickers {
			tickerSet[t] = true
		}
	}
	if len(tickerSet) == 0 {
		return
	}
	tickers := make([]string, 0, len(tickerSet))
	for t := range tickerSet {
		tickers = append(tickers, t)
	}
	sort.Strings(tickers)

	verdicts, err := data.LoadDataQuality(tickers)
	if err != nil {
		// An optional dataset that failed to load is not the run's problem,
		// and the run is about to produce real numbers either way.
		return
	}
	if verdicts == nil {
		missingAuditOnce.Do(func() {
			diagf("info", "",
				"this database has no data_quality table, so no holding was "+
					"checked for defective bars; run scripts/add_data_quality.py to "+
					"enable those warnings")
		})
		return
	}
	for _, w := range qualityWarnings(portfolios, verdicts) {
		diagf("warn", w.Portfolio, "%s", w.Message)
	}
}

// qualityWarnings decides what to say, given verdicts. Pure and separate from
// the query so the wording and the selection are testable without a database.
//
// One line per portfolio per condemned holding: a portfolio naming three bad
// tickers has three problems, and collapsing them would hide two.
func qualityWarnings(
	portfolios []*Portfolio, verdicts map[string]data.TickerQuality,
) []Diagnostic {
	var out []Diagnostic
	for _, p := range portfolios {
		tickers := append([]string(nil), p.Tickers...)
		sort.Strings(tickers)
		for _, t := range tickers {
			q, ok := verdicts[t]
			if !ok {
				// Absent from the audit is not a verdict. A ticker added after
				// the last audit run is unexamined, not condemned.
				continue
			}
			var lead string
			switch q.Verdict {
			case data.QualityUnusable:
				lead = fmt.Sprintf(
					"%s cannot support a backtest according to the data audit", t)
			case data.QualitySuspect:
				lead = fmt.Sprintf(
					"%s has a defect the data audit calls large enough to move a "+
						"metric on its own", t)
			default:
				// good and usable are the ordinary roughness of free daily
				// data, and warning about them would train the user to ignore
				// this channel entirely.
				continue
			}
			msg := lead
			if reason := data.QualityReason(q.Stats); reason != "" {
				msg += ": " + reason
			}
			msg += ". Nothing is excluded — this run includes it, and its " +
				"numbers include whatever the bars do."
			out = append(out, Diagnostic{
				Level: "warn", Portfolio: p.Pname, Message: msg,
			})
		}
	}
	return out
}
