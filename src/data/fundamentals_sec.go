package data

// Point-in-time fundamentals whose VALUES come from `sec_financials`, not just
// their dates (T139).
//
// PointInTimeFundamentals reads `financials`, which begins in 2020-07 and holds
// only companies still listed, and uses the SEC table merely to date it. This
// reads the figures themselves from SEC: 2009 onward, every metric the loader
// keeps. A run opts in with `FundamentalSource = "sec"`; without it nothing here
// is called and every existing number is unchanged.
//
// What a point means here, and why it differs from the `financials` reader:
//
//   - The value is the FIRST filing of each figure (FirstKnown), dated the day
//     after that filing. A later restatement is never substituted back, which
//     is the whole point-in-time guarantee.
//   - Frequency is exactly "annual" (a full fiscal year) or "quarterly" (one
//     quarter), derived from SEC's own qtrs field rather than a label, so a
//     year can never masquerade as a quarter — the T108 defect `financials`
//     has. Year-to-date rows (six and nine months) are left out: they are
//     neither, and mixing them in is what T108 is about.
//   - A balance-sheet figure is a level, not a flow, so it has no frequency of
//     its own. It is emitted as "quarterly" (every balance sheet, the freshest
//     reading) and, when a 10-K first published it, ALSO as "annual" (the
//     fiscal-year-end balance sheet, which lines up with an annual flow). Both
//     copies carry Instant.
//   - Values are AS REPORTED. SEC does not restate share counts or per-share
//     figures for a later split, so "Ordinary Shares Number", the EPS metrics
//     and "Diluted Average Shares" jump at every split. Ratios of two dollar
//     figures are unaffected.
//   - Only rows with a current ticker are reachable. Delisted companies are in
//     the table with ticker NULL, and joining them to a price series is T51.

import (
	"log"
	"sort"
	"time"
)

// SourceSEC marks a FundamentalPoint read from `sec_financials`.
const SourceSEC = "sec"

// SECMetrics is every metric name `add_sec_financials.py` writes, in the
// vocabulary of the `financials` table. A test reads the loader's TAGS list and
// fails if the two drift apart, so a config can be checked against this without
// a database.
var SECMetrics = []string{
	"Total Revenue",
	"Cost Of Revenue",
	"Gross Profit",
	"Research And Development",
	"Operating Income",
	"Interest Expense",
	"Pretax Income",
	"Tax Provision",
	"Net Income",
	"Diluted EPS",
	"Basic EPS",
	"Diluted Average Shares",
	"Operating Cash Flow",
	"Capital Expenditure",
	"Depreciation And Amortization",
	"Cash Dividends Paid",
	"Repurchase Of Capital Stock",
	"Total Assets",
	"Current Assets",
	"Total Liabilities Net Minority Interest",
	"Current Liabilities",
	"Stockholders Equity",
	"Cash And Cash Equivalents",
	"Accounts Receivable",
	"Inventory",
	"Net PPE",
	"Goodwill",
	"Retained Earnings",
	"Long Term Debt",
	"Ordinary Shares Number",
}

// IsSECMetric reports whether the SEC loader writes a metric of this name.
func IsSECMetric(metric string) bool {
	for _, m := range SECMetrics {
		if m == metric {
			return true
		}
	}
	return false
}

// SECPoints turns raw `sec_financials` rows into point-in-time fundamentals:
// the first filing of each figure, known from the day after it was filed.
//
// Pure, so the rules above can be tested without a database. The result is
// sorted by ticker, metric, frequency and period end, so it does not depend on
// map order.
func SECPoints(rows []SECFundamental) []FundamentalPoint {
	out := make([]FundamentalPoint, 0, len(rows))
	for _, f := range FirstKnown(rows) {
		if f.Ticker == "" || !f.Filed.After(f.PeriodEnd) {
			// No symbol to join to, or a filing dated on or before the period
			// it reports — not a figure anyone could have had in hand.
			continue
		}
		filed := time.Date(f.Filed.Year(), f.Filed.Month(), f.Filed.Day(),
			0, 0, 0, 0, f.Filed.Location())
		p := FundamentalPoint{
			RawFundamental: RawFundamental{
				Ticker: f.Ticker, Metric: f.Metric, PeriodEnd: f.PeriodEnd,
				Value: f.Value,
			},
			// From the following session, for the reason knownFrom gives:
			// EDGAR accepts filings into the evening.
			KnownFrom: filed.AddDate(0, 0, 1),
			LagSource: LagFromSECFiling,
			Source:    SourceSEC,
		}
		switch f.Frequency {
		case "annual", "quarterly":
			p.Frequency = f.Frequency
			out = append(out, p)
		case "instant":
			p.Instant = true
			p.Frequency = "quarterly"
			out = append(out, p)
			if f.Form == "10-K" {
				p.Frequency = "annual"
				out = append(out, p)
			}
		}
		// year_to_date, and anything a future loader adds, is dropped.
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Ticker != b.Ticker {
			return a.Ticker < b.Ticker
		}
		if a.Metric != b.Metric {
			return a.Metric < b.Metric
		}
		if a.Frequency != b.Frequency {
			return a.Frequency < b.Frequency
		}
		return a.PeriodEnd.Before(b.PeriodEnd)
	})
	return out
}

// SECPointInTimeFundamentals is PointInTimeFundamentals with the values, not
// only the dates, taken from `sec_financials`. Periods ending after endTime are
// dropped; earlier ones are kept for the same reason the other reader keeps
// them, and because a year-over-year comparison needs the year before.
//
// Returns nil when the table is absent or the query fails, matching the rest
// of this package: a missing optional dataset must not fail a backtest.
func SECPointInTimeFundamentals(
	tickers []string, metrics []string, startTime, endTime time.Time,
) []FundamentalPoint {
	if db == nil || len(tickers) == 0 || len(metrics) == 0 {
		return nil
	}
	if !tableExists("sec_financials") {
		log.Printf("FundamentalSource = %q but the database has no "+
			"sec_financials table; run add_sec_financials.py", SourceSEC)
		return nil
	}
	query := `SELECT ` + secColumns() + `
	          FROM sec_financials
	          WHERE ticker IN (` + placeholderList(len(tickers)) + `)
	            AND metric IN (` + placeholderList(len(metrics)) + `)
	            AND frequency IN ('annual', 'quarterly', 'instant')
	            AND period_end <= CAST(? AS DATE)
	            AND value IS NOT NULL
	          ORDER BY cik, metric, period_end, filed`
	args := make([]any, 0, len(tickers)+len(metrics)+1)
	for _, t := range tickers {
		args = append(args, t)
	}
	for _, m := range metrics {
		args = append(args, m)
	}
	args = append(args, endTime.Format("2006-01-02"))

	rows, err := querySECRows(query, args)
	if err != nil {
		log.Printf("SEC fundamentals: %v; continuing without them", err)
		return nil
	}
	return SECPoints(rows)
}
