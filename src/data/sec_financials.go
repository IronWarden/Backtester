package data

// Bulk XBRL fundamentals from SEC, with the date each figure was actually
// published.
//
// Two things the existing `financials` table cannot do. It starts in 2020-07,
// which is one macro regime, and it holds only tickers that still exist, so
// every factor study built on it is confined to five years of survivors. SEC's
// Financial Statement Data Sets go back to 2009, include the companies that
// later died, and — the part that matters most — carry `filed`, the real
// publication date. `PointInTimeFundamentals` currently has to approximate that
// as period end + 90 days whenever `earnings_calendar` has no entry.
//
// Measured on 2026Q1 (96,713 rows, 5,109 companies, 509 of them with no current
// ticker): the true lag for the figures a filing is actually reporting is a
// **median of 48 days**, p10 31, p90 62, max 90. So the 90-day fallback is not a
// typical lag, it is the worst case — it delays every fundamental by about six
// extra weeks. Conservative rather than biased, but it throws away real signal.
//
// `add_sec_financials.py` loads the table. Two shapes of row live in it and the
// difference matters:
//
//   - `period_end == filing_period` — the figure this filing is reporting.
//   - `period_end < filing_period` — a comparative. Every 10-K restates prior
//     years, so ~60% of rows are these. They are kept because their `filed`
//     date is honest: what a comparative tells you is that this figure was
//     public by then, which is exactly what a point-in-time query needs.
//
// Hence FirstKnown below: for each (company, metric, period) the earliest filing
// that carried it is the day that number became knowable, and that — not the
// period end — is the date a backtest may use it.
//
// Inert until the table exists.

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

// SECFundamental is one figure from one filing.
type SECFundamental struct {
	// CIK is the key SEC uses. Ticker is filled in only where the company still
	// has a current listing, so it is empty for exactly the delisted names.
	CIK         string
	Ticker      string
	CompanyName string
	SIC         string
	// Metric uses the same vocabulary as the `financials` table ("Total
	// Revenue", "Net Income", …) so the two can be compared over their overlap.
	// Tag is the raw XBRL tag it came from.
	Metric string
	Tag    string
	// PeriodEnd is the period the figure describes; Filed is when the filing
	// carrying it was published. FilingPeriod is the filing's own period, so
	// PeriodEnd == FilingPeriod marks a primary figure and PeriodEnd <
	// FilingPeriod marks a comparative.
	PeriodEnd    time.Time
	Filed        time.Time
	FilingPeriod time.Time
	Value        float64
	Form         string
	// Frequency is "instant" (a balance-sheet figure), "quarterly", or
	// "annual". Year-to-date partials are not loaded.
	Frequency string
	Accession string
}

// IsPrimary reports whether this is the figure the filing was reporting, rather
// than a prior period it restated for comparison.
func (f SECFundamental) IsPrimary() bool {
	return !f.FilingPeriod.IsZero() && f.PeriodEnd.Equal(f.FilingPeriod)
}

// PublicationLag is how long after the period end the figure was published.
// Meaningful only for a primary figure: a comparative from three years back
// would report a three-year "lag".
func (f SECFundamental) PublicationLag() time.Duration {
	return f.Filed.Sub(f.PeriodEnd)
}

// FundamentalKey identifies one figure independently of which filing carried it.
type FundamentalKey struct {
	CIK       string
	Metric    string
	PeriodEnd time.Time
}

// FirstKnown reduces rows to the earliest filing that carried each figure —
// which is the day that number became knowable, and therefore the first day a
// backtest may act on it. Later restatements of the same period are discarded
// here on purpose: acting on a restated figure at the original filing's date
// would be look-ahead bias wearing a plausible disguise.
//
// Pure, so the rule can be tested without a database. Ties (the same figure in
// two filings published the same day) keep the first row seen, which is stable
// because the loader inserts in quarter order.
func FirstKnown(rows []SECFundamental) map[FundamentalKey]SECFundamental {
	out := make(map[FundamentalKey]SECFundamental, len(rows))
	for _, r := range rows {
		key := FundamentalKey{CIK: r.CIK, Metric: r.Metric, PeriodEnd: r.PeriodEnd}
		if prev, ok := out[key]; ok && !r.Filed.Before(prev.Filed) {
			continue
		}
		out[key] = r
	}
	return out
}

// KnownOn returns the figures that were public on a date: the earliest filing of
// each figure, keeping only those filed on or before `when`. This is the
// point-in-time question — "what could a strategy have read that morning" — and
// it is the reason the comparatives are worth storing.
func KnownOn(rows []SECFundamental, when time.Time) map[FundamentalKey]SECFundamental {
	out := make(map[FundamentalKey]SECFundamental)
	for key, r := range FirstKnown(rows) {
		if !r.Filed.After(when) {
			out[key] = r
		}
	}
	return out
}

// LoadSECFundamentals reads the table for the given tickers, or every row when
// tickers is empty. Rows for delisted companies have no ticker, so they are
// reachable only by the empty-slice form or by CIK.
//
// Returns nil without error when the `sec_financials` table does not exist.
func LoadSECFundamentals(tickers []string) ([]SECFundamental, error) {
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	if !tableExists("sec_financials") {
		return nil, nil
	}

	query := `SELECT cik, ticker, company_name, sic, metric, tag,
	                 period_end, filed, filing_period, value, form,
	                 frequency, accession
	          FROM sec_financials`
	args := make([]any, 0, len(tickers))
	if len(tickers) > 0 {
		query += ` WHERE ticker IN (` + placeholderList(len(tickers)) + `)`
		for _, t := range tickers {
			args = append(args, t)
		}
	}
	query += ` ORDER BY cik, metric, period_end, filed`

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SECFundamental
	for rows.Next() {
		var f SECFundamental
		var ticker, name, sic, tag, form, frequency, accession sql.NullString
		var filingPeriod sql.NullTime
		var value sql.NullFloat64
		if err := rows.Scan(
			&f.CIK, &ticker, &name, &sic, &f.Metric, &tag,
			&f.PeriodEnd, &f.Filed, &filingPeriod, &value, &form,
			&frequency, &accession,
		); err != nil {
			log.Printf("scanning sec_financials: %v", err)
			continue
		}
		f.Ticker = ticker.String
		f.CompanyName = name.String
		f.SIC = sic.String
		f.Tag = tag.String
		f.FilingPeriod = filingPeriod.Time
		f.Value = value.Float64
		f.Form = form.String
		f.Frequency = frequency.String
		f.Accession = accession.String
		out = append(out, f)
	}
	return out, rows.Err()
}
