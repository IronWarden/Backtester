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
	// Frequency is "instant" (a balance-sheet figure), "quarterly", "annual",
	// or "year_to_date" (six or nine months, which is the only way a 10-Q
	// reports its cash flow statement). Qtrs is the span in quarters: 0 for an
	// instant, 1 a quarter, 2-3 year-to-date, 4 a year.
	Frequency string
	Qtrs      int
	// TagRank orders the XBRL tags that map to one Metric, 0 being preferred.
	// A filer can report two of them for the same period with different
	// values (LongTermDebt includes the current portion, LongTermDebtNoncurrent
	// does not), so without it the figure would depend on row order.
	TagRank   int
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
// Frequency is part of the identity: a Q2 10-Q reports net income for the three
// months AND the six months ending on the same date, and those are two figures,
// not a figure and its restatement.
type FundamentalKey struct {
	CIK       string
	Metric    string
	PeriodEnd time.Time
	Frequency string
}

// FirstKnown reduces rows to the earliest filing that carried each figure —
// which is the day that number became knowable, and therefore the first day a
// backtest may act on it. Later restatements of the same period are discarded
// here on purpose: acting on a restated figure at the original filing's date
// would be look-ahead bias wearing a plausible disguise.
//
// Two rows filed the same day — in practice two tags for one metric in one
// filing — are settled by TagRank, lower preferred. Earliness still comes
// first: a figure knowable under a less-preferred tag is not pushed later just
// because a better tag carried it afterwards. Exact ties keep the first row
// seen, which is stable because the loader inserts in quarter order.
//
// Pure, so the rule can be tested without a database.
func FirstKnown(rows []SECFundamental) map[FundamentalKey]SECFundamental {
	out := make(map[FundamentalKey]SECFundamental, len(rows))
	for _, r := range rows {
		key := FundamentalKey{CIK: r.CIK, Metric: r.Metric, PeriodEnd: r.PeriodEnd,
			Frequency: r.Frequency}
		if prev, ok := out[key]; ok {
			if r.Filed.After(prev.Filed) {
				continue
			}
			if r.Filed.Equal(prev.Filed) && r.TagRank >= prev.TagRank {
				continue
			}
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

	query := `SELECT ` + secColumns() + ` FROM sec_financials`
	args := make([]any, 0, len(tickers))
	if len(tickers) > 0 {
		query += ` WHERE ticker IN (` + placeholderList(len(tickers)) + `)`
		for _, t := range tickers {
			args = append(args, t)
		}
	}
	query += ` ORDER BY cik, metric, period_end, filed`
	return querySECRows(query, args)
}

// secColumns is the select list every sec_financials reader scans with
// querySECRows, in that function's order.
func secColumns() string {
	// tag_rank and qtrs arrived with the 2026-09 loader; a table loaded before
	// then has neither, and reads as rank 0 / qtrs unknown rather than failing.
	extra := `, 0, -1`
	if columnExists("sec_financials", "tag_rank") {
		extra = `, tag_rank, qtrs`
	}
	return `cik, ticker, company_name, sic, metric, tag,
	        period_end, filed, filing_period, value, form,
	        frequency, accession` + extra
}

// querySECRows runs a query selecting secColumns() and scans every row.
func querySECRows(query string, args []any) ([]SECFundamental, error) {
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
		var tagRank, qtrs sql.NullInt64
		if err := rows.Scan(
			&f.CIK, &ticker, &name, &sic, &f.Metric, &tag,
			&f.PeriodEnd, &f.Filed, &filingPeriod, &value, &form,
			&frequency, &accession, &tagRank, &qtrs,
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
		f.TagRank = int(tagRank.Int64)
		f.Qtrs = int(qtrs.Int64)
		if !qtrs.Valid {
			f.Qtrs = -1
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
