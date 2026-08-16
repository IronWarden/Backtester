package data

// Point-in-time access to the `financials` table.
//
// The table is keyed by FISCAL PERIOD END — its dates are overwhelmingly
// 12-31, 03-31, 06-30 and 09-30 — not by the date the figure was published.
// Joining it to prices on that date is look-ahead bias of the most damaging
// kind: it trades on Q4 numbers on December 31, four to eight weeks before
// anyone outside the company knew them. A factor backtest built that way
// looks excellent and means nothing.
//
// Everything here exists to make the unsafe join hard to write by accident.
// Read fundamentals through PointInTimeFundamentals, never from the raw table.
//
// Two lag rules, in order of preference:
//
//   1. The real report date, from `earnings_calendar` — the first report
//      timestamp strictly after the period end. Those timestamps are after
//      the close (16:00), so the first usable session is the NEXT day.
//      Available for ~1,000 of the ~6,900 tickers that have financials.
//   2. A fixed conservative lag otherwise. Late enough that essentially every
//      filer has reported, which is the right way to be wrong here: being
//      late costs a little signal, being early invents it.

import (
	"database/sql"
	"log"
	"sort"
	"time"
)

// FixedReportLagDays is the fallback publication lag for a fiscal period whose
// real report date is unknown. US quarterly filers are required to file within
// 40–45 days of period end and annual filers within 60–90, so 90 days clears
// essentially all of them. Deliberately conservative: a lag that is too long
// loses a little signal, while one that is too short manufactures it.
const FixedReportLagDays = 90

// LagSource records which rule dated a fundamental, so a result built mostly
// on the fallback can be read differently from one built on real report dates.
type LagSource string

const (
	// LagFromEarningsCalendar means the date came from a real report timestamp.
	LagFromEarningsCalendar LagSource = "earnings_calendar"
	// LagFromFixedWindow means FixedReportLagDays was applied instead.
	LagFromFixedWindow LagSource = "fixed"
)

// RawFundamental is one row of `financials` exactly as stored: a value stamped
// with the END of the fiscal period it describes, which is NOT when it became
// public.
type RawFundamental struct {
	Ticker    string
	Metric    string
	PeriodEnd time.Time
	Value     float64
	Frequency string
}

// FundamentalPoint is a fundamental value together with the first date it may
// legitimately be used in a simulation. KnownFrom is the whole point of this
// type: a strategy may read this value on or after that date and never before.
type FundamentalPoint struct {
	RawFundamental
	KnownFrom time.Time
	LagSource LagSource
}

// ApplyPublicationLag dates each raw fundamental with the day it became
// knowable. reportDates maps a ticker to its known report timestamps, in any
// order; a ticker absent from the map falls back to the fixed lag.
//
// Pure, so the lag rules can be tested without a database — which matters,
// because these rules are the difference between a factor study and a fiction.
//
// Duplicate rows (the raw table contains them — the same ticker, metric and
// period end appearing twice with identical values) are collapsed, keeping the
// first occurrence.
func ApplyPublicationLag(
	rows []RawFundamental, reportDates map[string][]time.Time,
) []FundamentalPoint {
	// Sort each ticker's report dates once so the lookup below can stop at the
	// first one past the period end.
	sorted := make(map[string][]time.Time, len(reportDates))
	for ticker, dates := range reportDates {
		cp := append([]time.Time(nil), dates...)
		sort.Slice(cp, func(i, j int) bool { return cp[i].Before(cp[j]) })
		sorted[ticker] = cp
	}

	seen := make(map[string]struct{}, len(rows))
	out := make([]FundamentalPoint, 0, len(rows))
	for _, r := range rows {
		key := r.Ticker + "\x00" + r.Metric + "\x00" +
			r.PeriodEnd.Format("2006-01-02") + "\x00" + r.Frequency
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}

		known, src := knownFrom(r, sorted[r.Ticker])
		out = append(out, FundamentalPoint{
			RawFundamental: r,
			KnownFrom:      known,
			LagSource:      src,
		})
	}
	return out
}

// knownFrom returns the first date a period's figures may be used, and which
// rule produced it. reports must be sorted ascending.
func knownFrom(
	r RawFundamental, reports []time.Time,
) (time.Time, LagSource) {
	for _, t := range reports {
		if t.After(r.PeriodEnd) {
			// Reports land after the close, so the earliest session that may
			// act on them is the following day. Truncating to the day first
			// keeps the result a date rather than a timestamp.
			day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
			return day.AddDate(0, 0, 1), LagFromEarningsCalendar
		}
	}
	return r.PeriodEnd.AddDate(0, 0, FixedReportLagDays), LagFromFixedWindow
}

// KnownAt filters points to those already public on the given date. This is
// the call a strategy makes: it cannot see a figure whose KnownFrom is later,
// however tempting the period end looks.
func KnownAt(points []FundamentalPoint, day time.Time) []FundamentalPoint {
	out := make([]FundamentalPoint, 0, len(points))
	for _, p := range points {
		if !p.KnownFrom.After(day) {
			out = append(out, p)
		}
	}
	return out
}

// LatestKnownAt returns, per ticker and metric, the most recent value that was
// public on the given date. This is what a cross-sectional factor actually
// wants: the newest figure a trader could have had in hand that morning.
func LatestKnownAt(
	points []FundamentalPoint, day time.Time,
) map[string]map[string]FundamentalPoint {
	out := make(map[string]map[string]FundamentalPoint)
	for _, p := range KnownAt(points, day) {
		byMetric, ok := out[p.Ticker]
		if !ok {
			byMetric = make(map[string]FundamentalPoint)
			out[p.Ticker] = byMetric
		}
		// Later period wins; on a tie the later KnownFrom wins, which favours
		// a restatement over the original.
		if cur, exists := byMetric[p.Metric]; exists {
			if p.PeriodEnd.Before(cur.PeriodEnd) {
				continue
			}
			if p.PeriodEnd.Equal(cur.PeriodEnd) && p.KnownFrom.Before(cur.KnownFrom) {
				continue
			}
		}
		byMetric[p.Metric] = p
	}
	return out
}

// PointInTimeFundamentals loads the named metrics for the given tickers and
// dates them with ApplyPublicationLag. Periods ending after endTime are
// dropped; periods before startTime are kept, because the most recent figure
// known at the start of a window is usually from the quarter before it.
//
// Returns an empty slice rather than an error on a query failure, matching the
// rest of this package: a missing optional dataset must not fail a backtest.
func PointInTimeFundamentals(
	tickers []string, metrics []string, startTime, endTime time.Time,
) []FundamentalPoint {
	if db == nil || len(tickers) == 0 || len(metrics) == 0 {
		return nil
	}
	rows, err := queryRawFundamentals(tickers, metrics, endTime)
	if err != nil {
		log.Printf("point-in-time fundamentals: %v; continuing without them", err)
		return nil
	}
	reports, err := queryReportDates(tickers)
	if err != nil {
		// Not fatal: every ticker then falls back to the fixed lag, which is
		// conservative rather than wrong.
		log.Printf("earnings calendar unavailable: %v; using the %d-day "+
			"fixed lag for every ticker", err, FixedReportLagDays)
		reports = map[string][]time.Time{}
	}
	return ApplyPublicationLag(rows, reports)
}

// queryRawFundamentals reads the raw table. It is deliberately unexported:
// callers must go through PointInTimeFundamentals so the lag is never skipped.
func queryRawFundamentals(
	tickers, metrics []string, endTime time.Time,
) ([]RawFundamental, error) {
	query := `SELECT ticker, metric, date, value, frequency
	          FROM financials
	          WHERE ticker IN (` + placeholderList(len(tickers)) + `)
	            AND metric IN (` + placeholderList(len(metrics)) + `)
	            AND date <= CAST(? AS TIMESTAMP_NS)
	            AND value IS NOT NULL
	          ORDER BY ticker, metric, date`

	args := make([]any, 0, len(tickers)+len(metrics)+1)
	for _, t := range tickers {
		args = append(args, t)
	}
	for _, m := range metrics {
		args = append(args, m)
	}
	args = append(args, endTime.Format("2006-01-02 15:04:05.000000000"))

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []RawFundamental
	for rows.Next() {
		var r RawFundamental
		var value sql.NullFloat64
		var frequency sql.NullString
		if err := rows.Scan(
			&r.Ticker, &r.Metric, &r.PeriodEnd, &value, &frequency,
		); err != nil {
			log.Printf("scanning fundamental: %v", err)
			continue
		}
		if !value.Valid {
			continue
		}
		r.Value = value.Float64
		r.Frequency = frequency.String
		out = append(out, r)
	}
	return out, rows.Err()
}

// queryReportDates reads the real publication timestamps, which exist for a
// minority of tickers.
func queryReportDates(tickers []string) (map[string][]time.Time, error) {
	query := `SELECT Ticker, Date FROM earnings_calendar
	          WHERE Ticker IN (` + placeholderList(len(tickers)) + `)
	          ORDER BY Ticker, Date`
	args := make([]any, len(tickers))
	for i, t := range tickers {
		args[i] = t
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string][]time.Time)
	for rows.Next() {
		var ticker string
		var date time.Time
		if err := rows.Scan(&ticker, &date); err != nil {
			log.Printf("scanning report date: %v", err)
			continue
		}
		out[ticker] = append(out[ticker], date)
	}
	return out, rows.Err()
}

// placeholderList builds "?, ?, ?" for an IN clause of n values.
func placeholderList(n int) string {
	if n <= 0 {
		return "NULL"
	}
	out := make([]byte, 0, n*3)
	for i := 0; i < n; i++ {
		if i > 0 {
			out = append(out, ',', ' ')
		}
		out = append(out, '?')
	}
	return string(out)
}
