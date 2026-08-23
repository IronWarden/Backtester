package data

import (
	"log"
	"time"
)

// CPI, for reporting returns in real terms.
//
// The series is CPIAUCSL — Consumer Price Index for All Urban Consumers, All
// Items, seasonally adjusted, the standard FRED deflator. Seasonal adjustment
// is the right choice here precisely because a backtest window is rarely a
// whole number of years: the unadjusted series would put a seasonal artefact
// into whichever months happen to be the endpoints.
//
// The index base (1982-84 = 100) is irrelevant — only ratios are ever taken.
//
// **On the publication-lag warning elsewhere in this codebase.** `regime.go`
// refuses to use CPI, and the assistant prompts warn that
// `economic_indicators.Date` is the PERIOD and not the release. Both are
// right, and neither applies here. The distinction is decision versus
// description: a strategy that TRADES on CPI must lag it or it peeks, whereas
// a real return reported after a finished window is a restatement of history
// in constant money, which is how every index provider quotes real returns.
// Nothing in the simulation reads this, so nothing can peek. What is true is
// that CPI is revised, so a real return can move slightly between runs months
// apart; that is a property of the statistic, not a bug here.
const cpiSeriesID = "CPIAUCSL"

// CPIPoint is one monthly index reading. Date is the first of the month the
// reading describes, which is the FRED convention for monthly series.
type CPIPoint struct {
	Date  time.Time
	Value float64
}

// GetCPISeries returns every monthly CPI reading in the database, sorted by
// date, or nil when the table or the series is absent. It takes no window: see
// below for why the whole series is read.
//
// Nil is not an error: `economic_indicators` is optional, exactly like
// `company_profile`, and a database without it must still run backtests. The
// caller reports "no real return available" rather than failing.
//
// **No date filter, deliberately.** The whole series is under a thousand
// monthly rows, so pushing a range into SQL buys nothing and costs a
// dependency on the `Date` column's declared type — DATE, TIMESTAMP and
// VARCHAR all appear in tables loaded by different scripts here, and a
// comparison that silently matches nothing is indistinguishable from "no
// data" to every caller. Read it all, filter in Go.
//
// For the same reason the date is scanned through `any` and normalised, rather
// than straight into a time.Time that a VARCHAR column would reject.
func GetCPISeries() []CPIPoint {
	if db == nil || !tableExists("economic_indicators") {
		return nil
	}
	rows, err := db.Query(
		`SELECT Date, value FROM economic_indicators WHERE series_id = ? ORDER BY Date`,
		cpiSeriesID)
	if err != nil {
		log.Printf("querying CPI: %v — real returns will not be reported", err)
		return nil
	}
	defer rows.Close()

	var out []CPIPoint
	for rows.Next() {
		var raw any
		var value float64
		if err := rows.Scan(&raw, &value); err != nil {
			log.Printf("scanning CPI row: %v", err)
			return nil
		}
		d, ok := asDate(raw)
		if !ok || value <= 0 {
			continue
		}
		out = append(out, CPIPoint{Date: d, Value: value})
	}
	if err := rows.Err(); err != nil {
		log.Printf("reading CPI rows: %v", err)
		return nil
	}
	if len(out) == 0 {
		// Distinct from "no table": the table is there and the series is not,
		// which means the loader never ran for it or the id changed.
		log.Printf("economic_indicators has no usable %s rows — "+
			"real returns will not be reported", cpiSeriesID)
	}
	return out
}

// asDate normalises whatever the driver hands back for a date column into a
// UTC time. DuckDB returns time.Time for DATE and TIMESTAMP; a column loaded
// as text comes back as a string or []byte.
func asDate(raw any) (time.Time, bool) {
	switch v := raw.(type) {
	case time.Time:
		return v.UTC(), true
	case string:
		return parseDateish(v)
	case []byte:
		return parseDateish(string(v))
	}
	return time.Time{}, false
}

func parseDateish(s string) (time.Time, bool) {
	for _, layout := range []string{
		"2006-01-02", "2006-01-02 15:04:05", time.RFC3339,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// CPIForMonth returns the reading for the calendar month containing date.
//
// Exact month match, deliberately, rather than "the most recent reading on or
// before". A window ending in August when CPI only runs to June would
// otherwise be deflated by four months less inflation than it actually
// experienced, which OVERSTATES the real return — silently, plausibly, and in
// the flattering direction. Missing is reported as missing.
func CPIForMonth(series []CPIPoint, date time.Time) (float64, bool) {
	y, m := date.UTC().Year(), date.UTC().Month()
	for _, p := range series {
		d := p.Date.UTC()
		if d.Year() == y && d.Month() == m {
			return p.Value, true
		}
	}
	return 0, false
}
