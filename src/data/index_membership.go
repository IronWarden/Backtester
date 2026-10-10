package data

// Who was in the index on a given date.
//
// The larger half of survivorship bias is selection, not missing prices. The
// default config backtests AAPL/MSFT/GOOGL/AMZN/NVDA over 2015-2025 — a basket
// chosen in 2026 with knowledge of who won — and no amount of walk-forward
// machinery repairs a universe picked that way. Point-in-time membership is the
// fix for that half, and it works without the dead names' prices: a backtest
// that knows `SIVB` was an S&P 500 member until 2023-03-15 is asking an honest
// question even if it has no bars for what happened next.
//
// `add_index_membership.py` reconstructs the spans from two free Wikipedia
// tables (today's constituents, plus 408 dated add/remove events) by undoing
// each change backwards from the present.
//
// HOW FAR BACK IT MEANS ANYTHING. The index really changes 20-25 constituents a
// year. The source records 16-30/year from 2011, 8-13/year for 2007-2010, and
// almost nothing before 2007 — so every span carries a `Confidence`, and the
// reconstruction stops at a horizon rather than pretending to reach 1976. The
// loader's own check is the member count per year, which held 501-510 across
// 2007-2026 on the 2026-08-17 run; a year that does not hold ~500 is a year
// whose changes are missing.
//
// This is a scrape of a community-maintained page, not a vendor feed. Every row
// records its source and it should never be presented as authoritative index
// data.
//
// Inert until the table exists, like every other optional dataset here.

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"time"
)

// MembershipSpan is one continuous stretch of index membership. A ticker can
// have several: leaving and rejoining the index is common.
type MembershipSpan struct {
	Index    string
	Ticker   string
	Security string
	Start    time.Time
	// End is 9999-12-31 for a current member, so a date comparison needs no
	// special case for "still in".
	End time.Time
	// StartIsHorizon marks a span whose beginning is the reconstruction's
	// horizon rather than a recorded addition: the ticker was already a member
	// when the reliable history begins. "At least since" — not "joined then".
	StartIsHorizon bool
	// Confidence is how densely the source records changes for the year the
	// span starts in: high, medium, or low.
	Confidence string
	Source     string
}

// Contains reports whether the span covers a date, inclusive at both ends.
func (s MembershipSpan) Contains(when time.Time) bool {
	return !when.Before(s.Start) && !when.After(s.End)
}

// MembersOn returns the tickers whose spans cover a date, sorted. Pure, so the
// boundary behaviour can be tested without a database — and the boundaries are
// the whole point: a backtest that starts on the day a company was removed
// must not hold it, and one that starts on the day a company was added must.
func MembersOn(spans []MembershipSpan, when time.Time) []string {
	seen := make(map[string]bool)
	for _, s := range spans {
		if s.Contains(when) {
			seen[s.Ticker] = true
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// LoadIndexMembership reads every span for one index (e.g. "SP500"), or for all
// of them when index is empty.
//
// Returns nil without error when the `index_membership` table does not exist,
// which is the state of any database where `add_index_membership.py` has not
// been run.
func LoadIndexMembership(index string) ([]MembershipSpan, error) {
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	if !tableExists("index_membership") {
		return nil, nil
	}

	query := `SELECT index_name, ticker, security, start_date, end_date,
	                 start_is_horizon, confidence, source
	          FROM index_membership`
	args := []any{}
	if index != "" {
		query += ` WHERE index_name = ?`
		args = append(args, index)
	}
	query += ` ORDER BY ticker, start_date`

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []MembershipSpan
	for rows.Next() {
		var s MembershipSpan
		var security, confidence, source sql.NullString
		var horizon sql.NullBool
		if err := rows.Scan(
			&s.Index, &s.Ticker, &security, &s.Start, &s.End,
			&horizon, &confidence, &source,
		); err != nil {
			log.Printf("scanning index membership: %v", err)
			continue
		}
		s.Security = security.String
		s.StartIsHorizon = horizon.Bool
		s.Confidence = confidence.String
		s.Source = source.String
		out = append(out, s)
	}
	return out, rows.Err()
}

// IndexMembersOn is the call a caller actually makes: the index's membership on
// one date, straight from the database.
//
// With no `index_membership` table it returns nothing and no error, so a caller
// gets an empty universe rather than a failure — the same degradation as an
// uncoverable benchmark.
func IndexMembersOn(index string, when time.Time) ([]string, error) {
	spans, err := LoadIndexMembership(index)
	if err != nil {
		return nil, err
	}
	return MembersOn(spans, when), nil
}
