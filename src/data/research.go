package data

// Company research views (T142a): DES, FA and ERN, read entirely from tables
// the database already holds. No network, nothing written.
//
// The point of the research tab is information without noise, so every figure
// here comes from data rather than prose and carries the date that makes it
// honest: a close says which day it closed, a financial figure says the day it
// was FILED, an earnings date says whether it fell before the open or after
// the close. The shaping is pure (FinancialsTable, EarningsReactions) so the
// rules can be tested without a database; the loaders are thin.

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

// PriceSummary is what the database's own bars say about a ticker. It is
// history, not a quote: LastDate is the session LastClose comes from.
type PriceSummary struct {
	FirstDate time.Time
	LastDate  time.Time
	Bars      int
	LastClose float64
	// The highest and lowest CLOSE over the 252 sessions ending LastDate
	// (fewer when the series is shorter). Closes, not intraday extremes.
	High52 float64
	Low52  float64
}

// CloseBar is one session's close, the only field the research views read.
type CloseBar struct {
	Date  time.Time
	Close float64
}

// LoadCloses returns a ticker's closes, oldest first, or nil when it has none.
func LoadCloses(ticker string) ([]CloseBar, error) {
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	rows, err := db.Query(`SELECT Date, Close FROM stock_data_optimized
		WHERE Ticker = ? ORDER BY Date`, ticker)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CloseBar
	for rows.Next() {
		var b CloseBar
		var c sql.NullFloat64
		if err := rows.Scan(&b.Date, &c); err != nil {
			return nil, err
		}
		if !c.Valid {
			continue
		}
		b.Close = c.Float64
		out = append(out, b)
	}
	return out, rows.Err()
}

// SummarizePrices is the DES price line. ok is false for an empty series.
func SummarizePrices(bars []CloseBar) (PriceSummary, bool) {
	if len(bars) == 0 {
		return PriceSummary{}, false
	}
	last := bars[len(bars)-1]
	s := PriceSummary{
		FirstDate: bars[0].Date, LastDate: last.Date, Bars: len(bars),
		LastClose: last.Close, High52: last.Close, Low52: last.Close,
	}
	from := len(bars) - 252
	if from < 0 {
		from = 0
	}
	for _, b := range bars[from:] {
		if b.Close > s.High52 {
			s.High52 = b.Close
		}
		if b.Close < s.Low52 {
			s.Low52 = b.Close
		}
	}
	return s, true
}

// ---------------------------------------------------------------------------
// FA: reported financials by period

// FinancialsView is a metric-by-period table of figures as first filed.
type FinancialsView struct {
	// CIK the figures belong to, and the name the filer reported under.
	CIK     string
	Company string
	// Frequency is "annual" or "quarterly"; Periods are period ends, newest
	// first.
	Frequency string
	Periods   []time.Time
	Rows      []FinancialsRow
}

// FinancialsRow is one metric across the view's periods; a nil cell is a
// figure the filings never carried.
type FinancialsRow struct {
	Metric string
	Cells  []*FinancialsCell
}

// FinancialsCell is one figure and the filing that first published it.
type FinancialsCell struct {
	Value float64
	Filed time.Time
	Form  string
}

// FinancialsTable shapes SEC rows for one company into the FA table: the
// newest `periods` period ends of the given frequency, each metric in
// SECMetrics order, each figure as FIRST filed (FirstKnown) — a later
// restatement is not shown, which is the point-in-time view and the one a
// backtest sees. Balance-sheet figures ("instant") fill the columns of either
// frequency. A metric with no figure in any shown period is left out.
//
// rows may span several CIKs (a ticker reused, or a holding-company
// reorganisation); preferCIK picks one, and when it is empty or absent the
// CIK with the most recent filing wins.
func FinancialsTable(rows []SECFundamental, frequency string, periods int, preferCIK string) FinancialsView {
	view := FinancialsView{Frequency: frequency}
	if len(rows) == 0 || periods <= 0 {
		return view
	}
	cik, ok := matchCIK(rows, preferCIK)
	if !ok {
		var latest time.Time
		for _, r := range rows {
			if r.Filed.After(latest) {
				latest, cik = r.Filed, r.CIK
			}
		}
	}
	view.CIK = cik
	// The name as of the newest filing: a renamed company reads under its
	// current name.
	var mine []SECFundamental
	var newest time.Time
	for _, r := range rows {
		if r.CIK != cik {
			continue
		}
		mine = append(mine, r)
		if r.CompanyName != "" && !r.Filed.Before(newest) {
			newest, view.Company = r.Filed, r.CompanyName
		}
	}
	first := FirstKnown(mine)

	seen := map[time.Time]bool{}
	for k := range first {
		if k.Frequency == frequency {
			seen[k.PeriodEnd] = true
		}
	}
	for p := range seen {
		view.Periods = append(view.Periods, p)
	}
	sort.Slice(view.Periods, func(i, j int) bool { return view.Periods[i].After(view.Periods[j]) })
	if len(view.Periods) > periods {
		view.Periods = view.Periods[:periods]
	}

	for _, metric := range SECMetrics {
		row := FinancialsRow{Metric: metric, Cells: make([]*FinancialsCell, len(view.Periods))}
		any := false
		for i, p := range view.Periods {
			for _, freq := range []string{frequency, "instant"} {
				if f, ok := first[FundamentalKey{CIK: cik, Metric: metric, PeriodEnd: p, Frequency: freq}]; ok {
					row.Cells[i] = &FinancialsCell{Value: f.Value, Filed: f.Filed, Form: f.Form}
					any = true
					break
				}
			}
		}
		if any {
			view.Rows = append(view.Rows, row)
		}
	}
	return view
}

// matchCIK finds the rows' spelling of a CIK. company_profile stores it
// zero-padded to ten digits ("0000320193") and sec_financials does not
// ("320193"), so they are compared without leading zeros.
func matchCIK(rows []SECFundamental, cik string) (string, bool) {
	want := strings.TrimLeft(cik, "0")
	if want == "" {
		return "", false
	}
	for _, r := range rows {
		if strings.TrimLeft(r.CIK, "0") == want {
			return r.CIK, true
		}
	}
	return "", false
}

// ---------------------------------------------------------------------------
// ERN: report dates and the price's answer

// EarningsReaction is one report and how the price met it.
type EarningsReaction struct {
	Reported time.Time
	// Timing is "after close", "before open" or "during the session", from
	// the report's timestamp in US Eastern time.
	Timing string
	// From and To are the two closes the move is measured between: the last
	// close the market had BEFORE the news, and the first close AFTER it.
	// Zero when the bars do not reach (a future report, or a gap in history).
	FromDate, ToDate time.Time
	Move             float64 // To / From - 1
	Upcoming         bool
}

var eastern = func() *time.Location {
	if loc, err := time.LoadLocation("America/New_York"); err == nil {
		return loc
	}
	return time.FixedZone("ET", -5*3600)
}()

// EarningsReactions pairs each report with the move across it, newest first.
// The timestamps in earnings_calendar are stored without a zone and are US
// Eastern wall-clock times (16:00 = at the close), so they are read as such.
// A report at or after 16:00 is met by the NEXT session's close; one before
// 09:30 by that day's close; one in between is "during the session" and is
// measured from the prior close to that day's close, which mixes some
// pre-news trading in and says so.
func EarningsReactions(reports []time.Time, bars []CloseBar, now time.Time) []EarningsReaction {
	out := make([]EarningsReaction, 0, len(reports))
	for _, rep := range reports {
		wall := time.Date(rep.Year(), rep.Month(), rep.Day(), rep.Hour(), rep.Minute(), 0, 0, eastern)
		day := time.Date(rep.Year(), rep.Month(), rep.Day(), 0, 0, 0, 0, time.UTC)
		mins := rep.Hour()*60 + rep.Minute()
		r := EarningsReaction{Reported: rep, Upcoming: wall.After(now)}
		var fromIdx, toIdx int
		switch {
		case mins >= 16*60:
			r.Timing = "after close"
			// The day's close (or the last before it, on a holiday) is the
			// last price without the news; the next session's is the answer.
			fromIdx = lastOnOrBefore(bars, day)
			toIdx = fromIdx + 1
		case mins < 9*60+30:
			r.Timing = "before open"
			toIdx = firstOnOrAfter(bars, day)
			fromIdx = toIdx - 1
		default:
			r.Timing = "during the session"
			toIdx = firstOnOrAfter(bars, day)
			fromIdx = toIdx - 1
		}
		if !r.Upcoming && fromIdx >= 0 && toIdx > fromIdx && toIdx < len(bars) && bars[fromIdx].Close > 0 {
			r.FromDate, r.ToDate = bars[fromIdx].Date, bars[toIdx].Date
			r.Move = bars[toIdx].Close/bars[fromIdx].Close - 1
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Reported.After(out[j].Reported) })
	return out
}

func dayOf(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// lastOnOrBefore is the index of the last bar dated on or before day, or -1.
func lastOnOrBefore(bars []CloseBar, day time.Time) int {
	i := sort.Search(len(bars), func(i int) bool { return dayOf(bars[i].Date).After(day) })
	return i - 1
}

// firstOnOrAfter is the index of the first bar dated on or after day, or
// len(bars).
func firstOnOrAfter(bars []CloseBar, day time.Time) int {
	return sort.Search(len(bars), func(i int) bool { return !dayOf(bars[i].Date).Before(day) })
}

// LoadEarningsDates returns a ticker's report timestamps, oldest first, or nil
// when the earnings_calendar table is absent.
func LoadEarningsDates(ticker string) ([]time.Time, error) {
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	if !tableExists("earnings_calendar") {
		return nil, nil
	}
	dates, err := queryReportDates([]string{ticker})
	if err != nil {
		return nil, err
	}
	return dates[ticker], nil
}

// MembershipFor is a ticker's spans in an index, oldest first.
func MembershipFor(spans []MembershipSpan, ticker string) []MembershipSpan {
	var out []MembershipSpan
	for _, s := range spans {
		if s.Ticker == ticker {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}
