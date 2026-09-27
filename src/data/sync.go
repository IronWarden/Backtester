package data

// Deciding what a price refresh should actually fetch.
//
// Refreshing the price table one ticker at a time is what the assistant's
// fetch_history tool does, and at three symbols per call it cannot keep a
// ten-thousand-ticker database current: it would take thousands of round
// trips, most of them re-downloading decades of history that has not changed.
//
// PlanSync answers the question that makes a bulk refresh cheap — for each
// ticker, what is the *first date we do not already have*, and is it worth
// asking Yahoo about at all. It is pure: the thresholds that decide whether a
// symbol is dead, local, or merely behind are testable without a database or
// a network.

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// ISODate is the date layout used throughout the sync path, matching the
// price table's own string form and yfinance's CLI arguments.
const ISODate = "2006-01-02"

// SyncTarget is one ticker that needs data, and the first date missing from
// the price table. Start is always the day after the ticker's last bar, so a
// sync re-downloads nothing it already has.
type SyncTarget struct {
	Ticker string `json:"ticker"`
	Start  string `json:"start"`
}

// SyncSkip is a ticker deliberately left out of a sync, and why. Skips are
// reported rather than silently dropped: "9,459 synced" means nothing without
// knowing what the other thousand were.
type SyncSkip struct {
	Ticker string `json:"ticker"`
	Reason string `json:"reason"`
}

// SyncPlan is the full decision for one refresh: what to fetch, what to leave
// alone, and how much was already current.
type SyncPlan struct {
	// Targets are the tickers to fetch, sorted by symbol.
	Targets []SyncTarget `json:"targets"`
	// Skipped are the tickers excluded, sorted by symbol.
	Skipped []SyncSkip `json:"skipped"`
	// UpToDate counts tickers whose last bar already reaches Through.
	UpToDate int `json:"upToDate"`
	// Through is the most recent session a sync could possibly obtain: the
	// latest weekday on or before the as-of date.
	Through string `json:"through"`
}

// Skip reasons, as they appear in SyncSkip.Reason and in the UI summary.
const (
	SkipLocalSeries = "local benchmark series, built by add_collections.py"
	SkipGeneration  = "earlier occupant of a recycled symbol; it stopped trading"
	SkipDelisted    = "delisted"
)

// sessionSettledHour is the New York hour after which a day's bar is treated
// as final. The close is 16:00, but Yahoo keeps revising the day's volume and
// adjusted close for a while after it; two hours is comfortably past that.
const sessionSettledHour = 18

// marketTZ is the exchange's clock. If the system has no tzdata, a fixed EST
// offset stands in: during daylight time it makes the sync wait one hour
// longer than necessary, which is the safe direction to be wrong in.
var marketTZ = func() *time.Location {
	if loc, err := time.LoadLocation("America/New_York"); err == nil {
		return loc
	}
	return time.FixedZone("EST", -5*3600)
}()

// latestSession returns the most recent weekday session that has finished and
// settled as of t, judged on the New York clock. A session still in progress
// is NOT the latest session: syncing during market hours used to fetch today's
// half-formed bar, store it as the day's close, and then count the ticker as
// current — so the partial bar was never replaced.
//
// It is a deliberately coarse stand-in for a market calendar: the sync path
// only needs to know whether a ticker is plausibly behind, and being wrong on
// the ten-odd exchange holidays a year costs one no-op fetch, not a wrong
// answer. Holidays are not modelled here because the alternative — shipping a
// holiday table that must be maintained forever — is worse than a wasted
// request.
func latestSession(t time.Time) time.Time {
	t = t.In(marketTZ)
	settled := t.Hour() >= sessionSettledHour
	// Calendar midnight, not Truncate(24h): Truncate snaps to a boundary
	// measured from the zero time in UTC, so west of Greenwich it lands on
	// the previous day and the whole plan aims a session too early.
	y, m, d := t.Date()
	t = time.Date(y, m, d, 0, 0, 0, 0, marketTZ)
	if !settled {
		t = t.AddDate(0, 0, -1)
	}
	for {
		switch t.Weekday() {
		case time.Saturday, time.Sunday:
			t = t.AddDate(0, 0, -1)
		default:
			return t
		}
	}
}

// PlanSync decides what a price refresh should fetch.
//
// ranges is every ticker's [first bar, last bar], as TickerDateRanges returns
// it. coverage is the delisting verdict per ticker, as ClassifyTickers returns
// it; it may be nil, which is the state of a database where add_delistings.py
// has not been run — no ticker is then skipped as dead, and the plan is
// exactly what it was before the registry existed.
//
// Three kinds of ticker are excluded:
//
//   - `$`-prefixed series, which are built locally from other rows in this
//     same table and have no Yahoo symbol to fetch.
//   - generation symbols (`COR.1`), which by construction hold a company that
//     stopped trading when the symbol was recycled away from it.
//   - tickers the registry records as delisted. A *renamed* symbol is not
//     skipped: renames.go exists precisely because the registry files a name
//     change the same way it files a death, and the company is still trading.
//
// Everything else is a target when its last bar predates the latest session,
// starting the day after that bar.
func PlanSync(
	ranges map[string]DateRange,
	coverage map[string]TickerCoverage,
	asOf time.Time,
) SyncPlan {
	through := latestSession(asOf)
	plan := SyncPlan{Through: through.Format(ISODate)}

	for ticker, r := range ranges {
		if strings.HasPrefix(ticker, "$") {
			plan.Skipped = append(plan.Skipped,
				SyncSkip{Ticker: ticker, Reason: SkipLocalSeries})
			continue
		}
		if _, gen := ParseGeneration(ticker); gen > 0 {
			plan.Skipped = append(plan.Skipped,
				SyncSkip{Ticker: ticker, Reason: SkipGeneration})
			continue
		}
		if reason, dead := deadReason(coverage[ticker]); dead {
			plan.Skipped = append(plan.Skipped,
				SyncSkip{Ticker: ticker, Reason: reason})
			continue
		}
		// Compared as calendar days, not instants: the price table's dates
		// come back as midnight UTC and `through` is midnight local, so an
		// instant comparison is an offset out — west of Greenwich that made
		// every ticker look a session behind the moment it caught up, and a
		// freshly synced database re-fetched its whole universe.
		if r.Max.Format(ISODate) >= plan.Through {
			plan.UpToDate++
			continue
		}
		plan.Targets = append(plan.Targets, SyncTarget{
			Ticker: ticker,
			Start:  r.Max.AddDate(0, 0, 1).Format(ISODate),
		})
	}

	sort.Slice(plan.Targets, func(i, j int) bool {
		return plan.Targets[i].Ticker < plan.Targets[j].Ticker
	})
	sort.Slice(plan.Skipped, func(i, j int) bool {
		return plan.Skipped[i].Ticker < plan.Skipped[j].Ticker
	})
	return plan
}

// deadReason reports whether a coverage verdict means the company behind the
// symbol has stopped trading, and says so in words the summary can print.
// A zero verdict (no registry, or no row for this ticker) is not evidence of
// death — see the coverage caveat in delistings.go.
func deadReason(tc TickerCoverage) (string, bool) {
	if tc.Delisting.DelistingDate.IsZero() {
		return "", false
	}
	if tc.Verdict == CoverageRenamed {
		return "", false
	}
	return fmt.Sprintf("%s %s", SkipDelisted,
		tc.Delisting.DelistingDate.Format(ISODate)), true
}

// SyncBatches groups targets that share a start date into fetch-sized chunks.
//
// The grouping is what makes a full refresh affordable. Yahoo's endpoint takes
// many symbols per request but only one date range, and in a database that has
// been synced before, almost every ticker shares the same last bar — so the
// ten thousand requests a per-ticker loop would make collapse into a hundred.
// Batches come back in a deterministic order: by start date, then by the first
// symbol in the batch.
func SyncBatches(targets []SyncTarget, size int) [][]SyncTarget {
	if size < 1 {
		size = 1
	}
	byStart := make(map[string][]SyncTarget)
	for _, t := range targets {
		byStart[t.Start] = append(byStart[t.Start], t)
	}

	starts := make([]string, 0, len(byStart))
	for s := range byStart {
		starts = append(starts, s)
	}
	sort.Strings(starts)

	var out [][]SyncTarget
	for _, s := range starts {
		group := byStart[s]
		sort.Slice(group, func(i, j int) bool {
			return group[i].Ticker < group[j].Ticker
		})
		for at := 0; at < len(group); at += size {
			end := at + size
			if end > len(group) {
				end = len(group)
			}
			out = append(out, group[at:end])
		}
	}
	return out
}
