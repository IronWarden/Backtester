package data

// What is wrong with each price series, and how badly.
//
// "The best possible data set" is unmeasurable without a defect inventory.
// Survivorship was audited (analysis/survivorship.md) and symbol recycling was
// audited (delistings.go); the bars themselves never were, and they turn out to
// need it. From the 2026-08-17 pass over 34,988,956 bars across 10,434 tickers:
//
//   - Four tickers carry NEGATIVE closes — VHI (5,429 bars, down to -639.30),
//     SVA (5,478), SAFE (1,024) and DEC (691); 11,931 bars in all. A negative
//     price makes every return computed from it meaningless, silently.
//   - 8,737 bars have High < Low, and 9,439 have a Close outside [Low, High].
//   - HUBB's 1972-1984 history is a flat synthetic series: Open = High = Low =
//     Close, zero volume, and 1,862 CONSECUTIVE identical closes. A dead feed
//     is indistinguishable from a flat stock to a strategy, and a moving
//     average over it produces confident nonsense.
//   - EMP has 526 daily moves over 50%, the largest +10,297%.
//
// None of that is visible from a backtest's output. This file is the vocabulary
// for saying it out loud: `add_data_quality.py` computes the statistics with
// SQL and writes the `data_quality` table, and ClassifyQuality — pure, so the
// thresholds are testable without a database — turns them into a verdict.
//
// The thresholds below are judgement calls, not derived constants. They are
// named and gathered here so they can be argued with, and the loader mirrors
// them so SQL and Go agree.
//
// Verdicts on the first pass: 7,193 good (68.9%), 1,915 usable (18.4%),
// 1,053 suspect (10.1%), 273 unusable (2.6%).
//
// Everything is inert until the table exists: with no `data_quality` table the
// reader returns nil and no result changes.

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
)

// Thresholds separating the quality classes. Each is the boundary of a claim
// about what a series can still be used for, and the comment is the claim.
const (
	// MinBars is the shortest series worth scoring. Below this there is not
	// enough history to compute an annualized figure from, whatever else is
	// true of the bars.
	MinBars = 20

	// ImpossibleShareUnusable is the share of bars that may be arithmetically
	// impossible (High < Low, Close outside [Low, High], a non-positive price)
	// before the series is written off entirely. Any impossible bar at all is
	// enough to make a series suspect; 1% of them means the feed, not a bad
	// day.
	ImpossibleShareUnusable = 0.01

	// FlatRunSuspect is a run of identical closes long enough that the series
	// is more likely a stalled feed than a quiet stock. A quarter of a year of
	// literally unchanged closes does not happen to a traded equity.
	FlatRunSuspect = 60

	// FlatRunUsable is where a flat run stops being ordinary. Weeks of
	// unchanged closes happen in illiquid names and are not by themselves
	// evidence of a broken feed.
	FlatRunUsable = 20

	// MissingShareSuspect and MissingShareUsable are the share of the market
	// calendar a series may be missing inside its own first-to-last span.
	// Above the first, the series has holes big enough to move a metric; above
	// the second, worth knowing but not disqualifying.
	MissingShareSuspect = 0.05
	MissingShareUsable  = 0.01

	// BigJumpReturn is what counts as an unexplained jump: a one-day move of
	// more than 50%. Real ones exist (a takeover, a biotech readout), which is
	// why one is not disqualifying and five are.
	BigJumpReturn    = 0.50
	BigJumpsSuspect  = 5
	BigJumpsUsable   = 1
	MaxJumpUnusable  = 5.0 // a 500% single-day move is a data error, not a stock
	ZeroVolumeUsable = 0.50
)

// Quality is the verdict on one ticker's bars.
type Quality string

const (
	// QualityUnknown means the ticker has no row in the audit — the table is
	// absent, or the audit has not been re-run since the ticker was added.
	QualityUnknown Quality = "unknown"

	// QualityGood means nothing in the audit fired.
	QualityGood Quality = "good"

	// QualityUsable means the defects found are the ordinary roughness of free
	// daily data: some missing days, some flat stretches, an occasional large
	// move. Trade it and know what it is.
	QualityUsable Quality = "usable"

	// QualitySuspect means at least one finding is large enough to move a
	// metric on its own. Results from it need reading with that in mind.
	QualitySuspect Quality = "suspect"

	// QualityUnusable means the series cannot support a backtest: negative or
	// zero prices, systematically impossible bars, or too little history.
	QualityUnusable Quality = "unusable"
)

// Tradable reports whether a series can support a simulation at all. Suspect
// series are tradable — with a caveat the caller should pass on — and unusable
// ones are not.
func (q Quality) Tradable() bool { return q != QualityUnusable }

// QualityStats is the measured evidence behind one ticker's verdict. Every
// field is a count or a share, so the verdict can be recomputed from the row
// and argued with rather than taken on trust.
type QualityStats struct {
	Ticker string
	Bars   int
	// DuplicateBars is bars minus distinct dates. The price table had none as
	// of 2026-08-17, unlike `financials`, but the check is cheap and the
	// absence is worth continuing to verify.
	DuplicateBars int
	// NonPositiveCloses counts bars priced at or below zero. Yahoo's
	// back-adjustment for a large special dividend can drive an adjusted close
	// negative, which is where VHI's -639.30 comes from.
	NonPositiveCloses int
	// ImpossibleBars counts bars whose own OHLC values contradict each other:
	// High < Low, or Close outside [Low, High].
	ImpossibleBars int
	// ZeroVolumeBars counts bars that report no trading. Common in old history
	// and in synthetic series.
	ZeroVolumeBars int
	// LongestFlatRun is the longest run of consecutive identical closes.
	LongestFlatRun int
	// BigJumps counts one-day moves larger than BigJumpReturn, and MaxJump is
	// the largest such move as a fraction (1.5 = +150%).
	BigJumps int
	MaxJump  float64
	// MissingDays is how many days of the market calendar the series lacks
	// inside its own first-to-last span, and ExpectedDays is that span's length
	// on the calendar. The calendar is the set of dates the $SP500 benchmark
	// series has, which is the NYSE calendar the rest of the data is built on.
	MissingDays  int
	ExpectedDays int
}

// share divides safely: a zero denominator yields zero rather than a NaN that
// would propagate into a comparison and quietly answer false.
func share(n, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(n) / float64(total)
}

// ClassifyQuality turns measurements into a verdict, worst finding first: a
// series is only as good as its worst defect. Pure, so the thresholds above can
// be tested without a database.
func ClassifyQuality(s QualityStats) Quality {
	// Unusable: the arithmetic itself is broken, or there is nothing to score.
	if s.Bars < MinBars {
		return QualityUnusable
	}
	if s.NonPositiveCloses > 0 {
		return QualityUnusable
	}
	if share(s.ImpossibleBars, s.Bars) > ImpossibleShareUnusable {
		return QualityUnusable
	}
	if s.MaxJump > MaxJumpUnusable {
		return QualityUnusable
	}

	// Suspect: one finding big enough to move a metric by itself.
	if s.ImpossibleBars > 0 {
		return QualitySuspect
	}
	if s.LongestFlatRun >= FlatRunSuspect {
		return QualitySuspect
	}
	if share(s.MissingDays, s.ExpectedDays) > MissingShareSuspect {
		return QualitySuspect
	}
	if s.BigJumps >= BigJumpsSuspect {
		return QualitySuspect
	}
	if s.DuplicateBars > 0 {
		return QualitySuspect
	}

	// Usable: the ordinary roughness of free daily data.
	if s.LongestFlatRun >= FlatRunUsable ||
		share(s.MissingDays, s.ExpectedDays) > MissingShareUsable ||
		s.BigJumps >= BigJumpsUsable ||
		share(s.ZeroVolumeBars, s.Bars) > ZeroVolumeUsable {
		return QualityUsable
	}

	return QualityGood
}

// TickerQuality pairs a verdict with the evidence, so a warning can say why.
type TickerQuality struct {
	Verdict Quality
	Stats   QualityStats
}

// LoadDataQuality reads the audit for the given tickers, or for every ticker
// when tickers is empty.
//
// Returns nil without error when the `data_quality` table does not exist, which
// is the state of any database where `add_data_quality.py` has not been run.
// Matches LoadDelistings and LoadCompanyProfiles: a missing optional dataset
// must never fail a backtest.
//
// The verdict is recomputed from the stored statistics rather than read from the
// table's own column, so a threshold change takes effect without re-running the
// audit — and so the two can be compared if they ever disagree.
func LoadDataQuality(tickers []string) (map[string]TickerQuality, error) {
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	if !tableExists("data_quality") {
		return nil, nil
	}

	query := `SELECT ticker, bars, duplicate_bars, non_positive_closes,
	                 impossible_bars, zero_volume_bars, longest_flat_run,
	                 big_jumps, max_jump, missing_days, expected_days
	          FROM data_quality`
	args := make([]any, 0, len(tickers))
	if len(tickers) > 0 {
		query += ` WHERE ticker IN (` + placeholderList(len(tickers)) + `)`
		for _, t := range tickers {
			args = append(args, t)
		}
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]TickerQuality)
	for rows.Next() {
		var s QualityStats
		var maxJump sql.NullFloat64
		if err := rows.Scan(
			&s.Ticker, &s.Bars, &s.DuplicateBars, &s.NonPositiveCloses,
			&s.ImpossibleBars, &s.ZeroVolumeBars, &s.LongestFlatRun,
			&s.BigJumps, &maxJump, &s.MissingDays, &s.ExpectedDays,
		); err != nil {
			log.Printf("scanning data quality: %v", err)
			continue
		}
		s.MaxJump = maxJump.Float64
		out[s.Ticker] = TickerQuality{Verdict: ClassifyQuality(s), Stats: s}
	}
	return out, rows.Err()
}

// UntradableTickers names the tickers whose bars cannot support a backtest,
// sorted, so a caller can refuse them by name rather than by count.
func UntradableTickers(q map[string]TickerQuality) []string {
	var out []string
	for ticker, tq := range q {
		if !tq.Verdict.Tradable() {
			out = append(out, ticker)
		}
	}
	sort.Strings(out)
	return out
}
