package data

import (
	"testing"
	"time"
)

func TestLatestSessionSkipsTheWeekend(t *testing.T) {
	cases := map[string]string{
		"2026-09-07": "2026-09-07", // Monday
		"2026-09-11": "2026-09-11", // Friday
		"2026-09-12": "2026-09-11", // Saturday -> Friday
		"2026-09-13": "2026-09-11", // Sunday   -> Friday
	}
	for in, want := range cases {
		if got := latestSession(afterClose(in)).Format(ISODate); got != want {
			t.Errorf("latestSession(%s) = %s, want %s", in, got, want)
		}
	}
}

func TestPlanSyncStartsTheDayAfterTheLastBar(t *testing.T) {
	ranges := map[string]DateRange{
		"AAPL": {Min: day("2010-06-17"), Max: day("2026-07-02")},
	}
	plan := PlanSync(ranges, nil, afterClose("2026-09-07"))

	if len(plan.Targets) != 1 {
		t.Fatalf("targets = %+v, want exactly one", plan.Targets)
	}
	if got := plan.Targets[0].Start; got != "2026-07-03" {
		t.Errorf("start = %s, want 2026-07-03 (the day after the last bar)", got)
	}
	if plan.Through != "2026-09-07" {
		t.Errorf("through = %s, want 2026-09-07", plan.Through)
	}
}

func TestPlanSyncCountsACurrentTickerAsUpToDate(t *testing.T) {
	ranges := map[string]DateRange{
		"AAPL": {Min: day("2010-06-17"), Max: day("2026-09-07")},
		"MSFT": {Min: day("2010-06-17"), Max: day("2026-09-08")}, // ahead of asOf
	}
	plan := PlanSync(ranges, nil, afterClose("2026-09-07"))

	if len(plan.Targets) != 0 {
		t.Errorf("targets = %+v, want none", plan.Targets)
	}
	if plan.UpToDate != 2 {
		t.Errorf("upToDate = %d, want 2", plan.UpToDate)
	}
}

func TestPlanSyncSkipsLocalSeriesAndGenerationSymbols(t *testing.T) {
	ranges := map[string]DateRange{
		"$SP500": {Min: day("1927-12-30"), Max: day("2026-07-02")},
		"COR.1":  {Min: day("1998-01-02"), Max: day("2010-04-15")},
		"COR":    {Min: day("2010-05-03"), Max: day("2026-07-02")},
	}
	plan := PlanSync(ranges, nil, afterClose("2026-09-07"))

	if len(plan.Targets) != 1 || plan.Targets[0].Ticker != "COR" {
		t.Fatalf("targets = %+v, want only COR", plan.Targets)
	}
	reasons := map[string]string{}
	for _, s := range plan.Skipped {
		reasons[s.Ticker] = s.Reason
	}
	if reasons["$SP500"] != SkipLocalSeries {
		t.Errorf("$SP500 reason = %q, want %q", reasons["$SP500"], SkipLocalSeries)
	}
	if reasons["COR.1"] != SkipGeneration {
		t.Errorf("COR.1 reason = %q, want %q", reasons["COR.1"], SkipGeneration)
	}
}

func TestPlanSyncSkipsDelistedButNotRenamed(t *testing.T) {
	ranges := map[string]DateRange{
		"DEAD": {Min: day("1998-01-02"), Max: day("2019-04-15")},
		"DD":   {Min: day("1998-01-02"), Max: day("2026-07-02")},
		"AAPL": {Min: day("2010-06-17"), Max: day("2026-07-02")},
	}
	coverage := map[string]TickerCoverage{
		"DEAD": {
			Ticker:  "DEAD",
			Verdict: CoverageClean,
			Delisting: Delisting{
				Symbol:        "DEAD",
				DelistingDate: day("2019-04-16"),
			},
		},
		// The registry files a rename as a death; the company is still trading.
		"DD": {
			Ticker:  "DD",
			Verdict: CoverageRenamed,
			Delisting: Delisting{
				Symbol:        "DD",
				DelistingDate: day("2019-06-01"),
			},
		},
		"AAPL": {Ticker: "AAPL", Verdict: CoverageUnknown},
	}

	plan := PlanSync(ranges, coverage, afterClose("2026-09-07"))

	got := map[string]bool{}
	for _, tgt := range plan.Targets {
		got[tgt.Ticker] = true
	}
	if !got["DD"] {
		t.Error("DD was skipped, but a renamed company is still trading")
	}
	if !got["AAPL"] {
		t.Error("AAPL was skipped, but it has no delisting row")
	}
	if got["DEAD"] {
		t.Error("DEAD was targeted, but the registry says it stopped trading")
	}
	for _, s := range plan.Skipped {
		if s.Ticker == "DEAD" && s.Reason != SkipDelisted+" 2019-04-16" {
			t.Errorf("DEAD reason = %q, want the delisting date in it", s.Reason)
		}
	}
}

// A database with no delistings table must plan exactly as it did before the
// registry existed: nothing is condemned on the strength of missing evidence.
func TestPlanSyncWithoutARegistrySkipsNothingAsDead(t *testing.T) {
	ranges := map[string]DateRange{
		"DEAD": {Min: day("1998-01-02"), Max: day("2019-04-15")},
		"AAPL": {Min: day("2010-06-17"), Max: day("2026-07-02")},
	}
	plan := PlanSync(ranges, nil, afterClose("2026-09-07"))
	if len(plan.Targets) != 2 {
		t.Errorf("targets = %+v, want both tickers", plan.Targets)
	}
	if len(plan.Skipped) != 0 {
		t.Errorf("skipped = %+v, want none", plan.Skipped)
	}
}

func TestSyncBatchesGroupsBySharedStartDate(t *testing.T) {
	targets := []SyncTarget{
		{Ticker: "AAPL", Start: "2026-07-03"},
		{Ticker: "MSFT", Start: "2026-07-03"},
		{Ticker: "NVDA", Start: "2026-07-03"},
		{Ticker: "TSLA", Start: "2025-01-02"},
	}
	batches := SyncBatches(targets, 2)

	if len(batches) != 3 {
		t.Fatalf("got %d batches, want 3 (one for the odd start, two for the shared)", len(batches))
	}
	// Sorted by start date, so the 2025 straggler leads.
	if len(batches[0]) != 1 || batches[0][0].Ticker != "TSLA" {
		t.Errorf("batch 0 = %+v, want TSLA alone", batches[0])
	}
	for _, b := range batches {
		start := b[0].Start
		for _, tgt := range b {
			if tgt.Start != start {
				t.Errorf("batch mixes start dates: %+v", b)
			}
		}
		if len(b) > 2 {
			t.Errorf("batch of %d exceeds the size limit", len(b))
		}
	}
}

func TestSyncBatchesTreatsAZeroSizeAsOne(t *testing.T) {
	batches := SyncBatches([]SyncTarget{
		{Ticker: "AAPL", Start: "2026-07-03"},
		{Ticker: "MSFT", Start: "2026-07-03"},
	}, 0)
	if len(batches) != 2 {
		t.Errorf("got %d batches, want 2 — a zero size must not divide by zero "+
			"or return everything in one request", len(batches))
	}
}

// afterClose is the evening of a date on the New York clock, when that day's
// session has settled and is therefore the latest one a sync can obtain.
func afterClose(s string) time.Time {
	y, m, d := day(s).Date()
	return time.Date(y, m, d, 20, 0, 0, 0, marketTZ)
}

// A time zone west of Greenwich must not push the target session back a day.
// Truncate(24h) snaps to a UTC boundary, so a Monday evening in New York
// truncated that way lands on Sunday and plans a sync that stops at Friday —
// silently leaving Monday's bar out of every ticker.
func TestLatestSessionUsesTheMarketDate(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("no tzdata")
	}
	evening := time.Date(2026, 9, 7, 19, 52, 0, 0, ny) // a Monday
	if got := latestSession(evening).Format(ISODate); got != "2026-09-07" {
		t.Errorf("latestSession = %s, want 2026-09-07 (the same market day)", got)
	}
	// 00:30 UTC Tuesday is still Monday evening on the exchange's clock.
	utc := time.Date(2026, 9, 8, 0, 30, 0, 0, time.UTC)
	if got := latestSession(utc).Format(ISODate); got != "2026-09-07" {
		t.Errorf("latestSession(UTC) = %s, want 2026-09-07", got)
	}
}

// A session still trading is not a session a sync can have. Yahoo serves the
// half-formed bar for today, and once stored it made the ticker look current,
// so the partial close was never replaced by the real one.
func TestLatestSessionExcludesAnUnfinishedSession(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("no tzdata")
	}
	cases := []struct {
		at   time.Time
		want string
	}{
		{time.Date(2026, 9, 14, 10, 4, 0, 0, ny), "2026-09-11"},  // Monday, open -> Friday
		{time.Date(2026, 9, 14, 16, 30, 0, 0, ny), "2026-09-11"}, // closed, not settled
		{time.Date(2026, 9, 14, 18, 0, 0, 0, ny), "2026-09-14"},  // settled
		{time.Date(2026, 9, 9, 9, 0, 0, 0, ny), "2026-09-08"},    // Wednesday premarket
	}
	for _, tc := range cases {
		if got := latestSession(tc.at).Format(ISODate); got != tc.want {
			t.Errorf("latestSession(%s) = %s, want %s",
				tc.at.Format(time.RFC3339), got, tc.want)
		}
	}
}

// The price table hands back midnight UTC and the clock is local, so the two
// sides of the staleness test used to be an offset apart: west of Greenwich
// every ticker looked a session behind the moment it had caught up, and a
// synced database re-fetched its whole universe on the next run.
func TestPlanSyncComparesCalendarDaysNotInstants(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("no tzdata")
	}
	ranges := map[string]DateRange{
		// As TickerDateRanges returns it: midnight UTC on the last session.
		"AAPL": {Min: day("2010-06-17"), Max: day("2026-09-11")},
	}
	plan := PlanSync(ranges, nil, time.Date(2026, 9, 11, 19, 30, 0, 0, ny))

	if len(plan.Targets) != 0 {
		t.Errorf("targets = %+v, want none — AAPL already has %s",
			plan.Targets, plan.Through)
	}
	if plan.UpToDate != 1 {
		t.Errorf("upToDate = %d, want 1", plan.UpToDate)
	}
}
