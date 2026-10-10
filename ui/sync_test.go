package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"my-backtester/src/data"
)

func TestPlanSyncRefusesWithoutADatabase(t *testing.T) {
	_, err := planSync("", nil)
	if err == nil || !strings.Contains(err.Error(), "no database chosen") {
		t.Errorf("planSync(\"\") err = %v, want it to name the missing database", err)
	}
}

func TestSkipBucketCollapsesDelistingDates(t *testing.T) {
	if got := skipBucket(data.SkipDelisted + " 2019-04-16"); got != data.SkipDelisted {
		t.Errorf("skipBucket = %q, want the date collapsed away", got)
	}
	if got := skipBucket(data.SkipLocalSeries); got != data.SkipLocalSeries {
		t.Errorf("skipBucket = %q, want it unchanged", got)
	}
}

func TestFormatSyncReportSaysWhatHappened(t *testing.T) {
	rep := SyncReport{
		Through:    "2026-09-07",
		Considered: 10482,
		UpToDate:   2,
		Stale:      10066,
		Updated:    10000,
		NoNewBars:  66,
		Rows:       450000,
		Skipped:    map[string]int{data.SkipDelisted: 400, data.SkipLocalSeries: 14},
		Failed:     map[string]string{},
	}
	got := formatSyncReport(rep)
	for _, want := range []string{
		"2026-09-07", "10482", "10066 stale", "414 skipped",
		"skipped 400: " + data.SkipDelisted,
		"Updated 10000 tickers with 450000 new daily bars",
		"66 had not traded",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report missing %q:\n%s", want, got)
		}
	}
}

// An interrupted sync must say so, and must say that what it stored is kept —
// otherwise a user who stops it has no way to know whether to trust the data.
func TestFormatSyncReportFlagsAnIncompleteRun(t *testing.T) {
	got := formatSyncReport(SyncReport{
		Through:    "2026-09-07",
		Considered: 100,
		Stale:      100,
		Updated:    40,
		Rows:       800,
		Incomplete: true,
		Reason:     "stopped after 4 of 10 batches (cancelled)",
		Remaining:  60,
		Skipped:    map[string]int{},
		Failed:     map[string]string{},
	})
	for _, want := range []string{
		"INCOMPLETE", "60 tickers were not attempted", "resumes from here",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report missing %q:\n%s", want, got)
		}
	}
}

func TestFormatSyncReportDryRunDownloadsNothing(t *testing.T) {
	got := formatSyncReport(SyncReport{
		Through: "2026-09-07", Considered: 10, UpToDate: 3, Stale: 7,
		Skipped: map[string]int{}, Failed: map[string]string{}, DryRun: true,
	})
	if !strings.Contains(got, "nothing was downloaded") {
		t.Errorf("dry run must say nothing was downloaded:\n%s", got)
	}
	if strings.Contains(got, "Updated") {
		t.Errorf("dry run must not claim to have updated anything:\n%s", got)
	}
}

func TestFailedPreviewTruncates(t *testing.T) {
	failed := map[string]string{}
	for _, s := range []string{"A", "B", "C", "D", "E", "F", "G"} {
		failed[s] = "boom"
	}
	got := failedPreview(failed)
	if !strings.Contains(got, "and 2 more") {
		t.Errorf("failedPreview = %q, want it truncated", got)
	}
	if !strings.Contains(got, "boom") {
		t.Errorf("failedPreview = %q, want a reason in it", got)
	}
}

func TestRunSyncReportsAPlanWithNothingToDo(t *testing.T) {
	rep := runSync(context.Background(), data.SyncPlan{
		Through:  "2026-09-07",
		UpToDate: 5,
		Skipped: []data.SyncSkip{
			{Ticker: "$SP500", Reason: data.SkipLocalSeries},
		},
	}, nil)

	if rep.Updated != 0 || rep.Rows != 0 {
		t.Errorf("nothing to fetch, but report claims work: %+v", rep)
	}
	if rep.Considered != 6 {
		t.Errorf("considered = %d, want 6 (5 current + 1 skipped)", rep.Considered)
	}
	if rep.Skipped[data.SkipLocalSeries] != 1 {
		t.Errorf("skipped = %v, want the local series counted", rep.Skipped)
	}
}

// A cancelled context must stop the run and report it as incomplete rather
// than as a success with suspiciously few rows.
func TestRunSyncStopsOnACancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rep := runSync(ctx, data.SyncPlan{
		Through: "2026-09-07",
		Targets: []data.SyncTarget{
			{Ticker: "AAPL", Start: "2026-07-03"},
			{Ticker: "MSFT", Start: "2026-07-03"},
		},
	}, nil)

	if !rep.Incomplete {
		t.Error("cancelled sync did not report itself incomplete")
	}
	if rep.Remaining != 2 {
		t.Errorf("remaining = %d, want 2", rep.Remaining)
	}
	if rep.Updated != 0 {
		t.Errorf("updated = %d, want 0 — nothing was fetched", rep.Updated)
	}
}

// Live end-to-end: seed a throwaway DB with a stale ticker, sync it the way
// `ui --sync` does, and check the tail actually arrived.
func TestRunSyncLive(t *testing.T) {
	if testing.Short() {
		t.Skip("live network test")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not on PATH")
	}

	dbPath := filepath.Join(t.TempDir(), "sync.db")
	if _, err := data.InitDB(dbPath); err != nil {
		t.Fatalf("init db: %v", err)
	}
	// A deliberately stale SPY: two bars from early 2024, nothing since.
	if err := data.UpsertBars("SPY", []data.Bar{
		{Date: "2024-01-02", Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 100},
		{Date: "2024-01-03", Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 100},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	plan, err := planSync(dbPath, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	out := formatSyncReport(runSync(context.Background(), plan, nil))
	// A throttled Yahoo is the network being unavailable, not a regression —
	// and since sync_prices.py started reporting a rate limit as the error it
	// is, rather than as an empty result, it arrives here in words.
	if strings.Contains(out, "helper failed") ||
		strings.Contains(out, "timed out") ||
		strings.Contains(out, "RateLimit") {
		t.Skipf("network/helper unavailable: %s", out)
	}
	if !strings.Contains(out, "Updated 1 ticker ") {
		t.Fatalf("unexpected output:\n%s", out)
	}

	_, rows, _, err := data.RunQuery(
		`SELECT MAX(Date) FROM stock_data_optimized WHERE Ticker = 'SPY'`, 10)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if strings.HasPrefix(rows[0][0], "2024-01") {
		t.Fatalf("SPY is still stale after a sync; output:\n%s", out)
	}
	t.Logf("live sync:\n%s\nlast bar now %s", out, rows[0][0])
}

func TestRunSyncCommandIgnoresAnAbsentFlag(t *testing.T) {
	// No --sync means the GUI must still launch; a false here would open a
	// window-less binary that silently does nothing.
	if handled, _ := runSyncCommand([]string{}); handled {
		t.Error("runSyncCommand claimed a bare argv")
	}
	if handled, _ := runSyncCommand([]string{"--other"}); handled {
		t.Error("runSyncCommand claimed an unrelated flag")
	}
}

func TestRunSyncCommandRejectsAMissingDatabase(t *testing.T) {
	handled, code := runSyncCommand([]string{
		"--sync", filepath.Join(t.TempDir(), "nope.db"), "--dry-run",
	})
	if !handled {
		t.Fatal("runSyncCommand did not handle --sync")
	}
	if code == 0 {
		t.Error("an empty database exited 0; a sync that found nothing to " +
			"sync must not look like a successful one")
	}
}

func TestPlanReportCountsWithoutFetching(t *testing.T) {
	rep := planReport(data.SyncPlan{
		Through:  "2026-09-07",
		UpToDate: 2,
		Targets:  []data.SyncTarget{{Ticker: "AAPL", Start: "2026-07-03"}},
		Skipped: []data.SyncSkip{
			{Ticker: "$SP500", Reason: data.SkipLocalSeries},
			{Ticker: "DEAD", Reason: data.SkipDelisted + " 2019-04-16"},
			{Ticker: "GONE", Reason: data.SkipDelisted + " 2020-01-02"},
		},
	})
	if !rep.DryRun {
		t.Error("planReport must be a dry run — it fetches nothing")
	}
	if rep.Considered != 6 {
		t.Errorf("considered = %d, want 6", rep.Considered)
	}
	if rep.Skipped[data.SkipDelisted] != 2 {
		t.Errorf("delisted tally = %d, want 2 collapsed from two dates",
			rep.Skipped[data.SkipDelisted])
	}
}

// A throttled batch used to be indistinguishable from a hundred tickers that
// had not traded, which is how a rate-limited run reported itself as a clean
// sync that found nothing to do.
func TestRateLimitedRecognizesAThrottledBatch(t *testing.T) {
	limit := "download failed: YFRateLimitError('Too Many Requests...')"
	cases := []struct {
		name    string
		results []syncResult
		err     error
		want    bool
	}{
		{"every symbol throttled", []syncResult{
			{Symbol: "AAPL", Error: limit}, {Symbol: "MSFT", Error: limit},
		}, nil, true},
		{"the request itself throttled", nil,
			errors.New("helper failed: YFRateLimitError"), true},
		{"one symbol answered", []syncResult{
			{Symbol: "AAPL", Error: limit},
			{Symbol: "MSFT", Rows: [][]any{{"2026-09-11", 1.0, 1.0, 1.0, 1.0, 1.0}}},
		}, nil, false},
		{"genuinely nothing new", []syncResult{
			{Symbol: "AAPL"}, {Symbol: "MSFT"},
		}, nil, false},
		{"an unrelated failure", []syncResult{
			{Symbol: "AAPL", Error: "no data (delisted?)"},
		}, nil, false},
		{"nothing came back", nil, nil, false},
	}
	for _, tc := range cases {
		if got := rateLimited(tc.results, tc.err); got != tc.want {
			t.Errorf("%s: rateLimited = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// fakeHelper writes a shell script standing in for sync_prices.py. Called as
// `sh script --start S --end E SYM...`, so $# is 4 plus the symbol count.
func fakeHelper(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "helper.sh")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func targets(syms ...string) []data.SyncTarget {
	out := make([]data.SyncTarget, len(syms))
	for i, s := range syms {
		out[i] = data.SyncTarget{Ticker: s, Start: "2026-09-10"}
	}
	return out
}

// A batch that times out is split and retried rather than failed wholesale,
// and a killed helper whose child still holds the output pipe must not hang
// the run past the timeout.
func TestFetchBatchResilientSplitsATimedOutBatch(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not on PATH")
	}
	old := syncBatchTimeout
	syncBatchTimeout = 500 * time.Millisecond
	defer func() { syncBatchTimeout = old }()

	// Two or more symbols hang (in a child that keeps stdout open); one
	// symbol answers straight away.
	script := fakeHelper(t, `if [ $# -gt 5 ]; then sleep 30; fi
printf '{"results":[{"symbol":"%s","rows":[["2026-09-11",1,1,1,1,1]]}]}' "$5"
`)

	start := time.Now()
	results, err, throttled := fetchBatchResilient(
		context.Background(), "sh", script, targets("AAA", "BBB"), "2026-09-12")
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("took %s — the hung child held the run past its timeout", elapsed)
	}
	if err != nil || throttled {
		t.Fatalf("err = %v, throttled = %v; want the split halves to succeed", err, throttled)
	}
	if len(results) != 2 || results[0].Symbol != "AAA" || results[1].Symbol != "BBB" {
		t.Fatalf("results = %+v, want AAA and BBB from the two halves", results)
	}
	for _, r := range results {
		if r.Error != "" || len(r.Rows) != 1 {
			t.Errorf("%s: %+v, want one row and no error", r.Symbol, r)
		}
	}
}

// A stop or an exhausted budget during a batch is the run ending, not the
// batch timing out, and must not be reported as one.
func TestFetchBatchReportsTheCallersDeadlineNotATimeout(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not on PATH")
	}
	script := fakeHelper(t, "sleep 30\n")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err := fetchBatch(ctx, "sh", script, targets("AAA"), "2026-09-12")
	if errors.Is(err, errBatchTimeout) {
		t.Errorf("err = %v, want the caller's deadline, not a batch timeout", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
}

// A rate-limit backoff longer than the time left is not worth sleeping
// through: the run gives up at once, while there is still time to say why.
func TestFetchBatchPatientlyDoesNotSleepPastTheDeadline(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not on PATH")
	}
	script := fakeHelper(t,
		`printf '{"results":[{"symbol":"%s","error":"YFRateLimitError"}]}' "$5"`+"\n")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	_, _, throttled := fetchBatchPatiently(ctx, "sh", script, targets("AAA"), "2026-09-12")
	if !throttled {
		t.Error("throttled = false, want the limit reported")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("took %s — slept into a backoff the deadline could not outlast", elapsed)
	}
}
