package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"my-backtester/src/data"
)

func TestRunSyncToolRefusesWithoutADatabase(t *testing.T) {
	got := runSyncTool(context.Background(), "", map[string]any{}, nil)
	if !strings.Contains(got, "no database chosen") {
		t.Errorf("runSyncTool = %q, want it to name the missing database", got)
	}
}

func TestSyncPreview(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]any
		want string
	}{
		{"default", map[string]any{}, "every stale ticker"},
		{"dry run", map[string]any{"dry_run": true}, "check what is stale"},
		{"symbols", map[string]any{"symbols": []any{"tsla", "NVDA"}},
			"TSLA, NVDA"},
	}
	for _, tc := range cases {
		if got := syncPreview(tc.in); got != tc.want {
			t.Errorf("%s: syncPreview = %q, want %q", tc.name, got, tc.want)
		}
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

// Live end-to-end: seed a throwaway DB with a stale ticker, sync it, and
// check the tail actually arrived.
func TestRunSyncToolLive(t *testing.T) {
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

	out := runSyncTool(context.Background(), dbPath, map[string]any{}, nil)
	if strings.Contains(out, "helper failed") || strings.Contains(out, "timed out") {
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
