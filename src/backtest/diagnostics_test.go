package backtest

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"my-backtester/src/data"
)

// captureDiagnostics installs a collecting sink for one test and restores the
// nil default afterwards. The sink is a process-global, so these tests must
// not run in parallel with anything that runs a portfolio.
func captureDiagnostics(t *testing.T) func() []Diagnostic {
	t.Helper()
	var (
		mu   sync.Mutex
		seen []Diagnostic
	)
	Diagnostics = func(d Diagnostic) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, d)
	}
	t.Cleanup(func() { Diagnostics = nil })
	return func() []Diagnostic {
		mu.Lock()
		defer mu.Unlock()
		return append([]Diagnostic(nil), seen...)
	}
}

// quietLog silences the package logger for a test that only cares about the
// diagnostics, and hands back what was written so a test can check both sides
// agree.
func quietLog(t *testing.T) func() string {
	t.Helper()
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prev) })
	return buf.String
}

// A script that raises every day is the case the console exists for: today it
// is invisible in the UI, and the user sees a flat equity curve with no
// explanation.
func TestDiagnosticsCollectLuaErrors(t *testing.T) {
	collected := captureDiagnostics(t)
	quietLog(t)

	runHostScript(t, `
function step(day)
  error("deliberate")
end
`, nil)

	got := collected()
	if len(got) == 0 {
		t.Fatal("a script that raises every day produced no diagnostics")
	}
	for _, d := range got {
		if d.Level != "error" {
			t.Errorf("level = %q, want error: %s", d.Level, d.Message)
		}
		if d.Portfolio != "host-test" {
			t.Errorf("portfolio = %q, want host-test", d.Portfolio)
		}
		if !strings.Contains(d.Message, "deliberate") {
			t.Errorf("message does not carry the Lua error: %q", d.Message)
		}
	}
}

// The coverage shortfall is a warning, not an error: the run produced numbers,
// they just answer a shorter question than the one that was asked.
func TestDiagnosticsCollectCoverageShortfall(t *testing.T) {
	collected := captureDiagnostics(t)
	quietLog(t)

	// BBB stops halfway, so only the days both tickers cover are simulated.
	hist := map[string][]data.AssetData{
		"AAA": seriesFrom([]float64{100, 101, 102, 103, 104, 105}),
		"BBB": seriesFrom([]float64{100, 101, 102}),
	}
	p, err := InitializePortfolio(
		10000, exactEpoch, exactEpoch.AddDate(0, 0, 6), "short",
		[]string{"AAA", "BBB"}, "buyAndHold:equalWeights", nil,
	)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	runOne(p, hist, map[int64]float64{})

	got := collected()
	if len(got) != 1 {
		t.Fatalf("got %d diagnostics, want 1: %+v", len(got), got)
	}
	if got[0].Level != "warn" {
		t.Errorf("level = %q, want warn", got[0].Level)
	}
	if got[0].Portfolio != "short" {
		t.Errorf("portfolio = %q, want short", got[0].Portfolio)
	}
	if !strings.Contains(got[0].Message, "BBB") {
		t.Errorf("message does not name the short ticker: %q", got[0].Message)
	}
}

// The point of routing diagnostics explicitly rather than capturing the log is
// that the console shows only what the user can act on. A clean run's metric
// dump and the engine's own timings must not reach it.
func TestDiagnosticsExcludeInternalLogging(t *testing.T) {
	collected := captureDiagnostics(t)
	quietLog(t)

	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{"AAA": {100, 110, 120}},
		CostConfig{})
	p.PrintMetrics()

	if got := collected(); len(got) != 0 {
		t.Errorf("a clean run reported %d diagnostics, want none: %+v",
			len(got), got)
	}
}

// With no sink installed — the CLI, and every other test in this package —
// nothing about the log changes. This is the assertion that protects every
// existing expectation about log output.
func TestDiagnosticsNilSinkStillLogs(t *testing.T) {
	if Diagnostics != nil {
		t.Fatal("a previous test left a sink installed")
	}
	logged := quietLog(t)

	diagf("warn", "somewhere", "the %s did not %s", "thing", "work")

	if !strings.Contains(logged(), "the thing did not work") {
		t.Errorf("diagf with no sink did not reach the log: %q", logged())
	}
}

// diagf is called from the worker pool, once per portfolio in flight. The sink
// owns its locking, so this test exists mostly to be run under -race — but it
// also checks nothing is dropped and no message is spliced into another.
func TestDiagnosticsConcurrentDelivery(t *testing.T) {
	collected := captureDiagnostics(t)
	quietLog(t)

	const workers, each = 8, 25
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				diagf("warn", "p", "worker %d message %d", w, i)
			}
		}(w)
	}
	wg.Wait()

	got := collected()
	if len(got) != workers*each {
		t.Fatalf("got %d diagnostics, want %d", len(got), workers*each)
	}
	for _, d := range got {
		if strings.Count(d.Message, "worker ") != 1 {
			t.Errorf("message was spliced: %q", d.Message)
		}
	}
}

// A run that simulates nothing produces a result full of zeros, and zeros sort
// perfectly well next to real results in a sweep. Both ways of getting there
// have to say so.
func TestEmptyRunsExplainThemselves(t *testing.T) {
	t.Run("no overlapping days", func(t *testing.T) {
		collected := captureDiagnostics(t)
		quietLog(t)

		// AAA's history stops before BBB's begins, so no day has both.
		hist := map[string][]data.AssetData{
			"AAA": seriesFrom([]float64{100, 101, 102}),
			"BBB": seriesStartingAt(exactEpoch.AddDate(0, 0, 30),
				[]float64{200, 201, 202}),
		}
		p, err := InitializePortfolio(
			10000, exactEpoch, exactEpoch.AddDate(0, 0, 60), "no-overlap",
			[]string{"AAA", "BBB"}, "buyAndHold:equalWeights", nil,
		)
		if err != nil {
			t.Fatalf("InitializePortfolio: %v", err)
		}
		runOne(p, hist, map[int64]float64{})

		got := collected()
		if len(got) != 1 {
			t.Fatalf("got %d diagnostics, want 1: %+v", len(got), got)
		}
		if got[0].Level != "error" {
			t.Errorf("level = %q, want error", got[0].Level)
		}
		// The actionable part is which ticker's history falls short, so the
		// message has to carry the real ranges, not just "nothing overlapped".
		for _, want := range []string{"no-overlap", "AAA", "BBB", "covers"} {
			if !strings.Contains(got[0].Message, want) {
				t.Errorf("message omits %q: %s", want, got[0].Message)
			}
		}
	})

	t.Run("no tickers", func(t *testing.T) {
		collected := captureDiagnostics(t)
		quietLog(t)

		p, err := InitializePortfolio(
			10000, exactEpoch, exactEpoch.AddDate(0, 0, 10), "empty",
			nil, "buyAndHold:equalWeights", nil,
		)
		if err != nil {
			t.Fatalf("InitializePortfolio: %v", err)
		}
		runOne(p, map[string][]data.AssetData{}, map[int64]float64{})

		got := collected()
		if len(got) != 1 {
			t.Fatalf("got %d diagnostics, want 1: %+v", len(got), got)
		}
		if got[0].Level != "error" || !strings.Contains(got[0].Message, "empty") {
			t.Errorf("unhelpful diagnostic: %+v", got[0])
		}
	})
}

// seriesStartingAt is seriesFrom with a chosen first date, for building two
// histories that deliberately do not overlap.
func seriesStartingAt(start time.Time, closes []float64) []data.AssetData {
	series := seriesFrom(closes)
	for i := range series {
		series[i].Date = start.AddDate(0, 0, i)
	}
	return series
}
