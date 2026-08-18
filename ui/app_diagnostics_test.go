package main

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"my-backtester/src/backtest"
)

// A run that reports nothing must produce an empty console, not a console
// containing a note about nothing having been suppressed.
func TestDiagCollectorEmpty(t *testing.T) {
	var c diagCollector
	if got := c.records(); len(got) != 0 {
		t.Errorf("got %d records from an untouched collector, want 0", len(got))
	}
}

func TestDiagCollectorKeepsOrderAndFields(t *testing.T) {
	var c diagCollector
	c.record(backtest.Diagnostic{Level: "warn", Portfolio: "a", Message: "first"})
	c.record(backtest.Diagnostic{Level: "error", Portfolio: "b", Message: "second"})

	got := c.records()
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}
	if got[0].Message != "first" || got[1].Message != "second" {
		t.Errorf("records out of order: %+v", got)
	}
	if got[1].Level != "error" || got[1].Portfolio != "b" {
		t.Errorf("fields not preserved: %+v", got[1])
	}
}

// A Lua script that raises on every one of several thousand trading days would
// otherwise send one record per day across the bridge. The cap holds, and says
// so rather than truncating silently.
func TestDiagCollectorCapsAndSaysSo(t *testing.T) {
	var c diagCollector
	const sent = maxDiagnostics + 57
	for i := 0; i < sent; i++ {
		c.record(backtest.Diagnostic{
			Level: "error", Message: fmt.Sprintf("message %d", i),
		})
	}

	got := c.records()
	if len(got) != maxDiagnostics+1 {
		t.Fatalf("got %d records, want %d plus the suppression note",
			len(got), maxDiagnostics)
	}
	last := got[len(got)-1]
	if !strings.Contains(last.Message, "57 further diagnostics") {
		t.Errorf("suppression note does not count the dropped records: %q",
			last.Message)
	}
	if last.Level != "warn" {
		t.Errorf("suppression note level = %q, want warn", last.Level)
	}
	// The kept records are the first ones: the earliest failure is the one
	// that explains the rest.
	if got[0].Message != "message 0" {
		t.Errorf("first kept record = %q, want the earliest", got[0].Message)
	}
}

// The engine calls record from the worker pool without locking on its own
// behalf, so the collector must. Worth running under -race.
func TestDiagCollectorConcurrentRecord(t *testing.T) {
	var c diagCollector
	const workers, each = 8, 10
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				c.record(backtest.Diagnostic{
					Level:   "warn",
					Message: fmt.Sprintf("worker %d message %d", w, i),
				})
			}
		}(w)
	}
	wg.Wait()

	if got := c.records(); len(got) != workers*each {
		t.Errorf("got %d records, want %d", len(got), workers*each)
	}
}
