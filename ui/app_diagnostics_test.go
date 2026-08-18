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

// The UI check must answer without a database: that is the whole point, since
// a config worth checking is usually one whose run takes minutes.
func TestCheckConfigNeedsNoDatabase(t *testing.T) {
	app := NewApp()
	out, err := app.CheckConfig(`
[[portfolio]]
Name = "backwards"
BuyingPower = 10000.0
StartDate = "2020-01-01"
EndDate = "2019-01-01"
Tickers = ["AAA"]
Strategy = "buyAndHold:equalWeights"
`, "")
	if err != nil {
		t.Fatalf("CheckConfig: %v", err)
	}
	if len(out.Results) != 0 {
		t.Errorf("a check produced %d results; it must not run anything",
			len(out.Results))
	}
	if len(out.Diagnostics) != 1 {
		t.Fatalf("got %d diagnostics, want 1: %+v",
			len(out.Diagnostics), out.Diagnostics)
	}
	if out.Diagnostics[0].Level != "error" {
		t.Errorf("level = %q, want error", out.Diagnostics[0].Level)
	}
	if !strings.Contains(out.Diagnostics[0].Message, "before StartDate") {
		t.Errorf("unexpected message: %s", out.Diagnostics[0].Message)
	}
}

// Unparseable TOML is an error from the call, not a diagnostic: there is no
// config to attribute anything to.
func TestCheckConfigRejectsBadToml(t *testing.T) {
	app := NewApp()
	if _, err := app.CheckConfig("this is not toml =", ""); err == nil {
		t.Error("malformed TOML was accepted")
	}
}

// A clean config must come back silent, or the console cries wolf on every
// press of the button.
func TestCheckConfigSilentOnACleanConfig(t *testing.T) {
	app := NewApp()
	out, err := app.CheckConfig(`
[[portfolio]]
Name = "clean"
BuyingPower = 10000.0
StartDate = "2015-01-01"
EndDate = "2020-01-01"
Tickers = ["AAA", "BBB"]
Strategy = "buyAndHold:equalWeights"
`, "")
	if err != nil {
		t.Fatalf("CheckConfig: %v", err)
	}
	if len(out.Diagnostics) != 0 {
		t.Errorf("a clean config reported %+v", out.Diagnostics)
	}
}
