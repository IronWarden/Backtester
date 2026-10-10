package backtest

import (
	"strconv"
	"strings"
	"testing"
)

// A strategy is handed `day` and, until num_days existed, had no way to learn
// what it counted up to — so "do this once at the end" could not be written.
func TestNumDaysReportsTheWindowLength(t *testing.T) {
	p, logs := runHostScript(t, `
function step(day)
  if day == 0 then log("total", num_days()) end
end
`, nil)
	_ = p

	if !strings.Contains(logs, "total "+strconv.Itoa(hostDays)) {
		t.Errorf("num_days did not report the window length %d: %s",
			hostDays, logs)
	}
}

// The idiom the feature exists for, and the off-by-one it invites: day is
// 0-based, so the last day is num_days() - 1. This asserts the comparison
// fires exactly once, on the final day, rather than never or twice.
func TestNumDaysIdentifiesTheLastDayExactlyOnce(t *testing.T) {
	_, logs := runHostScript(t, `
function step(day)
  if day == num_days() - 1 then log("final day", day) end
end
`, nil)

	if n := strings.Count(logs, "final day"); n != 1 {
		t.Fatalf("the last-day test fired %d times, want exactly 1: %s",
			n, logs)
	}
	if !strings.Contains(logs, "final day "+strconv.Itoa(hostDays-1)) {
		t.Errorf("fired on the wrong day: %s", logs)
	}
}

// The thing it makes possible: closing the book at the end, so realised P&L
// and the blotter's win rate describe something. A buy-and-hold that never
// sells realises nothing at all.
func TestLiquidatingOnTheLastDayRealisesPnL(t *testing.T) {
	p, _ := runHostScript(t, `
function step(day)
  if day == 0 then
    buy_max("AAA", price("AAA", day), "equalWeights", day)
  elseif day == num_days() - 1 then
    sell_all("AAA", price("AAA", day), day)
  end
end
`, nil)

	stats := SummarizeTrades(p.Trades, p.Positions)
	if stats.Sells != 1 {
		t.Fatalf("got %d sells, want the one closing trade", stats.Sells)
	}
	if stats.RoundTrips != 1 {
		t.Errorf("got %d round trips, want 1", stats.RoundTrips)
	}
	// hostHist rises over the window, so the round trip is a win.
	if stats.TotalRealized <= 0 {
		t.Errorf("TotalRealized = %v, want a gain on a rising series",
			stats.TotalRealized)
	}
}
