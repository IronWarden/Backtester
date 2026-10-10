package backtest

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTempScript puts a script on disk so NewLuaStrategy can resolve it the
// way a config would.
func writeTempScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// plot() records one value per series per day. The rule matters: a script
// plotting inside a loop over tickers must not produce one point per
// iteration, or a chart of a five-ticker portfolio is five lines drawn on top
// of each other under one name.
func TestPlotRecordsOneValuePerSeriesPerDay(t *testing.T) {
	var r plotRecorder
	r.record("sma50", 0, 10)
	r.record("sma50", 0, 11) // same day: replaces
	r.record("sma50", 1, 12)
	r.record("rsi", 1, 55)

	series := r.series()
	if len(series) != 2 {
		t.Fatalf("recorded %d series, want 2", len(series))
	}
	// First plotted, first drawn: the chart's colours follow the script's
	// order rather than a map's.
	if series[0].Name != "sma50" || series[1].Name != "rsi" {
		t.Errorf("series came back in %q, %q", series[0].Name, series[1].Name)
	}
	pts := series[0].Points
	if len(pts) != 2 {
		t.Fatalf("sma50 has %d points, want 2 (one per day)", len(pts))
	}
	if pts[0].Day != 0 || pts[0].Value != 11 {
		t.Errorf("day 0 = %+v, want the last value plotted that day (11)", pts[0])
	}
	if pts[1].Day != 1 || pts[1].Value != 12 {
		t.Errorf("day 1 = %+v", pts[1])
	}
}

// A series that starts late starts late: an indicator with 200 days of warm-up
// has nothing to say before then, and filling the gap with zeros would draw a
// line at zero that never happened.
func TestPlotSeriesAreSparse(t *testing.T) {
	var r plotRecorder
	r.record("slow", 200, 3.5)
	pts := r.series()[0].Points
	if len(pts) != 1 || pts[0].Day != 200 {
		t.Fatalf("a late series was padded: %+v", pts)
	}
}

// NaN and ±Inf do not survive JSON, and a series carrying one takes the whole
// result down with it.
func TestPlotDropsValuesThatCannotBeDrawn(t *testing.T) {
	var r plotRecorder
	r.record("x", 0, math.NaN())
	r.record("x", 1, math.Inf(1))
	r.record("x", 2, math.Inf(-1))
	r.record("x", 3, 1.5)

	series := r.series()
	if len(series) != 1 {
		t.Fatalf("recorded %d series", len(series))
	}
	if len(series[0].Points) != 1 || series[0].Points[0].Day != 3 {
		t.Errorf("a value that cannot be drawn was kept: %+v", series[0].Points)
	}
}

// A debugging aid that can be called per ticker per day has to be bounded, and
// has to say when it bounded something.
func TestPlotBoundsSeriesAndSaysSo(t *testing.T) {
	var r plotRecorder
	for i := 0; i < maxPlotSeries+3; i++ {
		r.record(strings.Repeat("s", i+1), 0, float64(i))
	}
	if got := len(r.series()); got != maxPlotSeries {
		t.Fatalf("recorded %d series, want the cap of %d", got, maxPlotSeries)
	}

	var found []Diagnostic
	prev := Diagnostics
	Diagnostics = func(d Diagnostic) { found = append(found, d) }
	defer func() { Diagnostics = prev }()
	r.reportLimits("P")

	if len(found) != 1 || found[0].Level != "warn" {
		t.Fatalf("the dropped series were not reported: %+v", found)
	}
	if !strings.Contains(found[0].Message, "ignored the rest") {
		t.Errorf("warning does not say what happened: %s", found[0].Message)
	}

	// Nothing to say when nothing was bounded: a run that plotted two series
	// must not be warned about plotting.
	found = nil
	var clean plotRecorder
	clean.record("a", 0, 1)
	clean.reportLimits("P")
	if len(found) != 0 {
		t.Errorf("an ordinary run was warned about plot(): %+v", found)
	}
}

// End to end through Lua: the global exists, records against the day being
// stepped, and reaches the portfolio.
func TestPlotFromLua(t *testing.T) {
	const script = `
function step(day)
  plot("close", price(tickers[1], day))
  if day == 1 then
    plot("named", 42)
    plot("named", 43)   -- same day, last wins
  end
  plot("", 1)           -- refused, with a warning
  plot("bad", "text")   -- refused, with a warning
end
`
	dir := t.TempDir()
	path := writeTempScript(t, dir, "plotter.lua", script)

	var found []Diagnostic
	prev := Diagnostics
	Diagnostics = func(d Diagnostic) { found = append(found, d) }
	defer func() { Diagnostics = prev }()

	p, logs := runStrategyScript(t, path, nil)
	if strings.Contains(logs, "error") {
		t.Fatalf("the plotting script raised:\n%s", logs)
	}

	series := p.plots.series()
	if len(series) != 2 {
		t.Fatalf("recorded %d series, want close and named: %+v", len(series), series)
	}
	if series[0].Name != "close" || len(series[0].Points) != libDays {
		t.Errorf("close has %d points, want one per day (%d)",
			len(series[0].Points), libDays)
	}
	named := series[1]
	if named.Name != "named" || len(named.Points) != 1 {
		t.Fatalf("named = %+v", named)
	}
	if named.Points[0].Day != 1 || named.Points[0].Value != 43 {
		t.Errorf("named point = %+v, want day 1 value 43", named.Points[0])
	}

	// Both refusals are reported: a plot that silently does nothing is a user
	// staring at a panel wondering where their line went.
	var warnings int
	for _, d := range found {
		if d.Level == "warn" && strings.Contains(d.Message, "plot(") {
			warnings++
		}
	}
	if warnings == 0 {
		t.Errorf("plot() refused two calls and said nothing: %+v", found)
	}
}
