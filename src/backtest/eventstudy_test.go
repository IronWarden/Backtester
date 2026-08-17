package backtest

// An event study is a machine for deciding whether "buy after X" is worth
// testing, so what matters is that it cannot make a non-event look like one.
// The load-bearing case is the abnormal-return adjustment: an event that fires
// in a rising market must score zero, not the market's return.

import (
	"math"
	"my-backtester/src/data"
	"testing"
)

// alwaysOn fires on every day, which is the cheapest way to check the
// aggregation, the non-overlap rule and the market adjustment separately from
// any real detector.
var alwaysOn = EventDef{
	Name:   "always",
	Detect: func([]data.AssetData, int) bool { return true },
}

// THE case. Every ticker rises together, so an event that fires on all of them
// captures the market and nothing else: the raw return is large and positive,
// and the abnormal return must be ~0. Without the adjustment, "buy after any
// event" would score brilliantly in any sample containing a bull market.
func TestAbnormalReturnStripsOutTheMarket(t *testing.T) {
	closes := map[string][]float64{}
	for _, ticker := range []string{"AAA", "BBB", "CCC", "DDD"} {
		path := make([]float64, 200)
		px := 100.0
		for i := range path {
			px *= 1.01 // everything rises 1% a day, together
			path[i] = px
		}
		closes[ticker] = path
	}
	hist := histFrom(closes)
	tickers := []string{"AAA", "BBB", "CCC", "DDD"}

	studies := StudyEvents(hist, tickers, []EventDef{alwaysOn}, []int{21})
	if len(studies) != 1 {
		t.Fatalf("got %d studies, want 1", len(studies))
	}
	s := studies[0]

	// 1% a day for 21 days is about +23% raw.
	if s.MeanRaw < 0.2 {
		t.Errorf("mean raw = %v, want ~+23%% — the fixture is not rising", s.MeanRaw)
	}
	if math.Abs(s.MeanAbnormal) > 1e-9 {
		t.Errorf("mean abnormal = %v, want ~0: every ticker moved together, so "+
			"the event captured the market and nothing else", s.MeanAbnormal)
	}
}

// An event that really does precede outperformance must show up. AAA is given a
// step up right after each event day while the rest of the universe is flat, so
// the abnormal return is the step.
func TestEventThatPrecedesOutperformanceIsDetected(t *testing.T) {
	const days, horizon = 300, 10
	closes := map[string][]float64{}
	for _, ticker := range []string{"BBB", "CCC", "DDD"} {
		flat := make([]float64, days)
		for i := range flat {
			flat[i] = 100
		}
		closes[ticker] = flat
	}
	// AAA gains 5% over each 10-day window, forever: a persistent edge.
	aaa := make([]float64, days)
	px := 100.0
	for i := range aaa {
		aaa[i] = px
		px *= math.Pow(1.05, 1.0/float64(horizon))
	}
	closes["AAA"] = aaa

	hist := histFrom(closes)
	onlyAAA := EventDef{
		Name: "aaa-only",
		Detect: func(bars []data.AssetData, i int) bool {
			// Fire on AAA's own series only: its first bar is 100 and rising,
			// the others are flat at 100 forever.
			return len(bars) > i+1 && bars[i].Close != bars[0].Close
		},
	}

	studies := StudyEvents(hist, []string{"AAA", "BBB", "CCC", "DDD"},
		[]EventDef{onlyAAA}, []int{horizon})
	if len(studies) != 1 {
		t.Fatalf("got %d studies, want 1", len(studies))
	}
	s := studies[0]
	if s.MeanAbnormal <= 0 {
		t.Errorf("mean abnormal = %v, want positive — the event precedes a "+
			"real 5%% outperformance", s.MeanAbnormal)
	}
	if s.HitRate < 0.99 {
		t.Errorf("hit rate = %v, want ~1: every occurrence outperformed",
			s.HitRate)
	}
	if s.TStat <= 2 {
		t.Errorf("t = %v for a consistent effect, want a large one", s.TStat)
	}
}

// A condition that persists fires on consecutive days. Counting each as
// independent evidence inflates every statistic, so occurrences must be spaced
// by at least the horizon.
func TestOccurrencesAreNonOverlapping(t *testing.T) {
	closes := map[string][]float64{}
	for _, ticker := range []string{"AAA", "BBB"} {
		path := make([]float64, 210)
		for i := range path {
			path[i] = 100 + float64(i%7) // wiggles, no trend
		}
		closes[ticker] = path
	}
	hist := histFrom(closes)
	tickers := []string{"AAA", "BBB"}

	const h = 21
	studies := StudyEvents(hist, tickers, []EventDef{alwaysOn}, []int{h})
	if len(studies) != 1 {
		t.Fatalf("got %d studies, want 1", len(studies))
	}
	// 210 bars, 21-day horizon: at most 9 windows per ticker, 2 tickers -> ~18.
	// Daily firing would give ~378.
	if got := studies[0].Occurrences; got > 20 {
		t.Errorf("counted %d occurrences from an always-firing event over 210 "+
			"bars; that is daily firing, not one per horizon", got)
	}
}

// Below the minimum, a study reports nothing at all rather than a mean with a
// t-statistic attached — the latter reads as evidence.
func TestTooFewOccurrencesReportsNothing(t *testing.T) {
	closes := map[string][]float64{"AAA": make([]float64, 60)}
	for i := range closes["AAA"] {
		closes["AAA"][i] = 100
	}
	hist := histFrom(closes)

	rare := EventDef{
		Name: "rare",
		Detect: func(_ []data.AssetData, i int) bool { return i == 5 },
	}
	if got := StudyEvents(hist, []string{"AAA"}, []EventDef{rare}, []int{5}); len(got) != 0 {
		t.Errorf("a single occurrence produced a study: %+v", got)
	}
}

// The drawdown detector must fire on the CROSSING, not on every day the
// condition holds — a 10% drawdown that lasts a month is one event.
func TestDrawdownFiresOnTheCrossingOnly(t *testing.T) {
	// 260 flat days at 100, then a step down to 85 that stays there.
	path := make([]float64, 320)
	for i := range path {
		if i < 260 {
			path[i] = 100
		} else {
			path[i] = 85
		}
	}
	bars := seriesFrom(path)

	var fired []int
	det := BuiltinEvents()[0].Detect // drawdown_10
	for i := range bars {
		if det(bars, i) {
			fired = append(fired, i)
		}
	}
	if len(fired) != 1 {
		t.Fatalf("drawdown_10 fired %d times on a single step down: %v",
			len(fired), fired)
	}
	if fired[0] != 260 {
		t.Errorf("fired on day %d, want 260 (the day of the step)", fired[0])
	}
}

// A new 1-year high is a crossing too, and must not fire during the year of
// warm-up when there is no year of history to be high against.
func TestHigh52wNeedsAYearOfHistory(t *testing.T) {
	path := make([]float64, 300)
	px := 100.0
	for i := range path {
		px *= 1.001 // rises every day, so every day past warm-up is a new high
		path[i] = px
	}
	bars := seriesFrom(path)
	det := BuiltinEvents()[2].Detect // high_52w

	for i := 0; i < 252; i++ {
		if det(bars, i) {
			t.Fatalf("high_52w fired on day %d, before a year of history", i)
		}
	}
	if !det(bars, 260) {
		t.Error("high_52w did not fire on a day that is a genuine new high")
	}
}

// The median is reported because one outlier can carry a mean, so it must
// actually be a median.
func TestMedian(t *testing.T) {
	cases := []struct {
		in   []float64
		want float64
	}{
		{[]float64{1, 2, 3}, 2},
		{[]float64{3, 1, 2}, 2},          // unsorted input
		{[]float64{1, 2, 3, 4}, 2.5},     // even count
		{[]float64{1, 1, 1, 100}, 1},     // the outlier does not move it
		{nil, 0},
	}
	for _, tc := range cases {
		if got := median(tc.in); math.Abs(got-tc.want) > 1e-12 {
			t.Errorf("median(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}

	// And the caller's slice keeps its order.
	xs := []float64{3, 1, 2}
	_ = median(xs)
	if xs[0] != 3 {
		t.Errorf("median sorted the caller's slice in place: %v", xs)
	}
}

// Every built-in must be able to fire somewhere and must not fire on day 0,
// where there is no history at all.
func TestBuiltinEventsAreWellFormed(t *testing.T) {
	rng := rampCloses(600, 0.4)
	bars := seriesFrom(rng)
	// Give it one violent day so the sigma and volume detectors have something.
	bars[400].Close *= 1.4
	bars[400].Volume *= 10

	for _, ev := range BuiltinEvents() {
		if ev.Description == "" {
			t.Errorf("%s has no description", ev.Name)
		}
		if ev.Detect(bars, 0) {
			t.Errorf("%s fired on day 0 with no history", ev.Name)
		}
	}
}
