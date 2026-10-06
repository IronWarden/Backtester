package backtest

import (
	"bytes"
	"log"
	"math"
	"testing"
	"time"

	"my-backtester/src/data"
)

// fundPoint builds a point that became public on knownFrom.
func fundPoint(ticker, metric string, value float64, knownFrom time.Time) data.FundamentalPoint {
	return data.FundamentalPoint{
		RawFundamental: data.RawFundamental{
			Ticker: ticker, Metric: metric, Value: value,
		},
		KnownFrom: knownFrom,
	}
}

// runFundamentalScript steps a script over the standard host window with the
// given declared metrics and prefetched points, and returns what it logged.
func runFundamentalScript(
	t *testing.T, src string, metrics []string, points []data.FundamentalPoint,
) string {
	t.Helper()
	return runFundamentalScriptFrom(t, src, metrics, points, "")
}

// runFundamentalScriptFrom is runFundamentalScript for a given
// FundamentalSource.
func runFundamentalScriptFrom(
	t *testing.T, src string, metrics []string, points []data.FundamentalPoint,
	source string,
) string {
	t.Helper()
	tickers := []string{"AAA", "BBB"}
	hist := hostHist(tickers)

	p, err := InitializePortfolio(
		10000,
		hist["AAA"][0].Date, hist["AAA"][hostDays-1].Date,
		"fund-test", tickers, "lua:"+hostScript(t, src), nil,
	)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	p.Fundamentals = metrics
	p.FundamentalData = points
	p.FundamentalSource = source

	strat := p.Strategy.(*LuaStrategy)
	defer strat.Close()

	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	for day := 0; day < hostDays; day++ {
		strat.Step(p, hist, day)
	}
	return logBuf.String()
}

// The whole point of the feature: a figure must be invisible until the day it
// was published, and visible from that day on. hostHist runs 2024-01-02 for
// five trading days, so publishing on day index 2 (2024-01-04) splits the
// window.
func TestFundamentalIsInvisibleUntilPublished(t *testing.T) {
	published := time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC)
	points := []data.FundamentalPoint{
		fundPoint("AAA", "Net Income", 500, published),
	}
	out := runFundamentalScript(t, `
		function step(day)
		  local v = fundamental("AAA", "Net Income", day)
		  if v == nil then log("day " .. day .. " nil")
		  else log("day " .. day .. " = " .. v) end
		end
	`, []string{"Net Income"}, points)

	for _, want := range []string{"day 0 nil", "day 1 nil"} {
		if !contains(out, want) {
			t.Errorf("expected %q before publication; got:\n%s", want, out)
		}
	}
	// Published on day 2, so day 2 already sees it — KnownFrom is the first
	// day the figure was public, not the period it describes.
	for _, want := range []string{"day 2 = 500", "day 3 = 500", "day 4 = 500"} {
		if !contains(out, want) {
			t.Errorf("expected %q from publication onward; got:\n%s", want, out)
		}
	}
}

// The newest figure published on or before the day wins, so a restatement or a
// later quarter supersedes an earlier one.
func TestFundamentalUsesTheNewestPublishedValue(t *testing.T) {
	points := []data.FundamentalPoint{
		fundPoint("AAA", "Net Income", 100, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)),
		fundPoint("AAA", "Net Income", 900, time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC)),
	}
	out := runFundamentalScript(t, `
		function step(day)
		  log("day " .. day .. " = " .. tostring(fundamental("AAA","Net Income",day)))
		end
	`, []string{"Net Income"}, points)

	if !contains(out, "day 1 = 100") {
		t.Errorf("expected the first figure on day 1; got:\n%s", out)
	}
	if !contains(out, "day 4 = 900") {
		t.Errorf("expected the newer figure on day 4; got:\n%s", out)
	}
}

// nil, not 0 — a company can genuinely report zero, so 0 must never stand in
// for "not published". This is the rule that breaks price()'s convention and
// so is the one most likely to be "fixed" later by mistake.
func TestFundamentalReturnsNilNotZeroWhenAbsent(t *testing.T) {
	points := []data.FundamentalPoint{
		fundPoint("AAA", "Net Income", 0, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)),
	}
	out := runFundamentalScript(t, `
		function step(day)
		  if day ~= 0 then return end
		  log("real zero: " .. tostring(fundamental("AAA","Net Income",0)))
		  log("absent: " .. tostring(fundamental("AAA","Cash",0)))
		  log("unknown ticker: " .. tostring(fundamental("ZZZ","Net Income",0)))
		end
	`, []string{"Net Income"}, points)

	if !contains(out, "real zero: 0") {
		t.Errorf("a genuine zero was not returned as 0; got:\n%s", out)
	}
	if !contains(out, "absent: nil") {
		t.Errorf("an unpublished metric was not nil; got:\n%s", out)
	}
	if !contains(out, "unknown ticker: nil") {
		t.Errorf("an unknown ticker was not nil; got:\n%s", out)
	}
}

func TestMarketCapIsSharesTimesClose(t *testing.T) {
	day0 := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	points := []data.FundamentalPoint{
		fundPoint("AAA", sharesMetric, 1000, day0),
		// BBB has a figure, but not the share count.
		fundPoint("BBB", "Net Income", 5, day0),
	}
	out := runFundamentalScript(t, `
		function step(day)
		  if day ~= 0 then return end
		  log("AAA: " .. tostring(market_cap("AAA", 0)))
		  log("BBB: " .. tostring(market_cap("BBB", 0)))
		  log("out of range: " .. tostring(market_cap("AAA", 99)))
		end
	`, []string{"Net Income"}, points)

	// hostHist prices AAA at 100 on day 0, so 1000 shares -> 100000.
	if !contains(out, "AAA: 100000") {
		t.Errorf("market_cap did not equal shares x close; got:\n%s", out)
	}
	if !contains(out, "BBB: nil") {
		t.Errorf("market_cap without a share count must be nil; got:\n%s", out)
	}
	if !contains(out, "out of range: nil") {
		t.Errorf("market_cap past the series must be nil; got:\n%s", out)
	}
}

// Registered even when nothing was declared, so a script gets a clean nil
// rather than "attempt to call a nil value" — which would be a crash the user
// could only diagnose by reading Go source.
func TestFundamentalGlobalsExistWithNoDeclaredMetrics(t *testing.T) {
	out := runFundamentalScript(t, `
		function step(day)
		  if day ~= 0 then return end
		  log("f: " .. tostring(fundamental("AAA","Net Income",0)))
		  log("m: " .. tostring(market_cap("AAA",0)))
		end
	`, nil, nil)

	if !contains(out, "f: nil") || !contains(out, "m: nil") {
		t.Errorf("globals missing or not nil with no declared metrics; got:\n%s", out)
	}
	if contains(out, "attempt to call a nil value") {
		t.Errorf("the globals were not registered:\n%s", out)
	}
}

func TestFundamentalCoverageCountsCells(t *testing.T) {
	d := func(day int) time.Time {
		return time.Date(2024, 1, day, 0, 0, 0, 0, time.UTC)
	}
	points := []data.FundamentalPoint{
		fundPoint("AAA", "Net Income", 1, d(3)),
	}
	dates := []time.Time{d(1), d(2), d(3), d(4)}

	// One ticker, one metric, known from the third of four days: 50%.
	got := FundamentalCoverage(points, []string{"AAA"}, []string{"Net Income"}, dates)
	if math.Abs(got-50) > 1e-9 {
		t.Errorf("coverage = %v, want 50", got)
	}

	// Two tickers, only one of which ever reports: halves it again.
	got = FundamentalCoverage(points, []string{"AAA", "BBB"}, []string{"Net Income"}, dates)
	if math.Abs(got-25) > 1e-9 {
		t.Errorf("coverage across two tickers = %v, want 25", got)
	}

	// Nothing declared, nothing fetched, no days: 0 rather than a divide by
	// zero or a misleading 100.
	if got := FundamentalCoverage(nil, []string{"AAA"}, []string{"Net Income"}, dates); got != 0 {
		t.Errorf("coverage with no points = %v, want 0", got)
	}
	if got := FundamentalCoverage(points, []string{"AAA"}, nil, dates); got != 0 {
		t.Errorf("coverage with no declared metrics = %v, want 0", got)
	}
	if got := FundamentalCoverage(points, []string{"AAA"}, []string{"Net Income"}, nil); got != 0 {
		t.Errorf("coverage with no days = %v, want 0", got)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && bytes.Contains([]byte(haystack), []byte(needle))
}
