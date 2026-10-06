package data

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"
)

func secRow(metric, freq, form, periodEnd, filed string, value float64) SECFundamental {
	return SECFundamental{
		CIK: "1", Ticker: "AAA", Metric: metric, Frequency: freq, Form: form,
		PeriodEnd: day(periodEnd), Filed: day(filed), Value: value,
	}
}

// pointsBy indexes points by metric, frequency and period end.
func pointsBy(points []FundamentalPoint) map[string]FundamentalPoint {
	out := map[string]FundamentalPoint{}
	for _, p := range points {
		out[p.Metric+"|"+p.Frequency+"|"+p.PeriodEnd.Format("2006-01-02")] = p
	}
	return out
}

func TestSECPointsTakesTheFirstFilingNotTheRestatement(t *testing.T) {
	points := SECPoints([]SECFundamental{
		secRow("Net Income", "annual", "10-K", "2022-12-31", "2023-02-20", 100),
		// The same year, restated as a comparative in the next 10-K.
		secRow("Net Income", "annual", "10-K", "2022-12-31", "2024-02-19", 90),
	})
	if len(points) != 1 {
		t.Fatalf("got %d points, want 1: %+v", len(points), points)
	}
	p := points[0]
	if p.Value != 100 {
		t.Errorf("Value = %v, want the original 100 — the restatement did not "+
			"exist on the day the original was filed", p.Value)
	}
	if want := day("2023-02-21"); !p.KnownFrom.Equal(want) {
		t.Errorf("KnownFrom = %v, want the day after filing", p.KnownFrom)
	}
	if p.Source != SourceSEC || p.LagSource != LagFromSECFiling || p.Instant {
		t.Errorf("Source=%q LagSource=%q Instant=%v", p.Source, p.LagSource, p.Instant)
	}
}

func TestSECPointsFrequencies(t *testing.T) {
	points := SECPoints([]SECFundamental{
		secRow("Net Income", "quarterly", "10-Q", "2023-03-31", "2023-05-05", 25),
		// Six months: neither a quarter nor a year, so it must not appear.
		secRow("Net Income", "year_to_date", "10-Q", "2023-06-30", "2023-08-04", 50),
		secRow("Net Income", "annual", "10-K", "2023-12-31", "2024-02-20", 100),
		// A quarter-end balance sheet: quarterly only.
		secRow("Stockholders Equity", "instant", "10-Q", "2023-03-31", "2023-05-05", 500),
		// The fiscal-year-end balance sheet: both, so it lines up with an
		// annual flow and still counts as the freshest quarterly reading.
		secRow("Stockholders Equity", "instant", "10-K", "2023-12-31", "2024-02-20", 520),
	})
	by := pointsBy(points)
	for _, key := range []string{
		"Net Income|quarterly|2023-03-31",
		"Net Income|annual|2023-12-31",
		"Stockholders Equity|quarterly|2023-03-31",
		"Stockholders Equity|quarterly|2023-12-31",
		"Stockholders Equity|annual|2023-12-31",
	} {
		if _, ok := by[key]; !ok {
			t.Errorf("missing %s", key)
		}
	}
	if len(points) != 5 {
		t.Errorf("got %d points, want 5 (the year-to-date row dropped, the "+
			"10-K balance sheet twice): %v", len(points), by)
	}
	if _, ok := by["Stockholders Equity|annual|2023-03-31"]; ok {
		t.Error("a 10-Q balance sheet was labelled annual")
	}
	for k, p := range by {
		if want := p.Metric == "Stockholders Equity"; p.Instant != want {
			t.Errorf("%s: Instant = %v, want %v", k, p.Instant, want)
		}
	}
}

func TestSECPointsDropsWhatCannotBeJoinedOrKnown(t *testing.T) {
	noTicker := secRow("Net Income", "annual", "10-K", "2015-12-31", "2016-02-20", 1)
	noTicker.Ticker = "" // a delisted company: no symbol to trade it under
	filedEarly := secRow("Net Income", "annual", "10-K", "2016-12-31", "2016-12-31", 1)
	if got := SECPoints([]SECFundamental{noTicker, filedEarly}); len(got) != 0 {
		t.Errorf("got %+v, want nothing", got)
	}
}

// The Go list is what config validation checks a declared metric against, and
// the loader is what decides which metrics exist. They must agree.
func TestSECMetricsMatchesTheLoader(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "..", "add_sec_financials.py"))
	if err != nil {
		t.Fatalf("read loader: %v", err)
	}
	start := regexp.MustCompile(`(?m)^TAGS: .*= \[$`).FindIndex(src)
	if start == nil {
		t.Fatal("could not find TAGS in add_sec_financials.py")
	}
	end := regexp.MustCompile(`(?m)^\]$`).FindIndex(src[start[1]:])
	if end == nil {
		t.Fatal("could not find the end of TAGS")
	}
	block := src[start[1] : start[1]+end[0]]
	// Each entry ends `, "Metric Name"),` — the tag may span lines before it.
	seen := map[string]bool{}
	for _, m := range regexp.MustCompile(`"([^"]+)"\),`).FindAllSubmatch(block, -1) {
		seen[string(m[1])] = true
	}
	var loader []string
	for m := range seen {
		loader = append(loader, m)
	}
	ours := append([]string(nil), SECMetrics...)
	sort.Strings(loader)
	sort.Strings(ours)
	if len(loader) != len(ours) {
		t.Fatalf("loader writes %d metrics, SECMetrics lists %d:\nloader %q\nours   %q",
			len(loader), len(ours), loader, ours)
	}
	for i := range ours {
		if ours[i] != loader[i] {
			t.Fatalf("mismatch at %d: loader %q, SECMetrics %q", i, loader[i], ours[i])
		}
	}
}

// End to end through DuckDB: the filters in the query (frequency, period end,
// ticker) and the scan must agree with the pure rules.
func TestSECPointInTimeFundamentalsReadsTheTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sec.db")
	if _, err := InitDB(path); err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec := func(sql string) {
		t.Helper()
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	mustExec(`CREATE TABLE sec_financials (
	            cik VARCHAR, ticker VARCHAR, company_name VARCHAR, sic VARCHAR,
	            metric VARCHAR, tag VARCHAR, period_end DATE, filed DATE,
	            filing_period DATE, value DOUBLE, form VARCHAR, fy VARCHAR,
	            fp VARCHAR, frequency VARCHAR, accession VARCHAR,
	            tag_rank SMALLINT, qtrs SMALLINT)`)
	mustExec(`INSERT INTO sec_financials VALUES
	  ('1','AAA','A','','Net Income','NetIncomeLoss', DATE '2012-12-31',
	   DATE '2013-02-20', DATE '2012-12-31', 10.0, '10-K','2012','FY','annual','a',0,4),
	  ('1','AAA','A','','Net Income','NetIncomeLoss', DATE '2013-06-30',
	   DATE '2013-08-05', DATE '2013-06-30', 12.0, '10-Q','2013','Q2','year_to_date','b',0,2),
	  ('1','AAA','A','','Total Assets','Assets', DATE '2012-12-31',
	   DATE '2013-02-20', DATE '2012-12-31', 99.0, '10-K','2012','FY','instant','a',0,0),
	  -- after endTime: dropped
	  ('1','AAA','A','','Net Income','NetIncomeLoss', DATE '2020-12-31',
	   DATE '2021-02-20', DATE '2020-12-31', 30.0, '10-K','2020','FY','annual','c',0,4),
	  -- not asked for
	  ('2','BBB','B','','Net Income','NetIncomeLoss', DATE '2012-12-31',
	   DATE '2013-02-20', DATE '2012-12-31', 5.0, '10-K','2012','FY','annual','d',0,4)`)

	points := SECPointInTimeFundamentals(
		[]string{"AAA"}, []string{"Net Income", "Total Assets"},
		day("2010-01-01"), day("2019-12-31"))
	by := pointsBy(points)
	if len(points) != 3 {
		t.Fatalf("got %d points, want 3: %v", len(points), by)
	}
	ni, ok := by["Net Income|annual|2012-12-31"]
	if !ok || ni.Value != 10 || !ni.KnownFrom.Equal(day("2013-02-21")) {
		t.Errorf("annual net income = %+v", ni)
	}
	if _, ok := by["Total Assets|annual|2012-12-31"]; !ok {
		t.Error("the 10-K balance sheet is missing its annual copy")
	}
	if _, ok := by["Total Assets|quarterly|2012-12-31"]; !ok {
		t.Error("the 10-K balance sheet is missing its quarterly copy")
	}
}

func TestSECPointInTimeFundamentalsWithoutTheTable(t *testing.T) {
	if _, err := InitDB(filepath.Join(t.TempDir(), "empty.db")); err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := SECPointInTimeFundamentals([]string{"AAA"}, []string{"Net Income"},
		day("2010-01-01"), day("2020-01-01")); got != nil {
		t.Errorf("got %v, want nil", got)
	}
}
