package econ

import (
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func day(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func series(id string) Series {
	for _, s := range Catalog {
		if s.ID == id {
			return s
		}
	}
	panic(id)
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// Recorded 2026-10-07. FRED left October 2025 blank for CPI and
// unemployment: the shutdown cancelled that month's collection.
func TestParseFREDCSVSkipsTheBlankMonth(t *testing.T) {
	obs, err := ParseFREDCSV(strings.NewReader(string(fixture(t, "fred-UNRATE.csv"))))
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 32 || !obs[0].Date.Equal(day("2024-01-01")) {
		t.Fatalf("%d observations from %v", len(obs), obs[0].Date)
	}
	for _, o := range obs {
		if o.Date.Equal(day("2025-10-01")) {
			t.Fatalf("a blank observation was read as %v", o.Value)
		}
	}
	last := obs[len(obs)-1]
	if !last.Date.Equal(day("2026-09-01")) || last.Value != 4.2 {
		t.Errorf("latest: %+v", last)
	}
	if _, err := ParseFREDCSV(strings.NewReader("only_one_column\n1\n")); err == nil {
		t.Error("a one-column file parsed")
	}
}

func TestEveryCatalogSeriesHasARecordedReading(t *testing.T) {
	for _, s := range Catalog {
		obs, err := ParseFREDCSV(strings.NewReader(string(fixture(t, "fred-"+s.ID+".csv"))))
		if err != nil {
			t.Fatalf("%s: %v", s.ID, err)
		}
		if _, ok := Summarize(s, obs); !ok {
			t.Errorf("%s: no reading", s.ID)
		}
		if s.ReleaseID == 0 || s.ReleaseName == "" || s.TypicalLagDays <= 0 {
			t.Errorf("%s: incomplete release info %+v", s.ID, s)
		}
	}
}

func TestSummarizeReadsEachKindItsOwnWay(t *testing.T) {
	cpi := series("CPIAUCSL")
	r, ok := Summarize(cpi, []Obs{
		{day("2025-08-01"), 100}, {day("2026-07-01"), 102}, {day("2026-08-01"), 103.02},
	})
	if !ok || !near(r.Change, 0.01) || !r.HasYearChange || !near(r.YearChange, 0.0302) {
		t.Errorf("an index changes by percent: %+v", r)
	}
	if !r.PeriodEnd.Equal(day("2026-08-31")) || PeriodLabel(cpi, r.Period) != "Aug 2026" {
		t.Errorf("period: %v %s", r.PeriodEnd, PeriodLabel(cpi, r.Period))
	}

	u, _ := Summarize(series("UNRATE"), []Obs{{day("2026-08-01"), 4.1}, {day("2026-09-01"), 4.2}})
	if !near(u.Change, 0.1) || u.HasYearChange {
		t.Errorf("a rate changes by points, and has no year change without the year-ago month: %+v", u)
	}

	p, _ := Summarize(series("PAYEMS"), []Obs{{day("2026-08-01"), 159000}, {day("2026-09-01"), 159044}})
	if !near(p.Change, 44) {
		t.Errorf("payrolls change by jobs: %+v", p)
	}

	gdp := series("A191RL1Q225SBEA")
	g, _ := Summarize(gdp, []Obs{{day("2026-04-01"), 2.2}})
	if g.HasPrior || PeriodLabel(gdp, g.Period) != "Q2 2026" || !g.PeriodEnd.Equal(day("2026-06-30")) {
		t.Errorf("a quarter: %+v %s", g, PeriodLabel(gdp, g.Period))
	}

	if _, ok := Summarize(cpi, nil); ok {
		t.Error("an empty history summarized")
	}
}

// The prior of the first reading after the blank month is the month before
// the blank, and the reading says which month that was.
func TestThePriorSkipsABlankPeriodAndSaysSo(t *testing.T) {
	r, _ := Summarize(series("UNRATE"), []Obs{{day("2025-09-01"), 4.4}, {day("2025-11-01"), 4.6}})
	if !r.PriorPeriod.Equal(day("2025-09-01")) || !near(r.Change, 0.2) {
		t.Errorf("%+v", r)
	}
}

func TestEstimatedReleaseFollowsTheUsualTiming(t *testing.T) {
	// August's CPI is published in mid-September.
	if got := EstimatedRelease(series("CPIAUCSL"), day("2026-08-01")); !got.Equal(day("2026-09-13")) {
		t.Errorf("CPI: %v", got)
	}
	// Q2 GDP's advance estimate comes at the end of July.
	if got := EstimatedRelease(series("A191RL1Q225SBEA"), day("2026-04-01")); !got.Equal(day("2026-07-29")) {
		t.Errorf("GDP: %v", got)
	}
}

// Hand-written in the shape of FRED's documented fred/release/dates
// response: the endpoint needs a key, so there is no recording.
const releaseDatesJSON = `{"realtime_start":"2025-10-07","realtime_end":"9999-12-31",
"order_by":"release_date","sort_order":"asc","count":3,"offset":0,"limit":1000,
"release_dates":[{"release_id":10,"date":"2026-10-15"},{"release_id":10,"date":"2026-09-11"},
{"release_id":10,"date":"2026-11-12"}]}`

func TestReleaseDatesGiveFirstPublicationAndTheNextOne(t *testing.T) {
	dates, err := ParseReleaseDates([]byte(releaseDatesJSON))
	if err != nil || len(dates) != 3 || !dates[0].Equal(day("2026-09-11")) {
		t.Fatalf("%v %v", err, dates)
	}
	// August's reading was first published in the first release after
	// August ended.
	if got, ok := FirstReleaseAfter(dates, day("2026-08-31")); !ok || !got.Equal(day("2026-09-11")) {
		t.Errorf("first after: %v", got)
	}
	// September's CPI is due in the first release after September ends.
	cpi := series("CPIAUCSL")
	if got, ok := NextReading(cpi, dates, day("2026-08-01"), day("2026-10-07")); !ok || !got.Equal(day("2026-10-15")) {
		t.Errorf("next: %v", got)
	}
	if _, ok := NextReading(cpi, dates, day("2026-08-01"), day("2026-12-01")); ok {
		t.Error("a next release past the calendar's end")
	}
	// An overdue reading (a release that came and went without it) is never
	// due in the past.
	if got, ok := NextReading(cpi, dates, day("2026-07-01"), day("2026-10-07")); !ok || !got.Equal(day("2026-10-15")) {
		t.Errorf("overdue: %v", got)
	}
	// A daily release: the next monthly average comes after the month ends,
	// not tomorrow.
	h15 := []time.Time{day("2026-10-07"), day("2026-10-08"), day("2026-10-30"), day("2026-11-02")}
	if got, _ := NextReading(series("FEDFUNDS"), h15, day("2026-09-01"), day("2026-10-07")); !got.Equal(day("2026-11-02")) {
		t.Errorf("fed funds: %v", got)
	}
	// A quarter: Q2's successor is due after September ends.
	gdp := []time.Time{day("2026-07-30"), day("2026-08-27"), day("2026-10-29")}
	if got, _ := NextReading(series("A191RL1Q225SBEA"), gdp, day("2026-04-01"), day("2026-10-07")); !got.Equal(day("2026-10-29")) {
		t.Errorf("GDP: %v", got)
	}
	if _, err := ParseReleaseDates([]byte(`{"error_code":400,"error_message":"Bad Request.  Variable api_key is not set."}`)); err == nil {
		t.Error("FRED's error answer parsed as an empty calendar")
	}
	alt, _ := ParseReleaseDates([]byte(`{"release_dates":[{"release_id":10,"release_date":"2026-10-15"}]}`))
	if len(alt) != 1 {
		t.Errorf("the release_date spelling: %v", alt)
	}
}

func TestParseFOMCCalendar(t *testing.T) {
	ms := ParseFOMCCalendar(fixture(t, "fomc-calendars.htm"))
	// 2021-2027: eight scheduled meetings a year, plus 2025's notation vote.
	if len(ms) != 57 {
		t.Fatalf("%d meetings", len(ms))
	}
	if !ms[0].Start.Equal(day("2021-01-26")) || !ms[len(ms)-1].End.Equal(day("2027-12-08")) {
		t.Errorf("sorted span: %v .. %v", ms[0].Start, ms[len(ms)-1].End)
	}
	find := func(start string) Meeting {
		for _, m := range ms {
			if m.Start.Equal(day(start)) {
				return m
			}
		}
		t.Fatalf("no meeting starting %s", start)
		return Meeting{}
	}

	jan := find("2026-01-27")
	if !jan.End.Equal(day("2026-01-28")) || jan.Projections ||
		jan.StatementURL != "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260128a.htm" ||
		jan.MinutesURL != "https://www.federalreserve.gov/monetarypolicy/fomcminutes20260128.htm" ||
		!jan.MinutesReleased.Equal(day("2026-02-18")) {
		t.Errorf("January 2026: %+v", jan)
	}
	if !find("2026-03-17").Projections {
		t.Error("March 2026 carries projections (the asterisk)")
	}
	// A meeting across a month end.
	if m := find("2024-04-30"); !m.End.Equal(day("2024-05-01")) {
		t.Errorf("Apr/May 30-1: %+v", m)
	}
	if m := find("2023-01-31"); !m.End.Equal(day("2023-02-01")) {
		t.Errorf("Jan/Feb 31-1: %+v", m)
	}
	if m := find("2025-08-22"); m.Note != "notation vote" || !m.End.Equal(m.Start) {
		t.Errorf("the notation vote: %+v", m)
	}
	// Next year's tentative schedule has no documents yet.
	if m := find("2027-06-08"); !m.Projections || m.StatementURL != "" || m.MinutesURL != "" {
		t.Errorf("June 2027: %+v", m)
	}
	if len(ParseFOMCCalendar([]byte("<html>redesigned</html>"))) != 0 {
		t.Error("meetings from a page without any")
	}
}

func TestClientCachesAndKeepsTheKeyOutOfErrors(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		switch {
		case r.URL.Path == "/graph/fredgraph.csv":
			w.Write(fixture(t, "fred-"+r.URL.Query().Get("id")+".csv"))
		case r.URL.Path == "/fred/release/dates":
			w.WriteHeader(http.StatusBadRequest)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	oldF, oldA := FREDHost, FREDAPIHost
	FREDHost, FREDAPIHost = srv.URL, srv.URL
	defer func() { FREDHost, FREDAPIHost = oldF, oldA }()

	c := New()
	now := day("2026-10-07")
	for i := 0; i < 2; i++ {
		obs, err := c.Observations("FEDFUNDS", now)
		if err != nil || len(obs) == 0 {
			t.Fatalf("%v %d", err, len(obs))
		}
	}
	if hits.Load() != 1 {
		t.Errorf("%d requests for one series; the second should be cached", hits.Load())
	}
	_, err := c.ReleaseDates(10, "SECRETKEY123", now)
	if err == nil || strings.Contains(err.Error(), "SECRETKEY123") {
		t.Errorf("a refused key must fail without echoing the key: %v", err)
	}
}
