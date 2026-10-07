package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"my-backtester/src/econ"
)

// recordedEcon serves the FRED CSVs and the Fed's calendar recorded in
// src/econ/testdata on 2026-10-07. down names the series FRED "fails" for;
// fedDown fails the calendar too. The release calendar answers with the
// dates in releaseDates, keyed by release id, in FRED's documented shape.
func recordedEcon(t *testing.T, down map[string]bool, fedDown bool, releaseDates map[string]string) *econ.Client {
	t.Helper()
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join("..", "src", "econ", "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/graph/fredgraph.csv":
			id := r.URL.Query().Get("id")
			if down[id] {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Write(read("fred-" + id + ".csv"))
		case "/monetarypolicy/fomccalendars.htm":
			if fedDown {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.Write(read("fomc-calendars.htm"))
		case "/fred/release/dates":
			if r.URL.Query().Get("api_key") != "test-key" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var rows []string
			for _, d := range strings.Fields(releaseDates[r.URL.Query().Get("release_id")]) {
				rows = append(rows, `{"release_id":0,"date":"`+d+`"}`)
			}
			w.Write([]byte(`{"release_dates":[` + strings.Join(rows, ",") + `]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	oldF, oldA, oldFed := econ.FREDHost, econ.FREDAPIHost, econ.FedHost
	econ.FREDHost, econ.FREDAPIHost, econ.FedHost = srv.URL, srv.URL, srv.URL
	t.Cleanup(func() { econ.FREDHost, econ.FREDAPIHost, econ.FedHost = oldF, oldA, oldFed })
	return econ.New()
}

var ecoNow = time.Date(2026, 10, 7, 15, 0, 0, 0, time.UTC)

func noDB(string) ([]econ.Obs, bool) { return nil, false }

func reading(t *testing.T, v EconomicCalendar, id string) EconReading {
	t.Helper()
	for _, r := range v.Readings {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("no %s reading in %+v", id, v.Readings)
	return EconReading{}
}

func TestEcoViewWithoutAKey(t *testing.T) {
	c := recordedEcon(t, nil, false, nil)
	v := ecoView(c, "", noDB, ecoNow)
	if v.Error != "" || len(v.Readings) != len(econ.Catalog) || v.HasReleaseKey {
		t.Fatalf("%+v", v)
	}
	u := reading(t, v, "UNRATE")
	if u.Period != "Sep 2026" || u.Value != 4.2 || u.PriorPeriod != "Aug 2026" || u.Source != "FRED" ||
		u.PublishedExact || u.Published != "2026-10-05" || u.NextRelease != "" {
		t.Errorf("unemployment, published date estimated, no next release: %+v", u)
	}
	cpi := reading(t, v, "CPIAUCSL")
	if cpi.Period != "Aug 2026" || !cpi.HasYearChange || cpi.Kind != "index" {
		t.Errorf("CPI: %+v", cpi)
	}
	if g := reading(t, v, "A191RL1Q225SBEA"); g.Period != "Q2 2026" || g.Kind != "rate" {
		t.Errorf("GDP: %+v", g)
	}
	if len(v.Notes) != 1 || !strings.Contains(v.Notes[0], "FRED_API_KEY") {
		t.Errorf("the missing key must be said, once: %v", v.Notes)
	}

	// The three meetings before today and the four after.
	var dates []string
	for _, m := range v.Meetings {
		dates = append(dates, m.Dates)
	}
	want := "Jun 16–17, 2026|Jul 28–29, 2026|Sep 15–16, 2026|Oct 27–28, 2026|Dec 8–9, 2026|" +
		"Jan 26–27, 2027|Mar 16–17, 2027"
	if strings.Join(dates, "|") != want {
		t.Errorf("meetings:\n got %s\nwant %s", strings.Join(dates, "|"), want)
	}
	if sep := v.Meetings[2]; sep.Upcoming || !sep.Projections || !strings.HasSuffix(sep.StatementURL, "monetary20260916a.htm") {
		t.Errorf("September: %+v", sep)
	}
	if oct := v.Meetings[3]; !oct.Upcoming || oct.StatementURL != "" {
		t.Errorf("October is ahead and has no statement yet: %+v", oct)
	}

	// The fixture ECO is smoke-rendered against, URLs pointed back at the
	// real host.
	fix := func(s string) string { return strings.ReplaceAll(s, econ.FedHost, "https://www.federalreserve.gov") }
	v.CalendarURL = fix(v.CalendarURL)
	for i := range v.Meetings {
		v.Meetings[i].StatementURL = fix(v.Meetings[i].StatementURL)
		v.Meetings[i].MinutesURL = fix(v.Meetings[i].MinutesURL)
	}
	want2, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want2 = append(want2, '\n')
	file := filepath.Join(fixtureDir, "research-eco.json")
	if *updateFixtures {
		if err := os.WriteFile(file, want2, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	got, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("%s: %v -- regenerate with `go test ./ui -run TestEcoView -update`", file, err)
	}
	if string(got) != string(want2) {
		t.Errorf("%s is stale; regenerate with `go test ./ui -run TestEcoView -update`", file)
	}
}

func TestEcoViewWithAKeyUsesTheReleaseCalendar(t *testing.T) {
	c := recordedEcon(t, nil, false, map[string]string{
		"50": "2026-09-05 2026-10-03 2026-11-06",
		"18": "2026-10-01 2026-10-07 2026-10-08 2026-11-02",
		"10": "2026-09-11 2026-10-15",
	})
	v := ecoView(c, "test-key", noDB, ecoNow)
	u := reading(t, v, "UNRATE")
	if !u.PublishedExact || u.Published != "2026-10-03" || u.NextRelease != "2026-11-06" {
		t.Errorf("September's jobs report came out Oct 3; the next is Nov 6: %+v", u)
	}
	cpi := reading(t, v, "CPIAUCSL")
	if !cpi.PublishedExact || cpi.Published != "2026-09-11" || cpi.NextRelease != "2026-10-15" {
		t.Errorf("CPI: %+v", cpi)
	}
	// H.15 is daily; October's average is due after October ends.
	if f := reading(t, v, "FEDFUNDS"); f.Published != "2026-10-01" || f.NextRelease != "2026-11-02" {
		t.Errorf("fed funds: %+v", f)
	}
	// A release with no dates on the calendar falls back to the estimate.
	if g := reading(t, v, "INDPRO"); g.PublishedExact || g.NextRelease != "" {
		t.Errorf("no calendar: %+v", g)
	}
	for _, n := range v.Notes {
		if strings.Contains(n, "FRED_API_KEY") {
			t.Errorf("a key is configured: %v", v.Notes)
		}
	}

	bad := ecoView(c, "wrong-key", noDB, ecoNow)
	if !strings.Contains(strings.Join(bad.Notes, " "), "could not be read") ||
		strings.Contains(strings.Join(bad.Notes, " "), "wrong-key") {
		t.Errorf("a refused key is a note, and never echoed: %v", bad.Notes)
	}
	if len(bad.Readings) != len(econ.Catalog) {
		t.Errorf("readings must survive a refused key: %d", len(bad.Readings))
	}
}

func TestEcoViewNeverBlanksForOneSource(t *testing.T) {
	c := recordedEcon(t, map[string]bool{"UNRATE": true, "PAYEMS": true}, true, nil)
	fromDB := func(id string) ([]econ.Obs, bool) {
		if id != "UNRATE" {
			return nil, false
		}
		return []econ.Obs{{Date: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Value: 4.3},
			{Date: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), Value: 4.4}}, true
	}
	v := ecoView(c, "", fromDB, ecoNow)
	if len(v.Readings) != len(econ.Catalog)-1 {
		t.Fatalf("one series with no source dropped, the rest kept: %d", len(v.Readings))
	}
	u := reading(t, v, "UNRATE")
	if u.Source != "database" || u.Period != "Feb 2026" {
		t.Errorf("unemployment from the database: %+v", u)
	}
	notes := strings.Join(v.Notes, "\n")
	for _, want := range []string{
		"showing the database's copy, which ends at Feb 2026",
		"Nonfarm payrolls is missing",
		"FOMC meetings could not be read",
	} {
		if !strings.Contains(notes, want) {
			t.Errorf("notes lack %q:\n%s", want, notes)
		}
	}
	if len(v.Meetings) != 0 {
		t.Errorf("meetings from a calendar that failed: %+v", v.Meetings)
	}
}

func TestMeetingDates(t *testing.T) {
	d := func(s string) time.Time { t, _ := time.Parse("2006-01-02", s); return t }
	for _, c := range []struct {
		m    econ.Meeting
		want string
	}{
		{econ.Meeting{Start: d("2026-10-27"), End: d("2026-10-28")}, "Oct 27–28, 2026"},
		{econ.Meeting{Start: d("2024-04-30"), End: d("2024-05-01")}, "Apr 30 – May 1, 2024"},
		{econ.Meeting{Start: d("2025-08-22"), End: d("2025-08-22")}, "Aug 22, 2025"},
	} {
		if got := meetingDates(c.m); got != c.want {
			t.Errorf("%s, want %s", got, c.want)
		}
	}
}
