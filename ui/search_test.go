package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"my-backtester/src/data"
	"my-backtester/src/edgar"
)

var searchNow = time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

func searchDB(t *testing.T) string {
	t.Helper()
	path := researchDB(t)
	db, err := data.InitDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO company_profile VALUES ('AAPL', '0000320193', 'Apple Inc.', '3571',
		'Electronic Computers', 'Manufacturing', 'Nasdaq', '', 'CA', '2026-01-01')`); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSearchOneCompany(t *testing.T) {
	c := recordedSEC(t)
	path := searchDB(t)
	v := searchFilings(path, `"share repurchase"`, " aapl", nil, "", 0, c, searchNow)
	if v.Error != "" || v.Company != "Apple Inc." || v.Total != 82 || len(v.Hits) != 82 {
		t.Fatalf("%+v", v.Error)
	}
	icahn := v.Hits[0]
	if icahn.Company != "APPLE INC" || len(icahn.Others) != 1 || icahn.Others[0] != "ICAHN CARL C" ||
		icahn.Filing.IndexURL == "" || icahn.Filing.PrimaryURL != icahn.URL || icahn.CIK != "0000320193" {
		t.Errorf("a holder's filing about Apple names both: %+v", icahn)
	}
	release := v.Hits[1]
	if release.Kind != "event" || release.DocType != "EX-99.1" || release.Title == "" {
		t.Errorf("a press release inside an 8-K, labelled like CN: %+v", release)
	}
	for _, n := range v.Notes {
		if strings.Contains(n, "100 at a time") {
			t.Errorf("every hit is on this page: %v", v.Notes)
		}
	}

	// The fixture the view is smoke-rendered against: the first twelve
	// hits, URLs on the real host.
	fix := func(s string) string { return strings.ReplaceAll(s, edgar.WWWHost, "https://www.sec.gov") }
	v.Hits = v.Hits[:12]
	for i := range v.Hits {
		h := &v.Hits[i]
		h.URL, h.Filing.IndexURL, h.Filing.PrimaryURL = fix(h.URL), fix(h.Filing.IndexURL), fix(h.Filing.PrimaryURL)
	}
	want, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want = append(want, '\n')
	file := filepath.Join(fixtureDir, "research-search.json")
	if *updateFixtures {
		if err := os.WriteFile(file, want, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	got, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("%s: %v -- regenerate with `go test ./ui -run TestSearchOneCompany -update`", file, err)
	}
	if string(got) != string(want) {
		t.Errorf("%s is stale; regenerate with `go test ./ui -run TestSearchOneCompany -update`", file)
	}
}

func TestSearchEveryFiler(t *testing.T) {
	c := recordedSEC(t)
	// No ticker needs no database.
	v := searchFilings("", `"going concern"`, "", []string{"8-K"}, "2026-09-01", 0, c, searchNow)
	if v.Error != "" || v.Total != 409 || len(v.Hits) != 100 {
		t.Fatalf("%+v", v.Error)
	}
	if !strings.Contains(strings.Join(v.Notes, " "), "100 at a time") {
		t.Errorf("309 more matches must be mentioned: %v", v.Notes)
	}
}

func TestSearchRefusals(t *testing.T) {
	c := recordedSEC(t)
	path := searchDB(t)
	for name, v := range map[string]FilingSearch{
		"unknown ticker": searchFilings(path, "buyback", "ZZZZ", nil, "", 0, c, searchNow),
		"no database":    searchFilings("", "buyback", "AAPL", nil, "", 0, c, searchNow),
		"empty search":   searchFilings(path, "  ", "", nil, "", 0, c, searchNow),
		"bad date":       searchFilings(path, "buyback", "", nil, "last year", 0, c, searchNow),
	} {
		if v.Error == "" {
			t.Errorf("%s: no error: %+v", name, v)
		}
	}
}
