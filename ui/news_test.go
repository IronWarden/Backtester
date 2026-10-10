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

	"my-backtester/src/news"
)

// NEWS's economic half against the agencies' recorded feeds, written out as
// the fixture the smoke render shows.
func TestEconomicReleasesListTheQuarter(t *testing.T) {
	files := map[string]string{"/cpi": "bls-cpi.xml", "/empsit": "bls-empsit.xml",
		"/bea": "bea.xml", "/census": "census.xml"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, ok := files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		b, err := os.ReadFile(filepath.Join("..", "src", "news", "testdata", f))
		if err != nil {
			t.Error(err)
		}
		w.Write(b)
	}))
	defer srv.Close()
	sources := []news.Source{
		{Agency: "BLS", Series: "Consumer Price Index", URL: srv.URL + "/cpi", Format: news.FormatBLS},
		{Agency: "BLS", Series: "Employment Situation", URL: srv.URL + "/empsit", Format: news.FormatBLS},
		{Agency: "BLS", Series: "Producer Price Index", URL: srv.URL + "/ppi", Format: news.FormatBLS},
		{Agency: "BEA", URL: srv.URL + "/bea", Format: news.FormatBEA},
		{Agency: "Census", URL: srv.URL + "/census", Format: news.FormatCensus},
	}
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	v := economicReleases(news.New("test@example.com"), sources, now)
	if v.Error != "" || v.Since != "2026-07-11" || len(v.Releases) == 0 {
		t.Fatalf("%+v", v)
	}
	if len(v.Notes) != 1 || !strings.Contains(v.Notes[0], "Producer Price Index") {
		t.Errorf("the failed feed is named, the rest still shown: %v", v.Notes)
	}
	first := v.Releases[0]
	if first.Agency != "BEA" || first.Date != "2026-10-06" || first.When != "2026-10-06 08:30" ||
		first.Next != "2026-11-04" || !strings.HasPrefix(first.URL, "https://www.bea.gov/news/") {
		t.Errorf("newest first, with its next release: %+v", first)
	}
	for _, r := range v.Releases {
		if r.Date < v.Since {
			t.Errorf("before the window: %+v", r)
		}
	}

	// The fixture the smoke render shows; the failed feed's note names the
	// test server, so it is written with the real host.
	for i, n := range v.Notes {
		v.Notes[i] = strings.ReplaceAll(n, srv.URL, "https://www.bls.gov/feed")
	}
	wantJSON, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	wantJSON = append(wantJSON, '\n')
	file := filepath.Join(fixtureDir, "research-news.json")
	if *updateFixtures {
		if err := os.WriteFile(file, wantJSON, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	gotJSON, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("%s: %v -- regenerate with `go test ./ui -run TestEconomicReleases -update`", file, err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Errorf("%s is stale; regenerate with `go test ./ui -run TestEconomicReleases -update`", file)
	}
}

func TestEconomicReleasesWithNoNetworkSaySo(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	v := economicReleases(news.New("x"), []news.Source{{Agency: "BEA", URL: srv.URL, Format: news.FormatBEA}}, time.Now())
	if v.Error == "" || len(v.Releases) != 0 {
		t.Errorf("every feed failing is an error, not an empty stream: %+v", v)
	}
}

// The reader on a BLS release, written out as the fixture the smoke render
// shows; and a URL off the agencies' sites refused.
func TestReadReleaseShowsTheReleaseInTheReader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := os.ReadFile(filepath.Join("..", "src", "news", "testdata", "page-bls-cpi.htm"))
		if err != nil {
			t.Error(err)
		}
		w.Write(b)
	}))
	defer srv.Close()
	c := news.New("test@example.com")
	c.Rewrite = func(string) string { return srv.URL }
	const u = "https://www.bls.gov/news.release/archives/cpi_09112026.htm"
	v := readRelease(c, u)
	if v.Error != "" || !strings.Contains(v.HTML, "CONSUMER PRICE INDEX - AUGUST 2026") || strings.Contains(v.HTML, "<script") {
		t.Fatalf("error %q, html %d bytes", v.Error, len(v.HTML))
	}
	if bad := readRelease(c, "https://example.com/"); !strings.HasPrefix(bad.Error, "Could not open the release") {
		t.Errorf("an off-site URL must be refused: %+v", bad)
	}

	wantJSON, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	wantJSON = append(wantJSON, '\n')
	file := filepath.Join(fixtureDir, "research-reader-release.json")
	if *updateFixtures {
		if err := os.WriteFile(file, wantJSON, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	gotJSON, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("%s: %v -- regenerate with `go test ./ui -run TestReadRelease -update`", file, err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Errorf("%s is stale; regenerate with `go test ./ui -run TestReadRelease -update`", file)
	}
}
