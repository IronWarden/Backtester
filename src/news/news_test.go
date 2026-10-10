package news

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The fixtures are the agencies' own feeds, recorded 2026-10-09.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestBLSFeedIsReadAsReleases(t *testing.T) {
	rs, err := Parse(Source{"BLS", "Consumer Price Index", "", FormatBLS}, fixture(t, "bls-cpi.xml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 12 {
		t.Fatalf("want a year of monthly releases, got %d", len(rs))
	}
	r := rs[0]
	if r.Title != "CPI for all items increases 0.4% in August; gasoline rises" ||
		r.URL != "https://www.bls.gov/news.release/archives/cpi_09112026.htm" ||
		r.Published.Format("2006-01-02") != "2026-09-11" || r.Series != "Consumer Price Index" {
		t.Errorf("%+v", r)
	}
	if !strings.HasPrefix(r.Summary, "In August, the Consumer Price Index for All Urban Consumers rose 0.4 percent") ||
		strings.Contains(r.Summary, "  ") {
		t.Errorf("summary is the agency's lede, whitespace collapsed: %q", r.Summary)
	}
}

func TestBEAKeepsOnlyTheMarketMovingReleases(t *testing.T) {
	rs, err := Parse(Source{"BEA", "", "", FormatBEA}, fixture(t, "bea.xml"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, r := range rs {
		seen[r.Series] = true
		for _, noise := range []string{"Satellite", "Guam", "Puerto Rico", "by State", "by County", "Outdoor"} {
			if strings.Contains(r.Title, noise) {
				t.Errorf("noise kept: %s", r.Title)
			}
		}
	}
	for _, want := range []string{"GDP", "Personal income and outlays (PCE)", "International trade", "International transactions"} {
		if !seen[want] {
			t.Errorf("no %s release kept", want)
		}
	}
	trade := rs[0]
	if trade.Series != "International trade" || trade.Next != "2026-11-04" ||
		trade.Published.Format("2006-01-02 15:04") != "2026-10-06 08:30" ||
		!strings.HasPrefix(trade.Summary, "The U.S. goods and services trade deficit increased in August 2026") {
		t.Errorf("%+v", trade)
	}
}

func TestBEANextReleaseIsADateOrNothing(t *testing.T) {
	for in, want := range map[string]string{
		"November 4, 2026": "2026-11-04",
		"April 09 2026":    "2026-04-09",
		"Spring 2027":      "",
		"xx":               "",
		"":                 "",
	} {
		if got := nextDate(in); got != want {
			t.Errorf("nextDate(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCensusKeepsTheHeadlineIndicators(t *testing.T) {
	rs, err := Parse(Source{"Census", "", "", FormatCensus}, fixture(t, "census.xml"))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range rs {
		got = append(got, r.Series)
	}
	want := "Factory orders,Construction spending,Advance goods trade,Durable goods orders," +
		"New home sales,Housing starts,Business inventories,Retail sales"
	if strings.Join(got, ",") != want {
		t.Errorf("series\n got %s\nwant %s", strings.Join(got, ","), want)
	}
	r := rs[len(rs)-1]
	if r.Summary == "" || strings.Contains(r.Summary, "% Change") || r.URL != "https://www.census.gov/retail/sales.html" {
		t.Errorf("the lede alone, not the repeated figures, linked to the current release: %+v", r)
	}
}

func TestReleasesMergesNewestFirstAndNamesAFailure(t *testing.T) {
	files := map[string]string{"/cpi": "bls-cpi.xml", "/empsit": "bls-empsit.xml", "/bea": "bea.xml", "/census": "census.xml"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.UserAgent(), "me@example.com") {
			http.Error(w, "no contact", http.StatusForbidden)
			return
		}
		f, ok := files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write(fixture(t, f))
	}))
	defer srv.Close()
	sources := []Source{
		{"BLS", "Consumer Price Index", srv.URL + "/cpi", FormatBLS},
		{"BLS", "Employment Situation", srv.URL + "/empsit", FormatBLS},
		{"BLS", "Producer Price Index", srv.URL + "/ppi", FormatBLS},
		{"BEA", "", srv.URL + "/bea", FormatBEA},
		{"Census", "", srv.URL + "/census", FormatCensus},
	}
	since := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	rs, notes := New("me@example.com").Releases(sources, since)
	if len(notes) != 1 || !strings.HasPrefix(notes[0], "BLS Producer Price Index could not be read") {
		t.Errorf("one note, naming the failed feed: %v", notes)
	}
	if len(rs) == 0 || rs[0].Published.Format("2006-01-02") != "2026-10-06" {
		t.Fatalf("the newest release in the fixtures is BEA's trade report of Oct 6: %+v", rs)
	}
	agencies := map[string]bool{}
	for i, r := range rs {
		agencies[r.Agency] = true
		if r.Published.Before(since) {
			t.Errorf("before the window: %s %s", r.Published, r.Title)
		}
		if i > 0 && r.Published.After(rs[i-1].Published) {
			t.Errorf("out of order at %d: %s after %s", i, r.Title, rs[i-1].Title)
		}
	}
	for _, a := range []string{"BLS", "BEA", "Census"} {
		if !agencies[a] {
			t.Errorf("no %s release in the window", a)
		}
	}
}

func TestReleaseURLOpensTheAgenciesOnly(t *testing.T) {
	for _, ok := range []string{"https://www.bls.gov/news.release/archives/cpi_09112026.htm",
		"https://www.bea.gov/news/2026/gdp", "https://www.census.gov/retail/sales.html"} {
		if _, err := ReleaseURL(ok); err != nil {
			t.Errorf("%s refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"https://example.com/x", "http://www.bls.gov/cpi", "https://www.bls.gov.evil.com/",
		"javascript:alert(1)", "https://www.sec.gov/Archives/edgar/data/1/2.htm", "https://www.bls.gov/../etc"} {
		if _, err := ReleaseURL(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

// Each agency's release page, recorded 2026-10-09 (scripts and embedded
// images shortened), read down to the release itself.
func TestReadShowsTheReleaseWithoutTheSite(t *testing.T) {
	pages := map[string]string{
		"/news.release/archives/cpi_09112026.htm":                          "page-bls-cpi.htm",
		"/news/2026/us-international-trade-goods-and-services-august-2026": "page-bea-trade.htm",
		"/construction/nrs/current/index.html":                             "page-census-home-sales.htm",
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, ok := pages[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write(fixture(t, f))
	}))
	defer srv.Close()
	c := New("me@example.com")
	c.Rewrite = func(u string) string {
		i := strings.Index(u[len("https://"):], "/")
		return srv.URL + u[len("https://")+i:]
	}
	cases := []struct {
		url        string
		want, gone []string
	}{
		{"https://www.bls.gov/news.release/archives/cpi_09112026.htm",
			[]string{"CONSUMER PRICE INDEX - AUGUST 2026", "increased 0.4 percent on a seasonally adjusted basis"},
			[]string{"Footer links", "<script", "<form", "submenu"}},
		{"https://www.bea.gov/news/2026/us-international-trade-goods-and-services-august-2026",
			[]string{"was $105.6 billion in August", `src="https://www.bea.gov/system/files/trad0826-chart-01.png"`},
			[]string{"<script", "<form", "favicon"}},
		{"https://www.census.gov/construction/nrs/current/index.html",
			[]string{"Sales of new single-family houses in August 2026", "684,000"},
			[]string{"<script", "<form", "landingIntro"}},
	}
	for _, tc := range cases {
		r, err := c.Read(tc.url)
		if err != nil {
			t.Errorf("%s: %v", tc.url, err)
			continue
		}
		for _, w := range tc.want {
			if !strings.Contains(r.HTML, w) {
				t.Errorf("%s lacks %q", tc.url, w)
			}
		}
		for _, g := range tc.gone {
			if strings.Contains(r.HTML, g) {
				t.Errorf("%s keeps %q", tc.url, g)
			}
		}
	}
	if _, err := c.Read("https://www.bls.gov/missing.htm"); err == nil {
		t.Error("a page that is not there read as a release")
	}
	if _, err := c.Read("https://example.com/"); err == nil {
		t.Error("a page off the agencies' sites was read")
	}
}
