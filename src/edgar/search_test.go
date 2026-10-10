package edgar

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

// Recorded 2026-10-07: "share repurchase" in Apple's filings, and "going
// concern" in 8-Ks from September 2026 on.
func TestParseSearch(t *testing.T) {
	r, err := ParseSearch(fixture(t, "search-aapl-share-repurchase.json"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Total != 82 || r.AtLeast || len(r.Hits) != 82 {
		t.Fatalf("total %d (at least %v), %d hits", r.Total, r.AtLeast, len(r.Hits))
	}
	// A proxy filed by Carl Icahn about Apple: two companies, and the
	// document lives under the first CIK's folder.
	icahn := r.Hits[0]
	if icahn.Accession != "0000928464-14-000003" || icahn.Form != "PX14A6G" ||
		len(icahn.Companies) != 2 || CompanyName(icahn.Companies[1]) != "ICAHN CARL C" ||
		icahn.URL != "https://www.sec.gov/Archives/edgar/data/320193/000092846414000003/aaplpx14a6g012314.htm" ||
		!icahn.Filed.Equal(time.Date(2014, 1, 23, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("%+v", icahn)
	}
	if r.Hits[1].DocType != "EX-99.1" || r.Hits[1].Form != "8-K" {
		t.Errorf("a press release matched inside an 8-K: %+v", r.Hits[1])
	}
	if len(r.Forms) == 0 || r.Forms[0] != (FormCount{"10-Q", 42}) {
		t.Errorf("form counts: %+v", r.Forms)
	}
	if CompanyName("APPLE INC  (AAPL)  (CIK 0000320193)") != "APPLE INC" {
		t.Error("company name")
	}

	g, err := ParseSearch(fixture(t, "search-going-concern-8k.json"))
	if err != nil || g.Total != 409 || len(g.Hits) != SearchPage {
		t.Fatalf("%v %d %d", err, g.Total, len(g.Hits))
	}
	SortNewestFirst(g.Hits)
	for i := 1; i < len(g.Hits); i++ {
		if g.Hits[i].Filed.After(g.Hits[i-1].Filed) {
			t.Fatal("not newest first")
		}
	}
	for _, h := range g.Hits {
		if !strings.HasPrefix(h.Form, "8-K") || len(h.Items) == 0 || h.URL == "" {
			t.Fatalf("an 8-K hit with its items and a document: %+v", h)
		}
	}
	if _, err := ParseSearch([]byte("<html>")); err == nil {
		t.Error("garbage parsed")
	}
}

func TestSearchQuery(t *testing.T) {
	u, err := SearchQuery(` "going concern" `, "320193", []string{"8-K", "10-K"},
		time.Date(2025, 10, 7, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC), 100)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(u)
	q := parsed.Query()
	// The index matches only a ten-digit CIK: "320193" finds nothing.
	if q.Get("q") != `"going concern"` || q.Get("ciks") != "0000320193" ||
		q.Get("forms") != "8-K,10-K" || q.Get("from") != "100" ||
		q.Get("startdt") != "2025-10-07" || q.Get("enddt") != "2026-10-07" ||
		!strings.HasPrefix(u, SearchHost+"/LATEST/search-index?") {
		t.Errorf("%s", u)
	}
	if _, err := SearchQuery("   ", "", nil, time.Time{}, time.Time{}, 0); err == nil {
		t.Error("an empty search")
	}
	if _, err := SearchQuery(strings.Repeat("x", 301), "", nil, time.Time{}, time.Time{}, 0); err == nil {
		t.Error("an endless search")
	}
	if u, _ := SearchQuery("buyback", "", nil, time.Time{}, time.Time{}, 0); strings.Contains(u, "ciks") ||
		strings.Contains(u, "from") || strings.Contains(u, "startdt") {
		t.Errorf("no company, first page: %s", u)
	}
}
