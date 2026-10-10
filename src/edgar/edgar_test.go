package edgar

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Every fixture under testdata/ is a real SEC response for Apple, recorded
// 2026-10-06 and trimmed: submissions (20 filings), one earnings 8-K's index
// page, the 8-K itself and its exhibit 99.1.

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestParseSubmissions(t *testing.T) {
	c, err := ParseSubmissions(fixture(t, "submissions-aapl.json"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Name != "Apple Inc." || c.CIK != "0000320193" || len(c.Filings) != 20 {
		t.Fatalf("%s %s %d", c.Name, c.CIK, len(c.Filings))
	}
	for i := 1; i < len(c.Filings); i++ {
		if c.Filings[i].Accepted.After(c.Filings[i-1].Accepted) {
			t.Fatalf("not newest first at %d", i)
		}
	}
	var earnings *Filing
	for i, f := range c.Filings {
		if f.Accession == "0000320193-26-000018" {
			earnings = &c.Filings[i]
		}
	}
	if earnings == nil || earnings.Form != "8-K" || len(earnings.Items) != 2 ||
		earnings.Items[0] != "2.02" || earnings.Filed.Format("2006-01-02") != "2026-07-30" ||
		earnings.Accepted.IsZero() {
		t.Fatalf("the July earnings 8-K: %+v", earnings)
	}
	if _, err := ParseSubmissions([]byte("<html>")); err == nil {
		t.Error("garbage must be an error")
	}
}

// The four ground truths are the "Accepted" line on each filing's own
// -index.htm page (Eastern time), read 2026-10-06; the API's raw values for
// the same filings are in submissions-aapl.json. A summer and a winter filing
// are both here because the API's error is twice the UTC offset, which
// changes with daylight saving.
func TestAcceptedTimesMatchEDGARsFilingPages(t *testing.T) {
	c, err := ParseSubmissions(fixture(t, "submissions-aapl.json"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"0000320193-26-000018": "2026-07-30 16:30:28", // 8-K, EDT
		"0000320193-26-000005": "2026-01-29 16:30:33", // 8-K, EST
		"0000320193-25-000079": "2025-10-31 06:01:26", // 10-K
		"0001140361-26-038674": "2026-10-05 18:42:45", // Form 4
	}
	for _, f := range c.Filings {
		if w, ok := want[f.Accession]; ok {
			if got := f.Accepted.In(Eastern).Format("2006-01-02 15:04:05"); got != w {
				t.Errorf("%s %s accepted %s ET, EDGAR's page says %s", f.Accession, f.Form, got, w)
			}
			delete(want, f.Accession)
		}
	}
	if len(want) > 0 {
		t.Errorf("fixture lacks %v", want)
	}
}

func TestDescribeSaysWhatAFilingIs(t *testing.T) {
	cases := []struct {
		f     Filing
		kind  Kind
		title string
	}{
		{Filing{Form: "8-K", Items: []string{"2.02", "9.01"}}, KindEvent, "Earnings results"},
		{Filing{Form: "8-K", Items: []string{"5.02"}}, KindEvent, "Directors or officers changed"},
		{Filing{Form: "8-K", Items: []string{"1.01", "2.03"}}, KindEvent, "Material agreement · New debt"},
		{Filing{Form: "8-K/A", Items: []string{"9.01"}}, KindEvent, "Current report (amended)"},
		{Filing{Form: "10-K"}, KindReport, "Annual report"},
		{Filing{Form: "10-Q"}, KindReport, "Quarterly report"},
		{Filing{Form: "4"}, KindInsider, "Insider transaction"},
		{Filing{Form: "144"}, KindInsider, "Insider intends to sell"},
		{Filing{Form: "SC 13D/A"}, KindOwnership, "Over 5% stake, active intent (13D) (amended)"},
		{Filing{Form: "SCHEDULE 13G"}, KindOwnership, "Over 5% stake, passive (13G)"},
		{Filing{Form: "DEF 14A"}, KindProxy, "Proxy statement"},
		{Filing{Form: "424B2"}, KindOffering, "Securities offering"},
		{Filing{Form: "SD", Description: "Specialized disclosure"}, KindOther, "Specialized disclosure"},
	}
	for _, c := range cases {
		k, title := Describe(c.f)
		if k != c.kind || title != c.title {
			t.Errorf("%s %v: got %s %q, want %s %q", c.f.Form, c.f.Items, k, title, c.kind, c.title)
		}
	}
}

func TestParseFilingIndexListsReadableDocuments(t *testing.T) {
	docs := ParseFilingIndex(fixture(t, "filing-index-8k.htm"))
	if len(docs) != 2 {
		t.Fatalf("want the 8-K and its exhibit, got %+v", docs)
	}
	if docs[0].Type != "8-K" || docs[0].Name != "aapl-20260730.htm" ||
		docs[0].URL != WWWHost+"/Archives/edgar/data/320193/000032019326000018/aapl-20260730.htm" {
		t.Errorf("the 8-K, with the iXBRL viewer prefix stripped: %+v", docs[0])
	}
	if docs[1].Type != "EX-99.1" || !strings.HasSuffix(docs[1].URL, "a8-kex991q3202606272026.htm") {
		t.Errorf("the press release: %+v", docs[1])
	}
}

func TestHTMLToTextReadsTheDocument(t *testing.T) {
	text := HTMLToText(string(fixture(t, "doc-ex991.htm")))
	for _, want := range []string{"Apple reports third quarter results", "Cupertino"} {
		if !strings.Contains(strings.ToLower(text), strings.ToLower(want)) {
			t.Errorf("press release text lacks %q:\n%.600s", want, text)
		}
	}
	for _, never := range []string{"<", "&nbsp;", "&#", "font-family"} {
		if strings.Contains(text, never) {
			t.Errorf("text still carries %q", never)
		}
	}
	if !strings.HasPrefix(text, "Exhibit 99.1") {
		t.Errorf("EDGAR's SGML header (<TYPE>, <SEQUENCE>, <FILENAME>) leaked in:\n%.200s", text)
	}
	if strings.Contains(text, "\n\n\n") {
		t.Error("blank lines were not collapsed")
	}
	// The 8-K's hidden inline-XBRL header (contexts, units) is not prose.
	k := HTMLToText(string(fixture(t, "doc-8k.htm")))
	if strings.Contains(k, "xbrli") || strings.Contains(k, "iso4217") {
		t.Errorf("XBRL header leaked into the text:\n%.800s", k)
	}
	if !strings.Contains(k, "Results of Operations and Financial Condition") {
		t.Errorf("the 8-K's item heading is missing:\n%.800s", k)
	}
	if got := HTMLToText("<p>A&amp;B</p><table><tr><td>x</td><td>y</td></tr></table><script>evil()</script>"); got != "A&B\nx   y" {
		t.Errorf("small document: %q", got)
	}
}

func TestArchiveURLOpensSECFilingsOnly(t *testing.T) {
	good := WWWHost + "/Archives/edgar/data/320193/000032019326000018/aapl-20260730.htm"
	if u, err := ArchiveURL(good + "?x=1#top"); err != nil || u != good {
		t.Errorf("%q %v", u, err)
	}
	for _, bad := range []string{
		"https://evil.example/Archives/edgar/data/1/x.htm",
		"http://www.sec.gov/Archives/edgar/data/1/x.htm",
		WWWHost + "/cgi-bin/browse-edgar",
		WWWHost + "/Archives/edgar/../../etc/passwd",
		"file:///etc/passwd",
	} {
		if _, err := ArchiveURL(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

// The client against a local server replaying the recorded responses: the
// User-Agent is sent, a 429 is retried, a 403 is explained, the cache serves
// a repeat, and requests are spaced.
func TestClientIsPolite(t *testing.T) {
	var hits, throttled int32
	var agent atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		agent.Store(r.Header.Get("User-Agent"))
		switch {
		case strings.HasPrefix(r.URL.Path, "/submissions/CIK0000320193.json"):
			if atomic.AddInt32(&throttled, 1) == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.Write(fixture(t, "submissions-aapl.json"))
		case strings.HasSuffix(r.URL.Path, "-index.htm"):
			w.Write(fixture(t, "filing-index-8k.htm"))
		case strings.HasSuffix(r.URL.Path, "a8-kex991q3202606272026.htm"):
			w.Write(fixture(t, "doc-ex991.htm"))
		case strings.Contains(r.URL.Path, "/submissions/CIK0000000002"):
			w.WriteHeader(http.StatusForbidden)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	oldData, oldWWW := DataHost, WWWHost
	DataHost, WWWHost = srv.URL, srv.URL
	defer func() { DataHost, WWWHost = oldData, oldWWW }()

	c := New("someone@example.com")
	start := time.Now()
	co, err := c.Filings("320193")
	if err != nil || len(co.Filings) != 20 {
		t.Fatalf("%v %d", err, len(co.Filings))
	}
	if ua, _ := agent.Load().(string); ua != "Backtester research someone@example.com" {
		t.Errorf("User-Agent %q", ua)
	}
	if time.Since(start) < MinGap {
		t.Error("the retry after a 429 was not paced")
	}
	before := atomic.LoadInt32(&hits)
	if _, err := c.Filings("0000320193"); err != nil || atomic.LoadInt32(&hits) != before {
		t.Errorf("a repeat within five minutes must come from the cache (%v)", err)
	}

	docs, err := c.Documents("320193", "0000320193-26-000018")
	if err != nil || len(docs) != 2 {
		t.Fatalf("documents: %v %+v", err, docs)
	}
	text, truncated, err := c.Read(docs[1].URL)
	if err != nil || truncated || !strings.Contains(text, "third quarter") {
		t.Errorf("read: %v %v %.200s", err, truncated, text)
	}
	if _, err := c.Filings("2"); err == nil || !strings.Contains(err.Error(), "SEC_CONTACT") {
		t.Errorf("a 403 must say what SEC wants: %v", err)
	}
	if _, _, err := c.Read("https://evil.example/Archives/edgar/x.htm"); err == nil {
		t.Error("the reader fetched a non-SEC URL")
	}
}

func TestMaterialKeepsTheEventsAnInvestorActsOn(t *testing.T) {
	cases := []struct {
		f    Filing
		want bool
	}{
		{Filing{Form: "8-K", Items: []string{"2.02", "9.01"}}, true},  // earnings
		{Filing{Form: "8-K", Items: []string{"5.02"}}, true},          // an officer leaves
		{Filing{Form: "8-K/A", Items: []string{"4.02"}}, true},        // restatement, amended
		{Filing{Form: "8-K", Items: []string{"1.03"}}, true},          // bankruptcy
		{Filing{Form: "8-K", Items: []string{"7.01", "9.01"}}, false}, // Reg FD slides
		{Filing{Form: "8-K", Items: []string{"2.03"}}, false},         // routine debt
		{Filing{Form: "8-K", Items: []string{"5.07"}}, false},         // vote results
		{Filing{Form: "8-K", Items: []string{"8.01"}}, false},         // other event
		{Filing{Form: "8-K"}, false},
		{Filing{Form: "10-Q"}, false},
		{Filing{Form: "4"}, false},
	}
	for _, c := range cases {
		if got := Material(c.f); got != c.want {
			t.Errorf("Material(%s %v) = %v, want %v", c.f.Form, c.f.Items, got, c.want)
		}
	}
}
