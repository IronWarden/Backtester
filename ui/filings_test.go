package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"my-backtester/src/edgar"
)

// recordedSEC serves the responses recorded in src/edgar/testdata, so CN is
// tested end to end without the network.
func recordedSEC(t *testing.T) *edgar.Client {
	t.Helper()
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join("..", "src", "edgar", "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/submissions/CIK0000320193.json"):
			w.Write(read("submissions-aapl.json"))
		case strings.HasSuffix(r.URL.Path, "-index.htm"):
			w.Write(read("filing-index-8k.htm"))
		case strings.HasSuffix(r.URL.Path, "a8-kex991q3202606272026.htm"):
			w.Write(read("doc-ex991.htm"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	oldData, oldWWW := edgar.DataHost, edgar.WWWHost
	edgar.DataHost, edgar.WWWHost = srv.URL, srv.URL
	t.Cleanup(func() { edgar.DataHost, edgar.WWWHost = oldData, oldWWW })
	return edgar.New("test@example.com")
}

func TestFilingsViewReadsEDGAR(t *testing.T) {
	c := recordedSEC(t)
	v := filingsView("AAPL", "0000320193", c)
	if v.Error != "" || v.Company != "Apple Inc." || len(v.Filings) != 20 {
		t.Fatalf("%+v", v)
	}
	var earnings *FilingRow
	for i, f := range v.Filings {
		if f.Accession == "0000320193-26-000018" {
			earnings = &v.Filings[i]
		}
	}
	if earnings == nil || earnings.Kind != "event" || earnings.Title != "Earnings results" ||
		!strings.HasSuffix(earnings.Accepted, " ET") || !strings.HasPrefix(earnings.Accepted, "2026-07-30 16:") {
		t.Fatalf("the earnings 8-K, accepted after the close US Eastern: %+v", earnings)
	}
	if !strings.HasSuffix(earnings.PrimaryURL, "/Archives/edgar/data/320193/000032019326000018/aapl-20260730.htm") {
		t.Errorf("primary document URL: %s", earnings.PrimaryURL)
	}
	if v.Filings[0].Kind != "insider" {
		t.Errorf("the newest filings in the record are Form 4s: %+v", v.Filings[0])
	}

	docs, err := c.Documents("320193", "0000320193-26-000018")
	if err != nil || len(docs) != 2 {
		t.Fatalf("%v %+v", err, docs)
	}
	text, _, err := c.Read(docs[1].URL)
	if err != nil {
		t.Fatal(err)
	}

	// The fixtures the CN view and the reader are smoke-rendered against.
	// URLs are rewritten from the test server to the real host, so the
	// fixture reads as what the app sees.
	fix := func(s string) string { return strings.ReplaceAll(s, edgar.WWWHost, "https://www.sec.gov") }
	for i := range v.Filings {
		v.Filings[i].IndexURL = fix(v.Filings[i].IndexURL)
		v.Filings[i].PrimaryURL = fix(v.Filings[i].PrimaryURL)
	}
	var fd FilingDocuments
	for _, d := range docs {
		fd.Documents = append(fd.Documents, FilingDocument{Name: d.Name, Type: d.Type,
			Description: d.Description, URL: fix(d.URL)})
	}
	for name, val := range map[string]any{
		"research-cn":     v,
		"research-docs":   fd,
		"research-reader": ReaderDoc{URL: fix(docs[1].URL), Text: text},
	} {
		want, err := json.MarshalIndent(val, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, '\n')
		file := filepath.Join(fixtureDir, name+".json")
		if *updateFixtures {
			if err := os.WriteFile(file, want, 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		got, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("%s: %v -- regenerate with `go test ./ui -run TestFilingsView -update`", file, err)
		}
		if string(got) != string(want) {
			t.Errorf("%s is stale; regenerate with `go test ./ui -run TestFilingsView -update`", file)
		}
	}
}

func TestReadFilingRefusesOtherHosts(t *testing.T) {
	if got := (&App{}).ReadFiling("https://example.com/Archives/edgar/x.htm"); got.Error == "" {
		t.Errorf("the reader fetched a non-SEC URL: %+v", got)
	}
}

func TestCompanyFilingsNeedsAProfile(t *testing.T) {
	path := researchDB(t)
	got := (&App{}).CompanyFilings(path, "ZZZZ")
	if got.Error != "" || len(got.Notes) == 0 || !strings.Contains(got.Notes[0], "delisted") {
		t.Errorf("an unknown ticker is a note, and no request is made: %+v", got)
	}
}
