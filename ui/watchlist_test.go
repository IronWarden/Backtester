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

func TestWatchlistIsKeptInTheConfigDirectory(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	a := &App{}
	if w := a.LoadWatchlist(); w.Saved || w.Error != "" || len(w.Tickers) != 0 {
		t.Fatalf("no file yet is an unsaved empty list, not an error: %+v", w)
	}
	saved := a.SaveWatchlist([]string{" aapl", "MSFT", "AAPL", "", "brk.b"})
	if !saved.Saved || saved.Error != "" || strings.Join(saved.Tickers, ",") != "AAPL,MSFT,BRK.B" {
		t.Fatalf("normalized, deduplicated, in the user's order: %+v", saved)
	}
	if w := a.LoadWatchlist(); !w.Saved || strings.Join(w.Tickers, ",") != "AAPL,MSFT,BRK.B" {
		t.Errorf("read back: %+v", w)
	}
	if w := a.SaveWatchlist(nil); !w.Saved || len(w.Tickers) != 0 || !a.LoadWatchlist().Saved {
		t.Errorf("an emptied list is still a saved choice, not a reason to re-seed: %+v", w)
	}

	path, _ := watchlistPath()
	os.WriteFile(path, []byte("{not json"), 0o644)
	if w := a.LoadWatchlist(); w.Error == "" {
		t.Errorf("a corrupt file must say so: %+v", w)
	}

	long := make([]string, maxWatchlist+10)
	for i := range long {
		long[i] = "T" + strings.Repeat("X", i%5) + string(rune('A'+i%26)) + string(rune('A'+i/26))
	}
	if n := len(a.SaveWatchlist(long).Tickers); n != maxWatchlist {
		t.Errorf("capped at %d: %d", maxWatchlist, n)
	}
}

// MY over the recorded Apple filings, a scratch database with Apple's
// earnings, ACME (a CIK EDGAR has no record for) and ZZZZ (no filer).
func TestWatchFeedMergesFilingsAndEarnings(t *testing.T) {
	c := recordedSEC(t)
	path := researchDB(t)
	db, err := data.InitDB(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`INSERT INTO company_profile VALUES ('AAPL', '0000320193', 'Apple Inc.', '3571',
			'Electronic Computers', 'Manufacturing', 'Nasdaq', '', 'CA', '2026-01-01')`,
		`INSERT INTO stock_data_optimized VALUES
			('2026-07-30', 'AAPL', 0, 0, 0, 200, 1),
			('2026-07-31', 'AAPL', 0, 0, 0, 210, 1)`,
		`INSERT INTO earnings_calendar VALUES
			('AAPL', '2026-07-30 16:30:00', 'Earnings'),
			('AAPL', '2026-10-29 16:30:00', 'Earnings'),
			('AAPL', '2025-10-30 16:30:00', 'Earnings')`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
	}

	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	v := watchFeed(path, []string{"aapl", "ACME", "ZZZZ"}, c, now)
	if v.Error != "" || v.Since != "2026-07-09" {
		t.Fatalf("%+v", v)
	}
	var got []string
	for _, it := range v.Recent {
		got = append(got, it.When+" "+it.Ticker+" "+it.Title)
	}
	want := []string{
		"2026-07-31 06:01 AAPL Quarterly report",
		"2026-07-30 16:30 AAPL Earnings report",
		"2026-07-30 16:30 AAPL Earnings results",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("newest first, insider forms and anything before the window left out:\n got %s\nwant %s",
			strings.Join(got, "\n     "), strings.Join(want, "\n     "))
	}
	if e := v.Recent[1]; e.Kind != "earnings" || !e.HasMove || e.Move < 0.0499 || e.Move > 0.0501 {
		t.Errorf("the earnings item carries ERN's move: %+v", e)
	}
	if f := v.Recent[2]; f.Kind != "filing" || f.Filing == nil || f.CIK != "0000320193" ||
		f.Filing.Accession != "0000320193-26-000018" {
		t.Errorf("the filing item carries what the reader needs: %+v", f)
	}
	if len(v.Upcoming) != 1 || v.Upcoming[0].When != "2026-10-29 16:30" {
		t.Errorf("upcoming: %+v", v.Upcoming)
	}
	notes := strings.Join(v.Notes, "\n")
	for _, want := range []string{"ACME: Could not read filings", "No SEC filer under ZZZZ"} {
		if !strings.Contains(notes, want) {
			t.Errorf("notes lack %q:\n%s", want, notes)
		}
	}

	// The fixture MY is smoke-rendered against, URLs on the real host.
	fix := func(s string) string { return strings.ReplaceAll(s, edgar.WWWHost, "https://www.sec.gov") }
	for _, it := range v.Recent {
		if it.Filing != nil {
			it.Filing.IndexURL, it.Filing.PrimaryURL = fix(it.Filing.IndexURL), fix(it.Filing.PrimaryURL)
		}
	}
	wantJSON, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	wantJSON = append(wantJSON, '\n')
	file := filepath.Join(fixtureDir, "research-my.json")
	if *updateFixtures {
		if err := os.WriteFile(file, wantJSON, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	gotJSON, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("%s: %v -- regenerate with `go test ./ui -run TestWatchFeed -update`", file, err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Errorf("%s is stale; regenerate with `go test ./ui -run TestWatchFeed -update`", file)
	}
}

func TestWatchFeedOfNothingAsksForNothing(t *testing.T) {
	v := watchFeed("", nil, nil, time.Now())
	if v.Error != "" || len(v.Recent) != 0 || len(v.Upcoming) != 0 {
		t.Errorf("an empty watchlist is an empty feed, not an error: %+v", v)
	}
}
