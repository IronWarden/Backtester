package main

// The Research tab's watchlist and MY view (T142d): the companies a user
// follows, kept in the per-user config directory (never the database, which
// may be a shipped read-only file), and one feed of what they have filed,
// newest first.
//
// MY is CN run across the list: the same filings, labelled the same way. Only events,
// reports and proxies are kept. Insider paperwork and securities offerings
// bury everything else across a watchlist -- JPMorgan files structured-note
// prospectuses by the hundred a day, and a live check found 6,500 of them in
// one quarter. Ownership filings (13D/13G) are left out because a company's
// EDGAR list mixes stakes others hold in it with stakes it holds in others:
// JPMorgan's own list is mostly JPMorgan reporting its holdings elsewhere, and
// the submissions record does not say which is which.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"my-backtester/src/apppath"
	"my-backtester/src/data"
	"my-backtester/src/edgar"
)

const watchlistFile = "watchlist.json"

// maxWatchlist keeps MY polite: each company is one EDGAR request, paced at
// ~8 a second.
const maxWatchlist = 50

// myKinds are the filing kinds MY shows.
var myKinds = map[string]bool{"event": true, "report": true, "proxy": true}

// maxFeed caps what MY returns, newest kept.
const maxFeed = 300

// myWindowDays is how far back MY looks: one quarter, so every company's
// latest quarterly report is in it.
const myWindowDays = 90

type Watchlist struct {
	Tickers []string `json:"tickers"`
	// Saved is false until the user has a watchlist file, so the frontend
	// knows to seed it from the form's holdings.
	Saved bool   `json:"saved"`
	Error string `json:"error"`
}

func watchlistPath() (string, error) {
	dir, err := apppath.ConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, watchlistFile), nil
}

// normalizeWatchlist upper-cases, trims, drops blanks and repeats, keeps the
// user's order, and caps the length.
func normalizeWatchlist(in []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, t := range in {
		t = normTicker(t)
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
		if len(out) == maxWatchlist {
			break
		}
	}
	return out
}

// LoadWatchlist reads the saved watchlist. A missing file is not an error:
// it is a user who has not made one yet.
func (a *App) LoadWatchlist() Watchlist {
	out := Watchlist{Tickers: []string{}}
	path, err := watchlistPath()
	if err != nil {
		out.Error = err.Error()
		return out
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return out
	}
	if err != nil {
		out.Error = err.Error()
		return out
	}
	var file struct {
		Tickers []string `json:"tickers"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		out.Error = fmt.Sprintf("%s is not a watchlist: %v", path, err)
		return out
	}
	out.Tickers, out.Saved = normalizeWatchlist(file.Tickers), true
	return out
}

// SaveWatchlist replaces the watchlist and returns it as stored.
func (a *App) SaveWatchlist(tickers []string) Watchlist {
	out := Watchlist{Tickers: normalizeWatchlist(tickers)}
	path, err := watchlistPath()
	if err != nil {
		out.Error = err.Error()
		return out
	}
	raw, _ := json.MarshalIndent(struct {
		Tickers []string `json:"tickers"`
	}{out.Tickers}, "", "  ")
	if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
		out.Error = err.Error()
		return out
	}
	out.Saved = true
	return out
}

type WatchItem struct {
	Ticker string `json:"ticker"`
	// When sorts the feed: "2026-07-30 16:30", US Eastern.
	When  string `json:"when"`
	Title string `json:"title"`
	// A filing's own fields, as CN has them, so the reader can open it.
	Filing *FilingRow `json:"filing"`
	CIK    string     `json:"cik"`
}

type WatchFeed struct {
	Tickers []string    `json:"tickers"`
	Recent  []WatchItem `json:"recent"`
	Since   string      `json:"since"`
	Notes   []string    `json:"notes"`
	Error   string      `json:"error"`
}

// WatchlistFeed is MY: the last quarter's filings across the tickers, newest
// first.
func (a *App) WatchlistFeed(dbPath string, tickers []string) WatchFeed {
	return watchFeed(dbPath, tickers, secClient(), time.Now())
}

func watchFeed(dbPath string, tickers []string, c *edgar.Client, now time.Time) WatchFeed {
	tickers = normalizeWatchlist(tickers)
	since := now.AddDate(0, 0, -myWindowDays)
	out := WatchFeed{Tickers: tickers, Recent: []WatchItem{}, Since: fmtDay(since), Notes: []string{}}
	if len(tickers) == 0 {
		return out
	}
	if err := openForResearch(dbPath); err != "" {
		out.Error = err
		return out
	}
	sinceKey := since.Format("2006-01-02")

	// Each company's CIK, from the database, finds it on EDGAR.
	profiles, err := data.LoadCompanyProfiles(tickers)
	switch {
	case err != nil:
		out.Notes = append(out.Notes, "Company profiles could not be read, so no filings are shown: "+err.Error())
	case profiles == nil:
		out.Notes = append(out.Notes, "Filings are found by each company's SEC CIK, which comes from "+
			"company_profile — this database has none (add_company_info.py loads it).")
	}

	// Then EDGAR, every company at once; the client paces the requests.
	var noFiler []string
	type fetched struct {
		ticker string
		view   CompanyFilings
	}
	var results []fetched
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, t := range tickers {
		p, ok := profiles[t]
		if profiles == nil {
			break
		}
		if !ok || p.CIK == "" {
			noFiler = append(noFiler, t)
			continue
		}
		wg.Add(1)
		go func(t, cik string) {
			defer wg.Done()
			v := filingsView(t, cik, c)
			mu.Lock()
			results = append(results, fetched{t, v})
			mu.Unlock()
		}(t, p.CIK)
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool { return results[i].ticker < results[j].ticker })
	for _, r := range results {
		if r.view.Error != "" {
			out.Notes = append(out.Notes, r.ticker+": "+r.view.Error)
			continue
		}
		for i, f := range r.view.Filings {
			if !myKinds[f.Kind] {
				continue
			}
			when := strings.TrimSuffix(f.Accepted, " ET")
			if when == "" {
				when = f.Filed
			}
			if when < sinceKey {
				continue
			}
			out.Recent = append(out.Recent, WatchItem{Ticker: r.ticker, When: when,
				Title: f.Title, Filing: &r.view.Filings[i], CIK: r.view.CIK})
		}
	}
	if len(noFiler) > 0 {
		out.Notes = append(out.Notes, "No SEC filer under "+strings.Join(noFiler, ", ")+
			" — delisted symbols and funds are not in EDGAR's ticker list, so they have no filings here.")
	}

	sort.SliceStable(out.Recent, func(i, j int) bool { return out.Recent[i].When > out.Recent[j].When })
	if len(out.Recent) > maxFeed {
		out.Recent = out.Recent[:maxFeed]
		out.Notes = append(out.Notes, fmt.Sprintf("Showing the newest %d items; open a company's CN for the rest.", maxFeed))
	}
	return out
}
