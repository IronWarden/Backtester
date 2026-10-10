package main

// The Research tab's NEWS view: the official economic release notices (BLS,
// BEA, Census) as one stream. The watchlist's material 8-K events join it on
// the frontend, from the same per-company feeds MY reads, so a slow EDGAR
// answer never holds up the releases. See src/news for the sources and the
// allow-lists that keep the noise out.

import (
	"sync"
	"time"

	"my-backtester/src/news"
)

// newsWindowDays matches MY's window, so the two halves of NEWS cover the
// same quarter.
const newsWindowDays = myWindowDays

var (
	newsOnce   sync.Once
	newsClient *news.Client
)

func releaseClient() *news.Client {
	newsOnce.Do(func() { newsClient = news.New(secContact()) })
	return newsClient
}

type ReleaseRow struct {
	// Agency is BLS, BEA or Census; Series the release it belongs to.
	Agency  string `json:"agency"`
	Series  string `json:"series"`
	Title   string `json:"title"`
	Summary string `json:"summary"`
	URL     string `json:"url"`
	// Date is the day it was published (YYYY-MM-DD), in the agency's zone;
	// When adds the time, for ordering releases on the same day.
	Date string `json:"date"`
	When string `json:"when"`
	// Next is the series' next release date (YYYY-MM-DD), when the agency
	// publishes one.
	Next string `json:"next"`
}

type EconomicNews struct {
	Releases []ReleaseRow `json:"releases"`
	Since    string       `json:"since"`
	Notes    []string     `json:"notes"`
	Error    string       `json:"error"`
}

// EconomicReleases is the economic half of NEWS: the last quarter's release
// notices, newest first.
func (a *App) EconomicReleases() EconomicNews {
	return economicReleases(releaseClient(), news.Sources, time.Now())
}

func economicReleases(c *news.Client, sources []news.Source, now time.Time) EconomicNews {
	since := now.AddDate(0, 0, -newsWindowDays)
	out := EconomicNews{Releases: []ReleaseRow{}, Since: fmtDay(since), Notes: []string{}}
	rs, notes := c.Releases(sources, since)
	out.Notes = append(out.Notes, notes...)
	if len(rs) == 0 && len(notes) == len(sources) {
		out.Error = "None of the agencies' release feeds could be read — check the network connection."
		return out
	}
	for _, r := range rs {
		out.Releases = append(out.Releases, ReleaseRow{
			Agency: r.Agency, Series: r.Series, Title: r.Title, Summary: r.Summary, URL: r.URL,
			Date: r.Published.Format("2006-01-02"), When: r.Published.Format("2006-01-02 15:04"),
			Next: r.Next,
		})
	}
	return out
}

// ReadRelease is the reader for a NEWS release: the release itself from the
// agency's page, without the site around it, sanitized as a filing is. Only
// pages on bls.gov, bea.gov and census.gov are fetched.
func (a *App) ReadRelease(url string) ReaderDoc {
	return readRelease(releaseClient(), url)
}

func readRelease(c *news.Client, url string) ReaderDoc {
	r, err := c.Read(url)
	if err != nil {
		return ReaderDoc{URL: url, Contents: []ReaderSection{}, Error: "Could not open the release: " + err.Error()}
	}
	return readerDoc(url, r)
}
