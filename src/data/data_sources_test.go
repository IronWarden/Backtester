package data

import (
	"strings"
	"testing"
	"time"
)

// `day` is defined in fundamentals_test.go.

func sampleSources() []DataSource {
	return []DataSource{
		{Table: "stock_data_optimized", Source: "Yahoo Finance via yfinance",
			Loader: "add_ticker.py", LoaderGitSHA: "a280c58",
			RowCount: 34988956, FetchedAt: day("2026-08-14")},
		{Table: "stock_data_optimized", Source: "spliced index/ETF legs",
			Loader: "add_collections.py", LoaderGitSHA: "a280c58",
			RowCount: 140000, FetchedAt: day("2026-07-02")},
		{Table: "delistings", Source: "Alpha Vantage LISTING_STATUS",
			Loader: "add_delistings.py", LoaderGitSHA: "59a97e6-dirty",
			RowCount: 9400, FetchedAt: day("2026-08-17")},
	}
}

// A table with two contributing loaders must report both. Returning one would
// hide half of where the price table came from — the $-benchmark series are
// built differently from the real companies and the difference matters.
func TestSourcesForReturnsEveryContributor(t *testing.T) {
	got := SourcesFor(sampleSources(), "stock_data_optimized")
	if len(got) != 2 {
		t.Fatalf("got %d sources for the price table, want 2", len(got))
	}
	loaders := map[string]bool{}
	for _, d := range got {
		loaders[d.Loader] = true
	}
	if !loaders["add_ticker.py"] || !loaders["add_collections.py"] {
		t.Errorf("missing a contributor: %v", loaders)
	}

	if got := SourcesFor(sampleSources(), "nothing_here"); len(got) != 0 {
		t.Errorf("unknown table returned %d sources", len(got))
	}
}

// A load from a modified tree cannot be repeated from its recorded sha, and
// saying so is the whole reason the suffix is stored.
func TestReproducible(t *testing.T) {
	cases := []struct {
		sha  string
		want bool
	}{
		{"a280c58", true},
		{"59a97e6-dirty", false},
		{"unknown", false},
		{"", false},
	}
	for _, tc := range cases {
		d := DataSource{LoaderGitSHA: tc.sha}
		if got := d.Reproducible(); got != tc.want {
			t.Errorf("sha %q: Reproducible() = %v, want %v", tc.sha, got, tc.want)
		}
	}
}

func TestAge(t *testing.T) {
	now := day("2026-08-17")
	d := DataSource{FetchedAt: day("2026-08-14")}
	if got := d.Age(now); got != 72*time.Hour {
		t.Errorf("Age = %v, want 72h", got)
	}
	// No fetch date recorded is not "fetched at the epoch", which would report
	// an age of half a century and read as catastrophically stale.
	if got := (DataSource{}).Age(now); got != 0 {
		t.Errorf("Age with no fetch date = %v, want 0", got)
	}
}

func TestSummarizeSources(t *testing.T) {
	got := SummarizeSources(sampleSources())
	lines := strings.Split(got, "\n")
	if len(lines) != 3 {
		t.Fatalf("summary has %d lines, want 3:\n%s", len(lines), got)
	}

	// Newest fetch first: the freshest source is the one a reader checks.
	if !strings.HasPrefix(lines[0], "delistings:") {
		t.Errorf("first line is %q, want the 2026-08-17 delistings row", lines[0])
	}
	if !strings.Contains(lines[0], "2026-08-17") {
		t.Errorf("first line omits the fetch date: %q", lines[0])
	}
	// The dirty-tree warning must reach the text a user reads, not just the
	// struct.
	if !strings.Contains(lines[0], "uncommitted") {
		t.Errorf("dirty load not flagged in the summary: %q", lines[0])
	}
	if strings.Contains(lines[1], "uncommitted") {
		t.Errorf("clean load wrongly flagged: %q", lines[1])
	}
	if strings.Contains(got, "no provenance") {
		t.Errorf("populated summary claims there is no provenance:\n%s", got)
	}
}

// An unloaded database must say so plainly rather than rendering an empty
// footer that reads as "no sources involved".
func TestSummarizeSourcesEmpty(t *testing.T) {
	if got := SummarizeSources(nil); got != "no provenance recorded" {
		t.Errorf("SummarizeSources(nil) = %q", got)
	}
}
