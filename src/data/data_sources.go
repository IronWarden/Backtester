package data

// Where every table came from, and when.
//
// This database is assembled from sources with very different reliability,
// licensing and staleness: Yahoo for prices, Alpha Vantage for delistings, SEC
// for fundamentals and industry codes, Wikipedia for index membership, FRED for
// yields. Until the loaders recorded it, nothing in the file said which was
// which — so a figure could not be attributed, a backtest could not be tied to
// a data snapshot, and a source that turned out to be wrong could not be
// corrected selectively. `analysis/free_data_sources.md` shows that last case is
// the normal one rather than the exception.
//
// `loader_provenance.py` writes one row per (table, loader) — `stock_data_optimized`
// genuinely has two contributors — as the final step of every loader. Provenance
// cannot be reconstructed after the fact: once a load has run without it, the
// fetch date and the code version that produced the rows are gone.
//
// Inert until the table exists, like every other optional dataset here.

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

// DataSource is one table's provenance.
type DataSource struct {
	Table    string
	Source   string
	Endpoint string
	Licence  string
	RowCount int64
	// Loader is the script that wrote the rows, and LoaderGitSHA the engine
	// version it ran at — suffixed "-dirty" when the tree had uncommitted
	// changes, which means the load is not reproducible from the sha alone.
	Loader       string
	LoaderGitSHA string
	FetchedAt    time.Time
	Note         string
}

// Reproducible reports whether the load can be repeated from the recorded
// version. A dirty tree means the code that produced these rows was never
// committed, so the answer is no.
func (d DataSource) Reproducible() bool {
	return d.LoaderGitSHA != "" && d.LoaderGitSHA != "unknown" &&
		!strings.HasSuffix(d.LoaderGitSHA, "-dirty")
}

// Age is how long ago the data was fetched. Prices go stale in days, an index
// membership reconstruction in months, and SIC codes barely at all — so this is
// deliberately a duration for the caller to judge rather than a staleness flag
// with a threshold baked in.
func (d DataSource) Age(now time.Time) time.Duration {
	if d.FetchedAt.IsZero() {
		return 0
	}
	return now.Sub(d.FetchedAt)
}

// LoadDataSources reads the provenance table, sorted by table then loader.
//
// Returns nil without error when `data_sources` does not exist, which is the
// state of any database whose loaders predate provenance recording.
func LoadDataSources() ([]DataSource, error) {
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	if !tableExists("data_sources") {
		return nil, nil
	}

	rows, err := db.Query(`SELECT table_name, source, endpoint, licence,
	                              row_count, loader, loader_git_sha, fetched_at,
	                              note
	                       FROM data_sources
	                       ORDER BY table_name, loader`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DataSource
	for rows.Next() {
		var d DataSource
		var endpoint, licence, loader, sha, note sql.NullString
		var count sql.NullInt64
		if err := rows.Scan(
			&d.Table, &d.Source, &endpoint, &licence, &count,
			&loader, &sha, &d.FetchedAt, &note,
		); err != nil {
			log.Printf("scanning data source: %v", err)
			continue
		}
		d.Endpoint = endpoint.String
		d.Licence = licence.String
		d.RowCount = count.Int64
		d.Loader = loader.String
		d.LoaderGitSHA = sha.String
		d.Note = note.String
		out = append(out, d)
	}
	return out, rows.Err()
}

// SourcesFor returns the provenance rows for one table — plural, because a
// table can have more than one contributing loader.
func SourcesFor(sources []DataSource, table string) []DataSource {
	var out []DataSource
	for _, d := range sources {
		if d.Table == table {
			out = append(out, d)
		}
	}
	return out
}

// SummarizeSources renders provenance as one line per source, newest fetch
// first, for a log line or a UI footer. Pure, so the formatting is testable:
// this is the string a user will read to decide whether a result is current.
func SummarizeSources(sources []DataSource) string {
	if len(sources) == 0 {
		return "no provenance recorded"
	}

	ordered := make([]DataSource, len(sources))
	copy(ordered, sources)
	sort.SliceStable(ordered, func(i, j int) bool {
		if !ordered[i].FetchedAt.Equal(ordered[j].FetchedAt) {
			return ordered[i].FetchedAt.After(ordered[j].FetchedAt)
		}
		return ordered[i].Table < ordered[j].Table
	})

	var b strings.Builder
	for i, d := range ordered {
		if i > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "%s: %s, fetched %s",
			d.Table, d.Source, d.FetchedAt.Format("2006-01-02"))
		if !d.Reproducible() {
			b.WriteString(" (from an uncommitted tree)")
		}
	}
	return b.String()
}
