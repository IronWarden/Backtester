package data

// The delisting registry, and what it says about the price table.
//
// `stock_data_optimized` is keyed by ticker symbol alone, and a ticker symbol
// is not a company — it is a lease. When a company dies the exchange hands its
// symbol to someone else, and a loader that fetches "every bar ever printed
// under DD" gets DowDuPont until 2019 and DuPont de Nemours afterwards, in one
// unbroken series with no marker where the seam is.
//
// This is not survivorship bias and it is worse. Survivorship bias makes a
// backtest optimistic in a direction you can reason about. A spliced series
// makes it arbitrary: the strategy holds one company and is paid by another.
//
// The `delistings` table (loaded by `add_delistings.py`) supplies the missing
// death dates. Everything here compares those dates against the bars actually
// present and reports what kind of series each ticker really is.
//
// Coverage caveat, which matters when reading any result from this: the free
// Alpha Vantage feed behind that table effectively begins in 2013 and is only
// dense from 2015. Companies that died before then are absent from the
// registry AND from the price table, so a `CoverageUnknown` verdict on a long
// backtest means "no evidence", never "verified healthy".

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Day thresholds separating the coverage classes below. A ticker's bars are
// compared against its delisting date, and how far past that date they run is
// what distinguishes a tolerable ragged edge from two companies glued together.
const (
	// StaleTailDays is how far past a delisting date bars may run and still be
	// treated as the same company. Exchanges and vendors disagree about the
	// final session by a day or two, so a few days of overhang is bookkeeping
	// noise rather than a second company.
	StaleTailDays = 5

	// SuspectStartDays begins the range where the overhang is too long to be a
	// vendor artifact but too short to prove a relisting.
	SuspectStartDays = 90

	// RecycledStartDays is where the overhang stops being explicable. Bars more
	// than two years past a delisting cannot belong to the delisted company.
	RecycledStartDays = 730
)

// GenerationSeparator joins a base symbol to its generation number when a
// recycled series is split into the companies that actually traded under it:
// `DD` stays the company trading today and the previous occupant becomes
// `DD.1`, numbering from the oldest. Every existing config keeps meaning the
// company it means now, and the dead one becomes addressable rather than
// deleted.
//
// It is one constant on purpose. `.` collides with the class-share convention
// (`BRK.B`), which ParseGeneration handles by requiring the suffix to be a
// number — but if that collision is unwanted, changing this character is the
// whole change.
const GenerationSeparator = "."

// ParseGeneration splits a symbol into its base and its generation: `DD.1` is
// generation 1 of `DD`, and a bare `DD` is generation 0, the company trading
// under the symbol today.
//
// A suffix that is not a positive integer is not a generation, so `BRK.B` comes
// back unchanged as generation 0 rather than being mangled into `BRK`.
func ParseGeneration(symbol string) (base string, generation int) {
	i := strings.LastIndex(symbol, GenerationSeparator)
	if i <= 0 || i == len(symbol)-len(GenerationSeparator) {
		return symbol, 0
	}
	suffix := symbol[i+len(GenerationSeparator):]
	n, err := strconv.Atoi(suffix)
	// A leading zero is not how these are written, so `DD.01` is somebody
	// else's convention and is left alone rather than guessed at.
	if err != nil || n < 1 || strings.HasPrefix(suffix, "0") {
		return symbol, 0
	}
	return symbol[:i], n
}

// GenerationSymbol is ParseGeneration's inverse. Generation 0 is the bare
// symbol, so round-tripping a live company changes nothing.
func GenerationSymbol(base string, generation int) string {
	if generation < 1 {
		return base
	}
	return base + GenerationSeparator + strconv.Itoa(generation)
}

// Coverage is the verdict on one ticker's price series.
type Coverage string

const (
	// CoverageUnknown means the registry has no death date for this ticker.
	// It is the absence of evidence, not a clean bill of health — see the
	// coverage caveat at the top of this file.
	CoverageUnknown Coverage = "unknown"

	// CoverageClean means the bars stop at or before the delisting date. The
	// series belongs to one company and ends when that company did.
	CoverageClean Coverage = "clean"

	// CoverageStaleTail means the bars run a few days past the delisting date.
	// Vendor disagreement about the final session; safe to use.
	CoverageStaleTail Coverage = "stale_tail"

	// CoverageSuspect means the bars run months past the delisting date. Either
	// the death date is wrong or a second company took the symbol; the data
	// cannot say which.
	CoverageSuspect Coverage = "suspect"

	// CoverageRecycled means the bars run years past the delisting date. Two
	// different companies are spliced into one price series. Do not trade it.
	CoverageRecycled Coverage = "recycled"
)

// Trustworthy reports whether a series may be used in a simulation without
// silently mixing companies. Suspect and recycled series may not.
func (c Coverage) Trustworthy() bool {
	return c != CoverageSuspect && c != CoverageRecycled
}

// Delisting is one row of the `delistings` table: a company that stopped
// trading, and when.
type Delisting struct {
	Symbol        string
	Name          string
	Exchange      string
	AssetType     string
	IPODate       time.Time
	DelistingDate time.Time
}

// TickerCoverage pairs a ticker's verdict with the evidence behind it, so a
// warning can say *why* rather than only that something is wrong.
type TickerCoverage struct {
	Ticker  string
	Verdict Coverage
	// LastBar is the final Date present in the price table.
	LastBar time.Time
	// Delisting is the registry row; zero when Verdict is CoverageUnknown.
	Delisting Delisting
	// DaysPastDelisting is LastBar minus the delisting date. Negative when the
	// bars stop before the company did.
	DaysPastDelisting int
}

// ClassifyCoverage buckets one ticker by how its bars sit against its delisting
// date. Pure, so the thresholds that decide whether a series is tradable can be
// tested without a database.
//
// A zero delisting date yields CoverageUnknown. Bars that stop well BEFORE the
// delisting date still read as clean: that is a gap in the price table, not a
// splice, and a strategy simply has nothing to trade over the gap.
//
// A series that BEGINS after the delisting date is also clean, and that is what
// makes a split verifiable: once `split_recycled.py` has moved the dead
// company's bars to `DD.1`, what is left under `DD` starts after the death and
// cannot contain it, while `DD.1` ends at it. Both halves come back clean; the
// unsplit series still comes back recycled.
func ClassifyCoverage(r DateRange, d Delisting) TickerCoverage {
	tc := TickerCoverage{Ticker: d.Symbol, LastBar: r.Max, Delisting: d}
	if d.DelistingDate.IsZero() {
		tc.Verdict = CoverageUnknown
		return tc
	}

	past := int(r.Max.Sub(d.DelistingDate).Hours() / 24)
	tc.DaysPastDelisting = past
	if r.Min.After(d.DelistingDate) {
		tc.Verdict = CoverageClean
		return tc
	}

	switch {
	case past <= StaleTailDays:
		tc.Verdict = CoverageClean
	case past < SuspectStartDays:
		tc.Verdict = CoverageStaleTail
	case past <= RecycledStartDays:
		tc.Verdict = CoverageSuspect
	default:
		tc.Verdict = CoverageRecycled
	}
	return tc
}

// ClassifyTickers applies ClassifyCoverage across a set of tickers. Tickers
// present in ranges but absent from delistings are reported as
// CoverageUnknown, so the result covers every ticker asked about rather than
// only the condemned ones. Pure, for the same reason as ClassifyCoverage.
func ClassifyTickers(
	ranges map[string]DateRange, delistings map[string]Delisting,
) map[string]TickerCoverage {
	out := make(map[string]TickerCoverage, len(ranges))
	for ticker, r := range ranges {
		d, ok := delistings[ticker]
		if !ok {
			// A generation symbol carries the base company's death date: the
			// registry never heard of `DD.1`, but `DD.1` is exactly the series
			// that ended when `DD` died.
			if base, gen := ParseGeneration(ticker); gen > 0 {
				d, ok = delistings[base]
			}
		}
		if !ok {
			out[ticker] = TickerCoverage{
				Ticker:  ticker,
				Verdict: CoverageUnknown,
				LastBar: r.Max,
			}
			continue
		}
		// The registry keys on its own symbol column; make sure the verdict is
		// labelled with the ticker actually asked about.
		d.Symbol = ticker
		out[ticker] = ClassifyCoverage(r, d)
	}
	return out
}

// UntrustworthyTickers returns the tickers whose series mixes companies,
// sorted, so a caller can refuse them or warn about them by name.
func UntrustworthyTickers(cov map[string]TickerCoverage) []string {
	var out []string
	for ticker, tc := range cov {
		if !tc.Verdict.Trustworthy() {
			out = append(out, ticker)
		}
	}
	sort.Strings(out)
	return out
}

// LoadDelistings reads the registry for the given tickers, or for every ticker
// when tickers is empty.
//
// Returns nil without error when the `delistings` table does not exist, which
// is the state of any database where `add_delistings.py` has not been run. The
// whole feature is inert in that case and results are identical to before it
// shipped — matching the rest of this package, where a missing optional
// dataset must never fail a backtest.
func LoadDelistings(tickers []string) (map[string]Delisting, error) {
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	if !tableExists("delistings") {
		return nil, nil
	}

	query := `SELECT symbol, name, exchange, asset_type, ipo_date, delisting_date
	          FROM delistings`
	args := make([]any, 0, len(tickers))
	if len(tickers) > 0 {
		query += ` WHERE symbol IN (` + placeholderList(len(tickers)) + `)`
		for _, t := range tickers {
			args = append(args, t)
		}
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]Delisting)
	for rows.Next() {
		var d Delisting
		var name, exchange, assetType sql.NullString
		var ipo, delisted sql.NullTime
		if err := rows.Scan(
			&d.Symbol, &name, &exchange, &assetType, &ipo, &delisted,
		); err != nil {
			log.Printf("scanning delisting: %v", err)
			continue
		}
		// A row with no delisting date carries no information; dropping it
		// keeps ClassifyTickers from reporting a false CoverageUnknown that
		// looks like a lookup miss.
		if !delisted.Valid {
			continue
		}
		d.Name = name.String
		d.Exchange = exchange.String
		d.AssetType = assetType.String
		d.IPODate = ipo.Time
		d.DelistingDate = delisted.Time
		out[d.Symbol] = d
	}
	return out, rows.Err()
}

// DelistingCoverage is the call a caller actually makes: it joins the price
// table's date ranges to the registry and returns a verdict per ticker. An
// empty tickers slice covers the whole price table.
//
// With no `delistings` table every ticker comes back CoverageUnknown.
func DelistingCoverage(tickers []string) (map[string]TickerCoverage, error) {
	ranges, err := TickerDateRanges(tickers)
	if err != nil {
		return nil, err
	}
	delistings, err := LoadDelistings(tickers)
	if err != nil {
		return nil, err
	}
	return ClassifyTickers(ranges, delistings), nil
}

// tableExists reports whether a table is present, so an optional dataset can
// be absent without turning into an error.
func tableExists(name string) bool {
	var n int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?`,
		name,
	).Scan(&n)
	if err != nil {
		log.Printf("checking for table %q: %v", name, err)
		return false
	}
	return n > 0
}
