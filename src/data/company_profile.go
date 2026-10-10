package data

// Company identity: what a ticker actually is, and what industry it trades in.
//
// The docs said for a long time that this database has no sector data, and they
// were right about the effect if not the cause: the `company_info` table exists
// but is empty, and its columns (`Ticker`, `Date`, `Value`) are a generic long-
// format shape that was never company metadata anyway.
//
// It does not have to stay that way, and it does not cost anything. SEC EDGAR
// publishes, free and without a key, the industry classification of every
// company that has ever filed with it:
//
//	https://www.sec.gov/files/company_tickers.json      ticker -> CIK
//	https://data.sec.gov/submissions/CIK##########.json  sic, sicDescription,
//	                                                     formerNames, exchange
//
// `add_company_info.py` walks those and fills the `company_profile` table this
// file reads. Two things make it worth more than a sector label:
//
//   - It covers companies that no longer exist. SEC keeps a filer's submissions
//     history after the company is gone, so a delisted name has a sector where
//     a live-universe vendor feed would have nothing.
//   - `formerNames` is the rename and merger trail, which is part of the
//     evidence for the recycled-ticker problem in delistings.go — a symbol whose
//     filer changed identity mid-series is exactly the case that file describes.
//
// Two limits to state plainly rather than discover later. SIC is the SEC's own
// classification and is coarser and older than GICS: 3571 is "Electronic
// Computers", which puts Apple in Manufacturing, and no crosswalk to a modern
// sector taxonomy is exact. And EDGAR knows only SEC filers, so a foreign issuer
// trading here on an ADR may be missing or thinly described.
//
// Everything here is inert until the table exists: with no `company_profile`
// table the loader returns nil and no result changes.

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"
)

// CompanyProfile is one company's identity as SEC EDGAR reports it.
type CompanyProfile struct {
	Ticker string
	// CIK is the SEC's permanent identifier for the filer. Unlike a ticker
	// symbol it is never reassigned, which makes it the only stable way to say
	// "the same company" across a rename, a merger, or a symbol change.
	CIK  string
	Name string
	// SIC is the Standard Industrial Classification code as a string, because
	// that is how EDGAR reports it and leading zeros are significant (0100 is
	// agriculture, 100 is not a code).
	SIC            string
	SICDescription string
	// Sector is the SIC division SIC falls in — see SectorForSIC. Stored by the
	// loader so the mapping is visible in SQL, and re-derived here when the
	// column is empty.
	Sector   string
	Exchange string
	// FormerNames is the filer's rename history, oldest first, as EDGAR
	// reports it. Empty for a company that has never changed its name.
	FormerNames []string
	// StateOfIncorporation is the two-letter code, or a country code for a
	// foreign filer.
	StateOfIncorporation string
	FetchedAt            time.Time
}

// SIC division boundaries, from the SEC's own published structure. These are
// inclusive ranges on the numeric code and they do not overlap; anything
// outside all of them is unclassifiable, which is a real answer rather than an
// error (blank-check shells and some foreign filers land there).
//
// Kept as data rather than as a switch so the boundaries can be read at a
// glance and tested at their edges, which is where a classification like this
// goes wrong.
var sicDivisions = []struct {
	Low, High int
	Sector    string
}{
	{100, 999, "Agriculture, Forestry & Fishing"},
	{1000, 1499, "Mining"},
	{1500, 1799, "Construction"},
	{2000, 3999, "Manufacturing"},
	{4000, 4999, "Transportation & Public Utilities"},
	{5000, 5199, "Wholesale Trade"},
	{5200, 5999, "Retail Trade"},
	{6000, 6799, "Finance, Insurance & Real Estate"},
	{7000, 8999, "Services"},
	{9100, 9729, "Public Administration"},
}

// SectorUnclassified is the sector of a company whose SIC code falls in no
// division, or which has no code at all. Distinct from an empty string so a
// caller can tell "looked it up and it has none" from "never looked".
const SectorUnclassified = "Unclassified"

// SectorForSIC maps a SIC code to its division. Pure, so the boundaries are
// testable without a database — the interesting cases are all at the edges
// (1799 is Construction, 1800 is nothing, 2000 is Manufacturing).
//
// An unparseable, zero, or out-of-range code yields SectorUnclassified. The
// gaps between divisions are genuine gaps in the SEC's scheme, not omissions
// here.
func SectorForSIC(sic string) string {
	code, err := strconv.Atoi(sic)
	if err != nil {
		return SectorUnclassified
	}
	// No explicit check for zero or negative: every division starts at 100, so
	// such a code falls through the loop and lands on SectorUnclassified with
	// the rest of the unclassifiable ones.
	for _, d := range sicDivisions {
		if code >= d.Low && code <= d.High {
			return d.Sector
		}
	}
	return SectorUnclassified
}

// LoadCompanyProfiles reads the profiles for the given tickers, or for every
// ticker when tickers is empty.
//
// Returns nil without error when the `company_profile` table does not exist,
// which is the state of any database where `add_company_info.py` has not been
// run. Matches LoadDelistings: a missing optional dataset must never fail a
// backtest.
func LoadCompanyProfiles(tickers []string) (map[string]CompanyProfile, error) {
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	if !tableExists("company_profile") {
		return nil, nil
	}

	query := `SELECT ticker, cik, name, sic, sic_description, sector,
	                 exchange, former_names, state_of_incorporation, fetched_at
	          FROM company_profile`
	args := make([]any, 0, len(tickers))
	if len(tickers) > 0 {
		query += ` WHERE ticker IN (` + placeholderList(len(tickers)) + `)`
		for _, t := range tickers {
			args = append(args, t)
		}
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]CompanyProfile)
	for rows.Next() {
		var p CompanyProfile
		var cik, name, sic, sicDesc, sector, exchange, former, state sql.NullString
		var fetched sql.NullTime
		if err := rows.Scan(
			&p.Ticker, &cik, &name, &sic, &sicDesc, &sector,
			&exchange, &former, &state, &fetched,
		); err != nil {
			log.Printf("scanning company profile: %v", err)
			continue
		}
		p.CIK = cik.String
		p.Name = name.String
		p.SIC = sic.String
		p.SICDescription = sicDesc.String
		p.Sector = sector.String
		p.Exchange = exchange.String
		p.StateOfIncorporation = state.String
		p.FetchedAt = fetched.Time
		p.FormerNames = parseFormerNames(former.String)
		// The loader writes the sector, but a row loaded by an older version of
		// it — or hand-inserted — may not have one. Deriving it here keeps the
		// two paths from disagreeing.
		if p.Sector == "" {
			p.Sector = SectorForSIC(p.SIC)
		}
		out[p.Ticker] = p
	}
	return out, rows.Err()
}

// parseFormerNames reads the JSON array the loader stores. A malformed or empty
// value yields no names rather than an error: a rename history is context, and
// losing it must not cost the caller the rest of the profile.
func parseFormerNames(raw string) []string {
	if raw == "" {
		return nil
	}
	var names []string
	if err := json.Unmarshal([]byte(raw), &names); err != nil {
		return nil
	}
	return names
}

// SectorsForTickers is the narrow question most callers have: which sector is
// each of these tickers in. Tickers with no profile are absent from the result
// rather than mapped to SectorUnclassified, so "not in the table" stays
// distinguishable from "in the table with no usable code".
func SectorsForTickers(tickers []string) (map[string]string, error) {
	profiles, err := LoadCompanyProfiles(tickers)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(profiles))
	for ticker, p := range profiles {
		out[ticker] = p.Sector
	}
	return out, nil
}
