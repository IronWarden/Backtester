package main

// The Research tab's company views (T142a): DES (what the company is), FA
// (reported financials as first filed) and ERN (report dates and the price's
// answer), all read from tables the database already holds. Read-only, no
// network.
//
// Dates cross the wire as strings, not time.Time (see T96): a view of
// historical facts has no use for a time zone the frontend would have to
// guess at, and "2025-01-28 16:00" is exactly what the user should read.
//
// Every optional table that is missing becomes a sentence in Notes saying
// which loader fills it, never an error: a database without earnings dates
// still has a perfectly good DES.

import (
	"fmt"
	"strings"
	"time"

	"my-backtester/src/data"
)

const dayFmt = "2006-01-02"

func fmtDay(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(dayFmt)
}

// normTicker is how the research views read a typed symbol.
func normTicker(s string) string { return strings.ToUpper(strings.TrimSpace(s)) }

// ---------------------------------------------------------------------------
// DES

type PriceLine struct {
	FirstDate string  `json:"firstDate"`
	LastDate  string  `json:"lastDate"`
	Bars      int     `json:"bars"`
	LastClose float64 `json:"lastClose"`
	High52    float64 `json:"high52"`
	Low52     float64 `json:"low52"`
}

type IndexSpan struct {
	Index string `json:"index"`
	Start string `json:"start"`
	// End is "" for a current member.
	End string `json:"end"`
	// AtLeastSince: the start is the reconstruction's horizon, not a
	// recorded addition.
	AtLeastSince bool   `json:"atLeastSince"`
	Confidence   string `json:"confidence"`
}

type CompanyOverview struct {
	Ticker         string      `json:"ticker"`
	Name           string      `json:"name"`
	CIK            string      `json:"cik"`
	Sector         string      `json:"sector"`
	SICDescription string      `json:"sicDescription"`
	Exchange       string      `json:"exchange"`
	Incorporated   string      `json:"incorporated"`
	FormerNames    []string    `json:"formerNames"`
	Price          *PriceLine  `json:"price"`
	Index          []IndexSpan `json:"index"`
	Quality        string      `json:"quality"`
	QualityReason  string      `json:"qualityReason"`
	// Notes are the plain-language reasons a section is missing.
	Notes []string `json:"notes"`
	Error string   `json:"error"`
}

// CompanyOverview is DES for one ticker.
func (a *App) CompanyOverview(dbPath, ticker string) CompanyOverview {
	t := normTicker(ticker)
	out := CompanyOverview{Ticker: t, FormerNames: []string{}, Index: []IndexSpan{}, Notes: []string{}}
	if err := openForResearch(dbPath); err != "" {
		out.Error = err
		return out
	}

	closes, err := data.LoadCloses(t)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	if s, ok := data.SummarizePrices(closes); ok {
		out.Price = &PriceLine{
			FirstDate: fmtDay(s.FirstDate), LastDate: fmtDay(s.LastDate), Bars: s.Bars,
			LastClose: s.LastClose, High52: s.High52, Low52: s.Low52,
		}
	} else {
		out.Notes = append(out.Notes, fmt.Sprintf("%s has no price history in this database.", t))
	}

	profiles, err := data.LoadCompanyProfiles([]string{t})
	switch {
	case err != nil:
		out.Notes = append(out.Notes, "Company profile could not be read: "+err.Error())
	case profiles == nil:
		out.Notes = append(out.Notes, "No company profiles in this database (add_company_info.py loads them from SEC EDGAR).")
	default:
		if p, ok := profiles[t]; ok {
			out.Name, out.CIK, out.Sector = p.Name, p.CIK, p.Sector
			out.SICDescription, out.Exchange = p.SICDescription, p.Exchange
			out.Incorporated = p.StateOfIncorporation
			if p.FormerNames != nil {
				out.FormerNames = p.FormerNames
			}
		} else {
			out.Notes = append(out.Notes, fmt.Sprintf(
				"SEC EDGAR has no current filer under %s — delisted symbols are not in its ticker list.", t))
		}
	}

	spans, err := data.LoadIndexMembership("SP500")
	switch {
	case err != nil:
		out.Notes = append(out.Notes, "Index membership could not be read: "+err.Error())
	case spans == nil:
		out.Notes = append(out.Notes, "No index membership in this database (add_index_membership.py).")
	default:
		for _, s := range data.MembershipFor(spans, t) {
			span := IndexSpan{Index: "S&P 500", Start: fmtDay(s.Start), AtLeastSince: s.StartIsHorizon,
				Confidence: s.Confidence}
			if s.End.Year() < 9999 {
				span.End = fmtDay(s.End)
			}
			out.Index = append(out.Index, span)
		}
	}

	quality, err := data.LoadDataQuality([]string{t})
	switch {
	case err != nil:
		out.Notes = append(out.Notes, "The data-quality audit could not be read: "+err.Error())
	case quality == nil:
		out.Notes = append(out.Notes, "No data-quality audit in this database (add_data_quality.py).")
	default:
		if q, ok := quality[t]; ok {
			out.Quality = string(q.Verdict)
			out.QualityReason = data.QualityReason(q.Stats)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// FA

type FinancialsCell struct {
	Value float64 `json:"value"`
	Filed string  `json:"filed"`
	Form  string  `json:"form"`
}

type FinancialsRow struct {
	Metric string            `json:"metric"`
	Cells  []*FinancialsCell `json:"cells"`
}

type CompanyFinancials struct {
	Ticker    string          `json:"ticker"`
	CIK       string          `json:"cik"`
	Company   string          `json:"company"`
	Frequency string          `json:"frequency"`
	Periods   []string        `json:"periods"`
	Rows      []FinancialsRow `json:"rows"`
	Notes     []string        `json:"notes"`
	Error     string          `json:"error"`
}

// How many periods FA shows: five years, or the last two years by quarter.
const (
	faAnnualPeriods    = 5
	faQuarterlyPeriods = 8
)

// CompanyFinancials is FA for one ticker; frequency is "annual" (default)
// or "quarterly".
func (a *App) CompanyFinancials(dbPath, ticker, frequency string) CompanyFinancials {
	t := normTicker(ticker)
	if frequency != "quarterly" {
		frequency = "annual"
	}
	out := CompanyFinancials{Ticker: t, Frequency: frequency, Periods: []string{},
		Rows: []FinancialsRow{}, Notes: []string{}}
	if err := openForResearch(dbPath); err != "" {
		out.Error = err
		return out
	}
	rows, err := data.LoadSECFundamentals([]string{t})
	if err != nil {
		out.Error = err.Error()
		return out
	}
	if rows == nil {
		out.Notes = append(out.Notes, "No SEC financials in this database (add_sec_financials.py loads them, 2009 on).")
		return out
	}
	if len(rows) == 0 {
		out.Notes = append(out.Notes, fmt.Sprintf("SEC filings carry no financials under %s.", t))
		return out
	}
	prefer := ""
	if p, _ := data.LoadCompanyProfiles([]string{t}); p != nil {
		prefer = p[t].CIK
	}
	n := faAnnualPeriods
	if frequency == "quarterly" {
		n = faQuarterlyPeriods
	}
	v := data.FinancialsTable(rows, frequency, n, prefer)
	out.CIK, out.Company = v.CIK, v.Company
	for _, p := range v.Periods {
		out.Periods = append(out.Periods, fmtDay(p))
	}
	for _, r := range v.Rows {
		row := FinancialsRow{Metric: r.Metric, Cells: make([]*FinancialsCell, len(r.Cells))}
		for i, c := range r.Cells {
			if c != nil {
				row.Cells[i] = &FinancialsCell{Value: c.Value, Filed: fmtDay(c.Filed), Form: c.Form}
			}
		}
		out.Rows = append(out.Rows, row)
	}
	if len(out.Periods) == 0 {
		out.Notes = append(out.Notes, fmt.Sprintf("No %s figures were filed under %s.", frequency, t))
	}
	return out
}

// ---------------------------------------------------------------------------
// ERN

type EarningsRow struct {
	Reported string  `json:"reported"` // "2025-01-28 16:00", US Eastern
	Timing   string  `json:"timing"`
	From     string  `json:"from"`
	To       string  `json:"to"`
	Move     float64 `json:"move"`
	HasMove  bool    `json:"hasMove"`
	Upcoming bool    `json:"upcoming"`
}

type CompanyEarnings struct {
	Ticker  string        `json:"ticker"`
	Reports []EarningsRow `json:"reports"`
	Notes   []string      `json:"notes"`
	Error   string        `json:"error"`
}

// CompanyEarnings is ERN for one ticker.
func (a *App) CompanyEarnings(dbPath, ticker string) CompanyEarnings {
	t := normTicker(ticker)
	out := CompanyEarnings{Ticker: t, Reports: []EarningsRow{}, Notes: []string{}}
	if err := openForResearch(dbPath); err != "" {
		out.Error = err
		return out
	}
	dates, err := data.LoadEarningsDates(t)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	if dates == nil {
		out.Notes = append(out.Notes, "No earnings dates for "+t+" in this database (earnings_calendar covers about 1,000 tickers).")
		return out
	}
	closes, err := data.LoadCloses(t)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	for _, r := range data.EarningsReactions(dates, closes, time.Now()) {
		out.Reports = append(out.Reports, EarningsRow{
			Reported: r.Reported.Format("2006-01-02 15:04"), Timing: r.Timing,
			From: fmtDay(r.FromDate), To: fmtDay(r.ToDate), Move: r.Move,
			HasMove: !r.ToDate.IsZero(), Upcoming: r.Upcoming,
		})
	}
	return out
}

func openForResearch(dbPath string) string {
	if dbPath == "" {
		return "Choose a database first (the database chip in the top bar)."
	}
	if _, err := data.InitDB(dbPath); err != nil {
		return fmt.Sprintf("Could not open %s: %v", dbPath, err)
	}
	return ""
}
