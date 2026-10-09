package main

// The Research tab's CN view (T142b): a company's own filings as its news,
// read from SEC EDGAR, and a reader that shows any filing document as clean
// text. See src/edgar for the client and the rules it keeps.

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"my-backtester/src/data"
	"my-backtester/src/edgar"
)

// secContact is who the app says it is to SEC, which requires a contact in
// every request's User-Agent: SEC_CONTACT from the environment or a .env the
// app already reads, else the same default every Python loader uses.
func secContact() string {
	if c := strings.TrimSpace(os.Getenv("SEC_CONTACT")); c != "" {
		return c
	}
	for _, p := range claudeKeyFiles() {
		if c := strings.TrimSpace(parseEnvFile(p)["SEC_CONTACT"]); c != "" {
			return c
		}
	}
	return "gadhiarishi@gmail.com"
}

var (
	edgarOnce   sync.Once
	edgarClient *edgar.Client
)

// One client for the process, so the request pacing is process-wide.
func secClient() *edgar.Client {
	edgarOnce.Do(func() { edgarClient = edgar.New(secContact()) })
	return edgarClient
}

type FilingRow struct {
	Accession string `json:"accession"`
	Form      string `json:"form"`
	// Kind is event | report | insider | ownership | proxy | offering | other.
	Kind  string `json:"kind"`
	Title string `json:"title"`
	Filed string `json:"filed"`
	// Accepted is when EDGAR accepted it, US Eastern, to the minute: the
	// moment the information became public.
	Accepted   string   `json:"accepted"`
	ReportDate string   `json:"reportDate"`
	Items      []string `json:"items"`
	IndexURL   string   `json:"indexUrl"`
	PrimaryURL string   `json:"primaryUrl"`
}

type CompanyFilings struct {
	Ticker  string      `json:"ticker"`
	CIK     string      `json:"cik"`
	Company string      `json:"company"`
	Filings []FilingRow `json:"filings"`
	Notes   []string    `json:"notes"`
	Error   string      `json:"error"`
}

// CompanyFilings is CN: a company's recent filings, newest first.
func (a *App) CompanyFilings(dbPath, ticker string) CompanyFilings {
	t := normTicker(ticker)
	out := CompanyFilings{Ticker: t, Filings: []FilingRow{}, Notes: []string{}}
	if err := openForResearch(dbPath); err != "" {
		out.Error = err
		return out
	}
	profiles, err := data.LoadCompanyProfiles([]string{t})
	if err != nil {
		out.Error = err.Error()
		return out
	}
	p, ok := profiles[t]
	if profiles == nil {
		out.Notes = append(out.Notes, "Filings are found by the company's SEC CIK, which comes from "+
			"company_profile — this database has none (add_company_info.py loads it).")
		return out
	}
	if !ok || p.CIK == "" {
		out.Notes = append(out.Notes, "SEC EDGAR has no current filer under "+t+
			" — delisted symbols are not in its ticker list.")
		return out
	}
	return filingsView(t, p.CIK, secClient())
}

// filingsView is CN for a known CIK; split out so it can be tested against a
// recorded server.
func filingsView(ticker, cik string, c *edgar.Client) CompanyFilings {
	out := CompanyFilings{Ticker: ticker, CIK: cik, Filings: []FilingRow{}, Notes: []string{}}
	co, err := c.Filings(cik)
	if err != nil {
		out.Error = "Could not read filings from SEC EDGAR: " + err.Error()
		return out
	}
	out.Company = co.Name
	for _, f := range co.Filings {
		kind, title := edgar.Describe(f)
		row := FilingRow{
			Accession: f.Accession, Form: f.Form, Kind: string(kind), Title: title,
			Filed: fmtDay(f.Filed), ReportDate: f.ReportDate, Items: f.Items,
			IndexURL: edgar.IndexURL(cik, f.Accession),
		}
		if row.Items == nil {
			row.Items = []string{}
		}
		if !f.Accepted.IsZero() {
			row.Accepted = f.Accepted.In(edgar.Eastern).Format("2006-01-02 15:04") + " ET"
		}
		if f.PrimaryDoc != "" {
			row.PrimaryURL = edgar.FilingFolder(cik, f.Accession) + "/" + f.PrimaryDoc
		}
		out.Filings = append(out.Filings, row)
	}
	if len(out.Filings) == 0 {
		out.Notes = append(out.Notes, "EDGAR lists no recent filings for this company.")
	}
	return out
}

type FilingDocument struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	URL         string `json:"url"`
}

type FilingDocuments struct {
	Documents []FilingDocument `json:"documents"`
	Error     string           `json:"error"`
}

// FilingDocuments lists the readable documents in one filing — for an
// earnings 8-K, the 8-K itself and the press release (EX-99.1).
func (a *App) FilingDocuments(cik, accession string) FilingDocuments {
	out := FilingDocuments{Documents: []FilingDocument{}}
	docs, err := secClient().Documents(cik, accession)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	for _, d := range docs {
		out.Documents = append(out.Documents, FilingDocument{
			Name: d.Name, Type: d.Type, Description: d.Description, URL: d.URL,
		})
	}
	return out
}

type ReaderDoc struct {
	URL string `json:"url"`
	// HTML is the filing as sec.gov shows it, sanitized (edgar.RenderHTML);
	// Text is set instead for plain-text and XML documents.
	HTML      string          `json:"html"`
	Text      string          `json:"text"`
	Contents  []ReaderSection `json:"contents"`
	Truncated bool            `json:"truncated"`
	Error     string          `json:"error"`
}

// ReaderSection is one entry of the document's own table of contents.
type ReaderSection struct {
	Label  string `json:"label"`
	Target string `json:"target"`
}

// ReadFiling fetches one SEC filing document and returns it ready to show:
// its own HTML, tables and table of contents intact, for a web page; clean
// text for anything else. Only www.sec.gov/Archives/ URLs are fetched.
func (a *App) ReadFiling(url string) ReaderDoc {
	r, err := secClient().Render(url)
	if err != nil {
		return ReaderDoc{URL: url, Contents: []ReaderSection{}, Error: err.Error()}
	}
	return readerDoc(url, r)
}

func readerDoc(url string, r edgar.Rendered) ReaderDoc {
	out := ReaderDoc{URL: url, HTML: r.HTML, Text: r.Text, Truncated: r.Truncated,
		Contents: []ReaderSection{}}
	for _, s := range r.Contents {
		out.Contents = append(out.Contents, ReaderSection{Label: s.Label, Target: s.Target})
	}
	return out
}

const dayFmt = "2006-01-02"

// Dates cross the wire as strings, not time.Time (see T96): a filing date has
// no use for a time zone the frontend would have to guess at.
func fmtDay(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(dayFmt)
}

// normTicker is how the Research tab reads a typed symbol.
func normTicker(s string) string { return strings.ToUpper(strings.TrimSpace(s)) }

func openForResearch(dbPath string) string {
	if dbPath == "" {
		return "Choose a database first (the database chip in the top bar)."
	}
	if _, err := data.InitDB(dbPath); err != nil {
		return fmt.Sprintf("Could not open %s: %v", dbPath, err)
	}
	return ""
}
