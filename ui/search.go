package main

// The Research tab's full-text search (T142e): `"going concern"` across every
// filer, or `"buyback" AAPL` within one company, answered by EDGAR's own
// search service and opened in CN's reader. See src/edgar/search.go.

import (
	"time"

	"my-backtester/src/data"
	"my-backtester/src/edgar"
)

type SearchHitRow struct {
	Accession string `json:"accession"`
	Form      string `json:"form"`
	// Kind and Title as CN labels the filing.
	Kind  string `json:"kind"`
	Title string `json:"title"`
	// DocType is the matching document's type: "EX-99.1" when a press
	// release matched inside an 8-K.
	DocType  string `json:"docType"`
	FileName string `json:"fileName"`
	Filed    string `json:"filed"`
	// Company is the first name on the filing, Others the rest (a holder
	// filing about a company names both).
	Company string   `json:"company"`
	Others  []string `json:"others"`
	CIK     string   `json:"cik"`
	URL     string   `json:"url"`
	// Filing is the hit as a CN row, so the reader opens it unchanged.
	Filing FilingRow `json:"filing"`
}

type SearchFormCount struct {
	Form  string `json:"form"`
	Count int    `json:"count"`
}

type FilingSearch struct {
	Query   string            `json:"query"`
	Ticker  string            `json:"ticker"`
	Company string            `json:"company"`
	Since   string            `json:"since"`
	Total   int               `json:"total"`
	AtLeast bool              `json:"atLeast"`
	From    int               `json:"from"`
	Hits    []SearchHitRow    `json:"hits"`
	Forms   []SearchFormCount `json:"forms"`
	Notes   []string          `json:"notes"`
	Error   string            `json:"error"`
}

// SearchFilings searches the text of every filing since 2001. ticker, when
// set, narrows to that company (its CIK comes from company_profile); forms
// to those forms; since ("YYYY-MM-DD", or "" for all of EDGAR) to filings
// from that day. from pages through the results, 100 at a time, in EDGAR's
// order -- relevance.
func (a *App) SearchFilings(dbPath, query, ticker string, forms []string, since string, from int) FilingSearch {
	return searchFilings(dbPath, query, ticker, forms, since, from, secClient(), time.Now())
}

func searchFilings(dbPath, query, ticker string, forms []string, since string, from int,
	c *edgar.Client, now time.Time) FilingSearch {
	t := normTicker(ticker)
	out := FilingSearch{Query: query, Ticker: t, Since: since, From: from, Hits: []SearchHitRow{},
		Forms: []SearchFormCount{}, Notes: []string{}}
	cik := ""
	if t != "" {
		if err := openForResearch(dbPath); err != "" {
			out.Error = err
			return out
		}
		profiles, err := data.LoadCompanyProfiles([]string{t})
		switch {
		case err != nil:
			out.Error = err.Error()
			return out
		case profiles == nil:
			out.Error = "Searching one company needs its SEC CIK, from company_profile — this database " +
				"has none (add_company_info.py loads it). Search without the ticker to cover every filer."
			return out
		}
		p, ok := profiles[t]
		if !ok || p.CIK == "" {
			out.Error = "SEC EDGAR has no current filer under " + t + "."
			return out
		}
		cik, out.Company = p.CIK, p.Name
	}
	var start time.Time
	if since != "" {
		d, err := time.Parse(dayFmt, since)
		if err != nil {
			out.Error = "Not a date: " + since
			return out
		}
		start = d
	}
	r, err := c.Search(query, cik, forms, start, now, from)
	if err != nil {
		out.Error = "EDGAR full-text search: " + err.Error()
		return out
	}
	out.Total, out.AtLeast = r.Total, r.AtLeast
	for _, f := range r.Forms {
		out.Forms = append(out.Forms, SearchFormCount{Form: f.Form, Count: f.Count})
	}
	for _, h := range r.Hits {
		kind, title := edgar.Describe(edgar.Filing{Form: h.Form, Items: h.Items})
		row := SearchHitRow{Accession: h.Accession, Form: h.Form, Kind: string(kind), Title: title,
			DocType: h.DocType, FileName: h.FileName, Filed: fmtDay(h.Filed), URL: h.URL, Others: []string{}}
		for i, name := range h.Companies {
			if i == 0 {
				row.Company = edgar.CompanyName(name)
			} else {
				row.Others = append(row.Others, edgar.CompanyName(name))
			}
		}
		if len(h.CIKs) > 0 {
			row.CIK = h.CIKs[0]
		}
		row.Filing = FilingRow{Accession: h.Accession, Form: h.Form, Kind: row.Kind, Title: title,
			Filed: row.Filed, Items: h.Items, PrimaryURL: h.URL}
		if row.CIK != "" {
			row.Filing.IndexURL = edgar.IndexURL(row.CIK, h.Accession)
		}
		out.Hits = append(out.Hits, row)
	}
	if out.Total == 0 {
		out.Notes = append(out.Notes, "No filing's text matches. Quotes search for an exact phrase; "+
			"without them every word must appear somewhere in the document.")
	}
	if out.Total > from+len(out.Hits) {
		out.Notes = append(out.Notes, "EDGAR ranks matches by relevance and returns 100 at a time; "+
			"narrow by form or date to see the most recent.")
	}
	return out
}
