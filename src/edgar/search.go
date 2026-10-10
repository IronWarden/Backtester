package edgar

// Full-text search (T142e): EDGAR's own search over every filing's text
// since 2001, the service behind efts.sec.gov's search page. Keyless, like
// the rest of EDGAR, and paced by the same client.

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SearchPage is how many hits one search returns, EDGAR's own page size.
const SearchPage = 100

// SearchHit is one matching document. A filing whose press release and
// 8-K both match appears twice, once per document.
type SearchHit struct {
	Accession string
	// FileName is the document within the filing; DocType is its type
	// ("10-K", "EX-99.1").
	FileName string
	DocType  string
	// Form is the filing's form ("8-K", "10-K/A").
	Form  string
	Items []string
	Filed time.Time
	// Companies are EDGAR's display names, "APPLE INC  (AAPL)  (CIK 0000320193)";
	// the first is the filer whose folder holds the document.
	Companies []string
	CIKs      []string
	URL       string
}

// FormCount is how many hits a form had, across all pages.
type FormCount struct {
	Form  string
	Count int
}

type SearchResult struct {
	Total int
	// AtLeast: EDGAR stopped counting at 10,000.
	AtLeast bool
	Hits    []SearchHit
	Forms   []FormCount
}

// SearchQuery builds the request. q is passed as the user typed it: quotes
// make a phrase, as on EDGAR's page. cik narrows to one company (padded to
// ten digits, the only form the index matches); forms to those forms; a
// non-zero since to filings from that day through until.
func SearchQuery(q, cik string, forms []string, since, until time.Time, from int) (string, error) {
	q = strings.TrimSpace(q)
	if q == "" {
		return "", fmt.Errorf("nothing to search for")
	}
	if len(q) > 300 {
		return "", fmt.Errorf("the search is too long")
	}
	v := url.Values{"q": {q}}
	if cik != "" {
		n, err := cikNumber(cik)
		if err != nil {
			return "", err
		}
		v.Set("ciks", fmt.Sprintf("%010d", n))
	}
	if len(forms) > 0 {
		v.Set("forms", strings.Join(forms, ","))
	}
	if !since.IsZero() {
		v.Set("dateRange", "custom")
		v.Set("startdt", since.Format("2006-01-02"))
		v.Set("enddt", until.Format("2006-01-02"))
	}
	if from > 0 {
		v.Set("from", strconv.Itoa(from))
	}
	return SearchHost + "/LATEST/search-index?" + v.Encode(), nil
}

var cikInName = regexp.MustCompile(`\(CIK (\d+)\)`)

// ParseSearch reads the search service's response.
func ParseSearch(raw []byte) (SearchResult, error) {
	var resp struct {
		Hits struct {
			Total struct {
				Value    int    `json:"value"`
				Relation string `json:"relation"`
			} `json:"total"`
			Hits []struct {
				ID     string `json:"_id"`
				Source struct {
					CIKs         []string `json:"ciks"`
					DisplayNames []string `json:"display_names"`
					Form         string   `json:"form"`
					FileType     string   `json:"file_type"`
					FileDate     string   `json:"file_date"`
					Adsh         string   `json:"adsh"`
					Items        []string `json:"items"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
		Aggregations struct {
			FormFilter struct {
				Buckets []struct {
					Key   string `json:"key"`
					Count int    `json:"doc_count"`
				} `json:"buckets"`
			} `json:"form_filter"`
		} `json:"aggregations"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return SearchResult{}, fmt.Errorf("EDGAR search: %v", err)
	}
	out := SearchResult{Total: resp.Hits.Total.Value, AtLeast: resp.Hits.Total.Relation == "gte",
		Hits: []SearchHit{}, Forms: []FormCount{}}
	for _, h := range resp.Hits.Hits {
		s := h.Source
		adsh, file, _ := strings.Cut(h.ID, ":")
		if s.Adsh != "" {
			adsh = s.Adsh
		}
		hit := SearchHit{Accession: adsh, FileName: file, DocType: s.FileType, Form: s.Form,
			Items: s.Items, Companies: s.DisplayNames, CIKs: s.CIKs}
		if hit.Items == nil {
			hit.Items = []string{}
		}
		if d, err := time.Parse("2006-01-02", s.FileDate); err == nil {
			hit.Filed = d
		}
		// The document sits in the folder of the filing's first CIK.
		if len(s.CIKs) > 0 && adsh != "" && file != "" {
			hit.URL = FilingFolder(s.CIKs[0], adsh) + "/" + file
		}
		out.Hits = append(out.Hits, hit)
	}
	for _, b := range resp.Aggregations.FormFilter.Buckets {
		out.Forms = append(out.Forms, FormCount{Form: b.Key, Count: b.Count})
	}
	return out, nil
}

// SortNewestFirst orders hits by filing date, newest first; EDGAR returns
// them by relevance.
func SortNewestFirst(hits []SearchHit) {
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].Filed.After(hits[j].Filed) })
}

// CompanyName is a display name without its tickers and CIK:
// "APPLE INC  (AAPL)  (CIK 0000320193)" -> "APPLE INC".
func CompanyName(display string) string {
	if i := strings.Index(display, "  ("); i > 0 {
		return strings.TrimSpace(display[:i])
	}
	return strings.TrimSpace(cikInName.ReplaceAllString(display, ""))
}

// Search runs one full-text search, cached for ten minutes.
func (c *Client) Search(q, cik string, forms []string, since, until time.Time, from int) (SearchResult, error) {
	u, err := SearchQuery(q, cik, forms, since, until, from)
	if err != nil {
		return SearchResult{}, err
	}
	raw, err := c.get(u, 10*time.Minute, 8<<20)
	if err != nil {
		return SearchResult{}, err
	}
	return ParseSearch(raw)
}
