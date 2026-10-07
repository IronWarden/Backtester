// Package edgar reads a company's filings from SEC EDGAR, keyless, for the
// Research tab's CN view (T142b).
//
// Filings are the primary source behind nearly all company news: an earnings
// story is a rewrite of the 8-K's exhibit 99.1, an executive departure is an
// 8-K item 5.02, an insider sale is a Form 4. Reading them directly is how the
// app gets the information without the adverts, the framing or the errors of
// whoever rewrote it.
//
// Three rules the code keeps:
//   - It identifies itself and stays polite: a User-Agent carrying SEC_CONTACT
//     (the same convention as every Python loader), no more than ~8 requests a
//     second across the whole process, retry with backoff on 429 and 5xx.
//   - It fetches only from SEC hosts. The reader takes a URL from the UI, so
//     ArchiveURL refuses anything that is not a document under
//     www.sec.gov/Archives/.
//   - Parsing is pure and tested against recorded responses; no test touches
//     the network.
package edgar

import (
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Hosts. Variables so tests can point the client at a recorded server.
var (
	DataHost   = "https://data.sec.gov"
	WWWHost    = "https://www.sec.gov"
	SearchHost = "https://efts.sec.gov"
)

// Client is a polite EDGAR reader. The zero value is not usable; use New.
type Client struct {
	HTTP      *http.Client
	UserAgent string

	mu    sync.Mutex
	last  time.Time
	cache map[string]cached
}

type cached struct {
	body    []byte
	expires time.Time
}

// MinGap spaces requests ~8 per second, inside SEC's 10/s fair-use limit.
const MinGap = 125 * time.Millisecond

// New returns a client identifying itself with contact, which SEC requires
// (a missing or anonymous User-Agent is answered with 403).
func New(contact string) *Client {
	return &Client{
		HTTP:      &http.Client{Timeout: 30 * time.Second},
		UserAgent: "Backtester research " + strings.TrimSpace(contact),
		cache:     map[string]cached{},
	}
}

// get fetches a URL, served from cache while fresh. ttl 0 means do not cache.
func (c *Client) get(u string, ttl time.Duration, maxBytes int64) ([]byte, error) {
	c.mu.Lock()
	if hit, ok := c.cache[u]; ok && time.Now().Before(hit.expires) {
		c.mu.Unlock()
		return hit.body, nil
	}
	c.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		c.pace()
		req, err := http.NewRequest(http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", c.UserAgent)
		resp, err := c.HTTP.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(1<<attempt) * 250 * time.Millisecond)
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
		resp.Body.Close()
		switch {
		case err != nil:
			lastErr = err
		case resp.StatusCode == http.StatusOK:
			if int64(len(body)) > maxBytes {
				body = body[:maxBytes]
			}
			if ttl > 0 {
				c.mu.Lock()
				c.cache[u] = cached{body: body, expires: time.Now().Add(ttl)}
				c.mu.Unlock()
			}
			return body, nil
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			lastErr = fmt.Errorf("SEC answered %s", resp.Status)
		case resp.StatusCode == http.StatusForbidden:
			return nil, fmt.Errorf("SEC refused the request (403) — it requires SEC_CONTACT " +
				"to be a real contact address")
		case resp.StatusCode == http.StatusNotFound:
			return nil, fmt.Errorf("not found on EDGAR (404)")
		default:
			return nil, fmt.Errorf("SEC answered %s", resp.Status)
		}
		time.Sleep(time.Duration(1<<attempt) * 500 * time.Millisecond)
	}
	return nil, fmt.Errorf("EDGAR did not answer: %v", lastErr)
}

// pace holds each request until MinGap after the previous one.
func (c *Client) pace() {
	c.mu.Lock()
	wait := MinGap - time.Since(c.last)
	if wait < 0 {
		wait = 0
	}
	c.last = time.Now().Add(wait)
	c.mu.Unlock()
	time.Sleep(wait)
}

// ---------------------------------------------------------------------------
// Filings list

// Filing is one entry in a company's filing history.
type Filing struct {
	Accession   string
	Form        string
	Filed       time.Time
	Accepted    time.Time // the instant EDGAR accepted it (see acceptedInstant)
	ReportDate  string
	Items       []string // 8-K items, e.g. ["2.02", "9.01"]
	PrimaryDoc  string
	Description string
}

// Company is a filer and its most recent filings, newest first.
type Company struct {
	CIK     string
	Name    string
	Filings []Filing
}

// ParseSubmissions reads data.sec.gov/submissions/CIK##########.json. Only
// the "recent" block is read: it holds the last thousand filings or the last
// year, whichever is more, which is the window a news view wants.
func ParseSubmissions(raw []byte) (Company, error) {
	var doc struct {
		CIK     string `json:"cik"`
		Name    string `json:"name"`
		Filings struct {
			Recent struct {
				Accession []string `json:"accessionNumber"`
				Filed     []string `json:"filingDate"`
				Report    []string `json:"reportDate"`
				Accepted  []string `json:"acceptanceDateTime"`
				Form      []string `json:"form"`
				Items     []string `json:"items"`
				Primary   []string `json:"primaryDocument"`
				Desc      []string `json:"primaryDocDescription"`
			} `json:"recent"`
		} `json:"filings"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return Company{}, fmt.Errorf("reading EDGAR submissions: %v", err)
	}
	r := doc.Filings.Recent
	at := func(xs []string, i int) string {
		if i < len(xs) {
			return xs[i]
		}
		return ""
	}
	out := Company{CIK: doc.CIK, Name: doc.Name}
	for i := range r.Accession {
		f := Filing{
			Accession:   r.Accession[i],
			Form:        at(r.Form, i),
			ReportDate:  at(r.Report, i),
			PrimaryDoc:  at(r.Primary, i),
			Description: at(r.Desc, i),
		}
		f.Filed, _ = time.Parse("2006-01-02", at(r.Filed, i))
		if raw, err := time.Parse(time.RFC3339, at(r.Accepted, i)); err == nil {
			f.Accepted = acceptedInstant(raw)
		}
		for _, it := range strings.Split(at(r.Items, i), ",") {
			if it = strings.TrimSpace(it); it != "" {
				f.Items = append(f.Items, it)
			}
		}
		out.Filings = append(out.Filings, f)
	}
	sort.SliceStable(out.Filings, func(i, j int) bool {
		return out.Filings[i].Accepted.After(out.Filings[j].Accepted)
	})
	return out, nil
}

// Eastern is the time zone EDGAR keeps its hours in.
var Eastern = func() *time.Location {
	if loc, err := time.LoadLocation("America/New_York"); err == nil {
		return loc
	}
	return time.FixedZone("ET", -5*3600)
}()

// acceptedInstant corrects the submissions API's acceptanceDateTime, which is
// NOT the UTC instant its "Z" claims: it is the Eastern acceptance time shifted
// by twice the UTC offset (+8h in summer, +10h in winter), as if converted to
// UTC twice. Checked against EDGAR's own filing pages, which state "Accepted"
// in Eastern time -- Apple's July 2026 earnings 8-K reads 16:30:28 there and
// 2026-07-31T00:30:28Z in the API (TestAcceptedTimesMatchEDGARsFilingPages).
// Undoing one conversion gives the true instant.
func acceptedInstant(raw time.Time) time.Time {
	w := raw.In(Eastern)
	return time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), w.Minute(), w.Second(), 0, time.UTC)
}

// Filings returns a company's recent filings, cached for five minutes.
func (c *Client) Filings(cik string) (Company, error) {
	n, err := cikNumber(cik)
	if err != nil {
		return Company{}, err
	}
	raw, err := c.get(fmt.Sprintf("%s/submissions/CIK%010d.json", DataHost, n), 5*time.Minute, 32<<20)
	if err != nil {
		return Company{}, err
	}
	return ParseSubmissions(raw)
}

func cikNumber(cik string) (int, error) {
	n, err := strconv.Atoi(strings.TrimLeft(strings.TrimSpace(cik), "0"))
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%q is not a CIK", cik)
	}
	return n, nil
}

// FilingFolder is the filing's directory on www.sec.gov.
func FilingFolder(cik, accession string) string {
	n, _ := cikNumber(cik)
	return fmt.Sprintf("%s/Archives/edgar/data/%d/%s", WWWHost, n,
		strings.ReplaceAll(accession, "-", ""))
}

// IndexURL is EDGAR's human page for one filing.
func IndexURL(cik, accession string) string {
	return FilingFolder(cik, accession) + "/" + accession + "-index.htm"
}

// ---------------------------------------------------------------------------
// What a filing is, in words

// itemLabels are the 8-K items a reader acts on, in plain words.
var itemLabels = map[string]string{
	"1.01": "Material agreement",
	"1.02": "Agreement terminated",
	"1.03": "Bankruptcy",
	"1.05": "Cybersecurity incident",
	"2.01": "Acquisition or sale completed",
	"2.02": "Earnings results",
	"2.03": "New debt",
	"2.05": "Restructuring costs",
	"2.06": "Impairment",
	"3.01": "Delisting notice",
	"3.02": "Unregistered share sale",
	"3.03": "Shareholder rights changed",
	"4.01": "Auditor changed",
	"4.02": "Past financials no longer reliable",
	"5.01": "Change in control",
	"5.02": "Directors or officers changed",
	"5.03": "Charter or bylaws amended",
	"5.07": "Shareholder vote results",
	"7.01": "Regulation FD disclosure",
	"8.01": "Other event",
}

// ItemLabel is an 8-K item in plain words, or "" for one that only says
// exhibits are attached (9.01) or is not in the table.
func ItemLabel(item string) string { return itemLabels[item] }

// Kind groups forms for the view's filters.
type Kind string

const (
	KindEvent     Kind = "event"     // 8-K, 6-K
	KindReport    Kind = "report"    // 10-K, 10-Q, 20-F, 40-F
	KindInsider   Kind = "insider"   // 3, 4, 5, 144
	KindOwnership Kind = "ownership" // 13D, 13G
	KindProxy     Kind = "proxy"     // DEF 14A and friends
	KindOffering  Kind = "offering"  // S-1, S-3, 424B
	KindOther     Kind = "other"
)

// Describe says what a filing is: its kind and a plain-language title.
func Describe(f Filing) (Kind, string) {
	form := strings.TrimSuffix(f.Form, "/A")
	amended := strings.HasSuffix(f.Form, "/A")
	title := func(s string) string {
		if amended {
			return s + " (amended)"
		}
		return s
	}
	switch {
	case form == "8-K":
		var parts []string
		for _, it := range f.Items {
			if l := ItemLabel(it); l != "" {
				parts = append(parts, l)
			}
		}
		if len(parts) == 0 {
			return KindEvent, title("Current report")
		}
		return KindEvent, title(strings.Join(parts, " · "))
	case form == "6-K":
		return KindEvent, title("Foreign issuer report")
	case form == "10-K":
		return KindReport, title("Annual report")
	case form == "10-Q":
		return KindReport, title("Quarterly report")
	case form == "20-F" || form == "40-F":
		return KindReport, title("Annual report (foreign issuer)")
	case form == "3":
		return KindInsider, title("Insider's first holdings")
	case form == "4":
		return KindInsider, title("Insider transaction")
	case form == "5":
		return KindInsider, title("Insider annual statement")
	case form == "144":
		return KindInsider, title("Insider intends to sell")
	case strings.HasPrefix(form, "SC 13D") || strings.HasPrefix(form, "SCHEDULE 13D"):
		return KindOwnership, title("Over 5% stake, active intent (13D)")
	case strings.HasPrefix(form, "SC 13G") || strings.HasPrefix(form, "SCHEDULE 13G"):
		return KindOwnership, title("Over 5% stake, passive (13G)")
	case strings.HasPrefix(form, "DEF 14") || strings.HasPrefix(form, "DEFA14") || strings.HasPrefix(form, "PRE 14"):
		return KindProxy, title("Proxy statement")
	case strings.HasPrefix(form, "S-1") || strings.HasPrefix(form, "S-3") ||
		strings.HasPrefix(form, "424B") || strings.HasPrefix(form, "F-1"):
		return KindOffering, title("Securities offering")
	}
	if f.Description != "" {
		return KindOther, title(f.Description)
	}
	return KindOther, title(f.Form)
}

// ---------------------------------------------------------------------------
// One filing's documents

// Document is one file in a filing, as the filing's index page lists it.
type Document struct {
	Name        string
	Type        string // EX-99.1, 8-K, GRAPHIC, ...
	Description string
	URL         string
}

var (
	tableFileRe = regexp.MustCompile(`(?is)<table class="tableFile"[^>]*summary="Document Format Files"[^>]*>(.*?)</table>`)
	rowRe       = regexp.MustCompile(`(?is)<tr[^>]*>(.*?)</tr>`)
	cellRe      = regexp.MustCompile(`(?is)<td[^>]*>(.*?)</td>`)
	hrefRe      = regexp.MustCompile(`(?i)href="([^"]+)"`)
	tagRe       = regexp.MustCompile(`(?s)<[^>]+>`)
)

// ParseFilingIndex reads the documents a filing carries from its
// -index.htm page: readable ones only (htm, html, txt, xml), graphics and the
// complete-submission bundle left out.
func ParseFilingIndex(page []byte) []Document {
	m := tableFileRe.FindSubmatch(page)
	if m == nil {
		return nil
	}
	var out []Document
	for _, row := range rowRe.FindAllSubmatch(m[1], -1) {
		cells := cellRe.FindAllSubmatch(row[1], -1)
		link := hrefRe.FindSubmatch(row[1])
		if len(cells) < 4 || link == nil {
			continue
		}
		clean := func(b []byte) string {
			return strings.TrimSpace(html.UnescapeString(tagRe.ReplaceAllString(string(b), " ")))
		}
		href := strings.TrimPrefix(string(link[1]), "/ix?doc=")
		name := strings.Fields(clean(cells[2][1]))
		d := Document{Description: clean(cells[1][1]), Type: clean(cells[3][1]), URL: WWWHost + href}
		if len(name) > 0 {
			d.Name = name[0]
		}
		low := strings.ToLower(d.Name)
		if d.Type == "GRAPHIC" || d.Type == "" || !(strings.HasSuffix(low, ".htm") ||
			strings.HasSuffix(low, ".html") || strings.HasSuffix(low, ".txt") || strings.HasSuffix(low, ".xml")) {
			continue
		}
		out = append(out, d)
	}
	return out
}

// Documents lists a filing's readable documents. A filing never changes, so
// its index is cached for a day.
func (c *Client) Documents(cik, accession string) ([]Document, error) {
	raw, err := c.get(IndexURL(cik, accession), 24*time.Hour, 4<<20)
	if err != nil {
		return nil, err
	}
	return ParseFilingIndex(raw), nil
}

// ---------------------------------------------------------------------------
// Reading a document

// ArchiveURL checks that a URL is a document under www.sec.gov/Archives/ and
// returns it canonical. The reader fetches only these.
func ArchiveURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("not a URL: %v", err)
	}
	www, _ := url.Parse(WWWHost)
	if u.Scheme != www.Scheme || u.Host != www.Host || !strings.HasPrefix(u.Path, "/Archives/edgar/") ||
		strings.Contains(u.Path, "..") {
		return "", fmt.Errorf("the reader opens SEC filings only, not %q", raw)
	}
	u.RawQuery, u.Fragment = "", ""
	return u.String(), nil
}

// MaxDocBytes caps a fetched document; MaxTextChars caps what is shown.
const (
	MaxDocBytes  = 16 << 20
	MaxTextChars = 400_000
)

// Read fetches a filing document and returns it as clean text. Documents are
// immutable, so they are cached for a day.
func (c *Client) Read(rawURL string) (text string, truncated bool, err error) {
	u, err := ArchiveURL(rawURL)
	if err != nil {
		return "", false, err
	}
	raw, err := c.get(u, 24*time.Hour, MaxDocBytes)
	if err != nil {
		return "", false, err
	}
	text = HTMLToText(string(raw))
	if len(text) > MaxTextChars {
		cut := strings.LastIndex(text[:MaxTextChars], "\n")
		if cut < MaxTextChars/2 {
			cut = MaxTextChars
		}
		return text[:cut], true, nil
	}
	return text, false, nil
}

var (
	dropRe = regexp.MustCompile(`(?is)<(script|style|head|title|ix:header|xbrli?:[a-z]+)[^>]*>.*?</(script|style|head|title|ix:header|xbrli?:[a-z]+)>`)
	// Block-level tags end a line; a table cell ends with a gap.
	breakRe   = regexp.MustCompile(`(?i)<(br|/p|/div|/tr|/li|/h[1-6]|/table|/title|hr)[^>]*>`)
	cellEndRe = regexp.MustCompile(`(?i)</t[dh]>`)
	commentRe = regexp.MustCompile(`(?s)<!--.*?-->`)
	// EDGAR wraps every document in SGML headers (<TYPE>EX-99.1 <SEQUENCE>2
	// <FILENAME>...); they are filing plumbing, not the document.
	sgmlRe  = regexp.MustCompile(`(?i)<(TYPE|SEQUENCE|FILENAME|DESCRIPTION)>[^<\n]*`)
	spaceRe = regexp.MustCompile(`[ \x{00a0}]+`)
	// A cell end is marked with a tab, so the gap survives space-collapsing.
	cellGapRe = regexp.MustCompile(` *\t[ \t]*`)
	blankRe   = regexp.MustCompile(`\n\s*\n\s*\n+`)
)

// HTMLToText strips a filing's HTML to readable text: no scripts, styles or
// hidden XBRL headers; one line per paragraph, row or heading; table cells
// separated by a gap; entities decoded; runs of blank lines collapsed. Plain
// text and XML pass through with tags removed. Standard library only.
func HTMLToText(doc string) string {
	s := commentRe.ReplaceAllString(doc, "")
	s = sgmlRe.ReplaceAllString(s, "")
	s = dropRe.ReplaceAllString(s, "")
	s = breakRe.ReplaceAllString(s, "\n")
	s = cellEndRe.ReplaceAllString(s, "\t")
	s = tagRe.ReplaceAllString(s, "")
	s = html.UnescapeString(s)
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		l = strings.TrimSpace(spaceRe.ReplaceAllString(l, " "))
		lines[i] = strings.TrimSpace(cellGapRe.ReplaceAllString(l, "   "))
	}
	s = strings.Join(lines, "\n")
	s = blankRe.ReplaceAllString(s, "\n\n")
	return strings.TrimSpace(s)
}
