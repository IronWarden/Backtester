// Package news reads the official economic release notices -- BLS, BEA and
// Census -- from the agencies' own feeds (keyless RSS and Atom).
//
// Only the releases that move markets are kept. Each agency's feed also
// carries plenty that does not (BEA's outdoor-recreation satellite account,
// Census's quarterly services survey), and a stream with those in it is the
// noise the Research tab exists to avoid. The allow-lists below are the
// filter; a release not named in one is dropped.
//
// Every release keeps the agency's own headline and lede and a link to the
// release itself: no rewriting, no commentary.
package news

import (
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	xhtml "golang.org/x/net/html"

	"my-backtester/src/edgar"
)

// Format is how a source's feed is written.
type Format string

const (
	FormatBLS    Format = "bls"    // Atom, one feed per release series
	FormatBEA    Format = "bea"    // RSS, every BEA release in one feed
	FormatCensus Format = "census" // RSS, the latest of each Census indicator
)

// Source is one feed.
type Source struct {
	Agency string
	// Series names a BLS feed's release; BEA and Census name each item.
	Series string
	URL    string
	Format Format
}

// Sources are the feeds read, in the order their notes are reported.
var Sources = []Source{
	{"BLS", "Consumer Price Index", "https://www.bls.gov/feed/cpi.rss", FormatBLS},
	{"BLS", "Employment Situation", "https://www.bls.gov/feed/empsit.rss", FormatBLS},
	{"BLS", "Producer Price Index", "https://www.bls.gov/feed/ppi.rss", FormatBLS},
	{"BLS", "Job openings and labor turnover (JOLTS)", "https://www.bls.gov/feed/jolts.rss", FormatBLS},
	{"BLS", "Employment Cost Index", "https://www.bls.gov/feed/eci.rss", FormatBLS},
	{"BLS", "Productivity and costs", "https://www.bls.gov/feed/prod2.rss", FormatBLS},
	{"BLS", "Import and export prices", "https://www.bls.gov/feed/ximpim.rss", FormatBLS},
	{"BEA", "", "https://apps.bea.gov/rss/rss.xml", FormatBEA},
	{"Census", "", "https://www.census.gov/economic-indicators/indicator.xml", FormatCensus},
}

// beaSeries is BEA's allow-list: an item's name attribute, matched by
// prefix, and the series it is shown as. The regional, territorial and
// satellite accounts are left out.
var beaSeries = []struct{ prefix, series string }{
	{"GDP", "GDP"},
	{"Personal Income and Outlays", "Personal income and outlays (PCE)"},
	{"U.S. International Trade in Goods and Services", "International trade"},
	{"U.S. International Transactions", "International transactions"},
}

// censusSeries is Census's allow-list, by the item's guid. Its own copy of
// the international trade report (intl_trade) is left out: the release is
// joint with BEA, which carries it.
//
// Census's feed links each indicator's landing page, which describes the
// survey; censusCurrent is the page that holds the latest release itself.
var censusCurrent = map[string]string{
	"retail_sales":       "https://www.census.gov/retail/sales.html",
	"durable_goods":      "https://www.census.gov/manufacturing/m3/adv/current/index.html",
	"housing_starts":     "https://www.census.gov/construction/nrc/current/index.html",
	"home_sales":         "https://www.census.gov/construction/nrs/current/index.html",
	"construction_value": "https://www.census.gov/construction/c30/current/index.html",
	"mfg_orders":         "https://www.census.gov/manufacturing/m3/current/index.html",
	"business_sales":     "https://www.census.gov/mtis/current/index.html",
	"adv_intl_trade":     "https://www.census.gov/econ/indicators/current/index.html",
}

var censusSeries = map[string]string{
	"retail_sales":       "Retail sales",
	"durable_goods":      "Durable goods orders",
	"housing_starts":     "Housing starts",
	"home_sales":         "New home sales",
	"construction_value": "Construction spending",
	"mfg_orders":         "Factory orders",
	"business_sales":     "Business inventories",
	"adv_intl_trade":     "Advance goods trade",
}

// Release is one published release notice.
type Release struct {
	Agency  string
	Series  string
	Title   string
	Summary string
	URL     string
	// Published is when the agency posted it, in the agency's own zone.
	Published time.Time
	// Next is the next release of the series (YYYY-MM-DD), when the agency
	// says; BEA does, the others do not.
	Next string
}

// ---------------------------------------------------------------------------
// Parsing

type atomFeed struct {
	Entries []struct {
		Title string `xml:"title"`
		Link  struct {
			Href string `xml:"href,attr"`
		} `xml:"link"`
		Content   string `xml:"content"`
		Published string `xml:"published"`
	} `xml:"entry"`
}

type rssFeed struct {
	Items []struct {
		Name        string `xml:"name,attr"`
		Title       string `xml:"title"`
		Link        string `xml:"link"`
		GUID        string `xml:"guid"`
		Description string `xml:"description"`
		PubDate     string `xml:"pubDate"`
		Next        string `xml:"NextReleaseDate"`
	} `xml:"channel>item"`
}

// Parse reads one source's feed. Items the allow-lists do not name, or
// whose date cannot be read, are dropped.
func Parse(src Source, raw []byte) ([]Release, error) {
	switch src.Format {
	case FormatBLS:
		var f atomFeed
		if err := decode(raw, &f); err != nil {
			return nil, err
		}
		var out []Release
		for _, e := range f.Entries {
			t, err := time.Parse(time.RFC3339, strings.TrimSpace(e.Published))
			if err != nil {
				continue
			}
			out = append(out, Release{Agency: src.Agency, Series: src.Series, Title: clean(e.Title),
				Summary: clean(e.Content), URL: strings.TrimSpace(e.Link.Href), Published: t})
		}
		return out, nil
	case FormatBEA, FormatCensus:
		var f rssFeed
		if err := decode(raw, &f); err != nil {
			return nil, err
		}
		var out []Release
		for _, it := range f.Items {
			series := ""
			if src.Format == FormatBEA {
				for _, s := range beaSeries {
					if strings.HasPrefix(strings.TrimSpace(it.Name), s.prefix) {
						series = s.series
						break
					}
				}
			} else {
				series = censusSeries[strings.TrimSpace(it.GUID)]
			}
			if series == "" {
				continue
			}
			t, ok := rssTime(it.PubDate)
			if !ok {
				continue
			}
			summary := it.Description
			// Census follows its lede with a <p> of figures that repeat it.
			if src.Format == FormatCensus {
				if i := strings.Index(summary, "<p>"); i > 0 {
					summary = summary[:i]
				}
			}
			link := strings.TrimSpace(it.Link)
			if cur := censusCurrent[strings.TrimSpace(it.GUID)]; src.Format == FormatCensus && cur != "" {
				link = cur
			}
			out = append(out, Release{Agency: src.Agency, Series: series, Title: clean(it.Title),
				Summary: clean(summary), URL: link, Published: t, Next: nextDate(it.Next)})
		}
		return out, nil
	}
	return nil, fmt.Errorf("unknown feed format %q", src.Format)
}

func decode(raw []byte, v any) error {
	d := xml.NewDecoder(strings.NewReader(string(raw)))
	// BEA declares us-ascii; everything it sends is ASCII.
	d.CharsetReader = func(_ string, r io.Reader) (io.Reader, error) { return r, nil }
	return d.Decode(v)
}

var (
	tagRe   = regexp.MustCompile(`(?s)<[^>]*>`)
	spaceRe = regexp.MustCompile(`\s+`)
)

// clean is a feed field as plain text: tags out, entities decoded, runs of
// whitespace one space.
func clean(s string) string {
	s = tagRe.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	return strings.TrimSpace(spaceRe.ReplaceAllString(s, " "))
}

// rssTime reads an RSS pubDate. BEA writes a zone abbreviation (EDT), Census
// an offset; either way the date and time are the agency's own.
func rssTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC1123Z, time.RFC1123, "Mon, 2 Jan 2006 15:04:05 -0700", "Mon, 2 Jan 2006 15:04:05 MST"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// nextDate reads BEA's next-release date, which is free text: "November 4,
// 2026" usually, "April 09 2026" sometimes, and "Spring 2027" or "xx" when
// there is no date to give -- those come back empty.
func nextDate(s string) string {
	s = strings.TrimSpace(s)
	for _, layout := range []string{"January 2, 2006", "January 2 2006"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Format("2006-01-02")
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Fetching

// CacheTTL is how long a feed is reused. The agencies publish a handful of
// releases a day, at fixed times.
const CacheTTL = 15 * time.Minute

const maxFeedBytes = 4 << 20

type cached struct {
	body    []byte
	expires time.Time
}

// Client fetches feeds, identifying itself, and caches them in memory.
type Client struct {
	HTTP      *http.Client
	UserAgent string
	// Rewrite, when set, maps a release URL to the URL actually fetched
	// (tests point it at a recorded server).
	Rewrite func(string) string

	mu    sync.Mutex
	cache map[string]cached
}

// New returns a client whose User-Agent carries contact, as the agencies ask
// of automated readers.
func New(contact string) *Client {
	return &Client{
		HTTP:      &http.Client{Timeout: 20 * time.Second},
		UserAgent: "Backtester research " + strings.TrimSpace(contact),
		cache:     map[string]cached{},
	}
}

func (c *Client) get(u string) ([]byte, error) {
	c.mu.Lock()
	if hit, ok := c.cache[u]; ok && time.Now().Before(hit.expires) {
		c.mu.Unlock()
		return hit.body, nil
	}
	c.mu.Unlock()
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.UserAgent)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("answered %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFeedBytes))
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.cache[u] = cached{body: body, expires: time.Now().Add(CacheTTL)}
	c.mu.Unlock()
	return body, nil
}

// Releases reads every source at once and returns the releases published
// on or after since, newest first. A source that fails is a note naming it;
// the others still come back.
func (c *Client) Releases(sources []Source, since time.Time) ([]Release, []string) {
	type answer struct {
		releases []Release
		note     string
	}
	answers := make([]answer, len(sources))
	var wg sync.WaitGroup
	for i, src := range sources {
		wg.Add(1)
		go func(i int, src Source) {
			defer wg.Done()
			name := src.Agency
			if src.Series != "" {
				name += " " + src.Series
			}
			raw, err := c.get(src.URL)
			if err != nil {
				answers[i].note = name + " could not be read: " + err.Error()
				return
			}
			rs, err := Parse(src, raw)
			if err != nil {
				answers[i].note = name + "'s feed could not be read: " + err.Error()
				return
			}
			answers[i].releases = rs
		}(i, src)
	}
	wg.Wait()

	var out []Release
	var notes []string
	for _, a := range answers {
		if a.note != "" {
			notes = append(notes, a.note)
		}
		for _, r := range a.releases {
			if !r.Published.Before(since) {
				out = append(out, r)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Published.After(out[j].Published) })
	return out, notes
}

// ---------------------------------------------------------------------------
// Reading a release in the app

// releaseParts says, per agency site, which element of a release page holds
// the release: everything else on the page is the site's banner, menus and
// footer.
var releaseParts = map[string]func(n *xhtml.Node) bool{
	"www.bls.gov":    func(n *xhtml.Node) bool { return edgar.ID(n) == "bodytext" },
	"www.bea.gov":    func(n *xhtml.Node) bool { return edgar.HasClass(n, "release-body") },
	"www.census.gov": func(n *xhtml.Node) bool { return edgar.HasClass(n, "textimage") },
}

// ReleaseURL checks that a URL is a page on one of the agencies' sites, so
// the reader cannot be pointed anywhere else, and returns it cleaned.
func ReleaseURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("not a URL: %v", err)
	}
	if u.Scheme != "https" || releaseParts[u.Host] == nil || strings.Contains(u.Path, "..") {
		return "", fmt.Errorf("the reader opens BLS, BEA and Census releases only, not %q", raw)
	}
	u.RawQuery, u.Fragment = "", ""
	return u.String(), nil
}

// Read fetches a release page and returns the release alone, sanitized as a
// filing is (no scripts, forms, frames or tracking), its tables and charts
// kept. A page whose layout is not recognised is an error that says so,
// rather than the whole site shown in the reader.
func (c *Client) Read(rawURL string) (edgar.Rendered, error) {
	u, err := ReleaseURL(rawURL)
	if err != nil {
		return edgar.Rendered{}, err
	}
	raw, err := c.get(c.base(u))
	if err != nil {
		return edgar.Rendered{}, fmt.Errorf("could not be read: %v", err)
	}
	host, _ := url.Parse(u)
	r, ok := edgar.RenderPart(string(raw), u, releaseParts[host.Host])
	if !ok {
		return edgar.Rendered{}, fmt.Errorf("the release text was not found on this page — the agency may have changed its layout")
	}
	return r, nil
}

// base is where a URL is fetched from: itself, unless a test points the
// client at a recorded server.
func (c *Client) base(u string) string {
	if c.Rewrite != nil {
		return c.Rewrite(u)
	}
	return u
}
