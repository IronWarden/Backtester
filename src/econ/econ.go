// Package econ reads the economy's official numbers for the Research tab's
// ECO view (T142c): the latest readings from FRED, the dates their releases
// come out, and the FOMC's meeting calendar from the Federal Reserve.
//
// Every source is primary or one step from it: FRED republishes the
// statistical agencies' own series unchanged, and the meeting calendar is the
// Fed's own page. Nothing here is a forecast or a consensus estimate.
//
// What needs what:
//   - Readings: FRED's fredgraph.csv, keyless (the same file fred_public.py
//     reads for build_db.py).
//   - Release dates: FRED's release calendar, which only the keyed API
//     serves. Without FRED_API_KEY a reading's publication date is an
//     estimate from the release's usual timing, and says so.
//   - FOMC meetings: federalreserve.gov's calendar page, keyless.
//
// Parsing is pure and tested against recorded responses; no test touches the
// network.
package econ

import (
	"encoding/csv"
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
	FREDHost    = "https://fred.stlouisfed.org"
	FREDAPIHost = "https://api.stlouisfed.org"
	FedHost     = "https://www.federalreserve.gov"
)

// Kind says how a series' change is read.
type Kind string

const (
	// KindIndex is a level whose change is a percentage (CPI, production).
	KindIndex Kind = "index"
	// KindRate is already a percentage; its change is in points.
	KindRate Kind = "rate"
	// KindCount is a level whose change is itself the headline (payrolls:
	// "+150K jobs"), in the series' own units.
	KindCount Kind = "count"
)

// Series is one indicator the ECO view shows.
type Series struct {
	ID    string
	Name  string
	Units string
	Kind  Kind
	// Quarterly, else monthly.
	Quarterly bool
	// ReleaseID and ReleaseName are FRED's release the series is published
	// in; the calendar is kept per release, not per series.
	ReleaseID   int
	ReleaseName string
	// TypicalLagDays is roughly how long after its period ends a reading is
	// first published: the estimate used when there is no API key.
	TypicalLagDays int
}

// Catalog is the ECO view, in the order it is shown: prices, jobs, output,
// money. Release ids were checked against FRED's release pages 2026-10-07.
var Catalog = []Series{
	{ID: "CPIAUCSL", Name: "Consumer prices (CPI)", Units: "index 1982-84=100", Kind: KindIndex,
		ReleaseID: 10, ReleaseName: "Consumer Price Index", TypicalLagDays: 13},
	{ID: "CPILFESL", Name: "Core CPI (excluding food and energy)", Units: "index 1982-84=100", Kind: KindIndex,
		ReleaseID: 10, ReleaseName: "Consumer Price Index", TypicalLagDays: 13},
	{ID: "PAYEMS", Name: "Nonfarm payrolls", Units: "thousands of jobs", Kind: KindCount,
		ReleaseID: 50, ReleaseName: "Employment Situation", TypicalLagDays: 5},
	{ID: "UNRATE", Name: "Unemployment rate", Units: "%", Kind: KindRate,
		ReleaseID: 50, ReleaseName: "Employment Situation", TypicalLagDays: 5},
	{ID: "A191RL1Q225SBEA", Name: "Real GDP growth (annualized)", Units: "%", Kind: KindRate, Quarterly: true,
		ReleaseID: 53, ReleaseName: "Gross Domestic Product", TypicalLagDays: 29},
	{ID: "INDPRO", Name: "Industrial production", Units: "index 2017=100", Kind: KindIndex,
		ReleaseID: 13, ReleaseName: "G.17 Industrial Production and Capacity Utilization", TypicalLagDays: 16},
	{ID: "M2SL", Name: "M2 money supply", Units: "billions of dollars", Kind: KindIndex,
		ReleaseID: 21, ReleaseName: "H.6 Money Stock Measures", TypicalLagDays: 24},
	{ID: "FEDFUNDS", Name: "Federal funds rate (monthly average)", Units: "%", Kind: KindRate,
		ReleaseID: 18, ReleaseName: "H.15 Selected Interest Rates", TypicalLagDays: 1},
}

// SeriesURL is the series' page on FRED, the link out for a reading.
func SeriesURL(id string) string { return "https://fred.stlouisfed.org/series/" + url.PathEscape(id) }

// ---------------------------------------------------------------------------
// Client

// Client fetches with an in-memory cache. The zero value is not usable; use
// New.
type Client struct {
	HTTP *http.Client

	mu    sync.Mutex
	cache map[string]cached
}

type cached struct {
	body    []byte
	expires time.Time
}

func New() *Client {
	return &Client{HTTP: &http.Client{Timeout: 30 * time.Second}, cache: map[string]cached{}}
}

// How long a response is reused. A reading changes at most daily and the
// meeting calendar a few times a year.
const (
	readingTTL  = time.Hour
	calendarTTL = 12 * time.Hour
)

const maxBody = 4 << 20

// userAgent keeps Go's own name in front. FRED's edge resets any request
// whose User-Agent claims a browser ("Mozilla/5.0 ...") it cannot back up,
// and hangs on names it does not recognise; Go's and curl's own are let
// through. Found 2026-10-07: curl and Python reached FRED while every
// request from this client failed with an HTTP/2 INTERNAL_ERROR.
const userAgent = "Go-http-client/1.1 (Backtester research)"

// get fetches a URL, served from cache while fresh under cacheKey. Errors
// never carry the URL, which may hold an API key.
func (c *Client) get(u, cacheKey, source string, ttl time.Duration) ([]byte, error) {
	c.mu.Lock()
	if hit, ok := c.cache[cacheKey]; ok && time.Now().Before(hit.expires) {
		c.mu.Unlock()
		return hit.body, nil
	}
	c.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, u, nil)
		if err != nil {
			return nil, fmt.Errorf("%s: bad request", source)
		}
		req.Header.Set("User-Agent", userAgent)
		resp, err := c.HTTP.Do(req)
		if err != nil {
			// The error text carries the URL, and with it any API key.
			lastErr = fmt.Errorf("no connection")
			time.Sleep(time.Duration(1<<attempt) * 250 * time.Millisecond)
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
		resp.Body.Close()
		switch {
		case err != nil:
			lastErr = fmt.Errorf("the response was cut off")
		case resp.StatusCode == http.StatusOK:
			c.mu.Lock()
			c.cache[cacheKey] = cached{body: body, expires: time.Now().Add(ttl)}
			c.mu.Unlock()
			return body, nil
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			lastErr = fmt.Errorf("answered %s", resp.Status)
		default:
			return nil, fmt.Errorf("%s answered %s", source, resp.Status)
		}
		time.Sleep(time.Duration(1<<attempt) * 500 * time.Millisecond)
	}
	return nil, fmt.Errorf("%s did not answer: %v", source, lastErr)
}

// ---------------------------------------------------------------------------
// Readings

// Obs is one observation. Date is the first day of the period it describes,
// FRED's convention: 2026-08-01 is August's reading, published in September.
type Obs struct {
	Date  time.Time
	Value float64
}

// ParseFREDCSV reads fredgraph.csv: a header, then date,value rows. A missing
// observation is "." or empty -- FRED left October 2025's CPI and
// unemployment blank when the shutdown cancelled their collection -- and is
// skipped, never read as zero. Columns are taken by position because the
// header's name for the date has changed over the years.
func ParseFREDCSV(r io.Reader) ([]Obs, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("FRED CSV: %v", err)
	}
	if len(rows) == 0 || len(rows[0]) < 2 {
		return nil, fmt.Errorf("FRED CSV has no data columns")
	}
	var out []Obs
	for _, row := range rows[1:] {
		if len(row) < 2 {
			continue
		}
		d, err := time.Parse("2006-01-02", strings.TrimSpace(row[0]))
		if err != nil {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(row[1]), 64)
		if err != nil {
			continue
		}
		out = append(out, Obs{Date: d, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Date.Before(out[j].Date) })
	return out, nil
}

// Observations fetches a series' recent history (enough for a year-on-year
// change) from FRED's keyless CSV.
func (c *Client) Observations(id string, now time.Time) ([]Obs, error) {
	start := now.AddDate(-3, 0, 0).Format("2006-01-02")
	u := fmt.Sprintf("%s/graph/fredgraph.csv?id=%s&cosd=%s", FREDHost, url.QueryEscape(id), start)
	body, err := c.get(u, u, "FRED", readingTTL)
	if err != nil {
		return nil, err
	}
	return ParseFREDCSV(strings.NewReader(string(body)))
}

// Reading is a series' latest value against the one before it.
type Reading struct {
	Series Series
	// Period is the start of the period the latest value describes; PeriodEnd
	// its last day.
	Period    time.Time
	PeriodEnd time.Time
	Value     float64
	// Prior is the previous observation, which is not always the previous
	// period: a period FRED left blank is skipped, and PriorPeriod says which
	// one it is.
	Prior       float64
	PriorPeriod time.Time
	HasPrior    bool
	// Change is from Prior: a fraction for KindIndex, points for KindRate,
	// the series' units for KindCount.
	Change float64
	// YearChange is the same against the same period a year earlier, when
	// that period has a value.
	YearChange    float64
	HasYearChange bool
}

// Summarize reads the latest value out of a sorted history. False when there
// is none.
func Summarize(s Series, obs []Obs) (Reading, bool) {
	if len(obs) == 0 {
		return Reading{}, false
	}
	last := obs[len(obs)-1]
	r := Reading{Series: s, Period: last.Date, PeriodEnd: PeriodEnd(s, last.Date), Value: last.Value}
	if len(obs) > 1 {
		prev := obs[len(obs)-2]
		r.Prior, r.PriorPeriod, r.HasPrior = prev.Value, prev.Date, true
		r.Change = change(s.Kind, prev.Value, last.Value)
	}
	yearAgo := last.Date.AddDate(-1, 0, 0)
	for _, o := range obs {
		if o.Date.Equal(yearAgo) {
			r.YearChange, r.HasYearChange = change(s.Kind, o.Value, last.Value), true
		}
	}
	return r, true
}

func change(k Kind, from, to float64) float64 {
	if k == KindIndex {
		if from == 0 {
			return 0
		}
		return to/from - 1
	}
	return to - from
}

// PeriodEnd is the last day of the month or quarter starting at start.
func PeriodEnd(s Series, start time.Time) time.Time {
	months := 1
	if s.Quarterly {
		months = 3
	}
	return start.AddDate(0, months, -1)
}

// PeriodLabel is how a reader names the period: "Aug 2026", "Q2 2026".
func PeriodLabel(s Series, start time.Time) string {
	if s.Quarterly {
		return fmt.Sprintf("Q%d %d", (int(start.Month())-1)/3+1, start.Year())
	}
	return start.Format("Jan 2006")
}

// EstimatedRelease is when a reading for the period was probably first
// published, from the release's usual timing. Only an estimate: the agencies
// move dates for holidays, and a shutdown moves them by weeks.
func EstimatedRelease(s Series, start time.Time) time.Time {
	return PeriodEnd(s, start).AddDate(0, 0, s.TypicalLagDays)
}

// ---------------------------------------------------------------------------
// Release calendar (keyed)

// ParseReleaseDates reads FRED's fred/release/dates response.
func ParseReleaseDates(raw []byte) ([]time.Time, error) {
	var resp struct {
		ErrorMessage string `json:"error_message"`
		// fred/release/dates names the field "date"; the all-releases
		// endpoint calls it "release_date". Either is read.
		ReleaseDates []struct {
			Date        string `json:"date"`
			ReleaseDate string `json:"release_date"`
		} `json:"release_dates"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("FRED release dates: %v", err)
	}
	if resp.ErrorMessage != "" {
		return nil, fmt.Errorf("FRED: %s", resp.ErrorMessage)
	}
	var out []time.Time
	for _, d := range resp.ReleaseDates {
		s := d.Date
		if s == "" {
			s = d.ReleaseDate
		}
		if t, err := time.Parse("2006-01-02", s); err == nil {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out, nil
}

// ReleaseDates fetches a release's dates from a year ago to as far ahead as
// FRED's calendar runs, future dates included. key is FRED_API_KEY.
func (c *Client) ReleaseDates(releaseID int, key string, now time.Time) ([]time.Time, error) {
	q := url.Values{
		"release_id":                         {strconv.Itoa(releaseID)},
		"api_key":                            {key},
		"file_type":                          {"json"},
		"realtime_start":                     {now.AddDate(-1, 0, 0).Format("2006-01-02")},
		"realtime_end":                       {"9999-12-31"},
		"include_release_dates_with_no_data": {"true"},
		"sort_order":                         {"asc"},
		"limit":                              {"1000"},
	}
	u := FREDAPIHost + "/fred/release/dates?" + q.Encode()
	// Keyed by the API key too, so a corrected key is not answered from what
	// the old one fetched.
	body, err := c.get(u, fmt.Sprintf("release/%d/%s", releaseID, key), "FRED's release calendar", calendarTTL)
	if err != nil {
		return nil, err
	}
	return ParseReleaseDates(body)
}

// FirstReleaseAfter is the first release date after a period ended: when a
// reading for that period was first published. The release calendar is per
// release, so a reading is matched to its release by date alone.
func FirstReleaseAfter(dates []time.Time, periodEnd time.Time) (time.Time, bool) {
	for _, d := range dates {
		if d.After(periodEnd) {
			return d, true
		}
	}
	return time.Time{}, false
}

// NextReading is when the reading after latest is due: the first release
// after the next period ends, and not before today. Matching on the period
// rather than just "the next release" matters for a release that comes out
// more often than its series -- H.15 is published every business day, but
// October's average fed funds rate only after October ends.
func NextReading(s Series, dates []time.Time, latest, today time.Time) (time.Time, bool) {
	months := 1
	if s.Quarterly {
		months = 3
	}
	after := PeriodEnd(s, latest.AddDate(0, months, 0))
	if yesterday := today.AddDate(0, 0, -1); yesterday.After(after) {
		after = yesterday
	}
	return FirstReleaseAfter(dates, after)
}

// ---------------------------------------------------------------------------
// FOMC meetings

// Meeting is one FOMC meeting as the Fed's calendar lists it.
type Meeting struct {
	Start, End time.Time
	// Projections: the meeting comes with a Summary of Economic Projections
	// (the "dot plot"), marked with an asterisk on the Fed's page.
	Projections bool
	// Note is the calendar's own annotation: "notation vote", "unscheduled".
	Note string
	// StatementURL and MinutesURL are absolute; empty until published.
	StatementURL    string
	MinutesURL      string
	MinutesReleased time.Time
}

var (
	reYear       = regexp.MustCompile(`<a id="\d+">(\d{4}) FOMC Meetings</a>`)
	reMonth      = regexp.MustCompile(`fomc-meeting__month[^>]*>\s*<strong>([^<]+)</strong>`)
	reDays       = regexp.MustCompile(`fomc-meeting__date[^>]*>([^<]+)<`)
	reDayRange   = regexp.MustCompile(`^(\d{1,2})(?:\s*-\s*(\d{1,2}))?\s*(\*)?\s*(?:\((.*)\))?`)
	reStatement  = regexp.MustCompile(`href="(/newsevents/pressreleases/monetary\d{8}a\.htm)"`)
	reMinutes    = regexp.MustCompile(`href="(/monetarypolicy/fomcminutes\d{8}\.htm)"`)
	reMinutesOut = regexp.MustCompile(`\(Released ([A-Za-z]+ \d{1,2}, \d{4})\)`)
)

// ParseFOMCCalendar reads the meetings off federalreserve.gov's
// fomccalendars.htm, sorted by date. The page lists years newest first with
// next year's tentative schedule at the end; a meeting row carries its month
// ("Apr/May" when it spans two), its days ("30-1", "16-17*", "22 (notation
// vote)") and, once they exist, links to its statement and minutes.
func ParseFOMCCalendar(page []byte) []Meeting {
	text := string(page)
	years := reYear.FindAllStringSubmatchIndex(text, -1)
	var out []Meeting
	for i, y := range years {
		year, _ := strconv.Atoi(text[y[2]:y[3]])
		end := len(text)
		if i+1 < len(years) {
			end = years[i+1][0]
		}
		// A row is cut from one month cell to the next: the row's own div is
		// not marked consistently (the 2025 notation vote's lacks a quote the
		// others carry), but every row starts with its month.
		panel := text[y[1]:end]
		rows := strings.Split(panel, `fomc-meeting__month`)
		for _, row := range rows[1:] {
			if m, ok := parseMeeting(`fomc-meeting__month`+row, year); ok {
				out = append(out, m)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

func parseMeeting(row string, year int) (Meeting, bool) {
	mm := reMonth.FindStringSubmatch(row)
	dm := reDays.FindStringSubmatch(row)
	if mm == nil || dm == nil {
		return Meeting{}, false
	}
	months := strings.Split(strings.TrimSpace(html.UnescapeString(mm[1])), "/")
	startMonth, ok := monthOf(months[0])
	if !ok {
		return Meeting{}, false
	}
	endMonth := startMonth
	if len(months) > 1 {
		if endMonth, ok = monthOf(months[1]); !ok {
			return Meeting{}, false
		}
	}
	days := reDayRange.FindStringSubmatch(strings.TrimSpace(html.UnescapeString(dm[1])))
	if days == nil {
		return Meeting{}, false
	}
	d1, _ := strconv.Atoi(days[1])
	d2 := d1
	if days[2] != "" {
		d2, _ = strconv.Atoi(days[2])
	}
	m := Meeting{
		Start:       time.Date(year, startMonth, d1, 0, 0, 0, 0, time.UTC),
		End:         time.Date(year, endMonth, d2, 0, 0, 0, 0, time.UTC),
		Projections: days[3] == "*",
		Note:        strings.TrimSpace(days[4]),
	}
	if s := reStatement.FindStringSubmatch(row); s != nil {
		m.StatementURL = FedHost + s[1]
	}
	if s := reMinutes.FindStringSubmatch(row); s != nil {
		m.MinutesURL = FedHost + s[1]
	}
	if s := reMinutesOut.FindStringSubmatch(row); s != nil {
		if t, err := time.Parse("January 02, 2006", s[1]); err == nil {
			m.MinutesReleased = t
		} else if t, err := time.Parse("January 2, 2006", s[1]); err == nil {
			m.MinutesReleased = t
		}
	}
	return m, true
}

// monthOf reads "January", "Jan" or "Sept".
func monthOf(s string) (time.Month, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	if len(s) < 3 {
		return 0, false
	}
	for m := time.January; m <= time.December; m++ {
		if strings.HasPrefix(strings.ToLower(m.String()), s[:3]) {
			return m, true
		}
	}
	return 0, false
}

// FOMCCalendarURL is the page the meetings are read from, and the link out.
func FOMCCalendarURL() string { return FedHost + "/monetarypolicy/fomccalendars.htm" }

// Meetings fetches and parses the Fed's meeting calendar.
func (c *Client) Meetings() ([]Meeting, error) {
	u := FOMCCalendarURL()
	body, err := c.get(u, u, "The Federal Reserve", calendarTTL)
	if err != nil {
		return nil, err
	}
	ms := ParseFOMCCalendar(body)
	if len(ms) == 0 {
		return nil, fmt.Errorf("the Federal Reserve's calendar page has changed shape; no meetings were found on it")
	}
	return ms, nil
}
