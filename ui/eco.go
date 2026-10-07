package main

// The Research tab's ECO view (T142c): the economy's latest official
// readings, when each was published and when the next comes out, and the
// FOMC's meetings. See src/econ for the sources and what each needs.
//
// One source failing never blanks the view: a series FRED does not answer for
// falls back to the database's economic_indicators copy, saying so, and a
// missing calendar is a note beside readings that still show.

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"my-backtester/src/data"
	"my-backtester/src/econ"
)

var (
	econOnce   sync.Once
	econClient *econ.Client
)

// One client for the process, so its cache outlives a view.
func ecoClient() *econ.Client {
	econOnce.Do(func() { econClient = econ.New() })
	return econClient
}

// fredKey is FRED_API_KEY from the environment or a .env the app already
// reads -- the key update_treasury_yields.py uses. Optional.
func fredKey() string {
	if k := strings.TrimSpace(os.Getenv("FRED_API_KEY")); k != "" {
		return k
	}
	for _, p := range claudeKeyFiles() {
		if k := strings.TrimSpace(parseEnvFile(p)["FRED_API_KEY"]); k != "" {
			return k
		}
	}
	return ""
}

type EconReading struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Units string `json:"units"`
	// Kind is index | rate | count: how the change reads (percent, points,
	// or the series' own units).
	Kind      string  `json:"kind"`
	Period    string  `json:"period"` // "Aug 2026", "Q2 2026"
	PeriodEnd string  `json:"periodEnd"`
	Value     float64 `json:"value"`
	// The previous observation, which skips a period FRED left blank, so it
	// is named.
	Prior         float64 `json:"prior"`
	PriorPeriod   string  `json:"priorPeriod"`
	HasPrior      bool    `json:"hasPrior"`
	Change        float64 `json:"change"`
	YearChange    float64 `json:"yearChange"`
	HasYearChange bool    `json:"hasYearChange"`
	// Published is when this reading first came out: the release calendar's
	// date when PublishedExact, otherwise an estimate from the usual timing.
	Published      string `json:"published"`
	PublishedExact bool   `json:"publishedExact"`
	// NextRelease is "" without a FRED API key.
	NextRelease string `json:"nextRelease"`
	ReleaseName string `json:"releaseName"`
	// Source is "FRED", or "database" when FRED could not be reached.
	Source string `json:"source"`
	URL    string `json:"url"`
}

type FOMCMeeting struct {
	Dates       string `json:"dates"` // "Oct 27–28, 2026"
	Start       string `json:"start"`
	End         string `json:"end"`
	Projections bool   `json:"projections"`
	Note        string `json:"note"`
	// Upcoming includes a meeting in progress.
	Upcoming        bool   `json:"upcoming"`
	StatementURL    string `json:"statementUrl"`
	MinutesURL      string `json:"minutesUrl"`
	MinutesReleased string `json:"minutesReleased"`
}

type EconomicCalendar struct {
	AsOf          string        `json:"asOf"`
	Readings      []EconReading `json:"readings"`
	Meetings      []FOMCMeeting `json:"meetings"`
	HasReleaseKey bool          `json:"hasReleaseKey"`
	CalendarURL   string        `json:"calendarUrl"`
	Notes         []string      `json:"notes"`
	Error         string        `json:"error"`
}

// How many meetings ECO lists either side of today.
const (
	ecoPastMeetings     = 3
	ecoUpcomingMeetings = 4
)

// EconomicReleases is ECO. dbPath is only read when FRED cannot be reached,
// and may be empty.
func (a *App) EconomicReleases(dbPath string) EconomicCalendar {
	fromDB := func(id string) ([]econ.Obs, bool) {
		if dbPath == "" {
			return nil, false
		}
		if _, err := data.InitDB(dbPath); err != nil {
			return nil, false
		}
		pts, err := data.LoadIndicator(id)
		if err != nil || len(pts) == 0 {
			return nil, false
		}
		obs := make([]econ.Obs, len(pts))
		for i, p := range pts {
			obs[i] = econ.Obs{Date: p.Date, Value: p.Value}
		}
		return obs, true
	}
	return ecoView(ecoClient(), fredKey(), fromDB, time.Now())
}

func ecoView(c *econ.Client, key string, fromDB func(string) ([]econ.Obs, bool), now time.Time) EconomicCalendar {
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	out := EconomicCalendar{AsOf: fmtDay(today), Readings: []EconReading{}, Meetings: []FOMCMeeting{},
		HasReleaseKey: key != "", CalendarURL: econ.FOMCCalendarURL(), Notes: []string{}}

	// Every request at once: eight series, the meeting calendar, and with a
	// key each release's dates.
	type fetched struct {
		obs []econ.Obs
		err error
	}
	series := make([]fetched, len(econ.Catalog))
	releases := map[int][]time.Time{}
	var releaseErr error
	var meetings []econ.Meeting
	var meetingsErr error
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i, s := range econ.Catalog {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			obs, err := c.Observations(id, now)
			series[i] = fetched{obs, err}
		}(i, s.ID)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		meetings, meetingsErr = c.Meetings()
	}()
	if key != "" {
		seen := map[int]bool{}
		for _, s := range econ.Catalog {
			if seen[s.ReleaseID] {
				continue
			}
			seen[s.ReleaseID] = true
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				dates, err := c.ReleaseDates(id, key, now)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					releaseErr = err
					return
				}
				releases[id] = dates
			}(s.ReleaseID)
		}
	}
	wg.Wait()

	for i, s := range econ.Catalog {
		obs, source := series[i].obs, "FRED"
		if err := series[i].err; err != nil || len(obs) == 0 {
			db, ok := fromDB(s.ID)
			if !ok {
				reason := "it returned no observations"
				if err != nil {
					reason = err.Error()
				}
				out.Notes = append(out.Notes, fmt.Sprintf("%s is missing: %s, and the database has no copy.",
					s.Name, reason))
				continue
			}
			obs, source = db, "database"
		}
		r, ok := econ.Summarize(s, obs)
		if !ok {
			continue
		}
		row := EconReading{
			ID: s.ID, Name: s.Name, Units: s.Units, Kind: string(s.Kind),
			Period: econ.PeriodLabel(s, r.Period), PeriodEnd: fmtDay(r.PeriodEnd),
			Value: r.Value, Prior: r.Prior, HasPrior: r.HasPrior,
			Change: r.Change, YearChange: r.YearChange, HasYearChange: r.HasYearChange,
			ReleaseName: s.ReleaseName, Source: source, URL: econ.SeriesURL(s.ID),
		}
		if r.HasPrior {
			row.PriorPeriod = econ.PeriodLabel(s, r.PriorPeriod)
		}
		dates := releases[s.ReleaseID]
		if d, ok := econ.FirstReleaseAfter(dates, r.PeriodEnd); ok && !d.After(today) {
			row.Published, row.PublishedExact = fmtDay(d), true
		} else {
			row.Published = fmtDay(econ.EstimatedRelease(s, r.Period))
		}
		if d, ok := econ.NextReading(s, dates, r.Period, today); ok {
			row.NextRelease = fmtDay(d)
		}
		if source == "database" {
			out.Notes = append(out.Notes, fmt.Sprintf(
				"FRED could not be reached for %s; showing the database's copy, which ends at %s.",
				s.Name, row.Period))
		}
		out.Readings = append(out.Readings, row)
	}

	switch {
	case key == "":
		out.Notes = append(out.Notes, "Release dates come from FRED's release calendar, which needs a free "+
			"FRED API key (FRED_API_KEY in .env). Without one, when each reading came out is an estimate "+
			"from its release's usual timing, and the next release dates are not shown.")
	case releaseErr != nil:
		out.Notes = append(out.Notes, "FRED's release calendar could not be read ("+releaseErr.Error()+
			"); publication dates are estimates.")
	}

	if meetingsErr != nil {
		out.Notes = append(out.Notes, "FOMC meetings could not be read: "+meetingsErr.Error()+".")
	} else {
		out.Meetings = meetingRows(meetings, today)
	}
	return out
}

// meetingRows keeps the last few meetings and the next few, oldest first.
func meetingRows(ms []econ.Meeting, today time.Time) []FOMCMeeting {
	var past, next []econ.Meeting
	for _, m := range ms {
		if m.End.Before(today) {
			past = append(past, m)
		} else if len(next) < ecoUpcomingMeetings {
			next = append(next, m)
		}
	}
	if len(past) > ecoPastMeetings {
		past = past[len(past)-ecoPastMeetings:]
	}
	out := []FOMCMeeting{}
	for _, m := range append(past, next...) {
		row := FOMCMeeting{
			Dates: meetingDates(m), Start: fmtDay(m.Start), End: fmtDay(m.End),
			Projections: m.Projections, Note: m.Note, Upcoming: !m.End.Before(today),
			StatementURL: m.StatementURL, MinutesURL: m.MinutesURL, MinutesReleased: fmtDay(m.MinutesReleased),
		}
		out = append(out, row)
	}
	return out
}

// meetingDates: "Oct 27–28, 2026", "Apr 30 – May 1, 2024", "Aug 22, 2025".
func meetingDates(m econ.Meeting) string {
	switch {
	case m.Start.Equal(m.End):
		return m.Start.Format("Jan 2, 2006")
	case m.Start.Month() == m.End.Month():
		return fmt.Sprintf("%s–%d, %d", m.Start.Format("Jan 2"), m.End.Day(), m.End.Year())
	default:
		return fmt.Sprintf("%s – %s", m.Start.Format("Jan 2"), m.End.Format("Jan 2, 2006"))
	}
}
