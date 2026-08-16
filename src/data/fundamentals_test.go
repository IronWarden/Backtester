package data

// The publication lag is the difference between a factor study and a fiction,
// so it is tested directly rather than through a query. Every case here is
// about one question: could a trader standing on this date actually have known
// this number?

import (
	"testing"
	"time"
)

func day(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func stamp(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04", s)
	if err != nil {
		panic(err)
	}
	return t
}

// A real report date beats the fixed lag, and the figure becomes usable the
// day AFTER the report — those timestamps are after the close.
func TestReportDateSetsKnownFromTheFollowingDay(t *testing.T) {
	raw := []RawFundamental{{
		Ticker: "AAPL", Metric: "Net Income",
		PeriodEnd: day("2025-12-31"), Value: 1e9, Frequency: "quarterly",
	}}
	reports := map[string][]time.Time{
		// Deliberately unsorted, and including one before the period end that
		// must not be chosen.
		"AAPL": {stamp("2026-04-30 16:00"), stamp("2025-10-30 16:00"), stamp("2026-01-29 16:00")},
	}

	got := ApplyPublicationLag(raw, reports)
	if len(got) != 1 {
		t.Fatalf("got %d points, want 1", len(got))
	}
	if want := day("2026-01-30"); !got[0].KnownFrom.Equal(want) {
		t.Errorf("KnownFrom = %s, want %s (the day after the 2026-01-29 report)",
			got[0].KnownFrom.Format("2006-01-02"), want.Format("2006-01-02"))
	}
	if got[0].LagSource != LagFromEarningsCalendar {
		t.Errorf("LagSource = %q, want %q", got[0].LagSource, LagFromEarningsCalendar)
	}
	// The period end itself must never be the usable date.
	if !got[0].KnownFrom.After(got[0].PeriodEnd) {
		t.Error("KnownFrom is not after PeriodEnd; this is the look-ahead bug")
	}
}

// A ticker with no report dates falls back to the fixed lag rather than to the
// period end.
func TestMissingReportDateFallsBackToTheFixedLag(t *testing.T) {
	raw := []RawFundamental{{
		Ticker: "OBSCURE", Metric: "Total Revenue",
		PeriodEnd: day("2025-03-31"), Value: 5e6, Frequency: "quarterly",
	}}
	got := ApplyPublicationLag(raw, map[string][]time.Time{})

	want := day("2025-03-31").AddDate(0, 0, FixedReportLagDays)
	if !got[0].KnownFrom.Equal(want) {
		t.Errorf("KnownFrom = %s, want %s",
			got[0].KnownFrom.Format("2006-01-02"), want.Format("2006-01-02"))
	}
	if got[0].LagSource != LagFromFixedWindow {
		t.Errorf("LagSource = %q, want %q", got[0].LagSource, LagFromFixedWindow)
	}
}

// A ticker whose only report dates predate the period gets the fixed lag too —
// a stale calendar must not date a figure earlier than it was published.
func TestReportDatesBeforeThePeriodAreIgnored(t *testing.T) {
	raw := []RawFundamental{{
		Ticker: "STALE", Metric: "Net Income",
		PeriodEnd: day("2025-12-31"), Value: 1, Frequency: "quarterly",
	}}
	reports := map[string][]time.Time{
		"STALE": {stamp("2024-02-01 16:00"), stamp("2025-11-01 16:00")},
	}
	got := ApplyPublicationLag(raw, reports)

	if got[0].LagSource != LagFromFixedWindow {
		t.Errorf("LagSource = %q; a report before the period end cannot have "+
			"announced it", got[0].LagSource)
	}
}

// The raw table contains exact duplicates. They must collapse, or any
// per-ticker aggregate is silently double-counted.
func TestDuplicateRowsCollapse(t *testing.T) {
	row := RawFundamental{
		Ticker: "AAPL", Metric: "Ordinary Shares Number",
		PeriodEnd: day("2025-09-30"), Value: 14773260000, Frequency: "quarterly",
	}
	got := ApplyPublicationLag([]RawFundamental{row, row, row}, nil)
	if len(got) != 1 {
		t.Errorf("got %d points from three identical rows, want 1", len(got))
	}

	// Different metrics or periods are not duplicates.
	other := row
	other.Metric = "Total Assets"
	later := row
	later.PeriodEnd = day("2025-12-31")
	got = ApplyPublicationLag([]RawFundamental{row, other, later}, nil)
	if len(got) != 3 {
		t.Errorf("got %d points, want 3 distinct", len(got))
	}
}

// The core guarantee: nothing is visible before it was published. Checked on
// the boundary, which is where an off-by-one would hide.
func TestNothingIsVisibleBeforeItWasPublished(t *testing.T) {
	raw := []RawFundamental{{
		Ticker: "AAPL", Metric: "Net Income",
		PeriodEnd: day("2025-12-31"), Value: 1e9, Frequency: "quarterly",
	}}
	reports := map[string][]time.Time{"AAPL": {stamp("2026-01-29 16:00")}}
	points := ApplyPublicationLag(raw, reports)

	cases := []struct {
		on      string
		visible bool
		why     string
	}{
		{"2025-12-31", false, "the period end itself — the naive join"},
		{"2026-01-29", false, "the report day; it landed after the close"},
		{"2026-01-30", true, "the first tradeable session after the report"},
		{"2026-06-01", true, "long after"},
	}
	for _, tc := range cases {
		got := len(KnownAt(points, day(tc.on))) == 1
		if got != tc.visible {
			t.Errorf("on %s visible=%v, want %v (%s)",
				tc.on, got, tc.visible, tc.why)
		}
	}
}

// LatestKnownAt is what a cross-sectional factor reads: the newest figure a
// trader could have had that morning, not the newest that exists.
func TestLatestKnownAtPicksTheNewestPublishedPeriod(t *testing.T) {
	mk := func(period string, v float64) RawFundamental {
		return RawFundamental{
			Ticker: "AAPL", Metric: "Net Income",
			PeriodEnd: day(period), Value: v, Frequency: "quarterly",
		}
	}
	raw := []RawFundamental{
		mk("2025-06-30", 100),
		mk("2025-09-30", 200),
		mk("2025-12-31", 300),
	}
	reports := map[string][]time.Time{"AAPL": {
		stamp("2025-07-31 16:00"), stamp("2025-10-30 16:00"), stamp("2026-01-29 16:00"),
	}}
	points := ApplyPublicationLag(raw, reports)

	for _, tc := range []struct {
		on   string
		want float64
	}{
		{"2025-08-01", 100}, // only Q2 is out
		{"2025-11-01", 200}, // Q3 is out, Q4 does not exist yet
		{"2026-01-29", 200}, // Q4 reported after today's close
		{"2026-01-30", 300}, // now Q4 is usable
	} {
		latest := LatestKnownAt(points, day(tc.on))
		got, ok := latest["AAPL"]["Net Income"]
		if !ok {
			t.Errorf("on %s: nothing known, want %v", tc.on, tc.want)
			continue
		}
		if got.Value != tc.want {
			t.Errorf("on %s: got %v (period %s), want %v",
				tc.on, got.Value, got.PeriodEnd.Format("2006-01-02"), tc.want)
		}
	}

	// Before anything was published, the map is empty rather than guessing.
	if len(LatestKnownAt(points, day("2025-07-01"))) != 0 {
		t.Error("returned a value before any period had been reported")
	}
}

// The fixed lag must be long enough to clear real filing deadlines. This pins
// the constant so shortening it is a deliberate, reviewed act rather than a
// tuning knob someone reaches for when a factor looks weak.
func TestFixedLagClearsFilingDeadlines(t *testing.T) {
	if FixedReportLagDays < 60 {
		t.Errorf("FixedReportLagDays = %d; annual filers have up to 90 days, "+
			"so anything under 60 dates figures before they could be public",
			FixedReportLagDays)
	}
}
