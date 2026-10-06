package data

import (
	"math"
	"testing"
	"time"
)

func d(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestSummarizePricesUsesTheLastYearOfCloses(t *testing.T) {
	var bars []CloseBar
	start := d("2020-01-01")
	for i := 0; i < 300; i++ {
		// A spike early on, outside the last 252 sessions, must not count.
		c := 100.0 + float64(i%10)
		if i == 5 {
			c = 999
		}
		bars = append(bars, CloseBar{Date: start.AddDate(0, 0, i), Close: c})
	}
	s, ok := SummarizePrices(bars)
	if !ok || s.Bars != 300 || s.LastClose != bars[299].Close {
		t.Fatalf("%+v", s)
	}
	if s.High52 != 109 || s.Low52 != 100 {
		t.Errorf("52-week range from closes: got %v..%v, want 100..109", s.Low52, s.High52)
	}
	if _, ok := SummarizePrices(nil); ok {
		t.Error("an empty series has no summary")
	}
}

func TestFinancialsTableShowsFirstFiledFigures(t *testing.T) {
	row := func(cik, metric, period, filed, freq string, v float64) SECFundamental {
		return SECFundamental{CIK: cik, CompanyName: "Acme " + cik, Metric: metric,
			PeriodEnd: d(period), Filed: d(filed), Frequency: freq, Value: v, Form: "10-K"}
	}
	rows := []SECFundamental{
		row("1", "Net Income", "2022-12-31", "2023-02-15", "annual", 10),
		// The same figure restated a year later: the table shows the original.
		row("1", "Net Income", "2022-12-31", "2024-02-15", "annual", 12),
		row("1", "Net Income", "2023-12-31", "2024-02-15", "annual", 20),
		row("1", "Total Assets", "2023-12-31", "2024-02-15", "instant", 500),
		// A quarterly figure must not leak into the annual view.
		row("1", "Total Revenue", "2023-09-30", "2023-11-01", "quarterly", 7),
		// Another company under the same ticker, filing earlier.
		row("2", "Net Income", "2023-12-31", "2023-03-01", "annual", 99),
	}
	v := FinancialsTable(rows, "annual", 5, "")
	if v.CIK != "1" || v.Company != "Acme 1" {
		t.Fatalf("picked CIK %q (%q); want the one with the newest filing", v.CIK, v.Company)
	}
	if len(v.Periods) != 2 || !v.Periods[0].Equal(d("2023-12-31")) {
		t.Fatalf("periods newest first: %v", v.Periods)
	}
	got := map[string][]*FinancialsCell{}
	for _, r := range v.Rows {
		got[r.Metric] = r.Cells
	}
	if c := got["Net Income"][1]; c == nil || c.Value != 10 || !c.Filed.Equal(d("2023-02-15")) {
		t.Errorf("2022 net income should be the first-filed 10, got %+v", c)
	}
	if c := got["Total Assets"]; c == nil || c[0] == nil || c[0].Value != 500 || c[1] != nil {
		t.Errorf("balance-sheet figure fills its own period only: %+v", c)
	}
	if _, ok := got["Total Revenue"]; ok {
		t.Error("a quarterly-only metric appeared in the annual view")
	}
	if v := FinancialsTable(rows, "annual", 5, "2"); v.CIK != "2" {
		t.Errorf("a preferred CIK that is present wins: %q", v.CIK)
	}
	// company_profile pads CIKs to ten digits; sec_financials does not.
	if v := FinancialsTable(rows, "annual", 5, "0000000002"); v.CIK != "2" {
		t.Errorf("a zero-padded preferred CIK must still match: %q", v.CIK)
	}
	if v := FinancialsTable(rows, "annual", 1, ""); len(v.Periods) != 1 {
		t.Errorf("periods cap: %v", v.Periods)
	}
}

func TestEarningsReactionsMeasureAcrossTheNews(t *testing.T) {
	bars := []CloseBar{
		{d("2025-01-27"), 100}, // Mon
		{d("2025-01-28"), 110}, // Tue
		{d("2025-01-29"), 121}, // Wed
		{d("2025-01-30"), 133.1},
	}
	at := func(day string, h, m int) time.Time {
		x := d(day)
		return time.Date(x.Year(), x.Month(), x.Day(), h, m, 0, 0, time.UTC)
	}
	now := at("2025-06-01", 0, 0)
	got := EarningsReactions([]time.Time{
		at("2025-01-28", 16, 0),  // after Tuesday's close: Tue -> Wed
		at("2025-01-28", 7, 0),   // before Tuesday's open: Mon -> Tue
		at("2025-01-30", 16, 30), // after the last bar: no answer yet
		at("2025-09-01", 16, 0),  // in the future
	}, bars, now)
	if len(got) != 4 || !got[0].Upcoming {
		t.Fatalf("newest first, future marked upcoming: %+v", got)
	}
	byTime := map[string]EarningsReaction{}
	for _, r := range got {
		byTime[r.Reported.Format("01-02 15:04")] = r
	}
	after := byTime["01-28 16:00"]
	if after.Timing != "after close" || !after.FromDate.Equal(d("2025-01-28")) ||
		!after.ToDate.Equal(d("2025-01-29")) || math.Abs(after.Move-0.10) > 1e-9 {
		t.Errorf("after close: %+v", after)
	}
	before := byTime["01-28 07:00"]
	if before.Timing != "before open" || !before.FromDate.Equal(d("2025-01-27")) ||
		math.Abs(before.Move-0.10) > 1e-9 {
		t.Errorf("before open: %+v", before)
	}
	if late := byTime["01-30 16:30"]; !late.ToDate.IsZero() || late.Move != 0 {
		t.Errorf("no bar after the report means no move: %+v", late)
	}
}
