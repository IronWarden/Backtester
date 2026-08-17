package data

import (
	"testing"
	"time"
)

// `day` is defined in fundamentals_test.go.

// One company's Q4-2023 net income as it really appears in the data: reported in
// the 10-K filed 2024-02-20, then restated as a comparative inside the 2025
// filing. A backtest reading the 2025 value on 2024-02-20 is using a number
// that did not exist yet.
func revenueHistory() []SECFundamental {
	return []SECFundamental{
		{CIK: "0000078003", Metric: "Net Income", PeriodEnd: day("2023-12-31"),
			Filed: day("2024-02-20"), FilingPeriod: day("2023-12-31"),
			Value: 100, Form: "10-K"},
		{CIK: "0000078003", Metric: "Net Income", PeriodEnd: day("2023-12-31"),
			Filed: day("2025-02-18"), FilingPeriod: day("2024-12-31"),
			Value: 95, Form: "10-K"}, // restated comparative
		{CIK: "0000078003", Metric: "Net Income", PeriodEnd: day("2024-12-31"),
			Filed: day("2025-02-18"), FilingPeriod: day("2024-12-31"),
			Value: 120, Form: "10-K"},
	}
}

func TestFirstKnownKeepsTheOriginalFiling(t *testing.T) {
	got := FirstKnown(revenueHistory())
	if len(got) != 2 {
		t.Fatalf("reduced to %d figures, want 2 (two periods)", len(got))
	}

	k2023 := FundamentalKey{CIK: "0000078003", Metric: "Net Income",
		PeriodEnd: day("2023-12-31")}
	f, ok := got[k2023]
	if !ok {
		t.Fatal("2023 figure missing")
	}
	// The original 100, not the restated 95: acting on a restatement at the
	// original filing's date is look-ahead bias.
	if f.Value != 100 {
		t.Errorf("2023 value = %v, want 100 (the originally filed figure)", f.Value)
	}
	if !f.Filed.Equal(day("2024-02-20")) {
		t.Errorf("2023 filed = %v, want 2024-02-20", f.Filed)
	}
}

// Order must not decide the answer: the same rows shuffled produce the same
// earliest filing. The loader inserts in quarter order today, and a future
// change to that must not silently flip which figure wins.
func TestFirstKnownIsOrderIndependent(t *testing.T) {
	rows := revenueHistory()
	reversed := []SECFundamental{rows[2], rows[1], rows[0]}

	a := FirstKnown(rows)
	b := FirstKnown(reversed)
	if len(a) != len(b) {
		t.Fatalf("different sizes: %d vs %d", len(a), len(b))
	}
	for key, want := range a {
		got, ok := b[key]
		if !ok {
			t.Errorf("%v missing from the reversed pass", key)
			continue
		}
		if got.Value != want.Value || !got.Filed.Equal(want.Filed) {
			t.Errorf("%v: reversed gave value %v filed %v, want %v / %v",
				key, got.Value, got.Filed, want.Value, want.Filed)
		}
	}
}

func TestKnownOn(t *testing.T) {
	rows := revenueHistory()

	// The day before the 10-K was filed, nothing is public.
	if got := KnownOn(rows, day("2024-02-19")); len(got) != 0 {
		t.Errorf("on 2024-02-19: %d figures known, want 0", len(got))
	}
	// The day it was filed, the 2023 figure is public and the 2024 one is not.
	got := KnownOn(rows, day("2024-02-20"))
	if len(got) != 1 {
		t.Fatalf("on 2024-02-20: %d figures known, want 1", len(got))
	}
	for key, f := range got {
		if !key.PeriodEnd.Equal(day("2023-12-31")) || f.Value != 100 {
			t.Errorf("on 2024-02-20 got period %v value %v, want 2023-12-31 / 100",
				key.PeriodEnd, f.Value)
		}
	}
	// After the next 10-K, both periods are public — and the 2023 figure is
	// still the originally filed 100, because that is what was knowable.
	got = KnownOn(rows, day("2025-06-01"))
	if len(got) != 2 {
		t.Fatalf("on 2025-06-01: %d figures known, want 2", len(got))
	}
}

func TestIsPrimaryAndPublicationLag(t *testing.T) {
	rows := revenueHistory()

	if !rows[0].IsPrimary() {
		t.Error("the figure a filing reports must be primary")
	}
	if rows[1].IsPrimary() {
		t.Error("a restated comparative must not be primary")
	}
	// 2023-12-31 to 2024-02-20 is 51 days, comfortably inside the 31-62 day
	// range the 2026Q1 sample showed — and well under the 90-day fallback
	// PointInTimeFundamentals uses without an earnings_calendar entry.
	if lag := rows[0].PublicationLag(); lag != 51*24*time.Hour {
		t.Errorf("publication lag = %v, want 51 days", lag)
	}

	// A row with no filing period cannot be called primary: unknown is not yes.
	noPeriod := SECFundamental{PeriodEnd: day("2023-12-31"),
		Filed: day("2024-02-20")}
	if noPeriod.IsPrimary() {
		t.Error("a row with no filing period must not read as primary")
	}
}

func TestFirstKnownSeparatesCompaniesAndMetrics(t *testing.T) {
	rows := []SECFundamental{
		{CIK: "1", Metric: "Net Income", PeriodEnd: day("2023-12-31"),
			Filed: day("2024-02-01"), Value: 10},
		{CIK: "2", Metric: "Net Income", PeriodEnd: day("2023-12-31"),
			Filed: day("2024-02-01"), Value: 20},
		{CIK: "1", Metric: "Total Revenue", PeriodEnd: day("2023-12-31"),
			Filed: day("2024-02-01"), Value: 30},
	}
	got := FirstKnown(rows)
	if len(got) != 3 {
		t.Fatalf("collapsed %d rows into %d figures, want 3 — the key must "+
			"separate companies and metrics", len(rows), len(got))
	}
}
