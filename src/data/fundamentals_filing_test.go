package data

// The SEC filing date as a publication-date rule, on top of the two in
// fundamentals_test.go. Same question as that file asks: could a trader standing
// on this date actually have known this number?
//
// Half of these are pure and half go through a throwaway DuckDB, on purpose. The
// join is `financials.date` (TIMESTAMP_NS) against `sec_financials.period_end`
// (DATE), and a type or timezone mismatch there would silently miss every row —
// falling back to the old rules, which is the safe direction and therefore
// invisible in any test that only checks the numbers are sane.

import (
	"path/filepath"
	"testing"
	"time"
)

// `day` and `stamp` are defined in fundamentals_test.go.

func filingRow(ticker, metric, periodEnd string, value float64) RawFundamental {
	return RawFundamental{
		Ticker: ticker, Metric: metric, PeriodEnd: day(periodEnd),
		Value: value, Frequency: "quarterly",
	}
}

// The whole point of T44: a real filing date beats the 90-day fallback, and by
// a lot. Q4-2023 filed on 2024-02-20 is knowable from the 21st, not from
// 2024-03-30.
func TestSECFilingDateBeatsTheFixedLag(t *testing.T) {
	rows := []RawFundamental{filingRow("AAA", "Net Income", "2023-12-31", 100)}
	filed := FilingDates{
		{Ticker: "AAA", PeriodEnd: day("2023-12-31")}: day("2024-02-20"),
	}

	got := ApplyPublicationLagWithFilings(rows, nil, filed)
	if len(got) != 1 {
		t.Fatalf("got %d points, want 1", len(got))
	}
	if want := day("2024-02-21"); !got[0].KnownFrom.Equal(want) {
		t.Errorf("KnownFrom = %v, want %v (the session after the filing)",
			got[0].KnownFrom.Format("2006-01-02"), want.Format("2006-01-02"))
	}
	if got[0].LagSource != LagFromSECFiling {
		t.Errorf("LagSource = %q, want %q", got[0].LagSource, LagFromSECFiling)
	}

	// Without the filing date, the same row lands 90 days after period end —
	// 38 days later than it really became public.
	plain := ApplyPublicationLag(rows, nil)
	if want := day("2024-03-30"); !plain[0].KnownFrom.Equal(want) {
		t.Errorf("fallback KnownFrom = %v, want %v",
			plain[0].KnownFrom.Format("2006-01-02"), want.Format("2006-01-02"))
	}
}

// Where the two evidence-based rules disagree, the filing date wins. The
// reasoning is in the file header: a company announces headline results before it
// files, but the balance-sheet metrics here are often not in the press release,
// and this file's principle is to be late rather than early.
func TestSECFilingDateBeatsTheEarningsCalendar(t *testing.T) {
	rows := []RawFundamental{filingRow("AAA", "Total Assets", "2023-12-31", 500)}
	reports := map[string][]time.Time{
		"AAA": {stamp("2024-01-29 16:00")}, // announced three weeks earlier
	}
	filed := FilingDates{
		{Ticker: "AAA", PeriodEnd: day("2023-12-31")}: day("2024-02-20"),
	}

	got := ApplyPublicationLagWithFilings(rows, reports, filed)
	if got[0].LagSource != LagFromSECFiling {
		t.Fatalf("LagSource = %q, want %q", got[0].LagSource, LagFromSECFiling)
	}
	if want := day("2024-02-21"); !got[0].KnownFrom.Equal(want) {
		t.Errorf("KnownFrom = %v, want %v",
			got[0].KnownFrom.Format("2006-01-02"), want.Format("2006-01-02"))
	}

	// And with no filing date, the calendar rule still applies unchanged.
	without := ApplyPublicationLagWithFilings(rows, reports, nil)
	if without[0].LagSource != LagFromEarningsCalendar {
		t.Errorf("without filings, LagSource = %q, want %q",
			without[0].LagSource, LagFromEarningsCalendar)
	}
	if want := day("2024-01-30"); !without[0].KnownFrom.Equal(want) {
		t.Errorf("without filings, KnownFrom = %v, want %v",
			without[0].KnownFrom.Format("2006-01-02"), want.Format("2006-01-02"))
	}
}

// The map is keyed on the period, not on (period, metric), so a metric that
// `sec_financials` does not carry — and there are many, the vocabulary there is
// nine tags wide against dozens in `financials` — still gets the filing date of
// the filing that carried its period.
func TestFilingDateAppliesToEveryMetricOfThePeriod(t *testing.T) {
	rows := []RawFundamental{
		filingRow("AAA", "Net Income", "2023-12-31", 100),
		filingRow("AAA", "Tax Effect Of Unusual Items", "2023-12-31", 7),
	}
	filed := FilingDates{
		{Ticker: "AAA", PeriodEnd: day("2023-12-31")}: day("2024-02-20"),
	}

	for _, p := range ApplyPublicationLagWithFilings(rows, nil, filed) {
		if p.LagSource != LagFromSECFiling {
			t.Errorf("%s: LagSource = %q, want %q",
				p.Metric, p.LagSource, LagFromSECFiling)
		}
		if want := day("2024-02-21"); !p.KnownFrom.Equal(want) {
			t.Errorf("%s: KnownFrom = %v, want %v", p.Metric,
				p.KnownFrom.Format("2006-01-02"), want.Format("2006-01-02"))
		}
	}
}

// A filing date at or before the period end cannot be right — a figure is not
// published before the period it describes has ended — so it is ignored rather
// than trusted. The loader's MIN(filed) makes this unlikely; a bad row must not
// be able to date a figure early.
func TestFilingDateBeforeThePeriodIsIgnored(t *testing.T) {
	rows := []RawFundamental{filingRow("AAA", "Net Income", "2023-12-31", 100)}
	for _, bad := range []string{"2023-12-31", "2023-06-30"} {
		filed := FilingDates{
			{Ticker: "AAA", PeriodEnd: day("2023-12-31")}: day(bad),
		}
		got := ApplyPublicationLagWithFilings(rows, nil, filed)
		if got[0].LagSource != LagFromFixedWindow {
			t.Errorf("filed %s: LagSource = %q, want the fixed fallback",
				bad, got[0].LagSource)
		}
	}
}

// A filing date for a different ticker or a different period must not leak
// across. Both are one map-key mistake away and both would date a figure by
// someone else's filing.
func TestFilingDatesDoNotLeakAcrossKeys(t *testing.T) {
	rows := []RawFundamental{filingRow("AAA", "Net Income", "2023-12-31", 100)}
	filed := FilingDates{
		{Ticker: "BBB", PeriodEnd: day("2023-12-31")}: day("2024-01-15"),
		{Ticker: "AAA", PeriodEnd: day("2023-09-30")}: day("2023-10-20"),
	}
	got := ApplyPublicationLagWithFilings(rows, nil, filed)
	if got[0].LagSource != LagFromFixedWindow {
		t.Errorf("LagSource = %q, want the fixed fallback — a filing date "+
			"leaked from another ticker or period", got[0].LagSource)
	}
}

// End to end through a real DuckDB, which is the only way to catch the join
// actually failing: `financials.date` is a TIMESTAMP_NS and
// `sec_financials.period_end` is a DATE, and a mismatch there silently drops
// every filing date and falls back to the old rules.
func TestPointInTimeFundamentalsUsesTheFilingDate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pit.db")
	if _, err := InitDB(path); err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec := func(sql string) {
		t.Helper()
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	mustExec(`CREATE TABLE financials (
	            metric VARCHAR, date TIMESTAMP_NS, value DOUBLE,
	            ticker VARCHAR, frequency VARCHAR)`)
	mustExec(`INSERT INTO financials VALUES
	            ('Net Income', TIMESTAMP '2023-12-31 00:00:00', 100.0,
	             'AAA', 'quarterly'),
	            ('Net Income', TIMESTAMP '2023-12-31 00:00:00', 100.0,
	             'BBB', 'quarterly')`)

	// AAA has a filing date; BBB has none, so it must keep the old behaviour
	// inside the same call.
	mustExec(`CREATE TABLE sec_financials (
	            cik VARCHAR, ticker VARCHAR, company_name VARCHAR, sic VARCHAR,
	            metric VARCHAR, tag VARCHAR, period_end DATE, filed DATE,
	            filing_period DATE, value DOUBLE, form VARCHAR, fy VARCHAR,
	            fp VARCHAR, frequency VARCHAR, accession VARCHAR)`)
	mustExec(`INSERT INTO sec_financials VALUES
	            ('1', 'AAA', 'A Co', '3571', 'Net Income', 'NetIncomeLoss',
	             DATE '2023-12-31', DATE '2024-02-20', DATE '2023-12-31',
	             100.0, '10-K', '2023', 'FY', 'quarterly', 'x-1'),
	            -- the same period restated a year later: MIN(filed) must win,
	            -- or a backtest reads a number that did not exist yet
	            ('1', 'AAA', 'A Co', '3571', 'Net Income', 'NetIncomeLoss',
	             DATE '2023-12-31', DATE '2025-02-18', DATE '2024-12-31',
	             95.0, '10-K', '2024', 'FY', 'quarterly', 'x-2')`)

	points := PointInTimeFundamentals(
		[]string{"AAA", "BBB"}, []string{"Net Income"},
		day("2023-01-01"), day("2026-01-01"))
	if len(points) != 2 {
		t.Fatalf("got %d points, want 2: %+v", len(points), points)
	}

	bySymbol := map[string]FundamentalPoint{}
	for _, p := range points {
		bySymbol[p.Ticker] = p
	}

	aaa := bySymbol["AAA"]
	if aaa.LagSource != LagFromSECFiling {
		t.Errorf("AAA LagSource = %q, want %q — the DATE/TIMESTAMP join missed",
			aaa.LagSource, LagFromSECFiling)
	}
	if want := day("2024-02-21"); !aaa.KnownFrom.Equal(want) {
		t.Errorf("AAA KnownFrom = %v, want %v (earliest filing + 1, not the "+
			"2025 restatement)", aaa.KnownFrom.Format("2006-01-02"),
			want.Format("2006-01-02"))
	}

	bbb := bySymbol["BBB"]
	if bbb.LagSource != LagFromFixedWindow {
		t.Errorf("BBB LagSource = %q, want the fixed fallback", bbb.LagSource)
	}
	if want := day("2024-03-30"); !bbb.KnownFrom.Equal(want) {
		t.Errorf("BBB KnownFrom = %v, want %v",
			bbb.KnownFrom.Format("2006-01-02"), want.Format("2006-01-02"))
	}
}

// The regression guard for every existing database: with no `sec_financials`
// table, the result must be exactly what it was before this shipped.
func TestPointInTimeFundamentalsWithoutTheSECTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pit-nosec.db")
	if _, err := InitDB(path); err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE financials (
	                        metric VARCHAR, date TIMESTAMP_NS, value DOUBLE,
	                        ticker VARCHAR, frequency VARCHAR)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO financials VALUES
	                        ('Net Income', TIMESTAMP '2023-12-31 00:00:00',
	                         100.0, 'AAA', 'quarterly')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	points := PointInTimeFundamentals(
		[]string{"AAA"}, []string{"Net Income"},
		day("2023-01-01"), day("2026-01-01"))
	if len(points) != 1 {
		t.Fatalf("got %d points, want 1", len(points))
	}
	if points[0].LagSource != LagFromFixedWindow {
		t.Errorf("LagSource = %q, want %q with no sec_financials table",
			points[0].LagSource, LagFromFixedWindow)
	}
	if want := day("2024-03-30"); !points[0].KnownFrom.Equal(want) {
		t.Errorf("KnownFrom = %v, want %v — the pre-existing behaviour must "+
			"be untouched", points[0].KnownFrom.Format("2006-01-02"),
			want.Format("2006-01-02"))
	}
}
