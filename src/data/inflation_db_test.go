package data

import (
	"path/filepath"
	"testing"
	"time"
)

// GetCPISeries against a throwaway DuckDB carrying the real column shape:
// economic_indicators(Date, series_id, indicator_name, value), with Date a
// DATE and value a DOUBLE. The SQL is the part that cannot be tested by pure
// unit tests, and it is exactly the part that failed silently the first time —
// a nil return is indistinguishable from "no table" to every caller, so a
// wrong query means the metric is quietly never reported.
func TestGetCPISeriesReadsTheRealColumnShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpi.db")
	if _, err := InitDB(path); err != nil {
		t.Fatalf("open: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE economic_indicators (
		Date DATE, series_id VARCHAR, indicator_name VARCHAR, value DOUBLE)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Two series, so the series_id filter has something to exclude.
	for i := 0; i < 24; i++ {
		d := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, i, 0)
		if _, err := db.Exec(
			`INSERT INTO economic_indicators VALUES (?, 'CPIAUCSL', 'CPI', ?)`,
			d.Format("2006-01-02"), 100+float64(i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if _, err := db.Exec(
			`INSERT INTO economic_indicators VALUES (?, 'UNRATE', 'Unemployment', ?)`,
			d.Format("2006-01-02"), 5.0); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	got := GetCPISeries()
	if len(got) == 0 {
		t.Fatal("no CPI points read — the query does not match the table's " +
			"real column shape, and every caller sees this as 'no data'")
	}
	for _, p := range got {
		if p.Value < 100 || p.Value > 200 {
			t.Errorf("value %v is not from CPIAUCSL — the series_id filter is not working", p.Value)
		}
	}
	if v, ok := CPIForMonth(got, time.Date(2015, 6, 2, 0, 0, 0, 0, time.UTC)); !ok {
		t.Error("June 2015 reading missing")
	} else if v != 105 {
		t.Errorf("June 2015 = %v, want 105", v)
	}
	if _, ok := CPIForMonth(got, time.Date(2016, 3, 2, 0, 0, 0, 0, time.UTC)); !ok {
		t.Error("March 2016 reading missing at the far end")
	}
}

// The Date column's declared type must not decide whether this metric works.
// Loaders in this repo write dates as DATE, as TIMESTAMP and as text depending
// on the script, and a reader that only handles one of them fails by returning
// nothing — which every caller reads as "no data" rather than as a bug.
func TestGetCPISeriesAcceptsEveryDateColumnType(t *testing.T) {
	for _, colType := range []string{"DATE", "TIMESTAMP", "VARCHAR"} {
		t.Run(colType, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cpi.db")
			if _, err := InitDB(path); err != nil {
				t.Fatalf("open: %v", err)
			}
			if _, err := db.Exec(`CREATE TABLE economic_indicators (
				Date ` + colType + `, series_id VARCHAR, indicator_name VARCHAR, value DOUBLE)`); err != nil {
				t.Fatalf("create: %v", err)
			}
			for i := 0; i < 3; i++ {
				d := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, i, 0)
				if _, err := db.Exec(
					`INSERT INTO economic_indicators VALUES (?, 'CPIAUCSL', 'CPI', ?)`,
					d.Format("2006-01-02"), 200+float64(i)); err != nil {
					t.Fatalf("insert: %v", err)
				}
			}
			got := GetCPISeries()
			if len(got) != 3 {
				t.Fatalf("read %d points from a %s column, want 3", len(got), colType)
			}
			v, ok := CPIForMonth(got, time.Date(2020, 2, 14, 0, 0, 0, 0, time.UTC))
			if !ok || v != 201 {
				t.Errorf("February lookup gave (%v, %v), want (201, true)", v, ok)
			}
		})
	}
}

// A database with no economic_indicators table must simply report nothing.
func TestGetCPISeriesWithoutTheTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nocpi.db")
	if _, err := InitDB(path); err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := GetCPISeries(); got != nil {
		t.Errorf("expected nil for a database with no economic_indicators, got %d points", len(got))
	}
}

func TestLoadIndicator(t *testing.T) {
	path := filepath.Join(t.TempDir(), "eco.db")
	if _, err := InitDB(path); err != nil {
		t.Fatalf("open: %v", err)
	}
	if got, err := LoadIndicator("UNRATE"); got != nil || err != nil {
		t.Fatalf("no table must be nil, nil: %v %v", got, err)
	}
	if _, err := db.Exec(`CREATE TABLE economic_indicators (
		Date DATE, series_id VARCHAR, indicator_name VARCHAR, value DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO economic_indicators VALUES
		('2026-02-01', 'FEDFUNDS', 'Federal Funds Rate', 0),
		('2026-01-01', 'FEDFUNDS', 'Federal Funds Rate', 0.25),
		('2026-01-01', 'UNRATE', 'Unemployment Rate', 4.3),
		('2026-03-01', 'FEDFUNDS', 'Federal Funds Rate', NULL)`); err != nil {
		t.Fatal(err)
	}
	got, err := LoadIndicator("FEDFUNDS")
	if err != nil || len(got) != 2 || got[0].Value != 0.25 || got[1].Value != 0 ||
		!got[1].Date.Equal(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("sorted, one series, a real zero kept, a NULL dropped: %v %+v", err, got)
	}
	if got, err := LoadIndicator("PAYEMS"); err != nil || got == nil || len(got) != 0 {
		t.Errorf("a missing series is empty, not nil: %v %v", got, err)
	}
}
