package data

import (
	"path/filepath"
	"testing"
)

// Exercises UpsertBars on a throwaway DuckDB: table creation, insert,
// and the delete-then-insert idempotency within the fetched range.
func TestUpsertBars(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upsert.db")
	if _, err := InitDB(path); err != nil {
		t.Fatalf("open: %v", err)
	}

	bars := []Bar{
		{Date: "2024-01-02", Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 100},
		{Date: "2024-01-03", Open: 1.5, High: 2.5, Low: 1, Close: 2, Volume: 200},
		{Date: "2024-01-04", Open: 2, High: 3, Low: 1.5, Close: 2.5, Volume: 300},
	}
	if err := UpsertBars("TEST", bars); err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	cols, rows, _, err := RunQuery(
		`SELECT COUNT(*), MIN(Date), MAX(Date) FROM stock_data_optimized
		 WHERE Ticker = 'TEST'`, 10)
	if err != nil {
		t.Fatalf("verify query: %v", err)
	}
	if len(cols) != 3 || rows[0][0] != "3" {
		t.Fatalf("after first upsert: rows = %v", rows)
	}

	// Overlapping re-upsert with a changed close: still 3 rows, new value.
	bars[1].Close = 9.9
	if err := UpsertBars("TEST", bars); err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	_, rows, _, err = RunQuery(
		`SELECT COUNT(*), MAX(Close) FROM stock_data_optimized
		 WHERE Ticker = 'TEST'`, 10)
	if err != nil {
		t.Fatalf("verify query 2: %v", err)
	}
	if rows[0][0] != "3" || rows[0][1] != "9.9" {
		t.Fatalf("after re-upsert: rows = %v", rows)
	}

	// A different ticker's rows must survive the upsert untouched.
	if err := UpsertBars("OTHER", bars[:1]); err != nil {
		t.Fatalf("other upsert: %v", err)
	}
	if err := UpsertBars("TEST", bars); err != nil {
		t.Fatalf("third upsert: %v", err)
	}
	_, rows, _, _ = RunQuery(
		`SELECT COUNT(*) FROM stock_data_optimized WHERE Ticker = 'OTHER'`, 10)
	if rows[0][0] != "1" {
		t.Fatalf("OTHER rows clobbered: %v", rows)
	}
}
