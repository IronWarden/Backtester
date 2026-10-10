package main

import (
	"strings"
	"testing"
)

func TestDBOverviewAndQueryTool(t *testing.T) {
	ov := dbOverview("../stock_data.db")
	t.Logf("overview:\n%s", ov)
	if strings.Contains(ov, "Conflicting lock") {
		// DuckDB is single-writer across processes: the running UI app
		// holds the lock, so this test only runs with the app closed.
		t.Skip("stock_data.db locked by a running app instance")
	}
	if strings.Contains(ov, "could not be opened") {
		t.Fatalf("db open failed: %s", ov)
	}
	if !strings.Contains(ov, "stock_data_optimized") {
		t.Fatalf("overview missing price table: %s", ov)
	}

	out := runQueryTool("../stock_data.db",
		`SELECT Ticker, MIN(Date), MAX(Date) FROM stock_data_optimized
		 WHERE Ticker IN ('AAPL','$SP500','CASH') GROUP BY Ticker ORDER BY Ticker`)
	t.Logf("query result:\n%s", out)
	if strings.HasPrefix(out, "ERROR") {
		t.Fatalf("query tool failed: %s", out)
	}

	if out := runQueryTool("../stock_data.db", "DELETE FROM stock_data_optimized"); !strings.HasPrefix(out, "ERROR") {
		t.Fatalf("write statement was not blocked: %s", out)
	}
	if out := runQueryTool("../stock_data.db", "-- sneaky\nUPDATE stock_data_optimized SET Close=0"); !strings.HasPrefix(out, "ERROR") {
		t.Fatalf("commented write was not blocked: %s", out)
	}
}
