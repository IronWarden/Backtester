package data

import (
	"path/filepath"
	"testing"
)

// Exercises the SQL-console path end to end on a throwaway DuckDB: open,
// query through RunQuery's string rendering, and InitDB's reuse/close
// semantics (same path → same handle; new path → old handle closed).
func TestConsoleSmoke(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "smoke.db")

	h, err := InitDB(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := h.Exec(
		`CREATE TABLE t AS SELECT 1 AS a, 'x' AS b UNION ALL SELECT 2, NULL`,
	); err != nil {
		t.Fatalf("create: %v", err)
	}

	cols, rows, truncated, err := RunQuery("SELECT * FROM t ORDER BY a", 10)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(cols) != 2 || cols[0] != "a" || cols[1] != "b" {
		t.Fatalf("cols = %v", cols)
	}
	if len(rows) != 2 || rows[0][0] != "1" || rows[0][1] != "x" || rows[1][1] != "NULL" {
		t.Fatalf("rows = %v", rows)
	}
	if truncated {
		t.Fatal("unexpected truncation")
	}

	if _, _, truncated, _ = RunQuery("SELECT * FROM t", 1); !truncated {
		t.Fatal("expected truncation at maxRows=1")
	}

	h2, err := InitDB(path)
	if err != nil {
		t.Fatalf("re-init: %v", err)
	}
	if h2 != h {
		t.Fatal("InitDB reopened a new handle for the same path")
	}

	other := filepath.Join(dir, "other.db")
	h3, err := InitDB(other)
	if err != nil {
		t.Fatalf("open other: %v", err)
	}
	if h3 == h {
		t.Fatal("InitDB returned the old handle for a new path")
	}
	if err := h.Ping(); err == nil {
		t.Fatal("old handle still open after switching databases")
	}
}
