package main

import (
	"path/filepath"
	"strings"
	"testing"

	"my-backtester/src/data"
)

func TestFirstSQLKeyword(t *testing.T) {
	cases := map[string]string{
		"SELECT * FROM x":                 "select",
		"  \n\t WITH t AS (SELECT 1) ...": "with",
		"(select 1)":                      "select",
		"-- comment\n-- more\nDESCRIBE x": "describe",
		"DELETE FROM t":                   "delete",
		"-- only a comment":               "",
		"":                                "",
		"\n\n":                            "",
		"UPDATE t SET a=1 -- sneaky":      "update",
	}
	for in, want := range cases {
		if got := firstSQLKeyword(in); got != want {
			t.Errorf("firstSQLKeyword(%q) = %q, want %q", in, got, want)
		}
	}
}

// seedTempDB opens a fresh DuckDB and stores one ticker's bars through the
// production write path, returning the db path.
func seedTempDB(t *testing.T, ticker string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	if _, err := data.InitDB(path); err != nil {
		t.Fatalf("open temp db: %v", err)
	}
	err := data.UpsertBars(ticker, []data.Bar{
		{Date: "2024-01-02", Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 100},
		{Date: "2024-01-03", Open: 1.5, High: 2.5, Low: 1, Close: 2, Volume: 200},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return path
}

// The write gate is the only thing between the model and the user's data —
// every mutating statement shape must come back as ERROR text.
func TestRunQueryToolReadOnlyGate(t *testing.T) {
	dbPath := seedTempDB(t, "GATE")

	out := runQueryTool(dbPath,
		"SELECT COUNT(*) AS n FROM stock_data_optimized WHERE Ticker='GATE'")
	if strings.HasPrefix(out, "ERROR") {
		t.Fatalf("read query failed: %s", out)
	}
	if !strings.Contains(out, "2") {
		t.Fatalf("expected 2 rows counted, got: %s", out)
	}

	for _, sql := range []string{
		"DELETE FROM stock_data_optimized",
		"UPDATE stock_data_optimized SET Close = 0",
		"INSERT INTO stock_data_optimized VALUES (now(),'X',1,1,1,1,1,NULL)",
		"DROP TABLE stock_data_optimized",
		"-- harmless comment\nDELETE FROM stock_data_optimized",
		"CREATE TABLE pwned (x INT)",
	} {
		if out := runQueryTool(dbPath, sql); !strings.HasPrefix(out, "ERROR") {
			t.Errorf("write statement not blocked: %q -> %s", sql, out)
		}
	}

	// Broken-but-allowed SQL must come back as ERROR text, never a panic.
	if out := runQueryTool(dbPath, "SELECT nope FROM missing"); !strings.HasPrefix(out, "ERROR") {
		t.Errorf("bad query did not return ERROR text: %s", out)
	}
}

func TestClipText(t *testing.T) {
	if got := clipText("short", 100); got != "short" {
		t.Errorf("clipText passthrough = %q", got)
	}
	long := strings.Repeat("x", 200)
	got := clipText(long, 50)
	if !strings.HasPrefix(got, strings.Repeat("x", 50)) ||
		!strings.HasSuffix(got, "… (truncated)") {
		t.Errorf("clipText truncation = %q", got)
	}
}

// buildSystemPrompt assembles the assistant's whole world; a section
// silently dropping out (or lean mode pulling in the heavy parts) degrades
// every chat without any visible error.
func TestBuildSystemPrompt(t *testing.T) {
	dbPath := seedTempDB(t, "PROMPT")

	full := buildSystemPrompt(dbPath, "cfg-sentinel", "lua-sentinel", false)
	for _, want := range []string{
		"Today's date: 20",
		"## Live database",
		"stock_data_optimized",
		"## User-defined configs on disk",
		"cfg-sentinel",
		"lua-sentinel",
	} {
		if !strings.Contains(full, want) {
			t.Errorf("full prompt missing %q", want)
		}
	}

	lean := buildSystemPrompt(dbPath, "", "", true)
	if strings.Contains(lean, "## User-defined configs on disk") {
		t.Error("lean prompt should skip the saved-config dump")
	}
	if strings.Contains(lean, "currently open in the editor") {
		t.Error("lean prompt with empty buffers should have no editor sections")
	}
}
