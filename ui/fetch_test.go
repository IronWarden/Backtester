package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"my-backtester/src/data"
)

func TestRunFetchToolValidation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		db   string
		in   map[string]any
		want string
	}{
		{"no db", "", map[string]any{"symbols": []any{"TSLA"}},
			"no database chosen"},
		{"no symbols", "x.db", map[string]any{},
			"ERROR: no symbols given."},
		{"benchmark", "x.db", map[string]any{"symbols": []any{"$SP500"}},
			"cannot be fetched"},
		{"junk symbol", "x.db", map[string]any{"symbols": []any{"A;B"}},
			"not a valid ticker"},
		{"bad date", "x.db",
			map[string]any{"symbols": []any{"TSLA"}, "start": "01/02/2024"},
			"not a YYYY-MM-DD date"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runFetchTool(ctx, tc.db, tc.in, nil)
			if !strings.Contains(got, tc.want) {
				t.Errorf("runFetchTool = %q, want it to contain %q", got, tc.want)
			}
		})
	}
}

func TestRowsToBars(t *testing.T) {
	bars, err := rowsToBars([][]any{
		{"2024-01-02", 1.0, 2.0, 0.5, 1.5, 100.0},
		{"2024-01-03", 1.5, 2.5, 1.0, 2.0, 200.0},
	})
	if err != nil {
		t.Fatalf("rowsToBars: %v", err)
	}
	if len(bars) != 2 || bars[0].Date != "2024-01-02" || bars[1].Close != 2.0 {
		t.Fatalf("bars = %+v", bars)
	}

	for name, rows := range map[string][][]any{
		"empty":      {},
		"short row":  {{"2024-01-02", 1.0}},
		"bad date":   {{"jan 2", 1.0, 2.0, 0.5, 1.5, 100.0}},
		"bad number": {{"2024-01-02", "x", 2.0, 0.5, 1.5, 100.0}},
	} {
		if _, err := rowsToBars(rows); err == nil {
			t.Errorf("%s: expected error, got none", name)
		}
	}
}

func TestFetchPreview(t *testing.T) {
	in := map[string]any{"symbols": []any{"tsla", "NVDA"}}
	if got := fetchPreview(in); got != "TSLA, NVDA (full history)" {
		t.Errorf("fetchPreview = %q", got)
	}
	in["start"] = "2024-01-01"
	if got := fetchPreview(in); got != "TSLA, NVDA (2024-01-01 → today)" {
		t.Errorf("fetchPreview = %q", got)
	}
}

// Live end-to-end: fetch a short SPY range from Yahoo and upsert it into a
// throwaway DuckDB. Skips when the environment can't run it.
func TestRunFetchToolLive(t *testing.T) {
	if testing.Short() {
		t.Skip("live network test")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not on PATH")
	}
	dbPath := filepath.Join(t.TempDir(), "fetch.db")
	out := runFetchTool(context.Background(), dbPath, map[string]any{
		"symbols": []any{"SPY"},
		"start":   "2024-01-02",
		"end":     "2024-01-10",
	}, nil)
	if strings.Contains(out, "timed out") ||
		strings.Contains(out, "fetch helper failed") {
		t.Skipf("network/helper unavailable: %s", out)
	}
	if !strings.Contains(out, "SPY: stored") {
		t.Fatalf("unexpected output:\n%s", out)
	}
	// The tool already ran InitDB(dbPath); verify through the same handle.
	_, rows, _, err := data.RunQuery(
		`SELECT COUNT(*) FROM stock_data_optimized WHERE Ticker = 'SPY'`, 10)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if rows[0][0] == "0" {
		t.Fatalf("no rows stored; tool output:\n%s", out)
	}
	t.Logf("live output:\n%s\nrows in db: %s", out, rows[0][0])
}
