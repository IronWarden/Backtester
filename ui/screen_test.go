package main

import (
	"context"
	"os/exec"
	"strings"
	"testing"
)

func TestScreenFiltersValidation(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]any
		want string
	}{
		{"no filters", map[string]any{}, "at least one filter"},
		{"only sort", map[string]any{"sort_by": "pe"}, "at least one filter"},
		{"negative pe", map[string]any{"pe_max": -5.0}, "non-negative"},
		{"pe not a number", map[string]any{"pe_max": "twenty"}, "non-negative number"},
		{"bad sector", map[string]any{"sector": "Tech; DROP TABLE"}, "not a valid sector"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, errMsg := screenFilters(tc.in)
			if !strings.Contains(errMsg, tc.want) {
				t.Errorf("screenFilters(%v) err = %q, want it to contain %q",
					tc.in, errMsg, tc.want)
			}
		})
	}
}

func TestScreenFiltersArgs(t *testing.T) {
	args, preview, errMsg := screenFilters(map[string]any{
		"pe_max":         20.0,
		"market_cap_min": 1e10,
		"sector":         "Technology",
		"sort_by":        "pe",
		"limit":          150.0, // clamped to 100
	})
	if errMsg != "" {
		t.Fatalf("unexpected error: %s", errMsg)
	}
	got := strings.Join(args, " ")
	for _, want := range []string{
		"--pe-max 20", "--cap-min 10000000000",
		"--sector Technology", "--sort pe", "--limit 100",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("args %q missing %q", got, want)
		}
	}
	p := strings.Join(preview, " ")
	if !strings.Contains(p, "pe<20") || !strings.Contains(p, "sector=Technology") {
		t.Errorf("preview = %q", p)
	}
}

func TestFormatScreenResult(t *testing.T) {
	out := formatScreenResult(1161, []map[string]any{
		{"symbol": "GOOG", "name": "Alphabet Inc.", "pe": 17.9,
			"market_cap": 4.36e12, "price": 356.65, "dividend_yield": 0.25},
		{"symbol": "SPCX", "market_cap": 1.4e12},
	})
	for _, want := range []string{
		"1161 total matches; showing 2:",
		"GOOG — Alphabet Inc. | PE 17.9 | cap 4.36T | $356.65 | yield 0.25%",
		"SPCX | cap 1.40T",
		"1159 more matches",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}

	if out := formatScreenResult(0, nil); !strings.Contains(out, "0 matches") {
		t.Errorf("empty result output = %q", out)
	}
}

// Live end-to-end against Yahoo's screener; skips when unavailable.
func TestRunScreenToolLive(t *testing.T) {
	if testing.Short() {
		t.Skip("live network test")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not on PATH")
	}
	out := runScreenTool(context.Background(), map[string]any{
		"pe_max":         20.0,
		"market_cap_min": 1e11,
		"limit":          3.0,
	})
	if strings.Contains(out, "timed out") ||
		strings.Contains(out, "helper failed") {
		t.Skipf("network/helper unavailable: %s", out)
	}
	if !strings.Contains(out, "total matches") || !strings.Contains(out, "PE ") {
		t.Fatalf("unexpected live output:\n%s", out)
	}
	t.Logf("live output:\n%s", out)
}
