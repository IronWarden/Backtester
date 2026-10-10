package main

import (
	"context"
	"os/exec"
	"strings"
	"testing"
)

// Validation-path tests need no python3 and no network: every rejection
// happens before the subprocess is spawned.
func TestRunQuoteToolValidation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		in   []any
		want string
	}{
		{"empty", []any{}, "ERROR: no symbols given."},
		{"blank entries", []any{"", "  "}, "ERROR: no symbols given."},
		{"benchmark", []any{"$SP500"}, "local benchmark series"},
		{"junk", []any{"AAPL; rm -rf"}, "not a valid ticker"},
		{"flag-like", []any{"--full"}, "not a valid ticker"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runQuoteTool(ctx, tc.in)
			if !strings.Contains(got, tc.want) {
				t.Errorf("runQuoteTool(%v) = %q, want it to contain %q",
					tc.in, got, tc.want)
			}
		})
	}
}

func TestQuoteSymbols(t *testing.T) {
	got := quoteSymbols([]any{" aapl ", "MSFT", "aapl", 42, "brk-b"})
	want := []string{"AAPL", "MSFT", "BRK-B"}
	if len(got) != len(want) {
		t.Fatalf("quoteSymbols = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("quoteSymbols = %v, want %v", got, want)
		}
	}
}

func TestFormatQuoteResult(t *testing.T) {
	quotes := []map[string]any{
		{
			"symbol": "AAPL", "name": "Apple Inc.",
			"sector": "Technology", "industry": "Consumer Electronics",
			"price": 213.25, "currency": "USD", "previous_close": 211.18,
			"change_pct": 0.98, "market_cap": 3.19e12,
			"year_low": 169.21, "year_high": 260.10,
			"trailing_pe": 33.1, "eps_ttm": 6.44, "dividend_yield": 0.45,
			"profile": "Apple Inc. designs things.",
		},
		{"symbol": "SPY", "price": 560.0, "currency": "USD",
			"note": "fundamentals unavailable; quote fields only"},
		{"symbol": "BADSYM", "error": "no data found"},
	}
	out := formatQuoteResult(quotes)
	for _, want := range []string{
		"AAPL — Apple Inc. (Technology / Consumer Electronics)",
		"price: 213.25 USD (+0.98% vs prev close)",
		"market cap: 3.19T",
		"52w range: 169.21 – 260.10",
		"P/E (ttm): 33.1",
		"div yield: 0.45%",
		"profile: Apple Inc. designs things.",
		"note: fundamentals unavailable",
		"BADSYM: ERROR: no data found",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("formatQuoteResult output missing %q:\n%s", want, out)
		}
	}
}

func TestFmtBig(t *testing.T) {
	cases := map[float64]string{
		3.19e12: "3.19T",
		1.1e9:   "1.10B",
		42.5e6:  "42.50M",
		1500:    "1.5k",
		999:     "999",
	}
	for in, want := range cases {
		if got := fmtBig(in); got != want {
			t.Errorf("fmtBig(%v) = %q, want %q", in, got, want)
		}
	}
}

// Live test: hits Yahoo via the real helper. Skips when the environment
// can't run it (no python3, no network), mirroring requireOllama's style.
func TestRunQuoteToolLive(t *testing.T) {
	if testing.Short() {
		t.Skip("live network test")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not on PATH")
	}
	out := runQuoteTool(context.Background(), []any{"AAPL"})
	if strings.Contains(out, "timed out") ||
		strings.Contains(out, "quote helper failed") {
		t.Skipf("network/helper unavailable: %s", out)
	}
	if !strings.Contains(out, "AAPL") || !strings.Contains(out, "price:") {
		t.Fatalf("unexpected live output:\n%s", out)
	}
	t.Logf("live output:\n%s", out)
}
