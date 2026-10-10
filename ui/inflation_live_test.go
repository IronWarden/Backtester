package main

import (
	"os"
	"strings"
	"testing"
)

// End-to-end real return against the real database.
//
// Skipped by default: it needs `stock_data.db`, which DuckDB single-writes, so
// it fails whenever the desktop app is open. Run it deliberately:
//
//	REAL_RUN=1 go test ./ui -run TestRealReturnAgainstTheRealDatabase -v
//
// It exists because the unit tests cover the arithmetic and the SQL against a
// synthetic table, and neither can catch the one thing that actually goes
// wrong here: the real `economic_indicators` not matching what the reader
// expects. A silent nil from GetCPISeries is indistinguishable from "no data"
// at every layer above it, so the metric would just never appear.
func TestRealReturnAgainstTheRealDatabase(t *testing.T) {
	if os.Getenv("REAL_RUN") == "" {
		t.Skip("set REAL_RUN=1; needs exclusive access to stock_data.db")
	}
	// A window that ends well inside CPI's coverage. CPI is published in
	// arrears, so a window ending at the last price bar will legitimately
	// report nothing.
	cfg := `
[[Portfolio]]
Name = "real check"
BuyingPower = 100000.0
StartDate = "2015-06-01"
EndDate = "2025-06-01"
Tickers = ["AAPL", "MSFT"]
Benchmark = "$SP500"
Strategy = "buyAndHold:equalWeights"
`
	a := &App{}
	out, err := a.RunBacktest(cfg, "../stock_data.db", "", "")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(out.Results) == 0 {
		t.Fatal("no results")
	}
	for _, r := range out.Results {
		t.Logf("%s: nominal %.2f%%  real %.2f%%  |  %s nominal %.2f%%  real %.2f%%",
			r.PortfolioName, r.AnnualReturn, r.RealAnnualReturn,
			r.BenchmarkStats.Ticker, r.BenchmarkStats.AnnualReturn,
			r.BenchmarkStats.RealAnnualReturn)

		if r.RealAnnualReturn == 0 {
			t.Error("no real return for a window CPI covers — GetCPISeries is " +
				"not reading the real economic_indicators table")
		}
		if r.RealAnnualReturn >= r.AnnualReturn {
			t.Errorf("real %.2f is not below nominal %.2f under positive inflation",
				r.RealAnnualReturn, r.AnnualReturn)
		}
		if r.BenchmarkStats.Ticker != "" && r.BenchmarkStats.RealAnnualReturn == 0 {
			t.Error("benchmark has a nominal return but no real one — the two " +
				"columns must be filled in together or neither is comparable")
		}
	}
	// The cashflow schedule went through the same clone, and was dropped by
	// it in exactly the same way. Checked here rather than only in the unit
	// tests, because the unit tests call runOne directly and never clone.
	withFlows := `
[[Portfolio]]
Name = "contributing"
BuyingPower = 100000.0
StartDate = "2015-06-01"
EndDate = "2025-06-01"
Tickers = ["AAPL", "MSFT"]
Strategy = "buyAndHold:equalWeights"

  [Portfolio.Cashflow]
  Amount = 1000.0
  Frequency = "monthly"
`
	flowOut, err := a.RunBacktest(withFlows, "../stock_data.db", "", "")
	if err != nil {
		t.Fatalf("cashflow run: %v", err)
	}
	for _, r := range flowOut.Results {
		t.Logf("%s: contributed %.0f, final %.0f, profit %.0f",
			r.PortfolioName, r.NetContributions, r.FinalValue,
			r.FinalValue-r.InitialCapital-r.NetContributions)
		if r.NetContributions == 0 {
			t.Error("a monthly schedule contributed nothing on the real run " +
				"path — the portfolio is cloned before simulation and the " +
				"schedule is not surviving the clone")
		}
	}

	for _, d := range out.Diagnostics {
		if strings.Contains(d.Message, "real return") {
			t.Logf("diagnostic: %s", d.Message)
		}
	}
}
