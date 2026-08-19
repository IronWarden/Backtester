package backtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// pageFixture is a result with every optional section populated, so a test can
// assert that a section appears and, by clearing a field, that it does not.
func pageFixture() Result {
	n := 300
	dates := make([]string, n)
	curve := make([]float64, n)
	bench := make([]float64, n)
	d := time.Date(2023, 1, 3, 0, 0, 0, 0, time.UTC)
	for i := range n {
		dates[i] = d.AddDate(0, 0, i).Format("2006-01-02")
		curve[i] = 100000 * (1 + float64(i)/1000)
		bench[i] = 100000 * (1 + float64(i)/1500)
	}
	// A dip, so the underwater plot has something to draw.
	for i := 100; i < 140; i++ {
		curve[i] *= 0.85
	}

	return Result{
		PortfolioName:  "Momentum 12-1",
		Strategy:       "lua:momentum.lua",
		InitialCapital: 100000,
		FinalValue:     curve[n-1],
		EquityCurve:    curve,
		Dates:          dates,
		BenchmarkCurve: bench,
		BenchmarkStats: BenchmarkStats{Ticker: "$SP500", AnnualReturn: 8.1, SharpeRatio: 0.9},
		Metrics: Metrics{
			SharpeRatio: 1.42, SortinoRatio: 1.98, MaxDrawdown: -18.3,
			AnnualReturn: 16.4, StandardDev: 0.115, Turnover: 3.1,
			AvgExposure: 87, TimeInMarket: 91, CalmarRatio: 0.9,
			LongestUnderwaterDays: 141, Beta: 0.88, Alpha: 3.1,
		},
		Baseline: BaselineStats{
			Computed: true, Name: "equal-weight buy & hold",
			ExcessAnnualReturn: 2.4, ExcessFinalValue: 8100,
		},
		Significance: Significance{Draws: 1000, PValue: 0.021, Percentile: 97.9},
		TradeStats: TradeStats{
			Trades: 84, Buys: 44, Sells: 40, RoundTrips: 40, Wins: 24, Losses: 16,
			WinRate: 60, ProfitFactor: 1.8, AvgHoldingDays: 31,
			TotalRealized: 12400, TotalFees: 310,
		},
		Trials: 200, TrialGroup: "momentum sweep",
		ExpectedMaxSharpe: 0.71, DeflatedSharpe: 0.62,
		Drawdowns: []Drawdown{{
			Start:     time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC),
			Trough:    time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC),
			Recovered: time.Date(2023, 7, 1, 0, 0, 0, 0, time.UTC),
			DepthPct:  18.3, DurationDays: 30, RecoveryDays: 61,
		}},
		RollingSharpe: []RollingPoint{
			{Date: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC), Value: 1.1},
			{Date: time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC), Value: 1.5},
			{Date: time.Date(2024, 3, 3, 0, 0, 0, 0, time.UTC), Value: 0.8},
		},
		YearlyReturns: []PeriodReturn{
			{Period: "2023", ReturnPct: 12.4, Start: time.Now(), End: time.Now()},
			{Period: "2024", ReturnPct: -3.1, Partial: true, Start: time.Now(), End: time.Now()},
		},
		MonthlyReturns: []PeriodReturn{
			{Period: "2023-01", ReturnPct: 2.1},
			{Period: "2023-02", ReturnPct: -1.4},
		},
		Splits: []SegmentStats{
			{Label: "in-sample", Start: "2023-01-03", End: "2023-09-01", Days: 170, TotalReturn: 14.2, SharpeRatio: 1.7},
			{Label: "out-of-sample", Start: "2023-09-04", End: "2024-01-01", Days: 130, TotalReturn: 2.1, SharpeRatio: 0.4},
		},
	}
}

func TestCandidatePageIsSelfContained(t *testing.T) {
	page := CandidatePage(pageFixture())

	if !strings.HasPrefix(page, "<!doctype html>") {
		t.Error("page is not a complete HTML document")
	}
	// The whole point of the format: it must work with no network and no JS.
	for _, forbidden := range []string{
		"<script", "http://", "https://", "@import", "<iframe", "srcset",
	} {
		if strings.Contains(page, forbidden) {
			t.Errorf("page is not self-contained: contains %q", forbidden)
		}
	}
	if !strings.Contains(page, "<svg") {
		t.Error("no inline SVG chart was rendered")
	}
}

// The task's actual requirement: the numbers that kill an idea must be at the
// headline, not in a footnote. This is the test that fails if someone later
// moves them.
func TestCandidatePageLeadsWithTheObjections(t *testing.T) {
	page := CandidatePage(pageFixture())

	verdict := strings.Index(page, "Does this survive the obvious objections?")
	if verdict < 0 {
		t.Fatal("the verdict strip is missing entirely")
	}
	// It must come before the charts and the metric dump.
	for _, later := range []string{"Equity curve", "All metrics", "Deepest drawdowns"} {
		if at := strings.Index(page, later); at >= 0 && at < verdict {
			t.Errorf("%q appears before the verdict strip; the objections must lead", later)
		}
	}
	for _, must := range []string{
		"equal-weight buy &amp; hold", // the baseline it has to beat
		"p = 0.021",                   // better than chance?
		"200",                         // the trial count
		"deflated Sharpe 0.620",
	} {
		if !strings.Contains(page, must) {
			t.Errorf("the verdict strip omits %q", must)
		}
	}
}

// A result selected from 200 trials with a weak deflated Sharpe must be flagged,
// not merely reported — that is the difference between disclosure and warning.
func TestCandidatePageFlagsAWeakDeflatedSharpe(t *testing.T) {
	r := pageFixture()
	r.DeflatedSharpe = 0.62
	if !strings.Contains(CandidatePage(r), `class="card bad"`) {
		t.Error("200 trials at a 0.62 deflated Sharpe was not flagged")
	}

	r.DeflatedSharpe = 0.97
	page := CandidatePage(r)
	if strings.Contains(page, `<div class="card bad"><span class="q">How many trials`) {
		t.Error("a 0.97 deflated Sharpe was flagged as bad")
	}
}

func TestCandidatePageOmitsSectionsWithNoData(t *testing.T) {
	var r Result
	r.PortfolioName = "empty"
	r.InitialCapital = 1000
	page := CandidatePage(r)

	for _, absent := range []string{
		"Equity curve", "Underwater", "Rolling 12-month Sharpe",
		"In-sample vs out-of-sample", "Walk-forward", "Deepest drawdowns",
	} {
		if strings.Contains(page, absent) {
			t.Errorf("section %q rendered with no data behind it", absent)
		}
	}
	// It still has to be a valid page, and it still has to say it never traded
	// rather than silently omitting the fact.
	if !strings.HasPrefix(page, "<!doctype html>") {
		t.Error("an empty result did not produce a valid document")
	}
	if !strings.Contains(page, "never traded") {
		t.Error("a portfolio that never traded does not say so")
	}
}

func TestCandidatePageEscapesUntrustedText(t *testing.T) {
	r := pageFixture()
	r.PortfolioName = `<script>alert(1)</script>`
	r.Strategy = `lua:"a&b".lua`
	page := CandidatePage(r)

	if strings.Contains(page, "<script>alert(1)</script>") {
		t.Error("a portfolio name was interpolated as raw HTML")
	}
	if !strings.Contains(page, "&lt;script&gt;") {
		t.Error("the portfolio name was not escaped")
	}
	if !strings.Contains(page, "a&amp;b") {
		t.Error("the strategy name was not escaped")
	}
}

func TestWriteCandidatePagesOneFilePerResult(t *testing.T) {
	dir := t.TempDir()
	a, b := pageFixture(), pageFixture()
	b.PortfolioName = "Mean Reversion"
	paths, err := WriteCandidatePages([]Result{a, b}, filepath.Join(dir, "pages"))
	if err != nil {
		t.Fatalf("WriteCandidatePages: %v", err)
	}
	if len(paths) != 2 {
		t.Fatalf("wrote %d pages for 2 results", len(paths))
	}
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("reading %s: %v", p, err)
		}
		if !strings.HasPrefix(string(data), "<!doctype html>") {
			t.Errorf("%s is not a complete document", p)
		}
	}
	if filepath.Base(paths[1]) != "Mean-Reversion.html" {
		t.Errorf("filename %q does not follow the portfolio name", filepath.Base(paths[1]))
	}
}

// Two portfolios may share a name. Losing one candidate's page silently is the
// one failure mode nobody would notice.
func TestWriteCandidatePagesDoesNotOverwriteADuplicateName(t *testing.T) {
	dir := t.TempDir()
	a, b := pageFixture(), pageFixture()
	b.FinalValue = 999
	paths, err := WriteCandidatePages([]Result{a, b}, dir)
	if err != nil {
		t.Fatalf("WriteCandidatePages: %v", err)
	}
	if paths[0] == paths[1] {
		t.Fatalf("both results wrote to %s", paths[0])
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s does not exist: %v", p, err)
		}
	}
}

func TestSafeFilenameNeverEmptyOrPathTraversing(t *testing.T) {
	cases := map[string]string{
		"Momentum 12-1":    "Momentum-12-1",
		"../../etc/passwd": "etc-passwd",
		"":                 "candidate",
		"///":              "candidate",
		"a/b":              "a-b",
	}
	for in, want := range cases {
		if got := safeFilename(in); got != want {
			t.Errorf("safeFilename(%q) = %q, want %q", in, got, want)
		}
	}
}
