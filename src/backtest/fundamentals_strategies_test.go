package backtest

import (
	"bytes"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"my-backtester/src/data"
)

// The shipped fundamentals strategies, fed SEC-shaped figures in which one
// company is unambiguously the pick. strategies_library_test.go runs them with
// no figures at all; this is the half that proves they read the right ones.

// annualSeries is one metric's fiscal years for a ticker, newest first: the
// first value is FY2018 (filed 2019-03-01), the next FY2017, and so on — all
// public before synthHist's first bar on 2020-01-01.
func annualSeries(ticker, metric string, instant bool, newestFirst ...float64) []data.FundamentalPoint {
	var out []data.FundamentalPoint
	for i, v := range newestFirst {
		year := 2018 - i
		p := data.FundamentalPoint{
			RawFundamental: data.RawFundamental{
				Ticker: ticker, Metric: metric, Value: v, Frequency: "annual",
				PeriodEnd: time.Date(year, 12, 31, 0, 0, 0, 0, time.UTC),
			},
			KnownFrom: time.Date(year+1, 3, 1, 0, 0, 0, 0, time.UTC),
			Source:    data.SourceSEC,
			Instant:   instant,
		}
		out = append(out, p)
		if instant {
			// The SEC reader emits every balance sheet as quarterly too.
			p.Frequency = "quarterly"
			out = append(out, p)
		}
	}
	return out
}

func runFundamentalStrategy(
	t *testing.T, name string, params map[string]any, points []data.FundamentalPoint,
) *Portfolio {
	t.Helper()
	hist := synthHist(42)
	p := libPortfolio(nil)
	metrics := map[string]bool{}
	for _, pt := range points {
		metrics[pt.Metric] = true
	}
	for m := range metrics {
		p.Fundamentals = append(p.Fundamentals, m)
	}
	p.FundamentalData = points
	p.FundamentalSource = data.SourceSEC

	strat, err := NewLuaStrategy(filepath.Join("..", "..", "strategies", name), params)
	if err != nil {
		t.Fatalf("NewLuaStrategy(%s): %v", name, err)
	}
	defer strat.Close()
	p.Strategy = strat

	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	for day := 0; day < libDays; day++ {
		for ticker, series := range hist {
			if pos, ok := p.Positions[ticker]; ok && pos != nil {
				pos.CurrentPrice = series[day].Close
			}
		}
		strat.Step(p, hist, day)
	}
	if out := strings.TrimSpace(logBuf.String()); out != "" {
		t.Fatalf("%s logged:\n%s", name, out)
	}
	return p
}

// assertHoldsOnly fails unless want is the one ticker with shares.
func assertHoldsOnly(t *testing.T, p *Portfolio, want string) {
	t.Helper()
	for _, ticker := range libTickers {
		amount := 0.0
		if pos := p.Positions[ticker]; pos != nil {
			amount = pos.Amount
		}
		if held := amount > 1e-9; held != (ticker == want) {
			t.Errorf("%s: amount %v — want only %s held", ticker, amount, want)
		}
	}
}

func concat(groups ...[]data.FundamentalPoint) []data.FundamentalPoint {
	var out []data.FundamentalPoint
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

func TestQualityROEHoldsTheMostProfitable(t *testing.T) {
	points := concat(
		annualSeries("AAA", "Net Income", false, 10),
		annualSeries("BBB", "Net Income", false, 50),
		annualSeries("CCC", "Net Income", false, 90),
		annualSeries("AAA", "Stockholders Equity", true, 100, 100),
		annualSeries("BBB", "Stockholders Equity", true, 100, 100),
		// The highest ROE on paper, from negative equity — excluded.
		annualSeries("CCC", "Stockholders Equity", true, -20, -10),
	)
	p := runFundamentalStrategy(t, "quality_roe.lua",
		map[string]any{"top_n": int64(1), "min_names": int64(2)}, points)
	assertHoldsOnly(t, p, "BBB")
}

func TestGrossProfitabilityRebuildsGrossProfit(t *testing.T) {
	points := concat(
		annualSeries("AAA", "Gross Profit", false, 10),
		// No Gross Profit line: rebuilt as 100 - 40 = 60.
		annualSeries("BBB", "Total Revenue", false, 100),
		annualSeries("BBB", "Cost Of Revenue", false, 40),
		annualSeries("CCC", "Gross Profit", false, 20),
		annualSeries("AAA", "Total Assets", true, 100),
		annualSeries("BBB", "Total Assets", true, 100),
		annualSeries("CCC", "Total Assets", true, 100),
	)
	p := runFundamentalStrategy(t, "gross_profitability.lua",
		map[string]any{"top_n": int64(1), "min_names": int64(2)}, points)
	assertHoldsOnly(t, p, "BBB")
}

func TestLowAssetGrowthHoldsTheShrinker(t *testing.T) {
	points := concat(
		annualSeries("AAA", "Total Assets", true, 150, 100),
		annualSeries("BBB", "Total Assets", true, 90, 100),
		annualSeries("CCC", "Total Assets", true, 120, 100),
	)
	p := runFundamentalStrategy(t, "low_asset_growth.lua",
		map[string]any{"top_n": int64(1), "min_names": int64(2)}, points)
	assertHoldsOnly(t, p, "BBB")
}

func TestStrongBalanceSheetNeedsCashFlow(t *testing.T) {
	points := concat(
		annualSeries("AAA", "Stockholders Equity", true, 30),
		// The strongest balance sheet, but burning cash — excluded.
		annualSeries("BBB", "Stockholders Equity", true, 80),
		annualSeries("CCC", "Stockholders Equity", true, 60),
		annualSeries("AAA", "Total Assets", true, 100),
		annualSeries("BBB", "Total Assets", true, 100),
		annualSeries("CCC", "Total Assets", true, 100),
		annualSeries("AAA", "Operating Cash Flow", false, 5),
		annualSeries("BBB", "Operating Cash Flow", false, -1),
		annualSeries("CCC", "Operating Cash Flow", false, 5),
	)
	p := runFundamentalStrategy(t, "strong_balance_sheet.lua",
		map[string]any{"top_n": int64(1), "min_names": int64(2)}, points)
	assertHoldsOnly(t, p, "CCC")
}

func TestPiotroskiHoldsOnlyTheImprover(t *testing.T) {
	improver := concat(
		// Every one of the eight tests passes.
		annualSeries("BBB", "Net Income", false, 20, 10),
		annualSeries("BBB", "Total Assets", true, 100, 100, 100),
		annualSeries("BBB", "Operating Cash Flow", false, 30),
		annualSeries("BBB", "Long Term Debt", true, 10, 20),
		annualSeries("BBB", "Current Assets", true, 200, 150),
		annualSeries("BBB", "Current Liabilities", true, 100, 100),
		annualSeries("BBB", "Gross Profit", false, 50, 40),
		annualSeries("BBB", "Total Revenue", false, 100, 90),
	)
	// Scoreable but failing nearly everything: a loss, worsening.
	decliner := func(ticker string) []data.FundamentalPoint {
		return concat(
			annualSeries(ticker, "Net Income", false, -5, 10),
			annualSeries(ticker, "Total Assets", true, 100, 100, 100),
			annualSeries(ticker, "Operating Cash Flow", false, -10),
		)
	}
	points := concat(improver, decliner("AAA"), decliner("CCC"))
	p := runFundamentalStrategy(t, "piotroski_fscore.lua",
		map[string]any{"min_score": int64(8)}, points)
	assertHoldsOnly(t, p, "BBB")
}
