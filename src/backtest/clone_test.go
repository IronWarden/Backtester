package backtest

import (
	"reflect"
	"testing"
	"time"

	"my-backtester/src/data"
)

// Portfolio.Clone rebuilds a struct from a hand-written field list, and the
// runner clones EVERY portfolio before handing it to a worker. So a field
// missing from that list is not a subtle bug: it works perfectly in every test
// that calls runOne directly, and does nothing at all in the real app.
//
// That has now happened twice. `CPI` was dropped, so real returns were 0 on
// every actual run while the unit tests passed; the cashflow schedule was
// dropped alongside it, so deposits and withdrawals never fired outside the
// tests either. Both were caught only by an end-to-end check against the real
// database, which is a slow and lucky way to find a one-line omission.
//
// This is the same shape as the Metrics-rebuild bug (see
// TestAccumulatedMetricsSurviveTheRebuild): a struct reassembled by hand
// silently forgets whatever was added after it was written.
//
// So every field must be CLASSIFIED. Adding one to Portfolio without deciding
// what Clone does with it fails this test, which is the whole point — the
// decision is cheap when you are already editing the struct and expensive six
// weeks later.
var clonePolicy = map[string]string{
	// Identity and configuration: a clone is the same portfolio, so these
	// must carry across or the worker simulates something else.
	"Pname":              "copy",
	"InitialBuyingPower": "copy",
	"StartTime":          "copy",
	"EndTime":            "copy",
	"Tickers":            "copy",
	"StrategySpec":       "copy",
	"StrategyParams":     "copy",
	"Costs":              "copy",
	"Benchmark":          "copy",
	"AllowShort":         "copy",
	"Margin":             "copy",
	"DelistingReturn":    "copy",
	"DelistingEvents":    "copy",
	"Fundamentals":       "copy",
	"FundamentalData":    "copy",
	"FundamentalSource":  "copy",
	"InSampleEnd":        "copy",
	"Trials":             "copy",
	"TrialGroup":         "copy",
	"Cashflow":           "copy",
	"cashflowStep":       "copy",
	"nextCashflow":       "copy",
	"cashflowAnchor":     "copy",
	"CPI":                "copy",

	// Rebuilt: a fresh strategy instance, because strategies carry per-run
	// state and two workers must not share one.
	"Strategy": "rebuild",

	// Deliberately reset: a clone starts the run over, so anything the
	// simulation produces begins empty. Carrying these would mean a worker
	// resuming someone else's half-finished run.
	"BuyingPower": "reset",
	"Positions":   "reset",
	"Baseline":    "reset",
	"Trades":      "reset",
	"openLots":    "reset",
	"TradeStats":  "reset",
	"Exposure":    "reset",
	// plot() output is a per-run result like Trades: the clone is starting
	// the run over, so it starts with nothing plotted.
	"plots":                "reset",
	"Regimes":              "reset",
	"Robustness":           "reset",
	"Significance":         "reset",
	"DailyReturns":         "reset",
	"PortfolioCloseValues": "reset",
	"Metrics":              "reset",
	"pendingCashflow":      "reset",
	"inflationFactor":      "reset",
	"forcedCover":          "reset",
	"tradedNotional":       "reset",
	"BenchmarkCurve":       "reset",
	"BenchmarkStats":       "reset",
	"Splits":               "reset",
}

func TestCloneHandlesEveryPortfolioField(t *testing.T) {
	pt := reflect.TypeOf(Portfolio{})
	for i := 0; i < pt.NumField(); i++ {
		name := pt.Field(i).Name
		if _, ok := clonePolicy[name]; !ok {
			t.Errorf("Portfolio.%s is not classified in clonePolicy. Decide "+
				"whether Clone should copy it, rebuild it, or reset it, and say "+
				"so here — the runner clones every portfolio before simulating "+
				"it, so a field Clone forgets works in the tests and does "+
				"nothing in the app.", name)
		}
	}
	// And the reverse: a stale entry means the field was renamed or removed
	// and the policy no longer describes the struct.
	for name := range clonePolicy {
		if _, ok := pt.FieldByName(name); !ok {
			t.Errorf("clonePolicy names %q, which Portfolio no longer has", name)
		}
	}
}

// The behavioural half: the configuration a clone must actually carry.
// Reflection can prove a field was considered; only this can prove it arrives.
func TestCloneCarriesTheRunConfiguration(t *testing.T) {
	pc := PortfolioConfig{
		Name: "orig", BuyingPower: 10000,
		StartTime: "2021-01-04", EndTime: "2021-12-31",
		Tickers: []string{"AAA"}, Strategy: "buyAndHold:equalWeights",
		Cashflow: CashflowConfig{Amount: 250, Frequency: "monthly"},
	}
	p, err := pc.ToPortfolio()
	if err != nil {
		t.Fatal(err)
	}
	p.CPI = []data.CPIPoint{{Date: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), Value: 100}}

	c, err := p.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.CPI) != len(p.CPI) {
		t.Error("Clone dropped CPI — real returns would be reported as " +
			"unavailable on every run that goes through the worker pool")
	}
	if c.Cashflow != p.Cashflow {
		t.Errorf("Clone dropped the cashflow schedule: %+v", c.Cashflow)
	}
	if c.cashflowStep != p.cashflowStep || !c.nextCashflow.Equal(p.nextCashflow) {
		t.Error("Clone dropped the derived cashflow schedule, so no flow would " +
			"ever fire on the real run path")
	}
	// And the reset half: a clone must not inherit a half-run simulation.
	if len(c.DailyReturns) != 0 || c.BuyingPower != p.InitialBuyingPower {
		t.Error("Clone inherited simulation state instead of starting fresh")
	}
}
