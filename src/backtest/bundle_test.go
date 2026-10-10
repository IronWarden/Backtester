package backtest

// A bundle is only worth having if its verification can FAIL. These tests are
// mostly about drift being caught: a check that always passes launders a
// regression as a reproduction.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sampleRun() BundleRun {
	return BundleRun{
		Name:     "sample",
		Strategy: "buyAndHold:equalWeights",
		Tickers:  []string{"AAA", "BBB"},
		Start:    "2021-01-04",
		End:      "2021-12-31",
		Capital:  10000,
		Metrics: Metrics{
			AnnualReturn: 12.5, SharpeRatio: 1.2, SortinoRatio: 1.6,
			MaxDrawdown: 8.4, StandardDev: 0.11, Turnover: 0.9,
		},
		FinalValue: 11250,
		Trades:     4,
		Days:       252,
	}
}

func matchingResult(run BundleRun) Result {
	return Result{
		PortfolioName: run.Name,
		Strategy:      run.Strategy,
		Metrics:       run.Metrics,
		FinalValue:    run.FinalValue,
		TradeStats:    TradeStats{Trades: run.Trades},
		EquityCurve:   make([]float64, run.Days),
	}
}

// The calibration case: an identical re-run reports nothing.
func TestIdenticalRunReproduces(t *testing.T) {
	run := sampleRun()
	if got := CompareRun(run, matchingResult(run), ""); len(got) != 0 {
		t.Errorf("an identical result reported %d discrepancies: %v",
			len(got), got)
	}
}

// Every figure the bundle records must be checked. A field captured but never
// compared is decoration, and the drift it would have caught goes unreported.
func TestEveryRecordedFigureIsChecked(t *testing.T) {
	run := sampleRun()

	cases := map[string]func(*Result){
		"final value":   func(r *Result) { r.FinalValue *= 1.01 },
		"annual return": func(r *Result) { r.Metrics.AnnualReturn += 0.5 },
		"sharpe":        func(r *Result) { r.Metrics.SharpeRatio += 0.1 },
		"sortino":       func(r *Result) { r.Metrics.SortinoRatio += 0.1 },
		"max drawdown":  func(r *Result) { r.Metrics.MaxDrawdown += 1 },
		"stdev":         func(r *Result) { r.Metrics.StandardDev += 0.01 },
		"turnover":      func(r *Result) { r.Metrics.Turnover += 0.1 },
		"trades":        func(r *Result) { r.TradeStats.Trades++ },
		"days":          func(r *Result) { r.EquityCurve = make([]float64, 251) },
	}
	for field, drift := range cases {
		t.Run(field, func(t *testing.T) {
			fresh := matchingResult(run)
			drift(&fresh)
			got := CompareRun(run, fresh, "")
			if len(got) == 0 {
				t.Fatalf("changing the %s was not detected", field)
			}
			if got[0].Field != field {
				t.Errorf("reported %q, want %q", got[0].Field, field)
			}
		})
	}
}

// Float noise is not drift: the engine is deterministic, so the tolerance
// exists only to absorb the last bits, and it must not swallow a real change.
func TestToleranceAbsorbsNoiseButNotChange(t *testing.T) {
	run := sampleRun()

	noisy := matchingResult(run)
	noisy.Metrics.SharpeRatio += run.Metrics.SharpeRatio * 1e-12
	if got := CompareRun(run, noisy, ""); len(got) != 0 {
		t.Errorf("a 1e-12 relative difference was reported as drift: %v", got)
	}

	real := matchingResult(run)
	real.Metrics.SharpeRatio += run.Metrics.SharpeRatio * 1e-6
	if got := CompareRun(run, real, ""); len(got) == 0 {
		t.Error("a 1e-6 relative difference was swallowed by the tolerance")
	}
}

// A changed strategy script is its own finding, reported first: every number
// below it is then expected to differ, and listing them individually would bury
// the reason.
func TestChangedStrategySourceIsItsOwnFinding(t *testing.T) {
	run := sampleRun()
	run.SourceSHA = "abc123"

	got := CompareRun(run, matchingResult(run), "def456")
	if len(got) == 0 {
		t.Fatal("a changed strategy source was not reported")
	}
	if !strings.Contains(got[0].Field, "source changed") {
		t.Errorf("first finding is %q, want the source change", got[0].Field)
	}

	// An unchanged source says nothing.
	if extra := CompareRun(run, matchingResult(run), "abc123"); len(extra) != 0 {
		t.Errorf("an unchanged source reported %v", extra)
	}
	// And a bundle from a built-in strategy has no source to compare.
	run.SourceSHA = ""
	if extra := CompareRun(run, matchingResult(run), ""); len(extra) != 0 {
		t.Errorf("a sourceless bundle reported %v", extra)
	}
}

// Save and load must round-trip, and a bundle whose portfolio cannot be rebuilt
// is useless — the config is most of what a bundle is for.
func TestBundleRoundTripsAndRebuilds(t *testing.T) {
	original := Bundle{
		Version:   BundleVersion,
		EngineSHA: "deadbeef",
		Runs:      []BundleRun{sampleRun()},
	}
	path := filepath.Join(t.TempDir(), "bundle.json")
	if err := original.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := LoadBundle(path)
	if err != nil {
		t.Fatalf("LoadBundle: %v", err)
	}
	if loaded.EngineSHA != "deadbeef" || len(loaded.Runs) != 1 {
		t.Fatalf("round trip lost data: %+v", loaded)
	}
	if loaded.Runs[0].Metrics.SharpeRatio != original.Runs[0].Metrics.SharpeRatio {
		t.Errorf("metrics did not survive: %+v", loaded.Runs[0].Metrics)
	}

	portfolios, err := loaded.BundleToPortfolios()
	if err != nil {
		t.Fatalf("BundleToPortfolios: %v", err)
	}
	if len(portfolios) != 1 {
		t.Fatalf("rebuilt %d portfolios, want 1", len(portfolios))
	}
	p := portfolios[0]
	if p.Pname != "sample" || p.InitialBuyingPower != 10000 ||
		len(p.Tickers) != 2 || p.StrategySpec != "buyAndHold:equalWeights" {
		t.Errorf("rebuilt portfolio does not match the bundle: %+v", p)
	}
}

// A future format must be refused rather than misread — silently interpreting a
// field that changed meaning is how a verification lies.
func TestUnknownVersionIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.json")
	if err := os.WriteFile(path,
		[]byte(`{"version": 99, "runs": []}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadBundle(path); err == nil {
		t.Error("a version 99 bundle was accepted")
	}
}

// A run in the bundle that produced no fresh result is missing, not reproduced:
// silence must never read as success.
func TestMissingRunIsNotAReproduction(t *testing.T) {
	b := Bundle{Version: BundleVersion, Runs: []BundleRun{sampleRun()}}
	report := b.Verify("x.json", nil)
	if report.Reproduced {
		t.Error("a bundle with no fresh results reported as reproduced")
	}
	if len(report.Missing) != 1 || report.Missing[0] != "sample" {
		t.Errorf("missing runs = %v, want [sample]", report.Missing)
	}
	if !strings.Contains(report.String(), "MISSING") {
		t.Errorf("report does not mention the missing run: %q", report.String())
	}
}

// The report's text has to state the verdict, since that is what a user reads.
func TestVerifyReportStatesTheVerdict(t *testing.T) {
	run := sampleRun()
	b := Bundle{Version: BundleVersion, Runs: []BundleRun{run}}

	good := b.Verify("x.json", []Result{matchingResult(run)})
	if !good.Reproduced || !strings.Contains(good.String(), "reproduced: every figure matches") {
		t.Errorf("a clean verification does not say so: %q", good.String())
	}

	drifted := matchingResult(run)
	drifted.FinalValue *= 1.5
	bad := b.Verify("x.json", []Result{drifted})
	if bad.Reproduced {
		t.Error("a drifted verification reported as reproduced")
	}
	if !strings.Contains(bad.String(), "DRIFT") {
		t.Errorf("report does not flag the drift: %q", bad.String())
	}
}
