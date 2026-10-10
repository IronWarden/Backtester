package backtest

// The registry's value is that it cannot be quietly edited: a re-run is a new
// row, not an overwrite, and the repetition is the signal. So the tests are
// about identity (does the same experiment hash the same?) and about
// append-only actually being append-only.

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func hashPortfolio(t *testing.T, edit func(*Portfolio)) string {
	t.Helper()
	p, err := InitializePortfolio(10000, exactEpoch,
		exactEpoch.AddDate(1, 0, 0), "hash-test", []string{"AAA", "BBB"},
		"buyAndHold:equalWeights", map[string]any{"period": int64(14)})
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	if edit != nil {
		edit(p)
	}
	return ConfigHash(p)
}

// The same experiment must hash the same, every time and in every process —
// otherwise "have I run this before?" can never be answered.
func TestConfigHashIsStable(t *testing.T) {
	first := hashPortfolio(t, nil)
	for i := 0; i < 20; i++ {
		if got := hashPortfolio(t, nil); got != first {
			t.Fatalf("hash changed between identical configs: %s vs %s",
				first, got)
		}
	}
	if len(first) != 24 {
		t.Errorf("hash %q is %d chars, want a stable short digest",
			first, len(first))
	}
}

// Every field that changes the experiment must change the hash. A config knob
// that hashes the same as its neighbour makes two different runs look like a
// repetition of one.
func TestConfigHashChangesWithTheConfig(t *testing.T) {
	base := hashPortfolio(t, nil)

	cases := map[string]func(*Portfolio){
		"strategy":   func(p *Portfolio) { p.StrategySpec = "buyAndHold:greedy" },
		"tickers":    func(p *Portfolio) { p.Tickers = []string{"AAA", "CCC"} },
		"start date": func(p *Portfolio) { p.StartTime = exactEpoch.AddDate(0, 1, 0) },
		"end date":   func(p *Portfolio) { p.EndTime = exactEpoch.AddDate(2, 0, 0) },
		"capital":    func(p *Portfolio) { p.InitialBuyingPower = 20000 },
		"benchmark":  func(p *Portfolio) { p.Benchmark = "$SP500" },
		"costs":      func(p *Portfolio) { p.Costs = CostConfig{SlippageBps: 5} },
		"a param":    func(p *Portfolio) { p.StrategyParams["period"] = int64(21) },
		"a new param": func(p *Portfolio) {
			p.StrategyParams["extra"] = 1.0
		},
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			if got := hashPortfolio(t, edit); got == base {
				t.Errorf("changing the %s did not change the hash", name)
			}
		})
	}
}

// Ticker ORDER is part of the experiment, not incidental: buyAndHold:greedy
// spends everything on the first ticker, so reordering the list is a different
// run and must not be recorded as a repeat of the same one.
func TestConfigHashRespectsTickerOrder(t *testing.T) {
	forward := hashPortfolio(t, func(p *Portfolio) {
		p.Tickers = []string{"AAA", "BBB"}
	})
	reversed := hashPortfolio(t, func(p *Portfolio) {
		p.Tickers = []string{"BBB", "AAA"}
	})
	if forward == reversed {
		t.Error("reordering the tickers hashed the same; greedy allocation " +
			"makes order a real difference")
	}
}

// Params live in a map, whose iteration order is randomised by Go. The hash must
// not be.
func TestConfigHashIgnoresParamMapOrder(t *testing.T) {
	var seen string
	for i := 0; i < 50; i++ {
		got := hashPortfolio(t, func(p *Portfolio) {
			p.StrategyParams = map[string]any{
				"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0,
			}
		})
		if seen == "" {
			seen = got
			continue
		}
		if got != seen {
			t.Fatalf("param map iteration order leaked into the hash: "+
				"%s vs %s", seen, got)
		}
	}
}

func openTestRegistry(t *testing.T) *Registry {
	t.Helper()
	reg, err := OpenRegistry(filepath.Join(t.TempDir(), "research.db"))
	if err != nil {
		t.Fatalf("OpenRegistry: %v", err)
	}
	t.Cleanup(func() { reg.Close() })
	return reg
}

func testRun(t *testing.T) (*Portfolio, Result) {
	t.Helper()
	p, err := InitializePortfolio(10000, exactEpoch,
		exactEpoch.AddDate(1, 0, 0), "run-test", []string{"AAA"},
		"buyAndHold:equalWeights", nil)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	res := Result{
		PortfolioName: "run-test",
		Strategy:      "buyAndHold:equalWeights",
		Metrics:       Metrics{SharpeRatio: 1.25, AnnualReturn: 0.11},
		FinalValue:    12000,
		Trials:        6,
		TrialGroup:    "grp",
		Baseline:      BaselineStats{Beat: true, ExcessFinalValue: 500},
		Significance:  Significance{PValue: 0.04},
		TradeStats:    TradeStats{Trades: 3, WinRate: 0.66},
	}
	return p, res
}

// Append-only, which is the whole discipline: running the same config twice
// leaves two rows sharing a hash, and that repetition is what stops you quietly
// retrying until something works.
func TestRegistryIsAppendOnly(t *testing.T) {
	reg := openTestRegistry(t)
	p, res := testRun(t)

	for i := 0; i < 3; i++ {
		if err := reg.Record("campaign-a", []*Portfolio{p}, []Result{res}); err != nil {
			t.Fatalf("Record %d: %v", i, err)
		}
	}

	count, last, err := reg.PriorRuns(ConfigHash(p))
	if err != nil {
		t.Fatalf("PriorRuns: %v", err)
	}
	if count != 3 {
		t.Errorf("recorded 3 identical runs, PriorRuns says %d", count)
	}
	if time.Since(last) > time.Hour {
		t.Errorf("last seen %v, want ~now", last)
	}

	// And the campaign total is the denominator T23 needs.
	trials, err := reg.CampaignTrials("campaign-a")
	if err != nil {
		t.Fatalf("CampaignTrials: %v", err)
	}
	if trials != 3 {
		t.Errorf("campaign trials = %d, want 3", trials)
	}
}

// A config never recorded must report zero rather than an error, so "first time
// trying this" is a normal answer.
func TestPriorRunsOnAnUnseenConfig(t *testing.T) {
	reg := openTestRegistry(t)
	count, _, err := reg.PriorRuns("never-seen")
	if err != nil {
		t.Fatalf("PriorRuns: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d for an unrecorded config, want 0", count)
	}
}

// The metrics have to survive the round trip, or the log is a list of names.
func TestRecordedRunReadsBack(t *testing.T) {
	reg := openTestRegistry(t)
	p, res := testRun(t)
	if err := reg.Record("camp", []*Portfolio{p}, []Result{res}); err != nil {
		t.Fatalf("Record: %v", err)
	}

	lines, err := reg.RecentRuns(10)
	if err != nil {
		t.Fatalf("RecentRuns: %v", err)
	}
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	for _, want := range []string{"camp", "run-test", "1.25", "beat baseline"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("recorded line %q is missing %q", lines[0], want)
		}
	}
}

// A result with no matching portfolio must still record — the metrics are the
// valuable part, and dropping the row because the config could not be matched
// would lose the run entirely.
func TestRecordWithoutAMatchingPortfolio(t *testing.T) {
	reg := openTestRegistry(t)
	_, res := testRun(t)
	if err := reg.Record("camp", nil, []Result{res}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	lines, err := reg.RecentRuns(10)
	if err != nil || len(lines) != 1 {
		t.Fatalf("got %d lines (err %v), want 1", len(lines), err)
	}
}

// Metrics.AnnualReturn is already a PERCENTAGE — GetAnnualReturn multiplies by
// 100 before returning. A reader that scales it again turns a 50% year into
// 5041%, which is what the first version of RecentRuns printed.
func TestRecentRunsDoesNotRescaleTheAnnualReturn(t *testing.T) {
	reg := openTestRegistry(t)
	p, res := testRun(t)
	res.Metrics.AnnualReturn = 12.5 // twelve and a half percent, as stored
	if err := reg.Record("scale", []*Portfolio{p}, []Result{res}); err != nil {
		t.Fatalf("Record: %v", err)
	}

	lines, err := reg.RecentRuns(1)
	if err != nil || len(lines) != 1 {
		t.Fatalf("got %d lines (err %v)", len(lines), err)
	}
	if !strings.Contains(lines[0], "12.50%") {
		t.Errorf("line %q does not show 12.50%% — the stored percentage was "+
			"rescaled", lines[0])
	}
	if strings.Contains(lines[0], "1250") {
		t.Errorf("line %q shows a rescaled figure", lines[0])
	}
}

// resultWithSharpe is a run whose Sharpe and return moments are set, so the
// deflation has something real to work on.
func resultWithSharpe(name string, sharpe float64) Result {
	return Result{
		PortfolioName:  name,
		Strategy:       "buyAndHold:equalWeights",
		Metrics:        Metrics{SharpeRatio: sharpe},
		Trials:         1,
		TrialGroup:     name,
		EquityCurve:    make([]float64, 500),
		returnSkew:     0,
		returnKurtosis: 3,
	}
}

// The point of the whole task: twenty sequential single-strategy runs in one
// campaign have spent twenty trials, and the winner must be judged against
// twenty — not against the one trial its own sweep counted. Per-sweep deflation
// calls each of them untested.
func TestCampaignDeflationCountsEveryRun(t *testing.T) {
	reg := openTestRegistry(t)

	// Twenty runs with a real spread of outcomes, recorded one at a time the
	// way a user actually iterates.
	for i := 0; i < 20; i++ {
		sharpe := 0.2 + float64(i)*0.08
		res := resultWithSharpe("try", sharpe)
		if err := reg.Record("hunt", nil, []Result{res}); err != nil {
			t.Fatalf("Record %d: %v", i, err)
		}
	}

	trials, bar, err := reg.CampaignDeflation("hunt")
	if err != nil {
		t.Fatalf("CampaignDeflation: %v", err)
	}
	if trials != 20 {
		t.Errorf("campaign trials = %d, want 20", trials)
	}
	if bar <= 0 {
		t.Errorf("campaign bar = %v after 20 varied trials, want a positive "+
			"threshold — twenty tries deserve a higher bar than one", bar)
	}

	// A single-trial sweep sets no bar at all, which is exactly the
	// understatement the campaign figure exists to correct.
	perSweep := ExpectedMaxSharpe(1, 0.5)
	if perSweep >= bar {
		t.Errorf("per-sweep bar %v is not below the campaign bar %v; counting "+
			"only one sweep's trials is the overstatement this fixes",
			perSweep, bar)
	}
}

// The bar rises with the number of trials at a fixed spread: more tries, more
// luck available, more to clear. Without this the correction would be decorative.
func TestCampaignBarRisesWithTrials(t *testing.T) {
	const spread = 0.4
	prev := ExpectedMaxSharpe(2, spread)
	for _, trials := range []int{5, 10, 50, 200, 2000} {
		bar := ExpectedMaxSharpe(trials, spread)
		if bar <= prev {
			t.Errorf("bar at %d trials (%v) is not above the previous (%v)",
				trials, bar, prev)
		}
		prev = bar
	}
}

// A campaign nobody has run has no history, and the first run of a new one must
// not be corrected against a spread that does not exist.
func TestCampaignDeflationOnAFreshCampaign(t *testing.T) {
	reg := openTestRegistry(t)
	trials, bar, err := reg.CampaignDeflation("brand-new")
	if err != nil {
		t.Fatalf("CampaignDeflation: %v", err)
	}
	if trials != 0 || bar != 0 {
		t.Errorf("fresh campaign reports %d trials and a bar of %v, want 0/0",
			trials, bar)
	}
}

// Campaigns are separate books: one campaign's trials must not raise another's
// bar, or every result would be corrected for work done on an unrelated question.
func TestCampaignsAreIndependent(t *testing.T) {
	reg := openTestRegistry(t)
	for i := 0; i < 30; i++ {
		if err := reg.Record("busy", nil,
			[]Result{resultWithSharpe("x", float64(i)*0.05)}); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}
	if err := reg.Record("quiet", nil,
		[]Result{resultWithSharpe("y", 1.0)}); err != nil {
		t.Fatalf("Record: %v", err)
	}

	busy, _, _ := reg.CampaignDeflation("busy")
	quiet, _, _ := reg.CampaignDeflation("quiet")
	if busy != 30 {
		t.Errorf("busy campaign trials = %d, want 30", busy)
	}
	if quiet != 1 {
		t.Errorf("quiet campaign trials = %d, want 1 — another campaign's "+
			"trials leaked in", quiet)
	}
}

// Each row stores the campaign trial count as it stood when the row was
// written. The log is append-only, so a row is a record of what was known then
// — and a row that stores only its own sweep's count would make the history
// unreadable later, which is the whole reason the column exists.
func TestRowStoresTheCampaignTrialCountAtWriteTime(t *testing.T) {
	reg := openTestRegistry(t)

	for i := 1; i <= 3; i++ {
		res := resultWithSharpe("try", 0.3+float64(i)*0.2)
		if err := reg.Record("growing", nil, []Result{res}); err != nil {
			t.Fatalf("Record %d: %v", i, err)
		}

		var stored int
		err := reg.db.QueryRow(`SELECT campaign_trials FROM runs
		    WHERE campaign = 'growing' ORDER BY recorded_at DESC, run_id DESC
		    LIMIT 1`).Scan(&stored)
		if err != nil {
			t.Fatalf("reading campaign_trials: %v", err)
		}
		if stored != i {
			t.Errorf("run %d stored campaign_trials = %d, want %d — the row "+
				"must count the campaign's history, not just this batch",
				i, stored, i)
		}
	}
}
