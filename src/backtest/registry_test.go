package backtest

// The registry's value is that it cannot be quietly edited: a re-run is a new
// row, not an overwrite, and the repetition is the signal. So the tests are
// about identity (does the same experiment hash the same?) and about
// append-only actually being append-only.

import (
	"path/filepath"
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
		if !contains(lines[0], want) {
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

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) &&
		(haystack == needle || len(needle) == 0 ||
			indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
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
	if !contains(lines[0], "12.50%") {
		t.Errorf("line %q does not show 12.50%% — the stored percentage was "+
			"rescaled", lines[0])
	}
	if contains(lines[0], "1250") {
		t.Errorf("line %q shows a rescaled figure", lines[0])
	}
}
