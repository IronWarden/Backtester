package backtest

// Walk-forward is the one place where a subtle indexing mistake produces a
// result that looks entirely plausible: an out-of-sample curve that quietly
// contains training days is indistinguishable from a good one by eye. These
// tests therefore pin the schedule arithmetic — which days land in which
// window, and which days reach the reported curve — rather than the metrics
// computed from it.

import (
	"math"
	"strings"
	"testing"
	"time"
)

// wfConfig builds a walk-forward config over a synthetic ticker, sweeping a
// parameter the strategy ignores. Ignoring it is the point for the schedule
// tests: every candidate then behaves identically, so window arithmetic is
// isolated from selection.
func wfConfig(n int, train, test, step int) *PortfolioConfig {
	return &PortfolioConfig{
		Name:        "wf",
		BuyingPower: exactCash,
		StartTime:   exactEpoch.Format("2006-01-02"),
		EndTime:     exactEpoch.AddDate(0, 0, n).Format("2006-01-02"),
		Tickers:     []string{"AAA"},
		Strategy:    "buyAndHold:equalWeights",
		Sweep:       map[string]any{"unused": []any{int64(1), int64(2)}},
		WalkForward: WalkForwardConfig{
			TrainDays: train, TestDays: test, StepDays: step,
		},
	}
}

// runWF drives the schedule over a synthetic price path, no database.
func runWF(t *testing.T, pc *PortfolioConfig, closes []float64) Result {
	t.Helper()
	schedule, err := pc.WalkForward.normalized()
	if err != nil {
		t.Fatalf("normalized: %v", err)
	}
	candidates, err := walkForwardCandidates(pc)
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	start, _ := time.Parse("2006-01-02", pc.StartTime)
	end, _ := time.Parse("2006-01-02", pc.EndTime)
	res, err := runWalkForwardOver(pc, schedule, candidates,
		histFrom(map[string][]float64{"AAA": closes}),
		map[int64]float64{}, start, end)
	if err != nil {
		t.Fatalf("runWalkForwardOver: %v", err)
	}
	return res
}

// The reported curve must contain the test days and ONLY the test days. With
// 300 bars, a 100-day train and a 50-day test stepping by 50, the schedule
// fits four windows and the first reported day is the 101st bar — never
// earlier, or a training day has leaked into the result.
func TestWalkForwardReportsOnlyUnseenDays(t *testing.T) {
	const bars = 300
	closes := rampCloses(bars, 1.0)
	res := runWF(t, wfConfig(bars, 100, 50, 50), closes)

	if res.WalkForward == nil {
		t.Fatal("no walk-forward record on the result")
	}
	windows := res.WalkForward.Windows
	// Window starts at 0, 50, 100, 150; the last needing index 150+150=300.
	if len(windows) != 4 {
		t.Fatalf("fitted %d windows, want 4", len(windows))
	}

	// Each test window costs its FIRST bar to entry: the engine records a
	// return only from the second day of a run onward, because day 0 is when
	// the position is opened and there is no prior close to measure from. So
	// a 50-bar test window contributes 49 scored days, and the first scored
	// day is one after the window opens. That is a real property of
	// restarting flat each window, not an off-by-one — but it does mean the
	// scored days do not literally tile the calendar.
	if got, want := len(res.Dates), 4*49; got != want {
		t.Errorf("curve holds %d days, want %d (4 windows x 49 scored)",
			got, want)
	}
	// Training ends on bar 99; bar 100 opens the first test window and bar
	// 101 is the first day it scores. Anything earlier is a training day
	// leaking into the reported result.
	firstScored := exactEpoch.AddDate(0, 0, 101).Format("2006-01-02")
	if res.Dates[0] != firstScored {
		t.Errorf("curve starts %s, want %s — an earlier start means training "+
			"days reached the reported result", res.Dates[0], firstScored)
	}
	trainEnd := exactEpoch.AddDate(0, 0, 99).Format("2006-01-02")
	if res.Dates[0] <= trainEnd {
		t.Errorf("curve starts %s, on or before the first training window "+
			"ended (%s)", res.Dates[0], trainEnd)
	}

	// The test segments must tile: no day repeated, none skipped.
	seen := map[string]bool{}
	for _, d := range res.Dates {
		if seen[d] {
			t.Errorf("day %s appears twice in the out-of-sample curve", d)
		}
		seen[d] = true
	}

	// And each window's declared test range must abut the next window's.
	for i := 1; i < len(windows); i++ {
		if windows[i].TestStart <= windows[i-1].TestEnd {
			t.Errorf("window %d starts %s, on or before window %d's end %s",
				i, windows[i].TestStart, i-1, windows[i-1].TestEnd)
		}
	}
}

// Capital carries from one window into the next, so the concatenated curve is
// a real running balance rather than a chain of independent experiments each
// restarted at the opening stake.
func TestWalkForwardCarriesCapitalBetweenWindows(t *testing.T) {
	const scored = 49 // 50-bar test window, first bar is the entry
	closes := rampCloses(300, 1.0)
	res := runWF(t, wfConfig(300, 100, 50, 50), closes)
	windows := res.WalkForward.Windows

	almost(t, "first window capital", windows[0].Capital, exactCash)
	for i := 1; i < len(windows); i++ {
		// The balance a window opens with is the balance the previous
		// window's last scored day closed at.
		closedAt := res.EquityCurve[i*scored-1]
		almost(t, "carried capital", windows[i].Capital, closedAt)
	}

	// And the account genuinely moved, or the check above is vacuous.
	if windows[len(windows)-1].Capital == exactCash {
		t.Error("capital never changed across the schedule; this fixture " +
			"cannot detect whether it carries")
	}
}

// Train and test windows must not overlap: the test window begins strictly
// after its own training window ends, or the selection has seen the days it
// is being scored on.
func TestWalkForwardTestWindowFollowsItsTraining(t *testing.T) {
	closes := rampCloses(300, 1.0)
	res := runWF(t, wfConfig(300, 100, 50, 50), closes)

	for i, w := range res.WalkForward.Windows {
		if w.TestStart <= w.TrainEnd {
			t.Errorf("window %d: test starts %s but training ran to %s",
				i, w.TestStart, w.TrainEnd)
		}
		if w.TrainStart >= w.TrainEnd {
			t.Errorf("window %d: training range %s..%s is empty or inverted",
				i, w.TrainStart, w.TrainEnd)
		}
	}
}

// selectBest must maximize, not take the first thing it is handed. The winner
// sits LAST here deliberately: an implementation that returns candidate 0
// passes any fixture where the best candidate happens to sort first, which is
// exactly the mistake an earlier version of this file made.
func TestSelectBestMaximizesTheObjective(t *testing.T) {
	mk := func(sharpe, sortino, annual float64) *Portfolio {
		return &Portfolio{
			DailyReturns: []DailyReturn{{Return: 0.01}},
			Metrics: Metrics{
				SharpeRatio:  sharpe,
				SortinoRatio: sortino,
				AnnualReturn: annual,
			},
		}
	}
	runs := []*Portfolio{
		mk(0.9, 3.0, 1.0), // best Sortino
		mk(0.2, 0.4, 9.0), // best AnnualReturn
		mk(1.7, 0.5, 2.0), // best Sharpe, and last
	}
	for objective, want := range map[string]int{
		"SharpeRatio":  2,
		"SortinoRatio": 0,
		"AnnualReturn": 1,
	} {
		if got := selectBest(runs, objective); got != want {
			t.Errorf("%s selected candidate %d, want %d", objective, got, want)
		}
	}

	// Candidates that produced no simulated days cannot be scored and must
	// be skipped rather than treated as a zero.
	withEmpty := []*Portfolio{{}, nil, mk(-5.0, -5.0, -5.0)}
	if got := selectBest(withEmpty, "SharpeRatio"); got != 2 {
		t.Errorf("selected %d; the only scorable candidate is 2", got)
	}
	if got := selectBest([]*Portfolio{nil, {}}, "SharpeRatio"); got != -1 {
		t.Errorf("selected %d with nothing scorable, want -1", got)
	}
	if got := selectBest(runs, "NotAnObjective"); got != -1 {
		t.Errorf("selected %d for an unknown objective, want -1", got)
	}
}

// End to end, the score recorded for each window must be the best score any
// candidate actually achieved on that window's training data. Recomputing the
// candidates independently keeps this true whatever the price fixture does,
// so it cannot pass because the winner happened to sort first.
func TestWalkForwardRecordsTheWinningTrainScore(t *testing.T) {
	closes := rampCloses(300, 1.0)
	pc := wfConfig(300, 100, 50, 50)
	// Sweep something the strategy actually responds to, so the candidates
	// differ: DCA that deploys everything at once versus a trickle.
	pc.Strategy = "lua:" + strategiesDir(t) + "/dca.lua"
	pc.Sweep = map[string]any{"amount": []any{25.0, exactCash}}
	pc.Params = map[string]any{"every_days": int64(5)}

	res := runWF(t, pc, closes)
	hist := histFrom(map[string][]float64{"AAA": closes})
	candidates, err := walkForwardCandidates(pc)
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}

	for i, w := range res.WalkForward.Windows {
		trainStart, _ := time.Parse("2006-01-02", w.TrainStart)
		trainEnd, _ := time.Parse("2006-01-02", w.TrainEnd)
		// At the window's own capital, not the starting capital: a parameter
		// denominated in dollars behaves differently at a different balance,
		// which is why the window records the capital it ran at.
		runs := runCandidates(candidates, hist, map[int64]float64{},
			trainStart, trainEnd, w.Capital)

		best, bestLabel := math.Inf(-1), ""
		for j, p := range runs {
			if p == nil || len(p.DailyReturns) == 0 {
				continue
			}
			if p.Metrics.SharpeRatio > best {
				best, bestLabel = p.Metrics.SharpeRatio, candidates[j].label
			}
		}
		if bestLabel == "" {
			t.Fatalf("window %d: no candidate was scorable", i)
		}
		if w.Selected != bestLabel {
			t.Errorf("window %d selected %s, but %s scored higher (%.4f)",
				i, w.Selected, bestLabel, best)
		}
		// TrainScore is recorded at the schedule's own running capital, and
		// Sharpe is scale-invariant, so it must match to the last bit.
		almost(t, "recorded train score", w.TrainScore, best)
	}
}

// Trials must count every simulation the schedule performed, since that is
// the size of the search the reported curve came out of and the input the
// deflated-Sharpe correction needs.
func TestWalkForwardCountsEverySimulationAsATrial(t *testing.T) {
	closes := rampCloses(300, 1.0)
	res := runWF(t, wfConfig(300, 100, 50, 50), closes)

	if want := 2 * 4; res.Trials != want {
		t.Errorf("Trials = %d, want %d (2 candidates x 4 windows)",
			res.Trials, want)
	}
	if !strings.Contains(res.PortfolioName, "walk-forward") {
		t.Errorf("result named %q; it must be distinguishable from an "+
			"ordinary run", res.PortfolioName)
	}
}

// A schedule that cannot fit even one window, or is otherwise unusable, must
// say so rather than return an empty result that reads as "no edge".
func TestWalkForwardRejectsUnusableSchedules(t *testing.T) {
	closes := rampCloses(300, 1.0)

	t.Run("window longer than the data", func(t *testing.T) {
		pc := wfConfig(300, 250, 100, 100)
		schedule, _ := pc.WalkForward.normalized()
		candidates, _ := walkForwardCandidates(pc)
		start, _ := time.Parse("2006-01-02", pc.StartTime)
		end, _ := time.Parse("2006-01-02", pc.EndTime)
		_, err := runWalkForwardOver(pc, schedule, candidates,
			histFrom(map[string][]float64{"AAA": closes}),
			map[int64]float64{}, start, end)
		if err == nil {
			t.Fatal("a schedule needing more days than exist was accepted")
		}
		if !strings.Contains(err.Error(), "trading days") {
			t.Errorf("error %q does not explain the shortfall", err)
		}
	})

	t.Run("no sweep to select from", func(t *testing.T) {
		pc := wfConfig(300, 100, 50, 50)
		pc.Sweep = nil
		if _, err := walkForwardCandidates(pc); err == nil {
			t.Fatal("walk-forward without candidates was accepted")
		}
	})

	for _, tc := range []struct {
		name string
		cfg  WalkForwardConfig
		want string
	}{
		{"train too short", WalkForwardConfig{TrainDays: 5, TestDays: 50}, "train_days"},
		{"test too short", WalkForwardConfig{TrainDays: 100, TestDays: 5}, "test_days"},
		{"unknown objective", WalkForwardConfig{
			TrainDays: 100, TestDays: 50, Objective: "Profit"}, "objective"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.cfg.normalized()
			if err == nil {
				t.Fatalf("%+v was accepted", tc.cfg)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// step_days defaults to test_days, which is the only value that tiles the
// scored segments with no overlap and no gap.
func TestWalkForwardStepDefaultsToTileTheTestWindows(t *testing.T) {
	got, err := WalkForwardConfig{TrainDays: 100, TestDays: 50}.normalized()
	if err != nil {
		t.Fatalf("normalized: %v", err)
	}
	if got.StepDays != 50 {
		t.Errorf("step_days defaulted to %d, want 50", got.StepDays)
	}
	if got.Objective != "SharpeRatio" {
		t.Errorf("objective defaulted to %q, want SharpeRatio", got.Objective)
	}
}
