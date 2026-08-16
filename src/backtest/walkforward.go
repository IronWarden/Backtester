package backtest

// Walk-forward evaluation.
//
// A sweep (T15) searches a parameter space over one window and reports the
// best result, which is the definition of an in-sample figure. A single split
// (T16) scores that winner once on held-out data. Walk-forward is the same
// idea applied repeatedly: select on a training window, score on the window
// that follows, step forward, and concatenate the scored segments into one
// equity curve that was never used for selection.
//
// Nothing here is "fitted" in the statistical sense — strategies take
// parameters rather than estimating them — so selection IS the fitting step,
// which is why this requires a Sweep to have something to choose between.
//
// This file deliberately does not touch runner.go's worker pool. It reuses
// runOne, the same per-day simulation the normal path uses, and does its own
// bounded fan-out over candidates.

import (
	"fmt"
	"log"
	"my-backtester/src/data"
	"runtime"
	"sync"
	"time"
)

// WalkForwardWindow records one train/test cycle: what was selected, on what
// evidence, and how the selection then did on days it had never seen.
//
// The training figures are kept alongside the test ones on purpose. The gap
// between them is the whole diagnostic — a schedule where training scores are
// excellent and test scores are not is describing overfitting, and that is
// only visible when both are reported.
type WalkForwardWindow struct {
	TrainStart string `json:"trainStart"`
	TrainEnd   string `json:"trainEnd"`
	TestStart  string `json:"testStart"`
	TestEnd    string `json:"testEnd"`
	// Selected is the winning candidate's parameter label, e.g.
	// "[buy_thresh=20 period=7]".
	Selected string `json:"selected"`
	// TrainScore is the objective's value for the winner on the training
	// window; TestReturn is what it then returned on the test window, in %.
	TrainScore float64 `json:"trainScore"`
	TestReturn float64 `json:"testReturn"`
	// Candidates is how many parameter sets were compared on this window.
	Candidates int `json:"candidates"`
	// Capital is the account balance the window opened with, and the balance
	// the candidates were simulated at. It matters because a parameter
	// denominated in dollars rather than proportions — DCA's amount, say —
	// behaves differently at different balances, so a window's scores are
	// only reproducible together with the capital that produced them.
	Capital float64 `json:"capital"`
}

// WalkForwardResult is one complete schedule: the concatenated out-of-sample
// curve and the per-window record of how it was produced.
type WalkForwardResult struct {
	// Objective is the figure the selection maximized, recorded because a
	// walk-forward whose objective is unstated cannot be reproduced.
	Objective string              `json:"objective"`
	Windows   []WalkForwardWindow `json:"windows"`
}

// wfPortfolio pairs a candidate portfolio with the sweep label that names it.
type wfPortfolio struct {
	config *PortfolioConfig
	label  string
}

// walkForwardCandidates expands the config's sweep into the candidate set the
// schedule selects from. Walk-forward without candidates is just a backtest
// with extra steps, so an absent sweep is an error rather than a one-candidate
// schedule.
func walkForwardCandidates(pc *PortfolioConfig) ([]wfPortfolio, error) {
	combos, err := sweepCombos(pc.Sweep)
	if err != nil {
		return nil, err
	}
	if len(combos) < 2 {
		return nil, fmt.Errorf(
			"WalkForward needs a [portfolio.Sweep] with at least two " +
				"combinations to select between; with one candidate there is " +
				"nothing to select and a plain backtest says the same thing")
	}
	out := make([]wfPortfolio, 0, len(combos))
	for _, combo := range combos {
		sub := *pc
		sub.Params = mergeParams(pc.Params, combo)
		// The schedule sets its own dates per window, and no sub-run should
		// inherit a split or a nested walk-forward.
		sub.Validation = ValidationConfig{}
		sub.WalkForward = WalkForwardConfig{}
		sub.Sweep = nil
		out = append(out, wfPortfolio{config: &sub, label: sweepLabel(combo)})
	}
	return out, nil
}

// tradingDays returns the calendar the schedule is measured in: the days
// common to every one of the portfolio's tickers inside its window. Counting
// windows in trading days rather than calendar days keeps a schedule meaning
// the same thing across periods with different holiday counts.
func tradingDays(
	hist map[string][]data.AssetData, tickers []string, start, end time.Time,
) []time.Time {
	windowed, n := alignToWindow(hist, tickers, start, end)
	if n == 0 {
		return nil
	}
	days := make([]time.Time, n)
	for i, bar := range windowed[tickers[0]] {
		days[i] = bar.Date
	}
	return days
}

// runCandidates simulates every candidate over one window and returns their
// finished portfolios, in the same order. Candidates are independent, so this
// fans out across CPUs; each gets its own portfolio built from the config, so
// nothing is shared but the read-only history.
func runCandidates(
	candidates []wfPortfolio,
	hist map[string][]data.AssetData,
	riskFreeRates map[int64]float64,
	start, end time.Time,
	capital float64,
) []*Portfolio {
	out := make([]*Portfolio, len(candidates))
	sem := make(chan struct{}, runtime.NumCPU())
	var wg sync.WaitGroup

	for i, c := range candidates {
		wg.Add(1)
		go func(i int, c wfPortfolio) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			sub := *c.config
			sub.BuyingPower = capital
			sub.StartTime = start.Format("2006-01-02")
			sub.EndTime = end.Format("2006-01-02")
			p, err := sub.ToPortfolio()
			if err != nil {
				log.Printf("walk-forward candidate %s: %v", c.label, err)
				return
			}
			runOne(p, hist, riskFreeRates)
			out[i] = p
		}(i, c)
	}
	wg.Wait()
	return out
}

// selectBest returns the index of the candidate with the highest objective
// value, or -1 when none produced a scorable run. Ties go to the earliest
// candidate, which combined with the sweep's sorted expansion makes the
// choice deterministic — a walk-forward whose selection could differ between
// identical runs would not be reproducible.
func selectBest(runs []*Portfolio, objective string) int {
	score, ok := walkForwardObjectives[objective]
	if !ok {
		return -1
	}
	best, bestIdx := 0.0, -1
	for i, p := range runs {
		if p == nil || len(p.DailyReturns) == 0 {
			continue
		}
		v := score(p.Metrics)
		if bestIdx == -1 || v > best {
			best, bestIdx = v, i
		}
	}
	return bestIdx
}

// RunWalkForward executes one config block's rolling schedule and returns a
// single Result whose equity curve is the concatenation of the test segments
// only — the days no selection ever saw.
//
// Capital carries forward between windows, so the curve is a real account
// balance rather than a chain of independent experiments. Positions do not:
// each window starts flat, because the selection may change and the new
// parameters are not entitled to the old one's holdings. That boundary
// liquidation is inherent to walk-forward and is worth remembering when
// reading turnover.
func RunWalkForward(pc *PortfolioConfig) (Result, error) {
	schedule, err := pc.WalkForward.normalized()
	if err != nil {
		return Result{}, err
	}
	candidates, err := walkForwardCandidates(pc)
	if err != nil {
		return Result{}, err
	}

	start, err := time.Parse("2006-01-02", pc.StartTime)
	if err != nil {
		return Result{}, err
	}
	end, err := time.Parse("2006-01-02", pc.EndTime)
	if err != nil {
		return Result{}, err
	}

	tickers := pc.Tickers
	if len(tickers) == 0 {
		return Result{}, fmt.Errorf("portfolio %q has no tickers", pc.Name)
	}
	fetch := append([]string{}, tickers...)
	if pc.Benchmark != "" {
		fetch = append(fetch, pc.Benchmark)
	}
	hist := data.QueryAssetsForTickers(fetch, start, end)
	riskFreeRates := data.GetRiskFreeRates(start, end)
	return runWalkForwardOver(pc, schedule, candidates, hist, riskFreeRates,
		start, end)
}

// runWalkForwardOver is RunWalkForward with the data already in hand. The
// split exists so the schedule logic — which is where the subtle mistakes
// live — can be exercised against synthetic history without a database.
func runWalkForwardOver(
	pc *PortfolioConfig,
	schedule WalkForwardConfig,
	candidates []wfPortfolio,
	hist map[string][]data.AssetData,
	riskFreeRates map[int64]float64,
	start, end time.Time,
) (Result, error) {
	tickers := pc.Tickers
	days := tradingDays(hist, tickers, start, end)
	needed := schedule.TrainDays + schedule.TestDays
	if len(days) < needed {
		return Result{}, fmt.Errorf(
			"walk-forward needs at least %d trading days (%d train + %d "+
				"test) but the window holds %d",
			needed, schedule.TrainDays, schedule.TestDays, len(days))
	}

	var (
		windows    []WalkForwardWindow
		returns    []DailyReturn
		curve      []float64
		capital    = pc.BuyingPower
		firstValue = pc.BuyingPower
	)

	for i := 0; i+needed <= len(days); i += schedule.StepDays {
		windowCapital := capital
		trainStart, trainEnd := days[i], days[i+schedule.TrainDays-1]
		testStart := days[i+schedule.TrainDays]
		testEnd := days[i+needed-1]

		// Selection: every candidate on the training window. Its capital is
		// the running balance so the objective is scored on the same scale
		// the account is actually at, though every ratio metric is
		// scale-invariant anyway.
		trained := runCandidates(
			candidates, hist, riskFreeRates, trainStart, trainEnd, capital)
		bestIdx := selectBest(trained, schedule.Objective)
		if bestIdx < 0 {
			log.Printf(
				"walk-forward %q: no candidate produced a scorable run on "+
					"%s..%s; skipping the window",
				pc.Name, trainStart.Format("2006-01-02"),
				trainEnd.Format("2006-01-02"))
			continue
		}

		// Scoring: the winner alone, on days no selection has seen.
		tested := runCandidates(
			candidates[bestIdx:bestIdx+1], hist, riskFreeRates,
			testStart, testEnd, capital)
		if tested[0] == nil || len(tested[0].DailyReturns) == 0 {
			log.Printf(
				"walk-forward %q: winner %s produced no test days on %s..%s",
				pc.Name, candidates[bestIdx].label,
				testStart.Format("2006-01-02"), testEnd.Format("2006-01-02"))
			continue
		}
		p := tested[0]

		segmentGrowth := 1.0
		for _, dr := range p.DailyReturns {
			segmentGrowth *= 1 + dr.Return
		}
		returns = append(returns, p.DailyReturns...)
		for _, v := range p.PortfolioCloseValues {
			curve = append(curve, v)
		}
		if n := len(p.PortfolioCloseValues); n > 0 {
			capital = p.PortfolioCloseValues[n-1]
		}

		windows = append(windows, WalkForwardWindow{
			TrainStart: trainStart.Format("2006-01-02"),
			TrainEnd:   trainEnd.Format("2006-01-02"),
			TestStart:  testStart.Format("2006-01-02"),
			TestEnd:    testEnd.Format("2006-01-02"),
			Selected:   candidates[bestIdx].label,
			TrainScore: walkForwardObjectives[schedule.Objective](
				trained[bestIdx].Metrics),
			TestReturn: (segmentGrowth - 1) * 100,
			Candidates: len(candidates),
			Capital:    windowCapital,
		})
	}

	if len(windows) == 0 {
		return Result{}, fmt.Errorf(
			"walk-forward %q produced no scorable windows", pc.Name)
	}
	return assembleWalkForward(pc, schedule, windows, returns, curve,
		firstValue, riskFreeRates, len(candidates)), nil
}

// assembleWalkForward turns the concatenated out-of-sample segments into a
// single Result, scored exactly as any other run is.
//
// Trials counts every simulation the schedule performed — candidates times
// windows — because that is the size of the search the reported curve came
// out of, and it is what the deflated-Sharpe correction needs.
func assembleWalkForward(
	pc *PortfolioConfig,
	schedule WalkForwardConfig,
	windows []WalkForwardWindow,
	returns []DailyReturn,
	curve []float64,
	initial float64,
	riskFreeRates map[int64]float64,
	candidates int,
) Result {
	raw := make([]float64, len(returns))
	excess := make([]float64, len(returns))
	dates := make([]string, len(returns))
	for i, dr := range returns {
		raw[i] = dr.Return
		excess[i] = dr.Return - riskFreeRates[dr.Date.Unix()]
		dates[i] = dr.Date.Format("2006-01-02")
	}
	numYears := 0.0
	if n := len(returns); n > 1 {
		numYears = returns[n-1].Date.Sub(returns[0].Date).Hours() / 24 / 365.25
	}
	final := initial
	if n := len(curve); n > 0 {
		final = curve[n-1]
	}
	return Result{
		PortfolioName: pc.Name + " [walk-forward]",
		Strategy:      pc.Strategy,
		Metrics: Metrics{
			SharpeRatio:  GetSharpeRatio(excess),
			SortinoRatio: GetSortinoRatio(excess),
			MaxDrawdown:  GetMaxDrawdown(curve),
			AnnualReturn: GetAnnualReturn(raw, numYears),
			StandardDev:  stddev(raw),
		},
		InitialCapital: initial,
		FinalValue:     final,
		EquityCurve:    curve,
		Dates:          dates,
		Trials:         candidates * len(windows),
		TrialGroup:     pc.Name + " [walk-forward]",
		WalkForward: &WalkForwardResult{
			Objective: schedule.Objective,
			Windows:   windows,
		},
	}
}
