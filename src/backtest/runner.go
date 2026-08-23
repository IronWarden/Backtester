package backtest

import (
	"fmt"
	"log"
	"my-backtester/src/data"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"gonum.org/v1/gonum/stat"
)

// Result holds the result of a backtest.
type Result struct {
	PortfolioName string
	Strategy      string
	Metrics       Metrics
	// InitialCapital is the cash the portfolio started with and FinalValue is
	// what it was worth on the last simulated day, so FinalValue-InitialCapital
	// is the profit in dollars. The baseline is the starting cash rather than
	// EquityCurve[0], which is already the day-1 close and so omits the first
	// day's move.
	InitialCapital float64
	FinalValue     float64
	// EquityCurve is the portfolio's daily total value, and Dates are the
	// matching trading days (YYYY-MM-DD) in the same order. Both come from
	// the per-day record kept during the simulation and are 1:1 in length,
	// so the frontend can plot value-over-time directly.
	EquityCurve []float64
	Dates       []string
	// Time-sliced views of the same run, derived from EquityCurve and the
	// daily returns: the deepest drawdowns with their recovery, a rolling
	// Sharpe series, and compounded calendar-year and month returns. These
	// are nested rather than scalar, so the reporter (which is table-shaped)
	// does not carry them; they exist for the UI.
	Drawdowns      []Drawdown
	RollingSharpe  []RollingPoint
	YearlyReturns  []PeriodReturn
	MonthlyReturns []PeriodReturn
	// BenchmarkCurve is the configured Benchmark's value over the same days,
	// compounded from InitialCapital so it shares an axis and an origin with
	// EquityCurve, and 1:1 with Dates. BenchmarkStats is what the benchmark
	// returned on its own terms, as opposed to the benchmark-relative fields
	// of Metrics, which describe the portfolio against it. Both are empty for
	// a portfolio with no Benchmark, or one whose benchmark does not cover
	// the window.
	BenchmarkCurve []float64
	BenchmarkStats BenchmarkStats
	// Baseline is what an equal-weight buy-and-hold of the SAME tickers would
	// have returned over the same days with the same costs — the comparison
	// that says whether the strategy was worth running at all. Zero-valued with
	// Computed false when no baseline could be built.
	Baseline BaselineStats
	// TradeStats is the blotter's summary: win rate, profit factor, holding
	// periods, and the per-ticker P&L attribution that answers whether an edge
	// was broad or was one lucky position.
	TradeStats TradeStats
	// Significance answers "is this one result better than chance", by
	// comparing the strategy against random timing with the same exposure.
	Significance Significance
	// Robustness holds the cost- and start-date-sensitivity battery, empty
	// unless it was asked for.
	Robustness Robustness
	// Regimes slices the run's returns by the market environment its universe
	// was in — the answer to "did this work everywhere, or in one stretch?"
	Regimes RegimeBreakdown
	// Trials is how many parameter sets the config block that produced this
	// result expanded to, and TrialGroup names that block. ExpectedMaxSharpe
	// is the Sharpe the best of those trials would be expected to show with
	// no edge at all, and DeflatedSharpe is the probability in [0,1] that
	// this result's true Sharpe beats that bar. For an unswept portfolio the
	// bar is 0 and DeflatedSharpe is the plain probabilistic Sharpe.
	Trials            int
	TrialGroup        string
	ExpectedMaxSharpe float64
	DeflatedSharpe    float64
	// returnSkew and returnKurtosis feed the deflated-Sharpe correction,
	// which is computed after every result is collected (it needs the spread
	// of Sharpes across the whole trial group). They are captured here in the
	// worker because that is the only place the raw daily returns exist.
	// Unexported: they are inputs to a metric, not a metric.
	returnSkew     float64
	returnKurtosis float64
	// WalkForward is set only on the synthetic result a rolling schedule
	// produces, and describes how it was produced: the objective the
	// selection maximized and one entry per train/test window. Nil for every
	// ordinary run.
	WalkForward *WalkForwardResult
	// Splits holds the in-sample and out-of-sample segments of this same run
	// when the config sets [portfolio.Validation], and is empty otherwise.
	// Two entries, in-sample first. The out-of-sample one is the figure that
	// carries information about a swept or hand-tuned parameter set.
	Splits []SegmentStats
}

// topDrawdowns is how many peak-to-trough declines a Result carries. Enough
// to see the shape of the bad periods, few enough to render as a table.
const topDrawdowns = 10

// rollingSharpeWindow is one trading year, matching the 252-day convention
// the metrics use.
const rollingSharpeWindow = 252

// dateRange returns the earliest StartTime and the latest EndTime across
// every portfolio. Panics if portfolios is empty.
func dateRange(portfolios []*Portfolio) (time.Time, time.Time) {
	minDate := portfolios[0].StartTime
	maxDate := portfolios[0].EndTime

	for _, p := range portfolios {
		if p.StartTime.Before(minDate) {
			minDate = p.StartTime
		}
		if p.EndTime.After(maxDate) {
			maxDate = p.EndTime
		}
	}
	return minDate, maxDate
}

// alignToWindow restricts the shared, union-range history to a single
// portfolio's [start, end] window and re-indexes its tickers onto the set of
// dates common to every ticker, so windowed[t][i] refers to the same calendar
// day for all tickers. It returns the aligned data (each series length N) and
// N. This is what lets the runner issue one union-range query yet simulate and
// score each portfolio strictly over its own dates; it also fixes index-based
// pairing for tickers whose histories start on different days. hist[t] is
// date-sorted (the query's ORDER BY), so the output stays date-sorted. hist is
// only read, so this is safe to call concurrently across worker goroutines.
func alignToWindow(
	hist map[string][]data.AssetData,
	tickers []string,
	start, end time.Time,
) (map[string][]data.AssetData, int) {
	if len(tickers) == 0 {
		return map[string][]data.AssetData{}, 0
	}

	// A zero bound means that side is open (use all available data).
	inWindow := func(d time.Time) bool {
		if !start.IsZero() && d.Before(start) {
			return false
		}
		if !end.IsZero() && d.After(end) {
			return false
		}
		return true
	}

	// Per-ticker date(Unix) -> bar, limited to the window, for O(1) membership.
	byDate := make([]map[int64]data.AssetData, len(tickers))
	for ti, t := range tickers {
		m := make(map[int64]data.AssetData, len(hist[t]))
		for _, ad := range hist[t] {
			if inWindow(ad.Date) {
				m[ad.Date.Unix()] = ad
			}
		}
		byDate[ti] = m
	}

	// Walk the first ticker's dates (the intersection is a subset of them) and
	// keep those present for every ticker.
	aligned := make(map[string][]data.AssetData, len(tickers))
	for _, t := range tickers {
		aligned[t] = make([]data.AssetData, 0, len(byDate[0]))
	}
	n := 0
	for _, ad := range hist[tickers[0]] {
		if !inWindow(ad.Date) {
			continue
		}
		key := ad.Date.Unix()
		inAll := true
		for ti := 1; ti < len(tickers); ti++ {
			if _, ok := byDate[ti][key]; !ok {
				inAll = false
				break
			}
		}
		if !inAll {
			continue
		}
		for ti, t := range tickers {
			aligned[t] = append(aligned[t], byDate[ti][key])
		}
		n++
	}
	return aligned, n
}

// windowCoverage reports what alignToWindow had to throw away. It keeps only
// the days every ticker has a bar for, so a holding whose series ends
// mid-window — a delisting, or a feed that went stale — shortens the whole
// backtest for every other holding, and a single missing interior bar drops
// that day for everyone. Both were silent until this existed.
//
// Returns the number of trading days at least one ticker has in the window
// (the union), the last such day, and for each ticker whose own final bar
// falls short of that day, the date its data ends. It changes no numbers; the
// caller logs it.
//
// Deliberately separate from alignToWindow rather than folded into it: the
// walk-forward driver realigns once per window and must not report the same
// shortfall dozens of times over.
func windowCoverage(
	hist map[string][]data.AssetData,
	tickers []string,
	start, end time.Time,
) (union int, lastDay time.Time, endsEarly map[string]time.Time) {
	inWindow := func(d time.Time) bool {
		if !start.IsZero() && d.Before(start) {
			return false
		}
		if !end.IsZero() && d.After(end) {
			return false
		}
		return true
	}

	days := make(map[int64]bool)
	last := make(map[string]time.Time, len(tickers))
	for _, t := range tickers {
		for _, ad := range hist[t] {
			if !inWindow(ad.Date) {
				continue
			}
			days[ad.Date.Unix()] = true
			if ad.Date.After(last[t]) {
				last[t] = ad.Date
			}
			if ad.Date.After(lastDay) {
				lastDay = ad.Date
			}
		}
	}

	endsEarly = make(map[string]time.Time)
	for _, t := range tickers {
		if last[t].Before(lastDay) {
			endsEarly[t] = last[t]
		}
	}
	return len(days), lastDay, endsEarly
}

// noOverlapMessage explains an empty run. A day is simulated only when every
// ticker has a bar for it, so the answer a reader needs is which ticker's
// history does not reach the window — which means listing what each one
// actually covers rather than only saying the intersection was empty.
func noOverlapMessage(p *Portfolio, hist map[string][]data.AssetData) string {
	const layout = "2006-01-02"
	ranges := make([]string, 0, len(p.Tickers))
	for _, t := range p.Tickers {
		bars := hist[t]
		if len(bars) == 0 {
			ranges = append(ranges, fmt.Sprintf("%q has no data at all", t))
			continue
		}
		ranges = append(ranges, fmt.Sprintf("%q covers %s..%s", t,
			bars[0].Date.Format(layout), bars[len(bars)-1].Date.Format(layout)))
	}
	window := "the configured window"
	if !p.StartTime.IsZero() && !p.EndTime.IsZero() {
		window = fmt.Sprintf("%s..%s",
			p.StartTime.Format(layout), p.EndTime.Format(layout))
	}
	return fmt.Sprintf(
		"portfolio %q: no trading day in %s is covered by every ticker, so "+
			"nothing was simulated — %s",
		p.Pname, window, strings.Join(ranges, "; "))
}

// logCoverageShortfall names the tickers responsible when a portfolio simulated
// fewer days than its window holds. Silence here was the whole problem: a
// delisted holding turned a ten-year backtest into a five-year one and reported
// the five-year metrics as if they answered the question that was asked.
func logCoverageShortfall(
	pname string,
	hist map[string][]data.AssetData,
	tickers []string,
	start, end time.Time,
	simulated int,
) {
	const layout = "2006-01-02"

	union, lastDay, endsEarly := windowCoverage(hist, tickers, start, end)
	if union <= simulated {
		return
	}

	early := make([]string, 0, len(endsEarly))
	for _, t := range tickers {
		if d, ok := endsEarly[t]; ok {
			early = append(early,
				fmt.Sprintf("%q ends %s", t, d.Format(layout)))
		}
	}

	msg := fmt.Sprintf(
		"portfolio %q: simulated %d of the %d trading days its window covers "+
			"(through %s) — a day is only simulated when every ticker has a "+
			"bar for it",
		pname, simulated, union, lastDay.Format(layout))
	if len(early) > 0 {
		msg += fmt.Sprintf("; %s, so the run was truncated there",
			strings.Join(early, ", "))
	} else {
		msg += "; the missing days are gaps inside the tickers' histories"
	}
	diagf("warn", pname, "%s", msg)
}

// runOne executes one full simulation pass over a single-strategy portfolio.
// The day loop lives here; the strategy decides what to do on each day. Data is
// clipped to the portfolio's own [StartTime, EndTime] window first, so metrics
// reflect that window regardless of other portfolios' ranges in the same run.
func runOne(
	p *Portfolio,
	hist map[string][]data.AssetData,
	riskFreeRates map[int64]float64,
) {
	// Both of these produce a result full of zeros. Left silent they read as a
	// strategy that returned nothing, which is a very different statement from
	// "this portfolio never ran" — and the zeros sort perfectly well next to
	// real results in a sweep.
	if len(p.Tickers) == 0 {
		diagf("error", p.Pname,
			"portfolio %q has no tickers, so nothing was simulated", p.Pname)
		return
	}
	windowed, dataLen := alignToWindow(hist, p.Tickers, p.StartTime, p.EndTime)
	if dataLen == 0 {
		diagf("error", p.Pname, "%s", noOverlapMessage(p, hist))
		return
	}
	logCoverageShortfall(p.Pname, hist, p.Tickers, p.StartTime, p.EndTime, dataLen)

	p.Strategy.Step(p, windowed, 0)
	prev := p.GetPortfolioValue(p.Tickers, windowed, 0)
	for day := 1; day < dataLen; day++ {
		// Before the strategy acts: the borrow is owed on what was held
		// overnight, not on what today's orders leave behind.
		p.chargeBorrow()
		p.Strategy.Step(p, windowed, day)
		// After the strategy has acted, so a cover it made itself counts, and
		// before the day is valued, so a forced cover is in the day's number.
		p.enforceMaintenanceMargin(windowed, day)
		curr := p.GetPortfolioValue(p.Tickers, windowed, day)
		p.AdjustPortfolioParameters(p.Tickers, windowed, day, prev, curr)
		prev = curr
	}
	// Before the metrics are computed, since it changes the final equity value.
	p.applyDelistingReturns(windowed, dataLen)
	p.GetBacktestingData(riskFreeRates, windowed, dataLen)
	// The benchmark is scored against the portfolio's own trading days, taken
	// from the aligned window. It is read from the unaligned prefetch, never
	// from windowed, because it is not one of p.Tickers and must not become
	// one — that slice is what drives allocation and valuation.
	dates := make([]time.Time, dataLen)
	for i, bar := range windowed[p.Tickers[0]] {
		dates[i] = bar.Date
	}
	p.applyFundamentalCoverage(dates)
	p.applyBenchmarkMetrics(hist, dates, riskFreeRates)
	p.applySplitMetrics(riskFreeRates)
	// After the portfolio's own metrics, since the comparison reads them.
	p.Baseline = equalWeightBaseline(p, windowed, dataLen, riskFreeRates)
	p.TradeStats = SummarizeTrades(p.Trades, p.Positions)
	p.applySignificance(windowed, dataLen)
	p.applyRegimes(windowed, dataLen)
	p.applyRobustness(windowed, dataLen, riskFreeRates)
	if c, ok := p.Strategy.(interface{ Close() }); ok {
		c.Close()
	}
}

// validateCoverage rejects any portfolio whose window is not fully covered by
// every one of its tickers' available data, so a backtest never runs (and
// silently produces empty/misleading metrics) over dates the data doesn't
// span. Errors name the offending ticker and its real range so the caller can
// fix the dates or the holdings.
func validateCoverage(portfolios []*Portfolio) error {
	const layout = "2006-01-02"

	tickerSet := make(map[string]bool)
	for _, p := range portfolios {
		for _, t := range p.Tickers {
			tickerSet[t] = true
		}
	}
	all := make([]string, 0, len(tickerSet))
	for t := range tickerSet {
		all = append(all, t)
	}

	ranges, err := data.TickerDateRanges(all)
	if err != nil {
		return fmt.Errorf("checking data coverage: %w", err)
	}

	for _, p := range portfolios {
		if len(p.Tickers) == 0 {
			return fmt.Errorf("portfolio %q has no tickers", p.Pname)
		}
		if p.StartTime.After(p.EndTime) {
			return fmt.Errorf(
				"portfolio %q: start date %s is after end date %s",
				p.Pname, p.StartTime.Format(layout), p.EndTime.Format(layout),
			)
		}
		var problems []string
		for _, t := range p.Tickers {
			r, ok := ranges[t]
			if !ok {
				problems = append(problems,
					fmt.Sprintf("%q has no price data in the database", t))
				continue
			}
			if p.StartTime.Before(r.Min) || p.EndTime.After(r.Max) {
				problems = append(problems, fmt.Sprintf(
					"%q only has data from %s to %s",
					t, r.Min.Format(layout), r.Max.Format(layout),
				))
			}
		}
		if len(problems) > 0 {
			return fmt.Errorf(
				"portfolio %q: requested window %s to %s is outside the "+
					"available data — %s",
				p.Pname, p.StartTime.Format(layout), p.EndTime.Format(layout),
				strings.Join(problems, "; "),
			)
		}
	}
	return nil
}

// Run executes every portfolio concurrently and always returns the
// collected results. If output is non-nil, results are also written to a
// file via the configured Reporter.

// prefetchFundamentals fetches every declared metric once, before the worker
// pool starts, and hands the same immutable slice to each portfolio that
// declared anything.
//
// Here rather than in a worker for two reasons: the raw table is 8.5M rows and
// one query beats N, and this runs single-threaded before any goroutine exists,
// so it adds nothing to the concurrency surface of the pool below.
//
// A portfolio declaring no Fundamentals is skipped entirely, so a config that
// does not use them performs exactly as it did.
func prefetchFundamentals(
	portfolios []*Portfolio, startTime, endTime time.Time,
) {
	tickerSet := make(map[string]bool)
	metricSet := make(map[string]bool)
	for _, p := range portfolios {
		if len(p.Fundamentals) == 0 {
			continue
		}
		for _, t := range p.Tickers {
			tickerSet[t] = true
		}
		for _, m := range p.Fundamentals {
			metricSet[m] = true
		}
		// market_cap() needs the share count whether or not the config named
		// it, since it is the one metric a user can need without knowing to
		// ask for it.
		metricSet[sharesMetric] = true
	}
	if len(tickerSet) == 0 || len(metricSet) == 0 {
		return
	}

	tickers := make([]string, 0, len(tickerSet))
	for t := range tickerSet {
		tickers = append(tickers, t)
	}
	metrics := make([]string, 0, len(metricSet))
	for m := range metricSet {
		metrics = append(metrics, m)
	}

	points := data.PointInTimeFundamentals(tickers, metrics, startTime, endTime)
	if len(points) == 0 {
		// Absent table, or nothing reported in the window. Every lookup then
		// returns nil and FundamentalCoverage reports 0, which is the honest
		// reading rather than a failure.
		diagf("warn", "",
			"no fundamentals were found for the declared metrics over %s..%s; "+
				"fundamental() will return nil throughout",
			startTime.Format("2006-01-02"), endTime.Format("2006-01-02"))
		return
	}
	for _, p := range portfolios {
		if len(p.Fundamentals) > 0 {
			p.FundamentalData = points
		}
	}
}

// prefetchDelistings loads the registry once, before the worker pool, for the
// portfolios that asked for a delisting return. Same reasoning as
// prefetchFundamentals: one query beats N, and it runs single-threaded before
// any goroutine exists so it adds nothing to the pool's concurrency surface.
func prefetchDelistings(portfolios []*Portfolio) {
	tickerSet := make(map[string]bool)
	for _, p := range portfolios {
		if p.DelistingReturn == 0 {
			continue
		}
		for _, t := range p.Tickers {
			tickerSet[t] = true
			// A generation symbol's death is recorded under its base company.
			if base, gen := data.ParseGeneration(t); gen > 0 {
				tickerSet[base] = true
			}
		}
	}
	if len(tickerSet) == 0 {
		return
	}
	tickers := make([]string, 0, len(tickerSet))
	for t := range tickerSet {
		tickers = append(tickers, t)
	}

	events := loadDelistingEvents(tickers)
	if len(events) == 0 {
		diagf("warn", "",
			"DelistingReturn is set but no delisting registry was found, so "+
				"no holding can be identified as dead and nothing is haircut; "+
				"run add_delistings.py to enable it")
		return
	}
	for _, p := range portfolios {
		if p.DelistingReturn != 0 {
			p.DelistingEvents = events
		}
	}
}

func Run(portfolios []*Portfolio, output *OutputConfig) ([]Result, error) {
	if len(portfolios) == 0 {
		return nil, fmt.Errorf("no portfolios to run")
	}
	if err := validateCoverage(portfolios); err != nil {
		return nil, err
	}

	reporter, err := NewReporter(output)
	if err != nil {
		return nil, fmt.Errorf("output config: %w", err)
	}

	startTime, endTime := dateRange(portfolios)
	riskFreeRates := data.GetRiskFreeRates(startTime, endTime)
	// One query for the union window, shared read-only by every worker, the
	// same way the risk-free series is. A few hundred monthly rows. Nil when
	// the table is absent, which leaves the real return unreported and changes
	// nothing else.
	cpi := data.GetCPISeries()
	for _, p := range portfolios {
		p.CPI = cpi
	}

	allTickersMap := make(map[string]bool)
	for _, p := range portfolios {
		for _, ticker := range p.Tickers {
			allTickersMap[ticker] = true
		}
		// Benchmarks are fetched alongside the holdings but are not holdings:
		// this is the only place they enter the run, and they never reach
		// p.Tickers.
		if p.Benchmark != "" {
			allTickersMap[p.Benchmark] = true
		}
	}
	allTickers := make([]string, 0, len(allTickersMap))
	for ticker := range allTickersMap {
		allTickers = append(allTickers, ticker)
	}

	historicalData := data.QueryAssetsForTickers(
		allTickers, startTime, endTime,
	)
	prefetchFundamentals(portfolios, startTime, endTime)
	prefetchDelistings(portfolios)

	numWorkers := runtime.NumCPU()
	totalJobs := len(portfolios)
	jobs := make(chan *Portfolio, totalJobs)
	results := make(chan Result, totalJobs)

	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				runOne(p, historicalData, riskFreeRates)
				// DailyReturns and PortfolioCloseValues are appended together
				// each day, so they share length and ordering.
				dates := make([]string, len(p.DailyReturns))
				dayTimes := make([]time.Time, len(p.DailyReturns))
				returns := make([]float64, len(p.DailyReturns))
				for i, dr := range p.DailyReturns {
					dates[i] = dr.Date.Format("2006-01-02")
					dayTimes[i] = dr.Date
					returns[i] = dr.Return
				}
				// A portfolio with no simulated days never traded, so it is
				// still worth exactly what it started with.
				finalValue := p.InitialBuyingPower
				if n := len(p.PortfolioCloseValues); n > 0 {
					finalValue = p.PortfolioCloseValues[n-1]
				}
				results <- Result{
					PortfolioName:  p.Pname,
					Strategy:       p.Strategy.Name(),
					Metrics:        p.Metrics,
					InitialCapital: p.InitialBuyingPower,
					FinalValue:     finalValue,
					EquityCurve:    p.PortfolioCloseValues,
					Dates:          dates,
					// EquityCurve and dayTimes are appended together each
					// day, so they share length and ordering.
					Drawdowns: GetDrawdowns(
						p.PortfolioCloseValues, dayTimes, topDrawdowns,
					),
					RollingSharpe: GetRollingSharpe(
						returns, dayTimes, rollingSharpeWindow,
					),
					YearlyReturns: GetCalendarReturns(
						p.PortfolioCloseValues, dayTimes, false,
					),
					MonthlyReturns: GetCalendarReturns(
						p.PortfolioCloseValues, dayTimes, true,
					),
					BenchmarkCurve: p.BenchmarkCurve,
					BenchmarkStats: p.BenchmarkStats,
					Baseline:       p.Baseline,
					TradeStats:     p.TradeStats,
					Significance:   p.Significance,
					Robustness:     p.Robustness,
					Regimes:        p.Regimes,
					Splits:         p.Splits,
					Trials:         p.Trials,
					TrialGroup:     p.TrialGroup,
					returnSkew:     stat.Skew(returns, nil),
					returnKurtosis: stat.ExKurtosis(returns, nil) + 3.0,
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, p := range portfolios {
			clone, err := p.Clone()
			if err != nil {
				log.Printf("clone portfolio %s: %v", p.Pname, err)
				continue
			}
			jobs <- clone
		}
	}()

	collected := make([]Result, 0, totalJobs)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		if reporter != nil {
			defer reporter.Close()
		}
		for result := range results {
			collected = append(collected, result)
			if reporter != nil {
				if werr := reporter.Write(result); werr != nil {
					log.Printf("Failed to write result: %v", werr)
				}
			}
		}
	}()

	wg.Wait()
	close(results)
	<-writerDone

	applyOverfittingStats(collected)
	return collected, nil
}

// applyOverfittingStats discounts each result by the size of the search that
// produced it. It runs after collection rather than inside a worker because
// the correction needs the SPREAD of Sharpe ratios across the whole trial
// group, which no single worker can see.
//
// Results are grouped by TrialGroup — the config block they expanded from —
// so two unrelated sweeps in one config do not deflate each other.
func applyOverfittingStats(results []Result) {
	byGroup := make(map[string][]int, len(results))
	for i, r := range results {
		byGroup[r.TrialGroup] = append(byGroup[r.TrialGroup], i)
	}
	for _, idxs := range byGroup {
		sharpes := make([]float64, len(idxs))
		for j, i := range idxs {
			sharpes[j] = results[i].Metrics.SharpeRatio
		}
		// stat.StdDev is NaN below two samples, which ExpectedMaxSharpe
		// treats as "no spread" and so as no bar to clear.
		spread := stat.StdDev(sharpes, nil)
		for _, i := range idxs {
			bar := ExpectedMaxSharpe(results[i].Trials, spread)
			results[i].ExpectedMaxSharpe = bar
			results[i].DeflatedSharpe = deflatedFromResult(&results[i], bar)
		}
	}
}

// deflatedFromResult applies the deflated-Sharpe correction using the
// skewness and kurtosis the worker captured from the raw daily returns.
// Reconstructing those from EquityCurve would be wrong once costs are on:
// the first day's return is measured from the post-trade value, not from
// InitialCapital.
func deflatedFromResult(r *Result, bar float64) float64 {
	n := len(r.EquityCurve)
	if n < 2 {
		return 0
	}
	return deflatedSharpeFromMoments(
		r.Metrics.SharpeRatio, bar, n, r.returnSkew, r.returnKurtosis,
	)
}

// RunFromConfigText decodes a TOML config from cfgText, initializes the DB
// at dbPath, and runs every configured portfolio. Portfolios that omit
// Strategy fall back to "lua:<defaultLuaPath>" so the UI's open Lua script
// acts as the default strategy. Designed as the entry point for callers
// (e.g. the UI) that hold the config as in-memory text.
func RunFromConfigText(cfgText, dbPath, defaultLuaPath string) ([]Result, error) {
	if _, err := data.InitDB(dbPath); err != nil {
		return nil, fmt.Errorf("open db %q: %w", dbPath, err)
	}
	var cfg Config
	if _, err := toml.Decode(cfgText, &cfg); err != nil {
		return nil, fmt.Errorf("parse toml: %w", err)
	}
	portfolios := make([]*Portfolio, 0, len(cfg.Portfolios))
	wfConfigs := make([]PortfolioConfig, 0, len(cfg.Portfolios))
	for _, pc := range cfg.Portfolios {
		if err := pc.ResolveStrategy(defaultLuaPath); err != nil {
			return nil, err
		}
		// A walk-forward block is not a portfolio but a schedule of them, so
		// it runs on its own path and contributes one synthetic result.
		if !pc.WalkForward.Zero() {
			wfConfigs = append(wfConfigs, pc)
			continue
		}
		// ToPortfolios, not ToPortfolio: one block expands to many when it
		// carries a Sweep, and to exactly one when it does not.
		expanded, err := pc.ToPortfolios()
		if err != nil {
			return nil, fmt.Errorf("portfolio %q: %w", pc.Name, err)
		}
		portfolios = append(portfolios, expanded...)
	}
	if len(portfolios) == 0 && len(wfConfigs) == 0 {
		return nil, fmt.Errorf("config defines no portfolios")
	}

	var results []Result
	if len(portfolios) > 0 {
		var err error
		if results, err = Run(portfolios, cfg.Output); err != nil {
			return nil, err
		}
	}
	return appendWalkForward(results, wfConfigs)
}

// appendWalkForward runs each walk-forward schedule and appends its single
// synthetic result. Schedules run after the ordinary portfolios because they
// are far more expensive — candidates times windows simulations each — and a
// config mixing the two should still show its cheap results if a schedule
// later fails.
func appendWalkForward(
	results []Result, configs []PortfolioConfig,
) ([]Result, error) {
	for i := range configs {
		wf, err := RunWalkForward(&configs[i])
		if err != nil {
			return nil, fmt.Errorf("portfolio %q: %w", configs[i].Name, err)
		}
		results = append(results, wf)
	}
	// The schedule's own trial count feeds the same correction every other
	// result gets; it is its own group, so it neither deflates nor is
	// deflated by the ordinary runs alongside it.
	applyOverfittingStats(results)
	return results, nil
}
