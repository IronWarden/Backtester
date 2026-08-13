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

// runOne executes one full simulation pass over a single-strategy portfolio.
// The day loop lives here; the strategy decides what to do on each day. Data is
// clipped to the portfolio's own [StartTime, EndTime] window first, so metrics
// reflect that window regardless of other portfolios' ranges in the same run.
func runOne(
	p *Portfolio,
	hist map[string][]data.AssetData,
	riskFreeRates map[int64]float64,
) {
	if len(p.Tickers) == 0 {
		return
	}
	windowed, dataLen := alignToWindow(hist, p.Tickers, p.StartTime, p.EndTime)
	if dataLen == 0 {
		return
	}

	p.Strategy.Step(p, windowed, 0)
	prev := p.GetPortfolioValue(p.Tickers, windowed, 0)
	for day := 1; day < dataLen; day++ {
		p.Strategy.Step(p, windowed, day)
		curr := p.GetPortfolioValue(p.Tickers, windowed, day)
		p.AdjustPortfolioParameters(p.Tickers, windowed, day, prev, curr)
		prev = curr
	}
	p.GetBacktestingData(riskFreeRates, windowed, dataLen)
	// The benchmark is scored against the portfolio's own trading days, taken
	// from the aligned window. It is read from the unaligned prefetch, never
	// from windowed, because it is not one of p.Tickers and must not become
	// one — that slice is what drives allocation and valuation.
	dates := make([]time.Time, dataLen)
	for i, bar := range windowed[p.Tickers[0]] {
		dates[i] = bar.Date
	}
	p.applyBenchmarkMetrics(hist, dates, riskFreeRates)
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

	return collected, nil
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
	for _, pc := range cfg.Portfolios {
		if strings.TrimSpace(pc.Strategy) == "" {
			if defaultLuaPath == "" {
				return nil, fmt.Errorf(
					"portfolio %q: Strategy is required and no default Lua script is set",
					pc.Name,
				)
			}
			pc.Strategy = "lua:" + defaultLuaPath
		}
		p, err := pc.ToPortfolio()
		if err != nil {
			return nil, fmt.Errorf("portfolio %q: %w", pc.Name, err)
		}
		portfolios = append(portfolios, p)
	}
	if len(portfolios) == 0 {
		return nil, fmt.Errorf("config defines no portfolios")
	}
	return Run(portfolios, cfg.Output)
}
