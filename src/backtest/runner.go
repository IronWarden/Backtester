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
	// EquityCurve is the portfolio's daily total value, and Dates are the
	// matching trading days (YYYY-MM-DD) in the same order. Both come from
	// the per-day record kept during the simulation and are 1:1 in length,
	// so the frontend can plot value-over-time directly.
	EquityCurve []float64
	Dates       []string
}

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

// runOne executes one full simulation pass over a single-strategy portfolio.
// The day loop lives here; the strategy decides what to do on each day.
func runOne(
	p *Portfolio,
	hist map[string][]data.AssetData,
	riskFreeRates map[int64]float64,
) {
	if len(p.Tickers) == 0 {
		return
	}
	dataLen := len(hist[p.Tickers[0]])
	if dataLen == 0 {
		return
	}

	p.Strategy.Step(p, hist, 0)
	prev := p.GetPortfolioValue(p.Tickers, hist, 0)
	for day := 1; day < dataLen; day++ {
		p.Strategy.Step(p, hist, day)
		curr := p.GetPortfolioValue(p.Tickers, hist, day)
		p.AdjustPortfolioParameters(p.Tickers, hist, day, prev, curr)
		prev = curr
	}
	p.GetBacktestingData(riskFreeRates, hist, dataLen)
	if c, ok := p.Strategy.(interface{ Close() }); ok {
		c.Close()
	}
}

// Run executes every portfolio concurrently and always returns the
// collected results. If output is non-nil, results are also written to a
// file via the configured Reporter.
func Run(portfolios []*Portfolio, output *OutputConfig) ([]Result, error) {
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
				for i, dr := range p.DailyReturns {
					dates[i] = dr.Date.Format("2006-01-02")
				}
				results <- Result{
					PortfolioName: p.Pname,
					Strategy:      p.Strategy.Name(),
					Metrics:       p.Metrics,
					EquityCurve:   p.PortfolioCloseValues,
					Dates:         dates,
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
