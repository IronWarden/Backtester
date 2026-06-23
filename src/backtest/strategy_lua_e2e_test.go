package backtest

import (
	"fmt"
	"my-backtester/src/data"
	"runtime"
	"sync"
	"testing"
	"time"
)

// E2E wall-clock measurement: runs a realistic batch (N portfolios × M sims)
// through a NumCPU worker pool for each strategy variant, with each worker
// constructing its own (portfolio, strategy) pair so Lua setup cost is also
// parallelized. Mirrors the runner.go pattern but skips DB / file I/O.

const (
	e2ePortfolios  = 50
	e2eSimsPerPort = 20
)

type e2eFactory func(tickers []string, hist map[string][]data.AssetData) (*Portfolio, error)

func nativeFactory(tickers []string, hist map[string][]data.AssetData) (*Portfolio, error) {
	return newBenchPortfolio(tickers, &benchSMACrossClose{
		Short: benchShort, Long: benchLong,
	}), nil
}

func luaGoFactory(tickers []string, hist map[string][]data.AssetData) (*Portfolio, error) {
	p := newBenchPortfolio(tickers, nil)
	ls, err := newLuaBenchStrategy(luaScriptGoIndicators, true, p, hist)
	if err != nil {
		return nil, err
	}
	p.Strategy = ls
	return p, nil
}

func luaPureFactory(tickers []string, hist map[string][]data.AssetData) (*Portfolio, error) {
	p := newBenchPortfolio(tickers, nil)
	ls, err := newLuaBenchStrategy(luaScriptPureLua, false, p, hist)
	if err != nil {
		return nil, err
	}
	p.Strategy = ls
	return p, nil
}

func runE2E(
	tb testing.TB,
	factory e2eFactory,
	tickers []string,
	hist map[string][]data.AssetData,
	totalJobs int,
) time.Duration {
	workers := runtime.NumCPU()
	sem := make(chan struct{}, totalJobs)
	for i := 0; i < totalJobs; i++ {
		sem <- struct{}{}
	}
	close(sem)

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range sem {
				p, err := factory(tickers, hist)
				if err != nil {
					tb.Errorf("factory: %v", err)
					return
				}
				runBenchSimulation(p, hist)
				if c, ok := p.Strategy.(*luaBenchStrategy); ok {
					c.Close()
				}
			}
		}()
	}
	wg.Wait()
	return time.Since(start)
}

func TestE2EWallClock(t *testing.T) {
	if testing.Short() {
		t.Skip("skip e2e wall-clock in -short mode")
	}
	benchInit()
	tickers, hist := generateBenchData()
	totalJobs := e2ePortfolios * e2eSimsPerPort
	workers := runtime.NumCPU()

	t.Logf("Workload: %d portfolios × %d sims = %d total jobs",
		e2ePortfolios, e2eSimsPerPort, totalJobs)
	t.Logf("Data: %d tickers × %d days, %d worker goroutines",
		benchTickers, benchDays, workers)
	t.Log("---")

	type result struct {
		name string
		dur  time.Duration
	}
	results := []result{}

	for _, v := range []struct {
		name    string
		factory e2eFactory
	}{
		{"native-go", nativeFactory},
		{"lua + go indicators", luaGoFactory},
		{"pure lua", luaPureFactory},
	} {
		// warm up once so the first variant doesn't pay cold-cache cost
		runE2E(t, v.factory, tickers, hist, workers)
		dur := runE2E(t, v.factory, tickers, hist, totalJobs)
		results = append(results, result{v.name, dur})
	}

	base := results[0].dur.Seconds()
	t.Log("Wall-clock results:")
	for _, r := range results {
		secs := r.dur.Seconds()
		simsPerSec := float64(totalJobs) / secs
		ratio := secs / base
		t.Log(fmt.Sprintf(
			"  %-22s %10v   %8.0f sims/sec   %5.2fx",
			r.name+":", r.dur.Round(time.Millisecond), simsPerSec, ratio,
		))
	}
}
