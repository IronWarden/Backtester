package backtest

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"my-backtester/src/data"
)

// WorkItem represents a single unit of work.
type WorkItem struct {
	Ticker string
}

// Result holds the result of a backtest.
type Result struct {
	Ticker  string
	Metrics Metrics
}

type BacktesterParams struct {
	StartTime      time.Time
	EndTime        time.Time
	HistoricalData map[string][]data.AssetData
	RiskFreeRates  map[int64]float64
	Tickers        []string
}

// Run runs the backtesting simulation.
func Run(startTime, endTime time.Time, buyingPower float64, simulationTimes int) {
	tickers := data.GetTickersWithSufficientData(startTime, endTime)
	riskFreeRates := data.GetRiskFreeRates(startTime, endTime)
	numWorkers := runtime.NumCPU()
	backtesterParams := BacktesterParams{
		StartTime:     startTime,
		EndTime:       endTime,
		RiskFreeRates: riskFreeRates,
	}

	jobs := make(chan WorkItem, len(tickers)*simulationTimes)
	results := make(chan Result, len(tickers)*simulationTimes)

	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			days := int(endTime.Sub(startTime).Hours() / 24)
			portfolio := InitializePortfolio(buyingPower, days)
			for work := range jobs {
				historicalData := data.QueryAssetData(work.Ticker, startTime, endTime)
				backtesterParams.HistoricalData = historicalData
				if historicalData == nil {
					continue
				}
				portfolio.Reset(buyingPower)
				portfolio.BuyAndHold(backtesterParams, "greedy")
				portfolio.GetBacktestingData(backtesterParams)
				results <- Result{Ticker: work.Ticker, Metrics: portfolio.Metrics}
			}
		}()
	}

	// Producer
	go func() {
		for i := 0; i < simulationTimes; i++ {
			for _, ticker := range tickers {
				jobs <- WorkItem{Ticker: ticker}
			}
		}
		close(jobs)
	}()

	// Collector
	go func() {
		writingTickerTime := time.Now()
		file, err := os.OpenFile("worthy_tickers.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()

		for result := range results {
			if result.Metrics.SharpeRatio > 1.0 {
				str := fmt.Sprintf("%s, Sharpe Ratio: %.2f, Sortino Ratio: %.2f, Max Drawdown: %.2f, Annual Return: %.2f", result.Ticker, result.Metrics.SharpeRatio, result.Metrics.SortinoRatio, result.Metrics.MaxDrawdown, result.Metrics.AnnualReturn)
				if _, err := file.WriteString(str + "\n"); err != nil {
					log.Printf("Failed to write to file: %v", err)
				}
			}
		}
		log.Printf("Writing ticker time: %s\n", time.Since(writingTickerTime))
	}()

	wg.Wait()
	close(results)
}
