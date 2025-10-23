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

// Result holds the result of a backtest.
type Result struct {
	PortfolioName string
	Metrics       Metrics
}

// Find the earliest day in each portfolio
// Returns two times minDate, maxDate
func findEarliestDate(portfolios []*Portfolio) (time.Time, time.Time) {
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

func Run(simulationTimes int, portfolios []*Portfolio) {
	startTime, endTime := findEarliestDate(portfolios)
	riskFreeRates := data.GetRiskFreeRates(startTime, endTime)
	// Consolidate all unique tickers from all portfolios
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

	// if len(allTickers) < 1 {
	// 	allTickers = data.GetTickersWithSufficientData(startTime, endTime)
	// }

	numWorkers := runtime.NumCPU()
	historicalData := make(map[string][]data.AssetData)

	for _, ticker := range allTickers {
		historicalData[ticker] = data.QueryAssetData(ticker, startTime, endTime)
	}

	jobs := make(chan *Portfolio, len(portfolios)*simulationTimes)
	results := make(chan Result, len(portfolios)*simulationTimes)

	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for portfolio := range jobs {
				for startTime := portfolio.StartTime; startTime.Before(portfolio.EndTime); startTime = startTime.AddDate(0, 0, 1) {

					portfolio.BuyAndHold(historicalData, "greedy")
					portfolio.GetBacktestingData(riskFreeRates)
					results <- Result{PortfolioName: portfolio.Pname, Metrics: portfolio.Metrics}
				}
			}
		}()
	}

	// Producer
	go func() {
		for i := 0; i < simulationTimes; i++ {
			for _, p := range portfolios {
				jobs <- p
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
				str := fmt.Sprintf("Portfolio: %s, Sharpe Ratio: %.2f, Sortino Ratio: %.2f, Max Drawdown: %.2f, Annual Return: %.2f", result.PortfolioName, result.Metrics.SharpeRatio, result.Metrics.SortinoRatio, result.Metrics.MaxDrawdown, result.Metrics.AnnualReturn)
				if _, err := file.WriteString(str + "\n"); err != nil {
					log.Printf("Failed to write to file: %v", err)
				}
			}
		}
		log.Printf("Writing portfolio time: %s\n", time.Since(writingTickerTime))
	}()

	wg.Wait()
	close(results)
}
