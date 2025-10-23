package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"my-backtester/src/backtest"
	"my-backtester/src/data"
)

var debug bool

func main() {
	flag.BoolVar(&debug, "debug", false, "Enable debug output")
	flag.Parse()

	// General logger
	if debug {
		file, err := os.OpenFile("backtester.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			log.Fatalf("Failed to open log file: %v", err)
		}
		log.SetOutput(file)
	}

	// Transaction logger
	if debug {
		file, err := os.OpenFile("transactions.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			log.Fatalf("Failed to open transaction log file: %v", err)
		}
		backtest.TransactionLogger = log.New(file, "", log.LstdFlags)
	} else {
		backtest.TransactionLogger = log.New(io.Discard, "", 0)
	}

	if debug {
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	duckDBPath := "./stock_data.db"
	_, err := data.InitDB(duckDBPath)
	if err != nil {
		log.Fatalf("Failed to open DuckDB: %v", err)
	}

	startTime := time.Date(2015, 3, 31, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2025, 3, 31, 0, 0, 0, 0, time.UTC)
	buyingPower := 20000.0
	simulationTimes := 1
	tickers1 := []string{"AAPL", "MSFT", "GOOGL", "AMZN"}
	tickers2 := []string{"NVDA", "AMD", "INTC"}

	portfolios := []*backtest.Portfolio{
		backtest.InitializePortfolio(buyingPower, startTime, endTime, "Tech Giants", tickers1),
		backtest.InitializePortfolio(buyingPower, startTime, endTime, "Semiconductors", tickers2),
	}

	backtest.Run(simulationTimes, portfolios)
}
