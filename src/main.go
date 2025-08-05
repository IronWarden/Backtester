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

var verbose bool

func main() {
	flag.BoolVar(&verbose, "verbose", false, "Enable verbose output")
	flag.Parse()

	// General logger
	if verbose {
		file, err := os.OpenFile("backtester.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			log.Fatalf("Failed to open log file: %v", err)
		}
		log.SetOutput(file)
	}

	// Transaction logger
	if verbose {
		file, err := os.OpenFile("transactions.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			log.Fatalf("Failed to open transaction log file: %v", err)
		}
		backtest.TransactionLogger = log.New(file, "", log.LstdFlags)
	} else {
		backtest.TransactionLogger = log.New(io.Discard, "", 0)
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	duckDBPath := "./stock_data.db"
	_, err := data.InitDB(duckDBPath)
	if err != nil {
		log.Fatalf("Failed to open DuckDB: %v", err)
	}

	startTime := time.Date(2015, 3, 31, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2025, 3, 31, 0, 0, 0, 0, time.UTC)
	buyingPower := 20000.0
	simulationTimes := 1

	backtest.Run(startTime, endTime, buyingPower, simulationTimes)
}