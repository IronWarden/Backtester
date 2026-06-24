package main

import (
	"flag"
	"io"
	"log"
	"my-backtester/src/backtest"
	"my-backtester/src/data"
	"net/http"
	_ "net/http/pprof"
	"os"
)

func main() {
	var (
		debug      bool
		configPath string
	)
	flag.BoolVar(&debug, "debug", false, "Enable debug output")
	flag.StringVar(
		&configPath, "config", "../config.toml",
		"Path to portfolio TOML config",
	)
	flag.Parse()

	if debug {
		file, err := os.OpenFile(
			"backtester.log",
			os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
			0666,
		)
		if err != nil {
			log.Fatalf("Failed to open log file: %v", err)
		}
		log.SetOutput(file)
		transactionFile, err := os.OpenFile(
			"transactions.log",
			os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
			0666,
		)
		if err != nil {
			log.Fatalf("Failed to open transaction log file: %v", err)
		}
		backtest.TransactionLogger = log.New(transactionFile, "", log.LstdFlags)
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	} else {
		backtest.TransactionLogger = log.New(io.Discard, "", 0)
	}

	duckDBPath := "../stock_data.db"
	_, err := data.InitDB(duckDBPath)
	if err != nil {
		log.Fatalf("Failed to open DuckDB: %v", err)
	}

	// Load configuration from TOML file
	config, err := backtest.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Convert config to portfolios
	portfolios := make([]*backtest.Portfolio, 0, len(config.Portfolios))
	for _, pc := range config.Portfolios {
		portfolio, err := pc.ToPortfolio()
		if err != nil {
			log.Fatalf(
				"Failed to convert portfolio %s: %v", pc.Name, err,
			)
		}
		portfolios = append(portfolios, portfolio)
	}

	if _, err := backtest.Run(portfolios, config.Output); err != nil {
		log.Fatalf("Run: %v", err)
	}
}
