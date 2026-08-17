package main

import (
	"flag"
	"fmt"
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
		debug       bool
		configPath  string
		scanSignals bool
		eventStudy  bool
		listStrats  bool
		record      bool
		campaign    string
		history     int
	)
	flag.BoolVar(&debug, "debug", false, "Enable debug output")
	flag.BoolVar(
		&scanSignals, "scan-signals", false,
		"Screen the built-in signal library over the config's tickers and "+
			"print the information coefficients, instead of running backtests",
	)
	flag.BoolVar(
		&eventStudy, "event-study", false,
		"Measure what followed each built-in event (drawdowns, breakouts, "+
			"sigma moves) over the config's tickers, instead of running "+
			"backtests",
	)
	flag.BoolVar(
		&listStrats, "list-strategies", false,
		"Print the strategy gallery — what each shipped script is for, how it "+
			"fails, and the parameter ranges worth sweeping",
	)
	flag.BoolVar(
		&record, "record", false,
		"Append every result to the research log (../research.db) so you can "+
			"tell later what you have already tried",
	)
	flag.StringVar(
		&campaign, "campaign", "",
		"Group these runs under a campaign name in the research log",
	)
	flag.IntVar(
		&history, "history", 0,
		"Print the last N runs from the research log and exit",
	)
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

	// The research log is its own database and needs neither the market data
	// nor a config, so it answers before either is opened.
	if history > 0 {
		reg, err := backtest.OpenRegistry(backtest.DefaultRegistryPath)
		if err != nil {
			log.Fatalf("opening the research log: %v", err)
		}
		defer reg.Close()
		lines, err := reg.RecentRuns(history)
		if err != nil {
			log.Fatalf("reading the research log: %v", err)
		}
		if len(lines) == 0 {
			fmt.Println("No runs recorded yet. Add -record to a run to start " +
				"keeping a log.")
		}
		for _, line := range lines {
			fmt.Println(line)
		}
		return
	}

	// The gallery is documentation: no database, no config, so it answers
	// before either is opened and works on a machine with neither.
	if listStrats {
		cards, err := backtest.LoadGallery("../strategies")
		if err != nil {
			log.Fatalf("reading the strategy gallery: %v", err)
		}
		fmt.Print(backtest.GalleryReport(cards))
		return
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

	// Convert config to portfolios. One block expands to several when it
	// carries a Sweep, and to exactly one when it does not.
	portfolios := make([]*backtest.Portfolio, 0, len(config.Portfolios))
	for _, pc := range config.Portfolios {
		expanded, err := pc.ToPortfolios()
		if err != nil {
			log.Fatalf(
				"Failed to convert portfolio %s: %v", pc.Name, err,
			)
		}
		portfolios = append(portfolios, expanded...)
	}

	// A screen, not a run: ask whether any signal ranks forward returns on this
	// universe before spending an afternoon writing a strategy around one.
	if scanSignals {
		fmt.Print(backtest.ScanConfigPortfolios(portfolios, nil))
		return
	}
	// "What happens after X?" — the other cheap question, answered before
	// writing a strategy around the answer.
	if eventStudy {
		fmt.Print(backtest.StudyConfigPortfolios(portfolios, nil))
		return
	}

	results, err := backtest.Run(portfolios, config.Output)
	if err != nil {
		log.Fatalf("Run: %v", err)
	}

	// Bookkeeping, and strictly after the fact: a failure to record must never
	// cost a run that already succeeded.
	if record {
		if err := recordRun(portfolios, results, campaign); err != nil {
			log.Printf("could not record this run: %v", err)
		}
	}
}

// recordRun appends the results to the research log and reports whether this
// exact config has been run before — which is the question the log exists to
// answer, and is worth saying at the moment it is still actionable.
func recordRun(
	portfolios []*backtest.Portfolio, results []backtest.Result, campaign string,
) error {
	reg, err := backtest.OpenRegistry(backtest.DefaultRegistryPath)
	if err != nil {
		return err
	}
	defer reg.Close()

	for _, p := range portfolios {
		hash := backtest.ConfigHash(p)
		if prior, last, err := reg.PriorRuns(hash); err == nil && prior > 0 {
			fmt.Printf("note: %q has been run %d time(s) before, last on %s "+
				"[%s]\n", p.Pname, prior, last.Format("2006-01-02"), hash)
		}
	}
	if err := reg.Record(campaign, portfolios, results); err != nil {
		return err
	}
	if campaign != "" {
		// The campaign figure is the honest one: a search that has tried 200
		// things must clear a higher bar than one that tried 6, and the
		// per-sweep number does not know about the other 194.
		if trials, bar, err := reg.CampaignDeflation(campaign); err == nil {
			fmt.Printf("campaign %q has now spent %d trials", campaign, trials)
			if bar > 0 {
				fmt.Printf("; luck alone would produce a Sharpe of %.2f across "+
					"that many, so treat anything below it as noise", bar)
			}
			fmt.Println()
		}
	}
	fmt.Printf("recorded %d result(s) to %s\n",
		len(results), backtest.DefaultRegistryPath)
	return nil
}
