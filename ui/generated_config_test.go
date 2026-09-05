package main

import (
	"os"
	"path/filepath"
	"testing"

	"my-backtester/src/backtest"
)

// The Backtest form's output is the input to every run started from it, and
// until now nothing ever handed it to the decoder: the frontend smoke render
// compares strings, and `npm run build` only type-checks. That is how a
// cashflow schedule shipped writing "[portfolio.Cashflow]" under
// "[[Portfolio]]" -- valid TOML, but a second top-level table, since TOML keys
// are case-sensitive -- and every run from a form with contributions on it
// died with "incompatible types: TOML value has type map[string]any;
// destination has type slice" before touching the database.
//
// So: every golden the form generates must parse into the config the form
// meant, checked here rather than in TypeScript, because the decoder is the
// authority on what parses.
func TestGeneratedConfigsParse(t *testing.T) {
	goldens, err := filepath.Glob(filepath.Join("frontend", "testdata", "golden*.toml"))
	if err != nil {
		t.Fatalf("glob goldens: %v", err)
	}
	if len(goldens) == 0 {
		t.Fatal("no generated goldens found; did they move?")
	}

	for _, path := range goldens {
		// The hypothesis golden is a pre-registration document, not a config.
		if filepath.Base(path) == "golden-hypothesis.toml" {
			continue
		}
		t.Run(filepath.Base(path), func(t *testing.T) {
			text, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			cfg, err := backtest.LoadConfigText(string(text))
			if err != nil {
				t.Fatalf("%s does not parse: %v", path, err)
			}
			if len(cfg.Portfolios) == 0 {
				t.Fatalf("%s parsed to zero portfolios; the blocks did not land "+
					"on Config.Portfolios", path)
			}
			for _, pc := range cfg.Portfolios {
				if pc.Name == "" || len(pc.Tickers) == 0 {
					t.Errorf("%s: portfolio decoded empty: %+v", path, pc)
				}
			}
		})
	}
}

// The cashflow golden carries the sub-table, so it gets the sharper assertion:
// the schedule has to reach the portfolio it hangs off, not merely parse.
func TestGeneratedCashflowConfigReachesThePortfolio(t *testing.T) {
	text, err := os.ReadFile(filepath.Join("frontend", "testdata", "golden-cashflow.toml"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	cfg, err := backtest.LoadConfigText(string(text))
	if err != nil {
		t.Fatalf("generated cashflow config does not parse: %v", err)
	}
	if len(cfg.Portfolios) != 1 {
		t.Fatalf("got %d portfolios, want 1", len(cfg.Portfolios))
	}
	cf := cfg.Portfolios[0].Cashflow
	if cf.Zero() {
		t.Fatal("the [Portfolio.Cashflow] block did not land on the portfolio")
	}
	if cf.Amount != 500 || cf.Frequency != "monthly" {
		t.Fatalf("got Amount=%v Frequency=%q, want 500 monthly", cf.Amount, cf.Frequency)
	}
}
