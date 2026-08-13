package backtest

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The config file is the whole user-facing surface of the CLI, so these
// tests pin the TOML key names as a contract: renaming a struct tag here
// silently stops honouring a user's existing config rather than failing,
// which is exactly the class of break these tests catch.

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}

func TestLoadConfigMultiplePortfolios(t *testing.T) {
	path := writeConfig(t, `
[[portfolio]]
Name = "first"
BuyingPower = 10000.0
StartDate = "2020-01-01"
EndDate = "2021-01-01"
Tickers = ["AAPL", "MSFT"]
Strategy = "greedy"

[[portfolio]]
Name = "second"
BuyingPower = 5000.0
StartDate = "2019-06-03"
EndDate = "2020-06-03"
Tickers = ["$SP500"]
Strategy = "smaCross:50:200:equalWeights"

  [portfolio.Params]
  threshold = 0.5
  lookback = 20

[Output]
path = "results.csv"
format = "csv"
fields = ["Name", "SharpeRatio"]
filter = "SharpeRatio > 0.5"
sort_by = "SharpeRatio"
order = "asc"
limit = 10
`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.Portfolios) != 2 {
		t.Fatalf("loaded %d portfolios, want 2", len(cfg.Portfolios))
	}

	first := cfg.Portfolios[0]
	if first.Name != "first" || first.Strategy != "greedy" {
		t.Errorf("first portfolio = %+v", first)
	}
	closeTo(t, "first buying power", first.BuyingPower, 10000)
	if len(first.Tickers) != 2 || first.Tickers[0] != "AAPL" {
		t.Errorf("first tickers = %v", first.Tickers)
	}

	second := cfg.Portfolios[1]
	if second.Strategy != "smaCross:50:200:equalWeights" {
		t.Errorf("second strategy = %q", second.Strategy)
	}
	// Params is untyped by design — the Lua host does its own coercion.
	if got, ok := second.Params["threshold"].(float64); !ok || got != 0.5 {
		t.Errorf("Params[threshold] = %#v, want 0.5", second.Params["threshold"])
	}
	if got, ok := second.Params["lookback"].(int64); !ok || got != 20 {
		t.Errorf("Params[lookback] = %#v, want int64 20", second.Params["lookback"])
	}

	if cfg.Output == nil {
		t.Fatal("Output block not decoded")
	}
	out := *cfg.Output
	if out.Path != "results.csv" || out.Format != "csv" || out.Filter != "SharpeRatio > 0.5" ||
		out.SortBy != "SharpeRatio" || out.Order != "asc" || out.Limit != 10 {
		t.Errorf("Output = %+v", out)
	}
	if len(out.Fields) != 2 || out.Fields[1] != "SharpeRatio" {
		t.Errorf("Output.Fields = %v", out.Fields)
	}
}

// An absent [Output] block leaves the pointer nil, which is how the CLI
// decides not to write a results file.
func TestLoadConfigNoOutputBlock(t *testing.T) {
	path := writeConfig(t, `
[[portfolio]]
Name = "only"
BuyingPower = 1000.0
StartDate = "2020-01-01"
EndDate = "2021-01-01"
Tickers = ["AAPL"]
Strategy = "greedy"
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Output != nil {
		t.Errorf("Output = %+v, want nil when the block is absent", cfg.Output)
	}
}

func TestLoadConfigErrors(t *testing.T) {
	t.Run("malformed toml", func(t *testing.T) {
		path := writeConfig(t, "[[portfolio]\nName = \"broken\"\n")
		if _, err := LoadConfig(path); err == nil {
			t.Error("malformed TOML decoded without error")
		}
	})

	t.Run("wrong type for a field", func(t *testing.T) {
		path := writeConfig(t, "[[portfolio]]\nBuyingPower = \"lots\"\n")
		if _, err := LoadConfig(path); err == nil {
			t.Error("string in a float field decoded without error")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "nope.toml")
		if _, err := LoadConfig(missing); err == nil {
			t.Error("missing file loaded without error")
		}
	})
}

// LoadConfig performs no validation — omitted keys zero-value. The failure
// surfaces later, in ToPortfolio, which is what the next test pins.
func TestLoadConfigDoesNotValidate(t *testing.T) {
	path := writeConfig(t, "[[portfolio]]\nName = \"sparse\"\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.Portfolios) != 1 {
		t.Fatalf("loaded %d portfolios, want 1", len(cfg.Portfolios))
	}
	if cfg.Portfolios[0].StartTime != "" || cfg.Portfolios[0].BuyingPower != 0 {
		t.Errorf("omitted keys should zero-value, got %+v", cfg.Portfolios[0])
	}
}

func TestToPortfolio(t *testing.T) {
	pc := PortfolioConfig{
		Name:        "first",
		BuyingPower: 10000,
		StartTime:   "2020-01-01",
		EndTime:     "2021-01-01",
		Tickers:     []string{"AAPL"},
		Strategy:    "greedy",
	}
	p, err := pc.ToPortfolio()
	if err != nil {
		t.Fatalf("ToPortfolio: %v", err)
	}
	if p.Pname != "first" {
		t.Errorf("Pname = %q", p.Pname)
	}
	closeTo(t, "buying power", p.BuyingPower, 10000)
	closeTo(t, "initial buying power", p.InitialBuyingPower, 10000)
	want := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if !p.StartTime.Equal(want) {
		t.Errorf("StartTime = %v, want %v", p.StartTime, want)
	}
	if p.Strategy == nil || p.Strategy.Name() == "" {
		t.Error("ToPortfolio did not build a Strategy")
	}
	if len(p.Positions) != 0 {
		t.Errorf("new portfolio has %d positions", len(p.Positions))
	}
}

func TestToPortfolioRejectsBadFields(t *testing.T) {
	base := PortfolioConfig{
		Name:        "p",
		BuyingPower: 1000,
		StartTime:   "2020-01-01",
		EndTime:     "2021-01-01",
		Tickers:     []string{"AAPL"},
		Strategy:    "greedy",
	}
	cases := []struct {
		name   string
		mutate func(*PortfolioConfig)
	}{
		{"empty start date", func(pc *PortfolioConfig) { pc.StartTime = "" }},
		{"empty end date", func(pc *PortfolioConfig) { pc.EndTime = "" }},
		{"US-style start date", func(pc *PortfolioConfig) { pc.StartTime = "01/02/2020" }},
		{"timestamped end date", func(pc *PortfolioConfig) { pc.EndTime = "2021-01-01T00:00:00Z" }},
		{"impossible date", func(pc *PortfolioConfig) { pc.StartTime = "2020-13-45" }},
		{"unknown strategy", func(pc *PortfolioConfig) { pc.Strategy = "moonPhase" }},
		{"strategy missing its arguments", func(pc *PortfolioConfig) { pc.Strategy = "smaCross:50" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pc := base
			tc.mutate(&pc)
			p, err := pc.ToPortfolio()
			if err == nil {
				t.Fatalf("ToPortfolio accepted %+v -> %+v", pc, p)
			}
			if p != nil {
				t.Errorf("got a portfolio alongside the error: %+v", p)
			}
		})
	}
}
