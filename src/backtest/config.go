package backtest

import (
	"time"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Portfolios []PortfolioConfig `toml:"portfolio"`
	Output     *OutputConfig     `toml:"Output"`
}

// OutputConfig controls how backtest Results are persisted.
// All fields are optional; an absent [Output] block disables file output.
type OutputConfig struct {
	Path   string   `toml:"path"`
	Format string   `toml:"format"` // "txt" (default), "csv", "json"
	Fields []string `toml:"fields"` // result fields to emit, in order
	Filter string   `toml:"filter"` // Go-style expression, e.g. "SharpeRatio > 0.5 && AnnualReturn > 5"
	SortBy string   `toml:"sort_by"` // result field to sort by; empty disables sorting
	Order  string   `toml:"order"`   // "asc" or "desc" (default "desc")
	Limit  int      `toml:"limit"`   // emit at most N results; 0 means unlimited
}

type PortfolioConfig struct {
	Name        string         `toml:"Name"`
	BuyingPower float64        `toml:"BuyingPower"`
	StartTime   string         `toml:"StartDate"`
	EndTime     string         `toml:"EndDate"`
	Tickers     []string       `toml:"Tickers"`
	Strategy    string         `toml:"Strategy"`
	Params      map[string]any `toml:"Params"`
}

func LoadConfig(filepath string) (*Config, error) {
	var config Config
	_, err := toml.DecodeFile(filepath, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (pc *PortfolioConfig) ToPortfolio() (*Portfolio, error) {
	startTime, err := time.Parse("2006-01-02", pc.StartTime)
	if err != nil {
		return nil, err
	}

	endTime, err := time.Parse("2006-01-02", pc.EndTime)
	if err != nil {
		return nil, err
	}

	return InitializePortfolio(
		pc.BuyingPower,
		startTime,
		endTime,
		pc.Name,
		pc.Tickers,
		pc.Strategy,
		pc.Params,
	)
}
