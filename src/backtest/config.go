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
	Format string   `toml:"format"`  // "txt" (default), "csv", "json"
	Fields []string `toml:"fields"`  // result fields to emit, in order
	Filter string   `toml:"filter"`  // Go-style expression, e.g. "SharpeRatio > 0.5 && AnnualReturn > 5"
	SortBy string   `toml:"sort_by"` // result field to sort by; empty disables sorting
	Order  string   `toml:"order"`   // "asc" or "desc" (default "desc")
	Limit  int      `toml:"limit"`   // emit at most N results; 0 means unlimited
}

// CostConfig models the cost of trading. Every field defaults to 0, and a
// zero CostConfig reproduces the frictionless behaviour the engine had
// before costs existed — so an absent [portfolio.Costs] block changes
// nothing. Unlike OutputConfig this is a value, not a pointer: "no costs"
// and "all costs zero" are the same thing, so there is nothing for nil to
// express and no nil to guard against on the trade path.
type CostConfig struct {
	// CommissionPerTrade is a flat amount charged per filled order.
	CommissionPerTrade float64 `toml:"commission_per_trade"`
	// CommissionBps is charged per filled order as basis points of the
	// order's notional value.
	CommissionBps float64 `toml:"commission_bps"`
	// SlippageBps worsens the fill price by basis points: buys fill higher
	// than the quoted close, sells fill lower.
	SlippageBps float64 `toml:"slippage_bps"`
}

// Zero reports whether the config charges nothing, i.e. whether trades
// execute exactly as they did before the cost model existed.
func (c CostConfig) Zero() bool {
	return c.CommissionPerTrade == 0 && c.CommissionBps == 0 && c.SlippageBps == 0
}

type PortfolioConfig struct {
	Name        string         `toml:"Name"`
	BuyingPower float64        `toml:"BuyingPower"`
	StartTime   string         `toml:"StartDate"`
	EndTime     string         `toml:"EndDate"`
	Tickers     []string       `toml:"Tickers"`
	Strategy    string         `toml:"Strategy"`
	Params      map[string]any `toml:"Params"`
	Costs       CostConfig     `toml:"Costs"`
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

	p, err := InitializePortfolio(
		pc.BuyingPower,
		startTime,
		endTime,
		pc.Name,
		pc.Tickers,
		pc.Strategy,
		pc.Params,
	)
	if err != nil {
		return nil, err
	}
	// Set after construction rather than as an eighth positional argument to
	// InitializePortfolio, which is already at the limit of what a positional
	// signature carries readably.
	p.Costs = pc.Costs
	return p, nil
}
