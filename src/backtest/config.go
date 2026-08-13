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

// bps converts a basis-point figure to a plain multiplier.
func bps(v float64) float64 { return v / 10000.0 }

// BuyFill is the price a buy actually executes at: slippage moves it
// against the buyer. With zero slippage this multiplies by exactly 1.0 and
// so returns quoted unchanged, bit for bit.
func (c CostConfig) BuyFill(quoted float64) float64 {
	return quoted * (1.0 + bps(c.SlippageBps))
}

// SellFill is the price a sell actually executes at: slippage moves it
// against the seller. Floored at 0 so an absurd slippage setting cannot
// produce a negative price.
func (c CostConfig) SellFill(quoted float64) float64 {
	fill := quoted * (1.0 - bps(c.SlippageBps))
	if fill < 0 {
		return 0
	}
	return fill
}

// Commission is the fee charged on one filled order of the given notional
// value: a flat amount plus a proportional one.
func (c CostConfig) Commission(notional float64) float64 {
	return c.CommissionPerTrade + notional*bps(c.CommissionBps)
}

// MaxAffordableNotional is the largest notional value whose commission the
// given cash balance can also cover, i.e. the n solving
// n + Commission(n) = cash. Returns 0 when the cash cannot even cover the
// flat fee. This is what lets the sub-cent clamp in Buy stay exact once
// commissions exist.
func (c CostConfig) MaxAffordableNotional(cash float64) float64 {
	n := (cash - c.CommissionPerTrade) / (1.0 + bps(c.CommissionBps))
	if n < 0 {
		return 0
	}
	return n
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
	// Benchmark is an optional ticker to score this portfolio against, e.g.
	// "$SP500". It is NOT traded and never joins Tickers — it only supplies
	// a return series for the benchmark-relative metrics. Empty means those
	// metrics are not computed.
	Benchmark string `toml:"Benchmark"`
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
	p.Benchmark = pc.Benchmark
	return p, nil
}
