package backtest

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	// Sweep turns one config block into many runs: every key maps to a LIST
	// of values, and the block expands to the cartesian product of them. Each
	// resulting portfolio takes Params with its own sweep values written over
	// the top, so a swept key overrides its base value and an unswept one is
	// inherited. Absent or empty, the block produces exactly one portfolio
	// with exactly Params — which is what every pre-sweep config does.
	//
	// Typed as map[string]any rather than map[string][]any so a scalar can be
	// rejected with an error naming the key, instead of failing as an opaque
	// TOML decode error.
	Sweep map[string]any `toml:"Sweep"`
}

// MaxSweepRuns caps how many portfolios one config block may expand to.
// Five keys of ten values each is 100,000 backtests and a hung UI, so an
// oversized product is refused up front rather than discovered by waiting.
const MaxSweepRuns = 1000

// sweepValues normalizes one Sweep entry to its list of values, rejecting a
// scalar (which would silently read as a one-value sweep the user did not
// ask for) and an empty list (which would drop the whole block to zero runs).
func sweepValues(key string, raw any) ([]any, error) {
	list, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf(
			"Sweep key %q must be a list of values, e.g. %s = [1, 2, 3]", key, key,
		)
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("Sweep key %q has an empty list of values", key)
	}
	return list, nil
}

// sweepCombos expands a Sweep block into one parameter map per combination.
// Returns nil for an absent or empty block, which callers read as "no sweep".
//
// Keys are iterated in sorted order and values in the order they were
// written, so the same config always expands to the same portfolios in the
// same sequence. A sweep whose ordering moved between runs could not be
// diffed against a previous one, which is most of what a sweep is for.
func sweepCombos(sweep map[string]any) ([]map[string]any, error) {
	if len(sweep) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(sweep))
	for k := range sweep {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Size the product first so an oversized sweep is refused before any of
	// it is built.
	total := 1
	lists := make([][]any, len(keys))
	for i, k := range keys {
		list, err := sweepValues(k, sweep[k])
		if err != nil {
			return nil, err
		}
		lists[i] = list
		total *= len(list)
		if total > MaxSweepRuns {
			return nil, fmt.Errorf(
				"Sweep expands to more than %d runs; narrow the lists or "+
					"split the block across several portfolios",
				MaxSweepRuns,
			)
		}
	}

	combos := make([]map[string]any, 0, total)
	idx := make([]int, len(keys))
	for {
		combo := make(map[string]any, len(keys))
		for i, k := range keys {
			combo[k] = lists[i][idx[i]]
		}
		combos = append(combos, combo)
		// Odometer increment over the last key first, which makes the earlier
		// (alphabetically first) keys the slow-moving ones and keeps runs
		// that share a prefix adjacent in the results.
		pos := len(keys) - 1
		for pos >= 0 {
			idx[pos]++
			if idx[pos] < len(lists[pos]) {
				break
			}
			idx[pos] = 0
			pos--
		}
		if pos < 0 {
			break
		}
	}
	return combos, nil
}

// sweepValueLabel renders one swept value for a portfolio name. TOML integers
// arrive as int64 and floats as float64; %v would print 1e+06 for a large
// float and is avoided so the label reads back as what was written.
func sweepValueLabel(v any) string {
	switch t := v.(type) {
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

// sweepLabel names a run for its parameters, e.g. "[buy_thresh=20 period=7]".
// Without it a sweep produces N rows with the same Name, and the parameter
// set — the only thing that distinguishes them — never reaches the results
// table or [Output].
func sweepLabel(combo map[string]any) string {
	keys := make([]string, 0, len(combo))
	for k := range combo {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + sweepValueLabel(combo[k])
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// mergeParams returns base with overrides written over the top, leaving base
// untouched — every combination shares the same base map, so mutating it
// would leak one combination's values into the next.
func mergeParams(base, overrides map[string]any) map[string]any {
	merged := make(map[string]any, len(base)+len(overrides))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range overrides {
		merged[k] = v
	}
	return merged
}

// ToPortfolios expands one config block into the portfolios it describes:
// exactly one when there is no Sweep, and the cartesian product of the sweep
// otherwise. This is the entry point callers should use; ToPortfolio remains
// the single-portfolio conversion it has always been.
//
// Expansion happens here rather than in the runner deliberately. The runner
// already accepts a slice and fans it out across its worker pool, so a sweep
// is N portfolios to it and nothing about its concurrency needs to change.
func (pc *PortfolioConfig) ToPortfolios() ([]*Portfolio, error) {
	combos, err := sweepCombos(pc.Sweep)
	if err != nil {
		return nil, err
	}
	if len(combos) == 0 {
		p, err := pc.ToPortfolio()
		if err != nil {
			return nil, err
		}
		return []*Portfolio{p}, nil
	}

	portfolios := make([]*Portfolio, 0, len(combos))
	for _, combo := range combos {
		sub := *pc
		sub.Params = mergeParams(pc.Params, combo)
		sub.Name = strings.TrimSpace(pc.Name + " " + sweepLabel(combo))
		p, err := sub.ToPortfolio()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", sweepLabel(combo), err)
		}
		portfolios = append(portfolios, p)
	}
	return portfolios, nil
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
