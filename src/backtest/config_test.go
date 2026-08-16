package backtest

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

// The cost model is opt-in: an absent [portfolio.Costs] block must leave a
// zero CostConfig, which is what preserves the engine's original
// frictionless behaviour for every config written before costs existed.
func TestLoadConfigCosts(t *testing.T) {
	path := writeConfig(t, `
[[portfolio]]
Name = "with costs"
BuyingPower = 10000.0
StartDate = "2020-01-01"
EndDate = "2021-01-01"
Tickers = ["AAPL"]
Strategy = "greedy"

  [portfolio.Costs]
  commission_per_trade = 1.0
  commission_bps = 2.5
  slippage_bps = 5.0

[[portfolio]]
Name = "no costs block"
BuyingPower = 10000.0
StartDate = "2020-01-01"
EndDate = "2021-01-01"
Tickers = ["AAPL"]
Strategy = "greedy"

[[portfolio]]
Name = "partial costs block"
BuyingPower = 10000.0
StartDate = "2020-01-01"
EndDate = "2021-01-01"
Tickers = ["AAPL"]
Strategy = "greedy"

  [portfolio.Costs]
  slippage_bps = 3.0
`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.Portfolios) != 3 {
		t.Fatalf("loaded %d portfolios, want 3", len(cfg.Portfolios))
	}

	full := cfg.Portfolios[0].Costs
	closeTo(t, "commission_per_trade", full.CommissionPerTrade, 1.0)
	closeTo(t, "commission_bps", full.CommissionBps, 2.5)
	closeTo(t, "slippage_bps", full.SlippageBps, 5.0)
	if full.Zero() {
		t.Error("a populated Costs block reports Zero()")
	}

	if got := cfg.Portfolios[1].Costs; !got.Zero() {
		t.Errorf("absent Costs block decoded as %+v, want zero", got)
	}

	// Keys omitted inside the block default to 0 rather than to anything
	// clever, so a partial block charges only what it names.
	partial := cfg.Portfolios[2].Costs
	closeTo(t, "partial slippage", partial.SlippageBps, 3.0)
	closeTo(t, "partial commission", partial.CommissionPerTrade, 0)
	closeTo(t, "partial commission bps", partial.CommissionBps, 0)
	if partial.Zero() {
		t.Error("a partial Costs block reports Zero()")
	}
}

// The benchmark is configuration, so it must survive Clone — and it must
// never be mistaken for a holding.
func TestBenchmarkConfigPlumbing(t *testing.T) {
	path := writeConfig(t, `
[[portfolio]]
Name = "benchmarked"
BuyingPower = 10000.0
StartDate = "2020-01-01"
EndDate = "2021-01-01"
Tickers = ["AAPL", "MSFT"]
Strategy = "greedy"
Benchmark = "$SP500"

[[portfolio]]
Name = "unbenchmarked"
BuyingPower = 10000.0
StartDate = "2020-01-01"
EndDate = "2021-01-01"
Tickers = ["AAPL"]
Strategy = "greedy"
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got := cfg.Portfolios[0].Benchmark; got != "$SP500" {
		t.Errorf("Benchmark = %q, want $SP500", got)
	}
	if got := cfg.Portfolios[1].Benchmark; got != "" {
		t.Errorf("absent Benchmark = %q, want empty", got)
	}

	p, err := cfg.Portfolios[0].ToPortfolio()
	if err != nil {
		t.Fatalf("ToPortfolio: %v", err)
	}
	if p.Benchmark != "$SP500" {
		t.Errorf("portfolio Benchmark = %q, want $SP500", p.Benchmark)
	}
	// The critical invariant: the benchmark is not a holding. Tickers drives
	// allocation, the trading-day intersection and valuation, so a benchmark
	// that leaked into it would silently change what the portfolio does.
	for _, tk := range p.Tickers {
		if tk == p.Benchmark {
			t.Fatalf("benchmark %q leaked into Tickers %v", p.Benchmark, p.Tickers)
		}
	}
	if len(p.Tickers) != 2 {
		t.Errorf("Tickers = %v, want the 2 configured holdings", p.Tickers)
	}

	c, err := p.Clone()
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	if c.Benchmark != "$SP500" {
		t.Errorf("clone Benchmark = %q, want $SP500", c.Benchmark)
	}
	if len(c.Tickers) != 2 {
		t.Errorf("clone Tickers = %v, want the 2 configured holdings", c.Tickers)
	}
}

// ToPortfolio has to carry the costs onto the Portfolio, and Clone has to
// carry them onto the object the runner actually simulates. Both links are
// tested because breaking either one is silent.
func TestToPortfolioCarriesCosts(t *testing.T) {
	pc := PortfolioConfig{
		Name:        "p",
		BuyingPower: 1000,
		StartTime:   "2020-01-01",
		EndTime:     "2021-01-01",
		Tickers:     []string{"AAPL"},
		Strategy:    "greedy",
		Costs: CostConfig{
			CommissionPerTrade: 1.0,
			CommissionBps:      2.5,
			SlippageBps:        5.0,
		},
	}
	p, err := pc.ToPortfolio()
	if err != nil {
		t.Fatalf("ToPortfolio: %v", err)
	}
	if p.Costs != pc.Costs {
		t.Fatalf("Costs = %+v, want %+v", p.Costs, pc.Costs)
	}

	c, err := p.Clone()
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	if c.Costs != pc.Costs {
		t.Errorf("clone Costs = %+v, want %+v", c.Costs, pc.Costs)
	}

	// A config with no Costs block yields a portfolio that charges nothing.
	pc.Costs = CostConfig{}
	plain, err := pc.ToPortfolio()
	if err != nil {
		t.Fatalf("ToPortfolio: %v", err)
	}
	if !plain.Costs.Zero() {
		t.Errorf("default portfolio Costs = %+v, want zero", plain.Costs)
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

// --- parameter sweeps -------------------------------------------------------

// sweepCfg is a minimal, valid portfolio config to hang a Sweep block on.
func sweepCfg(params map[string]any, sweep map[string]any) *PortfolioConfig {
	return &PortfolioConfig{
		Name:        "RSI",
		BuyingPower: 10_000,
		StartTime:   "2020-01-01",
		EndTime:     "2020-12-31",
		Tickers:     []string{"AAA"},
		Strategy:    "buyAndHold:equalWeights",
		Params:      params,
		Sweep:       sweep,
	}
}

// paramSets pulls the expanded Params out of a set of portfolios, so a test
// can assert the whole product rather than just its size — a count-only
// assertion passes for a sweep that produces six copies of one combination.
func paramSets(ps []*Portfolio) []map[string]any {
	out := make([]map[string]any, len(ps))
	for i, p := range ps {
		out[i] = p.StrategyParams
	}
	return out
}

func TestSweepExpandsToTheFullCartesianProduct(t *testing.T) {
	cfg := sweepCfg(
		map[string]any{"period": int64(14), "buy_thresh": int64(30)},
		map[string]any{
			"period":     []any{int64(7), int64(14), int64(21)},
			"buy_thresh": []any{int64(20), int64(30)},
		},
	)
	got, err := cfg.ToPortfolios()
	if err != nil {
		t.Fatalf("ToPortfolios: %v", err)
	}
	if len(got) != 6 {
		t.Fatalf("expanded to %d portfolios, want 3x2=6", len(got))
	}

	// Every combination appears exactly once.
	seen := map[string]int{}
	for _, ps := range paramSets(got) {
		seen[fmt.Sprintf("%v/%v", ps["period"], ps["buy_thresh"])]++
	}
	for _, period := range []int64{7, 14, 21} {
		for _, thresh := range []int64{20, 30} {
			key := fmt.Sprintf("%d/%d", period, thresh)
			if seen[key] != 1 {
				t.Errorf("combination %s appeared %d times, want exactly 1",
					key, seen[key])
			}
		}
	}
}

// A swept key replaces its base value; a key that is only in Params must
// survive into every run untouched.
func TestSweepOverridesParamsAndInheritsTheRest(t *testing.T) {
	cfg := sweepCfg(
		map[string]any{"period": int64(14), "buyType": "greedy"},
		map[string]any{"period": []any{int64(7), int64(21)}},
	)
	got, err := cfg.ToPortfolios()
	if err != nil {
		t.Fatalf("ToPortfolios: %v", err)
	}
	for i, ps := range paramSets(got) {
		if ps["buyType"] != "greedy" {
			t.Errorf("run %d lost the unswept base key: buyType = %v", i, ps["buyType"])
		}
		if ps["period"] == int64(14) {
			t.Errorf("run %d kept the base period; the sweep should override it", i)
		}
	}

	// The base map itself must not have been written through.
	if cfg.Params["period"] != int64(14) {
		t.Errorf("expansion mutated the shared base Params: period = %v",
			cfg.Params["period"])
	}
}

// Expansion order must be stable, or two runs of the same config cannot be
// diffed against each other — which is most of what a sweep is for.
func TestSweepOrderIsDeterministic(t *testing.T) {
	cfg := sweepCfg(nil, map[string]any{
		"zeta":  []any{int64(1), int64(2)},
		"alpha": []any{"a", "b"},
		"mid":   []any{true, false},
	})
	var first []string
	for attempt := 0; attempt < 8; attempt++ {
		got, err := cfg.ToPortfolios()
		if err != nil {
			t.Fatalf("ToPortfolios: %v", err)
		}
		names := make([]string, len(got))
		for i, p := range got {
			names[i] = p.Pname
		}
		if attempt == 0 {
			first = names
			continue
		}
		for i := range names {
			if names[i] != first[i] {
				t.Fatalf("expansion %d differs at %d: %q vs %q",
					attempt, i, names[i], first[i])
			}
		}
	}
	// Pin the whole sequence, not just its first entry. Keys are visited in
	// sorted order (alpha, mid, zeta) and the odometer advances the LAST key
	// fastest, so runs sharing a prefix stay adjacent. Asserting only the
	// first name would pass even with the key order reversed, since the
	// leading combination takes index 0 of every list either way.
	want := []string{
		"RSI [alpha=a mid=true zeta=1]",
		"RSI [alpha=a mid=true zeta=2]",
		"RSI [alpha=a mid=false zeta=1]",
		"RSI [alpha=a mid=false zeta=2]",
		"RSI [alpha=b mid=true zeta=1]",
		"RSI [alpha=b mid=true zeta=2]",
		"RSI [alpha=b mid=false zeta=1]",
		"RSI [alpha=b mid=false zeta=2]",
	}
	if !reflect.DeepEqual(first, want) {
		t.Errorf("expansion order:\n got %v\nwant %v", first, want)
	}
}

// Six rows all called "RSI" is an unreadable results table, and the name is
// the only place the parameter set reaches [Output].
func TestSweepNamesAreUniqueAndCarryTheParameters(t *testing.T) {
	cfg := sweepCfg(nil, map[string]any{
		"period": []any{int64(7), int64(21)},
		"k":      []any{2.5},
	})
	got, err := cfg.ToPortfolios()
	if err != nil {
		t.Fatalf("ToPortfolios: %v", err)
	}
	names := map[string]bool{}
	for _, p := range got {
		if names[p.Pname] {
			t.Errorf("duplicate portfolio name %q", p.Pname)
		}
		names[p.Pname] = true
	}
	for _, want := range []string{"RSI [k=2.5 period=7]", "RSI [k=2.5 period=21]"} {
		if !names[want] {
			t.Errorf("missing expected name %q; got %v", want, names)
		}
	}
}

// The regression guard for every config written before sweeps existed.
func TestNoSweepIsExactlyOnePortfolioWithTheBaseParams(t *testing.T) {
	base := map[string]any{"period": int64(14), "buyType": "greedy"}
	cfg := sweepCfg(base, nil)

	got, err := cfg.ToPortfolios()
	if err != nil {
		t.Fatalf("ToPortfolios: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expanded to %d portfolios, want exactly 1", len(got))
	}
	if got[0].Pname != "RSI" {
		t.Errorf("name = %q, want the unchanged %q", got[0].Pname, "RSI")
	}
	if !reflect.DeepEqual(got[0].StrategyParams, base) {
		t.Errorf("Params = %v, want the base map %v", got[0].StrategyParams, base)
	}

	// An empty (but present) block must behave the same as an absent one.
	empty, err := sweepCfg(base, map[string]any{}).ToPortfolios()
	if err != nil {
		t.Fatalf("ToPortfolios (empty sweep): %v", err)
	}
	if len(empty) != 1 || empty[0].Pname != "RSI" {
		t.Errorf("empty sweep gave %d portfolios named %q, want 1 named RSI",
			len(empty), empty[0].Pname)
	}
}

func TestSweepRejectsBadBlocks(t *testing.T) {
	cases := []struct {
		name  string
		sweep map[string]any
		want  string
	}{
		{
			name:  "scalar instead of a list",
			sweep: map[string]any{"period": int64(14)},
			want:  "must be a list",
		},
		{
			name:  "empty list",
			sweep: map[string]any{"period": []any{}},
			want:  "empty list",
		},
		{
			// 11 keys of 2 values is 2048, over the 1000 cap.
			name: "product over the cap",
			sweep: func() map[string]any {
				s := map[string]any{}
				for i := 0; i < 11; i++ {
					s[fmt.Sprintf("k%02d", i)] = []any{int64(1), int64(2)}
				}
				return s
			}(),
			want: "more than 1000 runs",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sweepCfg(nil, tc.sweep).ToPortfolios()
			if err == nil {
				t.Fatal("expected an error, got none")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// The cap must reject before building, not after: a 1000-run refusal that
// first allocates the product is the hang it exists to prevent.
func TestSweepCapRejectsWithoutBuildingTheProduct(t *testing.T) {
	sweep := map[string]any{}
	for i := 0; i < 20; i++ {
		sweep[fmt.Sprintf("k%02d", i)] = []any{int64(1), int64(2), int64(3)}
	}
	done := make(chan error, 1)
	go func() {
		_, err := sweepCfg(nil, sweep).ToPortfolios()
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("3^20 combinations expanded without error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expansion did not refuse promptly; the cap is checked too late")
	}
}

// The TOML key name is a user-facing contract, exactly like Costs and
// Benchmark: renaming the struct tag would silently stop honouring existing
// configs rather than failing loudly.
func TestLoadConfigSweep(t *testing.T) {
	path := writeConfig(t, `
[[Portfolio]]
Name        = "RSI"
BuyingPower = 10000.0
StartDate   = "2020-01-01"
EndDate     = "2020-12-31"
Tickers     = ["AAA"]
Strategy    = "buyAndHold:equalWeights"
[Portfolio.Params]
  period = 14
[Portfolio.Sweep]
  period     = [7, 14, 21]
  buy_thresh = [20, 30]
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.Portfolios) != 1 {
		t.Fatalf("decoded %d portfolios, want 1", len(cfg.Portfolios))
	}
	pc := cfg.Portfolios[0]
	if len(pc.Sweep) != 2 {
		t.Fatalf("Sweep decoded as %v, want 2 keys", pc.Sweep)
	}
	got, err := pc.ToPortfolios()
	if err != nil {
		t.Fatalf("ToPortfolios: %v", err)
	}
	if len(got) != 6 {
		t.Errorf("expanded to %d runs, want 6", len(got))
	}
}

// A config with no Sweep block must decode to a nil Sweep, so the expansion
// path is never entered for the configs that existed before this feature.
func TestLoadConfigNoSweepBlock(t *testing.T) {
	path := writeConfig(t, `
[[Portfolio]]
Name        = "plain"
BuyingPower = 10000.0
StartDate   = "2020-01-01"
EndDate     = "2020-12-31"
Tickers     = ["AAA"]
Strategy    = "buyAndHold:equalWeights"
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Portfolios[0].Sweep != nil {
		t.Errorf("Sweep = %v, want nil for a config that omits the block",
			cfg.Portfolios[0].Sweep)
	}
}
