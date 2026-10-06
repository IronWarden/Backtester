package backtest

import (
	"math"
	"math/rand"
	"my-backtester/src/data"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// rulesPortfolio builds a portfolio the way ToPortfolio would, for either a
// Lua spec or a rule set, so both halves of an equivalence test run through
// the identical runOne day loop.
func rulesPortfolio(t *testing.T, spec string, params map[string]any, benchmark string) *Portfolio {
	t.Helper()
	strat, err := NewStrategy(spec, params)
	if err != nil {
		t.Fatalf("NewStrategy(%q): %v", spec, err)
	}
	return &Portfolio{
		Pname:                "rules-test",
		BuyingPower:          libCash,
		InitialBuyingPower:   libCash,
		Positions:            make(map[string]*Position),
		DailyReturns:         make([]DailyReturn, 0, libDays),
		PortfolioCloseValues: make([]float64, 0, libDays),
		Tickers:              libTickers,
		StrategySpec:         spec,
		StrategyParams:       params,
		Strategy:             strat,
		Benchmark:            benchmark,
	}
}

// sameCurve fails unless two runs produced the same equity curve to the cent.
func sameCurve(t *testing.T, lua, rules *Portfolio) {
	t.Helper()
	if len(lua.PortfolioCloseValues) == 0 || len(lua.PortfolioCloseValues) != len(rules.PortfolioCloseValues) {
		t.Fatalf("curve lengths differ or are empty: lua %d, rules %d",
			len(lua.PortfolioCloseValues), len(rules.PortfolioCloseValues))
	}
	for i := range lua.PortfolioCloseValues {
		if d := math.Abs(lua.PortfolioCloseValues[i] - rules.PortfolioCloseValues[i]); d >= 0.005 {
			t.Fatalf("day %d: lua %.4f, rules %.4f (off by %.4f)",
				i, lua.PortfolioCloseValues[i], rules.PortfolioCloseValues[i], d)
		}
	}
	if len(rules.Trades) == 0 {
		t.Fatal("the rules never traded, so matching proves nothing")
	}
	if len(lua.Trades) != len(rules.Trades) {
		t.Errorf("lua placed %d trades, rules %d", len(lua.Trades), len(rules.Trades))
	}
}

func runBoth(t *testing.T, luaSpec string, luaParams map[string]any, rules RulesConfig) (*Portfolio, *Portfolio) {
	t.Helper()
	lua := rulesPortfolio(t, luaSpec, luaParams, "")
	runOne(lua, synthHist(42), map[int64]float64{})
	if ls, ok := lua.Strategy.(*LuaStrategy); ok {
		ls.Close()
	}
	r := rulesPortfolio(t, RulesSpec, rules.params(), "")
	runOne(r, synthHist(42), map[int64]float64{})
	return lua, r
}

// The shipped momentum_rotation.lua, restated as rules. Its "only the winners
// among the top N" is a rising_N filter ahead of the rank, which selects the
// same set because the rank is descending.
func TestRulesMatchMomentumRotation(t *testing.T) {
	lua, r := runBoth(t, "lua:strategies/momentum_rotation.lua",
		map[string]any{"lookback": int64(60), "top_n": int64(2), "rebalance_days": int64(21)},
		RulesConfig{Rebalance: "monthly", Rank: "return_60", Pick: 2, Filter: "rising_60"})
	sameCurve(t, lua, r)
}

// The shipped rebalance.lua, both equal-weighted and with fixed weights.
func TestRulesMatchRebalance(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		lua, r := runBoth(t, "lua:strategies/rebalance.lua",
			map[string]any{"rebalance_days": int64(63)},
			RulesConfig{Rebalance: "quarterly"})
		sameCurve(t, lua, r)
	})
	t.Run("fixed weights", func(t *testing.T) {
		lua, r := runBoth(t, "lua:strategies/rebalance.lua",
			map[string]any{"rebalance_days": int64(21), "weights": map[string]any{
				"AAA": 60.0, "BBB": 30.0, "CCC": 10.0}},
			RulesConfig{Rebalance: "monthly", Weight: "allocations",
				Allocations: map[string]float64{"AAA": 60, "BBB": 30, "CCC": 10}})
		sameCurve(t, lua, r)
	})
}

// trend_following.lua trades by event (buy_max / sell_all), not by target
// weight, so no rule set reproduces it. Its idea — hold what is above its
// average — is pinned against a Lua twin written with the same primitives
// instead, as is inverse-volatility weighting.
func TestRulesMatchLuaTwins(t *testing.T) {
	cases := []struct {
		name  string
		lua   string
		rules RulesConfig
	}{
		{"above its average, equal", `
function step(day)
  if day < 50 or day % 5 ~= 0 then return end
  local ok = {}
  for i = 1, #tickers do
    local t = tickers[i]
    if price(t, day) > sma(t, day, 50) then ok[#ok + 1] = t end
  end
  local w = {}
  for _, t in ipairs(ok) do w[t] = 1.0 / #ok end
  target_weights(day, w)
end`, RulesConfig{Rebalance: "weekly", Filter: "above_sma_50"}},
		{"inverse volatility", `
function step(day)
  if day < 40 or day % 21 ~= 0 then return end
  local w, total = {}, 0
  for i = 1, #tickers do
    local t = tickers[i]
    local sd, ok = stdev(t, day, 40)
    if ok and sd > 0 then w[t] = 1 / sd; total = total + 1 / sd end
  end
  for t, v in pairs(w) do w[t] = v / total end
  target_weights(day, w)
end`, RulesConfig{Rebalance: "monthly", Weight: "inverse_vol_40"}},
		{"lowest volatility, top 1", `
function step(day)
  if day < 30 or day % 21 ~= 0 then return end
  local ranked = rank(day, function(t)
    local sd, ok = stdev(t, day, 30)
    if ok then return -sd end
    return nil
  end)
  local w = {}
  if #ranked > 0 then w[ranked[1].ticker] = 1.0 end
  target_weights(day, w)
end`, RulesConfig{Rebalance: "monthly", Rank: "low_vol_30", Pick: 1}},
		{"RSI oversold", `
function step(day)
  if day < 14 or day % 5 ~= 0 then return end
  local ok = {}
  for i = 1, #tickers do
    local t = tickers[i]
    if rsi(t, day, 14) < 45 then ok[#ok + 1] = t end
  end
  local w = {}
  for _, t in ipairs(ok) do w[t] = 1.0 / #ok end
  target_weights(day, w)
end`, RulesConfig{Rebalance: "weekly", Filter: "rsi_below_45"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "twin.lua")
			if err := os.WriteFile(path, []byte(tc.lua), 0o644); err != nil {
				t.Fatal(err)
			}
			lua, r := runBoth(t, "lua:"+path, nil, tc.rules)
			sameCurve(t, lua, r)
		})
	}
}

// A signal computed on day d must not change when anything after d does.
// Every rank, filter and risk-off entry is exercised, so a future entry that
// peeks fails here rather than in someone's backtest.
func TestRulesNeverReadThePast(t *testing.T) {
	const d = 300
	for _, e := range rulesMenu {
		if e.Kind == "rebalance" || e.Kind == "weight" && e.Base != "inverse_vol" {
			continue
		}
		form := defaultForm(e)
		t.Run(form, func(t *testing.T) {
			pr, err := parseRule(e.Kind, form)
			if err != nil {
				t.Fatal(err)
			}
			base := synthHist(7)
			bench := base["AAA"]
			before := evalAll(pr, base, bench, d)

			mut := synthHist(7)
			rng := rand.New(rand.NewSource(99))
			for tk, s := range mut {
				for i := d + 1; i < len(s); i++ {
					s[i].Close *= 0.5 + rng.Float64()
				}
				mut[tk] = s
			}
			after := evalAll(pr, mut, mut["AAA"], d)
			if before != after {
				t.Fatalf("day %d changed when only later bars moved:\n before %s\n after  %s", d, before, after)
			}
		})
	}
}

func defaultForm(e RuleEntry) string {
	form := e.Base
	for _, a := range e.Args {
		form += "_" + strconv.Itoa(a.Default)
	}
	return form
}

func evalAll(pr parsedRule, hist map[string][]data.AssetData, bench []data.AssetData, day int) string {
	var b strings.Builder
	for _, t := range libTickers {
		switch pr.entry.Kind {
		case "rank":
			v, ok := evalRuleRank(pr, hist[t], day)
			b.WriteString(t + "=" + ftoa(v) + boolS(ok) + " ")
		case "filter":
			v, ok := evalRuleFilter(pr, hist[t], day)
			b.WriteString(t + "=" + boolS(v) + boolS(ok) + " ")
		case "weight":
			w := weigh(compiledRules{weight: pr}, []string{t}, hist, day)
			b.WriteString(t + "=" + ftoa(w[t]) + " ")
		}
	}
	if pr.entry.Kind == "riskoff" {
		v, ok := evalRuleRiskOff(pr, bench, day)
		b.WriteString(boolS(v) + boolS(ok))
	}
	return b.String()
}

func ftoa(v float64) string { return strconv.FormatFloat(v, 'g', 12, 64) }

func boolS(b bool) string {
	if b {
		return "T"
	}
	return "F"
}

// Every menu entry parses from its own Form with default numbers, yields a
// real (ok) value on a day with enough history, and has a phrase with every
// hole filled — so adding an entry without implementing it breaks the build.
func TestEveryRuleEntryIsImplemented(t *testing.T) {
	hist := synthHist(42)
	for _, e := range rulesMenu {
		form := defaultForm(e)
		pr, err := parseRule(e.Kind, form)
		if err != nil {
			t.Errorf("%s: its own default form does not parse: %v", e.Form, err)
			continue
		}
		if strings.TrimSpace(e.Phrase) == "" || strings.TrimSpace(e.Summary) == "" {
			t.Errorf("%s: has no phrase or summary", e.Form)
		}
		if got := fill(e.Phrase, pr.args, "$SP500"); strings.Contains(got, "{") {
			t.Errorf("%s: phrase keeps an unfilled hole: %q", e.Form, got)
		}
		day := 350
		var ok bool
		switch e.Kind {
		case "rank":
			_, ok = evalRuleRank(pr, hist["AAA"], day)
		case "filter":
			_, ok = evalRuleFilter(pr, hist["AAA"], day)
		case "riskoff":
			_, ok = evalRuleRiskOff(pr, hist["AAA"], day)
		case "weight":
			ok = len(weigh(compiledRules{weight: pr, cfg: RulesConfig{
				Allocations: map[string]float64{"AAA": 1}}}, []string{"AAA"}, hist, day)) == 1
		case "rebalance":
			_, ok = rebalanceEvery[e.Base]
		}
		if !ok {
			t.Errorf("%s: produced no value on day %d, so it is not implemented", e.Form, day)
		}
	}
}

// Keep lets a holding that slips a little stay put.
func TestRulesKeepHoldsASlippingName(t *testing.T) {
	hist := synthHist(42)
	p := rulesPortfolio(t, RulesSpec, RulesConfig{
		Rebalance: "daily", Rank: "return_20", Pick: 1, Keep: 3}.params(), "")
	s := p.Strategy.(*RulesStrategy)
	s.Step(p, hist, 25)
	first := ""
	for tk, pos := range p.Positions {
		if pos != nil && pos.Amount > 0 {
			first = tk
		}
	}
	if first == "" {
		t.Fatal("nothing was bought")
	}
	// Force the held name to the bottom of a top-3 that still includes it:
	// with three tickers, anything held is inside Keep = 3, so it must stay.
	for day := 26; day < 60; day++ {
		s.Step(p, hist, day)
		if pos, ok := p.FindPosition(first); !ok || pos == nil || pos.Amount <= 0 {
			t.Fatalf("day %d: %s was sold although it never left the top 3", day, first)
		}
	}
}

// A risk-off rule empties the book while the market condition holds.
func TestRulesRiskOffGoesToCash(t *testing.T) {
	hist := synthHist(42)
	// A benchmark that collapses halfway through.
	bench := make([]data.AssetData, libDays)
	for i := range bench {
		bench[i] = hist["AAA"][i]
		bench[i].Close = 100
		if i >= 250 {
			bench[i].Close = 50
		}
	}
	hist["$BM"] = bench
	p := rulesPortfolio(t, RulesSpec, RulesConfig{
		Rebalance: "daily", RiskOff: "benchmark_below_sma_20"}.params(), "$BM")
	runOne(p, hist, map[int64]float64{})

	// Read from the curve: an invested day moves, a day in cash is flat.
	c := p.PortfolioCloseValues
	heldAt249 := c[249] != c[248]
	heldAt260 := c[260] != c[259]
	if !heldAt249 {
		t.Error("the book was not invested before the benchmark fell")
	}
	if heldAt260 {
		t.Error("the book was still invested while the benchmark sat below its average")
	}
}

func TestRulesValidation(t *testing.T) {
	tickers := libTickers
	cases := []struct {
		name  string
		r     RulesConfig
		bench string
		want  string
	}{
		{"no rebalance", RulesConfig{}, "", "Rebalance is required"},
		{"bad rebalance", RulesConfig{Rebalance: "montly"}, "", "did you mean monthly"},
		{"typo in rank", RulesConfig{Rebalance: "monthly", Rank: "retrun_126", Pick: 1}, "", "did you mean return_126"},
		{"rank without pick", RulesConfig{Rebalance: "monthly", Rank: "return_126"}, "", "Pick is not"},
		{"pick without rank", RulesConfig{Rebalance: "monthly", Pick: 2}, "", "need a Rank"},
		{"pick too many", RulesConfig{Rebalance: "monthly", Rank: "return_126", Pick: 5}, "", "only 3 tickers"},
		{"keep below pick", RulesConfig{Rebalance: "monthly", Rank: "return_126", Pick: 2, Keep: 1}, "", "cannot be below Pick"},
		{"out of range", RulesConfig{Rebalance: "monthly", Filter: "above_sma_5000"}, "", "between 2 and 1000"},
		{"cross backwards", RulesConfig{Rebalance: "monthly", Filter: "sma_cross_200_50"}, "", "must be shorter"},
		{"risk-off needs benchmark", RulesConfig{Rebalance: "monthly", RiskOff: "benchmark_below_sma_200"}, "", "none is set"},
		{"allocations missing", RulesConfig{Rebalance: "monthly", Weight: "allocations"}, "", "is empty"},
		{"allocations stranger", RulesConfig{Rebalance: "monthly", Weight: "allocations",
			Allocations: map[string]float64{"ZZZ": 1}}, "", "not in Tickers"},
		{"allocations unused", RulesConfig{Rebalance: "monthly",
			Allocations: map[string]float64{"AAA": 1}}, "", "would be ignored"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compileRules(tc.r, tickers, tc.bench)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

func TestRulesInAConfig(t *testing.T) {
	base := `
[[Portfolio]]
Name = "R"
BuyingPower = 10000
StartDate = "2020-01-02"
EndDate = "2024-01-02"
Tickers = ["AAA", "BBB", "CCC"]
Benchmark = "$SP500"
`
	rules := `
[Portfolio.Rules]
Rebalance = "monthly"
Rank = "return_126"
Pick = 2
Keep = 3
Filter = "above_sma_200"
RiskOff = "benchmark_below_sma_200"
`
	t.Run("runs as the rules spec", func(t *testing.T) {
		cfg, err := LoadConfigText(base + rules)
		if err != nil {
			t.Fatal(err)
		}
		pc := cfg.Portfolios[0]
		if err := pc.ResolveStrategy(""); err != nil {
			t.Fatalf("ResolveStrategy: %v", err)
		}
		ps, err := pc.ToPortfolios()
		if err != nil {
			t.Fatalf("ToPortfolios: %v", err)
		}
		if ps[0].StrategySpec != RulesSpec || ps[0].StrategyParams["rank"] != "return_126" {
			t.Fatalf("spec %q params %v", ps[0].StrategySpec, ps[0].StrategyParams)
		}
		clone, err := ps[0].Clone()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := clone.Strategy.(*RulesStrategy); !ok {
			t.Fatalf("a clone lost the rules: %T", clone.Strategy)
		}
	})
	for _, tc := range []struct{ name, extra, want string }{
		{"with a Strategy", `Strategy = "greedy"` + "\n", "pick one"},
		{"with a Sweep", "[Portfolio.Sweep]\nx = [1, 2]\n", "cannot vary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadConfigText(base + tc.extra + rules)
			if err != nil {
				t.Fatal(err)
			}
			pc := cfg.Portfolios[0]
			err = pc.ResolveStrategy("")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
	t.Run("a window shorter than the warm-up", func(t *testing.T) {
		cfg, err := LoadConfigText(strings.Replace(base, `"2024-01-02"`, `"2020-03-02"`, 1) + rules)
		if err != nil {
			t.Fatal(err)
		}
		_, err = cfg.Portfolios[0].ToPortfolios()
		if err == nil || !strings.Contains(err.Error(), "200 trading days") {
			t.Fatalf("got %v, want the warm-up refused", err)
		}
	})
}

// The sentence for the example in TASKS.md and README.md. A change here is a
// change to what every user reads, so it is pinned word for word.
func TestDescribeRules(t *testing.T) {
	got := DescribeRules(RulesConfig{
		Rebalance: "monthly", Rank: "return_126", Pick: 3, Keep: 5,
		Filter: "above_sma_200", RiskOff: "benchmark_below_sma_200",
	}, []string{"A", "B", "C", "D", "E", "F"}, "$SP500")
	want := "Every 21 trading days (about monthly), hold the 3 stocks that rose most " +
		"over the last 126 trading days, counting only those trading above their " +
		"200-day average, equally weighted; keep a holding while it stays in the " +
		"top 5. Move everything to cash whenever the S&P 500 is below its 200-day " +
		"average. If none qualify, the money waits in cash. The first decision " +
		"waits for 200 trading days of history; until then it holds cash."
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}

	if got := DescribeRules(RulesConfig{Rebalance: "quarterly", Weight: "allocations",
		Allocations: map[string]float64{"A": 3, "B": 1}}, []string{"A", "B"}, ""); got !=
		"Every 63 trading days (about quarterly), hold every one of these stocks, "+
			"in fixed proportions (A 75%, B 25%)." {
		t.Errorf("fixed weights: %s", got)
	}
	head := RulesHeadline(RulesConfig{Rebalance: "monthly", Rank: "return_126", Pick: 3,
		Filter: "above_sma_200"}, []string{"A", "B", "C", "D"}, "")
	if head != "hold the 3 stocks that rose most over the last 126 trading days, "+
		"counting only those trading above their 200-day average" {
		t.Errorf("headline: %q", head)
	}
	if !strings.Contains(DescribeRules(RulesConfig{Rebalance: "monthly", Rank: "return_126", Pick: 3,
		Filter: "above_sma_200"}, []string{"A", "B", "C", "D"}, ""), head) {
		t.Error("the headline is not a verbatim piece of the sentence")
	}
	if RulesHeadline(RulesConfig{}, nil, "") != "" {
		t.Error("rules that cannot run have no headline")
	}
	if got := DescribeRules(RulesConfig{}, nil, ""); !strings.HasPrefix(got, "These rules cannot run:") {
		t.Errorf("an invalid rule set rendered as a sentence: %s", got)
	}
}

// Curve equivalence cannot pin a signal's definition on its own: an average
// that wrongly includes today's close flips a filter so rarely that a 400-day
// curve can come out identical. So each Go signal is compared with the Lua
// primitive it claims to mirror, on every day and every ticker.
func TestRuleSignalsEqualTheLuaPrimitives(t *testing.T) {
	script := `
function step(day)
  if day < 60 then return end
  for i = 1, #tickers do
    local t = tickers[i]
    local r = roc(t, day, 60)
    local sd = stdev(t, day, 30)
    log(string.format("%s %d %.12f %.12f %.12f %.12f", t, day,
      sma(t, day, 50), rsi(t, day, 14), r, sd))
  end
end`
	path := filepath.Join(t.TempDir(), "probe.lua")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	var lines []string
	Diagnostics = func(d Diagnostic) {
		if d.Level == "info" {
			lines = append(lines, d.Message)
		}
	}
	defer func() { Diagnostics = nil }()

	p := rulesPortfolio(t, "lua:"+path, nil, "")
	hist := synthHist(42)
	for day := 0; day < libDays; day++ {
		p.Strategy.Step(p, hist, day)
	}
	p.Strategy.(*LuaStrategy).Close()

	checked := 0
	for _, line := range lines {
		f := strings.Fields(line)
		if len(f) < 6 {
			continue
		}
		f = f[len(f)-6:]
		tk := f[0]
		day, _ := strconv.Atoi(f[1])
		want := make([]float64, 4)
		for i := range want {
			want[i], _ = strconv.ParseFloat(f[2+i], 64)
		}
		s := hist[tk]
		ma, _ := smaPrior(s, day, 50)
		rs, _ := rsiAt(s, day, 14)
		r, _ := roc(s, day, 60)
		rets, _ := strictReturns(s, day, 30)
		sd, _ := sampleStdev(rets)
		for i, got := range []float64{ma, rs, r, sd} {
			if math.Abs(got-want[i]) > 1e-9 {
				t.Fatalf("%s day %d: %s is %.12f in Go but %.12f in Lua",
					tk, day, []string{"sma", "rsi", "roc", "stdev"}[i], got, want[i])
			}
		}
		checked++
	}
	if checked < 3*(libDays-60) {
		t.Fatalf("compared only %d values; the probe's log lines were not parsed", checked)
	}
}
