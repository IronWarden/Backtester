package backtest

// Strategy rules: a strategy written as a handful of choices from a fixed
// menu, instead of as a script.
//
// The point is who can check it. A beginner who asks the assistant for "hold
// whatever has been rising, but get out when the market turns" cannot read the
// Lua that comes back, so they cannot tell whether it does what they meant. A
// rule set they can read, because DescribeRules turns it into one sentence,
// and the UI, the assistant and the results all show that same sentence.
//
// Three properties the design leans on:
//
//   - Every signal is written once, here, in Go, and reads nothing past the
//     decision day. Nobody writing rules can write a look-ahead bug, because
//     nobody writing rules writes code.
//   - The signals are defined exactly as the Lua primitives define them (roc,
//     sma, rsi, stdev), and orders go through rebalanceShares — the same code
//     target_weights uses. rules_test.go pins a rule set against its Lua twin to
//     the cent; a disagreement is a bug in whichever one is new.
//   - A rule set is carried as a flat params map on the "rules" spec, so it
//     survives Clone, sweeps of the runner, the research log's fingerprint and
//     robustness re-runs with no new Portfolio field.

import (
	"fmt"
	"math"
	"my-backtester/src/data"
	"sort"
	"strconv"
	"strings"
	"time"
)

// RulesSpec is the Strategy spec a rule set runs under.
const RulesSpec = "rules"

// RulesConfig is [Portfolio.Rules]. Absent (nil) means the portfolio runs its
// Strategy exactly as before; present, it replaces Strategy.
type RulesConfig struct {
	// Rebalance is how often the book is re-decided: daily, weekly, monthly,
	// quarterly or annually — counted in TRADING days (1, 5, 21, 63, 252),
	// which is what the library scripts have always meant by "monthly".
	Rebalance string `toml:"Rebalance" json:"rebalance"`
	// Rank orders the candidates, strongest first. Empty means no ranking:
	// hold every ticker that passes Filter.
	Rank string `toml:"Rank" json:"rank"`
	// Pick is how many of the ranked names to hold. Required with Rank.
	Pick int `toml:"Pick" json:"pick"`
	// Keep lets a holding stay while it is still in the top Keep, so a name
	// that slips from 3rd to 4th is not sold and bought back. 0 means Pick.
	Keep int `toml:"Keep" json:"keep"`
	// Filter is a test a ticker must pass to be held at all. Optional.
	Filter string `toml:"Filter" json:"filter"`
	// Weight splits the money across what is held: equal (the default),
	// inverse_vol_N, or allocations.
	Weight string `toml:"Weight" json:"weight"`
	// Allocations are the relative weights Weight = "allocations" uses.
	Allocations map[string]float64 `toml:"Allocations" json:"allocations"`
	// RiskOff moves the whole book to cash while it is true. It reads the
	// portfolio's Benchmark, so it needs one.
	RiskOff string `toml:"RiskOff" json:"riskOff"`
}

// RuleArg is one number in a menu entry, e.g. the 126 in return_126.
type RuleArg struct {
	Name string `json:"name"`
	Min  int    `json:"min"`
	Max  int    `json:"max"`
	// Default is what the UI fills in when the entry is first chosen.
	Default int `json:"default"`
}

// RuleEntry is one item on the menu. Form is how it is written in a config
// ("return_N"); Phrase is how it reads in the sentence, with {N}-style holes.
type RuleEntry struct {
	Kind    string    `json:"kind"` // rebalance | rank | filter | riskoff | weight
	Base    string    `json:"base"`
	Form    string    `json:"form"`
	Args    []RuleArg `json:"args"`
	Phrase  string    `json:"phrase"`
	Summary string    `json:"summary"`
}

var (
	argDays   = func(def int) RuleArg { return RuleArg{Name: "N", Min: 2, Max: 1000, Default: def} }
	argRSI    = RuleArg{Name: "X", Min: 1, Max: 99, Default: 30}
	argDDPct  = RuleArg{Name: "X", Min: 1, Max: 90, Default: 20}
	argShort  = RuleArg{Name: "S", Min: 2, Max: 500, Default: 50}
	argLong   = RuleArg{Name: "L", Min: 3, Max: 1000, Default: 200}
	rsiPeriod = 14
	// The benchmark drawdown rule measures from the highest close of the last
	// year of trading days.
	ddLookback = 252
)

// rulesMenu is the whole vocabulary. Adding an entry here without a phrase, or
// without an evaluator in the switch statements below, fails
// TestEveryRuleEntryIsImplemented.
var rulesMenu = []RuleEntry{
	{Kind: "rebalance", Base: "daily", Form: "daily", Phrase: "Every trading day", Summary: "Re-decide every trading day."},
	{Kind: "rebalance", Base: "weekly", Form: "weekly", Phrase: "Every 5 trading days (about weekly)", Summary: "Re-decide every 5 trading days."},
	{Kind: "rebalance", Base: "monthly", Form: "monthly", Phrase: "Every 21 trading days (about monthly)", Summary: "Re-decide every 21 trading days."},
	{Kind: "rebalance", Base: "quarterly", Form: "quarterly", Phrase: "Every 63 trading days (about quarterly)", Summary: "Re-decide every 63 trading days."},
	{Kind: "rebalance", Base: "annually", Form: "annually", Phrase: "Every 252 trading days (about yearly)", Summary: "Re-decide every 252 trading days."},

	{Kind: "rank", Base: "return", Form: "return_N", Args: []RuleArg{argDays(126)}, Phrase: "rose most over the last {N} trading days", Summary: "Total price change over the last N trading days; highest first."},
	{Kind: "rank", Base: "momentum_12_1", Form: "momentum_12_1", Phrase: "rose most over the past year, not counting the latest month", Summary: "Return from 273 to 21 trading days ago — classic 12-1 momentum; highest first."},
	{Kind: "rank", Base: "low_vol", Form: "low_vol_N", Args: []RuleArg{argDays(63)}, Phrase: "moved least day to day over the last {N} trading days", Summary: "Standard deviation of daily returns over N days; LOWEST first."},
	{Kind: "rank", Base: "drawdown", Form: "drawdown_N", Args: []RuleArg{argDays(252)}, Phrase: "are closest to their highest close of the last {N} trading days", Summary: "How far the close sits below its N-day highest close; closest first."},

	{Kind: "filter", Base: "above_sma", Form: "above_sma_N", Args: []RuleArg{argDays(200)}, Phrase: "trading above their {N}-day average", Summary: "Close is above the average of the previous N closes."},
	{Kind: "filter", Base: "below_sma", Form: "below_sma_N", Args: []RuleArg{argDays(200)}, Phrase: "trading below their {N}-day average", Summary: "Close is below the average of the previous N closes."},
	{Kind: "filter", Base: "sma_cross", Form: "sma_cross_S_L", Args: []RuleArg{argShort, argLong}, Phrase: "whose {S}-day average is above their {L}-day average", Summary: "The short average is above the long one (S must be smaller than L)."},
	{Kind: "filter", Base: "rising", Form: "rising_N", Args: []RuleArg{argDays(126)}, Phrase: "up over the last {N} trading days", Summary: "Price is higher than N trading days ago."},
	{Kind: "filter", Base: "rsi_below", Form: "rsi_below_X", Args: []RuleArg{argRSI}, Phrase: "with a 14-day RSI below {X}", Summary: "14-day RSI is below X (oversold when X is around 30)."},
	{Kind: "filter", Base: "rsi_above", Form: "rsi_above_X", Args: []RuleArg{{Name: "X", Min: 1, Max: 99, Default: 50}}, Phrase: "with a 14-day RSI above {X}", Summary: "14-day RSI is above X."},

	{Kind: "riskoff", Base: "benchmark_below_sma", Form: "benchmark_below_sma_N", Args: []RuleArg{argDays(200)}, Phrase: "{B} is below its {N}-day average", Summary: "The benchmark's close is below the average of its previous N closes."},
	{Kind: "riskoff", Base: "benchmark_drawdown_over", Form: "benchmark_drawdown_over_X", Args: []RuleArg{argDDPct}, Phrase: "{B} is more than {X}% below its highest close of the past year", Summary: "The benchmark has fallen more than X% from its 252-day highest close."},

	{Kind: "weight", Base: "equal", Form: "equal", Phrase: "equally weighted", Summary: "Split the money evenly."},
	{Kind: "weight", Base: "inverse_vol", Form: "inverse_vol_N", Args: []RuleArg{argDays(63)}, Phrase: "weighted so the steadier ones get more (by {N}-day volatility)", Summary: "Weight each holding by 1 / its N-day volatility."},
	{Kind: "weight", Base: "allocations", Form: "allocations", Phrase: "in fixed proportions", Summary: "Weight by the Allocations table, rescaled over what is held."},
}

// RulesVocabulary is the menu, for the UI's controls and the assistant's
// prompt, so neither can offer an entry the engine would refuse.
func RulesVocabulary() []RuleEntry {
	out := make([]RuleEntry, len(rulesMenu))
	copy(out, rulesMenu)
	return out
}

// parsedRule is a menu entry with its numbers filled in.
type parsedRule struct {
	entry RuleEntry
	args  map[string]int
}

// parseRule reads one config value against the entries of one kind.
func parseRule(kind, raw string) (parsedRule, error) {
	s := strings.TrimSpace(raw)
	var forms []string
	for _, e := range rulesMenu {
		if e.Kind != kind {
			continue
		}
		forms = append(forms, e.Form)
		if len(e.Args) == 0 {
			if s == e.Base {
				return parsedRule{entry: e, args: map[string]int{}}, nil
			}
			continue
		}
		if !strings.HasPrefix(s, e.Base+"_") {
			continue
		}
		parts := strings.Split(strings.TrimPrefix(s, e.Base+"_"), "_")
		if len(parts) != len(e.Args) {
			continue
		}
		args := make(map[string]int, len(parts))
		for i, a := range e.Args {
			n, err := strconv.Atoi(parts[i])
			if err != nil {
				return parsedRule{}, fmt.Errorf(
					"%q: %s must be a whole number, got %q (written as %s)",
					s, a.Name, parts[i], e.Form)
			}
			if n < a.Min || n > a.Max {
				return parsedRule{}, fmt.Errorf(
					"%q: %s must be between %d and %d, got %d",
					s, a.Name, a.Min, a.Max, n)
			}
			args[a.Name] = n
		}
		if e.Base == "sma_cross" && args["S"] >= args["L"] {
			return parsedRule{}, fmt.Errorf(
				"%q: the short average (%d days) must be shorter than the long one (%d days)",
				s, args["S"], args["L"])
		}
		return parsedRule{entry: e, args: args}, nil
	}
	return parsedRule{}, fmt.Errorf("%q is not a %s rule%s; the choices are %s",
		s, kindNoun(kind), suggestRule(kind, s), strings.Join(forms, ", "))
}

func kindNoun(kind string) string {
	switch kind {
	case "riskoff":
		return "risk-off"
	}
	return kind
}

// suggestRule offers the entry whose name is closest, keeping the numbers the
// user typed: "retrun_126" → "did you mean return_126?".
func suggestRule(kind, s string) string {
	base, tail := s, ""
	if i := strings.IndexAny(s, "0123456789"); i > 0 {
		base, tail = strings.TrimSuffix(s[:i], "_"), s[i:]
	}
	var bases []string
	byBase := map[string]RuleEntry{}
	for _, e := range rulesMenu {
		if e.Kind == kind {
			bases = append(bases, e.Base)
			byBase[e.Base] = e
		}
	}
	hint := didYouMean(base, bases)
	if !strings.HasPrefix(hint, " -- did you mean") {
		return ""
	}
	best := strings.TrimSuffix(strings.TrimPrefix(hint, ` -- did you mean "`), `"?`)
	if e := byBase[best]; len(e.Args) > 0 && tail != "" {
		best += "_" + tail
	}
	return fmt.Sprintf(" (did you mean %s?)", best)
}

var rebalanceEvery = map[string]int{
	"daily": 1, "weekly": 5, "monthly": 21, "quarterly": 63, "annually": 252,
}

// compiledRules is a validated rule set, ready to evaluate.
type compiledRules struct {
	cfg     RulesConfig
	every   int
	rank    *parsedRule
	filter  *parsedRule
	riskOff *parsedRule
	weight  parsedRule
	keep    int
	warmup  int
}

// compileRules validates a rule set against the portfolio it belongs to. The
// errors are written for the person who will read them in the status bar or
// in the assistant's reply, so each names the field and what would fix it.
func compileRules(r RulesConfig, tickers []string, benchmark string) (*compiledRules, error) {
	c := &compiledRules{cfg: r}
	reb := strings.TrimSpace(r.Rebalance)
	if reb == "" {
		return nil, fmt.Errorf("Rules.Rebalance is required: daily, weekly, monthly, quarterly or annually")
	}
	every, ok := rebalanceEvery[reb]
	if !ok {
		_, err := parseRule("rebalance", reb)
		return nil, fmt.Errorf("Rules.Rebalance: %v", err)
	}
	c.every = every

	if strings.TrimSpace(r.Rank) != "" {
		pr, err := parseRule("rank", r.Rank)
		if err != nil {
			return nil, fmt.Errorf("Rules.Rank: %v", err)
		}
		c.rank = &pr
		if r.Pick <= 0 {
			return nil, fmt.Errorf("Rules.Rank is set but Pick is not: say how many of the top-ranked stocks to hold")
		}
	} else if r.Pick != 0 || r.Keep != 0 {
		return nil, fmt.Errorf("Rules.Pick and Keep need a Rank to pick from; without Rank every stock that passes the Filter is held")
	}
	if r.Pick < 0 || r.Keep < 0 {
		return nil, fmt.Errorf("Rules.Pick and Keep cannot be negative")
	}
	if r.Pick > len(tickers) {
		return nil, fmt.Errorf("Rules.Pick is %d but the portfolio lists only %d %s",
			r.Pick, len(tickers), plural(len(tickers), "ticker", "tickers"))
	}
	c.keep = r.Keep
	if c.keep == 0 {
		c.keep = r.Pick
	}
	if c.keep < r.Pick {
		return nil, fmt.Errorf("Rules.Keep (%d) cannot be below Pick (%d): Keep is how far a holding may slip before it is sold", r.Keep, r.Pick)
	}

	if strings.TrimSpace(r.Filter) != "" {
		pr, err := parseRule("filter", r.Filter)
		if err != nil {
			return nil, fmt.Errorf("Rules.Filter: %v", err)
		}
		c.filter = &pr
	}
	if strings.TrimSpace(r.RiskOff) != "" {
		pr, err := parseRule("riskoff", r.RiskOff)
		if err != nil {
			return nil, fmt.Errorf("Rules.RiskOff: %v", err)
		}
		if strings.TrimSpace(benchmark) == "" {
			return nil, fmt.Errorf("Rules.RiskOff reads the market through the portfolio's Benchmark, and none is set; add Benchmark = \"$SP500\" or remove RiskOff")
		}
		c.riskOff = &pr
	}

	weight := strings.TrimSpace(r.Weight)
	if weight == "" {
		weight = "equal"
	}
	pw, err := parseRule("weight", weight)
	if err != nil {
		return nil, fmt.Errorf("Rules.Weight: %v", err)
	}
	c.weight = pw
	if pw.entry.Base == "allocations" {
		if len(r.Allocations) == 0 {
			return nil, fmt.Errorf("Rules.Weight is \"allocations\" but Rules.Allocations is empty")
		}
		listed := make(map[string]bool, len(tickers))
		for _, t := range tickers {
			listed[t] = true
		}
		var total float64
		for t, w := range r.Allocations {
			if !listed[t] {
				return nil, fmt.Errorf("Rules.Allocations names %q, which is not in Tickers", t)
			}
			if w < 0 || math.IsNaN(w) || math.IsInf(w, 0) {
				return nil, fmt.Errorf("Rules.Allocations for %q is %v; weights must be zero or positive", t, w)
			}
			total += w
		}
		if total <= 0 {
			return nil, fmt.Errorf("Rules.Allocations adds up to zero, so nothing would ever be bought")
		}
	} else if len(r.Allocations) > 0 {
		return nil, fmt.Errorf("Rules.Allocations is set but Weight is %q, so it would be ignored; set Weight = \"allocations\" or remove it", weight)
	}

	for _, pr := range []*parsedRule{c.rank, c.filter, c.riskOff, &c.weight} {
		if pr != nil {
			if w := ruleWarmup(*pr); w > c.warmup {
				c.warmup = w
			}
		}
	}
	return c, nil
}

// ruleWarmup is how many prior trading days an entry reads, so the first
// decision can wait until every signal is real rather than a neutral default.
func ruleWarmup(pr parsedRule) int {
	switch pr.entry.Base {
	case "momentum_12_1":
		return 273
	case "sma_cross":
		return pr.args["L"]
	case "rsi_below", "rsi_above":
		return rsiPeriod
	case "benchmark_drawdown_over":
		return ddLookback - 1
	case "drawdown":
		return pr.args["N"] - 1
	}
	return pr.args["N"] // 0 for entries without N
}

// weekdaysBetween over-counts trading days (it ignores holidays), which is the
// safe direction for refusing a window: if even the weekdays cannot cover the
// warm-up, the trading days certainly cannot.
func weekdaysBetween(start, end time.Time) int {
	n := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		if wd := d.Weekday(); wd != time.Saturday && wd != time.Sunday {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// The sentence

// DescribeRules renders a rule set as the sentence a user confirms. It is the
// one renderer: the UI, the assistant's check_rules tool and the results all
// call it, so they cannot describe one strategy differently. An invalid rule
// set renders as its error, prefixed, rather than as a plausible sentence.
func DescribeRules(r RulesConfig, tickers []string, benchmark string) string {
	c, err := compileRules(r, tickers, benchmark)
	if err != nil {
		return "These rules cannot run: " + err.Error()
	}
	return c.describe(benchmark)
}

func fill(phrase string, args map[string]int, benchmark string) string {
	out := phrase
	for k, v := range args {
		out = strings.ReplaceAll(out, "{"+k+"}", strconv.Itoa(v))
	}
	return strings.ReplaceAll(out, "{B}", benchmarkLabel(benchmark))
}

func benchmarkLabel(b string) string {
	switch b {
	case "$SP500":
		return "the S&P 500"
	case "$USTOT":
		return "the total US market"
	}
	return b
}

func (c *compiledRules) describe(benchmark string) string {
	var b strings.Builder
	var reb RuleEntry
	for _, e := range rulesMenu {
		if e.Kind == "rebalance" && e.Base == c.cfg.Rebalance {
			reb = e
		}
	}
	b.WriteString(reb.Phrase)
	b.WriteString(", ")
	b.WriteString(c.holdClause(benchmark))
	b.WriteString(", ")
	b.WriteString(c.weightPhrase(benchmark))
	if c.rank != nil && c.keep > c.cfg.Pick {
		fmt.Fprintf(&b, "; keep a holding while it stays in the top %d", c.keep)
	}
	b.WriteString(".")
	if c.riskOff != nil {
		b.WriteString(" Move everything to cash whenever ")
		b.WriteString(fill(c.riskOff.entry.Phrase, c.riskOff.args, benchmark))
		b.WriteString(".")
	}
	if c.filter != nil {
		b.WriteString(" If none qualify, the money waits in cash.")
	}
	if c.warmup > 0 {
		fmt.Fprintf(&b, " The first decision waits for %d trading days of history; until then it holds cash.", c.warmup)
	}
	return b.String()
}

// holdClause is what the rules hold, as the sentence says it: "hold the 3
// stocks that rose most over the last 126 trading days, counting only those
// trading above their 200-day average".
func (c *compiledRules) holdClause(benchmark string) string {
	var b strings.Builder
	b.WriteString("hold ")
	switch {
	case c.rank == nil && c.filter == nil:
		b.WriteString("every one of these stocks")
	case c.rank == nil:
		b.WriteString("every one of these stocks ")
		b.WriteString(fill(c.filter.entry.Phrase, c.filter.args, benchmark))
	default:
		if c.cfg.Pick == 1 {
			b.WriteString("the one stock that ")
		} else {
			fmt.Fprintf(&b, "the %d stocks that ", c.cfg.Pick)
		}
		b.WriteString(fill(c.rank.entry.Phrase, c.rank.args, benchmark))
		if c.filter != nil {
			b.WriteString(", counting only those ")
			b.WriteString(fill(c.filter.entry.Phrase, c.filter.args, benchmark))
		}
	}
	return b.String()
}

// RulesHeadline is the sentence's promise in a few words, for a button:
// the hold clause, which is a verbatim piece of DescribeRules, so the label
// and the sentence cannot disagree. Empty when the rules cannot run.
func RulesHeadline(r RulesConfig, tickers []string, benchmark string) string {
	c, err := compileRules(r, tickers, benchmark)
	if err != nil {
		return ""
	}
	return c.holdClause(benchmark)
}

func (c *compiledRules) weightPhrase(benchmark string) string {
	if c.weight.entry.Base != "allocations" {
		return fill(c.weight.entry.Phrase, c.weight.args, benchmark)
	}
	var total float64
	keys := make([]string, 0, len(c.cfg.Allocations))
	for t, w := range c.cfg.Allocations {
		total += w
		keys = append(keys, t)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, t := range keys {
		if w := c.cfg.Allocations[t]; w > 0 {
			parts = append(parts, fmt.Sprintf("%s %.0f%%", t, 100*w/total))
		}
	}
	return "in fixed proportions (" + strings.Join(parts, ", ") + ")"
}

// ---------------------------------------------------------------------------
// Carrying a rule set on the spec

// params flattens a rule set into StrategyParams: plain keys and values, so
// the research log's fingerprint and the bundle show it readably.
func (r RulesConfig) params() map[string]any {
	m := map[string]any{"rebalance": r.Rebalance}
	put := func(k, v string) {
		if strings.TrimSpace(v) != "" {
			m[k] = v
		}
	}
	put("rank", r.Rank)
	put("filter", r.Filter)
	put("weight", r.Weight)
	put("risk_off", r.RiskOff)
	if r.Pick != 0 {
		m["pick"] = r.Pick
	}
	if r.Keep != 0 {
		m["keep"] = r.Keep
	}
	if len(r.Allocations) > 0 {
		a := make(map[string]any, len(r.Allocations))
		for t, w := range r.Allocations {
			a[t] = w
		}
		m["allocations"] = a
	}
	return m
}

func rulesFromParams(m map[string]any) RulesConfig {
	str := func(k string) string { s, _ := m[k].(string); return s }
	num := func(k string) int {
		switch v := m[k].(type) {
		case int:
			return v
		case int64:
			return int(v)
		case float64:
			return int(v)
		}
		return 0
	}
	r := RulesConfig{
		Rebalance: str("rebalance"), Rank: str("rank"), Filter: str("filter"),
		Weight: str("weight"), RiskOff: str("risk_off"),
		Pick: num("pick"), Keep: num("keep"),
	}
	if a, ok := m["allocations"].(map[string]any); ok {
		r.Allocations = make(map[string]float64, len(a))
		for t, v := range a {
			switch w := v.(type) {
			case float64:
				r.Allocations[t] = w
			case int64:
				r.Allocations[t] = float64(w)
			case int:
				r.Allocations[t] = float64(w)
			}
		}
	}
	return r
}

// ---------------------------------------------------------------------------
// The strategy

// RulesStrategy runs a rule set. It is compiled lazily on the first Step,
// because only then does it know the portfolio's tickers and benchmark; the
// config was already validated by ToPortfolio, so compiling cannot fail on a
// config that reached a run.
type RulesStrategy struct {
	params    map[string]any
	c         *compiledRules
	benchmark []data.AssetData
	warned    map[string]bool
}

func newRulesStrategy(params map[string]any) *RulesStrategy {
	return &RulesStrategy{params: params, warned: map[string]bool{}}
}

func (s *RulesStrategy) Name() string { return RulesSpec }

// setBenchmark receives the benchmark's bars aligned 1:1 with the portfolio's
// trading days (see runOne). A day the benchmark has no bar for carries a zero
// close, which every signal treats as unknown.
func (s *RulesStrategy) setBenchmark(bars []data.AssetData) { s.benchmark = bars }

// benchmarkConsumer is implemented by a strategy that reads the benchmark.
type benchmarkConsumer interface {
	setBenchmark([]data.AssetData)
}

func (s *RulesStrategy) warnOnce(p *Portfolio, key, format string, args ...any) {
	if s.warned[key] {
		return
	}
	s.warned[key] = true
	diagf("warn", p.Pname, format, args...)
}

func (s *RulesStrategy) Step(p *Portfolio, hist map[string][]data.AssetData, day int) {
	if s.c == nil {
		c, err := compileRules(rulesFromParams(s.params), p.Tickers, p.Benchmark)
		if err != nil {
			// Unreachable from a config (ToPortfolio validates), but a
			// hand-built Portfolio can get here; say so instead of trading.
			s.warnOnce(p, "compile", "rules: %v", err)
			return
		}
		s.c = c
		if c.warmup > 0 {
			n := len(hist[p.Tickers[0]])
			if c.warmup >= n {
				s.warnOnce(p, "warmup",
					"rules need %d trading days of history before their first decision, "+
						"but the window has only %d, so nothing was ever bought",
					c.warmup, n)
			} else {
				diagf("info", p.Pname,
					"rules: holding cash for the first %d trading days while the signals "+
						"gather enough history (first decision on %s)",
					c.warmup, hist[p.Tickers[0]][c.warmup].Date.Format("2006-01-02"))
			}
		}
	}
	c := s.c
	if day < c.warmup || day%c.every != 0 {
		return
	}

	targets := s.targets(p, hist, day)

	priceOf := func(ticker string, d int) float64 {
		series := hist[ticker]
		if d < 0 || d >= len(series) {
			return 0
		}
		return series[d].Close
	}
	dateOf := func(ticker string, d int) (data.AssetData, bool) {
		series := hist[ticker]
		if d < 0 || d >= len(series) {
			return data.AssetData{}, false
		}
		return series[d], true
	}
	held := func(ticker string) float64 {
		pos, ok := p.FindPosition(ticker)
		if !ok || pos == nil {
			return 0
		}
		return pos.Amount
	}
	eq := p.BuyingPower
	for _, t := range p.Tickers {
		eq += held(t) * priceOf(t, day)
	}
	want := func(ticker string, eq, px float64) float64 {
		w := targets[ticker]
		if w <= 0 || px <= 0 {
			return 0
		}
		return eq * w / px
	}
	rebalanceShares(p, day, eq, priceOf, dateOf, want, held)
}

// targets decides the weights for one rebalance day. An empty map is cash.
func (s *RulesStrategy) targets(p *Portfolio, hist map[string][]data.AssetData, day int) map[string]float64 {
	c := s.c
	if c.riskOff != nil {
		on, ok := evalRuleRiskOff(*c.riskOff, s.benchmark, day)
		switch {
		case !ok:
			// Unknown is not "risk off": going to cash because a benchmark
			// bar is missing would be a trade nobody asked for. Said once.
			s.warnOnce(p, "riskoff",
				"rules: %s could not be read on %s (the benchmark has no usable "+
					"bars there), so the risk-off rule was treated as not triggered",
				c.riskOff.entry.Form, dayDate(hist, p, day))
		case on:
			return map[string]float64{}
		}
	}

	var candidates []string
	for _, t := range p.Tickers {
		if c.filter != nil {
			pass, ok := evalRuleFilter(*c.filter, hist[t], day)
			if !ok || !pass {
				continue
			}
		}
		candidates = append(candidates, t)
	}

	selected := candidates
	if c.rank != nil {
		type scored struct {
			ticker string
			value  float64
		}
		var ranked []scored
		for _, t := range candidates {
			if v, ok := evalRuleRank(*c.rank, hist[t], day); ok {
				ranked = append(ranked, scored{t, v})
			}
		}
		// Stable on the portfolio's ticker order, like Lua's rank(), so a
		// tie resolves the same way in both and run to run.
		sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].value > ranked[j].value })

		selected = nil
		chosen := map[string]bool{}
		for i := 0; i < len(ranked) && i < c.keep; i++ {
			if pos, ok := p.FindPosition(ranked[i].ticker); ok && pos != nil && pos.Amount > 0 {
				if len(selected) < c.cfg.Pick {
					selected = append(selected, ranked[i].ticker)
					chosen[ranked[i].ticker] = true
				}
			}
		}
		for _, r := range ranked {
			if len(selected) >= c.cfg.Pick {
				break
			}
			if !chosen[r.ticker] {
				selected = append(selected, r.ticker)
				chosen[r.ticker] = true
			}
		}
	}
	return weigh(*c, selected, hist, day)
}

func dayDate(hist map[string][]data.AssetData, p *Portfolio, day int) string {
	series := hist[p.Tickers[0]]
	if day < 0 || day >= len(series) {
		return fmt.Sprintf("day %d", day)
	}
	return series[day].Date.Format("2006-01-02")
}

func weigh(c compiledRules, names []string, hist map[string][]data.AssetData, day int) map[string]float64 {
	out := make(map[string]float64, len(names))
	raw := make(map[string]float64, len(names))
	var total float64
	for _, t := range names {
		var w float64
		switch c.weight.entry.Base {
		case "equal":
			w = 1
		case "inverse_vol":
			rets, ok := strictReturns(hist[t], day, c.weight.args["N"])
			if !ok {
				continue
			}
			sd, ok := sampleStdev(rets)
			if !ok || sd <= 0 {
				continue
			}
			w = 1 / sd
		case "allocations":
			w = c.cfg.Allocations[t]
		}
		if w > 0 {
			raw[t] = w
			total += w
		}
	}
	if total <= 0 {
		return out
	}
	for t, w := range raw {
		out[t] = w / total
	}
	return out
}

// ---------------------------------------------------------------------------
// Signals. Each mirrors the Lua primitive of the same idea exactly (roc, sma,
// rsi, stdev), and additionally treats a non-positive close anywhere in its
// window as unknown — the database has series with negative back-adjusted
// closes, and a rule should not rank on them.

func closeAt(series []data.AssetData, day int) (float64, bool) {
	if day < 0 || day >= len(series) || series[day].Close <= 0 {
		return 0, false
	}
	return series[day].Close, true
}

// roc matches the Lua roc(): close(day) / close(day-n) - 1.
func roc(series []data.AssetData, day, n int) (float64, bool) {
	now, ok1 := closeAt(series, day)
	past, ok2 := closeAt(series, day-n)
	if !ok1 || !ok2 {
		return 0, false
	}
	return now/past - 1, true
}

// smaPrior matches the Lua sma(): the mean of the n closes BEFORE day.
func smaPrior(series []data.AssetData, day, n int) (float64, bool) {
	if n <= 0 || day < n || day > len(series) {
		return 0, false
	}
	var sum float64
	for i := day - n; i < day; i++ {
		if series[i].Close <= 0 {
			return 0, false
		}
		sum += series[i].Close
	}
	return sum / float64(n), true
}

// rsiAt matches the Lua rsi(): simple-average gains and losses over the n
// changes ending at day.
func rsiAt(series []data.AssetData, day, n int) (float64, bool) {
	if n <= 0 || day < n || day >= len(series) {
		return 0, false
	}
	gain, loss := 0.0, 0.0
	for i := day - n + 1; i <= day; i++ {
		if series[i].Close <= 0 || series[i-1].Close <= 0 {
			return 0, false
		}
		ch := series[i].Close - series[i-1].Close
		if ch >= 0 {
			gain += ch
		} else {
			loss -= ch
		}
	}
	if loss == 0 {
		return 100, true
	}
	return 100 - 100/(1+(gain/loss)), true
}

// strictReturns is dailyReturns (the Lua stdev input) with the same
// non-positive-close rule the other signals use.
func strictReturns(series []data.AssetData, day, n int) ([]float64, bool) {
	rets, ok := dailyReturns(series, day, n)
	if !ok {
		return nil, false
	}
	for i := day - n; i <= day; i++ {
		if series[i].Close <= 0 {
			return nil, false
		}
	}
	return rets, true
}

// highestClose is the highest close over the n days ending at day, inclusive.
func highestClose(series []data.AssetData, day, n int) (float64, bool) {
	if n < 1 || day-n+1 < 0 || day >= len(series) {
		return 0, false
	}
	best := 0.0
	for i := day - n + 1; i <= day; i++ {
		if series[i].Close <= 0 {
			return 0, false
		}
		if series[i].Close > best {
			best = series[i].Close
		}
	}
	return best, true
}

func evalRuleRank(pr parsedRule, series []data.AssetData, day int) (float64, bool) {
	switch pr.entry.Base {
	case "return":
		return roc(series, day, pr.args["N"])
	case "momentum_12_1":
		recent, ok1 := closeAt(series, day-21)
		past, ok2 := closeAt(series, day-273)
		if !ok1 || !ok2 {
			return 0, false
		}
		return recent/past - 1, true
	case "low_vol":
		rets, ok := strictReturns(series, day, pr.args["N"])
		if !ok {
			return 0, false
		}
		sd, ok := sampleStdev(rets)
		return -sd, ok
	case "drawdown":
		hi, ok := highestClose(series, day, pr.args["N"])
		now, ok2 := closeAt(series, day)
		if !ok || !ok2 {
			return 0, false
		}
		return now/hi - 1, true
	}
	return 0, false
}

func evalRuleFilter(pr parsedRule, series []data.AssetData, day int) (bool, bool) {
	switch pr.entry.Base {
	case "above_sma", "below_sma":
		px, ok1 := closeAt(series, day)
		ma, ok2 := smaPrior(series, day, pr.args["N"])
		if !ok1 || !ok2 {
			return false, false
		}
		if pr.entry.Base == "above_sma" {
			return px > ma, true
		}
		return px < ma, true
	case "sma_cross":
		short, ok1 := smaPrior(series, day, pr.args["S"])
		long, ok2 := smaPrior(series, day, pr.args["L"])
		if !ok1 || !ok2 {
			return false, false
		}
		return short > long, true
	case "rising":
		r, ok := roc(series, day, pr.args["N"])
		return r > 0, ok
	case "rsi_below", "rsi_above":
		v, ok := rsiAt(series, day, rsiPeriod)
		if !ok {
			return false, false
		}
		if pr.entry.Base == "rsi_below" {
			return v < float64(pr.args["X"]), true
		}
		return v > float64(pr.args["X"]), true
	}
	return false, false
}

func evalRuleRiskOff(pr parsedRule, bench []data.AssetData, day int) (bool, bool) {
	switch pr.entry.Base {
	case "benchmark_below_sma":
		px, ok1 := closeAt(bench, day)
		ma, ok2 := smaPrior(bench, day, pr.args["N"])
		if !ok1 || !ok2 {
			return false, false
		}
		return px < ma, true
	case "benchmark_drawdown_over":
		hi, ok1 := highestClose(bench, day, ddLookback)
		px, ok2 := closeAt(bench, day)
		if !ok1 || !ok2 {
			return false, false
		}
		return px/hi-1 < -float64(pr.args["X"])/100, true
	}
	return false, false
}

// alignBenchmark lays the benchmark's bars over the portfolio's trading days,
// one per day. A missing day is a zero close, which the signals read as
// unknown rather than as a price.
func alignBenchmark(bench []data.AssetData, days []data.AssetData) []data.AssetData {
	byDate := make(map[int64]data.AssetData, len(bench))
	for _, b := range bench {
		byDate[b.Date.Unix()] = b
	}
	out := make([]data.AssetData, len(days))
	for i, d := range days {
		if b, ok := byDate[d.Date.Unix()]; ok {
			out[i] = b
		} else {
			out[i] = data.AssetData{Date: d.Date}
		}
	}
	return out
}
