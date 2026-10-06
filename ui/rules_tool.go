package main

// check_rules tool: validate a [portfolio.Rules] block and read it back.
//
// The assistant proposes rule sets instead of Lua so the user can check a
// sentence rather than code. That only works if the sentence is the
// ENGINE's: a model paraphrasing what it thinks it wrote can be wrong in
// exactly the way the user cannot see. So the assistant calls this before
// showing a rule set, and shows the sentence it returns — DescribeRules, the
// same renderer the app and the results use.
//
// The check is the run's own construction path (ToPortfolio, which compiles
// the rules and refuses a warm-up longer than the window), plus the coverage
// rule the runner applies, against the open database when there is one.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"my-backtester/src/backtest"
	"my-backtester/src/data"
)

const checkRulesToolName = "check_rules"

const checkRulesToolDescription = "Check a strategy written as rules " +
	"([portfolio.Rules]) before proposing it. Takes the rules, the tickers, " +
	"the window and an optional benchmark; returns either the errors, or " +
	"the engine's own one-sentence reading of the rules plus the window the " +
	"database covers for those tickers. ALWAYS call this before showing a " +
	"rule set, and show the user the returned sentence verbatim — not your " +
	"own paraphrase."

func checkRulesToolSchema() map[string]any {
	str := func(desc string) map[string]any {
		return map[string]any{"type": "string", "description": desc}
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"rules": map[string]any{
				"type":        "object",
				"description": "The [portfolio.Rules] block, keys as in the config.",
				"properties": map[string]any{
					"Rebalance": str("daily | weekly | monthly | quarterly | annually (required)"),
					"Rank":      str("rank signal, e.g. return_126, momentum_12_1, low_vol_63"),
					"Pick": map[string]any{"type": "integer",
						"description": "how many top-ranked names to hold; required with Rank"},
					"Keep": map[string]any{"type": "integer",
						"description": "keep a holding while it is in the top Keep; 0 = Pick"},
					"Filter":  str("filter, e.g. above_sma_200, rsi_below_30"),
					"Weight":  str("equal (default) | inverse_vol_N | allocations"),
					"RiskOff": str("e.g. benchmark_below_sma_200; needs benchmark"),
					"Allocations": map[string]any{"type": "object",
						"description": "ticker -> relative weight, for Weight = allocations"},
				},
				"required": []string{"Rebalance"},
			},
			"tickers": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "The portfolio's Tickers — the whole universe the rules choose from.",
			},
			"start_date": str("YYYY-MM-DD"),
			"end_date":   str("YYYY-MM-DD"),
			"benchmark":  str("optional, e.g. $SP500; required by RiskOff"),
		},
		"required": []string{"rules", "tickers", "start_date", "end_date"},
	}
}

func checkRulesPreview(input map[string]any) string {
	r, _ := input["rules"].(map[string]any)
	parts := []string{}
	for _, k := range []string{"Rebalance", "Rank", "Filter", "RiskOff"} {
		if v, ok := r[k].(string); ok && v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, " ")
}

// decodeRules reads the tool's rules object into the engine's type. Unknown
// keys are refused rather than dropped: a misspelt "Filtre" silently
// vanishing would make the sentence describe a strategy without its filter.
func decodeRules(raw any) (backtest.RulesConfig, error) {
	var rc backtest.RulesConfig
	m, ok := raw.(map[string]any)
	if !ok {
		return rc, fmt.Errorf("rules must be an object like {\"Rebalance\": \"monthly\", ...}")
	}
	buf, err := json.Marshal(m)
	if err != nil {
		return rc, err
	}
	dec := json.NewDecoder(bytes.NewReader(buf))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rc); err != nil {
		return rc, fmt.Errorf("rules: %v (valid keys: Rebalance, Rank, Pick, "+
			"Keep, Filter, Weight, RiskOff, Allocations)", err)
	}
	return rc, nil
}

// runCheckRulesTool returns "ERROR: ..." lines or "OK" plus the sentence.
// dbPath may be empty: the rules are still checked, coverage is not.
func runCheckRulesTool(dbPath string, input map[string]any) string {
	rc, err := decodeRules(input["rules"])
	if err != nil {
		return "ERROR: " + err.Error()
	}
	rawTickers, _ := input["tickers"].([]any)
	var tickers []string
	for _, t := range rawTickers {
		if s, ok := t.(string); ok && strings.TrimSpace(s) != "" {
			tickers = append(tickers, strings.TrimSpace(s))
		}
	}
	if len(tickers) == 0 {
		return "ERROR: no tickers given — the rules choose from the portfolio's Tickers."
	}
	start, _ := input["start_date"].(string)
	end, _ := input["end_date"].(string)
	bench, _ := input["benchmark"].(string)

	pc := backtest.PortfolioConfig{
		Name: "rules", BuyingPower: 100000, StartTime: start, EndTime: end,
		Tickers: tickers, Benchmark: strings.TrimSpace(bench), Rules: &rc,
	}
	if _, err := pc.ToPortfolio(); err != nil {
		return "ERROR: " + strings.TrimPrefix(err.Error(), `portfolio "rules": `)
	}

	var b strings.Builder
	b.WriteString("OK. Show the user this sentence verbatim:\n\n")
	b.WriteString(backtest.DescribeRules(rc, tickers, pc.Benchmark))
	b.WriteString("\n\n")
	b.WriteString(rulesCoverage(dbPath, tickers, pc.Benchmark, start, end))
	return b.String()
}

// rulesCoverage says whether the window fits the data, in the runner's
// terms: every ticker (and the benchmark) must cover all of it.
func rulesCoverage(dbPath string, tickers []string, bench, start, end string) string {
	if dbPath == "" {
		return "Coverage not checked: no database is open."
	}
	if _, err := data.InitDB(dbPath); err != nil {
		return fmt.Sprintf("Coverage not checked: open db: %v", err)
	}
	want := append([]string{}, tickers...)
	if bench != "" {
		want = append(want, bench)
	}
	ranges, err := data.TickerDateRanges(want)
	if err != nil {
		return fmt.Sprintf("Coverage not checked: %v", err)
	}
	return describeCoverage(ranges, want, start, end)
}

func describeCoverage(ranges map[string]data.DateRange, want []string, start, end string) string {
	const day = "2006-01-02"
	var missing []string
	var from, to time.Time
	for _, t := range want {
		r, ok := ranges[t]
		if !ok {
			missing = append(missing, t)
			continue
		}
		if from.IsZero() || r.Min.After(from) {
			from = r.Min
		}
		if to.IsZero() || r.Max.Before(to) {
			to = r.Max
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return "ERROR: not in the database: " + strings.Join(missing, ", ") +
			". The run would be refused; fetch them or drop them."
	}
	if !from.Before(to) {
		return "ERROR: these tickers share no common window in the database."
	}
	s, _ := time.Parse(day, start)
	e, _ := time.Parse(day, end)
	common := fmt.Sprintf("The data covers %s..%s for all of them.",
		from.Format(day), to.Format(day))
	if s.Before(from) || e.After(to) {
		return "ERROR: " + common + " The requested " + start + ".." + end +
			" is outside it, so the run would be refused; propose dates inside it."
	}
	return common + " The requested window fits."
}
