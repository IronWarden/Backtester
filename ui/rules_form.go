package main

// The Backtest tab's rules card (T135) reads everything it shows from here:
// the menu its controls offer, and the sentence it prints. Both come from the
// engine, so the card cannot offer an entry the engine would refuse or word a
// rule set differently from the results and the assistant.

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"

	"my-backtester/src/backtest"
)

// RulesReading is a rule set as the card shows it: the full sentence, the
// short promise the Run button carries, or why it cannot run.
type RulesReading struct {
	Sentence string `json:"sentence"`
	Headline string `json:"headline"`
	Error    string `json:"error"`
}

// RulesMenu is every rule the engine implements, with its numbers' ranges.
func (a *App) RulesMenu() []backtest.RuleEntry {
	return backtest.RulesVocabulary()
}

// ReadRules checks a rule set on the run's own construction path and reads
// it back. Dates may be empty while the form is still being filled in; the
// rules alone are then checked, and the window is left to the form, which
// already refuses one the data does not cover.
func (a *App) ReadRules(rc backtest.RulesConfig, tickers []string, start, end, benchmark string) RulesReading {
	if len(tickers) == 0 {
		return RulesReading{Error: "Add some holdings: the rules choose from them."}
	}
	if start != "" && end != "" {
		pc := backtest.PortfolioConfig{
			Name: "rules", BuyingPower: 1, StartTime: start, EndTime: end,
			Tickers: tickers, Benchmark: benchmark, Rules: &rc,
		}
		if _, err := pc.ToPortfolio(); err != nil {
			return RulesReading{Error: strings.TrimPrefix(err.Error(), `portfolio "rules": `)}
		}
	}
	head := backtest.RulesHeadline(rc, tickers, benchmark)
	sentence := backtest.DescribeRules(rc, tickers, benchmark)
	if head == "" {
		return RulesReading{Error: strings.TrimPrefix(sentence, "These rules cannot run: ")}
	}
	return RulesReading{Sentence: sentence, Headline: head}
}

var rulesHeader = regexp.MustCompile(`(?im)^\s*\[\s*portfolio\.rules\s*\]\s*$`)

// HasRulesBlock says whether a TOML snippet carries a rules block, so the
// chat can offer "Use these rules" only where there is something to use.
func HasRulesBlock(text string) bool { return rulesHeader.MatchString(text) }

// RulesFromToml lifts the rules out of a TOML snippet the assistant wrote:
// a whole config (the first portfolio with rules wins) or a bare
// [portfolio.Rules] block. Unknown keys are refused, as the config is.
func (a *App) RulesFromToml(text string) (backtest.RulesConfig, error) {
	if !HasRulesBlock(text) {
		return backtest.RulesConfig{}, fmt.Errorf("no [portfolio.Rules] block in this snippet")
	}
	if cfg, err := backtest.LoadConfigText(text); err == nil {
		for _, pc := range cfg.Portfolios {
			if pc.Rules != nil {
				return *pc.Rules, nil
			}
		}
	}
	// A bare block: its lines up to the next table header, keeping an
	// [portfolio.Rules.Allocations] sub-table, renamed so it decodes into
	// RulesConfig directly.
	var body []string
	in := false
	for _, line := range strings.Split(text, "\n") {
		t := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(line), " ", ""))
		switch {
		case t == "[portfolio.rules]":
			in = true
			continue
		case t == "[portfolio.rules.allocations]":
			if in {
				body = append(body, "[Allocations]")
			}
			continue
		case strings.HasPrefix(t, "["):
			in = false
		}
		if in {
			body = append(body, line)
		}
	}
	var rc backtest.RulesConfig
	md, err := toml.Decode(strings.Join(body, "\n"), &rc)
	if err != nil {
		return rc, fmt.Errorf("reading the rules: %v", err)
	}
	if un := md.Undecoded(); len(un) > 0 {
		return rc, fmt.Errorf("unknown rules key %q", un[0].String())
	}
	return rc, nil
}

// LibraryTwin is a shipped script restated as rules, with its sentence.
type LibraryTwin struct {
	Rules    backtest.RulesConfig `json:"rules"`
	Sentence string               `json:"sentence"`
}

// LibraryRules returns the twin of every library script the menu can say
// exactly (backtest.LibraryTwins), keyed by file name. The sentence is read
// without a portfolio: it never names the tickers, so placeholders stand in
// for as many as the twin picks, and none of the twins reads a benchmark.
func (a *App) LibraryRules() map[string]LibraryTwin {
	out := map[string]LibraryTwin{}
	for file, rc := range backtest.LibraryTwins() {
		n := rc.Pick
		if n < 1 {
			n = 1
		}
		placeholders := make([]string, n)
		for i := range placeholders {
			placeholders[i] = fmt.Sprintf("T%d", i)
		}
		out[file] = LibraryTwin{Rules: rc, Sentence: backtest.DescribeRules(rc, placeholders, "")}
	}
	return out
}
