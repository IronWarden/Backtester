package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"my-backtester/src/backtest"
)

func TestReadRules(t *testing.T) {
	a := &App{}
	rc := backtest.RulesConfig{Rebalance: "monthly", Rank: "return_126", Pick: 2}
	tickers := []string{"AAA", "BBB", "CCC"}

	got := a.ReadRules(rc, tickers, "2015-01-02", "2020-01-02", "$SP500")
	if got.Error != "" {
		t.Fatalf("valid rules refused: %s", got.Error)
	}
	if got.Sentence != backtest.DescribeRules(rc, tickers, "$SP500") {
		t.Errorf("sentence is not the engine's: %s", got.Sentence)
	}
	if got.Headline == "" || !strings.Contains(got.Sentence, got.Headline) {
		t.Errorf("headline %q is not a piece of the sentence", got.Headline)
	}

	// Mid-edit, with no dates yet: the rules are still read.
	if got := a.ReadRules(rc, tickers, "", "", ""); got.Error != "" || got.Sentence == "" {
		t.Errorf("no dates: %+v", got)
	}
	// The window check is the run's: a warm-up the window cannot hold.
	long := backtest.RulesConfig{Rebalance: "monthly", Filter: "above_sma_900"}
	if got := a.ReadRules(long, tickers, "2015-01-02", "2016-01-04", ""); !strings.Contains(got.Error, "trading days of history") {
		t.Errorf("warm-up: %+v", got)
	}
	if got := a.ReadRules(backtest.RulesConfig{Rebalance: "monthly", Rank: "retrun_126", Pick: 1}, tickers, "", "", ""); !strings.Contains(got.Error, "return_126") ||
		got.Sentence != "" || got.Headline != "" {
		t.Errorf("typo: %+v", got)
	}
	if got := a.ReadRules(rc, nil, "", "", ""); got.Error == "" {
		t.Error("no holdings must say so")
	}
}

func TestRulesFromToml(t *testing.T) {
	a := &App{}
	want := backtest.RulesConfig{Rebalance: "monthly", Rank: "return_126", Pick: 3,
		Filter: "above_sma_200"}

	full := `[[portfolio]]
Name = "X"
Tickers = ["A", "B", "C", "D"]

[portfolio.Rules]
Rebalance = "monthly"
Rank      = "return_126"
Pick      = 3
Filter    = "above_sma_200"
`
	if got, err := a.RulesFromToml(full); err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("full config: %+v %v", got, err)
	}

	// What a small model actually wrote (T134's live run): plain keys after
	// a sub-table, so the whole config does not load. The block alone does.
	broken := full + "\n[portfolio.Params]\n  Weight = \"equal\"\nBenchmark = \"$SP500\"\nCosts = {}\n" +
		"[portfolio.Costs]\nx = 1\n"
	if got, err := a.RulesFromToml(broken); err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("config that does not load: %+v %v", got, err)
	}

	bare := "  [Portfolio.Rules]\n  Rebalance = \"quarterly\"\n  Weight = \"allocations\"\n" +
		"  [portfolio.Rules.Allocations]\n  AAPL = 3.0\n  MSFT = 1.0\n"
	got, err := a.RulesFromToml(bare)
	if err != nil || got.Weight != "allocations" || got.Allocations["AAPL"] != 3 || got.Allocations["MSFT"] != 1 {
		t.Errorf("bare block with allocations: %+v %v", got, err)
	}

	if _, err := a.RulesFromToml("[portfolio.Rules]\nRebalance = \"monthly\"\nFiltre = \"x\"\n"); err == nil ||
		!strings.Contains(err.Error(), "Filtre") {
		t.Errorf("a misspelt key must be refused by name: %v", err)
	}
	if _, err := a.RulesFromToml("[[portfolio]]\nName = \"x\"\n"); err == nil {
		t.Error("a snippet with no rules block must be refused")
	}
}

// The rules card is smoke-rendered against the engine's real menu and real
// readings, written here from Go so the frontend can never be testing a menu
// or a sentence the engine does not produce. Same -update flag as the result
// fixtures.
func rulesFixtures() map[string]any {
	a := &App{}
	tickers := []string{"AAPL", "MSFT", "JNJ", "XOM"}
	read := func(rc backtest.RulesConfig, bench string) RulesReading {
		return a.ReadRules(rc, tickers, "2015-01-02", "2024-12-31", bench)
	}
	return map[string]any{
		"rules-menu":    backtest.RulesVocabulary(),
		"library-rules": a.LibraryRules(),
		"rules-readings": map[string]RulesReading{
			// What "Describe my own idea…" starts from (DEFAULT_RULES).
			"default": read(backtest.RulesConfig{Rebalance: "monthly", Weight: "equal"}, "$SP500"),
			// What an assistant proposal looks like once it is in the card.
			"proposed": read(backtest.RulesConfig{Rebalance: "monthly", Rank: "return_126",
				Pick: 2, Filter: "above_sma_200", RiskOff: "benchmark_below_sma_200",
				Weight: "equal"}, "$SP500"),
			// The same, edited: weekly, keep top 3.
			"edited": read(backtest.RulesConfig{Rebalance: "weekly", Rank: "return_126",
				Pick: 2, Keep: 3, Filter: "above_sma_200", RiskOff: "benchmark_below_sma_200",
				Weight: "equal"}, "$SP500"),
			// Invalid: risk-off with the comparison switched off.
			"invalid": read(backtest.RulesConfig{Rebalance: "monthly",
				RiskOff: "benchmark_below_sma_200", Weight: "equal"}, ""),
		},
	}
}

func TestRulesFixturesMatchTheEngine(t *testing.T) {
	for name, v := range rulesFixtures() {
		want, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, '\n')
		path := filepath.Join(fixtureDir, name+".json")
		if *updateFixtures {
			if err := os.WriteFile(path, want, 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v -- regenerate with `go test ./ui -run TestRulesFixtures -update`", path, err)
		}
		if string(got) != string(want) {
			t.Errorf("%s is stale: the engine's rules menu or sentences changed but the "+
				"fixture the rules card renders against did not. Regenerate with "+
				"`go test ./ui -run TestRulesFixtures -update`.", path)
		}
	}
}

// The TOML a rules card generates (pinned by the smoke render as
// golden-rules.toml) is a config the engine accepts: parsed, rules compiled,
// portfolios built -- the same check -check and the app's ✓ Check run.
func TestRulesCardTomlIsAConfigTheEngineAccepts(t *testing.T) {
	text, err := os.ReadFile(filepath.Join(fixtureDir, "golden-rules.toml"))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := backtest.LoadConfigText(string(text))
	if err != nil {
		t.Fatalf("golden-rules.toml does not parse: %v", err)
	}
	var diags []string
	diagMu.Lock()
	defer diagMu.Unlock()
	backtest.Diagnostics = func(d backtest.Diagnostic) { diags = append(diags, d.Message) }
	defer func() { backtest.Diagnostics = nil }()
	if n := backtest.CheckConfig(cfg, ""); n != 0 {
		t.Fatalf("the engine refuses what the rules card writes (%d errors): %v", n, diags)
	}
	for _, pc := range cfg.Portfolios {
		if pc.Rules == nil || pc.Strategy != "" {
			t.Errorf("%s: want rules and no Strategy, got %+v", pc.Name, pc)
		}
	}
	if w := cfg.Portfolios[1].Rules.Allocations; w["9988.HK"] != 20 || w["AAPL"] != 50 {
		t.Errorf("allocations did not survive the round trip: %v", w)
	}
}

func TestLibraryRulesReadAsSentences(t *testing.T) {
	list := (&App{}).LibraryRules()
	if len(list) == 0 {
		t.Fatal("no library twins")
	}
	twins := map[string]LibraryTwin{}
	for _, tw := range list {
		twins[tw.File] = tw
	}
	for file, tw := range twins {
		if strings.HasPrefix(tw.Sentence, "These rules cannot run") || tw.Sentence == "" {
			t.Errorf("%s: %q", file, tw.Sentence)
		}
		if strings.Contains(tw.Sentence, "T0") {
			t.Errorf("%s: a placeholder ticker leaked into the sentence: %s", file, tw.Sentence)
		}
	}
	if !strings.Contains(twins["momentum_rotation.lua"].Sentence, "rose most over the last 126") {
		t.Errorf("momentum: %s", twins["momentum_rotation.lua"].Sentence)
	}
}
