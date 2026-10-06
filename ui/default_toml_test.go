package main

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"

	"my-backtester/src/backtest"
)

// The Config tab opens on DEFAULT_TOML (frontend/src/App.tsx), which lists
// every setting the engine reads -- the two example portfolios live, the rest
// commented out -- so the tab doubles as the reference for what can be
// configured. These tests keep both halves of that honest.

func defaultTOML(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile("frontend/src/App.tsx")
	if err != nil {
		t.Fatalf("reading App.tsx: %v", err)
	}
	m := regexp.MustCompile("(?s)const DEFAULT_TOML = `(.*?)`;").FindSubmatch(src)
	if m == nil {
		t.Fatal("DEFAULT_TOML not found in App.tsx")
	}
	if strings.Contains(string(m[1]), "${") {
		t.Fatal("DEFAULT_TOML interpolates; it must be plain text")
	}
	return string(m[1])
}

// Turn every commented-out setting and section header back on. Explanation
// lines are left alone: they either have no `=` or begin with more comment.
var commentedSetting = regexp.MustCompile(`^# (\[[^\]]+\]|[A-Za-z_]+\s*=)`)

func uncommentAll(text string) string {
	lines := strings.Split(text, "\n")
	for i, l := range lines {
		if commentedSetting.MatchString(l) {
			lines[i] = strings.TrimPrefix(l, "# ")
		}
	}
	return strings.Join(lines, "\n")
}

// Untouched, the tab must run exactly what it ran before the options were
// listed: the same two portfolios, and nothing set beyond them.
func TestDefaultTOMLRunsWhatItAlwaysRan(t *testing.T) {
	cfg, err := backtest.LoadConfigText(defaultTOML(t))
	if err != nil {
		t.Fatalf("DEFAULT_TOML does not parse: %v", err)
	}
	if cfg.Output != nil {
		t.Error("an untouched default must not write an [Output] file")
	}
	want := []backtest.PortfolioConfig{
		{Name: "Default Lua", BuyingPower: 100000, StartTime: "2020-01-01",
			EndTime: "2024-01-01", Tickers: []string{"AAPL", "MSFT"}},
		{Name: "Buy and Hold", BuyingPower: 100000, StartTime: "2020-01-01",
			EndTime: "2024-01-01", Tickers: []string{"AAPL", "MSFT"},
			Strategy: "buyAndHold:equalWeights"},
	}
	if !reflect.DeepEqual(cfg.Portfolios, want) {
		t.Errorf("default portfolios changed:\n got %+v\nwant %+v", cfg.Portfolios, want)
	}
	// The Lua tab picks its script by matching `Strategy = "lua:..."` in the
	// raw text, comments included; a commented example in that form would
	// silently swap the scratch script for another one.
	if p := (&App{}).FindLuaPathInConfig(defaultTOML(t), ""); p != "" {
		t.Errorf("the default makes the Lua tab open %q; it must open the default script", p)
	}
}

// With every option switched on, the text must still be a config the engine
// accepts key for key -- a typo in a commented line is a typo the user copies --
// and it must name every key the engine has.
func TestDefaultTOMLListsEveryOption(t *testing.T) {
	text := uncommentAll(defaultTOML(t))
	var cfg backtest.Config
	md, err := toml.Decode(text, &cfg)
	if err != nil {
		t.Fatalf("with every option uncommented, the default does not parse: %v\n%s", err, text)
	}
	if un := md.Undecoded(); len(un) > 0 {
		// Params and Sweep are free-form maps; their example keys are fine.
		for _, k := range un {
			s := k.String()
			if !strings.HasPrefix(s, "portfolio.Params.") && !strings.HasPrefix(s, "portfolio.Sweep.") {
				t.Errorf("uncommented default sets %q, which the engine does not read", s)
			}
		}
	}

	defined := map[string]bool{}
	for _, k := range md.Keys() {
		defined[k.String()] = true
	}
	check := func(prefix string, typ reflect.Type) {
		for i := range typ.NumField() {
			tag := typ.Field(i).Tag.Get("toml")
			if tag == "" {
				continue
			}
			if !defined[prefix+tag] {
				t.Errorf("DEFAULT_TOML never lists %s%s -- add it, commented out, "+
					"so the Config tab shows it exists", prefix, tag)
			}
		}
	}
	check("portfolio.", reflect.TypeOf(backtest.PortfolioConfig{}))
	check("portfolio.Rules.", reflect.TypeOf(backtest.RulesConfig{}))
	check("portfolio.Costs.", reflect.TypeOf(backtest.CostConfig{}))
	check("portfolio.Margin.", reflect.TypeOf(backtest.MarginConfig{}))
	check("portfolio.Cashflow.", reflect.TypeOf(backtest.CashflowConfig{}))
	check("portfolio.Validation.", reflect.TypeOf(backtest.ValidationConfig{}))
	check("portfolio.WalkForward.", reflect.TypeOf(backtest.WalkForwardConfig{}))
	check("Output.", reflect.TypeOf(backtest.OutputConfig{}))
}
