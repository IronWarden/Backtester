package backtest

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestScriptFundamentalsReadsTheTable(t *testing.T) {
	for _, tc := range []struct {
		name, script, source string
		metrics              []string
	}{
		{"declared", `
fundamentals = {
    source  = "sec",  -- a comment is fine
    metrics = { "Net Income", "Total Assets" },
}
function step(day) end`, "sec", []string{"Net Income", "Total Assets"}},
		{"default source", `fundamentals = { metrics = { "Net Income" } }`,
			"", []string{"Net Income"}},
		{"nothing declared", `function step(day) end`, "", nil},
		// Inside a function is not the top level: not a declaration.
		{"not top level", `function step(day) fundamentals = { metrics = { "X" } } end`,
			"", nil},
		// A syntax error is the compiler's to report, not this.
		{"does not parse", `function step(day`, "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, metrics, err := ScriptFundamentals("s.lua", []byte(tc.script))
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if source != tc.source || !reflect.DeepEqual(metrics, tc.metrics) {
				t.Errorf("got (%q, %q), want (%q, %q)", source, metrics, tc.source, tc.metrics)
			}
		})
	}
}

// A declaration that cannot be read is an error naming its line — never a
// silent "nothing declared", which is the failure this table replaced.
func TestScriptFundamentalsRefusesWhatItCannotRead(t *testing.T) {
	for name, tc := range map[string]struct{ script, want string }{
		"not a table":   {`fundamentals = "sec"`, "s.lua:1: fundamentals must be a table"},
		"computed name": {"local m = \"Net Income\"\nfundamentals = {\n metrics = { m },\n}", "s.lua:3: fundamentals.metrics must list quoted"},
		"unknown field": {`fundamentals = { sorce = "sec" }`, `no field "sorce"`},
		"twice": {"fundamentals = { metrics = { \"A\" } }\nfundamentals = { metrics = { \"B\" } }",
			"s.lua:2: fundamentals is assigned twice"},
		"source not a string": {`fundamentals = { source = sec }`, "fundamentals.source must be"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := ScriptFundamentals("s.lua", []byte(tc.script))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want one containing %q", err, tc.want)
			}
		})
	}
}

// Picking a fundamentals strategy by name and nothing else must load what it
// reads — the bug this exists for was five gallery strategies holding cash
// all run when chosen from the Simple form.
func TestToPortfolioAppliesTheScriptsDeclaration(t *testing.T) {
	pc := func(fundamentals []string, source string) *PortfolioConfig {
		return &PortfolioConfig{
			Name: "f", BuyingPower: 1000, StartTime: "2015-01-02",
			EndTime: "2016-01-04", Tickers: []string{"AAA"},
			Strategy:     "lua:strategies/quality_roe.lua",
			Fundamentals: fundamentals, FundamentalSource: source,
		}
	}

	p, err := pc(nil, "").ToPortfolio()
	if err != nil {
		t.Fatalf("ToPortfolio: %v", err)
	}
	want := []string{"Net Income", "Stockholders Equity"}
	if p.FundamentalSource != "sec" || !reflect.DeepEqual(p.Fundamentals, want) {
		t.Errorf("got (%q, %q), want (sec, %q) from the header",
			p.FundamentalSource, p.Fundamentals, want)
	}

	// A config naming its own metrics means exactly those, source included.
	p, err = pc([]string{"Net Income"}, "").ToPortfolio()
	if err != nil {
		t.Fatalf("ToPortfolio: %v", err)
	}
	if p.FundamentalSource != "" || !reflect.DeepEqual(p.Fundamentals, []string{"Net Income"}) {
		t.Errorf("explicit config overridden: (%q, %q)", p.FundamentalSource, p.Fundamentals)
	}

	// A source with no metrics takes the header's metrics.
	p, err = pc(nil, "financials").ToPortfolio()
	if err != nil {
		t.Fatalf("ToPortfolio: %v", err)
	}
	if p.FundamentalSource != "" || !reflect.DeepEqual(p.Fundamentals, want) {
		t.Errorf("got (%q, %q), want the config's source and the header's metrics",
			p.FundamentalSource, p.Fundamentals)
	}
}

// Every shipped script that reads a fundamental declares it, so none of them
// can be picked by name and silently load nothing.
func TestShippedScriptsDeclareTheFundamentalsTheyRead(t *testing.T) {
	dir := filepath.Join("..", "..", "strategies")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	call := regexp.MustCompile(`fundamental\([^,()]+,\s*"([^"]+)"`)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".lua") || userScript(e.Name()) {
			continue
		}
		src := readStrategySource(t, filepath.Join(dir, e.Name()))
		_, declared, err := ScriptFundamentals(e.Name(), []byte(src))
		if err != nil {
			t.Errorf("%v", err)
		}
		have := map[string]bool{}
		for _, m := range declared {
			have[m] = true
		}
		for _, m := range call.FindAllStringSubmatch(src, -1) {
			if !have[m[1]] {
				t.Errorf("%s reads %q but its fundamentals table does not declare it",
					e.Name(), m[1])
			}
		}
	}
}

func TestUndeclaredMetricWarnsOnce(t *testing.T) {
	out := runFundamentalScript(t, `
		function step(day)
		  fundamental("AAA", "Net Income", day)
		  fundamental("AAA", "Total Assets", day)
		end
	`, []string{"Net Income"}, nil)
	if n := strings.Count(out, `fundamental("Total Assets") was called but that metric is not loaded`); n != 1 {
		t.Errorf("warned %d times about the undeclared metric, want 1:\n%s", n, out)
	}
	if strings.Contains(out, `fundamental("Net Income") was called`) {
		t.Errorf("warned about a declared metric:\n%s", out)
	}
}
