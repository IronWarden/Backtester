package backtest

import (
	"strings"
	"testing"
)

// A skeleton that does not run teaches a beginner that they broke it. Each one
// goes through the same check the app offers, which is the strictest thing
// available: it compiles them, requires step, runs three days with strict
// globals, and would reject a typo or a name that does not exist.
func TestSkeletonsPassTheScriptCheck(t *testing.T) {
	for _, sk := range LuaSkeletons() {
		t.Run(sk.Name, func(t *testing.T) {
			var found []Diagnostic
			prev := Diagnostics
			Diagnostics = func(d Diagnostic) { found = append(found, d) }
			defer func() { Diagnostics = prev }()

			if errs := CheckLuaScript(sk.Source, sk.Name, nil); errs != 0 {
				var b strings.Builder
				for _, d := range found {
					b.WriteString(d.Level + ": " + d.Message + "\n")
				}
				t.Errorf("the %q skeleton does not pass the check:\n%s", sk.Name, b.String())
			}
			for _, d := range found {
				if d.Level == "warn" && strings.Contains(d.Message, "raised while checking") {
					t.Errorf("the %q skeleton raised on the check's data:\n%s", sk.Name, d.Message)
				}
			}
		})
	}
}

// Each one has to be a starting point, not a finished strategy: somewhere the
// idea goes, and a name and sentence the button can carry.
func TestSkeletonsAreStartingPoints(t *testing.T) {
	sks := LuaSkeletons()
	if len(sks) < 3 {
		t.Fatalf("only %d skeletons; the point is that there is more than one shape", len(sks))
	}
	seen := map[string]bool{}
	for _, sk := range sks {
		if sk.Name == "" || sk.Summary == "" || sk.Source == "" {
			t.Errorf("incomplete skeleton: %+v", sk)
		}
		if seen[sk.Name] {
			t.Errorf("two skeletons named %q", sk.Name)
		}
		seen[sk.Name] = true
		if !strings.Contains(sk.Source, "TODO") {
			t.Errorf("%q has no TODO: a skeleton with nothing left to do is a strategy", sk.Name)
		}
		if !strings.Contains(sk.Source, "function step(day)") {
			t.Errorf("%q does not define step(day)", sk.Name)
		}
	}
}

// The skeletons are also the clearest demonstration of the API, so a global
// one of them uses must be one the engine registers -- the same drift the
// editor's completion table is pinned against.
func TestSkeletonsUseOnlyRealGlobals(t *testing.T) {
	registered := registeredGlobals(t)
	// Names that appear in the sources and are Lua's own, not the engine's.
	stdlib := map[string]bool{"math": true, "params": true}
	for _, sk := range LuaSkeletons() {
		for _, g := range []string{
			"price", "sma", "roc", "rank", "target_weights", "buy_max",
			"sell_all", "position", "plot", "tickers",
		} {
			if !strings.Contains(sk.Source, g) {
				continue
			}
			if !registered[g] && !stdlib[g] {
				t.Errorf("%q uses %q, which the engine does not register", sk.Name, g)
			}
		}
	}
}
