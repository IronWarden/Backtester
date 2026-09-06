package backtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// checkFindings runs the checker and returns its diagnostics, so a test can
// assert on what the user would actually see.
func checkFindings(t *testing.T, source string, params map[string]any) (int, []Diagnostic) {
	t.Helper()
	var found []Diagnostic
	prev := Diagnostics
	Diagnostics = func(d Diagnostic) { found = append(found, d) }
	defer func() { Diagnostics = prev }()
	errs := CheckLuaScript(source, "check.lua", params)
	return errs, found
}

// readShippedScript loads one script from the library beside the repo root.
func readShippedScript(t *testing.T, name string) (string, bool) {
	t.Helper()
	src, err := os.ReadFile(filepath.Join("..", "..", "strategies", name))
	if err != nil {
		t.Errorf("reading %s: %v", name, err)
		return "", false
	}
	return string(src), true
}

func messages(ds []Diagnostic) string {
	var b strings.Builder
	for _, d := range ds {
		b.WriteString(d.Level + ": " + d.Message + "\n")
	}
	return b.String()
}

// Every shipped strategy must pass its own checker. If the check cannot run
// the library, it is the check that is wrong, and a user's first experience of
// it would be a false alarm on a script they copied from the gallery.
func TestCheckPassesEveryShippedStrategy(t *testing.T) {
	for name, params := range shippedStrategies() {
		t.Run(name, func(t *testing.T) {
			source, ok := readShippedScript(t, name)
			if !ok {
				return
			}
			errs, found := checkFindings(t, source, params)
			if errs != 0 {
				t.Errorf("the shipped %s does not pass the check:\n%s",
					name, messages(found))
			}
			for _, d := range found {
				if d.Level == "warn" && strings.Contains(d.Message, "raised while checking") {
					t.Errorf("%s raised on the synthetic history:\n%s",
						name, d.Message)
				}
			}
		})
	}
}

func TestCheckLuaScript(t *testing.T) {
	cases := []struct {
		name    string
		source  string
		params  map[string]any
		errors  int
		want    []string
		notWant []string
	}{
		{
			name:   "a syntax error names its line",
			source: "function step(day)\n  local x = \nend\n",
			errors: 1,
			// The parser's own wording, which names the file and the line:
			// "check.lua line:3(column:3) near 'end': syntax error".
			want: []string{"check.lua", "line:3"},
		},
		{
			name:   "no step at all",
			source: "local function helper() return 1 end\n",
			errors: 1,
			want:   []string{"defines no global 'step'"},
		},
		{
			name:   "step is not a function",
			source: "step = 3\n",
			errors: 1,
			want:   []string{"'step' is not a function"},
		},
		{
			name:   "an empty file is not a strategy",
			source: "   \n\n",
			errors: 1,
			want:   []string{"is empty"},
		},
		{
			// The whole point: this compiles, defines step, and fails only
			// when the body actually runs. It was a warning until strict
			// globals landed (T129) -- a name nothing defines is a defect
			// whatever data it ran against, so it is now an error.
			name:   "a typo inside step is caught by running it",
			source: "function step(day)\n  local x = smaa(tickers[1], day, 20)\n end\n",
			errors: 1,
			want:   []string{"error", "unknown name", `did you mean "sma"`},
		},
		{
			name:   "a working strategy reports nothing",
			source: "function step(day)\n  if day == 0 then\n    buy_max(tickers[1], price(tickers[1], day), \"equalWeights\", day)\n  end\nend\n",
			errors: 0,
			// Silence is the pass: a check that always says something is a
			// check nobody reads.
			notWant: []string{"warn", "error"},
		},
		{
			name:   "a param the script never reads",
			source: "function step(day)\n  local p = params.period or 20\n  local _ = sma(tickers[1], day, p)\nend\n",
			params: map[string]any{"period": int64(20), "lookbck": int64(60)},
			errors: 0,
			want:   []string{"params.lookbck", "every trial ran identical settings"},
			// The key it DOES read must not be reported.
			notWant: []string{"params.period but"},
		},
		{
			name:    "a bracketed param read counts as a read",
			source:  "function step(day)\n  local _ = params[\"period\"]\nend\n",
			params:  map[string]any{"period": int64(20)},
			errors:  0,
			notWant: []string{"never reads it"},
		},
		{
			// The one failure a checker must not have: it is one keystroke
			// from the Check button.
			name:   "an endless loop is bounded, not hung",
			source: "while true do end\nfunction step(day) end\n",
			errors: 1,
			want:   []string{"context"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errs, found := checkFindings(t, tc.source, tc.params)
			got := messages(found)
			if errs != tc.errors {
				t.Errorf("reported %d errors, want %d:\n%s", errs, tc.errors, got)
			}
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("findings do not mention %q:\n%s", want, got)
				}
			}
			for _, never := range tc.notWant {
				if strings.Contains(got, never) {
					t.Errorf("findings should not mention %q:\n%s", never, got)
				}
			}
		})
	}
}

// Strict globals: the check's whole reason for running the body is to catch
// what only shows up when it runs, and Lua's silent nil for an undefined name
// is the worst of those. A strategy that quietly never trades looks exactly
// like one that decided not to.
func TestCheckCatchesAnUndefinedGlobal(t *testing.T) {
	cases := []struct {
		name   string
		source string
		want   []string
	}{
		{
			// A read, not a call: this is the one plain Lua never complains
			// about, at any point, ever.
			name:   "a misspelled name that is only read",
			source: "function step(day)\n  local n = my_thresold\n  if n then buy(tickers[1], 1, price(tickers[1], day), day) end\nend\n",
			want:   []string{"unknown name", "my_thresold", "nothing defines it"},
		},
		{
			name:   "a misspelled API function suggests the real one",
			source: "function step(day)\n  local x = smaa(tickers[1], day, 20)\nend\n",
			want:   []string{"unknown name", "smaa", `did you mean "sma"`},
		},
		{
			name:   "a misspelled context value suggests the real one",
			source: "function step(day)\n  local t = tickerz[1]\nend\n",
			want:   []string{`did you mean "tickers"`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errs, found := checkFindings(t, tc.source, nil)
			got := messages(found)
			if errs == 0 {
				t.Errorf("an undefined global was not reported as an error:\n%s", got)
			}
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("findings do not mention %q:\n%s", want, got)
				}
			}
		})
	}
}

// Strict mode must not break the ways a script legitimately uses globals: its
// own state, its own helper functions, and the standard library.
func TestStrictGlobalsAllowWhatLuaLegitimatelyDoes(t *testing.T) {
	const source = `
-- Own state, assigned before it is read.
held = false

-- A helper defined after step but called from it: by the time step runs, the
-- whole file has executed.
function step(day)
  local px = price(tickers[1], day)
  if not held and px > 0 and math.floor(px) > 0 and helper(px) then
    buy_max(tickers[1], px, "equalWeights", day)
    held = true
  end
  log("day", day, string.format("%.2f", px))
end

function helper(px)
  return px == px  -- table/string/math all reachable
end
`
	errs, found := checkFindings(t, source, nil)
	if errs != 0 {
		t.Errorf("strict mode rejected a legitimate script:\n%s", messages(found))
	}
	if strings.Contains(messages(found), "unknown name") {
		t.Errorf("strict mode flagged a name the script defines:\n%s", messages(found))
	}
}

// A suggestion that points at the wrong name sends the reader to the wrong
// line, so a name nothing resembles must get no suggestion at all.
func TestDidYouMeanOnlySuggestsWhenItIsClose(t *testing.T) {
	known := []string{"sma", "rsi", "tickers", "target_weights", "price"}
	if got := didYouMean("smaa", known); !strings.Contains(got, `"sma"`) {
		t.Errorf("didYouMean(smaa) = %q", got)
	}
	if got := didYouMean("prices", known); !strings.Contains(got, `"price"`) {
		t.Errorf("didYouMean(prices) = %q", got)
	}
	if got := didYouMean("completely_unrelated", known); strings.Contains(got, "did you mean") {
		t.Errorf("didYouMean invented a suggestion: %q", got)
	}
	// One character against a three-letter name is a third of it; suggesting
	// "sma" for "x" would be noise.
	if got := didYouMean("x", known); strings.Contains(got, "did you mean") {
		t.Errorf("didYouMean suggested a name for a one-character typo: %q", got)
	}
}
