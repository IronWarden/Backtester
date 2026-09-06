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
			// when the body actually runs.
			name:   "a typo inside step is caught by running it",
			source: "function step(day)\n  local x = smaa(tickers[1], day, 20)\n end\n",
			errors: 0,
			want:   []string{"warn", "raised while checking", "synthetic"},
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
