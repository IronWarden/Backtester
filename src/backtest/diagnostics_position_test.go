package backtest

import "testing"

func TestScriptPosition(t *testing.T) {
	cases := []struct {
		name    string
		message string
		script  string
		line    int
		ok      bool
	}{
		{
			name:    "a raised Lua error",
			message: `lua strategy "strategies/sma_cross.lua" step day=41: strategies/sma_cross.lua:23: attempt to compare nil with number`,
			script:  "strategies/sma_cross.lua", line: 23, ok: true,
		},
		{
			// The parser's own format shares no punctuation with a raised
			// error, and it is the finding whose line is most certain.
			name:    "a compile error, in the parser's own format",
			message: "check.lua line:3(column:3) near 'end':   syntax error",
			script:  "check.lua", line: 3, ok: true,
		},
		{
			name:    "the first position wins over the traceback",
			message: "check.lua:2: unknown name\nstack traceback:\n\tcheck.lua:9: in main chunk",
			script:  "check.lua", line: 2, ok: true,
		},
		{
			name:    "a message with no script at all",
			message: "benchmark $SP500 has no data over 2020-01-01..2024-01-01",
			ok:      false,
		},
		{
			// The reason the .lua suffix is required: a time or a date must
			// not send the editor to a line.
			name:    "a time is not a position",
			message: "run started 12:30:00 and covered 2020-01-01",
			ok:      false,
		},
		{
			name:    "line zero is not a line",
			message: "mine.lua:0: something",
			ok:      false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			script, line, ok := ScriptPosition(tc.message)
			if ok != tc.ok || script != tc.script || line != tc.line {
				t.Errorf("ScriptPosition = (%q, %d, %v), want (%q, %d, %v)",
					script, line, ok, tc.script, tc.line, tc.ok)
			}
		})
	}
}

// The parse happens in diagf, so every host gets it without asking.
func TestDiagnosticsCarryTheirPosition(t *testing.T) {
	var got []Diagnostic
	prev := Diagnostics
	Diagnostics = func(d Diagnostic) { got = append(got, d) }
	defer func() { Diagnostics = prev }()

	diagf("error", "P", "lua strategy %q: %s", "mine.lua", "mine.lua:7: boom")
	diagf("warn", "P", "no benchmark data")

	if len(got) != 2 {
		t.Fatalf("collected %d diagnostics", len(got))
	}
	if got[0].Script != "mine.lua" || got[0].Line != 7 {
		t.Errorf("position not attached: %+v", got[0])
	}
	if got[1].Script != "" || got[1].Line != 0 {
		t.Errorf("a message with no position got one: %+v", got[1])
	}
}
