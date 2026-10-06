package strategies

import "testing"

func TestIsLibrary(t *testing.T) {
	for path, want := range map[string]bool{
		"rsi.lua":                               true,
		"strategies/rsi.lua":                    true,
		"../strategies/rsi.lua":                 true,
		"/home/u/Backtester/strategies/rsi.lua": true,
		`C:\Backtester\strategies\rsi.lua`:      true,
		// The user's own: the scratch file and every copy.
		"strategies/my_strategy.lua": false,
		"strategies/my_rsi.lua":      false,
		// Not in the library, or not in a strategies directory.
		"strategies/not_shipped.lua": false,
		"elsewhere/rsi.lua":          false,
		"":                           false,
	} {
		if got := IsLibrary(path); got != want {
			t.Errorf("IsLibrary(%q) = %v, want %v", path, got, want)
		}
	}
}
