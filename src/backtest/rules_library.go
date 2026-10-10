package backtest

// The shipped scripts the rules menu can say exactly, restated as rules
// (T136), so a beginner's first contact with them is a sentence rather than
// Lua. Each twin is the script AT ITS DEFAULT PARAMETERS, and
// TestLibraryTwinsMatchTheirScripts holds every one to the cent against the
// script itself -- a twin that drifts from its script fails the build.
//
// What is NOT here, and why: trend_following.lua trades on crossings
// (buy_max / sell_all), which a target-weight rule set does not reproduce
// exactly; dca.lua is driven by a cash-flow schedule, sma_cross / donchian /
// rsi / bollinger trade on events or levels the menu lacks, risk_parity
// weights by inverse variance with its own lookback rules, and the
// fundamentals screens read SEC figures the menu has no signal for.

// libraryTwins maps a library file to its rule-set twin.
var libraryTwins = map[string]RulesConfig{
	// Rank by 126-day return, hold the top 2; the script gives capital only
	// to winners with positive momentum, which is a rising_126 filter.
	"momentum_rotation.lua": {Rebalance: "monthly", Rank: "return_126", Pick: 2, Filter: "rising_126"},
	// Back to equal weight every 63 trading days. The script's optional
	// weights are Weight = "allocations" with the card's percentages.
	"rebalance.lua": {Rebalance: "quarterly"},
}

// LibraryTwins returns a copy of every twin, keyed by library file name.
func LibraryTwins() map[string]RulesConfig {
	out := make(map[string]RulesConfig, len(libraryTwins))
	for k, v := range libraryTwins {
		out[k] = v
	}
	return out
}
