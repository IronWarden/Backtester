package backtest

// Checking a config without running it.
//
// This session's validation — reversed windows, duplicate tickers, negative
// capital, Params that reach a strategy which cannot read them, swept keys
// nothing reads — all fires while a run is being set up. That is the right
// place for it, but it means the only way to learn a config is wrong is to
// start a backtest and wait.
//
// CheckConfig runs the same construction path and stops there. It opens no
// database, simulates no days and reads only the config and any Lua scripts it
// names, so a whole config is checked in milliseconds against a 2.9 GB file it
// never touches.
//
// It deliberately reports through the same Diagnostics sink as a real run
// rather than inventing a second channel: the point is that -check tells you
// exactly what the run would have told you, only sooner.

import (
	"fmt"
	"strings"
)

// CheckConfig validates every portfolio a config describes and returns how
// many outright errors it found. Warnings are reported too, and do not count:
// a config with warnings still runs, it just may not do what was meant.
//
// defaultLuaPath stands in for a portfolio that omits Strategy, matching what
// the UI does with the script open in its editor. Empty means an omitted
// Strategy is an error, as it is on the CLI.
func CheckConfig(cfg *Config, defaultLuaPath string) int {
	errors := 0
	fail := func(name, format string, args ...any) {
		errors++
		diagf("error", name, format, args...)
	}

	if len(cfg.Portfolios) == 0 {
		fail("", "config defines no portfolios")
		return errors
	}

	seenNames := make(map[string]bool, len(cfg.Portfolios))
	for _, pc := range cfg.Portfolios {
		// Two portfolios with one name are not rejected by the engine, but
		// every result, log line and research-log row is keyed by that name,
		// so the pair is indistinguishable everywhere it matters.
		if pc.Name != "" && seenNames[pc.Name] {
			diagf("warn", pc.Name,
				"portfolio name %q is used more than once; results and log "+
					"lines for the two cannot be told apart", pc.Name)
		}
		seenNames[pc.Name] = true

		if strings.TrimSpace(pc.Strategy) == "" {
			if defaultLuaPath == "" {
				fail(pc.Name,
					"portfolio %q: Strategy is required and no default Lua "+
						"script is set", pc.Name)
				continue
			}
			pc.Strategy = "lua:" + defaultLuaPath
		}

		// The same fork a run takes: a walk-forward block is a schedule of
		// portfolios and never reaches ToPortfolios, so checking only the
		// latter would miss every walk-forward config in the file.
		if !pc.WalkForward.Zero() {
			if _, err := pc.WalkForward.normalized(); err != nil {
				fail(pc.Name, "portfolio %q: %v", pc.Name, err)
				continue
			}
			if _, err := walkForwardCandidates(&pc); err != nil {
				fail(pc.Name, "portfolio %q: %v", pc.Name, err)
				continue
			}
		} else if _, err := pc.ToPortfolios(); err != nil {
			// Already opens with `portfolio "name":`; wrapping it again
			// stutters. The walk-forward errors above describe the schedule
			// rather than the portfolio, so those do need the name.
			fail(pc.Name, "%v", err)
			continue
		}

		checkLuaParams(&pc)
	}

	// An unusable [Output] block wastes a whole run: the file is opened after
	// the simulation, so a typo in `fields` is discovered at the very end.
	if cfg.Output != nil {
		if _, err := NewReporter(cfg.Output); err != nil {
			fail("", "[Output]: %v", err)
		}
	}
	return errors
}

// checkLuaParams runs the unread-Params scan a Lua strategy would otherwise
// only get once its interpreter starts, which is partway into a simulation.
// Sweep keys are merged in the same way ToPortfolios merges them, so a swept
// key nothing reads is caught here too.
func checkLuaParams(pc *PortfolioConfig) {
	if !strings.HasPrefix(pc.Strategy, "lua:") {
		return // built-ins are covered by warnIgnoredParams
	}
	path, err := resolveLuaPath(strings.TrimPrefix(pc.Strategy, "lua:"))
	if err != nil {
		return // ToPortfolios already reported it
	}
	params := pc.Params
	if len(pc.Sweep) > 0 {
		combos, err := sweepCombos(pc.Sweep)
		if err == nil && len(combos) > 0 {
			params = mergeParams(pc.Params, combos[0])
		}
	}
	p := &Portfolio{Pname: pc.Name, Trials: 1}
	if len(pc.Sweep) > 0 {
		if combos, err := sweepCombos(pc.Sweep); err == nil {
			p.Trials = len(combos)
		}
	}
	warnUnreadParams(p, path, params)
}

// CheckSummary renders a one-line verdict for the CLI.
func CheckSummary(errors, warnings int) string {
	switch {
	case errors > 0:
		return fmt.Sprintf("config check FAILED: %d error(s), %d warning(s)",
			errors, warnings)
	case warnings > 0:
		return fmt.Sprintf("config check passed with %d warning(s)", warnings)
	}
	return "config check passed"
}
