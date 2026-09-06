package backtest

// Checking a strategy script without running a backtest.
//
// CheckConfig gave the TOML a way to be wrong sooner than a run; the script
// had none. A missing comma, a `step` spelled `Step`, a nil arithmetic on the
// first day -- all of it surfaced only by starting a backtest against a 2.9 GB
// database and reading the console afterwards. For someone learning both Lua
// and this API at once, that loop is the whole experience.
//
// This opens no database and simulates no market. It compiles the script,
// runs its top level, requires a `step` function, and then calls that function
// for a few days against a SYNTHETIC two-ticker history built here in memory.
// The dry run is what catches the errors that matter -- a nil compared with a
// number, a function called with the arguments in the wrong order -- because
// those live inside step, and nothing that only compiles the file will ever
// execute it.
//
// The dry run's findings are warnings, never errors, and they say what they
// ran against. A strategy that reads fundamentals gets nil here, because this
// history has no fundamentals; that is a limitation of the check and not
// necessarily a defect in the script, and the message has to admit it.

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"my-backtester/src/data"

	lua "github.com/yuin/gopher-lua"
)

// checkDays is how many days of the synthetic history the dry run steps
// through. Three is enough to execute the body, cheap enough to stay
// instantaneous, and small enough that a script logging per day does not fill
// the console.
const checkDays = 3

// checkTimeout bounds the whole dry run. A `while true do end` at the top
// level of a script would otherwise hang the app on a keystroke away from the
// Check button, which is the one failure a checker must not have.
const checkTimeout = 2 * time.Second

// checkHistoryDays is the length of the synthetic series. Long enough that a
// 200-day moving average has history to work with, so a trend strategy's
// warm-up branch is not the only branch the check ever sees.
const checkHistoryDays = 260

// CheckLuaScript compiles and dry-runs a strategy, reporting through the same
// Diagnostics sink a real run uses, and returns how many outright errors it
// found. Warnings do not count: a script with warnings still runs.
//
// name is what the findings call the script; params stands in for
// [portfolio.Params], so a script reading params.period sees what the config
// would give it.
func CheckLuaScript(source, name string, params map[string]any) int {
	errors := 0
	fail := func(format string, args ...any) {
		errors++
		diagf("error", name, format, args...)
	}

	if strings.TrimSpace(source) == "" {
		fail("%s is empty: a strategy is a file defining a step(day) function", name)
		return errors
	}

	L := lua.NewState()
	defer L.Close()
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()
	L.SetContext(ctx)

	chunk, err := L.Load(strings.NewReader(source), name)
	if err != nil {
		// A compile error already carries "name:line:" and the parser's
		// message, which is more precise than anything that could be added.
		fail("%v", err)
		return errors
	}

	hist := checkHistory()
	p := checkPortfolio(params)
	registerIndicators(L, hist)
	registerOHLCV(L, hist)
	registerTrading(L, p, hist, name)
	registerLogging(L, p, name)
	registerWindow(L, p, hist)
	registerSignals(L, hist)
	registerCrossSection(L, p)
	registerFundamentals(L, p, hist)
	registerBook(L, p, hist)
	tickersTbl := L.CreateTable(len(p.Tickers), 0)
	for i, t := range p.Tickers {
		tickersTbl.RawSetInt(i+1, lua.LString(t))
	}
	L.SetGlobal("tickers", tickersTbl)
	L.SetGlobal("params", goToLua(L, params))

	L.Push(chunk)
	if err := L.PCall(0, lua.MultRet, nil); err != nil {
		fail("%v", err)
		return errors
	}

	stepFn, ok := L.GetGlobal("step").(*lua.LFunction)
	if !ok {
		if L.GetGlobal("step") == lua.LNil {
			fail("%s defines no global 'step': the engine calls step(day) once "+
				"per trading day, and a script without one never trades", name)
		} else {
			fail("%s: 'step' is not a function, so the engine has nothing to call",
				name)
		}
		return errors
	}

	for day := 0; day < checkDays; day++ {
		L.Push(stepFn)
		L.Push(lua.LNumber(day))
		if err := L.PCall(1, 0, nil); err != nil {
			// A warning, not an error: this ran against invented prices with
			// no fundamentals, so a script that needs either can raise here
			// and still be correct in a real run. Saying what it ran against
			// is the difference between a useful warning and a confusing one.
			diagf("warn", name,
				"step(%d) raised while checking against %d days of synthetic "+
					"prices for %s (no fundamentals, no real dates): %v",
				day, checkHistoryDays, strings.Join(p.Tickers, " and "), err)
			return errors
		}
	}

	for _, w := range unreadParamWarnings(source, params) {
		diagf("warn", name, "%s", w)
	}
	return errors
}

// paramsReadRe finds the config keys a script actually reads: params.period
// and params["period"] both count. Matched on the source rather than observed
// during the dry run, because a key read inside a branch the three checked
// days never take is still a key the script reads.
var paramsReadRe = regexp.MustCompile(
	`params\s*(?:\.\s*([A-Za-z_][A-Za-z0-9_]*)|\[\s*["']([^"']+)["']\s*\])`)

// unreadParamWarnings reports keys the config sets that the script never
// mentions. It is the pre-run half of the warning the engine already emits
// during a run, and it matters most in a sweep, where a key nothing reads
// means every trial ran identical settings.
//
// The reverse -- a key the script reads that the config does not set -- is
// deliberately NOT reported: params.period defaulting to a literal when the
// config omits it is ordinary Lua, and warning about it would fire on half the
// shipped library.
func unreadParamWarnings(source string, params map[string]any) []string {
	if len(params) == 0 {
		return nil
	}
	read := map[string]bool{}
	for _, m := range paramsReadRe.FindAllStringSubmatch(source, -1) {
		if m[1] != "" {
			read[m[1]] = true
		}
		if m[2] != "" {
			read[m[2]] = true
		}
	}
	var out []string
	for key := range params {
		if !read[key] {
			out = append(out, fmt.Sprintf(
				"the config sets params.%s but the script never reads it; in a "+
					"sweep that means every trial ran identical settings", key))
		}
	}
	sort.Strings(out)
	return out
}

// checkHistory builds two deterministic price series in memory. Two rather
// than one so a cross-sectional script (rank, target_weights) has something to
// discriminate between, and deterministic so a check that passes today passes
// tomorrow.
func checkHistory() map[string][]data.AssetData {
	base := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)
	hist := make(map[string][]data.AssetData, 2)
	for i, ticker := range checkTickers {
		series := make([]data.AssetData, checkHistoryDays)
		px := 100.0
		for d := 0; d < checkHistoryDays; d++ {
			// A gentle deterministic wave, different per ticker: it rises,
			// falls and crosses its own averages, so a crossover strategy has
			// a crossing to find rather than a flat line.
			px = 100 + float64(10+5*i)*math.Sin(float64(d)/float64(40+20*i))
			series[d] = data.AssetData{
				Date:   base.AddDate(0, 0, d),
				Open:   px * 0.995,
				High:   px * 1.01,
				Low:    px * 0.99,
				Close:  px,
				Volume: 1_000_000,
			}
		}
		hist[ticker] = series
	}
	return hist
}

var checkTickers = []string{"AAA", "BBB"}

// checkPortfolio is a scratch book for the dry run: real cash, so orders
// succeed and the order paths are exercised, and nothing outside this function
// can see it.
func checkPortfolio(params map[string]any) *Portfolio {
	return &Portfolio{
		Pname:                "check",
		BuyingPower:          100_000,
		InitialBuyingPower:   100_000,
		Positions:            make(map[string]*Position),
		DailyReturns:         make([]DailyReturn, 0, checkHistoryDays),
		PortfolioCloseValues: make([]float64, 0, checkHistoryDays),
		Tickers:              checkTickers,
		StrategyParams:       params,
	}
}
