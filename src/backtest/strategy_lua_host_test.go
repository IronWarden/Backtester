package backtest

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"my-backtester/src/data"
)

// The Lua host boundary is where untrusted user scripts meet the worker
// pool. A misbehaving script must always fail the same way: log, keep the
// portfolio consistent, and let the run continue. A panic or a hang here
// takes down a worker and, with it, every other portfolio in the batch —
// so these tests care less about what a broken script produces than about
// the fact that it produces it without taking anything else with it.

const hostDays = 5

// hostScript writes src to a temp .lua file and returns its path.
func hostScript(t *testing.T, src string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "strategy.lua")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func hostHist(tickers []string) map[string][]data.AssetData {
	day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	hist := make(map[string][]data.AssetData, len(tickers))
	for ti, ticker := range tickers {
		series := make([]data.AssetData, hostDays)
		for i := range series {
			price := 100 + float64(i) + float64(ti*10)
			series[i] = data.AssetData{
				Date:   day.AddDate(0, 0, i),
				Open:   price,
				High:   price + 1,
				Low:    price - 1,
				Close:  price,
				Volume: 1000,
			}
		}
		hist[ticker] = series
	}
	return hist
}

// runHostScript steps a script across the whole window with the package
// logger captured, and reports the resulting portfolio plus anything the
// host logged. It fails the test if the script panics.
func runHostScript(
	t *testing.T, src string, params map[string]any,
) (*Portfolio, string) {
	t.Helper()
	tickers := []string{"AAA", "BBB"}
	hist := hostHist(tickers)

	p, err := InitializePortfolio(
		10000,
		hist["AAA"][0].Date, hist["AAA"][hostDays-1].Date,
		"host-test", tickers, "lua:"+hostScript(t, src), params,
	)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	strat := p.Strategy.(*LuaStrategy)
	defer strat.Close()

	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	defer func() {
		if r := recover(); r != nil {
			log.SetOutput(prev)
			t.Fatalf("host panicked on a misbehaving script: %v", r)
		}
	}()

	for day := 0; day < hostDays; day++ {
		strat.Step(p, hist, day)
	}
	return p, logBuf.String()
}

// assertNoTrades checks the portfolio is exactly as it started.
func assertNoTrades(t *testing.T, p *Portfolio) {
	t.Helper()
	if len(p.Positions) != 0 {
		t.Errorf("portfolio holds %d positions, want none", len(p.Positions))
	}
	closeTo(t, "buying power", p.BuyingPower, p.InitialBuyingPower)
}

// A script that does not parse must be reported once per attempted day and
// must never leave a half-built interpreter behind.
func TestLuaHostParseError(t *testing.T) {
	p, logs := runHostScript(t, "function step(day) this is not lua end", nil)

	if !strings.Contains(logs, "init") {
		t.Errorf("parse failure was not logged as an init error:\n%s", logs)
	}
	assertNoTrades(t, p)

	strat := p.Strategy.(*LuaStrategy)
	if strat.L != nil {
		t.Error("failed init left a live lua.LState on the strategy")
	}
}

// `step` is the entire contract with a script. Anything else — missing, or
// present but not callable — has to fail at init rather than at call time.
func TestLuaHostMissingStepFunction(t *testing.T) {
	cases := map[string]string{
		"no step at all":     "local x = 1",
		"step is a number":   "step = 42",
		"step is a table":    "step = {}",
		"step spelled wrong": "function Step(day) end",
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			p, logs := runHostScript(t, src, nil)
			if !strings.Contains(logs, "step") {
				t.Errorf("expected a 'step' complaint, got:\n%s", logs)
			}
			assertNoTrades(t, p)
		})
	}
}

// A script that loads but raises mid-run loses only the day it raised on.
// The rest of the window still runs, which is what keeps one bad branch
// from silently truncating a backtest.
func TestLuaHostRuntimeErrorDoesNotStopTheRun(t *testing.T) {
	p, logs := runHostScript(t, `
function step(day)
  if day == 1 then
    error("boom on day 1")
  end
  if day == 4 then
    buy("AAA", 1, price("AAA", day), day)
  end
end
`, nil)

	if !strings.Contains(logs, "boom on day 1") {
		t.Errorf("runtime error not logged:\n%s", logs)
	}
	if strings.Count(logs, "boom") != 1 {
		t.Errorf("expected exactly one error line, got:\n%s", logs)
	}
	// Day 4 came after the failure and must still have executed.
	pos, ok := p.FindPosition("AAA")
	if !ok {
		t.Fatal("no trade after the erroring day — the run stopped early")
	}
	closeTo(t, "shares", pos.Amount, 1)
}

// Indexing a nil value is the most common script bug; it must behave like
// any other runtime error rather than escaping as a Go panic.
func TestLuaHostNilIndexError(t *testing.T) {
	p, logs := runHostScript(t, `
function step(day)
  local pos = position("AAA")   -- nil while flat
  if pos.amount > 0 then sell_all("AAA", price("AAA", day), day) end
end
`, nil)

	if !strings.Contains(logs, "step") {
		t.Errorf("nil-index error not logged:\n%s", logs)
	}
	assertNoTrades(t, p)
}

// Host functions are called from an untyped language, so every one of them
// has to tolerate junk arguments. None may panic, and none may move money.
func TestLuaHostWrongArgumentTypes(t *testing.T) {
	p, _ := runHostScript(t, `
function step(day)
  if day ~= 0 then return end
  -- Strings, tables, booleans and nils where numbers are expected.
  -- A nil *ticker* is deliberately absent here: buy(nil, 1, 100, day)
  -- currently fills against the empty-string ticker and destroys the
  -- cash. See T9 in TASKS.md.
  buy("AAA", "many", "cheap", day)
  buy({}, {}, {}, {})
  buy_max(true, "free", 12345, "day")
  sell("AAA", "some", nil, day)
  sell_all(nil, nil, nil)
  -- Missing arguments entirely.
  buy()
  sell()
  sell_all()
  buy_max()
  position()
  price()
  sma()
  rsi()
end
`, nil)

	assertNoTrades(t, p)
}

// The read-only accessors are the ones scripts call in tight loops, so
// their out-of-range and unknown-ticker behaviour is a documented contract:
// numbers return 0, date_at returns "", and nothing raises.
func TestLuaHostAccessorsOutOfRange(t *testing.T) {
	p, logs := runHostScript(t, `
function step(day)
  if day ~= 0 then return end
  local checks = {
    {"price past end", price("AAA", 999), 0},
    {"price negative day", price("AAA", -1), 0},
    {"unknown ticker", price("ZZZ", 0), 0},
    {"open past end", open_at("AAA", 999), 0},
    {"high unknown", high_at("ZZZ", 0), 0},
    {"low negative", low_at("AAA", -5), 0},
    {"volume past end", volume_at("AAA", 999), 0},
    {"sma without history", sma("AAA", 0, 20), 0},
    {"sma zero period", sma("AAA", 3, 0), 0},
    {"rsi without history", rsi("AAA", 0, 14), 50},
    {"rsi zero period", rsi("AAA", 3, 0), 50},
  }
  for i = 1, #checks do
    local name, got, want = checks[i][1], checks[i][2], checks[i][3]
    assert(got == want, name..": got "..tostring(got).." want "..tostring(want))
  end
  assert(date_at("AAA", 999) == "", "date past end: "..date_at("AAA", 999))
  assert(date_at("ZZZ", 0) == "", "date unknown ticker")
  assert(date_at("AAA", 0) == "2024-01-02", "date day 0: "..date_at("AAA", 0))
end
`, nil)

	if logs != "" {
		t.Errorf("accessor contract violated:\n%s", logs)
	}
	assertNoTrades(t, p)
}

// A strategy that decides to sit in cash is legitimate, not an error: it
// must run silently and leave the portfolio untouched.
func TestLuaHostNoAllocation(t *testing.T) {
	cases := map[string]string{
		"empty step": "function step(day) end",
		"returns early": `
function step(day)
  if day >= 0 then return end
  buy_max("AAA", price("AAA", day), "equalWeights", day)
end`,
		"orders that are all rejected": `
function step(day)
  buy("AAA", 0, price("AAA", day), day)        -- zero shares
  buy("AAA", 1, 0, day)                        -- zero price
  buy("AAA", 1e9, price("AAA", day), day)      -- more than the cash on hand
  sell("AAA", 5, price("AAA", day), day)       -- nothing held
end`,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			p, logs := runHostScript(t, src, nil)
			if logs != "" {
				t.Errorf("expected a silent run, got:\n%s", logs)
			}
			assertNoTrades(t, p)
			if len(p.DailyReturns) != 0 {
				t.Errorf("strategy recorded %d daily returns; that is the "+
					"runner's job, not the host's", len(p.DailyReturns))
			}
		})
	}
}

// Close releases the interpreter and is called by the runner after every
// pass; it must tolerate being called twice and must not resurrect state.
func TestLuaHostCloseIsIdempotent(t *testing.T) {
	tickers := []string{"AAA"}
	hist := hostHist(tickers)
	path := hostScript(t, "function step(day) end")

	strat, err := NewLuaStrategy(path, nil)
	if err != nil {
		t.Fatalf("NewLuaStrategy: %v", err)
	}
	p, err := InitializePortfolio(
		1000, hist["AAA"][0].Date, hist["AAA"][hostDays-1].Date,
		"close-test", tickers, "lua:"+path, nil,
	)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}

	strat.Step(p, hist, 0)
	if strat.L == nil {
		t.Fatal("Step did not initialize the interpreter")
	}
	strat.Close()
	if strat.L != nil {
		t.Error("Close left the lua.LState attached")
	}
	strat.Close() // must not panic
}
