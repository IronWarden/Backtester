package backtest

import (
	"bytes"
	"log"
	"math"
	"math/rand"
	"my-backtester/src/data"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Every strategy shipped in strategies/ is exercised here against synthetic
// price series. LuaStrategy reports script failures via log.Printf rather
// than returning an error, so the run captures log output and treats any
// line as a failure — that is what catches a typo, a nil arithmetic error,
// or an API rename that silently breaks a shipped strategy.

const (
	libDays  = 400
	libCash  = 100_000.0
	libStart = 100.0
)

// libTickers exercises the multi-asset paths (ranking, weighting) that the
// rotation and allocation strategies depend on.
var libTickers = []string{"AAA", "BBB", "CCC"}

// synthHist builds deterministic random-walk series with distinct drifts and
// volatilities, so momentum ranking and inverse-volatility weighting both
// have something real to discriminate on.
func synthHist(seed int64) map[string][]data.AssetData {
	rng := rand.New(rand.NewSource(seed))
	hist := make(map[string][]data.AssetData, len(libTickers))
	drifts := []float64{0.0008, 0.0002, -0.0003}
	vols := []float64{0.010, 0.020, 0.006}
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	for ti, ticker := range libTickers {
		series := make([]data.AssetData, libDays)
		px := libStart
		for d := 0; d < libDays; d++ {
			px *= 1 + drifts[ti] + rng.NormFloat64()*vols[ti]
			if px < 1 {
				px = 1 // keep prices positive; a zero close is not a real bar
			}
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

func libPortfolio(strat Strategy) *Portfolio {
	return &Portfolio{
		Pname:                "lib-test",
		BuyingPower:          libCash,
		InitialBuyingPower:   libCash,
		Positions:            make(map[string]*Position),
		DailyReturns:         make([]DailyReturn, 0, libDays),
		PortfolioCloseValues: make([]float64, 0, libDays),
		Tickers:              libTickers,
	}
}

// runStrategyScript drives one script across the whole window and returns
// anything the strategy logged (empty means a clean run).
func runStrategyScript(
	t *testing.T, path string, params map[string]any,
) (*Portfolio, string) {
	t.Helper()
	hist := synthHist(42)
	p := libPortfolio(nil)

	strat, err := NewLuaStrategy(path, params)
	if err != nil {
		t.Fatalf("NewLuaStrategy(%s): %v", path, err)
	}
	defer strat.Close()
	p.Strategy = strat

	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	for day := 0; day < libDays; day++ {
		// Mark positions to market the way the runner does, so strategies
		// reading pos.current_price see live values.
		for ticker, series := range hist {
			if pos, ok := p.Positions[ticker]; ok && pos != nil {
				pos.CurrentPrice = series[day].Close
			}
		}
		strat.Step(p, hist, day)
	}
	return p, logBuf.String()
}

// shippedStrategies lists every script in strategies/ with params that
// exercise it inside the synthetic window (defaults tuned for multi-year
// history would never trigger in 400 days).
func shippedStrategies() map[string]map[string]any {
	return map[string]map[string]any{
		"buy_and_hold.lua":          nil,
		"buy_and_hold_weighted.lua": {"weights": map[string]any{"AAA": 0.5, "BBB": 0.3, "CCC": 0.2}},
		"rsi.lua":                   {"period": int64(14)},
		"sma_cross.lua":             {"short": int64(20), "long": int64(50)},
		"momentum_rotation.lua":     {"lookback": int64(60), "top_n": int64(2), "rebalance_days": int64(21)},
		"trend_following.lua":       {"period": int64(100)},
		"donchian_breakout.lua":     {"entry_period": int64(40), "exit_period": int64(20)},
		"bollinger_reversion.lua":   {"period": int64(20), "k": 2.0},
		"risk_parity.lua":           {"lookback": int64(60), "rebalance_days": int64(21)},
		"rebalance.lua":             {"rebalance_days": int64(21)},
		"dca.lua":                   {"amount": 5000.0, "every_days": int64(21)},
	}
}

// A config written as "lua:strategies/foo.lua" must load whether the
// process started at the repo root (CLI) or in ui/ (the desktop app).
// This test runs with cwd=src/backtest, where that path only exists two
// levels up, so a passing lookup proves the parent-walk works.
func TestResolveLuaPathWalksUp(t *testing.T) {
	got, err := resolveLuaPath(filepath.Join("strategies", "rsi.lua"))
	if err != nil {
		t.Fatalf("resolveLuaPath could not find a repo-root-relative script: %v", err)
	}
	if _, err := os.Stat(got); err != nil {
		t.Fatalf("resolved to %q which does not exist: %v", got, err)
	}

	abs, err := filepath.Abs(filepath.Join("..", "..", "strategies", "rsi.lua"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolveLuaPath(abs); err != nil {
		t.Errorf("absolute path rejected: %v", err)
	}
	if _, err := resolveLuaPath("strategies/definitely_not_here.lua"); err == nil {
		t.Error("a missing script should be an error, not a silent pass")
	}

	// The reported name keeps the spec the user wrote, not the resolved path.
	s, err := NewLuaStrategy(filepath.Join("strategies", "rsi.lua"), nil)
	if err != nil {
		t.Fatalf("NewLuaStrategy: %v", err)
	}
	defer s.Close()
	if want := "lua:strategies/rsi.lua"; s.Name() != want {
		t.Errorf("Name() = %q, want %q", s.Name(), want)
	}
}

// The directory and the test list must not drift apart: a new strategy
// added without a test entry (or a renamed file) fails here.
func TestShippedStrategiesAllCovered(t *testing.T) {
	dir := filepath.Join("..", "..", "strategies")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read strategies dir: %v", err)
	}
	onDisk := map[string]bool{}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".lua") {
			onDisk[e.Name()] = true
		}
	}
	covered := shippedStrategies()
	for name := range onDisk {
		if _, ok := covered[name]; !ok {
			t.Errorf("strategies/%s ships but has no test entry — add it to "+
				"shippedStrategies()", name)
		}
	}
	for name := range covered {
		if !onDisk[name] {
			t.Errorf("shippedStrategies() lists %q but the file is gone", name)
		}
	}
}

func TestShippedStrategiesRunClean(t *testing.T) {
	dir := filepath.Join("..", "..", "strategies")
	for name, params := range shippedStrategies() {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, name)
			p, logged := runStrategyScript(t, path, params)

			// A Lua error surfaces only in the log; treat any output as failure.
			if logged != "" {
				t.Fatalf("strategy logged errors:\n%s", strings.TrimSpace(logged))
			}
			if math.IsNaN(p.BuyingPower) || math.IsInf(p.BuyingPower, 0) {
				t.Fatalf("cash became %v", p.BuyingPower)
			}
			// Cash may legitimately reach ~0 when fully invested, but a
			// meaningfully negative balance means the strategy spent money
			// it did not have.
			if p.BuyingPower < -1e-6 {
				t.Errorf("cash went negative: %v", p.BuyingPower)
			}
			for ticker, pos := range p.Positions {
				if pos == nil {
					continue
				}
				if math.IsNaN(pos.Amount) || math.IsInf(pos.Amount, 0) {
					t.Errorf("%s: position amount is %v", ticker, pos.Amount)
				}
				if pos.Amount < -1e-6 {
					t.Errorf("%s: negative position %v (no shorting in this engine)",
						ticker, pos.Amount)
				}
			}
		})
	}
}

// Strategies that are supposed to trade must actually trade on this data —
// a script that silently no-ops (bad param name, warmup never satisfied)
// would otherwise pass every check above.
func TestShippedStrategiesActuallyTrade(t *testing.T) {
	dir := filepath.Join("..", "..", "strategies")
	params := shippedStrategies()
	for _, name := range []string{
		"buy_and_hold.lua",
		"buy_and_hold_weighted.lua",
		"momentum_rotation.lua",
		"trend_following.lua",
		"donchian_breakout.lua",
		"bollinger_reversion.lua",
		"risk_parity.lua",
		"rebalance.lua",
		"dca.lua",
	} {
		t.Run(name, func(t *testing.T) {
			p, logged := runStrategyScript(
				t, filepath.Join(dir, name), params[name])
			if logged != "" {
				t.Fatalf("strategy logged errors:\n%s", strings.TrimSpace(logged))
			}
			invested := 0.0
			for _, pos := range p.Positions {
				if pos != nil {
					invested += pos.Amount
				}
			}
			if invested == 0 && p.BuyingPower >= libCash {
				t.Errorf("strategy never took a position and never spent cash "+
					"(cash still %v) — it is a no-op on this data", p.BuyingPower)
			}
		})
	}
}
