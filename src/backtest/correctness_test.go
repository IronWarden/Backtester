package backtest

// End-to-end correctness of the numbers a run produces.
//
// The rest of the suite checks the pieces (metrics_test.go) or checks that a
// full run does not error (strategies_library_test.go). Neither catches a
// change that leaves every strategy running cleanly while quietly shifting
// every Sharpe — and the numbers are the product, so that is the regression
// that matters most.
//
// Every expectation here is derived by hand from a price path chosen so the
// right answer is knowable in closed form, NOT captured from a previous run.
// That distinction is the point: a golden file records whatever the code did
// last time, including its bugs, whereas "10000 dollars into a stock that
// goes 100 -> 150 is worth 15000" is true independently of this codebase and
// stays true if someone rewrites the engine.
//
// These drive runOne, the same per-day loop the real runner uses, so an error
// anywhere in strategy -> portfolio -> valuation -> metrics surfaces here.

import (
	"math"
	"my-backtester/src/data"
	"testing"
	"time"
)

const exactCash = 10_000.0

// exactEpoch is a fixed Monday. Bars are laid on consecutive calendar days:
// the engine never consults a market calendar, it just walks the bars it is
// given, and a fixed epoch keeps the elapsed-years divisor deterministic.
var exactEpoch = time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC)

// seriesFrom builds bars from explicit closes on consecutive days. Open,
// High and Low are set to the close deliberately: every assertion below is
// about figures the engine derives from Close alone, and giving the other
// fields distinct values would let a test pass while reading the wrong one.
func seriesFrom(closes []float64) []data.AssetData {
	series := make([]data.AssetData, len(closes))
	for i, c := range closes {
		series[i] = data.AssetData{
			Date:   exactEpoch.AddDate(0, 0, i),
			Open:   c,
			High:   c,
			Low:    c,
			Close:  c,
			Volume: 1_000_000,
		}
	}
	return series
}

func histFrom(closes map[string][]float64) map[string][]data.AssetData {
	hist := make(map[string][]data.AssetData, len(closes))
	for ticker, cs := range closes {
		hist[ticker] = seriesFrom(cs)
	}
	return hist
}

// runExact simulates a buy-and-hold portfolio over the given closes and
// returns it fully populated. The risk-free table is empty, so excess return
// equals raw return and the ratio metrics depend on the price path alone.
func runExact(
	t *testing.T, spec string, cash float64, tickers []string,
	closes map[string][]float64, costs CostConfig,
) *Portfolio {
	t.Helper()
	hist := histFrom(closes)
	n := len(closes[tickers[0]])
	p, err := InitializePortfolio(
		cash,
		exactEpoch,
		exactEpoch.AddDate(0, 0, n), // inclusive upper bound, past the last bar
		"exact",
		tickers,
		spec,
		nil,
	)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	p.Costs = costs
	runOne(p, hist, map[int64]float64{})
	return p
}

// almost compares against a hand-derived expectation. The tolerance is
// relative and tight: these are short float computations over exact decimal
// inputs, not statistical estimates, so anything looser would hide a real
// arithmetic change.
func almost(t *testing.T, label string, got, want float64) {
	t.Helper()
	const tol = 1e-9
	scale := math.Max(1.0, math.Abs(want))
	if math.Abs(got-want) > tol*scale {
		t.Errorf("%s = %.12f, want %.12f (diff %.3g)",
			label, got, want, math.Abs(got-want))
	}
}

// finalValue is the portfolio's worth on the last simulated day.
func finalValue(t *testing.T, p *Portfolio) float64 {
	t.Helper()
	if len(p.PortfolioCloseValues) == 0 {
		t.Fatal("run produced no daily values")
	}
	return p.PortfolioCloseValues[len(p.PortfolioCloseValues)-1]
}

// Buy-and-hold is a pure multiplier on the price: a portfolio that puts all
// its cash into one stock at 100 and holds to 150 is worth exactly 1.5x, and
// is worth exactly the same multiple of its start on every intermediate day.
// Checking the whole curve rather than just the endpoint is what catches a
// valuation that is right only where the test happens to look.
func TestBuyAndHoldTracksThePriceExactly(t *testing.T) {
	closes := []float64{100, 110, 90, 125, 150}
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{"AAA": closes}, CostConfig{})

	// PortfolioCloseValues records days 1..n-1: day 0 is the entry, and the
	// first return is measured from it.
	if got, want := len(p.PortfolioCloseValues), len(closes)-1; got != want {
		t.Fatalf("recorded %d daily values, want %d", got, want)
	}
	for i, value := range p.PortfolioCloseValues {
		day := i + 1
		want := exactCash * closes[day] / closes[0]
		almost(t, "value on day "+string(rune('0'+day)), value, want)
	}
	almost(t, "final value", finalValue(t, p), 15_000.0)

	// All capital is deployed, so nothing may be left idle in cash.
	almost(t, "leftover cash", p.BuyingPower, 0.0)
}

// equalWeights splits the starting balance evenly and each leg then moves
// with its own stock. One doubling and one halving from a 50/50 split is
// worth 0.5*2 + 0.5*0.5 = 1.25x — the classic result, and a check that
// multi-asset valuation sums the legs rather than tracking only the first.
func TestEqualWeightsValuesEveryLeg(t *testing.T) {
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA", "BBB"}, map[string][]float64{
			"AAA": {100, 150, 200},
			"BBB": {80, 60, 40},
		}, CostConfig{})

	// 5000 into AAA at 100 -> 100 shares -> 10000 at 200.
	// 5000 into BBB at 80  -> 62.5 shares -> 2500 at 40.
	almost(t, "final value", finalValue(t, p), 12_500.0)
	almost(t, "AAA shares", p.Positions["AAA"].Amount, 50.0)
	almost(t, "BBB shares", p.Positions["BBB"].Amount, 62.5)
}

// Max drawdown is the worst peak-to-trough decline. The path below peaks at
// 120 and bottoms at 60, so the answer is exactly 50% regardless of what
// happens afterwards — including the later recovery to a joint high, which
// must not be counted as a second, deeper decline.
func TestMaxDrawdownOfAKnownPricePath(t *testing.T) {
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{
			"AAA": {100, 120, 60, 90, 120},
		}, CostConfig{})

	almost(t, "MaxDrawdown", p.Metrics.MaxDrawdown, 50.0)
}

// A flat price is the one case with no floating-point slack anywhere: every
// daily return is exactly 0, so every metric has an exact right answer. It
// pins the degenerate-input guards to the values the results table needs,
// and would catch a divide-by-zero reintroduced as NaN or Inf.
func TestFlatPriceProducesExactZeros(t *testing.T) {
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{
			"AAA": {100, 100, 100, 100, 100},
		}, CostConfig{})

	almost(t, "final value", finalValue(t, p), exactCash)
	almost(t, "AnnualReturn", p.Metrics.AnnualReturn, 0.0)
	almost(t, "MaxDrawdown", p.Metrics.MaxDrawdown, 0.0)
	almost(t, "StandardDev", p.Metrics.StandardDev, 0.0)
	almost(t, "SharpeRatio", p.Metrics.SharpeRatio, 0.0)
	almost(t, "SortinoRatio", p.Metrics.SortinoRatio, 0.0)
}

// A flat commission is charged once, at entry, and everything after that
// compounds on what is left. 10000 cash with a 10 fee buys 9990 of stock,
// which at 1.5x is 14985 — not 15000 minus 10. Getting this wrong by
// charging the fee against the final value instead is a plausible mistake
// that no "does it run" test would notice.
func TestCommissionIsChargedOnceAtEntry(t *testing.T) {
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{
			"AAA": {100, 150},
		}, CostConfig{CommissionPerTrade: 10.0})

	almost(t, "shares", p.Positions["AAA"].Amount, 99.9)
	almost(t, "leftover cash", p.BuyingPower, 0.0)
	almost(t, "final value", finalValue(t, p), 14_985.0)
}

// The equivalent end-to-end test for slippage is deliberately absent: it
// fails, and it fails because the engine is wrong rather than because the
// test is. Any non-zero slippage_bps makes the built-in buy-and-hold
// strategies buy nothing at all, so the portfolio holds cash for the whole
// run. See task T13 in TASKS.md, which carries the repro and the failing
// test; fixing it changes the cost model's observable behaviour, so it is
// not a change to make unattended.

// Turnover is annualized traded notional over average portfolio value. A
// single entry trade of the full balance, held flat, has a known numerator
// (10000) and a known denominator (the mean of the recorded daily values),
// so the whole figure is arithmetic rather than a captured constant.
func TestTurnoverOfASingleEntryIsExact(t *testing.T) {
	closes := []float64{100, 100, 100, 100}
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{"AAA": closes}, CostConfig{})

	// Flat price: every recorded daily value is the starting cash, so the
	// average value is exactly exactCash and only the entry traded.
	n := float64(len(closes) - 1)
	want := (exactCash / exactCash) * (252.0 / n)
	almost(t, "Turnover", p.Metrics.Turnover, want)
}

// AnnualReturn must be the rate that actually compounds the portfolio from
// its start to its end over the elapsed time. This asserts the relationship
// rather than re-deriving the formula, so it stays honest even if the
// implementation of GetAnnualReturn is rewritten: a CAGR that does not
// reproduce the final value is wrong no matter how it was computed.
func TestAnnualReturnCompoundsBackToTheFinalValue(t *testing.T) {
	closes := []float64{100, 108, 96, 130, 121, 155}
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, map[string][]float64{"AAA": closes}, CostConfig{})

	// The engine annualizes over calendar time between the first and last
	// recorded return, which for consecutive-day bars is len(closes)-2 days.
	first := p.DailyReturns[0].Date
	last := p.DailyReturns[len(p.DailyReturns)-1].Date
	years := last.Sub(first).Hours() / 24 / 365.25

	growth := math.Pow(1+p.Metrics.AnnualReturn/100.0, years)
	almost(t, "compounded growth", growth, closes[len(closes)-1]/closes[0])
}

// Every ratio and percentage metric describes the shape of the return
// series, which does not depend on how much money was in the account. Ten
// times the capital must give bit-comparable metrics — if it does not,
// something is mixing an absolute dollar figure into a relative measure.
func TestMetricsAreInvariantToStartingCapital(t *testing.T) {
	closes := map[string][]float64{
		"AAA": {100, 108, 96, 130, 121, 155},
		"BBB": {50, 52, 47, 61, 58, 55},
	}
	tickers := []string{"AAA", "BBB"}

	small := runExact(t, "buyAndHold:equalWeights", exactCash, tickers, closes, CostConfig{})
	large := runExact(t, "buyAndHold:equalWeights", exactCash*10, tickers, closes, CostConfig{})

	almost(t, "AnnualReturn", large.Metrics.AnnualReturn, small.Metrics.AnnualReturn)
	almost(t, "MaxDrawdown", large.Metrics.MaxDrawdown, small.Metrics.MaxDrawdown)
	almost(t, "StandardDev", large.Metrics.StandardDev, small.Metrics.StandardDev)
	almost(t, "SharpeRatio", large.Metrics.SharpeRatio, small.Metrics.SharpeRatio)
	almost(t, "SortinoRatio", large.Metrics.SortinoRatio, small.Metrics.SortinoRatio)
	almost(t, "Turnover", large.Metrics.Turnover, small.Metrics.Turnover)

	// The absolute figure must scale, or the test above proves nothing.
	almost(t, "final value", finalValue(t, large), finalValue(t, small)*10)
}

// A portfolio holding exactly its own benchmark has no active risk: it is
// the benchmark. Beta 1, no alpha, no tracking error, and it captures 100%
// of both the upside and the downside. These are definitional, so they pin
// all six benchmark-relative metrics at once.
func TestPortfolioThatIsItsOwnBenchmark(t *testing.T) {
	closes := []float64{100, 108, 96, 130, 121, 155}
	hist := histFrom(map[string][]float64{"AAA": closes})

	p, err := InitializePortfolio(
		exactCash, exactEpoch, exactEpoch.AddDate(0, 0, len(closes)),
		"exact", []string{"AAA"}, "buyAndHold:equalWeights", nil,
	)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	p.Benchmark = "AAA"
	runOne(p, hist, map[int64]float64{})

	almost(t, "Beta", p.Metrics.Beta, 1.0)
	almost(t, "Alpha", p.Metrics.Alpha, 0.0)
	almost(t, "TrackingError", p.Metrics.TrackingError, 0.0)
	almost(t, "UpCapture", p.Metrics.UpCapture, 100.0)
	almost(t, "DownCapture", p.Metrics.DownCapture, 100.0)
}

// Twice the benchmark's daily move is a beta of exactly 2. Built by
// compounding a doubled return series into a price path, so the portfolio
// genuinely holds a security that moves that way rather than having its
// returns injected after the fact.
func TestBetaOfADoubleExposurePath(t *testing.T) {
	benchReturns := []float64{0.01, -0.02, 0.015, -0.005, 0.02}

	bench := []float64{100}
	levered := []float64{100}
	for _, r := range benchReturns {
		bench = append(bench, bench[len(bench)-1]*(1+r))
		levered = append(levered, levered[len(levered)-1]*(1+2*r))
	}

	hist := histFrom(map[string][]float64{"LEV": levered, "BENCH": bench})
	p, err := InitializePortfolio(
		exactCash, exactEpoch, exactEpoch.AddDate(0, 0, len(bench)),
		"exact", []string{"LEV"}, "buyAndHold:equalWeights", nil,
	)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	p.Benchmark = "BENCH"
	runOne(p, hist, map[int64]float64{})

	almost(t, "Beta", p.Metrics.Beta, 2.0)
	// Twice the move in both directions, so both capture ratios are 200%.
	almost(t, "UpCapture", p.Metrics.UpCapture, 200.0)
	almost(t, "DownCapture", p.Metrics.DownCapture, 200.0)
}
