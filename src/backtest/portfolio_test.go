package backtest

import (
	"math"
	"my-backtester/src/data"
	"testing"
	"time"
)

// Portfolio is the only place cash and share counts are mutated, so the
// invariants pinned here are the ones a strategy bug would otherwise hide:
// an order never spends cash the portfolio does not have, a sale never
// creates shares, and the daily return / close-value series stay aligned
// with each other and with the calendar.

var testDay = time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

func newTestPortfolio(t *testing.T, buyingPower float64) *Portfolio {
	t.Helper()
	p, err := InitializePortfolio(
		buyingPower,
		testDay, testDay.AddDate(1, 0, 0),
		"test", []string{"AAA"}, "greedy", nil,
	)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	return p
}

func TestBuyOpensAndAveragesPositions(t *testing.T) {
	p := newTestPortfolio(t, 1000)

	p.Buy("AAA", 10, 10, testDay)
	pos, ok := p.FindPosition("AAA")
	if !ok {
		t.Fatal("no position after first buy")
	}
	closeTo(t, "amount", pos.Amount, 10)
	closeTo(t, "average price", pos.AveragePrice, 10)
	closeTo(t, "buying power", p.BuyingPower, 900)

	// Adding to the position blends the cost basis by share count:
	// (10*10 + 10*20) / 20 = 15.
	p.Buy("AAA", 10, 20, testDay)
	closeTo(t, "amount after add", pos.Amount, 20)
	closeTo(t, "average price after add", pos.AveragePrice, 15)
	closeTo(t, "buying power after add", p.BuyingPower, 700)
}

func TestBuyRejectsBadOrders(t *testing.T) {
	cases := []struct {
		name   string
		amount float64
		price  float64
	}{
		{"zero amount", 0, 10},
		{"negative amount", -5, 10},
		{"zero price", 10, 0},
		{"negative price", 10, -10},
		// 10 * 20 = 200 against 100 of cash.
		{"insufficient buying power", 10, 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPortfolio(t, 100)
			p.Buy("AAA", tc.amount, tc.price, testDay)
			if _, ok := p.FindPosition("AAA"); ok {
				t.Errorf("position opened by rejected order")
			}
			closeTo(t, "buying power", p.BuyingPower, 100)
		})
	}
}

// A strategy sizing fractional shares to spend all of its cash can overshoot
// by a sub-cent rounding error. That order must still fill, clamped, rather
// than being dropped or driving BuyingPower negative.
func TestBuyClampsFloatOvershoot(t *testing.T) {
	p := newTestPortfolio(t, 1000)
	price := 3.0
	amount := 1000.0/price + 1e-9 // cost overshoots 1000 by ~3e-9

	p.Buy("AAA", amount, price, testDay)

	if p.BuyingPower != 0 {
		t.Errorf("BuyingPower = %v, want exactly 0 after clamped fill", p.BuyingPower)
	}
	pos, ok := p.FindPosition("AAA")
	if !ok {
		t.Fatal("clamped order did not fill")
	}
	// The clamp trims the amount back to what the cash actually buys.
	closeTo(t, "clamped amount", pos.Amount, 1000.0/price)
}

func TestBuyRejectsOvershootBeyondTolerance(t *testing.T) {
	p := newTestPortfolio(t, 1000)
	p.Buy("AAA", 1000.0/3.0+1e-3, 3, testDay)
	if _, ok := p.FindPosition("AAA"); ok {
		t.Error("order beyond the clamp tolerance should be rejected")
	}
	closeTo(t, "buying power", p.BuyingPower, 1000)
}

func TestSellPartialAndFull(t *testing.T) {
	p := newTestPortfolio(t, 1000)
	p.Buy("AAA", 10, 10, testDay) // 900 cash, 10 shares @ 10

	p.Sell("AAA", 4, 20, testDay)
	pos, ok := p.FindPosition("AAA")
	if !ok {
		t.Fatal("partial sell removed the position")
	}
	closeTo(t, "amount after partial sell", pos.Amount, 6)
	closeTo(t, "buying power after partial sell", p.BuyingPower, 980)
	// Selling does not re-mark the cost basis.
	closeTo(t, "average price after partial sell", pos.AveragePrice, 10)

	p.Sell("AAA", 6, 20, testDay)
	if _, ok := p.FindPosition("AAA"); ok {
		t.Error("fully sold position should be deleted")
	}
	closeTo(t, "buying power after full sell", p.BuyingPower, 1100)
}

func TestSellRejectsBadOrders(t *testing.T) {
	t.Run("no position", func(t *testing.T) {
		p := newTestPortfolio(t, 1000)
		p.Sell("AAA", 1, 10, testDay)
		closeTo(t, "buying power", p.BuyingPower, 1000)
	})

	t.Run("more shares than held", func(t *testing.T) {
		p := newTestPortfolio(t, 1000)
		p.Buy("AAA", 10, 10, testDay)
		p.Sell("AAA", 11, 10, testDay)

		pos, ok := p.FindPosition("AAA")
		if !ok {
			t.Fatal("rejected sell removed the position")
		}
		closeTo(t, "amount", pos.Amount, 10)
		closeTo(t, "buying power", p.BuyingPower, 900)
	})
}

func TestDepositWithdraw(t *testing.T) {
	p := newTestPortfolio(t, 1000)
	p.Deposit(250)
	closeTo(t, "after deposit", p.BuyingPower, 1250)
	p.Withdraw(500)
	closeTo(t, "after withdraw", p.BuyingPower, 750)
	// InitialBuyingPower is the CAGR denominator and must not move.
	closeTo(t, "initial buying power", p.InitialBuyingPower, 1000)
}

func TestAdjustPortfolioParametersRecordsSeries(t *testing.T) {
	p := newTestPortfolio(t, 1000)
	p.Buy("AAA", 10, 10, testDay)

	hist := map[string][]data.AssetData{
		"AAA": {
			{Date: testDay, Close: 10},
			{Date: testDay.AddDate(0, 0, 1), Close: 12},
			{Date: testDay.AddDate(0, 0, 2), Close: 9},
		},
	}
	tickers := []string{"AAA"}

	p.AdjustPortfolioParameters(tickers, hist, 0, 1000, 1000)
	p.AdjustPortfolioParameters(tickers, hist, 1, 1000, 1020)
	p.AdjustPortfolioParameters(tickers, hist, 2, 1020, 990)

	if len(p.DailyReturns) != 3 || len(p.PortfolioCloseValues) != 3 {
		t.Fatalf("recorded %d returns / %d close values, want 3 each",
			len(p.DailyReturns), len(p.PortfolioCloseValues))
	}

	wantReturns := []float64{0, 0.02, (990 - 1020) / 1020.0}
	for i, want := range wantReturns {
		closeTo(t, "daily return", p.DailyReturns[i].Return, want)
		if !p.DailyReturns[i].Date.Equal(hist["AAA"][i].Date) {
			t.Errorf("return %d dated %v, want %v",
				i, p.DailyReturns[i].Date, hist["AAA"][i].Date)
		}
	}
	for i, want := range []float64{1000, 1020, 990} {
		closeTo(t, "close value", p.PortfolioCloseValues[i], want)
	}

	// Open positions are marked to the day's close.
	pos, _ := p.FindPosition("AAA")
	closeTo(t, "current price", pos.CurrentPrice, 9)
}

func TestAdjustPortfolioParametersZeroStartingValue(t *testing.T) {
	p := newTestPortfolio(t, 0)
	hist := map[string][]data.AssetData{
		"AAA": {{Date: testDay, Close: 10}},
	}
	p.AdjustPortfolioParameters([]string{"AAA"}, hist, 0, 0, 100)

	got := p.DailyReturns[0].Return
	if math.IsNaN(got) || math.IsInf(got, 0) || got != 0 {
		t.Errorf("return from a zero starting value = %v, want 0", got)
	}
}

func TestGetPortfolioValue(t *testing.T) {
	p := newTestPortfolio(t, 1000)
	p.Buy("AAA", 10, 10, testDay) // 900 cash + 10 shares

	hist := map[string][]data.AssetData{
		"AAA": {{Date: testDay, Close: 10}, {Date: testDay, Close: 15}},
	}
	tickers := []string{"AAA"}

	closeTo(t, "day 0 value", p.GetPortfolioValue(tickers, hist, 0), 1000)
	closeTo(t, "day 1 value", p.GetPortfolioValue(tickers, hist, 1), 1050)
	// A day past the end of a ticker's history contributes nothing rather
	// than panicking on an out-of-range index.
	closeTo(t, "day past history", p.GetPortfolioValue(tickers, hist, 9), 900)
}

func TestCloneResetsState(t *testing.T) {
	p := newTestPortfolio(t, 1000)
	p.Buy("AAA", 10, 10, testDay)
	p.DailyReturns = append(p.DailyReturns, DailyReturn{Date: testDay, Return: 0.1})
	p.PortfolioCloseValues = append(p.PortfolioCloseValues, 1000)

	c, err := p.Clone()
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	closeTo(t, "clone buying power", c.BuyingPower, 1000)
	if len(c.Positions) != 0 {
		t.Errorf("clone carried %d positions", len(c.Positions))
	}
	if len(c.DailyReturns) != 0 || len(c.PortfolioCloseValues) != 0 {
		t.Error("clone carried recorded series")
	}
	// Workers must not share strategy state across passes.
	if c.Strategy == p.Strategy {
		t.Error("clone shares the parent's Strategy instance")
	}

	c.Buy("AAA", 5, 10, testDay)
	if pos, _ := p.FindPosition("AAA"); pos.Amount != 10 {
		t.Errorf("parent position mutated by clone activity: %v", pos.Amount)
	}
}

// The runner simulates a Clone of every portfolio, never the original, so a
// cost model that does not survive Clone is a cost model that never applies
// to a real run — while every test that builds a Portfolio directly still
// passes. That failure is silent and expensive, so it gets its own test.
func TestCloneCarriesCosts(t *testing.T) {
	p := newTestPortfolio(t, 1000)
	p.Costs = CostConfig{
		CommissionPerTrade: 1.5,
		CommissionBps:      2.5,
		SlippageBps:        5,
	}

	c, err := p.Clone()
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	if c.Costs != p.Costs {
		t.Errorf("clone dropped the cost model: got %+v, want %+v",
			c.Costs, p.Costs)
	}
}

// Every figure in this file's cost tests is computed by hand in the
// comment above it. The point of the cost model is to make results
// slightly worse in a precisely known way; an assertion derived from the
// implementation would prove nothing.

func TestBuyWithCosts(t *testing.T) {
	p := newTestPortfolio(t, 10000)
	p.Costs = CostConfig{
		CommissionPerTrade: 1.0,
		CommissionBps:      10, // 0.10% of notional
		SlippageBps:        50, // 0.50% worse fill
	}

	// Quoted 100, slippage 50bps -> fill 100 * 1.005 = 100.5
	// notional      = 10 * 100.5            = 1005
	// commission    = 1.0 + 1005 * 0.001    = 2.005
	// total         = 1005 + 2.005          = 1007.005
	// buying power  = 10000 - 1007.005      = 8992.995
	p.Buy("AAA", 10, 100, testDay)

	pos, ok := p.FindPosition("AAA")
	if !ok {
		t.Fatal("order did not fill")
	}
	closeTo(t, "shares", pos.Amount, 10)
	closeTo(t, "cost basis is the slipped price", pos.AveragePrice, 100.5)
	closeTo(t, "buying power", p.BuyingPower, 8992.995)
}

func TestSellWithCosts(t *testing.T) {
	p := newTestPortfolio(t, 10000)
	p.Costs = CostConfig{
		CommissionPerTrade: 1.0,
		CommissionBps:      10,
		SlippageBps:        50,
	}
	p.Buy("AAA", 10, 100, testDay) // leaves 8992.995, 10 shares

	// Quoted 120, slippage 50bps -> fill 120 * 0.995 = 119.4
	// notional     = 10 * 119.4           = 1194
	// commission   = 1.0 + 1194 * 0.001   = 2.194
	// proceeds     = 1194 - 2.194         = 1191.806
	// buying power = 8992.995 + 1191.806  = 10184.801
	p.Sell("AAA", 10, 120, testDay)

	if _, ok := p.FindPosition("AAA"); ok {
		t.Error("fully sold position should be deleted")
	}
	closeTo(t, "buying power", p.BuyingPower, 10184.801)
}

// Costs are a drag, never a boost: a round trip at an unchanged price must
// end with less cash than it started with, and the loss is exactly the two
// commissions plus the two slippage legs.
func TestCostsAreAlwaysADrag(t *testing.T) {
	p := newTestPortfolio(t, 10000)
	p.Costs = CostConfig{CommissionPerTrade: 1.0, SlippageBps: 50}

	p.Buy("AAA", 10, 100, testDay)
	p.Sell("AAA", 10, 100, testDay)

	// Buy:  notional 1005,  fee 1 -> -1006
	// Sell: notional 995,   fee 1 -> +994
	// Net: 10000 - 1006 + 994 = 9988
	closeTo(t, "round-trip cost", p.BuyingPower, 9988)
	if p.BuyingPower >= 10000 {
		t.Error("a round trip must never leave the portfolio better off")
	}
}

// The clamp is the delicate part of the cost model: a strategy sizing
// fractional shares against its whole balance must still fill, and the
// commission has to come out of that same balance rather than overdrawing
// it.
func TestBuyClampLeavesRoomForCommission(t *testing.T) {
	cases := []struct {
		name  string
		costs CostConfig
	}{
		{"flat commission only", CostConfig{CommissionPerTrade: 1}},
		{"bps commission only", CostConfig{CommissionBps: 10}},
		{"both", CostConfig{CommissionPerTrade: 1, CommissionBps: 10}},
		{"with slippage", CostConfig{CommissionPerTrade: 1, CommissionBps: 10, SlippageBps: 50}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPortfolio(t, 1000)
			p.Costs = tc.costs

			// Size the order the way a "spend it all" strategy does, then
			// overshoot by a rounding error.
			price := 3.0
			amount := 1000.0/p.Costs.BuyFill(price) + 1e-9

			p.Buy("AAA", amount, price, testDay)

			if p.BuyingPower < 0 {
				t.Fatalf("clamped buy overdrew the account: %v", p.BuyingPower)
			}
			pos, ok := p.FindPosition("AAA")
			if !ok {
				t.Fatal("clamped order did not fill at all")
			}
			// It filled for slightly fewer shares than the uncosted order
			// would have, because the fee had to come out of the same cash.
			if pos.Amount >= amount {
				t.Errorf("filled %v shares, expected fewer than the "+
					"requested %v once the fee is paid", pos.Amount, amount)
			}
			closeTo(t, "spent the whole balance", p.BuyingPower, 0)
		})
	}
}

// An order whose commission alone exceeds the balance cannot fill. It must
// leave the portfolio untouched rather than filling and overdrawing.
func TestBuyRejectsUnaffordableCommission(t *testing.T) {
	p := newTestPortfolio(t, 5)
	p.Costs = CostConfig{CommissionPerTrade: 10}

	// The shares are affordable; the fee is not.
	p.Buy("AAA", 1, 4, testDay)

	if _, ok := p.FindPosition("AAA"); ok {
		t.Error("order filled despite an unaffordable commission")
	}
	closeTo(t, "buying power", p.BuyingPower, 5)
}

// The same rule on the way out: a sale whose fee would overdraw the account
// does not happen, and the shares stay put.
func TestSellRejectsWhenFeeWouldOverdraw(t *testing.T) {
	p := newTestPortfolio(t, 1000)
	p.Buy("AAA", 10, 100, testDay) // 0 cash left, 10 shares
	closeTo(t, "cash after buy", p.BuyingPower, 0)

	p.Costs = CostConfig{CommissionPerTrade: 50}
	// Sell one share for 1: proceeds 1 - 50 = -49, against 0 cash.
	p.Sell("AAA", 1, 1, testDay)

	pos, ok := p.FindPosition("AAA")
	if !ok {
		t.Fatal("rejected sell removed the position")
	}
	closeTo(t, "shares untouched", pos.Amount, 10)
	closeTo(t, "buying power", p.BuyingPower, 0)
}

// Slippage alone, with no commission, moves the fill price in the right
// direction on both sides.
func TestSlippageDirection(t *testing.T) {
	c := CostConfig{SlippageBps: 100} // 1%
	closeTo(t, "buys fill higher", c.BuyFill(100), 101)
	closeTo(t, "sells fill lower", c.SellFill(100), 99)

	// A zero cost model must return the quoted price bit for bit, which is
	// what makes existing results reproducible.
	var zero CostConfig
	for _, quoted := range []float64{1, 3, 99.99, 1234.5678, 1e-8} {
		if got := zero.BuyFill(quoted); got != quoted {
			t.Errorf("zero-cost BuyFill(%v) = %v, want the quote unchanged", quoted, got)
		}
		if got := zero.SellFill(quoted); got != quoted {
			t.Errorf("zero-cost SellFill(%v) = %v, want the quote unchanged", quoted, got)
		}
	}
	if got := zero.Commission(1e6); got != 0 {
		t.Errorf("zero-cost Commission = %v, want 0", got)
	}
}

// The point of the cost model is that it penalises trading, so the drag has
// to scale with how much a strategy trades. This runs real shipped
// strategies through the runner's own path, twice each, and compares.
func TestCostsDragScalesWithTurnover(t *testing.T) {
	finalValue := func(script string, params map[string]any, costs CostConfig) float64 {
		t.Helper()
		hist := synthHist(42)
		p := libPortfolio(nil)
		p.Costs = costs
		strat, err := NewLuaStrategy(strategiesDir(t)+"/"+script, params)
		if err != nil {
			t.Fatalf("%s: %v", script, err)
		}
		defer strat.Close()
		p.Strategy = strat
		runOne(p, hist, map[int64]float64{})
		n := len(p.PortfolioCloseValues)
		if n == 0 {
			t.Fatalf("%s produced no simulated days", script)
		}
		return p.PortfolioCloseValues[n-1]
	}

	costs := CostConfig{CommissionPerTrade: 5, CommissionBps: 10, SlippageBps: 20}
	all := shippedStrategies()

	// One entry per ticker on day 0 and nothing after.
	holdFree := finalValue("buy_and_hold.lua", all["buy_and_hold.lua"], CostConfig{})
	holdCost := finalValue("buy_and_hold.lua", all["buy_and_hold.lua"], costs)
	// Buys on a cadence for the whole window.
	dcaFree := finalValue("dca.lua", all["dca.lua"], CostConfig{})
	dcaCost := finalValue("dca.lua", all["dca.lua"], costs)

	if holdCost >= holdFree {
		t.Errorf("buy-and-hold with costs (%v) should trail the free run (%v)",
			holdCost, holdFree)
	}
	if dcaCost >= dcaFree {
		t.Errorf("DCA with costs (%v) should trail the free run (%v)",
			dcaCost, dcaFree)
	}

	holdDrag := (holdFree - holdCost) / holdFree
	dcaDrag := (dcaFree - dcaCost) / dcaFree
	if dcaDrag <= holdDrag {
		t.Errorf("DCA drag %.6f should exceed buy-and-hold drag %.6f — "+
			"costs are not scaling with turnover", dcaDrag, holdDrag)
	}
	t.Logf("drag: buy_and_hold %.4f%%, dca %.4f%%", holdDrag*100, dcaDrag*100)
}

// Turnover is per-run state, not configuration: unlike Costs it must NOT
// survive Clone, or a reused portfolio would report the previous pass's
// trading on top of its own.
func TestCloneResetsTradedNotional(t *testing.T) {
	p := newTestPortfolio(t, 10000)
	p.Buy("AAA", 10, 100, testDay)
	if p.tradedNotional == 0 {
		t.Fatal("a filled buy did not record any traded notional")
	}

	c, err := p.Clone()
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	if c.tradedNotional != 0 {
		t.Errorf("clone carried %v of traded notional, want 0", c.tradedNotional)
	}
}

func TestTradedNotionalCountsBothSides(t *testing.T) {
	p := newTestPortfolio(t, 10000)
	p.Buy("AAA", 10, 100, testDay)  // 1000 notional
	p.Sell("AAA", 5, 120, testDay)  // 600 notional
	p.Sell("AAA", 99, 120, testDay) // rejected: more than held
	p.Buy("AAA", 10, 0, testDay)    // rejected: zero price

	closeTo(t, "gross traded notional", p.tradedNotional, 1600)
}

// End to end: the metric a real run reports must reflect how much the
// strategy actually traded.
func TestTurnoverReflectsStrategyActivity(t *testing.T) {
	turnoverOf := func(script string) float64 {
		t.Helper()
		hist := synthHist(42)
		p := libPortfolio(nil)
		strat, err := NewLuaStrategy(
			strategiesDir(t)+"/"+script, shippedStrategies()[script],
		)
		if err != nil {
			t.Fatalf("%s: %v", script, err)
		}
		defer strat.Close()
		p.Strategy = strat
		runOne(p, hist, map[int64]float64{})
		return p.Metrics.Turnover
	}

	hold := turnoverOf("buy_and_hold.lua")
	dca := turnoverOf("dca.lua")

	if hold <= 0 {
		t.Errorf("buy-and-hold turnover = %v; the day-0 entry should count", hold)
	}
	if dca <= hold {
		t.Errorf("DCA turnover %v should exceed buy-and-hold %v", dca, hold)
	}
	t.Logf("turnover: buy_and_hold %.4f, dca %.4f", hold, dca)
}

func TestCostConfigZero(t *testing.T) {
	if !(CostConfig{}).Zero() {
		t.Error("the zero CostConfig must report Zero()")
	}
	cases := map[string]CostConfig{
		"flat commission": {CommissionPerTrade: 1},
		"bps commission":  {CommissionBps: 1},
		"slippage":        {SlippageBps: 1},
	}
	for name, c := range cases {
		if c.Zero() {
			t.Errorf("%s: Zero() = true for %+v", name, c)
		}
	}
}
