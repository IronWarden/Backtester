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
