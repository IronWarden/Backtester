package backtest

import (
	"testing"
	"time"

	"my-backtester/src/data"
)

func shortDay(s string) time.Time {
	d, _ := time.Parse("2006-01-02", s)
	return d
}

func shortBook(cash float64, allow bool) *Portfolio {
	return &Portfolio{
		Pname:              "short",
		Tickers:            []string{"X"},
		BuyingPower:        cash,
		InitialBuyingPower: cash,
		Positions:          map[string]*Position{},
		AllowShort:         allow,
	}
}

// The default must be the long-only engine, unchanged. A sell with nothing held
// is refused exactly as it always was.
func TestSellWithoutAllowShortStillRefuses(t *testing.T) {
	p := shortBook(1000, false)
	p.Sell("X", 10, 20, shortDay("2020-01-02"))

	if len(p.Positions) != 0 || p.BuyingPower != 1000 || len(p.Trades) != 0 {
		t.Errorf("long-only sell filled: %d positions, cash %.2f, %d trades",
			len(p.Positions), p.BuyingPower, len(p.Trades))
	}
}

func TestSellOpensAShort(t *testing.T) {
	p := shortBook(1000, true)
	p.Sell("X", 10, 20, shortDay("2020-01-02"))

	pos, ok := p.Positions["X"]
	if !ok {
		t.Fatal("no position was opened")
	}
	if pos.Amount != -10 {
		t.Errorf("Amount = %.2f, want -10", pos.Amount)
	}
	if pos.AveragePrice != 20 {
		t.Errorf("AveragePrice = %.2f, want 20 (the price shorted at)",
			pos.AveragePrice)
	}
	// The proceeds are credited: 1000 + 10*20.
	if p.BuyingPower != 1200 {
		t.Errorf("cash = %.2f, want 1200", p.BuyingPower)
	}
	// Opening realises nothing, the same way opening a long does not.
	if len(p.Trades) != 1 || p.Trades[0].RealizedPnL != 0 {
		t.Errorf("opening a short realised %.2f, want 0",
			p.Trades[0].RealizedPnL)
	}
}

// The point of the whole feature: the book must be worth more when the shorted
// stock falls, and less when it rises.
func TestShortIsMarkedToMarket(t *testing.T) {
	bars := func(close float64) map[string][]data.AssetData {
		return map[string][]data.AssetData{
			"X": {{Date: shortDay("2020-01-02"), Close: close}},
		}
	}
	p := shortBook(1000, true)
	p.Sell("X", 10, 20, shortDay("2020-01-02"))

	cases := []struct {
		name  string
		close float64
		want  float64
	}{
		// Cash 1200, buying 10 shares back costs 10*price.
		{"unchanged", 20, 1000},
		{"stock halves", 10, 1100},
		{"stock doubles", 40, 800},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := p.GetPortfolioValue(p.Tickers, bars(tc.close), 0)
			if got != tc.want {
				t.Errorf("value at %.2f: got %.2f, want %.2f",
					tc.close, got, tc.want)
			}
		})
	}
}

func TestBuyCoversAShortAndRealisesTheGain(t *testing.T) {
	p := shortBook(1000, true)
	p.Sell("X", 10, 20, shortDay("2020-01-02"))
	// Shorted at 20, covered at 12: a gain of 8 a share.
	p.Buy("X", 10, 12, shortDay("2020-02-03"))

	if _, ok := p.Positions["X"]; ok {
		t.Error("the position survived a full cover")
	}
	if p.BuyingPower != 1080 {
		t.Errorf("cash = %.2f, want 1080 (1000 + 200 proceeds - 120 to cover)",
			p.BuyingPower)
	}
	if len(p.Trades) != 2 {
		t.Fatalf("got %d trades, want 2", len(p.Trades))
	}
	if got := p.Trades[1].RealizedPnL; got != 80 {
		t.Errorf("realised %.2f, want 80 — a cover's gain is what it was "+
			"shorted at less what it cost to close", got)
	}
}

func TestCoveringAtAHigherPriceRealisesALoss(t *testing.T) {
	p := shortBook(1000, true)
	p.Sell("X", 10, 20, shortDay("2020-01-02"))
	p.Buy("X", 10, 25, shortDay("2020-02-03"))

	if got := p.Trades[1].RealizedPnL; got != -50 {
		t.Errorf("realised %.2f, want -50", got)
	}
	if p.BuyingPower != 950 {
		t.Errorf("cash = %.2f, want 950", p.BuyingPower)
	}
}

func TestPartialCoverLeavesTheRestShort(t *testing.T) {
	p := shortBook(1000, true)
	p.Sell("X", 10, 20, shortDay("2020-01-02"))
	p.Buy("X", 4, 15, shortDay("2020-02-03"))

	pos, ok := p.Positions["X"]
	if !ok {
		t.Fatal("the remaining short was closed entirely")
	}
	if pos.Amount != -6 {
		t.Errorf("Amount = %.2f, want -6", pos.Amount)
	}
	// The unclosed shares keep their original entry price.
	if pos.AveragePrice != 20 {
		t.Errorf("AveragePrice = %.2f, want 20", pos.AveragePrice)
	}
	if got := p.Trades[1].RealizedPnL; got != 20 {
		t.Errorf("realised %.2f, want 20 (4 shares x 5)", got)
	}
}

// Selling more than is held closes the long first and shorts the remainder,
// which is what a broker does with a single oversized sell order.
func TestSellPastALongClosesItThenShorts(t *testing.T) {
	p := shortBook(1000, true)
	p.Buy("X", 10, 20, shortDay("2020-01-02"))
	p.Sell("X", 15, 30, shortDay("2020-02-03"))

	pos, ok := p.Positions["X"]
	if !ok {
		t.Fatal("no short remained")
	}
	if pos.Amount != -5 {
		t.Errorf("Amount = %.2f, want -5", pos.Amount)
	}
	if pos.AveragePrice != 30 {
		t.Errorf("AveragePrice = %.2f, want 30", pos.AveragePrice)
	}
	// Only the 10 closing shares realise: 10 x (30 - 20).
	if got := p.Trades[1].RealizedPnL; got != 100 {
		t.Errorf("realised %.2f, want 100 — only the closing portion of an "+
			"oversized sell realises anything", got)
	}
}

func TestShorterAveragePriceBlendsAcrossOrders(t *testing.T) {
	p := shortBook(10000, true)
	p.Sell("X", 10, 20, shortDay("2020-01-02"))
	p.Sell("X", 10, 30, shortDay("2020-02-03"))

	pos := p.Positions["X"]
	if pos.Amount != -20 {
		t.Fatalf("Amount = %.2f, want -20", pos.Amount)
	}
	if pos.AveragePrice != 25 {
		t.Errorf("AveragePrice = %.2f, want 25", pos.AveragePrice)
	}
}

// The only leverage limit this increment has. Without it a strategy could short
// without bound and report the result as a finding.
func TestShortIsCappedAtCash(t *testing.T) {
	p := shortBook(100, true)
	// 10 shares at 20 is 200 of notional against 100 of cash.
	p.Sell("X", 10, 20, shortDay("2020-01-02"))

	if len(p.Positions) != 0 {
		t.Errorf("an over-leveraged short filled: %+v", p.Positions["X"])
	}
	// 5 shares at 20 is exactly the cash, and must fill.
	p.Sell("X", 5, 20, shortDay("2020-01-02"))
	if pos, ok := p.Positions["X"]; !ok || pos.Amount != -5 {
		t.Errorf("a short exactly at the cap was refused")
	}
}

// Gross, not net: a market-neutral book is not an unexposed one.
func TestExposureIsGrossWhenShorting(t *testing.T) {
	bars := map[string][]data.AssetData{
		"L": {{Date: shortDay("2020-01-02"), Close: 10}},
		"S": {{Date: shortDay("2020-01-02"), Close: 10}},
	}
	p := &Portfolio{
		Pname: "neutral", Tickers: []string{"L", "S"},
		BuyingPower: 1000, InitialBuyingPower: 1000,
		Positions:  map[string]*Position{},
		AllowShort: true,
	}
	p.Buy("L", 20, 10, shortDay("2020-01-02"))
	p.Sell("S", 20, 10, shortDay("2020-01-02"))

	// Long 200, short 200: net zero, gross 400.
	value := p.GetPortfolioValue(p.Tickers, bars, 0)
	p.AdjustPortfolioParameters(p.Tickers, bars, 0, 1000, value)

	if len(p.Exposure) != 1 {
		t.Fatalf("got %d exposure points, want 1", len(p.Exposure))
	}
	if got := p.Exposure[0]; got != 0.4 {
		t.Errorf("exposure = %.4f, want 0.4000 (gross 400 over a 1000 book); "+
			"a net measure would report 0 and call a market-neutral book flat",
			got)
	}
}

func TestBorrowIsChargedOnShortsOnly(t *testing.T) {
	// 10% a year on a 200 short is 20 a year, or 20/252 a day.
	const rate = 1000.0 // bps
	want := 200 * (rate / 10000.0) / 252.0

	short := shortBook(1000, true)
	short.Costs = CostConfig{BorrowBpsAnnual: rate}
	short.Sell("X", 10, 20, shortDay("2020-01-02"))
	cashBefore := short.BuyingPower
	short.chargeBorrow()

	if got := cashBefore - short.BuyingPower; !nearly(got, want) {
		t.Errorf("borrow charged %.6f, want %.6f", got, want)
	}
	if !nearly(short.Metrics.BorrowPaid, want) {
		t.Errorf("BorrowPaid = %.6f, want %.6f", short.Metrics.BorrowPaid, want)
	}

	// The same rate on a long book charges nothing at all.
	long := shortBook(1000, false)
	long.Costs = CostConfig{BorrowBpsAnnual: rate}
	long.Buy("X", 10, 20, shortDay("2020-01-02"))
	cashBefore = long.BuyingPower
	long.chargeBorrow()

	if long.BuyingPower != cashBefore || long.Metrics.BorrowPaid != 0 {
		t.Errorf("a long book was charged borrow: cash moved %.6f, paid %.6f",
			cashBefore-long.BuyingPower, long.Metrics.BorrowPaid)
	}
}

// A long in the same book must not offset the short's borrow. Found by mutation
// testing: skipping the sign check still passed every other test here, because a
// long's negative contribution left `gross` at or below zero and a later guard
// bailed out. On a MIXED book that same slip silently under-charges the short.
func TestBorrowIgnoresLongsInAMixedBook(t *testing.T) {
	p := &Portfolio{
		Pname: "mixed", Tickers: []string{"L", "S"},
		BuyingPower: 10000, InitialBuyingPower: 10000,
		Positions:  map[string]*Position{},
		AllowShort: true,
		Costs:      CostConfig{BorrowBpsAnnual: 1000},
	}
	p.Buy("L", 50, 20, shortDay("2020-01-02"))  // long 1000
	p.Sell("S", 10, 20, shortDay("2020-01-02")) // short 200

	p.chargeBorrow()

	// The short's 200, not 200 netted against the long's 1000.
	want := 200 * 0.10 / 252.0
	if got := p.Metrics.BorrowPaid; !nearly(got, want) {
		t.Errorf("borrow on a mixed book = %.6f, want %.6f — the long must "+
			"not offset the short", got, want)
	}
}

func TestBorrowAccruesEveryDayItIsHeld(t *testing.T) {
	p := shortBook(1000, true)
	p.Costs = CostConfig{BorrowBpsAnnual: 1000}
	p.Sell("X", 10, 20, shortDay("2020-01-02"))

	one := 200 * 0.10 / 252.0
	for i := 0; i < 5; i++ {
		p.chargeBorrow()
	}
	if got := p.Metrics.BorrowPaid; !nearly(got, 5*one) {
		t.Errorf("five days of borrow = %.6f, want %.6f", got, 5*one)
	}

	// Covered: nothing further accrues.
	p.Buy("X", 10, 20, shortDay("2020-02-03"))
	p.chargeBorrow()
	if got := p.Metrics.BorrowPaid; !nearly(got, 5*one) {
		t.Errorf("borrow accrued after covering: %.6f, want %.6f", got, 5*one)
	}
}

// The borrow is valued at the mark the position was CARRIED at, not at a price
// it never had.
func TestBorrowUsesTheCarriedMark(t *testing.T) {
	p := shortBook(1000, true)
	p.Costs = CostConfig{BorrowBpsAnnual: 1000}
	p.Sell("X", 10, 20, shortDay("2020-01-02"))
	// The stock doubled and the position was marked there yesterday.
	p.Positions["X"].CurrentPrice = 40

	p.chargeBorrow()
	want := 400 * 0.10 / 252.0
	if got := p.Metrics.BorrowPaid; !nearly(got, want) {
		t.Errorf("borrow on a marked-up short = %.6f, want %.6f (10 shares at "+
			"the 40 mark, not the 20 it was shorted at)", got, want)
	}
}

// A borrow charge must never turn into a blotter entry: no shares move, and a
// trade record would corrupt the trade count, win rate and holding period.
func TestBorrowIsNotATrade(t *testing.T) {
	p := shortBook(1000, true)
	p.Costs = CostConfig{BorrowBpsAnnual: 1000}
	p.Sell("X", 10, 20, shortDay("2020-01-02"))
	before := len(p.Trades)
	p.chargeBorrow()

	if len(p.Trades) != before {
		t.Errorf("borrow wrote %d blotter entries, want 0",
			len(p.Trades)-before)
	}
}

func TestZeroBorrowRateChargesNothing(t *testing.T) {
	p := shortBook(1000, true)
	p.Sell("X", 10, 20, shortDay("2020-01-02"))
	cash := p.BuyingPower
	p.chargeBorrow()

	if p.BuyingPower != cash || p.Metrics.BorrowPaid != 0 {
		t.Errorf("the default borrow rate charged something: %.6f",
			cash-p.BuyingPower)
	}
}

func TestNegativeBorrowRateIsRejected(t *testing.T) {
	pc := PortfolioConfig{
		Name: "paid-to-short", BuyingPower: 1000,
		StartTime: "2020-01-01", EndTime: "2020-12-31",
		Tickers: []string{"AAPL"}, Strategy: "buyandhold",
		Costs: CostConfig{BorrowBpsAnnual: -50},
	}
	if _, err := pc.ToPortfolio(); err == nil {
		t.Error("a negative borrow rate was accepted; it would pay a strategy " +
			"to hold a short")
	}
}

func nearly(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d < 1e-9
}
