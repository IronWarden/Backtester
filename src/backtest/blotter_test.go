package backtest

// The blotter's job is to answer "where did the money come from", so the tests
// are about attribution being right rather than about the summing being novel.
// Every expectation is derived by hand from a path chosen to make the answer
// obvious.

import (
	"math"
	"testing"
	"time"
)

func blotterDay(offset int) time.Time {
	return exactEpoch.AddDate(0, 0, offset)
}

// One round trip, hand-derived end to end: buy 10 at 100, sell at 120, no
// costs. That is +200 realised, one win, and ten days held.
func TestBlotterRecordsARoundTrip(t *testing.T) {
	p, err := InitializePortfolio(exactCash, exactEpoch,
		exactEpoch.AddDate(0, 1, 0), "blotter", []string{"AAA"},
		"buyAndHold:equalWeights", nil)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}

	p.Buy("AAA", 10, 100, blotterDay(0))
	p.Sell("AAA", 10, 120, blotterDay(10))

	if len(p.Trades) != 2 {
		t.Fatalf("recorded %d trades, want 2: %+v", len(p.Trades), p.Trades)
	}
	buy, sell := p.Trades[0], p.Trades[1]
	if buy.Side != "buy" || sell.Side != "sell" {
		t.Fatalf("sides = %q, %q", buy.Side, sell.Side)
	}
	almost(t, "buy notional", buy.Notional, 1000)
	if buy.RealizedPnL != 0 {
		t.Errorf("a buy realised %v, want 0", buy.RealizedPnL)
	}
	almost(t, "realised P&L", sell.RealizedPnL, 200)
	almost(t, "holding days", sell.HoldingDays, 10)

	stats := SummarizeTrades(p.Trades, p.Positions)
	if stats.Trades != 2 || stats.Buys != 1 || stats.Sells != 1 {
		t.Errorf("counts = %+v", stats)
	}
	if stats.Wins != 1 || stats.Losses != 0 {
		t.Errorf("wins/losses = %d/%d, want 1/0", stats.Wins, stats.Losses)
	}
	almost(t, "win rate", stats.WinRate, 1.0)
	almost(t, "avg win", stats.AvgWin, 200)
	almost(t, "total realised", stats.TotalRealized, 200)
	almost(t, "avg holding days", stats.AvgHoldingDays, 10)
}

// Attribution is the point of the whole feature: one name makes the money and
// another loses it, and the per-ticker table must say which was which rather
// than reporting the net.
func TestBlotterAttributesPnLPerTicker(t *testing.T) {
	p, err := InitializePortfolio(exactCash, exactEpoch,
		exactEpoch.AddDate(0, 1, 0), "attr", []string{"WIN", "LOSE"},
		"buyAndHold:equalWeights", nil)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}

	p.Buy("WIN", 10, 100, blotterDay(0))   // 1,000 in
	p.Buy("LOSE", 10, 100, blotterDay(0))  // 1,000 in
	p.Sell("WIN", 10, 150, blotterDay(20)) // +500
	p.Sell("LOSE", 10, 80, blotterDay(20)) // -200

	stats := SummarizeTrades(p.Trades, p.Positions)
	if len(stats.PerTicker) != 2 {
		t.Fatalf("attribution has %d rows, want 2", len(stats.PerTicker))
	}
	// Sorted by contribution, so the winner is first.
	if stats.PerTicker[0].Ticker != "WIN" {
		t.Errorf("first row is %q, want WIN — attribution is not sorted by "+
			"contribution", stats.PerTicker[0].Ticker)
	}
	almost(t, "WIN realised", stats.PerTicker[0].Realized, 500)
	almost(t, "LOSE realised", stats.PerTicker[1].Realized, -200)
	almost(t, "net realised", stats.TotalRealized, 300)

	// The net alone would say "+300 and a 50% win rate", which is exactly the
	// summary that hides a strategy carried by one position.
	almost(t, "win rate", stats.WinRate, 0.5)
	almost(t, "profit factor", stats.ProfitFactor, 500.0/200.0)
	almost(t, "avg win", stats.AvgWin, 500)
	almost(t, "avg loss", stats.AvgLoss, 200)
}

// Fees belong to the trade that paid them, and realised P&L is net of them —
// otherwise a strategy that is gross-profitable and net-negative reads as a
// winner.
func TestBlotterChargesFeesToTheTrade(t *testing.T) {
	p, err := InitializePortfolio(exactCash, exactEpoch,
		exactEpoch.AddDate(0, 1, 0), "fees", []string{"AAA"},
		"buyAndHold:equalWeights", nil)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}
	p.Costs = CostConfig{CommissionPerTrade: 5.0}

	p.Buy("AAA", 10, 100, blotterDay(0))
	p.Sell("AAA", 10, 101, blotterDay(5))

	stats := SummarizeTrades(p.Trades, p.Positions)
	almost(t, "total fees", stats.TotalFees, 10) // 5 on each side
	// Gross +10, minus the 5 charged to the sell: the buy's own fee is already
	// in the average cost the engine keeps.
	almost(t, "realised P&L", stats.TotalRealized, 5)
	if stats.Wins != 1 {
		t.Errorf("wins = %d, want 1 (the trade cleared its own fee)", stats.Wins)
	}
}

// Holding periods are FIFO: selling half a position that was built in two
// parcels consumes the older one first. Average-cost has no answer to "how long
// was this held", which is why the two accountings differ on purpose.
func TestHoldingPeriodIsFIFO(t *testing.T) {
	p, err := InitializePortfolio(exactCash, exactEpoch,
		exactEpoch.AddDate(0, 2, 0), "fifo", []string{"AAA"},
		"buyAndHold:equalWeights", nil)
	if err != nil {
		t.Fatalf("InitializePortfolio: %v", err)
	}

	p.Buy("AAA", 10, 100, blotterDay(0))  // parcel one
	p.Buy("AAA", 10, 100, blotterDay(30)) // parcel two
	p.Sell("AAA", 10, 110, blotterDay(40))

	sell := p.Trades[len(p.Trades)-1]
	// FIFO: the shares sold are the ones bought on day 0, so 40 days — not the
	// 25 an average of the two parcels would give.
	almost(t, "holding days", sell.HoldingDays, 40)

	// Selling the rest consumes the second parcel: 10 days.
	p.Sell("AAA", 10, 110, blotterDay(40))
	sell = p.Trades[len(p.Trades)-1]
	almost(t, "second holding days", sell.HoldingDays, 10)
}

// Weighted by shares, so one token exit cannot drag the average.
func TestAvgHoldingDaysIsShareWeighted(t *testing.T) {
	trades := []Trade{
		{Ticker: "AAA", Side: "sell", Shares: 99, HoldingDays: 10},
		{Ticker: "AAA", Side: "sell", Shares: 1, HoldingDays: 1000},
	}
	stats := SummarizeTrades(trades, nil)
	want := (99*10.0 + 1*1000.0) / 100.0
	almost(t, "avg holding days", stats.AvgHoldingDays, want)
	if stats.AvgHoldingDays > 100 {
		t.Errorf("avg holding = %v; an unweighted mean would be 505 and would "+
			"be dominated by a one-share exit", stats.AvgHoldingDays)
	}
}

// An open position contributes unrealised P&L, and only when it has been
// marked — an unmarked position valued against a zero price would read as a
// total loss.
func TestUnrealizedNeedsAMark(t *testing.T) {
	positions := map[string]*Position{
		"AAA": {Amount: 10, AveragePrice: 100, CurrentPrice: 130},
		"BBB": {Amount: 10, AveragePrice: 100}, // never marked
	}
	stats := SummarizeTrades(nil, positions)

	byTicker := map[string]TickerPnL{}
	for _, row := range stats.PerTicker {
		byTicker[row.Ticker] = row
	}
	almost(t, "AAA unrealised", byTicker["AAA"].Unrealized, 300)
	if byTicker["BBB"].Unrealized != 0 {
		t.Errorf("unmarked position reported %v unrealised, want 0",
			byTicker["BBB"].Unrealized)
	}
	if byTicker["AAA"].SharesHeld != 10 {
		t.Errorf("shares held = %v, want 10", byTicker["AAA"].SharesHeld)
	}
}

// A run that never traded must report zeros rather than dividing by zero — and
// Trades == 0 is the single most important thing a result can say, since a
// strategy that never traded is not a strategy.
func TestEmptyBlotter(t *testing.T) {
	stats := SummarizeTrades(nil, nil)
	if stats.Trades != 0 || stats.RoundTrips != 0 {
		t.Errorf("counts = %+v, want zeros", stats)
	}
	for name, v := range map[string]float64{
		"win rate": stats.WinRate, "avg win": stats.AvgWin,
		"avg loss": stats.AvgLoss, "profit factor": stats.ProfitFactor,
		"avg holding": stats.AvgHoldingDays,
	} {
		if v != 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			t.Errorf("%s = %v on an empty blotter, want 0", name, v)
		}
	}
	if len(stats.PerTicker) != 0 {
		t.Errorf("attribution has %d rows on an empty blotter", len(stats.PerTicker))
	}
}

// A run of wins with no losses must not report an infinite profit factor.
func TestProfitFactorWithNoLosses(t *testing.T) {
	stats := SummarizeTrades([]Trade{
		{Ticker: "AAA", Side: "sell", Shares: 1, RealizedPnL: 100},
	}, nil)
	if math.IsInf(stats.ProfitFactor, 0) || math.IsNaN(stats.ProfitFactor) {
		t.Errorf("profit factor = %v with no losses", stats.ProfitFactor)
	}
	almost(t, "win rate", stats.WinRate, 1.0)
}

// End to end through a real run: the blotter must reflect what the strategy
// actually did, and a clone must start empty so a re-run does not inherit
// another portfolio's trades.
func TestBlotterFillsDuringARun(t *testing.T) {
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA", "BBB"}, map[string][]float64{
			"AAA": {100, 110, 120},
			"BBB": {100, 90, 80},
		}, CostConfig{})

	if p.TradeStats.Trades != 2 {
		t.Fatalf("run recorded %d trades, want 2 (one buy per ticker): %+v",
			p.TradeStats.Trades, p.Trades)
	}
	if p.TradeStats.Buys != 2 || p.TradeStats.Sells != 0 {
		t.Errorf("buy-and-hold made %d buys and %d sells, want 2 and 0",
			p.TradeStats.Buys, p.TradeStats.Sells)
	}
	// Nothing was sold, so all the P&L is unrealised — and the winner leads
	// the attribution table.
	if len(p.TradeStats.PerTicker) != 2 {
		t.Fatalf("attribution has %d rows", len(p.TradeStats.PerTicker))
	}
	if p.TradeStats.PerTicker[0].Ticker != "AAA" {
		t.Errorf("first attribution row is %q, want AAA",
			p.TradeStats.PerTicker[0].Ticker)
	}
	if p.TradeStats.TotalRealized != 0 {
		t.Errorf("realised %v with no sells", p.TradeStats.TotalRealized)
	}

	clone, err := p.Clone()
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	if len(clone.Trades) != 0 {
		t.Errorf("clone inherited %d trades", len(clone.Trades))
	}
}
