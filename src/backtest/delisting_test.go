package backtest

import (
	"testing"
	"time"

	"my-backtester/src/data"
)

func delistDay(s string) time.Time {
	d, _ := time.Parse("2006-01-02", s)
	return d
}

// bars builds a flat price series so any change in the final value is the
// haircut and nothing else.
func flatBars(start string, n int, close float64) []data.AssetData {
	d := delistDay(start)
	out := make([]data.AssetData, n)
	for i := range out {
		out[i] = data.AssetData{Date: d.AddDate(0, 0, i), Close: close}
	}
	return out
}

// holding builds a portfolio already long `shares` of `ticker`, so the test is
// about the terminal mark rather than about a strategy's entry.
func holding(ticker string, shares, price float64, ret float64,
	ev map[string]delistingEvent) *Portfolio {
	p := &Portfolio{
		Pname:           "test",
		Tickers:         []string{ticker},
		BuyingPower:     0,
		Positions:       map[string]*Position{},
		DelistingReturn: ret,
		DelistingEvents: ev,
	}
	p.Positions[ticker] = &Position{Amount: shares, AveragePrice: price}
	return p
}

func TestDelistingReturnAppliesOnlyToPerformanceDelistings(t *testing.T) {
	const shares, price = 10.0, 20.0
	windowed := map[string][]data.AssetData{"X": flatBars("2020-01-01", 5, price)}

	cases := []struct {
		name       string
		reason     data.Reason
		wantCash   float64
		wantCount  int
		wantUnclas int
	}{
		// -50% of a $200 position leaves $100.
		{"bankruptcy is haircut", data.ReasonBankruptcy, 100, 1, 0},
		{"compliance removal is haircut", data.ReasonCompliance, 100, 1, 0},
		// An acquisition pays roughly the last close, so the position is left
		// alone entirely — it is not even sold.
		{"acquisition is left alone", data.ReasonAcquired, 0, 0, 0},
		{"voluntary is left alone", data.ReasonVoluntary, 0, 0, 0},
		// Unknown is counted, never guessed at.
		{"unknown is counted, not haircut", data.ReasonUnknown, 0, 0, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := holding("X", shares, price, -0.50, map[string]delistingEvent{
				"X": {Date: delistDay("2020-01-03"), Reason: tc.reason},
			})
			p.applyDelistingReturns(windowed, 5)

			if p.BuyingPower != tc.wantCash {
				t.Errorf("cash after: got %.2f, want %.2f", p.BuyingPower, tc.wantCash)
			}
			if p.Metrics.DelistedHoldings != tc.wantCount {
				t.Errorf("DelistedHoldings: got %d, want %d",
					p.Metrics.DelistedHoldings, tc.wantCount)
			}
			if p.Metrics.UnclassifiedDelistings != tc.wantUnclas {
				t.Errorf("UnclassifiedDelistings: got %d, want %d",
					p.Metrics.UnclassifiedDelistings, tc.wantUnclas)
			}
		})
	}
}

// The default must reproduce previous behaviour exactly, which is the promise
// every config written before this relies on.
func TestZeroDelistingReturnChangesNothing(t *testing.T) {
	windowed := map[string][]data.AssetData{"X": flatBars("2020-01-01", 5, 20)}
	p := holding("X", 10, 20, 0, map[string]delistingEvent{
		"X": {Date: delistDay("2020-01-03"), Reason: data.ReasonBankruptcy},
	})
	p.applyDelistingReturns(windowed, 5)

	if p.BuyingPower != 0 || len(p.Trades) != 0 {
		t.Errorf("a zero DelistingReturn traded: cash %.2f, %d trades",
			p.BuyingPower, len(p.Trades))
	}
	if p.Metrics.DelistedHoldings != 0 {
		t.Errorf("DelistedHoldings: got %d, want 0", p.Metrics.DelistedHoldings)
	}
}

// A company that had not died yet by the last simulated day is untouched. This
// is the guard against haircutting a live holding because its symbol appears
// somewhere in the registry.
func TestDelistingAfterTheWindowIsNotApplied(t *testing.T) {
	windowed := map[string][]data.AssetData{"X": flatBars("2020-01-01", 5, 20)}
	p := holding("X", 10, 20, -0.50, map[string]delistingEvent{
		"X": {Date: delistDay("2021-06-01"), Reason: data.ReasonBankruptcy},
	})
	p.applyDelistingReturns(windowed, 5)

	if p.BuyingPower != 0 || p.Metrics.DelistedHoldings != 0 {
		t.Errorf("a delisting after the window was applied: cash %.2f, count %d",
			p.BuyingPower, p.Metrics.DelistedHoldings)
	}
}

// The dead half of a split series carries its base company's death, so a
// portfolio holding DD.1 must be haircut by DD's delisting.
func TestGenerationSymbolInheritsItsBaseDelisting(t *testing.T) {
	windowed := map[string][]data.AssetData{"X.1": flatBars("2020-01-01", 5, 20)}
	p := holding("X.1", 10, 20, -0.50, map[string]delistingEvent{
		"X": {Date: delistDay("2020-01-03"), Reason: data.ReasonBankruptcy},
	})
	p.applyDelistingReturns(windowed, 5)

	if p.Metrics.DelistedHoldings != 1 {
		t.Errorf("X.1 was not matched to X's delisting: count %d",
			p.Metrics.DelistedHoldings)
	}
}

// The liquidation must be visible in the blotter, not a silent mark-down —
// otherwise the attribution panel reports where the money went and is wrong.
func TestDelistingLiquidationReachesTheBlotter(t *testing.T) {
	windowed := map[string][]data.AssetData{"X": flatBars("2020-01-01", 5, 20)}
	p := holding("X", 10, 20, -0.50, map[string]delistingEvent{
		"X": {Date: delistDay("2020-01-03"), Reason: data.ReasonBankruptcy},
	})
	p.applyDelistingReturns(windowed, 5)

	if len(p.Trades) != 1 {
		t.Fatalf("got %d trades, want 1 — the liquidation must be a real sale",
			len(p.Trades))
	}
	tr := p.Trades[0]
	if tr.Ticker != "X" || tr.Price != 10 {
		t.Errorf("trade = %s at %.2f, want X at 10.00 (20.00 less 50%%)",
			tr.Ticker, tr.Price)
	}
	// Bought at 20, forced out at 10, ten shares: a realised loss of 100.
	if tr.RealizedPnL != -100 {
		t.Errorf("realised %.2f, want -100.00", tr.RealizedPnL)
	}
	if pos, ok := p.Positions["X"]; ok && pos.Amount > 0 {
		t.Errorf("position survived the liquidation: %.2f shares", pos.Amount)
	}
}

func TestDelistingReturnRejectsAnImpossibleValue(t *testing.T) {
	for _, v := range []float64{0.10, -1.5} {
		pc := PortfolioConfig{
			Name: "bad", BuyingPower: 1000,
			StartTime: "2020-01-01", EndTime: "2020-12-31",
			Tickers: []string{"AAPL"}, Strategy: "buyandhold",
			DelistingReturn: v,
		}
		if _, err := pc.ToPortfolio(); err == nil {
			t.Errorf("DelistingReturn %.2f was accepted; it must be in [-1, 0]", v)
		}
	}
}
