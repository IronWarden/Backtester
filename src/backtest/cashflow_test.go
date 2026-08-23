package backtest

import (
	"math"
	"strings"
	"testing"
)

// A portfolio with a monthly schedule, over a FLAT price series.
//
// Flat is the whole trick: with no market movement, every non-zero return the
// engine reports has to have come from the cashflow, so the assertions below
// cannot pass by accident.
func withSchedule(t *testing.T, amount float64, freq string, days int) *Portfolio {
	t.Helper()
	closes := make([]float64, days)
	for i := range closes {
		closes[i] = 100
	}
	pc := PortfolioConfig{
		Name:        "flows",
		BuyingPower: 100000,
		StartTime:   exactEpoch.Format("2006-01-02"),
		EndTime:     exactEpoch.AddDate(0, 0, days).Format("2006-01-02"),
		Tickers:     []string{"AAA"},
		Strategy:    "buyAndHold:equalWeights",
		Cashflow:    CashflowConfig{Amount: amount, Frequency: freq},
	}
	p, err := pc.ToPortfolio()
	if err != nil {
		t.Fatalf("ToPortfolio: %v", err)
	}
	runOne(p, histFrom(map[string][]float64{"AAA": closes}), map[int64]float64{})
	return p
}

// Hazard 1. Deposit into a flat book and the engine must report no return.
// Without the neutralisation each deposit posts as a one-day gain, and every
// ratio built on the daily series inherits it.
func TestADepositIsNotAReturn(t *testing.T) {
	p := withSchedule(t, 5000, "monthly", 400)

	if p.Metrics.NetContributions <= 0 {
		t.Fatalf("no contributions recorded (%v) — the schedule never fired",
			p.Metrics.NetContributions)
	}
	for i, dr := range p.DailyReturns {
		if math.Abs(dr.Return) > 1e-9 {
			t.Fatalf("day %d (%s) returned %.6f on a FLAT price series — the "+
				"deposit is being counted as performance",
				i, dr.Date.Format("2006-01-02"), dr.Return)
		}
	}
	if math.Abs(p.Metrics.AnnualReturn) > 1e-6 {
		t.Errorf("annual return %.6f on a flat series", p.Metrics.AnnualReturn)
	}
	if math.Abs(p.Metrics.SharpeRatio) > 1e-6 {
		t.Errorf("Sharpe %.6f on a flat series", p.Metrics.SharpeRatio)
	}
}

// A withdrawal is the same rule with the sign flipped: it must not look like a
// loss, and on a flat series it must not look like a drawdown either.
func TestAWithdrawalIsNotALoss(t *testing.T) {
	p := withSchedule(t, -2000, "monthly", 400)

	if p.Metrics.NetContributions >= 0 {
		t.Fatalf("withdrawals not recorded as negative: %v", p.Metrics.NetContributions)
	}
	for i, dr := range p.DailyReturns {
		if math.Abs(dr.Return) > 1e-9 {
			t.Fatalf("day %d returned %.6f on a flat series with withdrawals",
				i, dr.Return)
		}
	}
	if p.Metrics.MaxDrawdown > 1e-6 {
		t.Errorf("max drawdown %.4f on a flat price series — the withdrawals "+
			"are being read as a decline", p.Metrics.MaxDrawdown)
	}
}

// The schedule must actually pay on the stated cadence, and never on day one:
// the opening capital is already in the book.
func TestScheduleCadenceAndFirstPayment(t *testing.T) {
	for _, tc := range []struct {
		freq string
		want float64 // expected number of payments over ~13 months
	}{
		{"monthly", 13},
		{"quarterly", 4},
		{"annually", 1},
	} {
		t.Run(tc.freq, func(t *testing.T) {
			p := withSchedule(t, 1000, tc.freq, 400)
			got := p.Metrics.NetContributions / 1000
			if got != tc.want {
				t.Errorf("%s over 400 days made %v payments, want %v",
					tc.freq, got, tc.want)
			}
		})
	}

	// Nothing on the first simulated day.
	p := withSchedule(t, 1000, "monthly", 40)
	if p.PortfolioCloseValues[0] > 100000 {
		t.Errorf("a flow landed on day one (%v > 100000); the opening balance "+
			"is already in the book, so the first period must elapse first",
			p.PortfolioCloseValues[0])
	}
}

// Hazard 5. A withdrawal larger than the cash balance sells holdings to fund
// itself, and one that cannot be funded at all is COUNTED rather than skipped.
func TestWithdrawalsSellHoldingsAndAreCountedWhenTheyFail(t *testing.T) {
	// Equal-weight buy-and-hold puts essentially everything into the position,
	// so every withdrawal has to raise cash by selling.
	p := withSchedule(t, -20000, "monthly", 400)
	if p.Metrics.WithdrawalsMissed == 0 {
		t.Error("draining 20k a month from 100k for 13 months never reported a " +
			"missed withdrawal — a plan that runs out of money must say so")
	}
	final := p.PortfolioCloseValues[len(p.PortfolioCloseValues)-1]
	if final < 0 {
		t.Errorf("final value %v is negative; a discretionary withdrawal must "+
			"not overdraw the account", final)
	}
}

// The guarantee that protects every config written before this existed.
func TestNoScheduleIsBitIdentical(t *testing.T) {
	closes := make([]float64, 300)
	for i := range closes {
		closes[i] = 100 * math.Pow(1.001, float64(i))
	}
	run := func(cf CashflowConfig) *Portfolio {
		pc := PortfolioConfig{
			Name: "x", BuyingPower: 100000,
			StartTime: exactEpoch.Format("2006-01-02"),
			EndTime:   exactEpoch.AddDate(0, 0, 300).Format("2006-01-02"),
			Tickers:   []string{"AAA"}, Strategy: "buyAndHold:equalWeights",
			Cashflow: cf,
		}
		p, err := pc.ToPortfolio()
		if err != nil {
			t.Fatalf("ToPortfolio: %v", err)
		}
		runOne(p, histFrom(map[string][]float64{"AAA": closes}), map[int64]float64{})
		return p
	}
	a := run(CashflowConfig{})
	b := run(CashflowConfig{})
	if a.Metrics != b.Metrics {
		t.Fatal("two identical runs disagree")
	}
	if a.Metrics.NetContributions != 0 || a.Metrics.WithdrawalsMissed != 0 {
		t.Errorf("an empty Cashflow block moved something: %+v", a.Metrics)
	}
}

// Config errors are refused before the run rather than producing a schedule
// that quietly does nothing.
func TestCashflowConfigIsValidated(t *testing.T) {
	base := func() PortfolioConfig {
		return PortfolioConfig{
			Name: "x", BuyingPower: 1000,
			StartTime: "2021-01-04", EndTime: "2021-12-31",
			Tickers: []string{"AAA"}, Strategy: "buyAndHold:equalWeights",
		}
	}
	for _, tc := range []struct {
		name string
		cf   CashflowConfig
		want string
	}{
		{"unknown frequency", CashflowConfig{Amount: 1, Frequency: "fortnightly"}, "Frequency"},
		{"no amount", CashflowConfig{Frequency: "monthly"}, "Amount is 0"},
		{"unparseable start", CashflowConfig{Amount: 1, Frequency: "monthly", Start: "soon"}, "Start"},
		{"start outside window", CashflowConfig{Amount: 1, Frequency: "monthly", Start: "2030-01-01"}, "outside the window"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pc := base()
			pc.Cashflow = tc.cf
			_, err := pc.ToPortfolio()
			if err == nil {
				t.Fatalf("accepted %+v", tc.cf)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// Metrics counted DURING the simulation must survive the metrics rebuild.
//
// This is a regression test for a bug that predates cashflows and hid for as
// long as it existed: GetBacktestingData assigned a fresh Metrics struct, so
// every field accumulated in the daily loop was zeroed. The existing tests for
// those fields set them up and assert them without ever calling
// GetBacktestingData, which is precisely why nothing caught it — every real
// run reported 0 borrow paid, 0 margin calls and 0 delisted holdings while the
// docs promised otherwise.
func TestAccumulatedMetricsSurviveTheRebuild(t *testing.T) {
	closes := make([]float64, 60)
	for i := range closes {
		closes[i] = 100
	}
	hist := histFrom(map[string][]float64{"AAA": closes})

	p, err := InitializePortfolio(10000, exactEpoch, exactEpoch.AddDate(0, 0, 60),
		"accum", []string{"AAA"}, "buyAndHold:equalWeights", nil)
	if err != nil {
		t.Fatal(err)
	}
	p.AllowShort = true
	p.Costs.BorrowBpsAnnual = 500
	p.Sell("AAA", 50, 100, exactEpoch)
	p.chargeBorrow()

	// Set every accumulated counter to something recognisable.
	p.Metrics.MarginCalls = 3
	p.Metrics.DelistedHoldings = 2
	p.Metrics.UnclassifiedDelistings = 1
	p.Metrics.NetContributions = 4321
	p.Metrics.WithdrawalsMissed = 7
	borrow := p.Metrics.BorrowPaid
	if borrow == 0 {
		t.Fatal("test setup charged no borrow")
	}

	p.GetBacktestingData(map[int64]float64{}, hist, 60)

	for _, c := range []struct {
		name string
		got  float64
		want float64
	}{
		{"BorrowPaid", p.Metrics.BorrowPaid, borrow},
		{"MarginCalls", float64(p.Metrics.MarginCalls), 3},
		{"DelistedHoldings", float64(p.Metrics.DelistedHoldings), 2},
		{"UnclassifiedDelistings", float64(p.Metrics.UnclassifiedDelistings), 1},
		{"NetContributions", p.Metrics.NetContributions, 4321},
		{"WithdrawalsMissed", float64(p.Metrics.WithdrawalsMissed), 7},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v after the rebuild, want %v — it was counted "+
				"during the run and cannot be recomputed", c.name, c.got, c.want)
		}
	}
}

// Hazard 4. Without this the default strategy never spends a contribution and
// the whole feature looks broken to the people who asked for it.
func TestBuyAndHoldInvestsContributions(t *testing.T) {
	// Flat prices, so cash and stock are worth the same and any difference in
	// the final book value is purely about whether the money was deployed.
	closes := make([]float64, 200)
	for i := range closes {
		closes[i] = 100
	}
	hist := histFrom(map[string][]float64{"AAA": closes, "BBB": closes})

	pc := PortfolioConfig{
		Name: "contrib", BuyingPower: 10000,
		StartTime: exactEpoch.Format("2006-01-02"),
		EndTime:   exactEpoch.AddDate(0, 0, 200).Format("2006-01-02"),
		Tickers:   []string{"AAA", "BBB"}, Strategy: "buyAndHold:equalWeights",
		Cashflow: CashflowConfig{Amount: 1000, Frequency: "monthly"},
	}
	p, err := pc.ToPortfolio()
	if err != nil {
		t.Fatal(err)
	}
	runOne(p, hist, map[int64]float64{})

	if p.Metrics.NetContributions <= 0 {
		t.Fatal("no contributions were made")
	}
	// Nearly all of it should be invested: what is left in cash is the
	// rounding each leg leaves behind, not six months of deposits.
	invested := 0.0
	for _, ticker := range p.Tickers {
		if pos, ok := p.Positions[ticker]; ok {
			invested += pos.Amount * 100
		}
	}
	total := invested + p.BuyingPower
	if invested/total < 0.99 {
		t.Errorf("only %.1f%% of the book is invested — contributions are "+
			"piling up as cash (%.2f idle of %.2f)",
			100*invested/total, p.BuyingPower, total)
	}
	// Both legs must grow, not just the first: equal weights means equal
	// weights for new money too.
	a, b := p.Positions["AAA"].Amount, p.Positions["BBB"].Amount
	if math.Abs(a-b)/a > 0.01 {
		t.Errorf("legs diverged: AAA %.2f vs BBB %.2f — the contribution is "+
			"not being split at the configured weights", a, b)
	}
}

// And the guarantee that protects everyone else: without a schedule,
// buy-and-hold behaves exactly as it always did.
func TestBuyAndHoldUnchangedWithoutASchedule(t *testing.T) {
	closes := make([]float64, 200)
	for i := range closes {
		closes[i] = 100 * math.Pow(1.001, float64(i))
	}
	hist := histFrom(map[string][]float64{"AAA": closes, "BBB": closes})

	p, err := InitializePortfolio(10000, exactEpoch, exactEpoch.AddDate(0, 0, 200),
		"plain", []string{"AAA", "BBB"}, "buyAndHold:equalWeights", nil)
	if err != nil {
		t.Fatal(err)
	}
	runOne(p, hist, map[int64]float64{})

	// One buy per ticker on day zero and nothing after.
	if n := len(p.Trades); n != 2 {
		t.Errorf("%d trades without a cashflow schedule, want 2 — buy-and-hold "+
			"must still buy once and hold", n)
	}
}
