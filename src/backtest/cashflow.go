package backtest

import (
	"time"

	"my-backtester/src/data"
)

// Scheduled deposits and withdrawals.
//
// Contributing is how people actually invest, and a decumulation backtest is
// unanswerable without withdrawals. Both are applied at the top of the day,
// before the strategy acts, so a strategy can see and invest the money on the
// day it arrives rather than a day late.
//
// **A contribution is not a return.** The day's return is computed against the
// book's value at the previous close, so money added between those two points
// would post as performance the market never delivered — deposit £500 into a
// £10,000 book and the day shows a 5% gain. Every ratio the engine reports is
// built on that daily series, so the error would propagate into Sharpe,
// Sortino, CAGR, the rolling Sharpe and the significance nulls alike. The flow
// is therefore returned to the caller, which raises the day's baseline by the
// same amount before measuring.
//
// Note the deliberate contrast with chargeBorrow, which sits on the adjacent
// line of the same loop and needs the OPPOSITE treatment: borrow cost is a
// real cost of holding the position and *should* depress the day's return.
// Do not "fix" one to match the other.

// applyCashflow applies whatever the schedule owes on this date and returns
// the signed amount moved, or 0 when nothing is due.
//
// A withdrawal larger than the cash balance sells holdings pro-rata to fund
// the shortfall. That is deliberate and different from a margin call, which
// CLAUDE.md notes may overdraw precisely because it is not discretionary: a
// scheduled withdrawal IS discretionary, and the whole point of a retirement
// backtest is drawing the portfolio down. What cannot be funded even after
// liquidating everything is counted, not silently skipped — a plan whose
// withdrawals could not be met is a failed plan, and that is the headline.
func (p *Portfolio) applyCashflow(
	date time.Time, hist map[string][]data.AssetData, day int,
) float64 {
	if p.cashflowStep == 0 || date.Before(p.nextCashflow) {
		return 0
	}
	// Advance past every period the schedule has passed, so a gap in the
	// trading calendar (a long holiday, a thin series) cannot skip a payment
	// or double one up.
	for !date.Before(p.nextCashflow) {
		p.nextCashflow = p.nextCashflow.AddDate(0, p.cashflowStep, 0)
	}

	amount := p.Cashflow.Amount
	if amount > 0 {
		p.Deposit(amount)
		p.Metrics.NetContributions += amount
		return amount
	}

	want := -amount // positive size of the withdrawal
	if short := want - p.BuyingPower; short > 0 {
		p.raiseCash(short, hist, day, date)
	}
	got := want
	if p.BuyingPower < want {
		// Everything saleable is gone and it still is not enough.
		got = p.BuyingPower
		if got < 0 {
			got = 0
		}
		p.Metrics.WithdrawalsMissed++
		diagf("warn", p.Pname,
			"%s: withdrawal of %.2f could not be funded — %.2f taken and the "+
				"portfolio is exhausted. A plan whose withdrawals cannot be met "+
				"has failed; treat later figures as describing a spent account.",
			date.Format("2006-01-02"), want, got)
	}
	if got <= 0 {
		return 0
	}
	p.Withdraw(got)
	p.Metrics.NetContributions -= got
	return -got
}

// raiseCash sells holdings pro-rata by market value to raise `need` in cash.
//
// Pro-rata rather than largest-first so the act of funding a withdrawal does
// not itself rebalance the book: taking it all from one name would change the
// portfolio's composition, and the resulting performance would be partly an
// artefact of how the withdrawal was funded rather than of the strategy.
//
// Costs apply, because they do in life: the sale goes through Sell, so
// commission and slippage are charged and the trade appears in the blotter.
func (p *Portfolio) raiseCash(
	need float64, hist map[string][]data.AssetData, day int, date time.Time,
) {
	type holding struct {
		ticker string
		price  float64
		value  float64
	}
	var holdings []holding
	total := 0.0
	for _, ticker := range p.Tickers {
		bars := hist[ticker]
		if day >= len(bars) {
			continue
		}
		pos, ok := p.Positions[ticker]
		// Shorts are not a source of cash: buying them back consumes it.
		if !ok || pos.Amount <= 0 {
			continue
		}
		price := bars[day].Close
		if price <= 0 {
			continue
		}
		v := pos.Amount * price
		holdings = append(holdings, holding{ticker, price, v})
		total += v
	}
	if total <= 0 {
		return
	}
	// Oversell slightly rather than under: a sale that raises a penny less
	// than needed leaves the withdrawal short for no reason a user could act
	// on. Capped at the whole position.
	scale := need / total
	if scale > 1 {
		scale = 1
	}
	for _, h := range holdings {
		shares := p.Positions[h.ticker].Amount * scale
		if shares <= 0 {
			continue
		}
		p.Sell(h.ticker, shares, h.price, date)
	}
}
