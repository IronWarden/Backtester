package backtest

// The trade blotter, and what it says about where the money came from.
//
// Every fill already went through TransactionLogger and was then discarded as
// text, so the app could tell you a strategy returned 14% and nothing about
// how. That hides the question most likely to change a decision: was the edge
// broad, or was it one lucky position? A Sharpe cannot tell those apart and a
// per-ticker P&L column can, at a glance.
//
// Two accounting choices, both made to agree with the engine rather than with
// convention:
//
//   - **Realised P&L is average-cost**, because Position.AveragePrice is how
//     the engine already values a holding. A FIFO lot-matching P&L would be
//     defensible and would disagree with every other number in the app, which
//     is worse than being unconventional.
//   - **Holding periods are FIFO**, because "how long was this held" has no
//     average-cost answer — shares bought on different days are
//     indistinguishable once averaged. So lots are tracked for dates only, and
//     that split is deliberate: money is average-cost, time is FIFO.
//
// Fees are charged to the trade that paid them, so a strategy whose gross edge
// is real but whose net edge is not shows up as a positive gross and a
// negative realised P&L on the same row.

import (
	"sort"
	"time"
)

// Trade is one fill, recorded where the transaction log line is written so the
// two can never disagree.
type Trade struct {
	Ticker string    `json:"ticker"`
	Date   time.Time `json:"date"`
	// Side is "buy" or "sell".
	Side   string  `json:"side"`
	Shares float64 `json:"shares"`
	// Price is the FILL price — after slippage, not the quoted price. Notional
	// is Shares*Price, and Fee is the commission on top of (buy) or out of
	// (sell) that notional.
	Price    float64 `json:"price"`
	Notional float64 `json:"notional"`
	Fee      float64 `json:"fee"`
	// RealizedPnL is set on sells only: shares * (fill - average cost), net of
	// this trade's fee. Zero on buys, which realise nothing.
	RealizedPnL float64 `json:"realizedPnl"`
	// HoldingDays is the FIFO-weighted number of calendar days the sold shares
	// were held. Zero on buys.
	HoldingDays float64 `json:"holdingDays"`
}

// TickerPnL is one row of the attribution table — usually the most
// decision-changing view in the app, because it answers "which position
// actually made this".
type TickerPnL struct {
	Ticker      string  `json:"ticker"`
	Realized    float64 `json:"realized"`
	Unrealized  float64 `json:"unrealized"`
	Fees        float64 `json:"fees"`
	Trades      int     `json:"trades"`
	SharesHeld  float64 `json:"sharesHeld"`
	AverageCost float64 `json:"averageCost"`
}

// Total is realised plus unrealised: what this ticker contributed overall.
func (t TickerPnL) Total() float64 { return t.Realized + t.Unrealized }

// TradeStats summarises a run's trading. Every field is derived from the
// blotter, so a run that placed no trades reports zeros rather than absences —
// and Trades == 0 is itself the most important signal a result can carry, since
// a strategy that never traded is not a strategy.
type TradeStats struct {
	Trades int `json:"trades"`
	Buys   int `json:"buys"`
	Sells  int `json:"sells"`
	// RoundTrips counts sells, each of which realises a gain or a loss.
	RoundTrips int     `json:"roundTrips"`
	Wins       int     `json:"wins"`
	Losses     int     `json:"losses"`
	WinRate    float64 `json:"winRate"`
	AvgWin     float64 `json:"avgWin"`
	AvgLoss    float64 `json:"avgLoss"`
	// ProfitFactor is gross wins over gross losses. Above 1 means the winners
	// paid for the losers; it is 0 when nothing was realised and +Inf is
	// avoided by reporting 0 losses explicitly instead.
	ProfitFactor float64 `json:"profitFactor"`
	// TotalRealized and TotalFees are in dollars. Fees are worth their own line
	// because a strategy can be gross-profitable and net-negative, and that is
	// invisible in any return figure.
	TotalRealized  float64 `json:"totalRealized"`
	TotalFees      float64 `json:"totalFees"`
	AvgHoldingDays float64 `json:"avgHoldingDays"`
	// PerTicker is sorted by total contribution, largest first, so the name
	// that made the money is the first row.
	PerTicker []TickerPnL `json:"perTicker"`
}

// lot is an open FIFO parcel of shares, kept only to answer "how long was this
// held". See the accounting note at the top of the file.
type lot struct {
	shares float64
	bought time.Time
}

// record appends a fill to the blotter. Called from Buy and Sell at the point
// the transaction log line is written, with the same numbers, so the blotter and
// the log can never tell different stories.
func (p *Portfolio) record(t Trade) {
	p.Trades = append(p.Trades, t)
}

// fifoHoldingDays consumes `shares` from the front of a ticker's open lots and
// returns the share-weighted holding period in days. It mutates the lot list,
// which is why the caller owns it.
//
// A sell with no recorded lots — possible only if a position existed before the
// blotter did — reports zero rather than guessing.
func fifoHoldingDays(lots []lot, shares float64, sold time.Time) ([]lot, float64) {
	if shares <= 0 {
		return lots, 0
	}
	remaining := shares
	var weighted float64
	for len(lots) > 0 && remaining > 1e-12 {
		take := lots[0].shares
		if take > remaining {
			take = remaining
		}
		weighted += take * sold.Sub(lots[0].bought).Hours() / 24
		lots[0].shares -= take
		remaining -= take
		if lots[0].shares <= 1e-12 {
			lots = lots[1:]
		}
	}
	filled := shares - remaining
	if filled <= 0 {
		return lots, 0
	}
	return lots, weighted / filled
}

// SummarizeTrades turns a blotter into the stats above. Pure: it reads the
// trades and the final positions and computes nothing from the market, so it can
// be tested without a run.
//
// positions supplies the unrealised side. A nil map means "closed book" and only
// realised P&L is reported.
func SummarizeTrades(trades []Trade, positions map[string]*Position) TradeStats {
	stats := TradeStats{Trades: len(trades)}
	byTicker := make(map[string]*TickerPnL)
	get := func(ticker string) *TickerPnL {
		row, ok := byTicker[ticker]
		if !ok {
			row = &TickerPnL{Ticker: ticker}
			byTicker[ticker] = row
		}
		return row
	}

	var grossWins, grossLosses, holdingWeighted, holdingShares float64
	for _, t := range trades {
		row := get(t.Ticker)
		row.Trades++
		row.Fees += t.Fee
		stats.TotalFees += t.Fee

		switch t.Side {
		case "buy":
			stats.Buys++
		case "sell":
			stats.Sells++
			stats.RoundTrips++
			row.Realized += t.RealizedPnL
			stats.TotalRealized += t.RealizedPnL
			if t.RealizedPnL > 0 {
				stats.Wins++
				grossWins += t.RealizedPnL
			} else if t.RealizedPnL < 0 {
				stats.Losses++
				grossLosses += -t.RealizedPnL
			}
			// Share-weighted, so a large exit counts for more than a token
			// one — an unweighted average is dominated by whichever trades
			// happened to be small.
			holdingWeighted += t.HoldingDays * t.Shares
			holdingShares += t.Shares
		}
	}

	if stats.RoundTrips > 0 {
		stats.WinRate = float64(stats.Wins) / float64(stats.RoundTrips)
	}
	if stats.Wins > 0 {
		stats.AvgWin = grossWins / float64(stats.Wins)
	}
	if stats.Losses > 0 {
		stats.AvgLoss = grossLosses / float64(stats.Losses)
		stats.ProfitFactor = grossWins / grossLosses
	}
	if holdingShares > 0 {
		stats.AvgHoldingDays = holdingWeighted / holdingShares
	}

	for ticker, pos := range positions {
		if pos == nil || pos.Amount == 0 {
			continue
		}
		row := get(ticker)
		row.SharesHeld = pos.Amount
		row.AverageCost = pos.AveragePrice
		// CurrentPrice is maintained by GetPortfolioValue during the run. A
		// zero means the position was never marked, and an unrealised figure
		// computed against zero would read as a total loss.
		if pos.CurrentPrice > 0 {
			row.Unrealized = pos.Amount * (pos.CurrentPrice - pos.AveragePrice)
		}
	}

	stats.PerTicker = make([]TickerPnL, 0, len(byTicker))
	for _, row := range byTicker {
		stats.PerTicker = append(stats.PerTicker, *row)
	}
	sort.SliceStable(stats.PerTicker, func(i, j int) bool {
		a, b := stats.PerTicker[i].Total(), stats.PerTicker[j].Total()
		if a != b {
			return a > b
		}
		return stats.PerTicker[i].Ticker < stats.PerTicker[j].Ticker
	})
	return stats
}
