package backtest

import (
	"fmt"
	"io"
	"log"
	"my-backtester/src/data"
	"time"
)

// TransactionLogger receives BUY/SELL records. Defaults to a discard sink
// so callers that don't enable transaction logging (e.g. the UI) don't
// nil-panic; the CLI's --debug flag replaces it with a file-backed logger.
var TransactionLogger = log.New(io.Discard, "", 0)

type DailyReturn struct {
	Date   time.Time
	Return float64
	// Flow is the signed amount a cashflow schedule moved at the top of this
	// day, 0 when none did. Return already excludes it; it is kept so the
	// benchmark's dollar curve can be fed the same contributions.
	Flow float64
}

type Portfolio struct {
	Pname              string // Portfolio name for tracking purposes
	BuyingPower        float64
	InitialBuyingPower float64
	Positions          map[string]*Position
	// Baseline is filled in after the run: what an equal-weight buy-and-hold of
	// the same tickers would have done. See baseline.go.
	Baseline BaselineStats
	// Trades is the blotter: every fill, in order. openLots tracks the same
	// shares as FIFO parcels, for holding periods only — see blotter.go on why
	// money is average-cost and time is FIFO.
	Trades   []Trade
	openLots map[string][]lot
	// TradeStats is the blotter's summary, filled in after the run.
	TradeStats TradeStats
	// Exposure is the invested fraction of the book per day, 1:1 with
	// DailyReturns.
	Exposure []float64
	// Regimes slices the run by the market environment its universe was in.
	Regimes RegimeBreakdown
	// plots is what the strategy recorded with plot(), for the results panel
	// to draw. A per-run RESULT like Trades and DailyReturns, so Clone leaves
	// it empty; unexported because nothing outside the engine writes it.
	plots plotRecorder
	// Robustness is what survived re-running under more friction and from
	// shifted start dates. Empty unless RobustnessChecks is on.
	Robustness Robustness
	// Significance is the answer to "is this better than chance", filled in
	// after the run. See significance.go.
	Significance         Significance
	DailyReturns         []DailyReturn
	PortfolioCloseValues []float64
	Metrics              Metrics
	Tickers              []string
	StrategySpec         string
	StrategyParams       map[string]any
	Strategy             Strategy
	StartTime            time.Time
	EndTime              time.Time
	// Cashflow is the deposit/withdrawal schedule, zero when there is none.
	Cashflow CashflowConfig
	// cashflowStep is the schedule's period in months, and nextCashflow the
	// date the next flow is due. Both are derived once by ToPortfolio so the
	// daily loop does no date arithmetic beyond a comparison.
	cashflowStep int
	nextCashflow time.Time
	// cashflowAnchor is the resolved Start, kept so a clone can recompute its
	// own first due date. The baseline and the robustness runs are built AFTER
	// the parent has finished, by which point the parent's nextCashflow has
	// walked past the end of the window — copying it would leave the clone
	// never paying anything.
	cashflowAnchor time.Time
	// pendingCashflow is the signed amount the schedule moved TODAY, 0 on
	// every other day. It exists so a strategy can put a contribution to work
	// on the day it arrives; see Portfolio.PendingCashflow.
	pendingCashflow float64

	// CPI is the monthly deflator covering this run, loaded once by the runner
	// and shared read-only across the worker pool. Nil when the database has
	// no economic_indicators table, which simply leaves the real return
	// unreported — it is not an error and nothing else changes.
	CPI []data.CPIPoint
	// inflationFactor is CPI(end)/CPI(start) for the window actually
	// simulated, cached once the metrics are computed so the benchmark can be
	// deflated by the same number rather than re-deriving it and risking a
	// different answer.
	inflationFactor float64
	// Costs is the trading-cost model applied by Buy and Sell. The zero
	// value charges nothing. Clone must carry this across — the runner
	// simulates clones, so a cost model left behind here would be silently
	// absent from every real run.
	Costs CostConfig
	// Benchmark is an optional ticker this portfolio is scored against. It
	// is deliberately kept out of Tickers: that slice drives allocation, the
	// trading-day intersection, and valuation, so adding the benchmark to it
	// would change what the portfolio actually does. Like Costs it is
	// configuration, so Clone must carry it across.
	Benchmark string
	// AllowShort permits a position to go negative: a sell past the holding
	// opens a short instead of being refused. Off by default, and while it is
	// off every arithmetic path below is exactly what it was before shorting
	// existed. Configuration, so Clone must carry it across.
	AllowShort bool
	// forcedCover is set only while enforceMaintenanceMargin is liquidating.
	// A margin call is not a discretionary order: the broker buys the shares
	// back whether or not the account can pay, and the account goes into
	// debit. Without this, a book whose equity had gone NEGATIVE could not be
	// covered at all — Buy would refuse to overdraw and the breach would
	// simply persist, which is the one outcome a maintenance rule must not
	// produce.
	forcedCover bool
	// Margin is what a short must be backed by. Configuration, so Clone
	// carries it; ignored unless AllowShort is on.
	Margin MarginConfig
	// DelistingReturn and DelistingEvents drive the haircut applied to a
	// holding whose company failed — see delisting.go. Like Costs they are
	// configuration, so Clone must carry them across.
	DelistingReturn float64
	DelistingEvents map[string]delistingEvent
	// tradedNotional accumulates the gross value of every filled order and
	// feeds the Turnover metric. Unlike Costs this is per-run state, so
	// Clone deliberately leaves it at zero, exactly as it does for
	// DailyReturns and PortfolioCloseValues.
	tradedNotional float64
	// BenchmarkCurve is the Benchmark's value over the same trading days,
	// compounded from the portfolio's own starting capital so the two can be
	// plotted on one axis. 1:1 with DailyReturns and PortfolioCloseValues.
	// BenchmarkStats is what the benchmark did on its own terms. Both are
	// per-run results like DailyReturns, so Clone leaves them empty; both
	// stay empty when no Benchmark is set or its data does not cover the
	// window.
	BenchmarkCurve []float64
	BenchmarkStats BenchmarkStats
	// Fundamentals are the metric names the config declared; FundamentalData
	// is what was fetched for them, dated by the day each figure became
	// PUBLIC rather than the period it covers. Both are configuration in the
	// Clone sense: the fetch happens once per run and every clone shares the
	// same immutable slice rather than re-querying.
	//
	// Nil when the config declares nothing, which is every pre-T33 config.
	Fundamentals    []string
	FundamentalData []data.FundamentalPoint
	// FundamentalSource is "" for the `financials` table and data.SourceSEC
	// for `sec_financials`. Configuration, so Clone carries it.
	FundamentalSource string
	// InSampleEnd is the last day of the in-sample segment when the config
	// asks for a split, and the zero time when it does not. Like Costs and
	// Benchmark it is configuration, so Clone must carry it across; Splits is
	// the per-run result and Clone leaves it empty.
	InSampleEnd time.Time
	Splits      []SegmentStats
	// Trials is how many parameter sets the config block this portfolio came
	// from expanded to, and TrialGroup names that block. Together they let
	// the runner discount a swept winner by the size of the search that found
	// it — the best of 200 combinations is not the same thing as a good
	// strategy. Both are configuration, so Clone carries them; a block with
	// no Sweep is one trial in a group of one.
	Trials     int
	TrialGroup string
}

func InitializePortfolio(
	buyingPower float64,
	startTime, endTime time.Time,
	pname string,
	tickers []string,
	strategySpec string,
	strategyParams map[string]any,
) (*Portfolio, error) {
	// A window that ends before it starts makes days negative, and
	// make([]T, 0, negative) panics with "makeslice: cap out of range" — so
	// swapping two dates, or typing 2019 for 2029, used to crash the run
	// rather than report a config mistake. The CLI died with a stack trace;
	// the UI caught the panic and showed the user the stack instead of the
	// typo.
	if endTime.Before(startTime) {
		return nil, fmt.Errorf(
			"portfolio %q: EndDate %s is before StartDate %s",
			pname, endTime.Format("2006-01-02"), startTime.Format("2006-01-02"))
	}
	// Negative capital is not a portfolio. Zero is left alone: Deposit exists,
	// so starting empty and funding later is a shape the engine supports.
	if buyingPower < 0 {
		return nil, fmt.Errorf(
			"portfolio %q: BuyingPower %.2f is negative", pname, buyingPower)
	}
	// A repeated ticker corrupts the run rather than doubling a weight.
	// alignToWindow keys its output by ticker but appends once per INDEX, so
	// a duplicate appends the same bar twice per day and hands back a series
	// of 2n bars alongside a length of n — which panics in the day loop. Even
	// without that, GetPortfolioValue iterates Tickers and would count the one
	// position twice, inflating the equity curve.
	//
	// Rejected rather than deduplicated: weighting a holding up is what
	// [portfolio.Params.weights] and buy_and_hold.lua are for, and
	// silently dropping a line from a config is its own kind of surprise.
	seen := make(map[string]bool, len(tickers))
	for _, t := range tickers {
		if seen[t] {
			return nil, fmt.Errorf(
				"portfolio %q: ticker %q is listed more than once", pname, t)
		}
		seen[t] = true
	}

	strat, err := NewStrategy(strategySpec, strategyParams)
	if err != nil {
		return nil, err
	}
	days := int(endTime.Sub(startTime).Hours() / 24)

	return &Portfolio{
		Pname:                pname,
		BuyingPower:          buyingPower,
		InitialBuyingPower:   buyingPower,
		Positions:            make(map[string]*Position),
		DailyReturns:         make([]DailyReturn, 0, days),
		PortfolioCloseValues: make([]float64, 0, days),
		StartTime:            startTime,
		EndTime:              endTime,
		Tickers:              tickers,
		StrategySpec:         strategySpec,
		StrategyParams:       strategyParams,
		Strategy:             strat,
	}, nil
}

// Clone returns a fresh portfolio with reset state and a new Strategy
// instance built from StrategySpec. Used by the runner so each simulation
// pass gets independent state and workers don't race on shared portfolios.
func (p *Portfolio) Clone() (*Portfolio, error) {
	strat, err := NewStrategy(p.StrategySpec, p.StrategyParams)
	if err != nil {
		return nil, err
	}
	days := cap(p.DailyReturns)
	return &Portfolio{
		Pname:                p.Pname,
		BuyingPower:          p.InitialBuyingPower,
		InitialBuyingPower:   p.InitialBuyingPower,
		Positions:            make(map[string]*Position),
		DailyReturns:         make([]DailyReturn, 0, days),
		PortfolioCloseValues: make([]float64, 0, days),
		StartTime:            p.StartTime,
		EndTime:              p.EndTime,
		Tickers:              p.Tickers,
		StrategySpec:         p.StrategySpec,
		StrategyParams:       p.StrategyParams,
		Strategy:             strat,
		Costs:                p.Costs,
		Benchmark:            p.Benchmark,
		Fundamentals:         p.Fundamentals,
		FundamentalData:      p.FundamentalData,
		FundamentalSource:    p.FundamentalSource,
		AllowShort:           p.AllowShort,
		Margin:               p.Margin,
		DelistingReturn:      p.DelistingReturn,
		DelistingEvents:      p.DelistingEvents,
		InSampleEnd:          p.InSampleEnd,
		Trials:               p.Trials,
		TrialGroup:           p.TrialGroup,
		// Configuration, like Costs and Margin above. Omitting these was a
		// real bug twice over: the runner clones every portfolio before
		// handing it to a worker, so a field missing here is a field that
		// works in every direct-call test and does nothing whatsoever in the
		// actual app. TestCloneHandlesEveryPortfolioField exists to stop a
		// third time.
		Cashflow:       p.Cashflow,
		cashflowStep:   p.cashflowStep,
		nextCashflow:   p.nextCashflow,
		cashflowAnchor: p.cashflowAnchor,
		CPI:            p.CPI,
	}, nil
}

type Position struct {
	Amount       float64
	AveragePrice float64
	CurrentPrice float64
}

func (p *Portfolio) FindPosition(ticker string) (*Position, bool) {
	pos, ok := p.Positions[ticker]
	return pos, ok
}

func (p *Portfolio) PrintMetrics() {
	log.Printf("Buying Power: %.2f\n", p.BuyingPower)
	if len(p.Positions) == 0 {
		log.Println("No positions")
	}
	for key, pos := range p.Positions {
		log.Printf(
			"Ticker: %s, Amount: %.2f, Average Price: %.2f, CurrentPrice: %.2f\n",
			key, pos.Amount, pos.AveragePrice, pos.CurrentPrice,
		)
		currentValue := pos.Amount*pos.CurrentPrice + p.BuyingPower
		log.Println("Amount now is", currentValue)
	}
	log.Println("=============================================")
	log.Printf("Annual Metrics: \n")
	log.Printf("Sharpe Ratio: %.2f\n", p.Metrics.SharpeRatio)
	log.Printf("Sortino Ratio: %.2f\n", p.Metrics.SortinoRatio)
	log.Printf("MaxDrawdown: %.2f\n", p.Metrics.MaxDrawdown)
	log.Printf("Annual Return: %.2f\n", p.Metrics.AnnualReturn)
	log.Printf("Standard Deviation: %.4f\n", p.Metrics.StandardDev)
	log.Println("=============================================")
}

func (p *Portfolio) Buy(
	ticker string,
	amount float64,
	initialPrice float64,
	time time.Time,
) {
	if amount <= 0.0 || initialPrice <= 0.0 {
		return
	}
	// The order executes at the slipped price, not the quoted one. With a
	// zero cost model this is a multiplication by exactly 1.0, so every
	// figure below is bit-identical to the pre-costs arithmetic.
	fillPrice := p.Costs.BuyFill(initialPrice)
	if fillPrice <= 0.0 {
		return
	}
	notional := amount * fillPrice
	cost := notional + p.Costs.Commission(notional)
	// Reject orders that genuinely overshoot available cash, but tolerate the
	// sub-cent float overshoot that arises when a strategy sizes fractional
	// shares to spend all of its cash — clamp to the exact remaining balance so
	// the order still fills and BuyingPower never drifts negative.
	//
	// Affordability is judged on the notional, not on the total: a strategy
	// that sizes itself against cash() is asking for shares it can afford, and
	// it is the commission on top that no longer fits. Rejecting those would
	// silently stop every spend-it-all strategy from trading the moment costs
	// were switched on, which would read as "costs killed the returns" rather
	// than as an error. So an order that clears this gate but cannot cover its
	// fee is clamped down to the largest size whose fee the balance does
	// cover, exactly as a broker filling against available cash would.
	//
	// Slippage is kept out of this gate deliberately. It inflates the notional
	// itself, so a spend-it-all order sized against the quoted price used to be
	// rejected here and the strategy silently stopped trading — exactly the
	// outcome the paragraph above says must not happen (T13). The fix is on the
	// sizing side: generalBuy asks BuyFill what a share really costs, so the
	// order that arrives here is one the cash can already cover. Do not loosen
	// this gate to compensate; an order that genuinely overshoots is still an
	// error and must not fill.
	if notional > p.BuyingPower*(1.0+1e-9) && !p.forcedCover {
		return
	}
	if cost > p.BuyingPower && !p.forcedCover {
		notional = p.Costs.MaxAffordableNotional(p.BuyingPower)
		if notional <= 0.0 {
			// The balance cannot even cover the flat fee.
			return
		}
		amount = notional / fillPrice
		// By construction notional + Commission(notional) == BuyingPower;
		// assigning it directly keeps that exact rather than a rounding step
		// away from it, which is what stops the balance going negative.
		cost = p.BuyingPower
	}
	pos, ok := p.FindPosition(ticker)
	// Covering: a buy against a negative position buys the shares back, and
	// the gain is what the short was opened at LESS what it cost to close —
	// the mirror of a long, which is why it cannot share the arithmetic below.
	if ok && pos.Amount < 0 {
		covering := amount
		if short := -pos.Amount; covering > short {
			covering = short
		}
		fee := cost - notional
		realized := covering*(pos.AveragePrice-fillPrice) - fee
		p.record(Trade{
			Ticker: ticker, Date: time, Side: "buy", Shares: amount,
			Price: fillPrice, Notional: notional, Fee: fee,
			RealizedPnL: realized,
		})
		pos.Amount += amount
		switch {
		case pos.Amount == 0:
			delete(p.Positions, ticker)
		case pos.Amount > 0:
			// Covered the short and went long in one order; the surplus is a
			// fresh long at this fill.
			pos.AveragePrice = fillPrice
			if p.openLots == nil {
				p.openLots = make(map[string][]lot)
			}
			p.openLots[ticker] = append(p.openLots[ticker],
				lot{shares: pos.Amount, bought: time})
		}
		p.BuyingPower -= cost
		p.tradedNotional += notional
		return
	}
	if !ok {
		// Position does not exist, create a new one
		p.Positions[ticker] = &Position{
			Amount:       amount,
			AveragePrice: fillPrice,
		}
	} else {
		// Position exists, update it
		pos.AveragePrice = (pos.AveragePrice*pos.Amount +
			fillPrice*amount) / (pos.Amount + amount)
		pos.Amount += amount
	}
	TransactionLogger.Printf(
		"BUY: %s, Amount: %.6f, Price: %.2f, Fee: %.2f, Date: %s\n",
		ticker, amount, fillPrice, cost-notional, time,
	)
	p.record(Trade{
		Ticker: ticker, Date: time, Side: "buy", Shares: amount,
		Price: fillPrice, Notional: notional, Fee: cost - notional,
	})
	if p.openLots == nil {
		p.openLots = make(map[string][]lot)
	}
	p.openLots[ticker] = append(p.openLots[ticker],
		lot{shares: amount, bought: time})
	p.BuyingPower -= cost
	p.tradedNotional += notional
}

// PendingCashflow is the signed amount a [portfolio.Cashflow] schedule moved
// at the top of today, and 0 on any day it moved nothing.
//
// A strategy that wants to put contributions to work reads this rather than
// watching the cash balance: a balance can be high for reasons that have
// nothing to do with a deposit — a sale, a dividend, a strategy that
// deliberately holds cash — and buying on that would be a different strategy
// from the one the user asked for.
func (p *Portfolio) PendingCashflow() float64 {
	return p.pendingCashflow
}

// DrawdownSeries is the value series drawdowns should be measured on.
//
// Without a cashflow schedule it is the book value itself, returned unchanged,
// so every result computed before schedules existed is bit-identical -- not
// merely equal to within rounding, which compounding a return series would not
// have been.
//
// With one, the raw book value contains the deposits and withdrawals, and
// measuring a decline against it is wrong in both directions: a withdrawal
// reads as a drawdown that never happened, and a deposit refills the peak and
// ERASES one that did. MaxDrawdown, CalmarRatio, LongestUnderwaterDays and the
// Deepest Drawdowns panel all inherit that, plausibly and invisibly.
//
// So drawdowns come instead from an index built by compounding the
// flow-neutralised daily returns. Drawdown is a ratio, so the index needs no
// particular starting level -- it only has to move the way the strategy did.
//
// The equity CURVE stays the raw book value on purpose: the chart should show
// the account actually growing when money is added. Two series, two questions
// -- what the account was worth, and how the strategy performed.
func (p *Portfolio) DrawdownSeries() []float64 {
	if p.cashflowStep == 0 {
		return p.PortfolioCloseValues
	}
	returns := make([]float64, len(p.DailyReturns))
	for i, dr := range p.DailyReturns {
		returns[i] = dr.Return
	}
	return rebasedCurve(returns, p.InitialBuyingPower)
}

// firstCashflow is the date the first flow falls due, recomputed from the
// anchor so a clone starts the schedule from the beginning rather than
// inheriting a pointer the parent has already run past.
func (p *Portfolio) firstCashflow() time.Time {
	if p.cashflowStep == 0 {
		return time.Time{}
	}
	return p.cashflowAnchor.AddDate(0, p.cashflowStep, 0)
}

func (p *Portfolio) Deposit(cash float64) {
	p.BuyingPower += cash
}

func (p *Portfolio) Withdraw(cash float64) {
	p.BuyingPower -= cash
}

func (p *Portfolio) Sell(
	ticker string,
	stockAmount float64,
	currentPrice float64,
	time time.Time,
) {
	if stockAmount <= 0 {
		return
	}
	pos, ok := p.FindPosition(ticker)
	// Long-only: a sell may not exceed the holding, and there must be one.
	// This is the whole of the pre-shorting behaviour and is what runs whenever
	// AllowShort is off.
	if !p.AllowShort && (!ok || pos.Amount < stockAmount || pos.Amount <= 0) {
		return
	}
	// Slippage moves the fill against the seller; the commission comes out of
	// the proceeds. With a zero cost model both are no-ops and the arithmetic
	// is bit-identical to the pre-costs version.
	fillPrice := p.Costs.SellFill(currentPrice)
	notional := stockAmount * fillPrice
	proceeds := notional - p.Costs.Commission(notional)
	// A sale small enough that its fee exceeds the proceeds would withdraw
	// cash. That is allowed as long as the balance covers it — but an order
	// that would overdraw the account does not fill at all, matching Buy.
	if p.BuyingPower+proceeds < 0 {
		return
	}
	TransactionLogger.Printf(
		"SELL: %s, Amount: %.2f, Price: %.2f, Fee: %.2f, Date: %s\n",
		ticker, stockAmount, fillPrice, notional-proceeds, time,
	)
	fee := notional - proceeds

	// How much of this order closes an existing long, and how much of it opens
	// or extends a short. Long-only, the second is always zero.
	held := 0.0
	if ok && pos.Amount > 0 {
		held = pos.Amount
	}
	closing := stockAmount
	if closing > held {
		closing = held
	}
	opening := stockAmount - closing

	if opening > 0 && !p.withinInitialMargin(ticker, opening, fillPrice) {
		return
	}

	// Realised against the average cost the engine already keeps, net of this
	// trade's fee, and read BEFORE the position is decremented. Only the
	// closing portion realises anything: opening a short realises nothing, the
	// same way opening a long does not.
	var realized float64
	if closing > 0 {
		realized = closing*(fillPrice-pos.AveragePrice) - fee
	}
	var heldDays float64
	if p.openLots != nil && closing > 0 {
		p.openLots[ticker], heldDays = fifoHoldingDays(
			p.openLots[ticker], closing, time)
	}
	p.record(Trade{
		Ticker: ticker, Date: time, Side: "sell", Shares: stockAmount,
		Price: fillPrice, Notional: notional, Fee: fee,
		RealizedPnL: realized, HoldingDays: heldDays,
	})

	switch {
	case opening == 0:
		pos.Amount -= stockAmount
		if pos.Amount == 0 {
			delete(p.Positions, ticker)
		}
	case !ok || pos.Amount == 0 || closing == held:
		// The long (if any) is gone and a short of `opening` shares stands in
		// its place. AveragePrice becomes the average price SHORTED at, which
		// is what a cover is measured against.
		if !ok {
			p.Positions[ticker] = &Position{
				Amount: -opening, AveragePrice: fillPrice,
			}
		} else if pos.Amount-stockAmount == 0 {
			delete(p.Positions, ticker)
		} else {
			short := -(pos.Amount - stockAmount)
			pos.AveragePrice = averageShortPrice(pos, short, opening, fillPrice)
			pos.Amount -= stockAmount
		}
	}
	p.Deposit(proceeds)
	p.tradedNotional += notional
}

// averageShortPrice blends an existing short's entry price with new shares
// shorted. Split out because the sign conventions make it easy to get wrong
// inline: `existing` is the magnitude of the short BEFORE this order, which is
// `short - opening`.
func averageShortPrice(
	pos *Position, short, opening, fillPrice float64,
) float64 {
	existing := short - opening
	if existing <= 0 {
		return fillPrice
	}
	return (pos.AveragePrice*existing + fillPrice*opening) / short
}

// withinInitialMargin reports whether the book could still meet its initial
// requirement with this short opened on top of what it already carries.
//
// Equity, not cash, is the backing: shorting is equity-NEUTRAL — the proceeds
// enter cash exactly as the position's negative value leaves it — so the check
// is the same before and after the order fills, and can be made here.
func (p *Portfolio) withinInitialMargin(
	ticker string, opening, fillPrice float64,
) bool {
	gross := opening*fillPrice + p.grossShortValue(ticker)
	if gross <= 0 {
		return true
	}
	return p.markedEquity() >= p.Margin.Initial()*gross
}

// grossShortValue is the marked value of every short the book carries, at the
// price each was last marked at. `excluding` is skipped, so a caller can add
// its own pending order without double counting.
func (p *Portfolio) grossShortValue(excluding string) float64 {
	gross := 0.0
	for t, pos := range p.Positions {
		if t == excluding || pos.Amount >= 0 {
			continue
		}
		gross += -pos.Amount * p.markOf(pos)
	}
	return gross
}

// markedEquity is cash plus every position at its last mark. Used by the margin
// checks, which run inside the trade path where the day's price series is not
// in hand; GetPortfolioValue is the version to prefer everywhere else.
func (p *Portfolio) markedEquity() float64 {
	equity := p.BuyingPower
	for _, pos := range p.Positions {
		equity += pos.Amount * p.markOf(pos)
	}
	return equity
}

// markOf is the price a position was last marked at, falling back to what it
// was opened at before any mark has been written.
func (p *Portfolio) markOf(pos *Position) float64 {
	if pos.CurrentPrice > 0 {
		return pos.CurrentPrice
	}
	return pos.AveragePrice
}

// enforceMaintenanceMargin covers shorts until the book meets its maintenance
// requirement again, and is the reason a short can lose more than it made: the
// position moves against you, the requirement grows against a shrinking equity,
// and the cover happens whether or not the strategy wanted it.
//
// HOW MUCH IS COVERED. Covering is equity-neutral — cash out, position value
// back — so covering notional x leaves equity E alone and reduces gross short S
// to S-x. The requirement E >= m*S is therefore met exactly when
// x >= S - E/m, and that is what this covers rather than liquidating the whole
// book on a small breach.
//
// Costs break the neutrality slightly: slippage and commission come out of
// equity, so a book that was exactly at the line can still be under it after
// covering. Rather than iterate to a fixed point, a book still in breach after
// one pass is covered ENTIRELY. That is blunt, but it terminates, it is
// deterministic, and leaving a book in breach would be worse than over-covering
// a rare edge case.
//
// Tickers are covered in p.Tickers order, not map order, so the same run always
// produces the same book.
func (p *Portfolio) enforceMaintenanceMargin(
	windowed map[string][]data.AssetData, day int,
) {
	if !p.AllowShort {
		return
	}
	gross := p.grossShortValue("")
	if gross <= 0 {
		return
	}
	m := p.Margin.Maintenance()
	if !inBreach(p.markedEquity(), m, gross) {
		return
	}
	// The size that breached, kept for the diagnostic: `gross` is recomputed
	// below and would otherwise report what is LEFT rather than what was
	// called on.
	breached := gross

	p.forcedCover = true
	defer func() { p.forcedCover = false }()

	covered := p.coverShorts(windowed, day, gross-p.markedEquity()/m)
	// Second pass: costs may have eaten the margin the first cover restored.
	if gross = p.grossShortValue(""); gross > 0 {
		if inBreach(p.markedEquity(), m, gross) {
			covered += p.coverShorts(windowed, day, gross)
		}
	}
	if covered > 0 {
		p.Metrics.MarginCalls++
		diagf("warn", p.Pname,
			"portfolio %q: margin call — equity fell below %.0f%% of a "+
				"%.2f short book, so %.2f of it was covered whether the "+
				"strategy wanted it or not",
			p.Pname, m*100, breached, covered)
	}
}

// inBreach compares equity against the requirement with a relative tolerance.
// Exact comparison is wrong here: covering restores equity to the requirement
// EXACTLY, so a book that has just been brought back into line lands within a
// float ulp of the boundary, and a bare `<` reads that as a second breach and
// liquidates the whole book. The tolerance is what keeps the fallback for real
// shortfalls — costs — rather than for rounding.
func inBreach(equity, maintenance, gross float64) bool {
	return equity < maintenance*gross*(1.0-1e-9)
}

// coverShorts buys back up to `need` of short notional, in ticker order.
// Returns the notional actually covered.
func (p *Portfolio) coverShorts(
	windowed map[string][]data.AssetData, day int, need float64,
) float64 {
	covered := 0.0
	for _, ticker := range p.Tickers {
		if need <= 0 {
			break
		}
		pos, ok := p.Positions[ticker]
		if !ok || pos.Amount >= 0 {
			continue
		}
		bars := windowed[ticker]
		if day >= len(bars) || bars[day].Close <= 0 {
			continue
		}
		price := bars[day].Close
		shares := -pos.Amount
		if want := need / price; want < shares {
			shares = want
		}
		// Measure what the order actually did, not what it was asked to do.
		// Buy has guards of its own, and counting an attempt as a cover would
		// report a margin call that never happened.
		before := pos.Amount
		p.Buy(ticker, shares, price, bars[day].Date)
		filled := 0.0
		if after, ok := p.Positions[ticker]; ok {
			filled = after.Amount - before
		} else {
			filled = -before
		}
		covered += filled * price
		need -= filled * price
	}
	return covered
}

func (p *Portfolio) GetPortfolioValue(
	tickers []string,
	historicalData map[string][]data.AssetData,
	day int,
) float64 {
	value := p.BuyingPower
	for _, ticker := range tickers {
		tickerData := historicalData[ticker]
		if day >= len(tickerData) {
			continue
		}
		// != 0 rather than > 0: a short's Amount is negative, so this same
		// line subtracts what it would cost to buy the shares back. The
		// proceeds are already in BuyingPower, so the pair marks the short to
		// market correctly.
		if position, ok := p.Positions[ticker]; ok && position.Amount != 0 {
			value += position.Amount * tickerData[day].Close
		}
	}
	return value
}

// AdjustPortfolioParameters records the day's return and refreshes
// current prices on open positions.
// cashflow is the signed amount the schedule moved at the top of this day, 0
// when there was none. It is a PARAMETER rather than something the caller
// folds into startingValue because there are four hand-written daily loops in
// this package: making it explicit means a new one cannot silently omit it,
// since it will not compile without saying what the flow was.
func (p *Portfolio) AdjustPortfolioParameters(
	tickers []string,
	currentDayData map[string][]data.AssetData,
	day int,
	startingValue float64,
	endingValue float64,
	cashflow float64,
) {
	// Money added or taken between the two marks is not performance. Raising
	// the baseline by the flow is the standard time-weighted treatment and is
	// what makes the reported return answer "how did the STRATEGY do" rather
	// than "how much did the account grow", which is the only question a
	// backtest can answer about a book whose size the user was changing.
	startingValue += cashflow
	dailyChange := 0.0
	if startingValue > 0.0 {
		dailyChange = (endingValue - startingValue) / startingValue
	}
	TransactionLogger.Printf("dailyChange: %.4f\n", dailyChange*100)
	date := currentDayData[tickers[0]][day].Date
	p.DailyReturns = append(p.DailyReturns,
		DailyReturn{Date: date, Return: dailyChange, Flow: cashflow})
	p.PortfolioCloseValues = append(p.PortfolioCloseValues, endingValue)
	// The invested fraction of the book on this day, which is what a random
	// timing null has to hold fixed while destroying WHEN the exposure
	// happened — see significance.go. Also the honest answer to "how much of
	// the time was this strategy actually in the market".
	invested := 0.0
	if endingValue > 0 {
		// Long-only, the invested fraction is everything that is not cash.
		// That identity breaks with shorts — a short RAISES cash while adding
		// exposure — so shorting portfolios measure gross exposure instead.
		// The two agree exactly for a long-only book, but the long-only branch
		// is kept so those results stay bit-identical rather than merely equal.
		if p.AllowShort {
			invested = p.grossExposure(tickers, currentDayData, day) / endingValue
		} else {
			invested = (endingValue - p.BuyingPower) / endingValue
		}
	}
	p.Exposure = append(p.Exposure, invested)

	for _, ticker := range tickers {
		if pos, ok := p.Positions[ticker]; ok && pos.Amount != 0 {
			tickerData := currentDayData[ticker]
			if day < len(tickerData) {
				pos.CurrentPrice = tickerData[day].Close
			}
		}
	}
}

// chargeBorrow accrues one trading day of borrow cost on every short the book
// is carrying INTO this day, and is why it runs before the strategy acts: a
// position opened today was not borrowed overnight, and one closed today was.
//
// The short is valued at the mark it was carried at — the previous day's close,
// which AdjustPortfolioParameters left on the position — rather than at today's,
// because that is the price the borrow was actually against.
//
// Charged straight to cash rather than as a trade: no shares change hands, so a
// blotter entry would corrupt the trade count, the win rate and the holding
// period. Metrics.BorrowPaid is where it surfaces instead.
func (p *Portfolio) chargeBorrow() {
	if p.Costs.BorrowBpsAnnual == 0 {
		return
	}
	gross := 0.0
	for _, pos := range p.Positions {
		if pos.Amount >= 0 {
			continue
		}
		gross += -pos.Amount * p.markOf(pos)
	}
	if gross <= 0 {
		return
	}
	// One day of the annual rate, over the same 252-trading-day year every
	// other annualised figure here uses.
	fee := gross * bps(p.Costs.BorrowBpsAnnual) / 252.0
	p.BuyingPower -= fee
	p.Metrics.BorrowPaid += fee
}

// grossExposure is the absolute notional at risk, long and short alike. A
// market-neutral book nets to nothing and is not thereby unexposed, which is
// the whole reason this is not a signed sum.
func (p *Portfolio) grossExposure(
	tickers []string, currentDayData map[string][]data.AssetData, day int,
) float64 {
	gross := 0.0
	for _, ticker := range tickers {
		pos, ok := p.Positions[ticker]
		if !ok || pos.Amount == 0 {
			continue
		}
		bars := currentDayData[ticker]
		if day >= len(bars) {
			continue
		}
		notional := pos.Amount * bars[day].Close
		if notional < 0 {
			notional = -notional
		}
		gross += notional
	}
	return gross
}
