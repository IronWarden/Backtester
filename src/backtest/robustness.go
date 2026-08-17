package backtest

// Pushing on a result to see whether it holds.
//
// A Sharpe from one window with one cost assumption is a point estimate dressed
// as a finding. Two questions decide whether it survives contact with reality,
// and neither is answerable from a single run:
//
//   - **How much friction kills it?** "Dies above 7bps" is a complete review in
//     three words. A strategy whose edge evaporates at realistic costs is not a
//     strategy, and the only way to know is to re-run it paying more.
//   - **Does it depend on when you started?** Shifting the start by a few weeks
//     changes nothing real about a strategy. If the numbers move a lot, the
//     result was a calendar artifact — the window happened to begin just before
//     one good stretch.
//
// Both re-run the SAME strategy over the SAME already-aligned window, so they
// cost no query and no new data. They do cost simulations: eight per portfolio,
// which is why they are opt-in rather than always on.
//
// What is deliberately not here yet: the parameter-neighbourhood check (score
// the swept winner's neighbours — a winner on a plateau is plausible, a spike
// between two bad neighbours is overfit) and universe subsampling. Both need to
// know about the sweep that produced a result, which is a different piece of
// machinery from re-running one portfolio, and bolting them on here would make
// this the wrong shape for both.

import (
	"fmt"
	"math"
	"my-backtester/src/data"
	"strings"
)

// RobustnessChecks turns the battery on. Set once before Run, like
// TransactionLogger: it multiplies the simulation count by about nine, so it is
// off unless asked for.
var RobustnessChecks bool

// CostLevels are the round-trip slippage levels the battery re-runs at, in
// basis points. Chosen to bracket reality: 0 is the frictionless fantasy, 5 is
// a liquid large cap, 20 is a small cap or an impatient fill, and 50 is a
// genuinely expensive market.
var CostLevels = []float64{0, 5, 20, 50}

// StartShifts are how many trading days LATER the window is made to begin.
// Forward only: a window cannot start before its own first bar, so a negative
// shift has nothing to run on. Small enough that nothing about the strategy
// changes, large enough that a calendar artifact shows up.
var StartShifts = []int{5, 20, 60, 120}

// CostPoint is one re-run at one friction level.
type CostPoint struct {
	SlippageBps  float64 `json:"slippageBps"`
	FinalValue   float64 `json:"finalValue"`
	AnnualReturn float64 `json:"annualReturn"`
	SharpeRatio  float64 `json:"sharpeRatio"`
	// Trades is how many fills the re-run placed. Reported because the
	// interesting failure is not a strategy that pays more, it is one that
	// STOPS TRADING: the returns then collapse in a cliff and stay flat, which
	// reads as "survives higher costs" when the truth is "no longer a
	// strategy". T13 makes exactly this happen to buyAndHold at any non-zero
	// slippage.
	Trades int `json:"trades"`
}

// StartPoint is one re-run with the window start moved.
type StartPoint struct {
	ShiftDays    int     `json:"shiftDays"`
	FinalValue   float64 `json:"finalValue"`
	AnnualReturn float64 `json:"annualReturn"`
	SharpeRatio  float64 `json:"sharpeRatio"`
}

// Robustness is what survived the pushing.
type Robustness struct {
	Costs  []CostPoint  `json:"costs"`
	Starts []StartPoint `json:"starts"`
	// BreakEvenBps is the lowest tested friction at which the annual return
	// turns negative. -1 means it survived every level tested, which is the
	// good answer and is not the same as "untested".
	BreakEvenBps float64 `json:"breakEvenBps"`
	// StartSpread is the range of annual returns across the start shifts, in
	// percentage points. A result that only exists for one start date shows up
	// here as a wide spread.
	StartSpread float64 `json:"startSpread"`
	// WorstStart and BestStart are the extremes of that range, so a reader can
	// see whether the spread is one bad shift or a genuine dependence.
	WorstStart float64 `json:"worstStart"`
	BestStart  float64 `json:"bestStart"`
	// StoppedTradingAtBps is the lowest friction at which the re-run placed no
	// trades at all, or 0 if it always traded. A strategy that stops trading
	// looks identical to one whose edge is cost-proof: both flatten out. This
	// distinguishes them.
	StoppedTradingAtBps float64 `json:"stoppedTradingAtBps"`
	// TradesChangedAtBps is the lowest friction at which the re-run placed a
	// DIFFERENT number of trades than the frictionless one. Costs are supposed
	// to change returns, not behaviour; when they change behaviour, the cost
	// curve is measuring two things at once and the drop cannot be read as a
	// cost. On the default config this fires at 5 bps, where one order of five
	// is rejected and the final value falls from 590k to 69k — T13.
	TradesChangedAtBps float64 `json:"tradesChangedAtBps"`
	Computed           bool    `json:"computed"`
}

// String renders the battery for the CLI.
func (r Robustness) String() string {
	if !r.Computed {
		return "robustness: not computed"
	}
	var b strings.Builder
	b.WriteString("  cost sensitivity (round-trip slippage):\n")
	for _, c := range r.Costs {
		note := ""
		if c.Trades == 0 && c.SlippageBps > 0 {
			note = "  <-- STOPPED TRADING"
		}
		fmt.Fprintf(&b,
			"    %4.0f bps  CAGR %7.2f%%  Sharpe %6.2f  trades %4d  final %12.2f%s\n",
			c.SlippageBps, c.AnnualReturn, c.SharpeRatio, c.Trades,
			c.FinalValue, note)
	}
	if r.TradesChangedAtBps > 0 {
		fmt.Fprintf(&b, "    trade count CHANGES at %.0f bps (%d fills against "+
			"%d when free) — costs are altering behaviour, not just returns, "+
			"so the drop below is not a cost\n",
			r.TradesChangedAtBps, tradesAt(r.Costs, r.TradesChangedAtBps),
			r.Costs[0].Trades)
	}
	if r.StoppedTradingAtBps > 0 {
		fmt.Fprintf(&b, "    stops trading at %.0f bps — the flat line above "+
			"is a book sitting in cash, NOT an edge surviving friction\n",
			r.StoppedTradingAtBps)
	}
	if r.BreakEvenBps < 0 {
		fmt.Fprintf(&b, "    survives every level tested (to %.0f bps)\n",
			CostLevels[len(CostLevels)-1])
	} else {
		fmt.Fprintf(&b, "    edge dies at %.0f bps\n", r.BreakEvenBps)
	}

	b.WriteString("  start-date sensitivity:\n")
	for _, s := range r.Starts {
		fmt.Fprintf(&b, "    %+4dd     CAGR %7.2f%%  Sharpe %6.2f\n",
			s.ShiftDays, s.AnnualReturn, s.SharpeRatio)
	}
	fmt.Fprintf(&b, "    spread %.2f points (worst %.2f%%, best %.2f%%)%s\n",
		r.StartSpread, r.WorstStart, r.BestStart,
		startVerdict(r.StartSpread))
	return b.String()
}

// tradesAt is the fill count recorded at one cost level.
func tradesAt(points []CostPoint, bps float64) int {
	for _, c := range points {
		if c.SlippageBps == bps {
			return c.Trades
		}
	}
	return 0
}

// startVerdict names what a spread means, since the number alone invites
// wishful reading.
func startVerdict(spread float64) string {
	switch {
	case spread > 10:
		return " — the result depends heavily on when you started"
	case spread > 5:
		return " — noticeably start-dependent"
	default:
		return ""
	}
}

// applyRobustness re-runs the strategy under more friction and from shifted
// starts, over the same aligned window.
func (p *Portfolio) applyRobustness(
	windowed map[string][]data.AssetData,
	dataLen int,
	riskFreeRates map[int64]float64,
) {
	if !RobustnessChecks || dataLen < 2 || len(p.Tickers) == 0 {
		return
	}
	r := Robustness{BreakEvenBps: -1}

	for _, bps := range CostLevels {
		costs := p.Costs
		costs.SlippageBps = bps
		run, ok := p.rerun(windowed, dataLen, riskFreeRates, costs, 0)
		if !ok {
			continue
		}
		point := CostPoint{
			SlippageBps:  bps,
			FinalValue:   finalOf(run),
			AnnualReturn: run.Metrics.AnnualReturn,
			SharpeRatio:  run.Metrics.SharpeRatio,
			Trades:       len(run.Trades),
		}
		r.Costs = append(r.Costs, point)
		// The first level at which the edge turns negative. Recorded once:
		// later levels are worse by construction and would only overwrite it.
		if point.AnnualReturn < 0 && r.BreakEvenBps < 0 {
			r.BreakEvenBps = bps
		}
		if point.Trades == 0 && bps > 0 && r.StoppedTradingAtBps == 0 {
			r.StoppedTradingAtBps = bps
		}
		if bps > 0 && len(r.Costs) > 1 && r.TradesChangedAtBps == 0 &&
			point.Trades != r.Costs[0].Trades {
			r.TradesChangedAtBps = bps
		}
	}

	for _, shift := range StartShifts {
		run, ok := p.rerun(windowed, dataLen, riskFreeRates, p.Costs, shift)
		if !ok {
			continue
		}
		r.Starts = append(r.Starts, StartPoint{
			ShiftDays:    shift,
			FinalValue:   finalOf(run),
			AnnualReturn: run.Metrics.AnnualReturn,
			SharpeRatio:  run.Metrics.SharpeRatio,
		})
	}

	if len(r.Starts) > 0 {
		r.WorstStart, r.BestStart = r.Starts[0].AnnualReturn, r.Starts[0].AnnualReturn
		for _, s := range r.Starts {
			r.WorstStart = math.Min(r.WorstStart, s.AnnualReturn)
			r.BestStart = math.Max(r.BestStart, s.AnnualReturn)
		}
		r.StartSpread = r.BestStart - r.WorstStart
	}

	r.Computed = len(r.Costs) > 0 || len(r.Starts) > 0
	p.Robustness = r
}

// rerun simulates the same strategy again over the same window, with a
// different cost model and/or a later start.
//
// A fresh Portfolio and a fresh Strategy on purpose: a strategy carries state
// across days, so reusing the one that just ran would have it resume mid-thought
// rather than start over. The Lua host is closed afterwards for the same reason
// runOne closes it.
//
// A later start SLICES the data rather than skipping days into it, and the
// difference is not cosmetic. Strategies key off the absolute day index —
// buy_and_hold buys on day 0, every cadence rule is `day % n == 0` — so a loop
// that merely began at day 5 would leave buy-and-hold sitting in cash for the
// whole run and report a flat 0% as though the start date had destroyed the
// strategy. Re-basing the window so the strategy sees its own day 0 is what
// makes the comparison mean "started later" rather than "was never called
// properly".
func (p *Portfolio) rerun(
	windowed map[string][]data.AssetData,
	dataLen int,
	riskFreeRates map[int64]float64,
	costs CostConfig,
	shiftDays int,
) (*Portfolio, bool) {
	if shiftDays < 0 || shiftDays >= dataLen-1 {
		return nil, false
	}
	// Re-base the window: the strategy must see its own day 0.
	shifted := windowed
	length := dataLen
	if shiftDays > 0 {
		shifted = make(map[string][]data.AssetData, len(p.Tickers))
		for _, ticker := range p.Tickers {
			bars := windowed[ticker]
			if shiftDays >= len(bars) {
				return nil, false
			}
			shifted[ticker] = bars[shiftDays:]
		}
		length = dataLen - shiftDays
	}
	if length < 2 {
		return nil, false
	}

	clone, err := InitializePortfolio(
		p.InitialBuyingPower, p.StartTime, p.EndTime,
		p.Pname, p.Tickers, p.StrategySpec, p.StrategyParams,
	)
	if err != nil {
		return nil, false
	}
	clone.Costs = costs
	defer func() {
		if c, ok := clone.Strategy.(interface{ Close() }); ok {
			c.Close()
		}
	}()

	clone.Strategy.Step(clone, shifted, 0)
	prev := clone.GetPortfolioValue(clone.Tickers, shifted, 0)
	for day := 1; day < length; day++ {
		clone.Strategy.Step(clone, shifted, day)
		curr := clone.GetPortfolioValue(clone.Tickers, shifted, day)
		clone.AdjustPortfolioParameters(clone.Tickers, shifted, day, prev, curr)
		prev = curr
	}
	clone.GetBacktestingData(riskFreeRates, shifted, length)
	return clone, len(clone.PortfolioCloseValues) > 0
}

// finalOf is the last daily value of a finished run.
func finalOf(p *Portfolio) float64 {
	if p == nil || len(p.PortfolioCloseValues) == 0 {
		return 0
	}
	return p.PortfolioCloseValues[len(p.PortfolioCloseValues)-1]
}
