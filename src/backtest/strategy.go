package backtest

import (
	"fmt"
	"math"
	"my-backtester/src/data"
	"sort"
	"strconv"
	"strings"
)

// Strategy operates on a portfolio one day at a time. Step is called for
// every day in the backtest window; the strategy decides whether to act.
// Per-strategy state (e.g. previous SMA values) lives on the implementing
// struct, so a Strategy instance is single-use — clone the portfolio (which
// rebuilds the strategy from its spec) for repeat runs.
type Strategy interface {
	Name() string
	Step(p *Portfolio, hist map[string][]data.AssetData, day int)
}

// NewStrategy builds a Strategy from a spec string and optional typed
// params. Formats:
//   - "greedy" / "equalWeights"          -> BuyAndHold with that buy type
//   - "buyAndHold:<buyType>"             -> BuyAndHold
//   - "smaCross:<short>:<long>:<buyType>" -> SMACross
//   - "lua:<path>"                       -> LuaStrategy (params from arg)
//
// warnIgnoredParams reports Params handed to a strategy that cannot read them.
//
// NewStrategy passes params to the "lua" case and to nothing else, so a
// [portfolio.Params] block on a built-in spec is silently discarded. The Lua
// host has its own per-key check (see warnUnreadParams); this is the case that
// check can never see, because there is no interpreter to ask.
//
// The severe version is a sweep. Sweep values are merged into Params, so
// sweeping a built-in expands to N portfolios that are byte-identical runs,
// while Trials is N and the deflated Sharpe discounts the result for a search
// that never happened.
//
// smaCross earns its own sentence: it is the built-in people most want to
// sweep, and its periods live in the SPEC STRING rather than in Params, so
// "Sweep = { short = [5, 10] }" looks exactly like it should work.
func warnIgnoredParams(
	name, spec string, params, sweep map[string]any, combos int,
) {
	if strings.HasPrefix(spec, "lua:") {
		return
	}
	// Both maps, because a swept block usually has NO [portfolio.Params] at
	// all — the keys arrive only in [portfolio.Sweep] and are merged in per
	// combination afterwards. Checking Params alone would miss exactly the
	// case that matters most.
	seen := make(map[string]bool, len(params)+len(sweep))
	for k := range params {
		seen[k] = true
	}
	for k := range sweep {
		seen[k] = true
	}
	if len(seen) == 0 {
		return
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	msg := fmt.Sprintf(
		"portfolio %q: strategy %q takes no Params, so %s %s ignored",
		name, spec, strings.Join(quoteAll(keys), ", "),
		plural(len(keys), "is", "are"))
	if strings.HasPrefix(spec, "smaCross") {
		msg += "; smaCross reads its periods from the spec string itself, " +
			"as smaCross:<short>:<long>:<buyType>"
	}
	if combos > 1 {
		msg += fmt.Sprintf(
			"; this block expands to %d trials that will therefore be "+
				"identical, and the deflated Sharpe will discount them as a "+
				"search of %d", combos, combos)
	}
	diagf("warn", name, "%s", msg)
}

func quoteAll(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = fmt.Sprintf("%q", s)
	}
	return out
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

func NewStrategy(spec string, params map[string]any) (Strategy, error) {
	parts := strings.SplitN(spec, ":", 2)
	switch parts[0] {
	case "greedy", "equalWeights":
		return &BuyAndHold{BuyType: parts[0]}, nil
	case "buyAndHold":
		if len(parts) < 2 {
			return nil, fmt.Errorf("buyAndHold spec needs a buy type: %q", spec)
		}
		return &BuyAndHold{BuyType: parts[1]}, nil
	case "smaCross":
		sub := strings.Split(parts[1], ":")
		if len(sub) < 3 {
			return nil, fmt.Errorf(
				"smaCross spec needs short:long:buyType: %q", spec,
			)
		}
		short, err := strconv.Atoi(sub[0])
		if err != nil {
			return nil, fmt.Errorf("smaCross short period: %w", err)
		}
		long, err := strconv.Atoi(sub[1])
		if err != nil {
			return nil, fmt.Errorf("smaCross long period: %w", err)
		}
		return &SMACross{Short: short, Long: long, BuyType: sub[2]}, nil
	case "lua":
		if len(parts) < 2 || parts[1] == "" {
			return nil, fmt.Errorf("lua spec needs a script path: %q", spec)
		}
		return NewLuaStrategy(parts[1], params)
	}
	return nil, fmt.Errorf("unknown strategy spec: %q", spec)
}

func SMA(stocks []data.AssetData) float64 {
	var mean float64
	for _, stock := range stocks {
		mean += stock.Close
	}
	mean /= float64(len(stocks))
	return mean
}

func RSI(closeValues []float64, rsPeriod float64) float64 {
	upDays := make([]float64, 0)
	downDays := make([]float64, 0)
	upDayRs, downDayRs := 0.0, 0.0

	for i := range closeValues {
		if closeValues[i] >= 0 {
			upDays = append(upDays, closeValues[i])
		} else {
			downDays = append(downDays, closeValues[i])
		}
	}
	for _, value := range upDays {
		upDayRs += value
	}
	upDayRs /= rsPeriod
	for _, value := range downDays {
		downDayRs += value
	}
	downDayRs /= rsPeriod
	rs := upDayRs / downDayRs

	return 100 - (100 / (1 + rs))
}

type BuyAndHold struct {
	BuyType string
}

func (s *BuyAndHold) Name() string { return "buyAndHold:" + s.BuyType }

// Step buys the whole book on the first day and then holds — except that a
// portfolio with a [portfolio.Cashflow] schedule keeps receiving money, and
// "buy and hold" to someone contributing every month means "hold, and keep
// buying". Without this the deposits would simply pile up as cash forever in
// the DEFAULT strategy, which is what the Backtest tab generates unless a
// library script is picked, and the feature would look broken to precisely the
// people who asked for it.
//
// Nothing changes for a portfolio without a schedule: PendingCashflow is 0 on
// every day, so the day != 0 branch returns exactly as it always did and every
// existing result is bit-identical.
func (s *BuyAndHold) Step(
	p *Portfolio, hist map[string][]data.AssetData, day int,
) {
	if day != 0 {
		s.investContribution(p, hist, day)
		return
	}
	// equalWeights sizes every leg against the balance as it stood before any
	// of them filled. Reading the live balance instead would divide an
	// already-drained figure by the full ticker count, shrinking each leg in
	// turn and stranding ((N-1)/N)^N of the capital in cash forever — 25% at
	// two tickers, converging to 1/e. greedy is the opposite by design: it
	// pours whatever is left into each ticker in order, so it must keep
	// reading the live balance.
	budget := p.BuyingPower
	for _, ticker := range p.Tickers {
		td := hist[ticker]
		if len(td) == 0 {
			continue
		}
		price := td[0].Close
		available := p.BuyingPower
		if s.BuyType == "equalWeights" {
			available = budget
		}
		amount := generalBuy(available, price, s.BuyType, p.Tickers, p.Costs)
		p.Buy(ticker, amount, price, td[0].Date)
	}
}

// investContribution puts a deposit to work on the day it arrives, at the same
// weights the opening purchase used.
//
// Only the NEW money is spent, not the whole cash balance. A balance can be
// high for reasons that have nothing to do with a contribution — a forced sale
// to fund a withdrawal, a strategy deliberately holding cash — and sweeping all
// of it into the market would be a different strategy from the one the user
// configured.
//
// Withdrawals are ignored here: raiseCash has already sold pro-rata to fund
// them, and buying back into the position it just sold would defeat the point.
func (s *BuyAndHold) investContribution(
	p *Portfolio, hist map[string][]data.AssetData, day int,
) {
	cash := p.PendingCashflow()
	if cash <= 0 {
		return
	}
	// Sized against the contribution as it stood before any leg filled, for
	// the same reason the opening purchase is: reading the live balance would
	// shrink each leg in turn and strand a fraction of the money in cash.
	budget := cash
	for _, ticker := range p.Tickers {
		td := hist[ticker]
		if day >= len(td) {
			continue
		}
		price := td[day].Close
		if price <= 0 {
			continue
		}
		available := budget
		if s.BuyType == "greedy" {
			// greedy pours what is left into each ticker in order, so it must
			// keep reading the live figure — but capped at the contribution,
			// so it cannot reach into cash the schedule did not provide.
			available = math.Min(cash, p.BuyingPower)
		}
		amount := generalBuy(available, price, s.BuyType, p.Tickers, p.Costs)
		if amount <= 0 {
			continue
		}
		p.Buy(ticker, amount, price, td[day].Date)
	}
}

type SMACross struct {
	Short, Long int
	BuyType     string
	prevShort   map[string]float64
	prevLong    map[string]float64
	sumShort    map[string]float64
	sumLong     map[string]float64
	havePrev    map[string]bool
}

func (s *SMACross) Name() string {
	return fmt.Sprintf("smaCross:%d:%d:%s", s.Short, s.Long, s.BuyType)
}

func (s *SMACross) Step(
	p *Portfolio, hist map[string][]data.AssetData, day int,
) {
	if day < s.Long {
		return
	}
	if s.prevShort == nil {
		s.prevShort = make(map[string]float64, len(p.Tickers))
		s.prevLong = make(map[string]float64, len(p.Tickers))
		s.sumShort = make(map[string]float64, len(p.Tickers))
		s.sumLong = make(map[string]float64, len(p.Tickers))
		s.havePrev = make(map[string]bool, len(p.Tickers))
	}
	for _, ticker := range p.Tickers {
		td := hist[ticker]
		if day >= len(td) {
			continue
		}
		var sShort, sLong float64
		if _, seeded := s.sumShort[ticker]; seeded {
			sShort = s.sumShort[ticker] - td[day-s.Short-1].Close + td[day-1].Close
			sLong = s.sumLong[ticker] - td[day-s.Long-1].Close + td[day-1].Close
		} else {
			for i := day - s.Short; i < day; i++ {
				sShort += td[i].Close
			}
			for i := day - s.Long; i < day; i++ {
				sLong += td[i].Close
			}
		}
		s.sumShort[ticker] = sShort
		s.sumLong[ticker] = sLong
		smaShort := sShort / float64(s.Short)
		smaLong := sLong / float64(s.Long)

		if s.havePrev[ticker] {
			currentDayData := td[day]
			avg := (currentDayData.Low +
				currentDayData.High + currentDayData.Close) / 3.0
			if smaShort > smaLong && s.prevShort[ticker] <= s.prevLong[ticker] {
				amount := generalBuy(
					p.BuyingPower, avg, s.BuyType, p.Tickers, p.Costs)
				p.Buy(ticker, amount, avg, currentDayData.Date)
			} else if smaShort < smaLong && s.prevShort[ticker] >= s.prevLong[ticker] {
				if pos, _ := p.FindPosition(ticker); pos != nil {
					p.Sell(ticker, pos.Amount, avg, currentDayData.Date)
				}
			}
		}
		s.prevShort[ticker] = smaShort
		s.prevLong[ticker] = smaLong
		s.havePrev[ticker] = true
	}
}

// generalBuy sizes an order against the price it will actually fill at, not
// the quoted one. That distinction is the whole of T13: sizing against the
// quote asks for more shares than the cash covers once slippage inflates the
// notional, and Buy's affordability gate then drops the order entirely — so
// any non-zero slippage_bps stopped every spend-it-all strategy from trading
// rather than making its fills slightly worse.
//
// With zero slippage BuyFill multiplies by exactly 1.0, so this is bit-for-bit
// the previous arithmetic and no existing result moves. Commission is not
// handled here: an order sized to spend all its cash still cannot cover a fee
// on top, and the clamp in Buy resizes it, which is the behaviour commission
// already had.
func generalBuy(
	buyingPower float64,
	stockValue float64,
	strategyType string,
	tickers []string,
	costs CostConfig,
) float64 {
	fill := costs.BuyFill(stockValue)
	switch strategyType {
	case "greedy":
		return greedyBuy(buyingPower, fill)
	case "equalWeights":
		return greedyBuy(buyingPower/float64(len(tickers)), fill)
	}
	return 0.0
}

// greedyBuy returns the (fractional) share quantity that spends all of
// buyingPower at stockValue. Fractional shares mean the full allocation is
// invested regardless of the asset's price scale, instead of whole-share
// truncation leaving a chunk of capital idle (or, for high-priced synthetic
// index series, buying zero shares).
func greedyBuy(buyingPower float64, stockValue float64) float64 {
	if stockValue <= 0 {
		return 0.0
	}
	return buyingPower / stockValue
}
