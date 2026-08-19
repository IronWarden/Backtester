package backtest

// Reported fundamentals, reachable from a Lua strategy.
//
// `src/data.PointInTimeFundamentals` has been correct and tested since T29 and
// nothing called it, so the whole value/quality/leverage/size hypothesis space
// was unreachable from a strategy. This is the binding.
//
// The look-ahead guarantee is enforced in Go, upstream of anything here: every
// figure carries the day it became PUBLIC (a real filing date where the SEC
// data has one, the earnings-calendar date next, period end + 90 days as the
// worst-case fallback), and this file only ever asks for figures known on or
// before the simulated day. A script cannot reach a value it could not have
// had, whatever it does with the arguments.
//
// TWO GLOBALS, and deliberately no more:
//
//	fundamental(ticker, metric, day)  --> number, or nil if not yet published
//	market_cap(ticker, day)           --> shares x close, or nil
//
// Ratios, rankings and cross-sectional sorts are arithmetic a strategy can do
// itself. Inventing a wider surface before anything uses the narrow one is how
// APIs get bloated.
//
// NIL, NOT ZERO — and this breaks an otherwise consistent rule. `price()` and
// friends return 0 out of range, which is safe because a price of zero is
// impossible, so 0 unambiguously means "absent". That reasoning does not carry
// over: a company can genuinely report zero net income, zero cash, or negative
// equity. Returning 0 here would make "not published yet" indistinguishable
// from a real zero, which is exactly the ambiguity that produces a strategy
// trading on a figure it never had. So these return nil and the author has to
// handle the gap.

import (
	"time"

	lua "github.com/yuin/gopher-lua"

	"my-backtester/src/data"
)

// sharesMetric is the figure market_cap() divides by. Named once here because
// it is the one metric a user can need without naming it in the config.
const sharesMetric = "Ordinary Shares Number"

// fundamentalLookup answers "what was known about this ticker's metric on this
// day" without rescanning every point per call. Built once per strategy init.
//
// Points are grouped by ticker and metric and sorted by KnownFrom ascending, so
// a lookup is a binary search for the last entry at or before the day.
type fundamentalLookup struct {
	byKey map[string][]data.FundamentalPoint
}

// lookupKey is ticker+metric joined by a byte no ticker or metric contains.
func lookupKey(ticker, metric string) string { return ticker + "\x00" + metric }

func newFundamentalLookup(points []data.FundamentalPoint) *fundamentalLookup {
	fl := &fundamentalLookup{byKey: make(map[string][]data.FundamentalPoint)}
	for _, p := range points {
		k := lookupKey(p.Ticker, p.Metric)
		fl.byKey[k] = append(fl.byKey[k], p)
	}
	for k, ps := range fl.byKey {
		// Insertion sort: the per-key slices are a handful of quarters, and
		// this avoids pulling sort.Slice's reflection into the hot path.
		for i := 1; i < len(ps); i++ {
			for j := i; j > 0 && ps[j].KnownFrom.Before(ps[j-1].KnownFrom); j-- {
				ps[j], ps[j-1] = ps[j-1], ps[j]
			}
		}
		fl.byKey[k] = ps
	}
	return fl
}

// at returns the newest value published on or before day, and whether one
// exists. A figure published exactly on day counts as known: KnownFrom is
// already the first day it was public, not the period it describes.
func (fl *fundamentalLookup) at(ticker, metric string, day time.Time) (float64, bool) {
	ps := fl.byKey[lookupKey(ticker, metric)]
	var best data.FundamentalPoint
	found := false
	for _, p := range ps {
		if p.KnownFrom.After(day) {
			break // sorted ascending, so nothing later can qualify
		}
		best, found = p, true
	}
	if !found {
		return 0, false
	}
	return best.Value, true
}

// registerFundamentals installs the two globals. Registered unconditionally so
// a script calling them against a config that declared no Fundamentals gets a
// clean nil rather than "attempt to call a nil value".
func registerFundamentals(
	L *lua.LState, p *Portfolio, hist map[string][]data.AssetData,
) {
	fl := newFundamentalLookup(p.FundamentalData)

	// dayOf maps a bar index to its date, using the ticker's own series so a
	// short series cannot read another ticker's calendar.
	dayOf := func(ticker string, day int) (time.Time, bool) {
		series, ok := hist[ticker]
		if !ok || day < 0 || day >= len(series) {
			return time.Time{}, false
		}
		return series[day].Date, true
	}

	L.SetGlobal("fundamental", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		metric := L.ToString(2)
		day := L.ToInt(3)
		d, ok := dayOf(ticker, day)
		if !ok {
			L.Push(lua.LNil)
			return 1
		}
		v, ok := fl.at(ticker, metric, d)
		if !ok {
			L.Push(lua.LNil)
			return 1
		}
		L.Push(lua.LNumber(v))
		return 1
	}))

	L.SetGlobal("market_cap", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		day := L.ToInt(2)
		d, ok := dayOf(ticker, day)
		if !ok {
			L.Push(lua.LNil)
			return 1
		}
		shares, ok := fl.at(ticker, sharesMetric, d)
		if !ok || shares <= 0 {
			// No share count means no market cap. Returning the price alone,
			// or zero, would both be a number that looks like an answer.
			L.Push(lua.LNil)
			return 1
		}
		series := hist[ticker]
		close := series[day].Close
		if close <= 0 {
			// Negative and zero closes are real in this database (see the
			// data-quality audit); a market cap computed from one is garbage.
			L.Push(lua.LNil)
			return 1
		}
		L.Push(lua.LNumber(shares * close))
		return 1
	}))
}

// FundamentalCoverage is the fraction of (day x ticker x declared metric)
// cells for which a published figure existed, in percent.
//
// It exists because the silent failure here is expensive and invisible: the
// data begins in 2020, so a strategy run from 2010 sees nil for the first
// decade, trades only in the back half, and looks like it found an edge. The
// engine deliberately does NOT refuse such a window — a strategy may legitimately
// act only when fundamentals exist — so the trap has to be measured instead.
//
// Pure, and 0 when nothing was declared, which is every pre-T33 config.
func FundamentalCoverage(
	points []data.FundamentalPoint,
	tickers []string,
	metrics []string,
	dates []time.Time,
) float64 {
	if len(points) == 0 || len(tickers) == 0 || len(metrics) == 0 || len(dates) == 0 {
		return 0
	}
	fl := newFundamentalLookup(points)
	known, total := 0, 0
	for _, d := range dates {
		for _, t := range tickers {
			for _, m := range metrics {
				total++
				if _, ok := fl.at(t, m, d); ok {
					known++
				}
			}
		}
	}
	if total == 0 {
		return 0
	}
	return float64(known) / float64(total) * 100
}

// applyFundamentalCoverage records the measurement on the portfolio's metrics.
// Called after GetBacktestingData, which assigns Metrics wholesale.
func (p *Portfolio) applyFundamentalCoverage(dates []time.Time) {
	p.Metrics.FundamentalCoverage = FundamentalCoverage(
		p.FundamentalData, p.Tickers, p.Fundamentals, dates,
	)
}
