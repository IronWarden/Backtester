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
//	fundamental(ticker, metric, day [, frequency [, periods_back]])
//	                                  --> number, or nil if not yet published
//	market_cap(ticker, day)           --> shares x close, or nil
//
// frequency is "annual" or "quarterly" and keeps a flow from switching between
// a quarter and a year (T108); periods_back steps back through earlier periods
// of that frequency, so a year-over-year change needs no second window of
// history. Without them, a `financials` run reads exactly what it always has.
// A run with FundamentalSource = "sec" reads `sec_financials` instead (T139),
// where the bare form means the newest full year for a flow and the newest
// balance sheet for a level, and market_cap() is nil.
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
	"math"
	"sort"
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
// Two indexes. byKey groups by ticker and metric, sorted by KnownFrom, and
// serves the three-argument form over `financials` exactly as it always has.
// byFreq adds the frequency to the key and sorts by period end, and serves
// everything that names a frequency — and every SEC lookup, which always has
// one (see defaultFrequency).
type fundamentalLookup struct {
	byKey  map[string][]data.FundamentalPoint
	byFreq map[string][]data.FundamentalPoint
	// sec is set when the points came from `sec_financials`; instant names
	// the metrics that are balance-sheet levels rather than flows.
	sec     bool
	instant map[string]bool
}

// lookupKey is ticker+metric joined by a byte no ticker or metric contains.
func lookupKey(ticker, metric string) string { return ticker + "\x00" + metric }

func newFundamentalLookup(points []data.FundamentalPoint) *fundamentalLookup {
	fl := &fundamentalLookup{
		byKey:   make(map[string][]data.FundamentalPoint),
		byFreq:  make(map[string][]data.FundamentalPoint),
		instant: make(map[string]bool),
	}
	for _, p := range points {
		if p.Source == data.SourceSEC {
			fl.sec = true
		}
		if p.Instant {
			fl.instant[p.Metric] = true
		}
		fk := lookupKey(p.Ticker, p.Metric) + "\x00" + p.Frequency
		fl.byFreq[fk] = append(fl.byFreq[fk], p)
	}
	for k, ps := range fl.byFreq {
		sort.SliceStable(ps, func(i, j int) bool {
			if !ps[i].PeriodEnd.Equal(ps[j].PeriodEnd) {
				return ps[i].PeriodEnd.Before(ps[j].PeriodEnd)
			}
			return ps[i].KnownFrom.Before(ps[j].KnownFrom)
		})
		fl.byFreq[k] = ps
	}
	if fl.sec {
		// The SEC path never reads byKey, and an SEC balance sheet appears
		// twice in points, once per frequency.
		return fl
	}
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

// defaultFrequency is what an SEC lookup reads when the script names no
// frequency: the newest balance sheet for a level, the newest full year for a
// flow. Never a mix — a flow that returned a quarter one day and a year the
// next would step 4x at each company's fiscal year end (T108).
func (fl *fundamentalLookup) defaultFrequency(metric string) string {
	if fl.instant[metric] {
		return "quarterly"
	}
	return "annual"
}

// at returns the figure the three-argument fundamental() returns, and whether
// one exists.
//
// Over `financials` that is the newest value published on or before day,
// whatever its frequency — unchanged since T33, so every existing config reads
// the same numbers. A figure published exactly on day counts as known:
// KnownFrom is already the first day it was public, not the period it
// describes. Over SEC it is atFrequency with defaultFrequency.
func (fl *fundamentalLookup) at(ticker, metric string, day time.Time) (float64, bool) {
	if fl.sec {
		return fl.atFrequency(ticker, metric, day, fl.defaultFrequency(metric), 0)
	}
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

// atFrequency returns the figure of one frequency for the latest period known
// on day — or, with back > 0, for the period that many before it among the
// periods known on day. back = 1 on "annual" is the prior fiscal year as it was
// first published, which is what a year-over-year change needs.
//
// Latest PERIOD, not latest publication: a filing can publish an older period
// for the first time (a comparative, or a metric a company starts tagging), and
// that must not displace a newer period already known.
func (fl *fundamentalLookup) atFrequency(
	ticker, metric string, day time.Time, freq string, back int,
) (float64, bool) {
	ps := fl.byFreq[lookupKey(ticker, metric)+"\x00"+freq]
	seen := 0
	var last time.Time
	for i := len(ps) - 1; i >= 0; i-- {
		p := ps[i]
		if p.KnownFrom.After(day) || (seen > 0 && p.PeriodEnd.Equal(last)) {
			continue
		}
		if seen == back {
			return p.Value, true
		}
		seen++
		last = p.PeriodEnd
	}
	return 0, false
}

// registerFundamentals installs the two globals. Registered unconditionally so
// a script calling them against a config that declared no Fundamentals gets a
// clean nil rather than "attempt to call a nil value".
//
// warnUnloaded says whether p.Fundamentals is known to be the complete list of
// what was loaded, so asking for anything else deserves a warning. A real run
// knows; the script checker, which never sees the config, does not.
func registerFundamentals(
	L *lua.LState, p *Portfolio, hist map[string][]data.AssetData,
	warnUnloaded bool,
) {
	fl := newFundamentalLookup(p.FundamentalData)
	// An empty FundamentalData cannot say which source it came from, so the
	// config decides: SEC semantics apply even before any point is loaded.
	if p.FundamentalSource == data.SourceSEC {
		fl.sec = true
	}

	// dayOf maps a bar index to its date, using the ticker's own series so a
	// short series cannot read another ticker's calendar.
	dayOf := func(ticker string, day int) (time.Time, bool) {
		series, ok := hist[ticker]
		if !ok || day < 0 || day >= len(series) {
			return time.Time{}, false
		}
		return series[day].Date, true
	}

	// A metric nobody declared is never fetched, so asking for it returns nil
	// on every day — indistinguishable from a company that has not reported,
	// and a strategy that holds cash all run because of it looks like a
	// result. Said once per metric, the first time it is asked for.
	declared := make(map[string]bool, len(p.Fundamentals))
	for _, m := range p.Fundamentals {
		declared[m] = true
	}
	warned := make(map[string]bool)
	warnUndeclared := func(metric string) {
		if !warnUnloaded || declared[metric] || warned[metric] {
			return
		}
		warned[metric] = true
		diagf("warn", p.Pname,
			"fundamental(%q) was called but that metric is not loaded, so it "+
				"returns nil on every day. Add it to the portfolio's "+
				"Fundamentals = [...] or to the script's fundamentals = { "+
				"metrics = {...} } table", metric)
	}
	warnedCap := false

	L.SetGlobal("fundamental", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		metric := L.ToString(2)
		day := L.ToInt(3)

		// Both optional arguments are checked BEFORE the day is resolved, so
		// a typo raises on the first call rather than whenever a figure
		// happens to exist. An unknown frequency is an error, never a fall
		// back to "any": a typo that silently widened the query would be
		// the very bug the argument exists to remove.
		freq := ""
		if v := L.Get(4); v != lua.LNil {
			s, ok := v.(lua.LString)
			if !ok || (s != "annual" && s != "quarterly") {
				L.ArgError(4, `frequency must be "annual" or "quarterly"`)
				return 0
			}
			freq = string(s)
		}
		back := 0
		if v := L.Get(5); v != lua.LNil {
			n, ok := v.(lua.LNumber)
			if !ok || n < 0 || float64(n) != math.Trunc(float64(n)) {
				L.ArgError(5, "periods_back must be a whole number, 0 or more")
				return 0
			}
			if freq == "" && !fl.sec {
				L.ArgError(5, `periods_back needs a frequency: `+
					`fundamental(t, metric, day, "annual", 1)`)
				return 0
			}
			back = int(n)
		}

		warnUndeclared(metric)
		d, ok := dayOf(ticker, day)
		if !ok {
			L.Push(lua.LNil)
			return 1
		}
		var v float64
		if freq == "" && back == 0 {
			v, ok = fl.at(ticker, metric, d)
		} else {
			if freq == "" {
				freq = fl.defaultFrequency(metric)
			}
			v, ok = fl.atFrequency(ticker, metric, d, freq, back)
		}
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
		if fl.sec {
			// SEC share counts are as reported and the closes are
			// split-adjusted, so their product is off by every split since
			// the filing. nil is the honest answer, and said once.
			if !warnedCap {
				warnedCap = true
				diagf("warn", p.Pname,
					"market_cap() always returns nil with FundamentalSource = "+
						"\"sec\": SEC share counts are not adjusted for later "+
						"splits. Rank on ratios of two reported figures instead")
			}
			L.Push(lua.LNil)
			return 1
		}
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
