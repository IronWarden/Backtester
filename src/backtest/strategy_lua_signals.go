package backtest

// The signal and portfolio primitives a Lua strategy can build on.
//
// The original API was two indicators wide — `sma` and `rsi` — so every idea
// beyond a moving-average cross meant hand-writing the arithmetic in Lua.
// `momentum_rotation.lua` and `rebalance.lua` each carried their own copy of the
// same `equity()` and the same forty-line sell-then-buy rebalancer, which is
// duplicated logic in the one place a bug is invisible: a strategy that
// mis-sizes an order does not error, it just reports a different number.
//
// Everything here is ADDITIVE. No existing global changes behaviour, so no
// shipped script and no user script can break — the two rewritten scripts in
// `strategies/` are byte-identical in output, which is the test that these
// primitives are right rather than merely plausible.
//
// Two conventions worth knowing before adding more:
//
//   - **A second return value says whether the answer is real.** The original
//     API returns a neutral number when there is not enough history (`sma` → 0,
//     `rsi` → 50), which silently turns "unknown" into "zero". The new
//     functions keep that first return for compatibility with that style, and
//     add `ok` as a second: `local r, ok = roc(t, day, 21)`. A script that
//     ignores it behaves exactly as the old style did.
//   - **Nothing here reads past `day`.** Every window ends at the day being
//     asked about, which is what keeps a strategy honest without the author
//     having to think about it.

import (
	"math"
	"my-backtester/src/data"
	"sort"

	lua "github.com/yuin/gopher-lua"
)

// rebalanceEpsilon is the share count below which an order is not worth
// placing, lifted verbatim from the hand-rolled rebalancers in
// `momentum_rotation.lua` and `rebalance.lua` so the extracted helper is
// equivalent by construction rather than by argument.
//
// Its exact value turns out not to matter, which is worth recording: mutating it
// to 1e-3 and to 0 both leave every test's output unchanged, because realistic
// deltas are either many shares or exactly zero. What it guards against is
// floating-point dust generating a trade on every rebalance — turnover, and a
// slow leak once costs are switched on — not correctness.
const rebalanceEpsilon = 1e-9

// pushNumOK pushes (value, ok) — the two-return convention described above.
func pushNumOK(L *lua.LState, v float64, ok bool) int {
	if !ok || math.IsNaN(v) || math.IsInf(v, 0) {
		L.Push(lua.LNumber(0))
		L.Push(lua.LBool(false))
		return 2
	}
	L.Push(lua.LNumber(v))
	L.Push(lua.LBool(true))
	return 2
}

// dailyReturns is the return series over the n days ending at day, or false when
// the window does not fit or a price is unusable.
func dailyReturns(series []data.AssetData, day, n int) ([]float64, bool) {
	if n < 1 || day < n || day >= len(series) {
		return nil, false
	}
	out := make([]float64, 0, n)
	for i := day - n + 1; i <= day; i++ {
		prev := series[i-1].Close
		if prev <= 0 {
			return nil, false
		}
		out = append(out, series[i].Close/prev-1)
	}
	return out, true
}

// mean() lives in signalscan.go — the screen and the Lua library measure the
// same things and must not drift apart.
//
// sampleStdev uses the n-1 denominator, matching metrics.go's convention so a
// volatility computed in Lua and one reported in the results mean the same
// thing.
func sampleStdev(xs []float64) (float64, bool) {
	if len(xs) < 2 {
		return 0, false
	}
	m := mean(xs)
	var ss float64
	for _, x := range xs {
		ss += (x - m) * (x - m)
	}
	return math.Sqrt(ss / float64(len(xs)-1)), true
}

// registerSignals adds the per-ticker signal primitives.
func registerSignals(L *lua.LState, hist map[string][]data.AssetData) {
	// roc(ticker, day, n) — trailing return over the n days ending at day.
	// The building block of every momentum and trend idea.
	L.SetGlobal("roc", L.NewFunction(func(L *lua.LState) int {
		series := hist[L.ToString(1)]
		day, n := L.ToInt(2), L.ToInt(3)
		if n < 1 || day-n < 0 || day >= len(series) {
			return pushNumOK(L, 0, false)
		}
		past := series[day-n].Close
		if past <= 0 {
			return pushNumOK(L, 0, false)
		}
		return pushNumOK(L, series[day].Close/past-1, true)
	}))

	// stdev(ticker, day, n) — realised volatility: the standard deviation of
	// the n daily returns ending at day, NOT annualized. Multiply by
	// math.sqrt(252) for an annual figure.
	L.SetGlobal("stdev", L.NewFunction(func(L *lua.LState) int {
		series := hist[L.ToString(1)]
		rets, ok := dailyReturns(series, L.ToInt(2), L.ToInt(3))
		if !ok {
			return pushNumOK(L, 0, false)
		}
		sd, ok := sampleStdev(rets)
		return pushNumOK(L, sd, ok)
	}))

	// zscore(ticker, day, n) — how far today's close sits from its own n-day
	// mean, in standard deviations of the CLOSE (not of returns). The natural
	// mean-reversion signal: strongly negative is stretched to the downside.
	L.SetGlobal("zscore", L.NewFunction(func(L *lua.LState) int {
		series := hist[L.ToString(1)]
		day, n := L.ToInt(2), L.ToInt(3)
		if n < 2 || day-n+1 < 0 || day >= len(series) {
			return pushNumOK(L, 0, false)
		}
		window := make([]float64, 0, n)
		for i := day - n + 1; i <= day; i++ {
			window = append(window, series[i].Close)
		}
		sd, ok := sampleStdev(window)
		if !ok || sd == 0 {
			return pushNumOK(L, 0, false)
		}
		return pushNumOK(L, (series[day].Close-mean(window))/sd, true)
	}))

	// atr(ticker, day, n) — average true range over the n days ending at day,
	// in price units. The usual input to volatility-scaled position sizing and
	// to stops that widen with the instrument.
	L.SetGlobal("atr", L.NewFunction(func(L *lua.LState) int {
		series := hist[L.ToString(1)]
		day, n := L.ToInt(2), L.ToInt(3)
		if n < 1 || day-n < 0 || day >= len(series) {
			return pushNumOK(L, 0, false)
		}
		var sum float64
		for i := day - n + 1; i <= day; i++ {
			prevClose := series[i-1].Close
			tr := series[i].High - series[i].Low
			if d := math.Abs(series[i].High - prevClose); d > tr {
				tr = d
			}
			if d := math.Abs(series[i].Low - prevClose); d > tr {
				tr = d
			}
			sum += tr
		}
		return pushNumOK(L, sum/float64(n), true)
	}))

	// high_n / low_n(ticker, day, n) — the extreme High/Low over the n days
	// ending at day, inclusive. Breakout channels without the loop.
	extreme := func(pick func(data.AssetData) float64, better func(a, b float64) bool) lua.LGFunction {
		return func(L *lua.LState) int {
			series := hist[L.ToString(1)]
			day, n := L.ToInt(2), L.ToInt(3)
			if n < 1 || day-n+1 < 0 || day >= len(series) {
				return pushNumOK(L, 0, false)
			}
			best := pick(series[day-n+1])
			for i := day - n + 2; i <= day; i++ {
				if v := pick(series[i]); better(v, best) {
					best = v
				}
			}
			return pushNumOK(L, best, true)
		}
	}
	L.SetGlobal("high_n", L.NewFunction(extreme(
		func(a data.AssetData) float64 { return a.High },
		func(a, b float64) bool { return a > b },
	)))
	L.SetGlobal("low_n", L.NewFunction(extreme(
		func(a data.AssetData) float64 { return a.Low },
		func(a, b float64) bool { return a < b },
	)))

	// corr(a, b, day, n) — Pearson correlation of the two tickers' daily
	// returns over the n days ending at day. For pairs and for checking that a
	// "diversified" book is not one bet in five costumes.
	L.SetGlobal("corr", L.NewFunction(func(L *lua.LState) int {
		day, n := L.ToInt(3), L.ToInt(4)
		ra, oka := dailyReturns(hist[L.ToString(1)], day, n)
		rb, okb := dailyReturns(hist[L.ToString(2)], day, n)
		if !oka || !okb || len(ra) != len(rb) || len(ra) < 2 {
			return pushNumOK(L, 0, false)
		}
		ma, mb := mean(ra), mean(rb)
		var cov, va, vb float64
		for i := range ra {
			da, db := ra[i]-ma, rb[i]-mb
			cov += da * db
			va += da * da
			vb += db * db
		}
		if va <= 0 || vb <= 0 {
			return pushNumOK(L, 0, false)
		}
		return pushNumOK(L, cov/math.Sqrt(va*vb), true)
	}))
}

// registerCrossSection adds the ranking primitive, which is what turns a
// per-ticker signal into a cross-sectional strategy.
func registerCrossSection(L *lua.LState, p *Portfolio) {
	// rank(day, fn) — evaluate fn(ticker, day) for every ticker and return an
	// array of { ticker = …, value = … }, sorted DESCENDING by value, so
	// ranked[1] is the strongest. Tickers whose fn returns nil, false, or a
	// non-number are left out rather than sorted to one end, which is the
	// difference between "no signal" and "the worst signal".
	//
	// Descending because "hold the top N" is the common case; a script wanting
	// the bottom N reads the array backwards or negates its own signal.
	L.SetGlobal("rank", L.NewFunction(func(L *lua.LState) int {
		day := L.ToInt(1)
		fn := L.ToFunction(2)
		if fn == nil {
			L.Push(L.CreateTable(0, 0))
			return 1
		}

		type scored struct {
			ticker string
			value  float64
		}
		var out []scored
		for _, ticker := range p.Tickers {
			L.Push(fn)
			L.Push(lua.LString(ticker))
			L.Push(lua.LNumber(day))
			if err := L.PCall(2, 1, nil); err != nil {
				// A script whose ranking function errors gets an empty rank
				// rather than a dead worker; the host logs it the same way it
				// logs a failing step.
				L.Push(L.CreateTable(0, 0))
				return 1
			}
			ret := L.Get(-1)
			L.Pop(1)
			num, ok := ret.(lua.LNumber)
			if !ok {
				continue
			}
			v := float64(num)
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			out = append(out, scored{ticker: ticker, value: v})
		}

		// Stable, and ties break on the portfolio's ticker order, so a rank is
		// reproducible run to run — an unstable sort would make a tie-heavy
		// signal produce different trades on identical data.
		sort.SliceStable(out, func(i, j int) bool {
			return out[i].value > out[j].value
		})

		tbl := L.CreateTable(len(out), 0)
		for i, s := range out {
			row := L.CreateTable(0, 2)
			row.RawSetString("ticker", lua.LString(s.ticker))
			row.RawSetString("value", lua.LNumber(s.value))
			tbl.RawSetInt(i+1, row)
		}
		L.Push(tbl)
		return 1
	}))
}

// registerBook adds the portfolio-level primitives that every rebalancing
// strategy was reimplementing.
func registerBook(
	L *lua.LState, p *Portfolio, hist map[string][]data.AssetData,
) {
	priceOf := func(ticker string, day int) float64 {
		series, ok := hist[ticker]
		if !ok || day < 0 || day >= len(series) {
			return 0
		}
		return series[day].Close
	}
	dateOf := func(ticker string, day int) (t data.AssetData, ok bool) {
		series, exists := hist[ticker]
		if !exists || day < 0 || day >= len(series) {
			return data.AssetData{}, false
		}
		return series[day], true
	}

	// equity(day) — cash plus every position marked at that day's close. The
	// figure a target weight is a fraction OF, and previously copied into every
	// script that needed it.
	equity := func(day int) float64 {
		total := p.BuyingPower
		for _, ticker := range p.Tickers {
			pos, ok := p.FindPosition(ticker)
			if !ok || pos == nil {
				continue
			}
			total += pos.Amount * priceOf(ticker, day)
		}
		return total
	}

	L.SetGlobal("equity", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LNumber(equity(L.ToInt(1))))
		return 1
	}))

	// target_weights(day, { TICKER = fraction, … }) — move the book to those
	// fractions of equity. Weights are fractions, not percentages, and a ticker
	// absent from the table is a target of zero, so passing {} goes to cash.
	//
	// Sells run in a first pass and buys in a second, so the proceeds fund the
	// purchases — a single interleaved pass would fail to buy whenever the book
	// is fully invested. Buys are clamped to available cash. This is lifted
	// verbatim from the two hand-rolled rebalancers it replaces, including the
	// epsilon and the pass order, because those scripts' output must not move.
	L.SetGlobal("target_weights", L.NewFunction(func(L *lua.LState) int {
		day := L.ToInt(1)
		targets := L.ToTable(2)
		want := func(ticker string, eq, px float64) float64 {
			if targets == nil || px <= 0 {
				return 0
			}
			w, ok := targets.RawGetString(ticker).(lua.LNumber)
			if !ok {
				return 0
			}
			return (eq * float64(w)) / px
		}
		held := func(ticker string) float64 {
			pos, ok := p.FindPosition(ticker)
			if !ok || pos == nil {
				return 0
			}
			return pos.Amount
		}

		eq := equity(day)
		for _, ticker := range p.Tickers {
			px := priceOf(ticker, day)
			if px <= 0 {
				continue
			}
			if excess := held(ticker) - want(ticker, eq, px); excess > rebalanceEpsilon {
				bar, _ := dateOf(ticker, day)
				p.Sell(ticker, excess, px, bar.Date)
			}
		}
		for _, ticker := range p.Tickers {
			px := priceOf(ticker, day)
			if px <= 0 {
				continue
			}
			short := want(ticker, eq, px) - held(ticker)
			if affordable := p.BuyingPower / px; short > affordable {
				short = affordable
			}
			if short > rebalanceEpsilon {
				bar, _ := dateOf(ticker, day)
				p.Buy(ticker, short, px, bar.Date)
			}
		}
		return 0
	}))

	// weight_of(ticker, day) — the fraction of equity a position currently
	// represents, so a script can check drift without recomputing equity.
	L.SetGlobal("weight_of", L.NewFunction(func(L *lua.LState) int {
		ticker := L.ToString(1)
		day := L.ToInt(2)
		eq := equity(day)
		if eq <= 0 {
			return pushNumOK(L, 0, false)
		}
		pos, ok := p.FindPosition(ticker)
		if !ok || pos == nil {
			return pushNumOK(L, 0, true)
		}
		return pushNumOK(L, pos.Amount*priceOf(ticker, day)/eq, true)
	}))
}
