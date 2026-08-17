package backtest

// An adversarial gate a strategy must clear before its numbers mean anything.
//
// A strategy that peeks at future data, or never trades, or quietly loses cash
// to a typo'd ticker produces a perfectly plausible-looking result. None of
// those failures announces itself: the equity curve is smooth, the metrics are
// finite, and the Sharpe is often excellent. So the checks have to be
// adversarial rather than observational.
//
// The look-ahead detector is the one worth the file. Run the strategy twice on
// histories that are IDENTICAL up to day k and different after it. Any strategy
// reading only the past must behave identically up to k in both runs — the
// future it cannot see has changed, and it did not notice. A strategy whose
// trades differ before k has read the future, and there is no way to fake
// passing this: it is a property of the code's behaviour, not of its text.
//
// Everything here runs on synthetic data generated in-process, so validating a
// strategy needs no database and no config.

import (
	"fmt"
	"math"
	"math/rand"
	"my-backtester/src/data"
	"strings"
	"time"
)

// Validation constants. The split point sits well past any reasonable warm-up so
// a strategy has had time to start trading before the future is swapped out.
const (
	validationDays = 400
	validationCash = 100_000.0
	validationSeed = 424242
)

// validationSplits are the days at which the future is forked, and their spread
// is the whole design.
//
// Forking at day k makes the histories differ from k+1 onward, so to catch a
// strategy that peeks h days ahead and acts on day d, some fork must land in
// [d, d+h). Two consequences, both learned by watching the detector miss things:
//
//   - ONE fork is not enough. A strategy peeking a single day ahead differs only
//     on the fork day itself, which is a coin flip on whether that day's
//     decision changes. Nine forks turn that into a near-certainty.
//   - The forks must start EARLY. A strategy that peeks twenty days ahead, buys
//     once and holds makes its only future-dependent decision in the first
//     week — every fork after it compares two identical trade lists and sees
//     nothing.
var validationSplits = []int{0, 5, 15, 40, 90, 150, 220, 300, 360}

// validationTickers is wide enough for a cross-sectional strategy to rank and
// narrow enough to keep the report readable.
var validationTickers = []string{"VAL1", "VAL2", "VAL3", "VAL4"}

// Finding is one thing wrong with a candidate.
type Finding struct {
	Check  string `json:"check"`
	Failed bool   `json:"failed"`
	Detail string `json:"detail"`
	// Fatal marks a finding that invalidates the result rather than merely
	// qualifying it. Look-ahead and lost cash are fatal; never trading is
	// fatal in a different way — there is no result to invalidate.
	Fatal bool `json:"fatal"`
}

// ValidationReport is the verdict on a candidate.
type ValidationReport struct {
	Strategy string    `json:"strategy"`
	Findings []Finding `json:"findings"`
	Passed   bool      `json:"passed"`
}

// String renders the report.
func (v ValidationReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "validating %s\n", v.Strategy)
	for _, f := range v.Findings {
		mark := "ok  "
		if f.Failed {
			mark = "FAIL"
			if f.Fatal {
				mark = "FATAL"
			}
		}
		fmt.Fprintf(&b, "  %-5s %-22s %s\n", mark, f.Check, f.Detail)
	}
	if v.Passed {
		b.WriteString("  passed: nothing here invalidates a result from this " +
			"strategy\n")
	} else {
		b.WriteString("  FAILED: fix these before believing any backtest of " +
			"it\n")
	}
	return b.String()
}

// ValidateStrategy runs the whole battery against a strategy spec.
func ValidateStrategy(spec string, params map[string]any) ValidationReport {
	report := ValidationReport{Strategy: spec}
	report.Findings = append(report.Findings,
		checkLookAhead(spec, params),
		checkTraded(spec, params),
		checkCashConservation(spec, params),
		checkDegenerateInputs(spec, params),
	)
	report.Passed = true
	for _, f := range report.Findings {
		if f.Failed {
			report.Passed = false
		}
	}
	return report
}

// validationHist builds `days` bars for each validation ticker from a seed. Two
// histories built with the same seed are identical; with different seeds they
// diverge immediately.
func validationHist(days int, seed int64) map[string][]data.AssetData {
	rng := rand.New(rand.NewSource(seed))
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	hist := make(map[string][]data.AssetData, len(validationTickers))

	for _, ticker := range validationTickers {
		series := make([]data.AssetData, days)
		px := 100.0
		for i := range series {
			px *= 1 + rng.NormFloat64()*0.012
			if px < 1 {
				px = 1
			}
			series[i] = data.AssetData{
				Date:   base.AddDate(0, 0, i),
				Open:   px * 0.998,
				High:   px * 1.008,
				Low:    px * 0.992,
				Close:  px,
				Volume: 1_000_000,
			}
		}
		hist[ticker] = series
	}
	return hist
}

// forkFuture returns a copy of hist whose bars up to and including `split` are
// identical and whose bars after it come from a different random path. This is
// the whole trick: a strategy that reads only the past cannot tell the two
// apart before `split`.
func forkFuture(
	hist map[string][]data.AssetData, split int, seed int64,
) map[string][]data.AssetData {
	future := validationHist(len(hist[validationTickers[0]]), seed)
	forked := make(map[string][]data.AssetData, len(hist))

	for ticker, bars := range hist {
		cp := make([]data.AssetData, len(bars))
		copy(cp, bars)
		alt := future[ticker]
		// Continue from the real price at the split so the seam is a plausible
		// path rather than a jump, which a strategy could otherwise detect as
		// an event rather than as a different future.
		scale := 1.0
		if split < len(alt) && alt[split].Close > 0 {
			scale = bars[split].Close / alt[split].Close
		}
		for i := split + 1; i < len(cp); i++ {
			cp[i].Open = alt[i].Open * scale
			cp[i].High = alt[i].High * scale
			cp[i].Low = alt[i].Low * scale
			cp[i].Close = alt[i].Close * scale
			cp[i].Volume = alt[i].Volume
		}
		forked[ticker] = cp
	}
	return forked
}

// runValidation simulates a strategy over the given history and returns the
// finished portfolio, or nil if it could not be built.
func runValidation(
	spec string, params map[string]any,
	hist map[string][]data.AssetData, days int,
) *Portfolio {
	tickers := make([]string, 0, len(hist))
	for _, t := range validationTickers {
		if _, ok := hist[t]; ok {
			tickers = append(tickers, t)
		}
	}
	if len(tickers) == 0 || days < 1 {
		return nil
	}

	p, err := InitializePortfolio(
		validationCash,
		hist[tickers[0]][0].Date, hist[tickers[0]][days-1].Date,
		"validate", tickers, spec, params,
	)
	if err != nil {
		return nil
	}
	defer func() {
		if c, ok := p.Strategy.(interface{ Close() }); ok {
			c.Close()
		}
	}()

	p.Strategy.Step(p, hist, 0)
	prev := p.GetPortfolioValue(p.Tickers, hist, 0)
	for day := 1; day < days; day++ {
		p.Strategy.Step(p, hist, day)
		curr := p.GetPortfolioValue(p.Tickers, hist, day)
		p.AdjustPortfolioParameters(p.Tickers, hist, day, prev, curr)
		prev = curr
	}
	return p
}

// checkLookAhead is the detector described at the top of the file.
func checkLookAhead(spec string, params map[string]any) Finding {
	f := Finding{Check: "look-ahead", Fatal: true}

	real := validationHist(validationDays, validationSeed)
	base := runValidation(spec, params, real, validationDays)
	if base == nil {
		f.Detail = "could not run the strategy"
		f.Failed = true
		return f
	}

	compared := 0
	for i, split := range validationSplits {
		forked := forkFuture(real, split, validationSeed+int64(i)+1)
		alt := runValidation(spec, params, forked, validationDays)
		if alt == nil {
			f.Detail = "could not run the strategy on a forked history"
			f.Failed = true
			return f
		}

		// Only trades placed on or before the fork can be compared: after it
		// the histories genuinely differ and the strategies SHOULD diverge.
		cutoff := real[validationTickers[0]][split].Date
		want := tradesUpTo(base.Trades, cutoff)
		got := tradesUpTo(alt.Trades, cutoff)
		compared += len(want)

		if len(want) != len(got) {
			f.Failed = true
			f.Detail = fmt.Sprintf(
				"placed %d trades up to day %d on the real history and %d on "+
					"one IDENTICAL up to that day — the strategy is reading "+
					"the future", len(want), split, len(got))
			return f
		}
		for j := range want {
			if !sameTrade(want[j], got[j]) {
				f.Failed = true
				f.Detail = fmt.Sprintf(
					"trade %d differs on or before day %d (%s %s %.4f @ %.4f "+
						"vs %s %s %.4f @ %.4f) on histories identical up to "+
						"that day — the strategy is reading the future",
					j+1, split,
					want[j].Side, want[j].Ticker, want[j].Shares, want[j].Price,
					got[j].Side, got[j].Ticker, got[j].Shares, got[j].Price)
				return f
			}
		}
	}

	f.Detail = fmt.Sprintf(
		"identical behaviour across %d forked futures (%d trade comparisons)",
		len(validationSplits), compared)
	return f
}

func tradesUpTo(trades []Trade, cutoff time.Time) []Trade {
	var out []Trade
	for _, t := range trades {
		if !t.Date.After(cutoff) {
			out = append(out, t)
		}
	}
	return out
}

func sameTrade(a, b Trade) bool {
	return a.Ticker == b.Ticker && a.Side == b.Side &&
		math.Abs(a.Shares-b.Shares) < 1e-9 &&
		math.Abs(a.Price-b.Price) < 1e-9 &&
		a.Date.Equal(b.Date)
}

// checkTraded reports a strategy that never trades. Zero trades is not a 0%
// return, it is the absence of a strategy — and it is exactly what the T13
// slippage bug produces, which is why it needs saying out loud.
func checkTraded(spec string, params map[string]any) Finding {
	f := Finding{Check: "placed trades", Fatal: true}
	p := runValidation(spec, params,
		validationHist(validationDays, validationSeed), validationDays)
	if p == nil {
		f.Failed = true
		f.Detail = "could not run the strategy"
		return f
	}
	if len(p.Trades) == 0 {
		f.Failed = true
		f.Detail = "placed no trades at all over 400 days — a strategy that " +
			"never trades has no result to report"
		return f
	}
	f.Detail = fmt.Sprintf("%d fills over %d days", len(p.Trades), validationDays)
	return f
}

// checkCashConservation verifies that the money is all still accounted for.
//
// Two ways it can leak, both silent. A position under a ticker outside
// p.Tickers is never valued by GetPortfolioValue, so the cash that bought it
// simply disappears from the equity curve — that is T9, and a typo'd symbol is
// enough to cause it. And the reported final value must equal cash plus the
// marked-to-market book; anything else means the two halves of the accounting
// disagree.
func checkCashConservation(spec string, params map[string]any) Finding {
	f := Finding{Check: "cash conserved", Fatal: true}
	hist := validationHist(validationDays, validationSeed)
	p := runValidation(spec, params, hist, validationDays)
	if p == nil {
		f.Failed = true
		f.Detail = "could not run the strategy"
		return f
	}

	known := make(map[string]bool, len(p.Tickers))
	for _, t := range p.Tickers {
		known[t] = true
	}
	for ticker, pos := range p.Positions {
		if pos == nil || pos.Amount == 0 {
			continue
		}
		if !known[ticker] {
			f.Failed = true
			f.Detail = fmt.Sprintf(
				"holds %.4f shares of %q, which is not in the portfolio's "+
					"ticker list — that position is never valued, so the cash "+
					"that bought it has vanished from the equity curve",
				pos.Amount, ticker)
			return f
		}
	}

	last := validationDays - 1
	var book float64
	for _, ticker := range p.Tickers {
		if pos, ok := p.Positions[ticker]; ok && pos != nil {
			book += pos.Amount * hist[ticker][last].Close
		}
	}
	want := p.BuyingPower + book
	got := p.PortfolioCloseValues[len(p.PortfolioCloseValues)-1]
	if diff := math.Abs(want - got); diff > 1e-6*math.Max(1, math.Abs(want)) {
		f.Failed = true
		f.Detail = fmt.Sprintf(
			"final value %.6f but cash plus book is %.6f (off by %.6f)",
			got, want, diff)
		return f
	}

	f.Detail = fmt.Sprintf("final value matches cash plus book (%.2f)", got)
	return f
}

// degenerateCases are the price paths that break naive arithmetic: a flat
// series (zero volatility, zero denominator), a series that only falls, one
// that never moves off its first bar, and a single bar with no history at all.
func degenerateCases() map[string][]float64 {
	flat := make([]float64, 120)
	falling := make([]float64, 120)
	px := 100.0
	for i := range flat {
		flat[i] = 100
		falling[i] = px
		px *= 0.98
	}
	return map[string][]float64{
		"flat prices":  flat,
		"only falls":   falling,
		"a single bar": {100},
		"two bars":     {100, 100},
	}
}

// checkDegenerateInputs runs the strategy against paths that break naive
// arithmetic. It must not panic and must not produce NaN metrics; producing no
// trades on a degenerate path is fine.
func checkDegenerateInputs(spec string, params map[string]any) Finding {
	f := Finding{Check: "degenerate inputs"}
	var broke []string

	for name, closes := range degenerateCases() {
		hist := make(map[string][]data.AssetData, len(validationTickers))
		base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		for _, ticker := range validationTickers {
			series := make([]data.AssetData, len(closes))
			for i, c := range closes {
				series[i] = data.AssetData{
					Date: base.AddDate(0, 0, i), Open: c, High: c, Low: c,
					Close: c, Volume: 1000,
				}
			}
			hist[ticker] = series
		}

		if bad := runDegenerate(spec, params, hist, len(closes)); bad != "" {
			broke = append(broke, fmt.Sprintf("%s: %s", name, bad))
		}
	}

	if len(broke) > 0 {
		f.Failed = true
		f.Detail = strings.Join(broke, "; ")
		return f
	}
	f.Detail = fmt.Sprintf("survived %d degenerate paths", len(degenerateCases()))
	return f
}

// runDegenerate runs one degenerate case and returns a description of what went
// wrong, or "" when nothing did. A panic is caught and reported rather than
// taking the process down: a validator that dies on a bad candidate is useless
// for exactly the candidates it exists to judge.
func runDegenerate(
	spec string, params map[string]any,
	hist map[string][]data.AssetData, days int,
) (problem string) {
	defer func() {
		if r := recover(); r != nil {
			problem = fmt.Sprintf("panicked (%v)", r)
		}
	}()

	p := runValidation(spec, params, hist, days)
	if p == nil {
		return "could not run"
	}
	for _, v := range p.PortfolioCloseValues {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return "produced a NaN or infinite portfolio value"
		}
	}
	if math.IsNaN(p.BuyingPower) || math.IsInf(p.BuyingPower, 0) {
		return "produced a NaN or infinite cash balance"
	}
	return ""
}
