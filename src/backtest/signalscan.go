package backtest

// Screening signals before writing a strategy around them.
//
// A backtest is an expensive way to learn that a signal contains no
// information. The standard research funnel is cheap first: ask whether the
// signal ranks tomorrow's winners above tomorrow's losers, across the whole
// universe, and only then build a strategy around the ones that do. Everything
// in T15-T19 makes *testing* a candidate rigorous; this is what makes *finding*
// one tractable.
//
// The measurement is the information coefficient: the cross-sectional Spearman
// rank correlation between a signal on day t and the return from t to t+h. Rank
// correlation rather than Pearson because a strategy acts on the ordering, not
// on the magnitude, and because one outlier should not decide the verdict.
//
// Three things here exist to stop the scan flattering a worthless signal, and
// they matter more than the statistic itself:
//
//   - **Non-overlapping samples.** Scoring every day with a 21-day forward
//     return reuses each day's return 21 times, which makes the IC series
//     autocorrelated and inflates the t-statistic by roughly sqrt(h). Sampling
//     every h-th day is the cheapest honest fix.
//   - **A minimum cross-section.** Ranking three tickers is not a
//     cross-section. Days with too few usable values are skipped, and the
//     quintile spread is withheld entirely below a wider universe.
//   - **No look-ahead, structurally.** A Signal is handed the bars and an index
//     and may read only up to that index. It never receives the forward window
//     it is being scored against.

import (
	"fmt"
	"math"
	"my-backtester/src/data"
	"sort"
	"strings"
)

// Sampling and cross-section limits. These are judgement calls, named so they
// can be argued with rather than found by reading the loops.
const (
	// MinCrossSection is the fewest tickers that can produce a meaningful rank
	// correlation on one day. Below this the day is skipped.
	MinCrossSection = 5

	// MinQuintileUniverse is the fewest tickers worth splitting into fifths.
	// Below it the quintile spread is not reported at all: with eight names a
	// "quintile" is one or two of them, and the spread is one stock's return
	// wearing a portfolio's clothes.
	MinQuintileUniverse = 10

	// MinSamples is the fewest sampled days a (signal, horizon) pair needs
	// before its summary means anything.
	MinSamples = 12
)

// Signal computes one number per ticker per day from that ticker's own bars.
//
// The contract is the whole point: Compute may read bars[0..i] and nothing
// past i. Returning ok=false means the signal is undefined there — too little
// history, a zero denominator — and the ticker is left out of that day's
// cross-section rather than given a fabricated value.
type Signal struct {
	Name string
	// Description is what the signal claims, in one line, for the report.
	Description string
	Compute     func(bars []data.AssetData, i int) (float64, bool)
}

// SignalScore summarises one (signal, horizon) pair.
type SignalScore struct {
	Signal  string
	Horizon int
	// MeanIC is the average daily cross-sectional rank correlation, and StdErr
	// its standard error across sampled days. TStat is MeanIC/StdErr: as a rule
	// of thumb |t| above 2 is worth a second look and an IC of 0.03 with a
	// large t is a normal, useful signal — ICs are small in real data.
	MeanIC float64
	StdErr float64
	TStat  float64
	// HitRate is the share of sampled days whose IC was positive. A signal with
	// a decent mean IC driven by two spectacular days has a hit rate near 0.5
	// and is not a signal.
	HitRate float64
	// QuintileSpread is the mean forward return of the top fifth minus the
	// bottom fifth, averaged over sampled days — the IC translated into money.
	// Zero and QuintileOK false when the universe was too small to split.
	QuintileSpread float64
	QuintileOK     bool
	// Samples is the number of non-overlapping days scored, and Skipped the
	// number rejected for too small a cross-section.
	Samples int
	Skipped int
}

// TStatDefined reports whether TStat means anything. It does not when the IC
// series has no dispersion at all: the ratio is then 0/0, and reporting a
// t-statistic of zero would read as "no evidence" for what is actually the
// strongest possible evidence. Real market data always disperses; this is the
// degenerate case, and it must not be silently mislabelled.
func (s SignalScore) TStatDefined() bool { return s.StdErr > 0 }

// strength is the sort key: weight of evidence. |t| where it exists, and a
// zero-dispersion non-zero IC ranks above everything finite, because an IC that
// never varies is as strong as evidence gets.
func (s SignalScore) strength() float64 {
	if !s.TStatDefined() {
		if s.MeanIC != 0 {
			return math.Inf(1)
		}
		return 0
	}
	return math.Abs(s.TStat)
}

// String renders one row of the report.
func (s SignalScore) String() string {
	spread := "     n/a"
	if s.QuintileOK {
		spread = fmt.Sprintf("%7.3f%%", s.QuintileSpread*100)
	}
	tstat := "   n/a"
	if s.TStatDefined() {
		tstat = fmt.Sprintf("%+6.2f", s.TStat)
	}
	return fmt.Sprintf("%-18s %4dd  IC %+7.4f  t %s  hit %5.1f%%  "+
		"Q5-Q1 %s  n=%d", s.Signal, s.Horizon, s.MeanIC, tstat,
		s.HitRate*100, spread, s.Samples)
}

// ScanReport renders a whole scan as a printable block, including the caveats
// that decide how the numbers should be read. The header is not decoration: an
// IC of 0.03 is a normal, useful signal and looks like nothing to anyone
// expecting a correlation, and a t-statistic from overlapping windows is the
// classic way a screen lies.
func ScanReport(scores []SignalScore, tickers []string, days int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Signal scan: %d tickers, %d trading days\n",
		len(tickers), days)
	b.WriteString(
		"IC is the cross-sectional Spearman rank correlation between the " +
			"signal and\nthe forward return, averaged over NON-OVERLAPPING " +
			"samples. Real ICs are\nsmall: 0.02-0.05 with |t| > 2 is a " +
			"usable signal. A high IC with a hit rate\nnear 50% is a few " +
			"lucky days, not an edge.\n\n")

	if len(scores) == 0 {
		b.WriteString("No signal produced a scoreable result. Likely causes: " +
			"fewer than " + fmt.Sprint(MinCrossSection) + " tickers, or a " +
			"window too short for the horizons.\n")
		return b.String()
	}

	for _, s := range scores {
		b.WriteString(s.String())
		b.WriteString("\n")
	}
	b.WriteString("\nThis is a screen, not a backtest: it ignores costs, " +
		"position sizing and\nwhen you could actually trade. A signal that " +
		"survives here still has to survive\na real run with costs, a " +
		"split (T16) and walk-forward (T19).\n")
	return b.String()
}

// spearman returns the rank correlation between two equal-length series, and
// false when it is undefined — fewer than two points, or either series
// constant, which is the case a Pearson correlation on ranks silently turns
// into a division by zero and hence a NaN.
//
// Ties take the average rank, which is what makes this a proper Spearman rather
// than a correlation of arbitrary orderings.
func spearman(a, b []float64) (float64, bool) {
	if len(a) != len(b) || len(a) < 2 {
		return 0, false
	}
	ra, oka := ranks(a)
	rb, okb := ranks(b)
	if !oka || !okb {
		return 0, false
	}

	var meanA, meanB float64
	for i := range ra {
		meanA += ra[i]
		meanB += rb[i]
	}
	n := float64(len(ra))
	meanA /= n
	meanB /= n

	var cov, varA, varB float64
	for i := range ra {
		da, db := ra[i]-meanA, rb[i]-meanB
		cov += da * db
		varA += da * da
		varB += db * db
	}
	if varA <= 0 || varB <= 0 {
		return 0, false
	}
	return cov / math.Sqrt(varA*varB), true
}

// ranks converts values to average ranks, reporting false when every value is
// identical — a constant series has no ordering to correlate.
func ranks(values []float64) ([]float64, bool) {
	idx := make([]int, len(values))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(i, j int) bool {
		return values[idx[i]] < values[idx[j]]
	})

	out := make([]float64, len(values))
	for i := 0; i < len(idx); {
		j := i
		for j+1 < len(idx) && values[idx[j+1]] == values[idx[i]] {
			j++
		}
		// Ranks are 1-based; a tied group shares the average of its positions.
		avg := (float64(i+1) + float64(j+1)) / 2
		for k := i; k <= j; k++ {
			out[idx[k]] = avg
		}
		i = j + 1
	}

	for _, v := range values[1:] {
		if v != values[0] {
			return out, true
		}
	}
	return out, false
}

// forwardReturn is the simple return from day i to day i+h, and false when the
// window runs past the data or starts from a non-positive price.
func forwardReturn(bars []data.AssetData, i, h int) (float64, bool) {
	if i < 0 || h <= 0 || i+h >= len(bars) {
		return 0, false
	}
	start, end := bars[i].Close, bars[i+h].Close
	if start <= 0 {
		return 0, false
	}
	return end/start - 1, true
}

// ScanSignals scores every signal at every horizon over the given history.
//
// hist is the same prefetched map the runner uses, so a scan costs one query and
// no new data access. Only the tickers listed are considered, and days are taken
// from the first ticker's bars — the same convention alignToWindow uses.
//
// Results come back sorted by absolute t-statistic, strongest first, because the
// question a user is asking is "which of these is worth my afternoon".
func ScanSignals(
	hist map[string][]data.AssetData,
	tickers []string,
	signals []Signal,
	horizons []int,
) []SignalScore {
	var out []SignalScore
	for _, sig := range signals {
		for _, h := range horizons {
			if score, ok := scanOne(hist, tickers, sig, h); ok {
				out = append(out, score)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].strength() > out[j].strength()
	})
	return out
}

// scanOne scores a single (signal, horizon) pair. Returns false when there were
// too few usable days to summarise.
func scanOne(
	hist map[string][]data.AssetData,
	tickers []string,
	sig Signal,
	h int,
) (SignalScore, bool) {
	score := SignalScore{Signal: sig.Name, Horizon: h}
	if len(tickers) == 0 || h <= 0 {
		return score, false
	}
	days := len(hist[tickers[0]])

	var ics, spreads []float64
	// Step by h, not by 1: overlapping forward windows reuse each day's return
	// h times, which inflates the t-statistic by roughly sqrt(h).
	for t := 0; t+h < days; t += h {
		var values, forwards []float64
		for _, ticker := range tickers {
			bars := hist[ticker]
			if t >= len(bars) {
				continue
			}
			v, ok := sig.Compute(bars, t)
			if !ok || math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			fwd, ok := forwardReturn(bars, t, h)
			if !ok {
				continue
			}
			values = append(values, v)
			forwards = append(forwards, fwd)
		}

		if len(values) < MinCrossSection {
			score.Skipped++
			continue
		}
		ic, ok := spearman(values, forwards)
		if !ok {
			score.Skipped++
			continue
		}
		ics = append(ics, ic)
		if len(values) >= MinQuintileUniverse {
			spreads = append(spreads, quintileSpread(values, forwards))
		}
	}

	if len(ics) < MinSamples {
		return score, false
	}

	score.Samples = len(ics)
	score.MeanIC = mean(ics)
	score.StdErr = stdErr(ics)
	if score.StdErr > 0 {
		score.TStat = score.MeanIC / score.StdErr
	}
	var positive int
	for _, ic := range ics {
		if ic > 0 {
			positive++
		}
	}
	score.HitRate = float64(positive) / float64(len(ics))
	if len(spreads) >= MinSamples {
		score.QuintileSpread = mean(spreads)
		score.QuintileOK = true
	}
	return score, true
}

// quintileSpread is the mean forward return of the top fifth by signal value
// minus that of the bottom fifth: the IC expressed as money rather than as a
// correlation. Callers must check MinQuintileUniverse first.
func quintileSpread(values, forwards []float64) float64 {
	idx := make([]int, len(values))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(i, j int) bool {
		return values[idx[i]] < values[idx[j]]
	})

	// At least one name per bucket, and never more than a fifth.
	size := len(idx) / 5
	if size < 1 {
		size = 1
	}
	var low, high float64
	for k := 0; k < size; k++ {
		low += forwards[idx[k]]
		high += forwards[idx[len(idx)-1-k]]
	}
	return (high - low) / float64(size)
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

// stdErr is the standard error of the mean, with the sample (n-1) denominator.
// Zero for fewer than two points, which keeps a one-sample scan from reporting
// an infinite t-statistic.
func stdErr(xs []float64) float64 {
	if len(xs) < 2 {
		return 0
	}
	m := mean(xs)
	var ss float64
	for _, x := range xs {
		ss += (x - m) * (x - m)
	}
	variance := ss / float64(len(xs)-1)
	return math.Sqrt(variance / float64(len(xs)))
}

// ScanConfigPortfolios screens the built-in signal library over the union of the
// given portfolios' tickers, across the widest window any of them asks for, and
// returns the printable report.
//
// It reuses the runner's own prefetch, so a scan is one query and shares the
// alignment conventions of a real run. Deliberately a separate entry point
// rather than a flag inside Run: a screen is not a backtest and must not be
// mistakable for one in the output.
func ScanConfigPortfolios(portfolios []*Portfolio, horizons []int) string {
	if len(portfolios) == 0 {
		return "No portfolios in the config, so there is no universe to scan.\n"
	}
	if len(horizons) == 0 {
		horizons = DefaultHorizons
	}

	seen := map[string]bool{}
	var tickers []string
	for _, p := range portfolios {
		for _, t := range p.Tickers {
			if !seen[t] {
				seen[t] = true
				tickers = append(tickers, t)
			}
		}
	}
	sort.Strings(tickers)

	start, end := dateRange(portfolios)
	hist := data.QueryAssetsForTickers(tickers, start, end)

	// Align on the union of days each ticker actually has, using the longest
	// series as the day axis — a scan should not be truncated by one short
	// history the way a portfolio simulation is (see the coverage rule in
	// runner.go).
	longest := 0
	for _, t := range tickers {
		if n := len(hist[t]); n > longest {
			longest = n
		}
	}
	ordered := make([]string, 0, len(tickers))
	for _, t := range tickers {
		if len(hist[t]) == longest {
			ordered = append(ordered, t)
		}
	}
	ordered = append(ordered, func() []string {
		var rest []string
		for _, t := range tickers {
			if len(hist[t]) != longest {
				rest = append(rest, t)
			}
		}
		return rest
	}()...)

	scores := ScanSignals(hist, ordered, BuiltinSignals(), horizons)
	return ScanReport(scores, ordered, longest)
}

// --- the built-in signal library ------------------------------------------
//
// Each covers one well-known axis of cross-sectional prediction. They are
// deliberately plain: the point of a screen is to find which axis has
// information in this universe over this window, not to arrive with a finished
// strategy.

// trailingReturn is the return over the n days ending at i, skipping the most
// recent `skip` days — `skip` is what separates classic 12-1 momentum (which
// ignores the last month, where short-term reversal dominates) from plain
// trailing return.
func trailingReturn(bars []data.AssetData, i, n, skip int) (float64, bool) {
	end := i - skip
	start := end - n
	if start < 0 || end < 0 || end >= len(bars) {
		return 0, false
	}
	if bars[start].Close <= 0 {
		return 0, false
	}
	return bars[end].Close/bars[start].Close - 1, true
}

// realisedVol is the standard deviation of the last n daily returns ending at i.
func realisedVol(bars []data.AssetData, i, n int) (float64, bool) {
	if n < 2 || i-n < 0 {
		return 0, false
	}
	rets := make([]float64, 0, n)
	for k := i - n + 1; k <= i; k++ {
		if bars[k-1].Close <= 0 {
			return 0, false
		}
		rets = append(rets, bars[k].Close/bars[k-1].Close-1)
	}
	m := mean(rets)
	var ss float64
	for _, r := range rets {
		ss += (r - m) * (r - m)
	}
	return math.Sqrt(ss / float64(len(rets)-1)), true
}

// BuiltinSignals is the starting library. Signs are chosen so that a POSITIVE
// information coefficient means "high signal value predicted high return",
// which is why volatility is negated: the low-volatility anomaly says calm
// stocks do better, and reporting that as a negative IC would read as a broken
// signal rather than a known effect.
func BuiltinSignals() []Signal {
	sig := func(name, desc string, f func([]data.AssetData, int) (float64, bool)) Signal {
		return Signal{Name: name, Description: desc, Compute: f}
	}
	return []Signal{
		sig("mom_21", "1-month trailing return",
			func(b []data.AssetData, i int) (float64, bool) {
				return trailingReturn(b, i, 21, 0)
			}),
		sig("mom_63", "3-month trailing return",
			func(b []data.AssetData, i int) (float64, bool) {
				return trailingReturn(b, i, 63, 0)
			}),
		sig("mom_126", "6-month trailing return",
			func(b []data.AssetData, i int) (float64, bool) {
				return trailingReturn(b, i, 126, 0)
			}),
		sig("mom_252", "12-month trailing return",
			func(b []data.AssetData, i int) (float64, bool) {
				return trailingReturn(b, i, 252, 0)
			}),
		sig("mom_12_1", "12-month return skipping the last month",
			func(b []data.AssetData, i int) (float64, bool) {
				return trailingReturn(b, i, 231, 21)
			}),
		sig("rev_5", "negated 1-week return (short-term reversal)",
			func(b []data.AssetData, i int) (float64, bool) {
				r, ok := trailingReturn(b, i, 5, 0)
				return -r, ok
			}),
		sig("lowvol_21", "negated 1-month realised volatility",
			func(b []data.AssetData, i int) (float64, bool) {
				v, ok := realisedVol(b, i, 21)
				return -v, ok
			}),
		sig("lowvol_63", "negated 3-month realised volatility",
			func(b []data.AssetData, i int) (float64, bool) {
				v, ok := realisedVol(b, i, 63)
				return -v, ok
			}),
		sig("dist_52w_high", "closeness to the 252-day high (0 = at the high)",
			func(b []data.AssetData, i int) (float64, bool) {
				if i-252 < 0 {
					return 0, false
				}
				high := 0.0
				for k := i - 251; k <= i; k++ {
					if b[k].High > high {
						high = b[k].High
					}
				}
				if high <= 0 || b[i].Close <= 0 {
					return 0, false
				}
				return b[i].Close/high - 1, true
			}),
		sig("sma_50_200", "50-day SMA over 200-day SMA, minus one",
			func(b []data.AssetData, i int) (float64, bool) {
				if i-200 < 0 {
					return 0, false
				}
				var fast, slow float64
				for k := i - 49; k <= i; k++ {
					fast += b[k].Close
				}
				for k := i - 199; k <= i; k++ {
					slow += b[k].Close
				}
				fast /= 50
				slow /= 200
				if slow <= 0 {
					return 0, false
				}
				return fast/slow - 1, true
			}),
		sig("vol_trend_21", "1-month average volume over the 3-month average",
			func(b []data.AssetData, i int) (float64, bool) {
				if i-63 < 0 {
					return 0, false
				}
				var short, long float64
				for k := i - 20; k <= i; k++ {
					short += b[k].Volume
				}
				for k := i - 62; k <= i; k++ {
					long += b[k].Volume
				}
				short /= 21
				long /= 63
				if long <= 0 {
					return 0, false
				}
				return short/long - 1, true
			}),
		sig("vol_scaled_mom", "3-month return divided by 3-month volatility",
			func(b []data.AssetData, i int) (float64, bool) {
				r, ok := trailingReturn(b, i, 63, 0)
				if !ok {
					return 0, false
				}
				v, ok := realisedVol(b, i, 63)
				if !ok || v <= 0 {
					return 0, false
				}
				return r / v, true
			}),
	}
}

// DefaultHorizons are the forward windows worth screening: a week, a month, a
// quarter. Shorter than a week is noise at daily resolution, and longer than a
// quarter leaves too few non-overlapping samples in a decade of data.
var DefaultHorizons = []int{5, 21, 63}
