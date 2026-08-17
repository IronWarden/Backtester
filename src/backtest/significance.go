package backtest

// Is THIS result better than chance?
//
// The deflated Sharpe (T17) corrects the best of N trials for the size of the
// search. It says nothing about a single strategy run once, which is the
// question a user actually asks first: a Sharpe of 1.4 over ten years might be
// skill, or might be what a coin flip looks like on this particular price path.
//
// The null model here is RANDOM TIMING. The strategy's daily exposure — how much
// of the book was invested each day — is shuffled in time and applied to the
// market's actual returns. That keeps everything except the thing being tested:
// the same amount of market exposure, the same price path, the same number of
// days invested, and only WHEN it happened is destroyed. If the strategy's
// Sharpe sits inside the distribution of Sharpes that random timing produces,
// the timing added nothing.
//
// Why not permute the strategy's own returns: the Sharpe ratio is
// order-independent, so shuffling its return series leaves the Sharpe exactly
// unchanged and the test would report p = 1 for everything. That is a real trap
// and worth stating, because "permute the returns" is the obvious first idea.
//
// The second output is a block bootstrap on the strategy's own returns, which
// answers a different question: not "is it real" but "how precise is this
// number". Twenty-day blocks preserve the autocorrelation that daily resampling
// would destroy.
//
// Both are seeded and deterministic. An unreproducible p-value is worse than
// none, because it cannot be checked.

import (
	"math"
	"math/rand"
	"my-backtester/src/data"
	"sort"
)

const (
	// SignificanceDraws is how many random-timing strategies each result is
	// compared against. A thousand gives a p-value with a resolution of 0.001,
	// which is finer than the question deserves, and costs a few million
	// floating-point operations — far less than the run itself.
	SignificanceDraws = 1000

	// SignificanceSeed fixes the draws. Deterministic on purpose: the same run
	// must produce the same p-value tomorrow, or nobody can check it.
	SignificanceSeed = 20260817

	// tieTolerance is the relative slack in the "at least as good" comparison.
	// See the note at its use: without it, floating-point noise decides
	// p-values.
	tieTolerance = 1e-9

	// BootstrapBlockDays is the resampling block length. Daily resampling would
	// destroy the autocorrelation in a real return series and give a
	// confidence interval that is too narrow; a month of trading days keeps it.
	BootstrapBlockDays = 20
)

// Significance is the verdict on one result, against chance.
type Significance struct {
	// Draws is how many random-timing strategies were compared, and Seed the
	// value that makes them reproducible.
	Draws int   `json:"draws"`
	Seed  int64 `json:"seed"`
	// Sharpe is the strategy's own, repeated here so the comparison is legible
	// on its own.
	Sharpe float64 `json:"sharpe"`
	// PValue is the share of random-timing strategies that matched or beat the
	// strategy's Sharpe. Small means the timing did something; 0.5 means the
	// strategy is indistinguishable from being invested at random.
	PValue float64 `json:"pValue"`
	// Percentile is where the strategy sits in the null distribution, 0-100.
	Percentile float64 `json:"percentile"`
	// NullMeanSharpe and NullP95Sharpe describe the null itself, so a reader
	// can see what "chance" looked like rather than trusting a p-value alone.
	NullMeanSharpe float64 `json:"nullMeanSharpe"`
	NullP95Sharpe  float64 `json:"nullP95Sharpe"`
	// BootstrapLow and BootstrapHigh are the 5th and 95th percentiles of the
	// strategy's own Sharpe under a block bootstrap: how precise the figure is,
	// which is a different question from whether it is real.
	BootstrapLow  float64 `json:"bootstrapLow"`
	BootstrapHigh float64 `json:"bootstrapHigh"`
	// Computed is false when there was not enough to test — too few days, or a
	// strategy with no exposure at all.
	Computed bool `json:"computed"`
}

// MinSignificanceDays is the shortest return series worth testing. Below this
// the null distribution is so wide that every strategy is "not significant",
// which is true but useless.
const MinSignificanceDays = 60

// TestSignificance compares a strategy's returns against random timing.
//
// strategyReturns and exposure must be 1:1 and aligned; marketReturns is the
// return of the universe on the same days — the thing the exposure was applied
// to. Pure and seeded, so it can be tested without a run.
func TestSignificance(
	strategyReturns, exposure, marketReturns []float64,
) Significance {
	sig := Significance{Draws: SignificanceDraws, Seed: SignificanceSeed}
	n := len(strategyReturns)
	if n < MinSignificanceDays || len(exposure) != n || len(marketReturns) != n {
		return sig
	}

	// A strategy that was never invested has no timing to test.
	var exposed float64
	for _, e := range exposure {
		exposed += math.Abs(e)
	}
	if exposed <= 0 {
		return sig
	}

	sig.Sharpe = GetSharpeRatio(strategyReturns)

	rng := rand.New(rand.NewSource(SignificanceSeed))
	shuffled := make([]float64, n)
	synthetic := make([]float64, n)
	nullSharpes := make([]float64, 0, SignificanceDraws)
	var atLeastAsGood int

	for d := 0; d < SignificanceDraws; d++ {
		copy(shuffled, exposure)
		rng.Shuffle(n, func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		for i := range synthetic {
			synthetic[i] = shuffled[i] * marketReturns[i]
		}
		s := GetSharpeRatio(synthetic)
		nullSharpes = append(nullSharpes, s)
		// Ties count as "at least as good", with a tolerance, because a null
		// draw differing only in the last bits IS the same result. Letting
		// float noise decide a p-value is not hypothetical: a fully invested
		// buy-and-hold reproduces its own null exactly, and a strict
		// comparison reported p = 0 for it — maximum significance for a
		// strategy with no timing at all. Counting near-ties raises the
		// p-value and never lowers it, which is the conservative direction.
		if s >= sig.Sharpe-tieTolerance*math.Max(1, math.Abs(sig.Sharpe)) {
			atLeastAsGood++
		}
	}

	sig.PValue = float64(atLeastAsGood) / float64(SignificanceDraws)
	sig.Percentile = (1 - sig.PValue) * 100
	sig.NullMeanSharpe = mean(nullSharpes)
	sig.NullP95Sharpe = percentile(nullSharpes, 95)
	sig.BootstrapLow, sig.BootstrapHigh = bootstrapSharpe(
		strategyReturns, BootstrapBlockDays, rng)
	sig.Computed = true
	return sig
}

// bootstrapSharpe resamples the return series in blocks and returns the 5th and
// 95th percentiles of the resulting Sharpes.
func bootstrapSharpe(
	returns []float64, block int, rng *rand.Rand,
) (low, high float64) {
	n := len(returns)
	if block < 1 || n < block*2 {
		return 0, 0
	}
	sharpes := make([]float64, 0, SignificanceDraws)
	sample := make([]float64, 0, n)

	for d := 0; d < SignificanceDraws; d++ {
		sample = sample[:0]
		for len(sample) < n {
			start := rng.Intn(n - block)
			sample = append(sample, returns[start:start+block]...)
		}
		sharpes = append(sharpes, GetSharpeRatio(sample[:n]))
	}
	return percentile(sharpes, 5), percentile(sharpes, 95)
}

// percentile is the nearest-rank percentile of a sample. It sorts a copy, so
// the caller's slice keeps its order.
func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sorted := append([]float64(nil), xs...)
	sort.Float64s(sorted)
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// universeDailyReturns is the equal-weight daily return of the traded universe —
// the market a random-timing null is applied to.
//
// It starts at day 1 to line up with DailyReturns and Exposure, both of which
// are recorded from the first day that HAS a previous day to compare against. A
// day where no ticker has a usable pair of prices contributes zero rather than
// dropping out, so the three series stay 1:1 and a hole cannot silently shift
// the exposure against the market by one day.
func universeDailyReturns(
	windowed map[string][]data.AssetData, tickers []string, dataLen int,
) []float64 {
	if dataLen < 2 {
		return nil
	}
	out := make([]float64, 0, dataLen-1)
	for day := 1; day < dataLen; day++ {
		var sum float64
		var n int
		for _, ticker := range tickers {
			bars := windowed[ticker]
			if day >= len(bars) || bars[day-1].Close <= 0 {
				continue
			}
			sum += bars[day].Close/bars[day-1].Close - 1
			n++
		}
		if n == 0 {
			out = append(out, 0)
			continue
		}
		out = append(out, sum/float64(n))
	}
	return out
}

// applySignificance fills in the test from a finished run.
func (p *Portfolio) applySignificance(
	windowed map[string][]data.AssetData, dataLen int,
) {
	returns := make([]float64, 0, len(p.DailyReturns))
	for _, dr := range p.DailyReturns {
		returns = append(returns, dr.Return)
	}
	market := universeDailyReturns(windowed, p.Tickers, dataLen)
	p.Significance = TestSignificance(returns, p.Exposure, market)
}
