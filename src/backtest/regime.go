package backtest

// Which environment did this strategy actually work in?
//
// A single Sharpe blends every market the window contained. A strategy that made
// all its money in one calm bull stretch and gave some back whenever volatility
// arrived reports the same headline as one that worked throughout, and the
// difference is the whole question when deciding whether to trade it now.
//
// Labels come from the traded universe's own price history: how deep it is below
// its running peak, and whether its recent volatility is high or low against its
// own past. Both are computed from data up to and including the labelled day —
// causal by construction, so no lag discipline is needed and no series can leak
// backwards.
//
// That is deliberately the cheap half of the idea. Macro regimes — rate
// direction, the 10y-3m curve, CPI and industrial-production terciles — are the
// richer half and are NOT here, because every macro series is published with a
// lag and revised afterwards: `economic_indicators.Date` is the period, not the
// release. A regime label that peeks is the same bug as a fundamental that
// peeks and considerably harder to notice, so those labels want the same
// publication-lag treatment PointInTimeFundamentals applies, and that is a
// database read inside the run path rather than an arithmetic change here.

import (
	"fmt"
	"math"
	"my-backtester/src/data"
	"strings"
)

// Regime is one environment label for one day.
type Regime string

const (
	// RegimeUnknown covers the warm-up, before there is enough history to say
	// anything. Excluded from the per-regime statistics rather than lumped in
	// with whatever it resembles.
	RegimeUnknown Regime = "warmup"
	// The four combinations of trend and volatility. Named rather than
	// numbered because "bear-volatile" is what a reader wants to see next to a
	// negative number.
	RegimeBullCalm     Regime = "bull-calm"
	RegimeBullVolatile Regime = "bull-volatile"
	RegimeBearCalm     Regime = "bear-calm"
	RegimeBearVolatile Regime = "bear-volatile"
)

// Regime thresholds. Judgement calls, named so they can be argued with.
const (
	// BearDrawdown is how far the universe must sit below its running peak
	// before the environment counts as a bear. 10% is the conventional
	// "correction" line and is deep enough that ordinary noise does not flip
	// the label every week.
	BearDrawdown = -0.10

	// RegimeVolWindow is the trailing window for realised volatility, and
	// RegimeWarmup is how much history is needed before any label is asserted.
	RegimeVolWindow = 21
	RegimeWarmup    = 63
)

// RegimeStats is one environment's slice of a run.
type RegimeStats struct {
	Regime Regime `json:"regime"`
	// Days is how many trading days carried this label, and Share its fraction
	// of the labelled window. A regime holding 3% of the days cannot support a
	// conclusion, and printing the share next to the return is what stops one.
	Days  int     `json:"days"`
	Share float64 `json:"share"`
	// AnnualReturn compounds this regime's daily returns and annualizes over
	// its own DAY COUNT at 252/year — not over the calendar span, since a
	// regime's days are scattered through the window rather than contiguous.
	AnnualReturn float64 `json:"annualReturn"`
	SharpeRatio  float64 `json:"sharpeRatio"`
	// TotalReturn is the compounded return across just these days, which is the
	// honest figure when a regime holds few of them.
	TotalReturn float64 `json:"totalReturn"`
}

// RegimeBreakdown is the whole slicing, ordered by day count so the environment
// the run spent most of its time in comes first.
type RegimeBreakdown struct {
	Stats    []RegimeStats `json:"stats"`
	Computed bool          `json:"computed"`
}

// String renders the breakdown.
func (r RegimeBreakdown) String() string {
	if !r.Computed {
		return "  regimes: not computed"
	}
	var b strings.Builder
	b.WriteString("  by market regime (labels from the traded universe):\n")
	for _, s := range r.Stats {
		thin := ""
		if s.Share < 0.10 {
			thin = "  (too few days to conclude from)"
		}
		fmt.Fprintf(&b,
			"    %-14s %4d days (%4.1f%%)  CAGR %8.2f%%  Sharpe %6.2f%s\n",
			s.Regime, s.Days, s.Share*100, s.AnnualReturn, s.SharpeRatio, thin)
	}
	return b.String()
}

// LabelRegimes assigns one label per day from the universe's daily returns.
//
// Pure, and causal by construction: day i's label uses returns[0..i] and nothing
// after. The two axes are the drawdown of the compounded index from its running
// peak, and whether trailing volatility is above the median of every trailing
// volatility seen so far — an expanding comparison, so it never consults the
// future to decide what "high" means.
func LabelRegimes(returns []float64) []Regime {
	labels := make([]Regime, len(returns))
	if len(returns) == 0 {
		return labels
	}

	index := 1.0
	peak := 1.0
	var vols []float64

	for i, r := range returns {
		index *= 1 + r
		if index > peak {
			peak = index
		}
		labels[i] = RegimeUnknown
		if i < RegimeWarmup {
			continue
		}

		drawdown := index/peak - 1
		window := returns[i-RegimeVolWindow+1 : i+1]
		vol, ok := sampleStdev(window)
		if !ok {
			continue
		}
		vols = append(vols, vol)
		// Compared against the median of every window seen SO FAR, so "high
		// volatility" means high relative to what had already happened rather
		// than relative to the whole run — which would be a peek.
		volatile := vol > median(vols)

		switch {
		case drawdown <= BearDrawdown && volatile:
			labels[i] = RegimeBearVolatile
		case drawdown <= BearDrawdown:
			labels[i] = RegimeBearCalm
		case volatile:
			labels[i] = RegimeBullVolatile
		default:
			labels[i] = RegimeBullCalm
		}
	}
	return labels
}

// SliceByRegime splits a strategy's daily returns by label and scores each
// environment separately. strategyReturns and labels must be 1:1.
func SliceByRegime(strategyReturns []float64, labels []Regime) RegimeBreakdown {
	var breakdown RegimeBreakdown
	if len(strategyReturns) == 0 || len(strategyReturns) != len(labels) {
		return breakdown
	}

	byRegime := make(map[Regime][]float64)
	labelled := 0
	for i, label := range labels {
		if label == RegimeUnknown {
			continue
		}
		byRegime[label] = append(byRegime[label], strategyReturns[i])
		labelled++
	}
	if labelled == 0 {
		return breakdown
	}

	for _, regime := range []Regime{
		RegimeBullCalm, RegimeBullVolatile, RegimeBearCalm, RegimeBearVolatile,
	} {
		rets := byRegime[regime]
		if len(rets) == 0 {
			continue
		}
		growth := 1.0
		for _, r := range rets {
			growth *= 1 + r
		}
		years := float64(len(rets)) / 252.0
		stats := RegimeStats{
			Regime:      regime,
			Days:        len(rets),
			Share:       float64(len(rets)) / float64(labelled),
			TotalReturn: (growth - 1) * 100,
			SharpeRatio: GetSharpeRatio(rets),
		}
		if years > 0 && growth > 0 {
			stats.AnnualReturn = (math.Pow(growth, 1/years) - 1) * 100
		}
		breakdown.Stats = append(breakdown.Stats, stats)
	}

	// Most-inhabited environment first: that is the one the headline number is
	// mostly measuring.
	for i := 1; i < len(breakdown.Stats); i++ {
		for j := i; j > 0 && breakdown.Stats[j].Days > breakdown.Stats[j-1].Days; j-- {
			breakdown.Stats[j], breakdown.Stats[j-1] =
				breakdown.Stats[j-1], breakdown.Stats[j]
		}
	}
	breakdown.Computed = true
	return breakdown
}

// applyRegimes labels the run's days from the universe it traded and slices its
// returns by them. The universe return series is the same one the significance
// test uses, so "the market" means one thing across the whole package.
func (p *Portfolio) applyRegimes(
	windowed map[string][]data.AssetData, dataLen int,
) {
	if dataLen < 2 || len(p.DailyReturns) == 0 {
		return
	}
	market := universeDailyReturns(windowed, p.Tickers, dataLen)
	if len(market) != len(p.DailyReturns) {
		return
	}
	returns := make([]float64, 0, len(p.DailyReturns))
	for _, dr := range p.DailyReturns {
		returns = append(returns, dr.Return)
	}
	p.Regimes = SliceByRegime(returns, LabelRegimes(market))
}
