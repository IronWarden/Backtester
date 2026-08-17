package backtest

// The significance test exists to stop a lucky run reading as skill, so the
// tests are calibration cases: a strategy that is indistinguishable from random
// timing must score p ~ 0.5, and one with genuine timing must score near zero.

import (
	"math"
	"math/rand"
	"testing"
)

// marketPath is a seeded return series with real variation, used as the market
// a random-timing null is applied to.
func marketPath(n int, seed int64) []float64 {
	rng := rand.New(rand.NewSource(seed))
	out := make([]float64, n)
	for i := range out {
		out[i] = rng.NormFloat64() * 0.01
	}
	return out
}

// Constant exposure is the calibration case: shuffling a constant changes
// nothing, so every random-timing draw reproduces the strategy exactly and the
// p-value must be 1. Anything else means the null is not holding exposure
// fixed.
func TestConstantExposureIsIndistinguishableFromRandomTiming(t *testing.T) {
	const n = 500
	market := marketPath(n, 1)
	exposure := make([]float64, n)
	returns := make([]float64, n)
	for i := range exposure {
		exposure[i] = 1.0
		returns[i] = market[i]
	}

	sig := TestSignificance(returns, exposure, market)
	if !sig.Computed {
		t.Fatal("no significance computed for a 500-day series")
	}
	if math.Abs(sig.PValue-1.0) > 1e-9 {
		t.Errorf("p = %v for a fully invested strategy, want 1.0: shuffling a "+
			"constant exposure cannot change anything", sig.PValue)
	}
}

// Perfect timing: invested only on days the market rose. Random timing cannot
// reproduce that, so the p-value must be 0 — the strategy sits outside its own
// null entirely.
func TestPerfectTimingIsSignificant(t *testing.T) {
	const n = 500
	market := marketPath(n, 2)
	exposure := make([]float64, n)
	returns := make([]float64, n)
	for i := range market {
		if market[i] > 0 {
			exposure[i] = 1
		}
		returns[i] = exposure[i] * market[i]
	}

	sig := TestSignificance(returns, exposure, market)
	if !sig.Computed {
		t.Fatal("no significance computed")
	}
	if sig.PValue > 0.01 {
		t.Errorf("p = %v for a strategy invested only on up days, want ~0",
			sig.PValue)
	}
	if sig.Percentile < 99 {
		t.Errorf("percentile = %v, want ~100", sig.Percentile)
	}
	if sig.Sharpe <= sig.NullP95Sharpe {
		t.Errorf("strategy Sharpe %v is not above the null's 95th percentile "+
			"%v", sig.Sharpe, sig.NullP95Sharpe)
	}
}

// A strategy whose exposure is itself random must land in the middle of the
// null: that is what "no timing skill" looks like, and it is the case a broken
// test would most likely get wrong in the flattering direction.
func TestRandomTimingIsNotSignificant(t *testing.T) {
	const n = 800
	market := marketPath(n, 3)
	rng := rand.New(rand.NewSource(77))
	exposure := make([]float64, n)
	returns := make([]float64, n)
	for i := range exposure {
		if rng.Float64() < 0.5 {
			exposure[i] = 1
		}
		returns[i] = exposure[i] * market[i]
	}

	sig := TestSignificance(returns, exposure, market)
	if !sig.Computed {
		t.Fatal("no significance computed")
	}
	if sig.PValue < 0.05 || sig.PValue > 0.95 {
		t.Errorf("p = %v for randomly timed exposure, want the middle of the "+
			"distribution", sig.PValue)
	}
}

// Seeded and deterministic: the same inputs must give the same p-value, or the
// number cannot be checked by anyone.
func TestSignificanceIsDeterministic(t *testing.T) {
	const n = 300
	market := marketPath(n, 4)
	exposure := make([]float64, n)
	returns := make([]float64, n)
	for i := range exposure {
		exposure[i] = float64(i%3) / 2
		returns[i] = exposure[i] * market[i]
	}

	first := TestSignificance(returns, exposure, market)
	second := TestSignificance(returns, exposure, market)
	if first != second {
		t.Errorf("two identical calls disagreed:\n%+v\n%+v", first, second)
	}
	if first.Seed != SignificanceSeed || first.Draws != SignificanceDraws {
		t.Errorf("seed/draws not reported: %+v", first)
	}
}

// The bootstrap interval must bracket the strategy's own Sharpe — it is a
// confidence interval for that number, so a Sharpe outside its own interval
// would mean the resampling is not resampling the right series.
func TestBootstrapBracketsTheSharpe(t *testing.T) {
	const n = 600
	market := marketPath(n, 5)
	exposure := make([]float64, n)
	returns := make([]float64, n)
	for i := range exposure {
		exposure[i] = 1
		returns[i] = market[i] + 0.0004 // a small positive drift
	}

	sig := TestSignificance(returns, exposure, market)
	if sig.BootstrapLow >= sig.BootstrapHigh {
		t.Fatalf("bootstrap interval is inverted: [%v, %v]",
			sig.BootstrapLow, sig.BootstrapHigh)
	}
	if sig.Sharpe < sig.BootstrapLow || sig.Sharpe > sig.BootstrapHigh {
		t.Errorf("Sharpe %v falls outside its own bootstrap interval [%v, %v]",
			sig.Sharpe, sig.BootstrapLow, sig.BootstrapHigh)
	}
}

// Refusals: too few days, mismatched lengths, and a strategy that never
// invested all report nothing rather than a p-value.
func TestSignificanceRefusals(t *testing.T) {
	short := make([]float64, MinSignificanceDays-1)
	if got := TestSignificance(short, short, short); got.Computed {
		t.Errorf("a %d-day series produced a verdict", len(short))
	}

	n := MinSignificanceDays + 10
	ok := make([]float64, n)
	if got := TestSignificance(ok, ok[:n-1], ok); got.Computed {
		t.Error("mismatched series lengths produced a verdict")
	}

	// Never invested: exposure is all zeros, so there is no timing to test.
	market := marketPath(n, 6)
	flat := make([]float64, n)
	if got := TestSignificance(flat, flat, market); got.Computed {
		t.Error("a strategy that never invested produced a verdict")
	}
}

// End to end: a real run fills the field, exposure lines up with the returns,
// and a fully invested buy-and-hold is not significant — which is correct and
// worth pinning, since buy-and-hold has no timing at all.
func TestSignificanceDuringARun(t *testing.T) {
	closes := map[string][]float64{"AAA": rampCloses(400, 0.5)}
	p := runExact(t, "buyAndHold:equalWeights", exactCash,
		[]string{"AAA"}, closes, CostConfig{})

	if len(p.Exposure) != len(p.DailyReturns) {
		t.Fatalf("exposure has %d entries against %d daily returns",
			len(p.Exposure), len(p.DailyReturns))
	}
	if !p.Significance.Computed {
		t.Fatal("no significance computed for a 400-day run")
	}
	// Buy-and-hold is fully invested every day, so shuffling its exposure
	// changes nothing and it cannot beat its own null.
	if p.Significance.PValue < 0.9 {
		t.Errorf("p = %v for buy-and-hold, want ~1: it has no timing to test",
			p.Significance.PValue)
	}
}

// The block length is not decoration. On an autocorrelated return series —
// which every real strategy produces — resampling single days destroys the
// autocorrelation and yields a confidence interval that is far too narrow, i.e.
// a Sharpe that looks more precise than it is. Blocks keep it, so the block
// interval must be the wider of the two.
func TestBlockBootstrapIsWiderThanDailyResampling(t *testing.T) {
	// Strongly autocorrelated: today's return is mostly yesterday's.
	const n = 600
	rng := rand.New(rand.NewSource(9))
	returns := make([]float64, n)
	prev := 0.0
	for i := range returns {
		prev = 0.9*prev + rng.NormFloat64()*0.004
		returns[i] = prev
	}

	daily := rand.New(rand.NewSource(SignificanceSeed))
	blocked := rand.New(rand.NewSource(SignificanceSeed))
	dLow, dHigh := bootstrapSharpe(returns, 1, daily)
	bLow, bHigh := bootstrapSharpe(returns, BootstrapBlockDays, blocked)

	dailyWidth, blockWidth := dHigh-dLow, bHigh-bLow
	if blockWidth <= dailyWidth {
		t.Errorf("block bootstrap width %v is not above the daily one %v — "+
			"resampling single days destroys the autocorrelation and makes "+
			"the interval too narrow", blockWidth, dailyWidth)
	}
}
