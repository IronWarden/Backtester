package backtest

import (
	"math"
	"my-backtester/src/data"

	"gonum.org/v1/gonum/stat"
)

type Metrics struct {
	SharpeRatio       float64
	SortinoRatio      float64
	MaxDrawdown       float64
	AnnualReturn      float64
	StandardDev       float64
	AvgCorrelation    float64
	CointegratedPairs int
	// Turnover is annualized gross traded notional as a multiple of the
	// portfolio's average value. 1.0 means the portfolio traded its own
	// value once over a year. Reported whether or not costs are configured,
	// since it is the thing costs are charged against.
	Turnover float64

	// Benchmark-relative metrics. All zero unless the portfolio configures a
	// Benchmark ticker whose data covers the simulated window.
	Alpha            float64 // annualized %, excess of what beta explains
	Beta             float64 // slope against the benchmark; 1.0 = moves with it
	TrackingError    float64 // annualized stdev of active return, a decimal
	InformationRatio float64 // active return per unit of tracking error
	UpCapture        float64 // % of benchmark's gain captured on its up days
	DownCapture      float64 // % of benchmark's loss taken on its down days
}

// GetTurnover annualizes gross traded notional against the portfolio's
// average value over the same 252-trading-day year the other metrics use.
// Both sides of a round trip count, so a buy-and-hold portfolio that opens
// its position and never trades again still reports the entry.
//
// Returns 0 rather than NaN or Inf for an empty series or a portfolio whose
// average value is not positive.
func GetTurnover(tradedNotional float64, closeValues []float64) float64 {
	n := len(closeValues)
	if n == 0 || tradedNotional <= 0 {
		return 0
	}
	var total float64
	for _, v := range closeValues {
		total += v
	}
	avgValue := total / float64(n)
	if avgValue <= 0 {
		return 0
	}
	turnover := (tradedNotional / avgValue) * (252.0 / float64(n))
	// A vanishingly small average value divides to +Inf even though it
	// passed the > 0 check above. No metric may put a non-finite number in
	// the results table, so a portfolio worth that little reports no turnover.
	if math.IsInf(turnover, 0) || math.IsNaN(turnover) {
		return 0
	}
	return turnover
}

// --- benchmark-relative metrics ---------------------------------------------
//
// Each of these takes two return series of equal length, already aligned so
// that index i is the same calendar day in both. They are only computed for
// a portfolio that configures a Benchmark; an unbenchmarked portfolio leaves
// the corresponding Metrics fields at zero.
//
// Every function returns 0 rather than NaN or Inf on degenerate input, in
// keeping with the rest of this file: no metric may put a non-finite number
// in the results table.

// alignedOK reports whether two return series can be compared at all.
func alignedOK(a, b []float64) bool {
	return len(a) == len(b) && len(a) >= 2
}

// negligibleStdDev is the dispersion below which a series is treated as
// constant. Daily returns run to about 1e-2, and differencing two of them
// leaves float noise around 1e-17 where the true answer is exactly zero —
// so an exact `== 0` test misses the constant case and lets a ratio divide
// by noise into a meaningless 1e16. Anything under this threshold is
// numerically indistinguishable from no dispersion at all.
const negligibleStdDev = 1e-12

// GetBeta is the slope of the portfolio's returns regressed on the
// benchmark's: cov(p, b) / var(b). 1.0 means the portfolio moved
// one-for-one with the benchmark. A benchmark with no variance has no
// slope to measure and yields 0.
func GetBeta(portfolio, benchmark []float64) float64 {
	if !alignedOK(portfolio, benchmark) {
		return 0
	}
	varB := stat.Variance(benchmark, nil)
	if math.IsNaN(varB) || math.Sqrt(varB) <= negligibleStdDev {
		return 0
	}
	cov := stat.Covariance(portfolio, benchmark, nil)
	beta := cov / varB
	if math.IsNaN(beta) || math.IsInf(beta, 0) {
		return 0
	}
	return beta
}

// GetAlpha is the annualized excess return the portfolio earned beyond what
// its beta exposure to the benchmark explains — the intercept of the same
// regression, scaled to a 252-day year and expressed in percent to match
// AnnualReturn. Both series must already be excess of the risk-free rate.
func GetAlpha(portfolioExcess, benchmarkExcess []float64) float64 {
	if !alignedOK(portfolioExcess, benchmarkExcess) {
		return 0
	}
	beta := GetBeta(portfolioExcess, benchmarkExcess)
	daily := stat.Mean(portfolioExcess, nil) - beta*stat.Mean(benchmarkExcess, nil)
	alpha := daily * 252.0 * 100.0
	if math.IsNaN(alpha) || math.IsInf(alpha, 0) {
		return 0
	}
	return alpha
}

// activeReturns is the per-day difference between the two series.
func activeReturns(portfolio, benchmark []float64) []float64 {
	active := make([]float64, len(portfolio))
	for i := range portfolio {
		active[i] = portfolio[i] - benchmark[i]
	}
	return active
}

// GetTrackingError is the annualized standard deviation of the difference
// between the two series — how much the portfolio wanders from its
// benchmark. Reported as a decimal, like StandardDev.
func GetTrackingError(portfolio, benchmark []float64) float64 {
	if !alignedOK(portfolio, benchmark) {
		return 0
	}
	sd := stat.StdDev(activeReturns(portfolio, benchmark), nil)
	if math.IsNaN(sd) || sd <= negligibleStdDev {
		return 0
	}
	te := sd * math.Sqrt(252.0)
	if math.IsInf(te, 0) {
		return 0
	}
	return te
}

// GetInformationRatio is annualized mean active return over tracking error:
// how much outperformance the portfolio earned per unit of deviation from
// its benchmark. A portfolio that tracks perfectly has no tracking error to
// divide by and yields 0.
func GetInformationRatio(portfolio, benchmark []float64) float64 {
	if !alignedOK(portfolio, benchmark) {
		return 0
	}
	active := activeReturns(portfolio, benchmark)
	sd := stat.StdDev(active, nil)
	if math.IsNaN(sd) || sd <= negligibleStdDev {
		return 0
	}
	ir := stat.Mean(active, nil) / sd * math.Sqrt(252.0)
	if math.IsNaN(ir) || math.IsInf(ir, 0) {
		return 0
	}
	return ir
}

// capture is the shared body of up- and down-capture: the portfolio's mean
// return over the days the benchmark moved in the wanted direction, as a
// percentage of the benchmark's own mean over those same days. 100 means
// the portfolio matched the benchmark on those days.
func capture(portfolio, benchmark []float64, up bool) float64 {
	if !alignedOK(portfolio, benchmark) {
		return 0
	}
	var sumP, sumB float64
	var n int
	for i := range benchmark {
		if (up && benchmark[i] > 0) || (!up && benchmark[i] < 0) {
			sumP += portfolio[i]
			sumB += benchmark[i]
			n++
		}
	}
	// No days in that direction, or they cancel out: nothing to capture.
	if n == 0 || sumB == 0 {
		return 0
	}
	c := (sumP / float64(n)) / (sumB / float64(n)) * 100.0
	if math.IsNaN(c) || math.IsInf(c, 0) {
		return 0
	}
	return c
}

// GetUpCapture measures the portfolio against the benchmark over only the
// days the benchmark rose. Above 100 means it gained more than its
// benchmark in up markets.
func GetUpCapture(portfolio, benchmark []float64) float64 {
	return capture(portfolio, benchmark, true)
}

// GetDownCapture measures the portfolio over only the days the benchmark
// fell. Below 100 means it lost less than its benchmark in down markets,
// which is the desirable direction — unlike up-capture, lower is better.
func GetDownCapture(portfolio, benchmark []float64) float64 {
	return capture(portfolio, benchmark, false)
}

// GetSortinoRatio annualizes mean excess return divided by the downside
// deviation. Downside deviation is the target semideviation with target = the
// risk-free rate: the root-mean-square of the negative excess returns taken
// over ALL periods (positive periods contribute 0), not the standard deviation
// of only the negative days. Returns 0 when there is no downside or no data.
func GetSortinoRatio(excess []float64) float64 {
	if len(excess) == 0 {
		return 0.0
	}
	var sumSq float64
	for _, e := range excess {
		if e < 0 {
			sumSq += e * e
		}
	}
	downsideDeviation := math.Sqrt(sumSq / float64(len(excess)))
	if downsideDeviation == 0 {
		return 0.0 // Avoid division by zero
	}
	return stat.Mean(excess, nil) / downsideDeviation * math.Sqrt(252.0)
}

// GetAnnualReturn is the CAGR of the compounded daily returns, annualized over
// the actual elapsed calendar time (numYears) rather than a fixed 252-day
// count, so it matches date-based CAGR conventions.
func GetAnnualReturn(dailyReturns []float64, numYears float64) float64 {
	if numYears <= 0 {
		return 0.0
	}
	growth := 1.0
	for _, r := range dailyReturns {
		growth *= 1 + r
	}
	// Compound Annual Growth Rate - (end/start) ^ 1/years - 1
	return (math.Pow(growth, 1/numYears) - 1) * 100
}

func GetMaxDrawdown(portfolioCloseValues []float64) float64 {
	if len(portfolioCloseValues) == 0 {
		return 0.0
	}
	peak := portfolioCloseValues[0]
	maxDrawdown := 0.0

	for _, value := range portfolioCloseValues {
		if value > peak {
			peak = value
		}
		drawdown := (peak - value) / peak
		if drawdown > maxDrawdown {
			maxDrawdown = drawdown
		}
	}

	return maxDrawdown * 100
}

// GetSharpeRatio annualizes the daily excess-return series (portfolio return
// minus the per-day risk-free rate). Returns 0 when the series has no
// dispersion, avoiding a divide-by-zero NaN.
func GetSharpeRatio(excess []float64) float64 {
	// stat.StdDev is NaN for fewer than two samples (the sample stdev's
	// n-1 denominator is 0), which a bare `== 0` check would let through
	// and surface as "NaN" in the results table.
	excessStdev := stat.StdDev(excess, nil)
	if excessStdev == 0 || math.IsNaN(excessStdev) {
		return 0.0
	}
	return stat.Mean(excess, nil) / excessStdev * math.Sqrt(252.0)
}

func (p *Portfolio) GetBacktestingData(
	riskFreeRates map[int64]float64,
	hist map[string][]data.AssetData,
	dataLen int,
) {
	dailyReturns := make([]float64, 0, len(p.DailyReturns))
	excess := make([]float64, 0, len(p.DailyReturns))
	for _, dr := range p.DailyReturns {
		dailyReturns = append(dailyReturns, dr.Return)
		// A day absent from riskFreeRates contributes a 0 rate rather than
		// dropping the return, so Sharpe/Sortino span the whole window instead
		// of only the days the risk-free table happens to cover.
		excess = append(excess, dr.Return-riskFreeRates[dr.Date.Unix()])
	}

	// Annualize over the actual elapsed time between the first and last day.
	numYears := 0.0
	if n := len(p.DailyReturns); n > 1 {
		span := p.DailyReturns[n-1].Date.Sub(p.DailyReturns[0].Date)
		numYears = span.Hours() / 24 / 365.25
	}

	// annualize standard deviation
	standardDev := stat.StdDev(dailyReturns, nil) * math.Sqrt(252.0)
	sharpeRatio := GetSharpeRatio(excess)
	sortinoRatio := GetSortinoRatio(excess)
	annualReturn := GetAnnualReturn(dailyReturns, numYears)
	maxDrawdown := GetMaxDrawdown(p.PortfolioCloseValues)
	avgCorrelation := AvgPairwiseCorrelation(p.Tickers, hist, dataLen)
	cointegratedPairs := CountCointegratedPairs(p.Tickers, hist, dataLen)
	metrics := Metrics{
		StandardDev:       standardDev,
		SharpeRatio:       sharpeRatio,
		SortinoRatio:      sortinoRatio,
		MaxDrawdown:       maxDrawdown,
		AnnualReturn:      annualReturn,
		AvgCorrelation:    avgCorrelation,
		CointegratedPairs: cointegratedPairs,
		Turnover:          GetTurnover(p.tradedNotional, p.PortfolioCloseValues),
	}
	p.Metrics = metrics
}
