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
