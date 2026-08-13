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
	}
	p.Metrics = metrics
}
