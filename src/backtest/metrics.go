package backtest

import (
	"math"
	"time"

	"gonum.org/v1/gonum/stat"
)

type Metrics struct {
	SharpeRatio  float64
	SortinoRatio float64
	MaxDrawdown  float64
	AnnualReturn float64
	StandardDev  float64
}

func GetSortinoRatio(riskFreeRates map[int64]float64, dailyAvg map[int64]float64) float64 {
	excessReturns := make([]float64, 0)
	downsideReturns := make([]float64, 0)

	// Iterate through map for common dates to calculate excess returns
	for key, val := range dailyAvg {
		if riskFreeRate, ok := riskFreeRates[key]; ok {
			excessReturn := val - riskFreeRate
			excessReturns = append(excessReturns, excessReturn)
			if excessReturn < 0 {
				downsideReturns = append(downsideReturns, excessReturn)
			}
		}
	}

	if len(downsideReturns) == 0 {
		return 0.0 // Avoid division by zero if there are no negative returns
	}

	averageExcessReturn := stat.Mean(excessReturns, nil)
	downsideDeviation := stat.StdDev(downsideReturns, nil)

	if downsideDeviation == 0 {
		return 0.0 // Avoid division by zero
	}

	sortinoRatio := averageExcessReturn / downsideDeviation
	// Annualize
	annualizedSortino := sortinoRatio * math.Sqrt(252.0)
	return annualizedSortino
}

func GetAnnualReturn(dailyAvg []float64) float64 {
	startValue := 1.0

	for i := range dailyAvg {
		startValue *= (1 + dailyAvg[i])
	}
	numYears := float64(len(dailyAvg)) / 252.0
	// Compound Annual Growth Rate - (end/start) ^ 1/n - 1
	CAGR := math.Pow(startValue, 1/numYears) - 1
	return CAGR * 100
}

func GetMaxDrawdown(portfolioCloseValues []float64) float64 {
	peak := portfolioCloseValues[0]
	peakIdx := 0
	for i, value := range portfolioCloseValues {
		// Find peak but allow atleast 1 day remaining in the period
		if value > peak && i < len(portfolioCloseValues)-2 {
			peakIdx = i
			peak = value
		}
	}
	minValue := peak
	for i := peakIdx + 1; i < len(portfolioCloseValues); i++ {
		minValue = math.Min(minValue, portfolioCloseValues[i])
	}
	change := (minValue - peak) / peak

	return change * 100
}

func GetSharpeRatio(riskFreeRates map[int64]float64, dailyAvg map[int64]float64) float64 {
	excessReturns := make([]float64, 0)
	// Iterate through map for common dates calculate excess return
	for key, val := range dailyAvg {
		if _, ok := riskFreeRates[key]; ok {
			excessReturn := val - riskFreeRates[key]
			excessReturns = append(excessReturns, excessReturn)
		}
	}
	excessStdev := stat.StdDev(excessReturns, nil)
	sharpeRatio := stat.Mean(excessReturns, nil) / excessStdev
	annualizedSharpe := sharpeRatio * math.Sqrt(252.0)
	return annualizedSharpe
}

func (p *Portfolio) GetBacktestingData(startTime time.Time, endTime time.Time, riskFreeRates map[int64]float64) {
	dailyAvg := make(map[int64]float64, len(p.DailyReturns))
	dailyAvgSlice := make([]float64, 0, len(p.DailyReturns))
	for _, dr := range p.DailyReturns {
		dailyAvg[dr.Date.Unix()] = dr.Return
		dailyAvgSlice = append(dailyAvgSlice, dr.Return)
	}

	// annualize standard deviation
	standardDev := stat.StdDev(dailyAvgSlice, nil) * math.Sqrt(252.0)
	sharpeRatio := GetSharpeRatio(riskFreeRates, dailyAvg)
	sortinoRatio := GetSortinoRatio(riskFreeRates, dailyAvg)
	annualReturn := GetAnnualReturn(dailyAvgSlice)
	maxDrawdown := GetMaxDrawdown(p.PortfolioCloseValues)
	metrics := Metrics{
		StandardDev:  standardDev,
		SharpeRatio:  sharpeRatio,
		SortinoRatio: sortinoRatio,
		MaxDrawdown:  maxDrawdown,
		AnnualReturn: annualReturn,
	}
	p.Metrics = metrics
}
