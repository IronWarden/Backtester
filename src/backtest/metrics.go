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

func GetSortinoRatio(
	riskFreeRates map[int64]float64,
	dailyAvg map[int64]float64,
) float64 {
	excessReturns := make([]float64, 0, len(dailyAvg))
	downsideReturns := make([]float64, 0)

	for key, val := range dailyAvg {
		if rate, ok := riskFreeRates[key]; ok {
			excessReturn := val - rate
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

func GetSharpeRatio(
	riskFreeRates map[int64]float64,
	dailyAvg map[int64]float64,
) float64 {
	excessReturns := make([]float64, 0, len(dailyAvg))
	for key, val := range dailyAvg {
		if rate, ok := riskFreeRates[key]; ok {
			excessReturns = append(excessReturns, val-rate)
		}
	}
	excessStdev := stat.StdDev(excessReturns, nil)
	sharpeRatio := stat.Mean(excessReturns, nil) / excessStdev
	annualizedSharpe := sharpeRatio * math.Sqrt(252.0)
	return annualizedSharpe
}

<<<<<<< Updated upstream
func (p *Portfolio) GetBacktestingData(params BacktesterParams) {
=======
func (p *Portfolio) GetBacktestingData(
	riskFreeRates map[int64]float64,
	hist map[string][]data.AssetData,
	dataLen int,
) {
>>>>>>> Stashed changes
	dailyAvg := make(map[int64]float64, len(p.DailyReturns))
	dailyAvgSlice := make([]float64, 0, len(p.DailyReturns))
	for _, dr := range p.DailyReturns {
		dailyAvg[dr.Date.Unix()] = dr.Return
		dailyAvgSlice = append(dailyAvgSlice, dr.Return)
	}

	// annualize standard deviation
	standardDev := stat.StdDev(dailyAvgSlice, nil) * math.Sqrt(252.0)
	sharpeRatio := GetSharpeRatio(params.RiskFreeRates, dailyAvg)
	sortinoRatio := GetSortinoRatio(params.RiskFreeRates, dailyAvg)
	annualReturn := GetAnnualReturn(dailyAvgSlice)
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
