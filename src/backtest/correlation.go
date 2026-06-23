package backtest

import (
	"math"
	"my-backtester/src/data"

	"gonum.org/v1/gonum/stat"
)

// closesOnly returns the first n Close prices from series. Series shorter
// than n are truncated to their own length; n < 1 yields nil.
func closesOnly(series []data.AssetData, n int) []float64 {
	if n > len(series) {
		n = len(series)
	}
	if n < 1 {
		return nil
	}
	closes := make([]float64, n)
	for i := 0; i < n; i++ {
		closes[i] = series[i].Close
	}
	return closes
}

// returnsFromCloses returns the n-1 simple daily returns derived from the
// first n Close prices. Series shorter than n are truncated to their own
// length; n < 2 yields nil.
func returnsFromCloses(series []data.AssetData, n int) []float64 {
	if n > len(series) {
		n = len(series)
	}
	if n < 2 {
		return nil
	}
	returns := make([]float64, n-1)
	for i := 1; i < n; i++ {
		prev := series[i-1].Close
		if prev == 0 {
			continue
		}
		returns[i-1] = (series[i].Close - prev) / prev
	}
	return returns
}

// AvgPairwiseCorrelation is the mean Pearson correlation of daily returns
// across every distinct ticker pair in the portfolio. Returns 0 when
// fewer than two tickers carry usable data.
func AvgPairwiseCorrelation(
	tickers []string,
	hist map[string][]data.AssetData,
	dataLen int,
) float64 {
	returns := make([][]float64, 0, len(tickers))
	for _, t := range tickers {
		r := returnsFromCloses(hist[t], dataLen)
		if len(r) >= 2 {
			returns = append(returns, r)
		}
	}
	n := len(returns)
	if n < 2 {
		return 0
	}
	var total float64
	var count int
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			a, b := returns[i], returns[j]
			m := len(a)
			if len(b) < m {
				m = len(b)
			}
			if m < 2 {
				continue
			}
			c := stat.Correlation(a[:m], b[:m], nil)
			if !math.IsNaN(c) {
				total += c
				count++
			}
		}
	}
	if count == 0 {
		return 0
	}
	return total / float64(count)
}

// engleGrangerCointegrated tests whether two price series cointegrate at
// the 5% level. Step 1 runs OLS y = α + β·x to extract residuals; step 2
// runs an ADF(1) regression Δe_t = ρ·e_{t-1} + γ·Δe_{t-1} + ε on those
// residuals (no constant — OLS residuals have zero mean by construction)
// and compares the t-stat on ρ to MacKinnon's asymptotic Engle-Granger 5%
// critical value of −3.34 (two variables, constant in step 1).
func engleGrangerCointegrated(y, x []float64) bool {
	n := len(y)
	if n != len(x) || n < 30 {
		return false
	}

	meanY := stat.Mean(y, nil)
	meanX := stat.Mean(x, nil)
	var sxx, sxy float64
	for i := 0; i < n; i++ {
		dx := x[i] - meanX
		dy := y[i] - meanY
		sxx += dx * dx
		sxy += dx * dy
	}
	if sxx == 0 {
		return false
	}
	beta := sxy / sxx
	alpha := meanY - beta*meanX
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = y[i] - alpha - beta*x[i]
	}

	m := n - 2
	if m < 5 {
		return false
	}
	var s11, s22, s12, s1y, s2y float64
	for i := 2; i < n; i++ {
		eLag := res[i-1]
		deLag := res[i-1] - res[i-2]
		deT := res[i] - res[i-1]
		s11 += eLag * eLag
		s22 += deLag * deLag
		s12 += eLag * deLag
		s1y += eLag * deT
		s2y += deLag * deT
	}
	det := s11*s22 - s12*s12
	if det == 0 {
		return false
	}
	rho := (s22*s1y - s12*s2y) / det
	gamma := (s11*s2y - s12*s1y) / det

	var rss float64
	for i := 2; i < n; i++ {
		eLag := res[i-1]
		deLag := res[i-1] - res[i-2]
		deT := res[i] - res[i-1]
		e := deT - rho*eLag - gamma*deLag
		rss += e * e
	}
	df := m - 2
	if df <= 0 {
		return false
	}
	sigma2 := rss / float64(df)
	seRho := math.Sqrt(sigma2 * s22 / det)
	if seRho == 0 {
		return false
	}
	tStat := rho / seRho
	return tStat < -3.34
}

// CountCointegratedPairs returns the number of ticker pairs whose Close
// prices pass an Engle-Granger cointegration test at the 5% level.
func CountCointegratedPairs(
	tickers []string,
	hist map[string][]data.AssetData,
	dataLen int,
) int {
	prices := make([][]float64, 0, len(tickers))
	for _, t := range tickers {
		c := closesOnly(hist[t], dataLen)
		if len(c) >= 30 {
			prices = append(prices, c)
		}
	}
	n := len(prices)
	count := 0
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			a, b := prices[i], prices[j]
			m := len(a)
			if len(b) < m {
				m = len(b)
			}
			if engleGrangerCointegrated(a[:m], b[:m]) {
				count++
			}
		}
	}
	return count
}
