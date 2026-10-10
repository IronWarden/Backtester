package backtest

import (
	"math"
	"time"

	"my-backtester/src/data"
)

// Real (inflation-adjusted) returns.
//
// A 10% year in 2021 and a 10% year in 2013 are not the same result, and until
// now the engine reported both as 10.00. This restates the growth figure in
// constant money so the two can be compared.
//
// Only the RETURN is deflated. The equity curve, the drawdowns and every
// ratio built on them stay nominal on purpose: a drawdown is a nominal
// experience — you watched the account fall by that much — and restating it in
// today's money answers a question nobody asked while corrupting the metric
// users trust most. Sharpe is already quoted on excess-of-risk-free returns,
// which prices the time value of money from a different direction; deflating
// on top of that would double-count.

// InflationFactor is CPI(end) / CPI(start): the cumulative price rise across
// the window, as a multiplier. Reports false when either endpoint's month has
// no reading, which is the only honest answer — see data.CPIForMonth for why a
// nearest-available fallback would silently overstate the real return.
//
// Deliberately computed from the two endpoints rather than day by day. CPI is
// monthly and the engine is daily; interpolating within a month would invent
// precision the source does not have, and the endpoints are all a
// point-to-point deflation needs.
func InflationFactor(series []data.CPIPoint, start, end time.Time) (float64, bool) {
	if len(series) == 0 {
		return 0, false
	}
	from, ok := data.CPIForMonth(series, start)
	if !ok || from <= 0 {
		return 0, false
	}
	to, ok := data.CPIForMonth(series, end)
	if !ok || to <= 0 {
		return 0, false
	}
	return to / from, true
}

// GetRealAnnualReturn is the CAGR of the compounded daily returns after
// dividing out inflation, annualized over numYears.
//
// This is the compounding form, not `nominal - inflation`. The subtraction is
// an approximation that drifts as both the return and the window grow, which
// is exactly the decade-long case this metric exists for: 10% nominal against
// 3% inflation is 6.80% real, not 7%.
//
// Mirrors GetAnnualReturn's shape and its numYears guard so the two cannot
// disagree about what a year is.
func GetRealAnnualReturn(
	dailyReturns []float64, numYears, inflationFactor float64,
) float64 {
	if numYears <= 0 || inflationFactor <= 0 {
		return 0.0
	}
	growth := 1.0
	for _, r := range dailyReturns {
		growth *= 1 + r
	}
	return (math.Pow(growth/inflationFactor, 1/numYears) - 1) * 100
}

// realFromNominal deflates an already-annualized nominal return. Used for the
// benchmark, whose per-day series the portfolio does not keep — only its
// headline figures — so the daily-compounding form above is not available.
//
// Algebraically identical to GetRealAnnualReturn over the same window:
// both undo one window's worth of inflation and re-annualize.
func realFromNominal(annualReturnPct, numYears, inflationFactor float64) float64 {
	if numYears <= 0 || inflationFactor <= 0 {
		return 0.0
	}
	growth := math.Pow(1+annualReturnPct/100, numYears)
	return (math.Pow(growth/inflationFactor, 1/numYears) - 1) * 100
}
