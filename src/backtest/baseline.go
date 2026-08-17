package backtest

// What doing nothing clever would have returned.
//
// Most strategies do not beat buy-and-hold, and until now nothing in the output
// said so — you had to remember to build the comparison by hand, which means it
// happened when you were sceptical and not when you were excited. A baseline
// reported beside every result kills a bad idea faster than any statistic,
// because it answers the only question that matters first: was this worth doing
// at all?
//
// The baseline is an equal-weight buy-and-hold of the portfolio's OWN tickers,
// over the same days, with the same starting capital and the same cost model.
// Same universe on purpose: beating the S&P 500 by picking five megacaps is a
// statement about the universe, not about the strategy, and this comparison
// isolates the strategy by holding the universe fixed. The configured
// `Benchmark` answers the other question and is reported separately
// (BenchmarkStats).
//
// It is a real simulation rather than an analytic shortcut: the same
// buyAndHold:equalWeights strategy, through the same Buy path, paying the same
// commission and slippage. An analytic curve would quietly compare a costed
// strategy against a frictionless baseline and flatter every result.

import (
	"my-backtester/src/data"
	"time"
)

// BaselineStats is the baseline's own performance, plus how the strategy fared
// against it. It deliberately mirrors BenchmarkStats' four headline figures so
// the two comparisons read the same way in a table.
type BaselineStats struct {
	// Name says what the baseline was, since "the baseline" alone invites the
	// reader to assume whichever comparison flatters the result.
	Name         string  `json:"name"`
	FinalValue   float64 `json:"finalValue"`
	AnnualReturn float64 `json:"annualReturn"`
	SharpeRatio  float64 `json:"sharpeRatio"`
	MaxDrawdown  float64 `json:"maxDrawdown"`
	StandardDev  float64 `json:"standardDev"`
	// ExcessFinalValue is the strategy's final value minus the baseline's, in
	// dollars — the plainest possible statement of whether the work paid.
	// ExcessAnnualReturn is the same comparison annualized.
	ExcessFinalValue   float64 `json:"excessFinalValue"`
	ExcessAnnualReturn float64 `json:"excessAnnualReturn"`
	// Beat is true when the strategy finished ahead of the baseline. Stated as
	// a flag because it is the first thing a reader looks for and should not
	// require subtracting two numbers in their head.
	Beat bool `json:"beat"`
	// Computed is false when no baseline could be built — no tickers, no days,
	// or a universe the equal-weight hold could not enter. Distinguishes "the
	// strategy matched the baseline exactly" from "there was no baseline".
	Computed bool `json:"computed"`
}

// BaselineName is what the comparison is called wherever it surfaces.
const BaselineName = "equal-weight buy & hold"

// equalWeightBaseline simulates the baseline over the same aligned window and
// returns its stats alongside the comparison to the strategy that just ran.
//
// windowed and dataLen are the caller's already-aligned data, so this costs no
// query and no re-alignment — it is one extra pass over data already in hand.
func equalWeightBaseline(
	p *Portfolio,
	windowed map[string][]data.AssetData,
	dataLen int,
	riskFreeRates map[int64]float64,
) BaselineStats {
	var stats BaselineStats
	if dataLen < 2 || len(p.Tickers) == 0 {
		return stats
	}

	// A fresh portfolio with the same capital, window, universe and costs. It
	// must not inherit the strategy under test, obviously, and it must not
	// inherit the benchmark or the validation split either: the baseline is a
	// yardstick, not a second result, and computing its benchmark metrics would
	// be work whose output nothing reads.
	base, err := InitializePortfolio(
		p.InitialBuyingPower,
		p.StartTime, p.EndTime,
		p.Pname+" ["+BaselineName+"]",
		p.Tickers,
		"buyAndHold:equalWeights",
		nil,
	)
	if err != nil {
		return stats
	}
	base.Costs = p.Costs

	base.Strategy.Step(base, windowed, 0)
	prev := base.GetPortfolioValue(base.Tickers, windowed, 0)
	for day := 1; day < dataLen; day++ {
		base.Strategy.Step(base, windowed, day)
		curr := base.GetPortfolioValue(base.Tickers, windowed, day)
		base.AdjustPortfolioParameters(base.Tickers, windowed, day, prev, curr)
		prev = curr
	}
	base.GetBacktestingData(riskFreeRates, windowed, dataLen)

	if len(base.PortfolioCloseValues) == 0 {
		return stats
	}
	final := base.PortfolioCloseValues[len(base.PortfolioCloseValues)-1]

	stats = BaselineStats{
		Name:         BaselineName,
		FinalValue:   final,
		AnnualReturn: base.Metrics.AnnualReturn,
		SharpeRatio:  base.Metrics.SharpeRatio,
		MaxDrawdown:  base.Metrics.MaxDrawdown,
		StandardDev:  base.Metrics.StandardDev,
		Computed:     true,
	}
	if len(p.PortfolioCloseValues) > 0 {
		strategyFinal := p.PortfolioCloseValues[len(p.PortfolioCloseValues)-1]
		stats.ExcessFinalValue = strategyFinal - final
		stats.Beat = stats.ExcessFinalValue > 0
	}
	stats.ExcessAnnualReturn = p.Metrics.AnnualReturn - stats.AnnualReturn
	return stats
}

// elapsedYears is the window length in years, used where a comparison needs to
// be annualized outside GetBacktestingData. Kept here rather than duplicated at
// the call site so the 365.25-day convention stays in one place.
func elapsedYears(first, last time.Time) float64 {
	if last.Before(first) || last.Equal(first) {
		return 0
	}
	return last.Sub(first).Hours() / 24 / 365.25
}
