package backtest

import (
	"log"
	"math"
	"my-backtester/src/data"
	"time"

	"gonum.org/v1/gonum/stat"
	"gonum.org/v1/gonum/stat/distuv"
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

// benchmarkReturns aligns a benchmark's bars onto the portfolio's own
// trading days and returns the day-over-day returns for days 1..n-1, so the
// result lines up 1:1 with the portfolio's DailyReturns.
//
// ok is false when the benchmark does not cover every one of those days, or
// when a day's previous close is not positive. A partial series is worse
// than none here: metrics computed over a benchmark that silently skipped
// the days it was missing would be quietly wrong rather than absent.
func benchmarkReturns(
	bars []data.AssetData, dates []time.Time,
) ([]float64, bool) {
	if len(dates) < 2 || len(bars) == 0 {
		return nil, false
	}
	byDate := make(map[int64]float64, len(bars))
	for _, b := range bars {
		byDate[b.Date.Unix()] = b.Close
	}
	returns := make([]float64, len(dates)-1)
	for i := 1; i < len(dates); i++ {
		prev, okPrev := byDate[dates[i-1].Unix()]
		curr, okCurr := byDate[dates[i].Unix()]
		if !okPrev || !okCurr || prev <= 0 {
			return nil, false
		}
		returns[i-1] = (curr - prev) / prev
	}
	return returns, true
}

// applyBenchmarkMetrics fills in the benchmark-relative fields of p.Metrics.
// It is a no-op for a portfolio with no Benchmark configured, and logs and
// leaves the fields zeroed when the benchmark's data does not cover the
// simulated window — an uncoverable benchmark must never fail a backtest
// that would otherwise succeed.
//
// Beta and alpha are computed on returns excess of the risk-free rate, as
// CAPM defines them. Tracking error, information ratio and the capture
// ratios are computed on raw returns, which is how those are conventionally
// quoted.
func (p *Portfolio) applyBenchmarkMetrics(
	hist map[string][]data.AssetData,
	dates []time.Time,
	riskFreeRates map[int64]float64,
) {
	if p.Benchmark == "" {
		return
	}
	bars, ok := hist[p.Benchmark]
	if !ok || len(bars) == 0 {
		log.Printf(
			"portfolio %q: benchmark %q has no data; skipping benchmark metrics",
			p.Pname, p.Benchmark,
		)
		return
	}
	benchmark, ok := benchmarkReturns(bars, dates)
	if !ok {
		log.Printf(
			"portfolio %q: benchmark %q does not cover every trading day in "+
				"the window; skipping benchmark metrics",
			p.Pname, p.Benchmark,
		)
		return
	}

	portfolio := make([]float64, len(p.DailyReturns))
	portfolioExcess := make([]float64, len(p.DailyReturns))
	benchmarkExcess := make([]float64, 0, len(benchmark))
	for i, dr := range p.DailyReturns {
		portfolio[i] = dr.Return
		rf := riskFreeRates[dr.Date.Unix()]
		portfolioExcess[i] = dr.Return - rf
		if i < len(benchmark) {
			benchmarkExcess = append(benchmarkExcess, benchmark[i]-rf)
		}
	}
	if len(portfolio) != len(benchmark) {
		log.Printf(
			"portfolio %q: benchmark %q produced %d returns against the "+
				"portfolio's %d; skipping benchmark metrics",
			p.Pname, p.Benchmark, len(benchmark), len(portfolio),
		)
		return
	}

	p.Metrics.Beta = GetBeta(portfolioExcess, benchmarkExcess)
	p.Metrics.Alpha = GetAlpha(portfolioExcess, benchmarkExcess)
	p.Metrics.TrackingError = GetTrackingError(portfolio, benchmark)
	p.Metrics.InformationRatio = GetInformationRatio(portfolio, benchmark)
	p.Metrics.UpCapture = GetUpCapture(portfolio, benchmark)
	p.Metrics.DownCapture = GetDownCapture(portfolio, benchmark)

	// The same return series, kept rather than discarded, so the UI can draw
	// the benchmark next to the portfolio and report what it did on its own.
	p.BenchmarkCurve = rebasedCurve(benchmark, p.InitialBuyingPower)
	p.BenchmarkStats = benchmarkStatsFrom(
		p.Benchmark, benchmark, benchmarkExcess, p.BenchmarkCurve, p.DailyReturns,
	)
}

// BenchmarkStats is what the benchmark did on its own terms, as opposed to
// the benchmark-relative fields of Metrics, which describe the portfolio's
// behaviour against it. It is deliberately a separate type rather than more
// fields on Metrics: Metrics describes the portfolio, and every consumer of
// it — the reporter's field switch, [Output] fields, the results table —
// would otherwise start reporting two different subjects in one row.
//
// All zero for a portfolio with no Benchmark, or one whose benchmark does not
// cover the window.
type BenchmarkStats struct {
	// Ticker is the benchmark symbol, so the UI can label the series.
	Ticker string `json:"ticker"`
	// The same four headline figures the portfolio reports, computed the same
	// way over the benchmark's own return series.
	AnnualReturn float64 `json:"annualReturn"`
	SharpeRatio  float64 `json:"sharpeRatio"`
	MaxDrawdown  float64 `json:"maxDrawdown"`
	StandardDev  float64 `json:"standardDev"`
}

// rebasedCurve compounds a return series into a value series starting from
// initial. Rebasing to the portfolio's own starting capital is what lets both
// lines share one axis: the raw index level would put a 5,000-point index
// against a $10,000 portfolio and make the chart unreadable. Returns are
// scale-invariant, so this changes no metric.
//
// The result is 1:1 with the return series, and so with the portfolio's daily
// returns and PortfolioCloseValues — index i is the same trading day in all
// three, which is what keeps the two chart lines aligned.
func rebasedCurve(returns []float64, initial float64) []float64 {
	if len(returns) == 0 {
		return nil
	}
	curve := make([]float64, len(returns))
	level := initial
	for i, r := range returns {
		level *= 1 + r
		curve[i] = level
	}
	return curve
}

// benchmarkStatsFrom computes the benchmark's own headline metrics, reusing
// the portfolio's metric functions so the two columns are computed
// identically and stay comparable. numYears is taken from the portfolio's own
// day series for the same reason.
func benchmarkStatsFrom(
	ticker string,
	returns, excess, curve []float64,
	days []DailyReturn,
) BenchmarkStats {
	numYears := 0.0
	if n := len(days); n > 1 {
		span := days[n-1].Date.Sub(days[0].Date)
		numYears = span.Hours() / 24 / 365.25
	}
	return BenchmarkStats{
		Ticker:       ticker,
		AnnualReturn: GetAnnualReturn(returns, numYears),
		SharpeRatio:  GetSharpeRatio(excess),
		MaxDrawdown:  GetMaxDrawdown(curve),
		StandardDev:  stat.StdDev(returns, nil) * math.Sqrt(252.0),
	}
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

// --- in-sample / out-of-sample split ----------------------------------------

// SegmentStats is what one slice of a run did on its own. It carries the
// subset of metrics that are meaningful over an arbitrary sub-period: the
// ratio and drawdown figures describe whatever return series they are handed,
// whereas Turnover needs per-segment traded notional (which is not tracked)
// and AvgCorrelation/CointegratedPairs describe the tickers rather than the
// run, so neither is split.
//
// Like BenchmarkStats this is deliberately not part of Metrics. Metrics
// describes one series over one window; a run with a split has three windows,
// and folding them together would make every consumer of Metrics report
// several subjects in one row.
type SegmentStats struct {
	// Label is "in-sample" or "out-of-sample".
	Label string `json:"label"`
	// Start and End are the first and last trading day actually in the
	// segment, YYYY-MM-DD — not the requested split date, which usually falls
	// on a day the market was shut.
	Start string `json:"start"`
	End   string `json:"end"`
	Days  int    `json:"days"`
	// TotalReturn is the compounded return over the segment, in %. The two
	// segments' TotalReturns compound to the whole run's, which is the
	// invariant that says the split did not lose or double-count a day.
	TotalReturn  float64 `json:"totalReturn"`
	AnnualReturn float64 `json:"annualReturn"`
	SharpeRatio  float64 `json:"sharpeRatio"`
	SortinoRatio float64 `json:"sortinoRatio"`
	MaxDrawdown  float64 `json:"maxDrawdown"`
	StandardDev  float64 `json:"standardDev"`
}

// minSegmentDays is the fewest trading days a segment may have and still be
// reported. Ratio metrics over a handful of days are noise quoted to two
// decimal places, which is worse than reporting nothing.
const minSegmentDays = 30

// segmentStats computes one segment's figures from the slice of the run it
// covers. days, values and excess are parallel and already cut to the
// segment; values are portfolio close values, so max drawdown is measured
// within the segment rather than against an earlier peak outside it — a
// segment's drawdown is what someone starting at its first day would have
// experienced.
func segmentStats(
	label string, days []DailyReturn, values, excess []float64,
) SegmentStats {
	if len(days) == 0 {
		return SegmentStats{Label: label}
	}
	returns := make([]float64, len(days))
	growth := 1.0
	for i, dr := range days {
		returns[i] = dr.Return
		growth *= 1 + dr.Return
	}
	numYears := 0.0
	if len(days) > 1 {
		numYears = days[len(days)-1].Date.Sub(days[0].Date).Hours() / 24 / 365.25
	}
	return SegmentStats{
		Label:        label,
		Start:        days[0].Date.Format("2006-01-02"),
		End:          days[len(days)-1].Date.Format("2006-01-02"),
		Days:         len(days),
		TotalReturn:  (growth - 1) * 100,
		AnnualReturn: GetAnnualReturn(returns, numYears),
		SharpeRatio:  GetSharpeRatio(excess),
		SortinoRatio: GetSortinoRatio(excess),
		MaxDrawdown:  GetMaxDrawdown(values),
		StandardDev:  stat.StdDev(returns, nil) * math.Sqrt(252.0),
	}
}

// applySplitMetrics fills p.Splits by cutting the run that already happened
// at InSampleEnd. It is a no-op when no split is configured.
//
// The segments are sliced out of one simulation rather than produced by two.
// Running the out-of-sample half separately would start it with a flat
// balance and no positions, which is a different experiment: every strategy
// that holds across the boundary would be measured as if it had been forced
// to liquidate and re-enter there.
//
// A segment shorter than minSegmentDays is dropped with a log line rather
// than an error — by this point the backtest has already succeeded, and a
// sparse calendar is not a reason to throw the result away.
func (p *Portfolio) applySplitMetrics(riskFreeRates map[int64]float64) {
	if p.InSampleEnd.IsZero() || len(p.DailyReturns) == 0 {
		return
	}
	// DailyReturns and PortfolioCloseValues are appended together each day,
	// so they share length and ordering; cut is an index into both.
	cut := len(p.DailyReturns)
	for i, dr := range p.DailyReturns {
		if dr.Date.After(p.InSampleEnd) {
			cut = i
			break
		}
	}
	tail := len(p.DailyReturns) - cut
	if cut < minSegmentDays || tail < minSegmentDays {
		log.Printf(
			"portfolio %q: split at %s leaves %d in-sample and %d "+
				"out-of-sample trading days; fewer than %d either side is "+
				"too few to score, skipping split metrics",
			p.Pname, p.InSampleEnd.Format("2006-01-02"), cut, tail,
			minSegmentDays,
		)
		return
	}

	excess := make([]float64, len(p.DailyReturns))
	for i, dr := range p.DailyReturns {
		excess[i] = dr.Return - riskFreeRates[dr.Date.Unix()]
	}
	p.Splits = []SegmentStats{
		segmentStats("in-sample",
			p.DailyReturns[:cut], p.PortfolioCloseValues[:cut], excess[:cut]),
		segmentStats("out-of-sample",
			p.DailyReturns[cut:], p.PortfolioCloseValues[cut:], excess[cut:]),
	}
}

// --- overfitting statistics -------------------------------------------------
//
// Bailey & López de Prado, "The Deflated Sharpe Ratio: Correcting for
// Selection Bias, Backtest Overfitting and Non-Normality" (Journal of
// Portfolio Management, 2014). The point of both functions below is that the
// best of N backtests is not the same thing as a good strategy: search hard
// enough over a fixed window and something will look excellent by luck alone.
//
// Both take and return ANNUALIZED Sharpe ratios, matching every other figure
// in this file, and de-annualize internally where the published formula needs
// a per-period ratio.

// eulerGamma is the Euler-Mascheroni constant, which appears in the
// expected-maximum-of-N-normals approximation below.
const eulerGamma = 0.5772156649015329

// ExpectedMaxSharpe is the Sharpe the BEST of `trials` strategies would be
// expected to show even if not one of them had any edge — the bar a swept
// winner has to clear before it means anything.
//
// sharpeStdev is the spread of Sharpe ratios actually observed across the
// trials: a sweep whose candidates all behave alike offers luck little room
// to work, while a spread of wildly different Sharpes offers it a lot. Both
// arguments describe the search, not the winner, which is why a single
// untried strategy (trials < 2) has a bar of zero.
func ExpectedMaxSharpe(trials int, sharpeStdev float64) float64 {
	if trials < 2 || sharpeStdev <= 0 || math.IsNaN(sharpeStdev) {
		return 0
	}
	n := float64(trials)
	z := distuv.Normal{Mu: 0, Sigma: 1}
	// E[max of n standard normals] ~= (1-γ)·Φ⁻¹(1 - 1/n) + γ·Φ⁻¹(1 - 1/(n·e))
	expected := (1-eulerGamma)*z.Quantile(1-1/n) +
		eulerGamma*z.Quantile(1-1/(n*math.E))
	out := sharpeStdev * expected
	if math.IsNaN(out) || math.IsInf(out, 0) {
		return 0
	}
	return out
}

// GetDeflatedSharpe is the probability, in [0,1], that a strategy's true
// Sharpe exceeds `benchmark` — given how many observations support it, how
// non-normal its returns are, and (through the benchmark) how hard the search
// that found it had to work.
//
// It is a CONFIDENCE, not a Sharpe: 0.95 means the result survives the
// correction, 0.10 means the same headline Sharpe is most likely selection
// bias. Read alongside ExpectedMaxSharpe, which is on the Sharpe scale.
//
// Returns 0 for series too short or too degenerate to say anything, in
// keeping with the rest of this file: no metric may put a non-finite number
// in the results table.
func GetDeflatedSharpe(observed, benchmark float64, returns []float64) float64 {
	return deflatedSharpeFromMoments(
		observed, benchmark, len(returns),
		stat.Skew(returns, nil),
		// The formula uses raw kurtosis; gonum reports excess.
		stat.ExKurtosis(returns, nil)+3.0,
	)
}

// deflatedSharpeFromMoments is the core of GetDeflatedSharpe, taking the
// return series' moments rather than the series itself. The runner needs this
// form: it computes the correction after collecting every result, by which
// point the raw returns are gone but their moments were captured.
func deflatedSharpeFromMoments(
	observed, benchmark float64, n int, skew, kurtosis float64,
) float64 {
	t := float64(n)
	if t < 2 || math.IsNaN(skew) || math.IsNaN(kurtosis) {
		return 0
	}
	const periods = 252.0
	sr := observed / math.Sqrt(periods)
	srBench := benchmark / math.Sqrt(periods)

	// Variance of the Sharpe estimator under non-normal returns.
	variance := 1 - skew*sr + (kurtosis-1)/4*sr*sr
	if variance <= 0 || math.IsNaN(variance) {
		return 0
	}
	z := (sr - srBench) * math.Sqrt(t-1) / math.Sqrt(variance)
	if math.IsNaN(z) {
		return 0
	}
	p := distuv.Normal{Mu: 0, Sigma: 1}.CDF(z)
	if math.IsNaN(p) {
		return 0
	}
	// Guard the tails against float noise pushing it outside [0,1].
	return math.Min(1, math.Max(0, p))
}
