package backtest

import (
	"math"
	"time"
)

// Time-sliced views of a finished run. Everything here is derived from the
// daily close values and returns the simulation already recorded, so none
// of it can affect a backtest — these are read-only lenses over a result.
//
// All of it is date-driven, so each function takes the equity curve and the
// matching dates. They must be the same length and in chronological order,
// which is how the runner records them; mismatched input yields no output
// rather than a guess.

// Drawdown is one peak-to-trough decline and its recovery.
type Drawdown struct {
	// Start is the peak the decline began from, Trough the low point, and
	// Recovered the day the equity curve first regained Start. For a
	// drawdown still open at the end of the run, Recovered is the zero time
	// and Ongoing is true.
	Start     time.Time `json:"start"`
	Trough    time.Time `json:"trough"`
	Recovered time.Time `json:"recovered"`
	Ongoing   bool      `json:"ongoing"`
	// DepthPct is the decline from peak to trough, as a positive percent.
	DepthPct float64 `json:"depthPct"`
	// DurationDays counts calendar days from Start to Trough; RecoveryDays
	// from Trough to Recovered, and is 0 while Ongoing.
	DurationDays int `json:"durationDays"`
	RecoveryDays int `json:"recoveryDays"`
}

// PeriodReturn is one calendar slice of the equity curve.
type PeriodReturn struct {
	// Period is "2024" for a year or "2024-03" for a month.
	Period string `json:"period"`
	// ReturnPct is the compounded return across the slice, in percent.
	ReturnPct float64 `json:"returnPct"`
	// Partial marks a slice the run does not fully span — the first and
	// last are usually partial. Partial periods are NOT annualized or
	// extrapolated; the figure is simply the return over the days present.
	Partial bool `json:"partial"`
	// Start and End are the first and last trading days in the slice.
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// RollingPoint is one window's annualized Sharpe ratio, dated at the last
// day of the window.
type RollingPoint struct {
	Date  time.Time `json:"date"`
	Value float64   `json:"value"`
}

// seriesOK reports whether an equity curve and its dates can be analyzed.
func seriesOK(values []float64, dates []time.Time) bool {
	return len(values) > 0 && len(values) == len(dates)
}

// GetDrawdowns returns every peak-to-trough decline in the equity curve,
// deepest first, keeping at most topN (all of them when topN <= 0).
//
// A decline still underwater on the final day is reported with Ongoing set
// rather than dropped — an unrecovered drawdown is usually the single most
// interesting thing about a run, and silently omitting it would be the
// worst possible failure mode for this function.
func GetDrawdowns(values []float64, dates []time.Time, topN int) []Drawdown {
	if !seriesOK(values, dates) {
		return nil
	}

	var out []Drawdown
	peak := values[0]
	peakIdx := 0
	troughIdx := -1

	// close finishes the drawdown that started at peakIdx, if there was one.
	closeDrawdown := func(recoveredIdx int, ongoing bool) {
		if troughIdx < 0 {
			return
		}
		depth := (peak - values[troughIdx]) / peak * 100.0
		if math.IsNaN(depth) || math.IsInf(depth, 0) || depth <= 0 {
			troughIdx = -1
			return
		}
		d := Drawdown{
			Start:        dates[peakIdx],
			Trough:       dates[troughIdx],
			Ongoing:      ongoing,
			DepthPct:     depth,
			DurationDays: daysBetween(dates[peakIdx], dates[troughIdx]),
		}
		if !ongoing {
			d.Recovered = dates[recoveredIdx]
			d.RecoveryDays = daysBetween(dates[troughIdx], dates[recoveredIdx])
		}
		out = append(out, d)
		troughIdx = -1
	}

	for i := 1; i < len(values); i++ {
		switch {
		case values[i] >= peak:
			// Regaining the peak ends any decline that was in progress.
			closeDrawdown(i, false)
			peak = values[i]
			peakIdx = i
		case troughIdx < 0 || values[i] < values[troughIdx]:
			troughIdx = i
		}
	}
	// Whatever is still underwater on the last day is an open drawdown.
	closeDrawdown(len(values)-1, true)

	sortDrawdownsByDepth(out)
	if topN > 0 && len(out) > topN {
		out = out[:topN]
	}
	return out
}

// sortDrawdownsByDepth orders deepest first, breaking ties by start date so
// the output is deterministic.
func sortDrawdownsByDepth(ds []Drawdown) {
	for i := 1; i < len(ds); i++ {
		for j := i; j > 0; j-- {
			deeper := ds[j].DepthPct > ds[j-1].DepthPct
			sameDepth := ds[j].DepthPct == ds[j-1].DepthPct
			earlier := ds[j].Start.Before(ds[j-1].Start)
			if deeper || (sameDepth && earlier) {
				ds[j], ds[j-1] = ds[j-1], ds[j]
				continue
			}
			break
		}
	}
}

func daysBetween(a, b time.Time) int {
	return int(b.Sub(a).Hours() / 24)
}

// GetRollingSharpe returns the annualized Sharpe ratio over each trailing
// window of the given size, dated at the window's last day. Windows shorter
// than the requested size are not emitted, so a run shorter than one window
// yields nothing rather than a misleadingly short-sampled figure.
//
// The returns passed here are the portfolio's daily returns; pass excess
// returns to get an excess-return Sharpe, matching GetSharpeRatio.
func GetRollingSharpe(
	returns []float64, dates []time.Time, window int,
) []RollingPoint {
	if window < 2 || len(returns) != len(dates) || len(returns) < window {
		return nil
	}
	out := make([]RollingPoint, 0, len(returns)-window+1)
	for end := window; end <= len(returns); end++ {
		out = append(out, RollingPoint{
			Date:  dates[end-1],
			Value: GetSharpeRatio(returns[end-window : end]),
		})
	}
	return out
}

// GetCalendarReturns compounds the equity curve into calendar slices —
// per year when byMonth is false, per month when it is true.
//
// The first and last slices are usually partial, since a run rarely starts
// on 1 January or ends on 31 December. They are marked Partial and are
// reported as the plain return over the days actually present: no
// annualizing, no extrapolation, because inventing the missing days is
// exactly how a chart starts lying.
//
// Each slice's return is measured from the previous slice's closing value,
// so the compounded slices reproduce the run's total return. The first
// slice is the exception: it has no predecessor and so is measured from its
// own first day, which means the very first day's move is not counted in
// any slice — the same convention the equity curve itself uses, where day 0
// is the starting value rather than a return.
func GetCalendarReturns(
	values []float64, dates []time.Time, byMonth bool,
) []PeriodReturn {
	if !seriesOK(values, dates) {
		return nil
	}

	key := func(t time.Time) string {
		if byMonth {
			return t.Format("2006-01")
		}
		return t.Format("2006")
	}

	var out []PeriodReturn
	start := 0
	for i := 1; i <= len(values); i++ {
		if i < len(values) && key(dates[i]) == key(dates[start]) {
			continue
		}
		// [start, i) is one calendar slice.
		base := values[start]
		if start > 0 {
			base = values[start-1]
		}
		end := i - 1
		ret := 0.0
		if base > 0 {
			ret = (values[end] - base) / base * 100.0
		}
		if math.IsNaN(ret) || math.IsInf(ret, 0) {
			ret = 0
		}
		out = append(out, PeriodReturn{
			Period:    key(dates[start]),
			ReturnPct: ret,
			Partial:   isPartial(dates[start], dates[end], byMonth),
			Start:     dates[start],
			End:       dates[end],
		})
		start = i
	}
	return out
}

// isPartial reports whether a slice fails to span its whole calendar
// period. Trading calendars have holidays and weekends, so this asks
// whether the slice reaches the period's first and last *calendar* days
// within a week's tolerance rather than demanding exact boundaries.
func isPartial(first, last time.Time, byMonth bool) bool {
	var periodStart, periodEnd time.Time
	if byMonth {
		periodStart = time.Date(first.Year(), first.Month(), 1, 0, 0, 0, 0, first.Location())
		periodEnd = periodStart.AddDate(0, 1, -1)
	} else {
		periodStart = time.Date(first.Year(), 1, 1, 0, 0, 0, 0, first.Location())
		periodEnd = time.Date(first.Year(), 12, 31, 0, 0, 0, 0, first.Location())
	}
	const toleranceDays = 7
	return daysBetween(periodStart, first) > toleranceDays ||
		daysBetween(last, periodEnd) > toleranceDays
}
