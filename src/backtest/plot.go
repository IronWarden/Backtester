package backtest

// What the strategy saw.
//
// A script can already say things with log(), and the console shows them. What
// it could not do was show a NUMBER over time: the moving average it is
// crossing, the z-score it is waiting on, the position size it thinks it is
// holding. Those are the things a person needs to see to understand why a
// strategy did what it did, and reading them out of a thousand log lines is
// not seeing them.
//
// plot(name, value) records one value per series per day, and the results
// panel draws each series against the same window as the equity curve. It is a
// debugging and teaching instrument, not a metric: nothing here reaches a
// number the engine reports.

import "math"

// Plot bounds. A strategy can call plot() inside a loop over every ticker,
// which is how a debugging aid turns into a hundred megabytes of JSON. Both
// limits are per portfolio and both report themselves once rather than
// silently dropping data.
const (
	// maxPlotSeries is how many distinct names one portfolio may plot. Beyond
	// this a chart is unreadable anyway.
	maxPlotSeries = 8
	// maxPlotPoints bounds a single series. One value per day is the shape
	// this records, so the cap is only reached by a run far longer than any
	// available history -- it exists for the pathological case, not the
	// ordinary one.
	maxPlotPoints = 20000
)

// PlotPoint is one value on one simulated day. Sparse on purpose: a series
// that starts on day 200 because that is when its indicator has history
// carries no leading filler, and the chart draws it from where it begins.
type PlotPoint struct {
	Day   int     `json:"day"`
	Value float64 `json:"value"`
}

// PlotSeries is everything one plot(name, ...) recorded, in day order.
type PlotSeries struct {
	Name   string      `json:"name"`
	Points []PlotPoint `json:"points"`
}

// plotRecorder collects a run's series. One per portfolio, written only by
// that portfolio's own worker -- the engine clones a portfolio before handing
// it to a goroutine, so nothing here is shared across them.
type plotRecorder struct {
	order   []string
	byName  map[string]*PlotSeries
	dropped bool // a name was refused past maxPlotSeries
	full    bool // a series hit maxPlotPoints
}

// record adds or replaces the value for a day.
//
// The LAST call on a given day wins, so a script that plots inside a loop over
// tickers records the loop's final value rather than one point per iteration.
// That is what keeps a series at one value per day and a chart at one line;
// a script wanting each ticker plots each under its own name.
func (r *plotRecorder) record(name string, day int, value float64) {
	// NaN and ±Inf have no place on a chart and do not survive JSON. Dropping
	// the point is right: the alternative is a series that fails to serialise
	// and takes the whole result with it.
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	if r.byName == nil {
		r.byName = make(map[string]*PlotSeries, maxPlotSeries)
	}
	s, ok := r.byName[name]
	if !ok {
		if len(r.order) >= maxPlotSeries {
			r.dropped = true
			return
		}
		s = &PlotSeries{Name: name}
		r.byName[name] = s
		r.order = append(r.order, name)
	}
	if n := len(s.Points); n > 0 && s.Points[n-1].Day == day {
		s.Points[n-1].Value = value
		return
	}
	if len(s.Points) >= maxPlotPoints {
		r.full = true
		return
	}
	s.Points = append(s.Points, PlotPoint{Day: day, Value: value})
}

// series returns the recorded series in the order they were first plotted, so
// the chart's colours follow the script's own order rather than a map's.
func (r *plotRecorder) series() []PlotSeries {
	if len(r.order) == 0 {
		return nil
	}
	out := make([]PlotSeries, 0, len(r.order))
	for _, name := range r.order {
		out = append(out, *r.byName[name])
	}
	return out
}

// reportLimits says once, at the end of a run, that a bound was reached.
// Reporting per call would fill the console with the same line thousands of
// times; reporting not at all would leave a chart quietly missing the series
// the user was looking for.
func (r *plotRecorder) reportLimits(portfolio string) {
	if r.dropped {
		diagf("warn", portfolio,
			"plot() recorded the first %d series and ignored the rest; "+
				"name fewer series, or plot one value per ticker under one name",
			maxPlotSeries)
	}
	if r.full {
		diagf("warn", portfolio,
			"a plot() series reached %d points and stopped recording; "+
				"plot() keeps one value per day, so this means it was called "+
				"with days that never advanced", maxPlotPoints)
	}
}
