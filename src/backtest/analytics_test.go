package backtest

import (
	"math"
	"testing"
	"time"
)

var anchor = time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

// days returns n consecutive daily timestamps from anchor.
func days(n int) []time.Time {
	out := make([]time.Time, n)
	for i := range out {
		out[i] = anchor.AddDate(0, 0, i)
	}
	return out
}

// The analytics attached to a Result must describe the same run the
// scalar metrics describe. This drives a real strategy through runOne and
// reconciles the two.
func TestResultAnalyticsMatchTheRun(t *testing.T) {
	hist := synthHist(42)
	p := libPortfolio(nil)
	strat, err := NewLuaStrategy(
		strategiesDir(t)+"/rebalance.lua", shippedStrategies()["rebalance.lua"],
	)
	if err != nil {
		t.Fatalf("NewLuaStrategy: %v", err)
	}
	defer strat.Close()
	p.Strategy = strat
	runOne(p, hist, map[int64]float64{})

	dayTimes := make([]time.Time, len(p.DailyReturns))
	returns := make([]float64, len(p.DailyReturns))
	for i, dr := range p.DailyReturns {
		dayTimes[i] = dr.Date
		returns[i] = dr.Return
	}

	// The deepest reported drawdown must equal the metric the results table
	// already shows.
	ds := GetDrawdowns(p.PortfolioCloseValues, dayTimes, topDrawdowns)
	if len(ds) == 0 {
		t.Fatal("a real run produced no drawdowns at all")
	}
	closeTo(t, "deepest drawdown vs MaxDrawdown",
		ds[0].DepthPct, p.Metrics.MaxDrawdown)
	if len(ds) > topDrawdowns {
		t.Errorf("got %d drawdowns, capped at %d", len(ds), topDrawdowns)
	}
	for _, d := range ds {
		if d.DepthPct <= 0 {
			t.Errorf("non-positive drawdown depth: %+v", d)
		}
		if d.Trough.Before(d.Start) {
			t.Errorf("trough precedes peak: %+v", d)
		}
		if !d.Ongoing && d.Recovered.Before(d.Trough) {
			t.Errorf("recovery precedes trough: %+v", d)
		}
	}

	// Compounded yearly returns reproduce the run's total return.
	years := GetCalendarReturns(p.PortfolioCloseValues, dayTimes, false)
	if len(years) == 0 {
		t.Fatal("no calendar years")
	}
	compounded := 1.0
	for _, y := range years {
		compounded *= 1 + y.ReturnPct/100.0
	}
	curve := p.PortfolioCloseValues
	want := curve[len(curve)-1] / curve[0]
	if math.Abs(compounded-want) > 1e-9 {
		t.Errorf("compounded years %v, actual total %v", compounded, want)
	}

	// The rolling series is dated within the run and never non-finite.
	for _, pt := range GetRollingSharpe(returns, dayTimes, rollingSharpeWindow) {
		if math.IsNaN(pt.Value) || math.IsInf(pt.Value, 0) {
			t.Errorf("rolling Sharpe at %v = %v — must be finite", pt.Date, pt.Value)
		}
		if pt.Date.Before(dayTimes[0]) || pt.Date.After(dayTimes[len(dayTimes)-1]) {
			t.Errorf("rolling point dated outside the run: %v", pt.Date)
		}
	}
}

func TestGetDrawdownsBasics(t *testing.T) {
	// 100 -> 80 (-20%) -> back to 100 -> 200 -> 150 (-25%) -> 200 again.
	values := []float64{100, 80, 100, 200, 150, 200}
	got := GetDrawdowns(values, days(len(values)), 0)

	if len(got) != 2 {
		t.Fatalf("found %d drawdowns, want 2: %+v", len(got), got)
	}
	// Deepest first.
	closeTo(t, "deepest depth", got[0].DepthPct, 25)
	closeTo(t, "shallower depth", got[1].DepthPct, 20)

	deep := got[0]
	if !deep.Start.Equal(anchor.AddDate(0, 0, 3)) {
		t.Errorf("deep start = %v, want day 3 (the 200 peak)", deep.Start)
	}
	if !deep.Trough.Equal(anchor.AddDate(0, 0, 4)) {
		t.Errorf("deep trough = %v, want day 4", deep.Trough)
	}
	if !deep.Recovered.Equal(anchor.AddDate(0, 0, 5)) {
		t.Errorf("deep recovery = %v, want day 5", deep.Recovered)
	}
	if deep.Ongoing {
		t.Error("a recovered drawdown must not be marked ongoing")
	}
	if deep.DurationDays != 1 || deep.RecoveryDays != 1 {
		t.Errorf("durations = %d/%d, want 1/1", deep.DurationDays, deep.RecoveryDays)
	}
}

// The deepest drawdown must agree exactly with the already-tested
// GetMaxDrawdown. That is a free cross-check against code with its own
// pinned tests, so any disagreement means this file is wrong.
func TestDrawdownsAgreeWithMaxDrawdown(t *testing.T) {
	cases := [][]float64{
		{100, 80, 100, 200, 150, 200},
		{100, 50},
		{100, 110, 120},
		{100, 50, 100},
		{100, 80, 200, 150},
		{100, 90, 80, 70, 60},
		{50, 100, 25, 200, 100, 300},
		{100, 100, 100},
	}
	for _, values := range cases {
		want := GetMaxDrawdown(values)
		ds := GetDrawdowns(values, days(len(values)), 0)
		var got float64
		if len(ds) > 0 {
			got = ds[0].DepthPct
		}
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("values %v: deepest drawdown %v, GetMaxDrawdown %v",
				values, got, want)
		}
	}
}

// A decline still underwater on the last day must be reported, not dropped.
func TestGetDrawdownsOngoing(t *testing.T) {
	values := []float64{100, 120, 60}
	got := GetDrawdowns(values, days(len(values)), 0)

	if len(got) != 1 {
		t.Fatalf("found %d drawdowns, want 1: %+v", len(got), got)
	}
	d := got[0]
	if !d.Ongoing {
		t.Error("an unrecovered drawdown must be marked ongoing")
	}
	if !d.Recovered.IsZero() {
		t.Errorf("ongoing drawdown has a recovery date: %v", d.Recovered)
	}
	if d.RecoveryDays != 0 {
		t.Errorf("ongoing drawdown has %d recovery days, want 0", d.RecoveryDays)
	}
	closeTo(t, "depth", d.DepthPct, 50)
}

func TestGetDrawdownsTopN(t *testing.T) {
	// Three distinct drawdowns: 20%, 50%, 10%.
	values := []float64{100, 80, 100, 50, 100, 90, 100}
	all := GetDrawdowns(values, days(len(values)), 0)
	if len(all) != 3 {
		t.Fatalf("found %d drawdowns, want 3: %+v", len(all), all)
	}
	// Sorted deepest first.
	for i := 1; i < len(all); i++ {
		if all[i].DepthPct > all[i-1].DepthPct {
			t.Errorf("not sorted by depth: %+v", all)
		}
	}
	top := GetDrawdowns(values, days(len(values)), 2)
	if len(top) != 2 {
		t.Fatalf("topN=2 returned %d", len(top))
	}
	closeTo(t, "deepest", top[0].DepthPct, 50)
	closeTo(t, "second", top[1].DepthPct, 20)
}

func TestGetDrawdownsDegenerate(t *testing.T) {
	cases := []struct {
		name   string
		values []float64
		dates  []time.Time
	}{
		{"nil", nil, nil},
		{"empty", []float64{}, []time.Time{}},
		{"single day", []float64{100}, days(1)},
		{"monotonic rise", []float64{100, 110, 120}, days(3)},
		{"flat", []float64{100, 100, 100}, days(3)},
		{"mismatched lengths", []float64{100, 90}, days(3)},
		{"no dates", []float64{100, 90}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := GetDrawdowns(tc.values, tc.dates, 0); len(got) != 0 {
				t.Errorf("GetDrawdowns = %+v, want none", got)
			}
		})
	}
}

func TestGetRollingSharpe(t *testing.T) {
	returns := []float64{0.02, 0.0, 0.02, 0.0, 0.02, 0.0}
	d := days(len(returns))

	got := GetRollingSharpe(returns, d, 4)
	if len(got) != 3 {
		t.Fatalf("got %d points from 6 returns at window 4, want 3", len(got))
	}
	// Each point equals the plain Sharpe of its own window, and is dated at
	// the window's last day.
	for i, pt := range got {
		want := GetSharpeRatio(returns[i : i+4])
		closeTo(t, "rolling value", pt.Value, want)
		if !pt.Date.Equal(d[i+3]) {
			t.Errorf("point %d dated %v, want %v", i, pt.Date, d[i+3])
		}
	}

	// A window covering the whole series gives exactly one point equal to
	// the full-series Sharpe.
	full := GetRollingSharpe(returns, d, len(returns))
	if len(full) != 1 {
		t.Fatalf("full-length window gave %d points, want 1", len(full))
	}
	closeTo(t, "full-window Sharpe", full[0].Value, GetSharpeRatio(returns))
}

func TestGetRollingSharpeDegenerate(t *testing.T) {
	returns := []float64{0.01, 0.02, 0.03}
	cases := []struct {
		name    string
		returns []float64
		dates   []time.Time
		window  int
	}{
		{"window longer than the run", returns, days(3), 10},
		{"window of one", returns, days(3), 1},
		{"window of zero", returns, days(3), 0},
		{"negative window", returns, days(3), -5},
		{"mismatched lengths", returns, days(2), 2},
		{"empty", nil, nil, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := GetRollingSharpe(tc.returns, tc.dates, tc.window); got != nil {
				t.Errorf("got %d points, want none", len(got))
			}
		})
	}
}

func TestGetCalendarReturnsByYear(t *testing.T) {
	// Two calendar years, hand-built: 2023 ends at 110 from a 100 start,
	// 2024 ends at 132 from 110.
	dates := []time.Time{
		time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2023, 12, 29, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
	}
	values := []float64{100, 110, 115, 132}

	got := GetCalendarReturns(values, dates, false)
	if len(got) != 2 {
		t.Fatalf("got %d years, want 2: %+v", len(got), got)
	}

	// 2023 is the first slice, measured from its own first value: 100 -> 110.
	if got[0].Period != "2023" {
		t.Errorf("first period = %q", got[0].Period)
	}
	closeTo(t, "2023 return", got[0].ReturnPct, 10)
	if !got[0].Partial {
		t.Error("a year starting in June must be marked partial")
	}

	// 2024 measures from 2023's close: 110 -> 132 = +20%.
	if got[1].Period != "2024" {
		t.Errorf("second period = %q", got[1].Period)
	}
	closeTo(t, "2024 return", got[1].ReturnPct, 20)
	if got[1].Partial {
		t.Error("a full calendar year must not be marked partial")
	}
}

// Compounding the calendar slices must reproduce the run's total return —
// the cross-check that catches an off-by-one in the slice boundaries.
func TestCalendarReturnsCompoundToTotal(t *testing.T) {
	// Three years of daily-ish data at a steady climb.
	var values []float64
	var dates []time.Time
	v := 1000.0
	d := time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 700; i++ {
		v *= 1.0005
		values = append(values, v)
		dates = append(dates, d)
		d = d.AddDate(0, 0, 1)
	}

	for _, byMonth := range []bool{false, true} {
		periods := GetCalendarReturns(values, dates, byMonth)
		if len(periods) == 0 {
			t.Fatal("no periods")
		}
		compounded := 1.0
		for _, p := range periods {
			compounded *= 1 + p.ReturnPct/100.0
		}
		want := values[len(values)-1] / values[0]
		if math.Abs(compounded-want) > 1e-9 {
			t.Errorf("byMonth=%v: compounded %v, total %v", byMonth, compounded, want)
		}
	}
}

func TestGetCalendarReturnsByMonth(t *testing.T) {
	dates := []time.Time{
		time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
	}
	values := []float64{100, 110, 105, 121, 100}

	got := GetCalendarReturns(values, dates, true)
	if len(got) != 3 {
		t.Fatalf("got %d months, want 3: %+v", len(got), got)
	}
	if got[0].Period != "2024-01" || got[1].Period != "2024-02" || got[2].Period != "2024-03" {
		t.Errorf("periods = %q, %q, %q", got[0].Period, got[1].Period, got[2].Period)
	}
	// January: 100 -> 110 (first slice, from its own start).
	closeTo(t, "jan", got[0].ReturnPct, 10)
	// February: from January's close of 110 to 121 = +10%.
	closeTo(t, "feb", got[1].ReturnPct, 10)
	// March: from 121 to 100.
	closeTo(t, "mar", got[2].ReturnPct, (100-121)/121.0*100)

	// A single trading day in March cannot span the month.
	if !got[2].Partial {
		t.Error("a one-day March must be marked partial")
	}
	if got[1].Partial {
		t.Error("a February spanning the 1st to the 29th is not partial")
	}
}

// Partial periods are never annualized or extrapolated: a two-day slice
// reports its two-day move and nothing more.
func TestPartialPeriodsAreNotAnnualized(t *testing.T) {
	dates := []time.Time{
		time.Date(2024, 12, 30, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
	}
	values := []float64{100, 101}

	got := GetCalendarReturns(values, dates, false)
	if len(got) != 1 {
		t.Fatalf("got %d periods, want 1", len(got))
	}
	closeTo(t, "two-day year", got[0].ReturnPct, 1)
	if !got[0].Partial {
		t.Error("a two-day year must be marked partial")
	}
}

func TestGetCalendarReturnsDegenerate(t *testing.T) {
	cases := []struct {
		name   string
		values []float64
		dates  []time.Time
	}{
		{"nil", nil, nil},
		{"empty", []float64{}, []time.Time{}},
		{"mismatched lengths", []float64{100, 90}, days(3)},
		{"no dates", []float64{100, 90}, nil},
	}
	for _, tc := range cases {
		for _, byMonth := range []bool{false, true} {
			t.Run(tc.name, func(t *testing.T) {
				if got := GetCalendarReturns(tc.values, tc.dates, byMonth); got != nil {
					t.Errorf("got %+v, want none", got)
				}
			})
		}
	}

	// A single day is a legitimate, if degenerate, period: zero return.
	single := GetCalendarReturns([]float64{100}, days(1), false)
	if len(single) != 1 {
		t.Fatalf("single day gave %d periods, want 1", len(single))
	}
	closeTo(t, "single-day return", single[0].ReturnPct, 0)

	// A portfolio that reaches zero must not produce Inf or NaN.
	zeroed := GetCalendarReturns([]float64{100, 0, 0}, days(3), false)
	for _, p := range zeroed {
		if math.IsNaN(p.ReturnPct) || math.IsInf(p.ReturnPct, 0) {
			t.Errorf("period %s = %v — must be finite", p.Period, p.ReturnPct)
		}
	}
}
