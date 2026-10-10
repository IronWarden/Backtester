package backtest

import (
	"math"
	"testing"
	"time"

	"my-backtester/src/data"
)

func cpiAt(year int, month time.Month, value float64) data.CPIPoint {
	return data.CPIPoint{
		Date:  time.Date(year, month, 1, 0, 0, 0, 0, time.UTC),
		Value: value,
	}
}

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// The safety property this whole feature turns on. A window whose end month
// has no reading must report nothing, NOT deflate by a shorter span: the
// latter overstates the real return, silently, in the flattering direction.
func TestInflationFactorRefusesAMissingEndpoint(t *testing.T) {
	series := []data.CPIPoint{
		cpiAt(2015, time.January, 100),
		cpiAt(2015, time.February, 101),
		// March deliberately absent — CPI is published in arrears.
	}
	if _, ok := InflationFactor(series, day(2015, 1, 5), day(2015, 3, 20)); ok {
		t.Error("reported a factor for a window ending in a month with no CPI " +
			"reading — that deflates by less inflation than the window actually " +
			"saw, which overstates the real return")
	}
	if _, ok := InflationFactor(series, day(2014, 12, 1), day(2015, 2, 20)); ok {
		t.Error("reported a factor for a window STARTING before the series")
	}
	f, ok := InflationFactor(series, day(2015, 1, 5), day(2015, 2, 20))
	if !ok {
		t.Fatal("refused a window both of whose months are present")
	}
	if math.Abs(f-1.01) > 1e-9 {
		t.Errorf("factor = %v, want 1.01", f)
	}
	if _, ok := InflationFactor(nil, day(2015, 1, 5), day(2015, 2, 20)); ok {
		t.Error("reported a factor with no series at all — a database without " +
			"economic_indicators must simply not report a real return")
	}
}

// Exact month, not nearest-available.
func TestCPIForMonthIsExact(t *testing.T) {
	series := []data.CPIPoint{cpiAt(2015, time.January, 100), cpiAt(2015, time.March, 102)}
	if v, ok := data.CPIForMonth(series, day(2015, 1, 31)); !ok || v != 100 {
		t.Errorf("January lookup gave (%v, %v), want (100, true)", v, ok)
	}
	if _, ok := data.CPIForMonth(series, day(2015, 2, 15)); ok {
		t.Error("February resolved to a neighbouring month's reading")
	}
}

// The compounding form, not `nominal - inflation`. Over a decade the
// difference is not a rounding detail, which is the whole reason this metric
// exists.
func TestRealAnnualReturnCompoundsRatherThanSubtracts(t *testing.T) {
	// Ten years at exactly 10%/yr nominal, 3%/yr inflation.
	const years = 10.0
	daily := make([]float64, 2520)
	r := math.Pow(1.10, 1/252.0) - 1
	for i := range daily {
		daily[i] = r
	}
	factor := math.Pow(1.03, years)

	got := GetRealAnnualReturn(daily, years, factor)
	want := (1.10/1.03 - 1) * 100 // 6.7961...

	if math.Abs(got-want) > 1e-6 {
		t.Errorf("real return = %.6f, want %.6f", got, want)
	}
	// And explicitly NOT the naive subtraction, which would be 7.00.
	if math.Abs(got-7.0) < 0.1 {
		t.Errorf("real return %.4f looks like nominal-minus-inflation; it must "+
			"be the compounded form", got)
	}
}

// realFromNominal is used for the benchmark, which keeps no daily series. The
// comment on it claims algebraic identity with the daily-compounding form;
// this is that claim, checked.
func TestRealFromNominalAgreesWithTheDailyForm(t *testing.T) {
	for _, tc := range []struct{ annual, years, factor float64 }{
		{10, 10, math.Pow(1.03, 10)},
		{-4, 3.5, math.Pow(1.02, 3.5)},
		{25, 1, 1.06},
		{0, 7, math.Pow(1.025, 7)},
	} {
		nominalGrowth := math.Pow(1+tc.annual/100, tc.years)
		n := 252
		daily := make([]float64, n)
		r := math.Pow(nominalGrowth, 1/float64(n)) - 1
		for i := range daily {
			daily[i] = r
		}
		a := GetRealAnnualReturn(daily, tc.years, tc.factor)
		b := realFromNominal(tc.annual, tc.years, tc.factor)
		if math.Abs(a-b) > 1e-6 {
			t.Errorf("annual=%v years=%v: daily form %.8f vs closed form %.8f",
				tc.annual, tc.years, a, b)
		}
	}
}

// Guards that are not decoration: a zero-length window and an absent deflator
// both have to return 0 rather than NaN or +Inf.
func TestRealAnnualReturnGuards(t *testing.T) {
	daily := []float64{0.01, 0.01}
	if got := GetRealAnnualReturn(daily, 0, 1.05); got != 0 {
		t.Errorf("numYears=0 gave %v, want 0", got)
	}
	if got := GetRealAnnualReturn(daily, 5, 0); got != 0 {
		t.Errorf("no inflation factor gave %v, want 0", got)
	}
	if got := realFromNominal(10, 5, 0); got != 0 {
		t.Errorf("realFromNominal with no factor gave %v, want 0", got)
	}
}

// End to end: the same run with and without a CPI series must differ in
// exactly one metric. This is the guarantee that every existing config keeps
// producing identical numbers.
func TestCPIChangesOnlyTheRealReturn(t *testing.T) {
	closes := make([]float64, 400)
	for i := range closes {
		closes[i] = 100 * math.Pow(1.0002, float64(i))
	}

	run := func(cpi []data.CPIPoint) *Portfolio {
		p, err := InitializePortfolio(
			exactCash, exactEpoch, exactEpoch.AddDate(0, 0, 400),
			"cpi", []string{"AAA"}, "buyAndHold:equalWeights", nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		p.CPI = cpi
		runOne(p, histFrom(map[string][]float64{"AAA": closes}), map[int64]float64{})
		return p
	}

	// exactEpoch is 2021-01-04, so the series has to cover 2021 onward.
	series := make([]data.CPIPoint, 24)
	for i := range series {
		d := time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, i, 0)
		series[i] = data.CPIPoint{Date: d, Value: 100 * math.Pow(1.03, float64(i)/12)}
	}

	without := run(nil)
	with := run(series)

	if without.Metrics.RealAnnualReturn != 0 {
		t.Errorf("no CPI should mean no real return, got %v",
			without.Metrics.RealAnnualReturn)
	}
	if with.Metrics.RealAnnualReturn == 0 {
		t.Fatal("CPI present but no real return computed")
	}
	if with.Metrics.RealAnnualReturn >= with.Metrics.AnnualReturn {
		t.Errorf("real %.4f is not below nominal %.4f under positive inflation",
			with.Metrics.RealAnnualReturn, with.Metrics.AnnualReturn)
	}

	// Everything else must be untouched, which is what protects every config
	// written before this feature existed.
	a, b := without.Metrics, with.Metrics
	a.RealAnnualReturn, b.RealAnnualReturn = 0, 0
	if a != b {
		t.Errorf("adding a CPI series moved a metric other than the real "+
			"return:\n without: %+v\n with:    %+v", a, b)
	}
}
