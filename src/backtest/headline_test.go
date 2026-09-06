package backtest

import (
	"strings"
	"testing"
	"time"
)

// dailyDates returns n consecutive calendar days from start, which is close
// enough to a trading calendar for a sentence that talks in months and years.
func dailyDates(start string, n int) []string {
	t0, err := time.Parse("2006-01-02", start)
	if err != nil {
		panic(err)
	}
	out := make([]string, n)
	for i := range out {
		out[i] = t0.AddDate(0, 0, i).Format("2006-01-02")
	}
	return out
}

func TestSentence(t *testing.T) {
	fourYears := dailyDates("2020-01-02", 4*365+1)
	sevenMonths := dailyDates("2020-01-02", 213)

	cases := []struct {
		name    string
		result  Result
		want    []string // substrings that must appear
		notWant []string // claims the sentence must never make
	}{
		{
			name: "a good four years, against a benchmark",
			result: Result{
				Dates:          fourYears,
				InitialCapital: 100_000,
				FinalValue:     184_300,
				Metrics: Metrics{
					AnnualReturn:          16.5,
					MaxDrawdown:           34,
					LongestUnderwaterDays: 426,
				},
				BenchmarkStats: BenchmarkStats{Ticker: "$SP500", AnnualReturn: 14.2},
			},
			want: []string{
				"$100,000 became $184,300 over 4 years",
				"16.5% a year",
				"The worst fall was 34%",
				"longest stretch below a previous high was 14 months",
				"Holding $SP500 instead would have made 14.2% a year",
				"2.3 points ahead.",
			},
			notWant: []string{"made that back", "paid in"},
		},
		{
			// The case a beginner most needs stated plainly, and the one a
			// percentage sign alone makes easy to misread.
			name: "a loss says so in words",
			result: Result{
				Dates:          fourYears,
				InitialCapital: 100_000,
				FinalValue:     61_000,
				Metrics:        Metrics{AnnualReturn: -11.6, MaxDrawdown: 52},
			},
			want: []string{"$100,000 became $61,000", "a loss of 11.6% a year"},
			// A minus sign in the middle of a sentence is the one character
			// that reverses its meaning and the first one a reader skips.
			notWant: []string{"a gain of", "-11.6"},
		},
		{
			// Annualizing 213 days would turn a 3% run into a double-digit
			// "year" that never happened.
			name: "a window under a year is never annualized",
			result: Result{
				Dates:          sevenMonths,
				InitialCapital: 10_000,
				FinalValue:     10_300,
				Metrics:        Metrics{AnnualReturn: 5.2, MaxDrawdown: 4},
			},
			want:    []string{"over 7 months", "a gain of 3.0%"},
			notWant: []string{"5.2% a year", "a year."},
		},
		{
			// Two figures in one sentence must be the same kind of figure. A
			// total portfolio return beside an annualized benchmark return is
			// a comparison of two different things.
			name: "a short window compares totals, not annualized figures",
			result: Result{
				Dates:          sevenMonths,
				InitialCapital: 10_000,
				FinalValue:     10_300,
				Metrics:        Metrics{AnnualReturn: 5.2, MaxDrawdown: 4},
				BenchmarkStats: BenchmarkStats{Ticker: "$SP500", AnnualReturn: 9.9},
				BenchmarkCurve: []float64{10_000, 10_580},
			},
			want:    []string{"a gain of 3.0%", "Holding $SP500 instead would have been a gain of 5.8% over the same time."},
			notWant: []string{"9.9%", "a year"},
		},
		{
			// With no curve there is nothing honest to say at this length, so
			// the clause is dropped rather than filled with the annualized
			// figure that happens to be at hand.
			name: "a short window with no benchmark curve says nothing about it",
			result: Result{
				Dates:          sevenMonths,
				InitialCapital: 10_000,
				FinalValue:     10_300,
				Metrics:        Metrics{AnnualReturn: 5.2, MaxDrawdown: 4},
				BenchmarkStats: BenchmarkStats{Ticker: "$SP500", AnnualReturn: 9.9},
			},
			notWant: []string{"Holding", "9.9"},
		},
		{
			name: "a benchmark that lost money did not \"make\" it",
			result: Result{
				Dates:          fourYears,
				InitialCapital: 100_000,
				FinalValue:     150_000,
				Metrics:        Metrics{AnnualReturn: 10.6, MaxDrawdown: 18},
				BenchmarkStats: BenchmarkStats{Ticker: "$SP500", AnnualReturn: -3.2},
			},
			want:    []string{"would have lost 3.2% a year", "13.8 points ahead"},
			notWant: []string{"made -3.2", "-3.2%"},
		},
		{
			name: "no benchmark, no comparison",
			result: Result{
				Dates:          fourYears,
				InitialCapital: 100_000,
				FinalValue:     120_000,
				Metrics:        Metrics{AnnualReturn: 4.7, MaxDrawdown: 12},
			},
			notWant: []string{"instead", "Holding"},
		},
		{
			// Recovery is not knowable from the deepest few drawdowns, so the
			// sentence may only report the absence of it.
			name: "a decline never recovered",
			result: Result{
				Dates:          fourYears,
				InitialCapital: 100_000,
				FinalValue:     70_000,
				Metrics:        Metrics{AnnualReturn: -8.6, MaxDrawdown: 41, LongestUnderwaterDays: 900},
				Drawdowns:      []Drawdown{{DepthPct: 41, Ongoing: true}},
			},
			want: []string{"It had not made that back by the end."},
		},
		{
			name: "money paid in is not performance",
			result: Result{
				Dates:          fourYears,
				InitialCapital: 100_000,
				FinalValue:     184_300,
				Metrics: Metrics{
					AnnualReturn:     16.5,
					MaxDrawdown:      20,
					NetContributions: 24_000,
				},
			},
			want: []string{"$24,000 of that was money you paid in, not gains."},
		},
		{
			name: "money taken out is named too",
			result: Result{
				Dates:          fourYears,
				InitialCapital: 100_000,
				FinalValue:     54_000,
				Metrics: Metrics{
					AnnualReturn:     -14.3,
					MaxDrawdown:      30,
					NetContributions: -40_000,
				},
			},
			want: []string{"You also took out $40,000"},
		},
		{
			// For a decumulation backtest this is not a footnote, it is the
			// result, so it leads.
			name: "a plan that ran out of money leads with it",
			result: Result{
				Dates:          fourYears,
				InitialCapital: 100_000,
				FinalValue:     0,
				Metrics: Metrics{
					AnnualReturn:      -100,
					MaxDrawdown:       100,
					WithdrawalsMissed: 7,
					NetContributions:  -90_000,
				},
			},
			want: []string{"The money ran out: 7 scheduled withdrawals could not be paid."},
		},
		{
			name: "a benchmark that matched is not dressed up as a difference",
			result: Result{
				Dates:          fourYears,
				InitialCapital: 100_000,
				FinalValue:     150_000,
				Metrics:        Metrics{AnnualReturn: 10.6, MaxDrawdown: 18},
				BenchmarkStats: BenchmarkStats{Ticker: "$SP500", AnnualReturn: 10.62},
			},
			want:    []string{"about the same"},
			notWant: []string{"points ahead", "points behind"},
		},
		{
			name: "a run with no simulated days says nothing at all",
			result: Result{
				InitialCapital: 100_000,
			},
			notWant: []string{"$"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Sentence(tc.result)
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("sentence is missing %q:\n%s", want, got)
				}
			}
			for _, never := range tc.notWant {
				if strings.Contains(got, never) {
					t.Errorf("sentence claims %q, which this result does not support:\n%s", never, got)
				}
			}
		})
	}
}

// The first case must hold for the ordinary result too: whatever else the
// sentence says, it must agree with the metrics it was built from.
func TestSentenceStatesNoFigureTheResultDoesNotHave(t *testing.T) {
	r := Result{
		Dates:          dailyDates("2015-06-01", 2000),
		InitialCapital: 25_000,
		FinalValue:     31_500,
		Metrics:        Metrics{AnnualReturn: 4.3, MaxDrawdown: 22.4, LongestUnderwaterDays: 300},
	}
	got := Sentence(r)
	for _, want := range []string{"$25,000", "$31,500", "4.3%", "22%", "5.5 years"} {
		if !strings.Contains(got, want) {
			t.Errorf("sentence is missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "0.0%") {
		t.Errorf("a zero crept in where no figure was computed:\n%s", got)
	}
}

func TestHumanSpan(t *testing.T) {
	cases := []struct {
		days int
		want string
	}{
		{1, "1 day"},
		{20, "20 days"},
		{45, "1 month"},
		{213, "7 months"},
		{365, "12 months"},
		{729, "24 months"},
		{730, "2 years"},
		{1461, "4 years"},
		{2000, "5.5 years"},
	}
	for _, tc := range cases {
		if got := humanSpan(tc.days); got != tc.want {
			t.Errorf("humanSpan(%d) = %q, want %q", tc.days, got, tc.want)
		}
	}
}

func TestPlainDollars(t *testing.T) {
	cases := []struct {
		v    float64
		want string
	}{
		{0, "$0"},
		{999, "$999"},
		{1000, "$1,000"},
		{184_312.47, "$184,312"},
		{1_234_567, "$1,234,567"},
		{-2500, "−$2,500"},
	}
	for _, tc := range cases {
		if got := plainDollars(tc.v); got != tc.want {
			t.Errorf("plainDollars(%v) = %q, want %q", tc.v, got, tc.want)
		}
	}
}
