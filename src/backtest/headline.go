package backtest

// The result, in a sentence.
//
// Everything else the app reports is a number in a column: 0.87, 1.24, 34.2,
// 16.5. Every one is documented and every one is honest, and none of them
// tells a person who has never run a backtest whether that was a good four
// years. This says it in English, from figures the engine has already
// computed -- it derives nothing new, so it cannot disagree with the table
// above it.
//
// Written here rather than in the frontend because three surfaces need the
// same words: the desktop app, the -pages export, and the assistant, which
// would otherwise be re-deriving prose from a table on every question.
//
// The rule throughout is that a sentence may be short but may never be wrong.
// Every clause below is either stated from a figure or omitted:
//
//   - a window under a year is never annualized, because a 3% month
//     annualizes to 43% a year and that is not a thing that happened;
//   - recovery is never asserted, only its absence. Result.Drawdowns is the
//     deepest few, not all of them, so "it recovered" is not knowable from
//     here while "it was still down at the end" is;
//   - money paid in by a cashflow schedule is named, because "$100,000 became
//     $184,000" reads as performance when a third of it was deposits;
//   - a plan that ran out of money says so first. For a decumulation backtest
//     that is not a footnote, it is the answer.

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// Sentence renders a result as a short paragraph a beginner can read. It
// returns "" for a result with no simulated days, which is a run that failed
// rather than one worth describing.
func Sentence(r Result) string {
	if len(r.Dates) == 0 || r.InitialCapital <= 0 {
		return ""
	}
	days := spanDays(r.Dates)

	var parts []string
	parts = append(parts, growthClause(r, days))
	if c := contributionClause(r); c != "" {
		parts = append(parts, c)
	}
	if c := fallClause(r); c != "" {
		parts = append(parts, c)
	}
	if c := benchmarkClause(r, days); c != "" {
		parts = append(parts, c)
	}
	if c := ruinClause(r); c != "" {
		// First, not last: everything above it is about a plan that did not
		// survive the window.
		parts = append([]string{c}, parts...)
	}
	return strings.Join(parts, " ")
}

// growthClause is the one clause that is always present: what the money did.
func growthClause(r Result, days int) string {
	period := humanSpan(days)
	// Under a year, an annualized figure is an extrapolation of a sample too
	// short to extrapolate from, so the total is stated instead.
	if days < 365 {
		total := (r.FinalValue/r.InitialCapital - 1) * 100
		return fmt.Sprintf("%s became %s over %s — %s.",
			plainDollars(r.InitialCapital), plainDollars(r.FinalValue), period,
			signedPct(total, "a gain of", "a loss of"))
	}
	return fmt.Sprintf("%s became %s over %s — %s.",
		plainDollars(r.InitialCapital), plainDollars(r.FinalValue), period,
		annualPhrase(r.Metrics.AnnualReturn))
}

// contributionClause names money the user paid in or took out. Without it the
// figures above read as performance.
func contributionClause(r Result) string {
	net := r.Metrics.NetContributions
	if math.Abs(net) < 1 {
		return ""
	}
	if net > 0 {
		return fmt.Sprintf(
			"%s of that was money you paid in, not gains.", plainDollars(net))
	}
	return fmt.Sprintf("You also took out %s along the way.", plainDollars(-net))
}

// fallClause is the part people skip and then live through: how deep the
// worst hole was, and how long the longest one lasted. They are different
// drawdowns as often as not, so they are stated as two facts, never as one.
func fallClause(r Result) string {
	if r.Metrics.MaxDrawdown <= 0 {
		return ""
	}
	s := fmt.Sprintf("The worst fall was %.0f%%", r.Metrics.MaxDrawdown)
	if d := r.Metrics.LongestUnderwaterDays; d > 0 {
		s += fmt.Sprintf(", and the longest stretch below a previous high was %s",
			humanSpan(d))
	}
	s += "."
	// Only the absence of recovery is assertable: Drawdowns carries the
	// deepest few, so a missing ongoing one would make "it recovered" a claim
	// this function cannot support.
	for _, d := range r.Drawdowns {
		if d.Ongoing {
			return s + " It had not made that back by the end."
		}
	}
	return s
}

// benchmarkClause answers the question the user actually had. Omitted
// entirely when no benchmark covered the window -- an unanswered comparison
// is better than one against nothing.
func benchmarkClause(r Result, days int) string {
	b := r.BenchmarkStats
	if b.Ticker == "" {
		return ""
	}
	// Under a year the benchmark is compared on its total too, from its own
	// rebased curve -- quoting an annualized benchmark figure beside a total
	// portfolio figure would be comparing two different things, and quoting
	// both annualized would be extrapolating twice instead of once.
	if days < 365 {
		total, ok := benchmarkTotal(r)
		if !ok {
			return ""
		}
		return fmt.Sprintf("Holding %s instead would have been %s over the same time.",
			b.Ticker, signedPct(total, "a gain of", "a loss of"))
	}
	diff := r.Metrics.AnnualReturn - b.AnnualReturn
	if math.Abs(diff) < 0.05 {
		return fmt.Sprintf("Holding %s instead would have %s — about the same.",
			b.Ticker, madeOrLost(b.AnnualReturn))
	}
	verb := "ahead"
	if diff < 0 {
		verb = "behind"
	}
	return fmt.Sprintf("Holding %s instead would have %s, so this was %.1f points %s.",
		b.Ticker, madeOrLost(b.AnnualReturn), math.Abs(diff), verb)
}

// ruinClause reports the run that did not survive its own schedule.
func ruinClause(r Result) string {
	n := r.Metrics.WithdrawalsMissed
	if n <= 0 {
		return ""
	}
	if n == 1 {
		return "The money ran out: one scheduled withdrawal could not be paid."
	}
	return fmt.Sprintf(
		"The money ran out: %d scheduled withdrawals could not be paid.", n)
}

// spanDays is the calendar length of the simulated window. Dates are the
// trading days, so the count of them is not the answer -- a year is 252 of
// them and 365 of these.
func spanDays(dates []string) int {
	const layout = "2006-01-02"
	first, err := time.Parse(layout, dates[0])
	if err != nil {
		return 0
	}
	last, err := time.Parse(layout, dates[len(dates)-1])
	if err != nil {
		return 0
	}
	d := int(last.Sub(first).Hours() / 24)
	if d < 0 {
		return 0
	}
	return d
}

// humanSpan says a duration the way a person would. Months up to two years,
// because "1.4 years" is a unit nobody thinks in; years after that, because
// "62 months" is worse.
func humanSpan(days int) string {
	switch {
	case days < 45:
		return countWithUnit(days, "day")
	case days < 730:
		return countWithUnit(int(math.Round(float64(days)/30.44)), "month")
	default:
		years := float64(days) / 365.25
		if math.Abs(years-math.Round(years)) < 0.08 {
			return countWithUnit(int(math.Round(years)), "year")
		}
		return fmt.Sprintf("%.1f years", years)
	}
}

func countWithUnit(n int, unit string) string {
	if n == 1 {
		return "1 " + unit
	}
	return fmt.Sprintf("%d %ss", n, unit)
}

// annualPhrase and madeOrLost both exist to keep a minus sign out of the
// prose. "-11.6% a year" hides the one character that reverses its meaning in
// the middle of a sentence; "a loss of 11.6% a year" cannot be skim-read
// wrong, which is the entire point of writing the result out in words.
func annualPhrase(pct float64) string {
	if pct < 0 {
		return fmt.Sprintf("a loss of %.1f%% a year", -pct)
	}
	return fmt.Sprintf("%.1f%% a year", pct)
}

func madeOrLost(pct float64) string {
	if pct < 0 {
		return fmt.Sprintf("lost %.1f%% a year", -pct)
	}
	return fmt.Sprintf("made %.1f%% a year", pct)
}

// benchmarkTotal is the benchmark's total return over the window, from the
// curve rebased to the portfolio's own starting capital. Measured from
// InitialCapital rather than from curve[0] for the same reason the portfolio
// is: curve[0] is already the first day's close, so dividing by it drops the
// first day's move.
func benchmarkTotal(r Result) (float64, bool) {
	if len(r.BenchmarkCurve) == 0 || r.InitialCapital <= 0 {
		return 0, false
	}
	last := r.BenchmarkCurve[len(r.BenchmarkCurve)-1]
	return (last/r.InitialCapital - 1) * 100, true
}

// signedPct spells out the direction, because "a loss of 12.0%" cannot be
// misread the way "-12.0%" can.
func signedPct(pct float64, up, down string) string {
	if pct < 0 {
		return fmt.Sprintf("%s %.1f%%", down, -pct)
	}
	return fmt.Sprintf("%s %.1f%%", up, pct)
}

// plainDollars formats dollars without cents: the sentence is about scale, and
// "$184,312.47" reads as precision the backtest does not have.
func plainDollars(v float64) string {
	neg := v < 0
	if neg {
		v = -v
	}
	s := fmt.Sprintf("%.0f", v)
	// Thousands separators, inserted from the right.
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "−$" + string(out)
	}
	return "$" + string(out)
}
