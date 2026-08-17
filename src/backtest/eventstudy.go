package backtest

// What happens after X?
//
// "Buy after a 10% drawdown", "buy the breakout", "fade a three-sigma day" —
// a large and natural class of strategy ideas that the app could not express or
// measure. An event study answers the question directly and cheaply: find every
// occurrence, look at what followed, and report the average with enough context
// to tell a real effect from twelve lucky coincidences.
//
// Three things make the answer honest, and they matter more than the average:
//
//   - **Abnormal, not raw.** The return after an event is measured net of what
//     the rest of the universe did over the same window. Without that, "buy
//     after a drawdown" scores brilliantly in any sample containing a market
//     recovery — the event would be taking credit for the market.
//   - **Non-overlapping occurrences.** A drawdown that persists fires on
//     twenty consecutive days; counting all twenty as independent evidence
//     inflates every statistic. One occurrence per ticker per horizon.
//   - **The occurrence count is reported first.** Twelve events is an anecdote,
//     and a confident mean over twelve is how a backtest lies.
//
// Events here are price-based, so a study costs no query beyond the history the
// runner already fetched. Earnings and macro-release events are the obvious next
// set and are deliberately not here: they need a database read inside the run
// path and, more importantly, the publication-lag rules from
// PointInTimeFundamentals — an earnings event dated at the period end rather
// than the report date would make every study of it look wonderful and mean
// nothing.

import (
	"fmt"
	"math"
	"my-backtester/src/data"
	"sort"
	"strings"
)

// MinOccurrences is the fewest events worth summarising. Below it the study
// reports nothing at all rather than a mean with a t-statistic attached, because
// the latter reads as evidence and the former reads as what it is.
const MinOccurrences = 10

// EventDef detects one kind of event on one ticker. Detect may read bars[0..i]
// and nothing past i — the same contract as a Signal, for the same reason.
type EventDef struct {
	Name        string
	Description string
	Detect      func(bars []data.AssetData, i int) bool
}

// EventStudy is what followed one kind of event, at one horizon.
type EventStudy struct {
	Event   string
	Horizon int
	// Occurrences is the number of NON-OVERLAPPING events found. Read this
	// first: everything below is meaningless when it is small.
	Occurrences int
	// MeanAbnormal and MedianAbnormal are the average and median return over
	// the horizon, net of what the rest of the universe did. The median matters
	// because one 400% recovery can carry a mean on its own.
	MeanAbnormal   float64
	MedianAbnormal float64
	// MeanRaw is the same window without the universe adjustment, kept so the
	// difference between the two is visible: when they are far apart, the event
	// is mostly the market.
	MeanRaw float64
	// TStat is MeanAbnormal over its standard error, and HitRate the share of
	// occurrences with a positive abnormal return. A good mean with a hit rate
	// near half is a few outliers, not an effect.
	TStat   float64
	HitRate float64
}

// String renders one row of the report.
func (e EventStudy) String() string {
	return fmt.Sprintf(
		"%-16s %4dd  n=%4d  abnormal %+7.2f%% (median %+6.2f%%)  "+
			"raw %+7.2f%%  t %+5.2f  hit %5.1f%%",
		e.Event, e.Horizon, e.Occurrences, e.MeanAbnormal*100,
		e.MedianAbnormal*100, e.MeanRaw*100, e.TStat, e.HitRate*100)
}

// trailingHigh is the highest close over the n days ending at i.
func trailingHigh(bars []data.AssetData, i, n int) (float64, bool) {
	if n < 1 || i-n+1 < 0 || i >= len(bars) {
		return 0, false
	}
	high := bars[i-n+1].Close
	for k := i - n + 2; k <= i; k++ {
		if bars[k].Close > high {
			high = bars[k].Close
		}
	}
	return high, true
}

// drawdownFrom is how far below its trailing n-day high the close sits, as a
// negative fraction.
func drawdownFrom(bars []data.AssetData, i, n int) (float64, bool) {
	high, ok := trailingHigh(bars, i, n)
	if !ok || high <= 0 {
		return 0, false
	}
	return bars[i].Close/high - 1, true
}

// crossedBelow reports whether a threshold was breached on this day and not the
// day before — the crossing, not the state. Without this a 10% drawdown that
// lasts a month would fire on every one of those days.
func crossedBelow(bars []data.AssetData, i, window int, threshold float64) bool {
	if i < 1 {
		return false
	}
	now, ok := drawdownFrom(bars, i, window)
	if !ok {
		return false
	}
	prev, ok := drawdownFrom(bars, i-1, window)
	if !ok {
		return false
	}
	return now <= threshold && prev > threshold
}

// BuiltinEvents is the starting set. All price-based, so a study needs no data
// beyond the bars the runner already has.
func BuiltinEvents() []EventDef {
	return []EventDef{
		{
			Name:        "drawdown_10",
			Description: "close crosses 10% below its 1-year high",
			Detect: func(b []data.AssetData, i int) bool {
				return crossedBelow(b, i, 252, -0.10)
			},
		},
		{
			Name:        "drawdown_20",
			Description: "close crosses 20% below its 1-year high",
			Detect: func(b []data.AssetData, i int) bool {
				return crossedBelow(b, i, 252, -0.20)
			},
		},
		{
			Name:        "high_52w",
			Description: "close makes a new 1-year high",
			Detect: func(b []data.AssetData, i int) bool {
				if i < 252 {
					return false
				}
				prior, ok := trailingHigh(b, i-1, 252)
				return ok && b[i].Close > prior
			},
		},
		{
			Name:        "drop_3sigma",
			Description: "one-day fall beyond three times its 1-month volatility",
			Detect: func(b []data.AssetData, i int) bool {
				return sigmaMove(b, i, -3)
			},
		},
		{
			Name:        "jump_3sigma",
			Description: "one-day rise beyond three times its 1-month volatility",
			Detect: func(b []data.AssetData, i int) bool {
				return sigmaMove(b, i, 3)
			},
		},
		{
			Name:        "volume_spike",
			Description: "volume at three times its 1-month average",
			Detect: func(b []data.AssetData, i int) bool {
				if i < 21 {
					return false
				}
				var sum float64
				for k := i - 21; k < i; k++ {
					sum += b[k].Volume
				}
				avg := sum / 21
				return avg > 0 && b[i].Volume > 3*avg
			},
		},
	}
}

// sigmaMove reports whether day i's return is beyond `sigmas` times the
// trailing 21-day volatility, in the sign of `sigmas`.
func sigmaMove(bars []data.AssetData, i int, sigmas float64) bool {
	if i < 22 || bars[i-1].Close <= 0 {
		return false
	}
	rets := make([]float64, 0, 21)
	for k := i - 21; k < i; k++ {
		if bars[k-1].Close <= 0 {
			return false
		}
		rets = append(rets, bars[k].Close/bars[k-1].Close-1)
	}
	sd, ok := sampleStdev(rets)
	if !ok || sd <= 0 {
		return false
	}
	move := bars[i].Close/bars[i-1].Close - 1
	if sigmas < 0 {
		return move < sigmas*sd
	}
	return move > sigmas*sd
}

// StudyEvents measures what followed each event at each horizon.
//
// hist and tickers are the runner's already-fetched history. The "market" the
// abnormal return is measured against is the equal-weight average return of the
// universe itself over the same window — no benchmark series required, and it
// asks the right question for a study run on one basket: did the event add
// anything beyond what holding everything would have given?
func StudyEvents(
	hist map[string][]data.AssetData,
	tickers []string,
	events []EventDef,
	horizons []int,
) []EventStudy {
	var out []EventStudy
	for _, ev := range events {
		for _, h := range horizons {
			if study, ok := studyOne(hist, tickers, ev, h); ok {
				out = append(out, study)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return math.Abs(out[i].TStat) > math.Abs(out[j].TStat)
	})
	return out
}

func studyOne(
	hist map[string][]data.AssetData,
	tickers []string,
	ev EventDef,
	h int,
) (EventStudy, bool) {
	study := EventStudy{Event: ev.Name, Horizon: h}
	if h <= 0 || len(tickers) == 0 {
		return study, false
	}

	var abnormal, raw []float64
	for _, ticker := range tickers {
		bars := hist[ticker]
		lastAccepted := -1
		for i := 0; i+h < len(bars); i++ {
			if !ev.Detect(bars, i) {
				continue
			}
			// One occurrence per horizon: a persistent condition fires on
			// consecutive days, and counting each as independent evidence
			// inflates every statistic below.
			if lastAccepted >= 0 && i-lastAccepted < h {
				continue
			}
			r, ok := forwardReturn(bars, i, h)
			if !ok {
				continue
			}
			market, ok := universeReturn(hist, tickers, i, h)
			if !ok {
				continue
			}
			lastAccepted = i
			raw = append(raw, r)
			abnormal = append(abnormal, r-market)
		}
	}

	if len(abnormal) < MinOccurrences {
		return study, false
	}
	study.Occurrences = len(abnormal)
	study.MeanAbnormal = mean(abnormal)
	study.MedianAbnormal = median(abnormal)
	study.MeanRaw = mean(raw)
	if se := stdErr(abnormal); se > 0 {
		study.TStat = study.MeanAbnormal / se
	}
	var positive int
	for _, a := range abnormal {
		if a > 0 {
			positive++
		}
	}
	study.HitRate = float64(positive) / float64(len(abnormal))
	return study, true
}

// universeReturn is the equal-weight average return of every ticker that has
// bars for the window — the "market" an abnormal return is measured against.
func universeReturn(
	hist map[string][]data.AssetData, tickers []string, i, h int,
) (float64, bool) {
	var sum float64
	var n int
	for _, ticker := range tickers {
		if r, ok := forwardReturn(hist[ticker], i, h); ok {
			sum += r
			n++
		}
	}
	if n == 0 {
		return 0, false
	}
	return sum / float64(n), true
}

// median sorts a copy, so the caller's slice keeps the order it was built in.
func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sorted := append([]float64(nil), xs...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

// EventReport renders a whole study run, with the caveats that decide how to
// read it.
func EventReport(studies []EventStudy, tickers []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Event study: %d tickers\n", len(tickers))
	b.WriteString(
		"'abnormal' is the return after the event NET of what the rest of the " +
			"universe did\nover the same window — without that, any event in a " +
			"rising market looks like\nan edge. Occurrences are " +
			"non-overlapping. Read n first: a confident mean over\na dozen " +
			"events is how a backtest lies.\n\n")

	if len(studies) == 0 {
		fmt.Fprintf(&b, "No event produced at least %d non-overlapping "+
			"occurrences over this universe and window.\n", MinOccurrences)
		return b.String()
	}
	for _, s := range studies {
		b.WriteString(s.String())
		b.WriteString("\n")
	}
	b.WriteString("\nA large gap between abnormal and raw means the event is " +
		"mostly the market.\nThis is a study, not a backtest: no costs, no " +
		"position sizing, and every\noccurrence weighted equally.\n")
	return b.String()
}

// StudyConfigPortfolios runs the built-in events over the union of the given
// portfolios' tickers, mirroring ScanConfigPortfolios.
func StudyConfigPortfolios(portfolios []*Portfolio, horizons []int) string {
	if len(portfolios) == 0 {
		return "No portfolios in the config, so there is no universe to study.\n"
	}
	if len(horizons) == 0 {
		horizons = DefaultHorizons
	}

	seen := map[string]bool{}
	var tickers []string
	for _, p := range portfolios {
		for _, t := range p.Tickers {
			if !seen[t] {
				seen[t] = true
				tickers = append(tickers, t)
			}
		}
	}
	sort.Strings(tickers)

	start, end := dateRange(portfolios)
	hist := data.QueryAssetsForTickers(tickers, start, end)
	return EventReport(StudyEvents(hist, tickers, BuiltinEvents(), horizons), tickers)
}
