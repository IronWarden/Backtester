package backtest

// One page per candidate: everything needed to decide whether a result is real,
// on a single self-contained HTML page that can be kept or sent to someone.
//
// Triage today means reading a table row, squinting at a chart, and re-running
// with different dates. The numbers that kill an idea — the baseline it has to
// beat, the p-value against random timing, the trial count it was selected from
// — are the easiest ones to skip, because they live furthest from the headline.
// So this page puts them AT the headline, in the same visual weight as the
// return. A candidate that looks good and was picked from 200 trials should not
// be able to hide the 200.
//
// Self-contained on purpose: inline CSS, inline SVG, no scripts and no external
// requests. The file works from a thumb drive in five years, which a page that
// pulls a chart library from a CDN does not.
//
// This is the export half of the task. The in-app view is separate work and
// shares nothing with this beyond the Result it reads.

import (
	"fmt"
	"html"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Chart geometry. Fixed rather than responsive: the page is a record of one
// run, and a stable size makes two candidates comparable side by side.
const (
	chartWidth   = 900
	chartHeight  = 260
	chartPadding = 44
	// chartPadLeft is wider than the others because the y labels live there,
	// and a money label like "$129.9k" is about 50px at this font size. At the
	// symmetric padding this started with, the leading "$" was clipped off the
	// left edge of the viewBox — invisible in a unit test, obvious on sight.
	chartPadLeft = 72
)

// WriteCandidatePages writes one page per result into dir, creating it if
// needed, and returns the paths written in the same order as results.
//
// Filenames come from the portfolio name, so a sweep that produced twenty
// trials produces twenty readable filenames rather than page1..page20.
func WriteCandidatePages(results []Result, dir string) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating %s: %w", dir, err)
	}
	paths := make([]string, 0, len(results))
	used := make(map[string]int, len(results))
	for _, r := range results {
		// Two portfolios can share a name; suffix rather than overwrite, since
		// silently losing a candidate's page is the one failure that would not
		// be noticed. The tally is keyed on the BASE name, never the suffixed
		// one, or the second duplicate would start its own count.
		base := safeFilename(r.PortfolioName)
		name := base
		if n := used[base]; n > 0 {
			name = fmt.Sprintf("%s-%d", base, n+1)
		}
		used[base]++

		path := filepath.Join(dir, name+".html")
		if err := os.WriteFile(path, []byte(CandidatePage(r)), 0o644); err != nil {
			return nil, fmt.Errorf("writing %s: %w", path, err)
		}
		paths = append(paths, path)
	}
	return paths, nil
}

// safeFilename reduces a portfolio name to something a filesystem accepts,
// never returning empty.
func safeFilename(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-.")
	if out == "" {
		return "candidate"
	}
	return out
}

// CandidatePage renders one result as a complete HTML document.
func CandidatePage(r Result) string {
	var b strings.Builder
	b.Grow(32 << 10)

	fmt.Fprintf(&b, `<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>%s — candidate</title>
<style>%s</style>
</head><body>
`, esc(r.PortfolioName), pageCSS)

	writeHeadline(&b, r)
	writeVerdictStrip(&b, r)
	writeEquityChart(&b, r)
	writeUnderwaterChart(&b, r)
	writeRollingSharpe(&b, r)
	writeSplits(&b, r)
	writeWalkForward(&b, r)
	writePeriodTables(&b, r)
	writeDrawdownTable(&b, r)
	writeTradeStats(&b, r)
	writeFullMetrics(&b, r)

	fmt.Fprintf(&b, `<footer>Generated %s by the backtester. Self-contained: no scripts, no external requests.</footer>
</body></html>
`, time.Now().Format("2006-01-02 15:04"))

	return b.String()
}

func writeHeadline(b *strings.Builder, r Result) {
	profit := r.FinalValue - r.InitialCapital
	pct := 0.0
	if r.InitialCapital != 0 {
		pct = profit / r.InitialCapital * 100
	}
	window := ""
	if len(r.Dates) > 0 {
		window = fmt.Sprintf("%s → %s · %d trading days",
			esc(r.Dates[0]), esc(r.Dates[len(r.Dates)-1]), len(r.Dates))
	}

	fmt.Fprintf(b, `<header>
<h1>%s</h1>
<p class="sub">%s</p>
<p class="window">%s</p>
<div class="headline">
  <div class="big"><span class="v">%s</span><span class="l">final value</span></div>
  <div class="big"><span class="v %s">%+.1f%%</span><span class="l">total return (%s)</span></div>
  <div class="big"><span class="v">%.2f</span><span class="l">Sharpe</span></div>
  <div class="big"><span class="v neg">%.1f%%</span><span class="l">max drawdown</span></div>
</div>
</header>
`,
		esc(r.PortfolioName), esc(r.Strategy), window,
		money(r.FinalValue), sign(pct), pct, money(profit),
		r.Metrics.SharpeRatio, r.Metrics.MaxDrawdown)
}

// writeVerdictStrip is the part that exists to be unavoidable. Three questions
// that kill an idea, in the same weight as the return above them: was it better
// than doing nothing, was it better than chance, and how many tries did it take
// to find.
func writeVerdictStrip(b *strings.Builder, r Result) {
	b.WriteString(`<section class="verdict"><h2>Does this survive the obvious objections?</h2><div class="cards">`)

	// Beat the baseline?
	if r.Baseline.Computed {
		cls, verdict := "bad", "NO"
		if r.Baseline.ExcessAnnualReturn > 0 {
			cls, verdict = "good", "YES"
		}
		fmt.Fprintf(b, `<div class="card %s"><span class="q">Beat %s?</span>
<span class="a">%s</span>
<span class="d">%+.2f%%/yr · %s in final value</span></div>`,
			cls, esc(r.Baseline.Name), verdict,
			r.Baseline.ExcessAnnualReturn, money(r.Baseline.ExcessFinalValue))
	} else {
		b.WriteString(`<div class="card unknown"><span class="q">Beat the baseline?</span>
<span class="a">—</span><span class="d">no baseline could be built</span></div>`)
	}

	// Better than chance?
	if r.Significance.Draws > 0 {
		cls, verdict := "bad", "NO"
		if r.Significance.PValue <= 0.05 {
			cls, verdict = "good", "YES"
		}
		fmt.Fprintf(b, `<div class="card %s"><span class="q">Better than random timing?</span>
<span class="a">%s</span>
<span class="d">p = %.3f · %.0fth percentile of %d draws</span></div>`,
			cls, verdict, r.Significance.PValue,
			r.Significance.Percentile, r.Significance.Draws)
	} else {
		b.WriteString(`<div class="card unknown"><span class="q">Better than random timing?</span>
<span class="a">—</span><span class="d">not computed</span></div>`)
	}

	// How many trials bought this result?
	trials := r.Trials
	if trials < 1 {
		trials = 1
	}
	cls := "good"
	if trials > 1 && r.DeflatedSharpe < 0.90 {
		cls = "bad"
	} else if trials > 1 {
		cls = "warn"
	}
	group := r.TrialGroup
	if group == "" {
		group = "this portfolio"
	}
	fmt.Fprintf(b, `<div class="card %s"><span class="q">How many trials was this picked from?</span>
<span class="a">%d</span>
<span class="d">deflated Sharpe %.3f · a no-edge best-of-%d would show %.2f · %s</span></div>`,
		cls, trials, r.DeflatedSharpe, trials, r.ExpectedMaxSharpe, esc(group))

	b.WriteString("</div></section>\n")
}

func writeEquityChart(b *strings.Builder, r Result) {
	if len(r.EquityCurve) < 2 {
		return
	}
	series := []namedSeries{{"Portfolio", r.EquityCurve, "#3987e5"}}
	if len(r.BenchmarkCurve) == len(r.EquityCurve) {
		label := r.BenchmarkStats.Ticker
		if label == "" {
			label = "Benchmark"
		}
		series = append(series, namedSeries{label, r.BenchmarkCurve, "#8a8a85"})
	}
	b.WriteString(`<section><h2>Equity curve</h2>`)
	writeLineChart(b, series, r.Dates, money)
	b.WriteString("</section>\n")
}

// writeUnderwaterChart plots depth below the running peak. Recomputed here from
// the equity curve rather than read from Drawdowns, which carries only the top
// N declines and so cannot draw a continuous series.
func writeUnderwaterChart(b *strings.Builder, r Result) {
	if len(r.EquityCurve) < 2 {
		return
	}
	under := make([]float64, len(r.EquityCurve))
	peak := r.EquityCurve[0]
	for i, v := range r.EquityCurve {
		if v > peak {
			peak = v
		}
		if peak > 0 {
			under[i] = (v - peak) / peak * 100
		}
	}
	b.WriteString(`<section><h2>Underwater — depth below the running peak</h2>`)
	writeLineChart(b, []namedSeries{{"Drawdown %", under, "#d95926"}}, r.Dates,
		func(v float64) string { return fmt.Sprintf("%.1f%%", v) })
	b.WriteString("</section>\n")
}

func writeRollingSharpe(b *strings.Builder, r Result) {
	if len(r.RollingSharpe) < 2 {
		return
	}
	vals := make([]float64, len(r.RollingSharpe))
	dates := make([]string, len(r.RollingSharpe))
	for i, p := range r.RollingSharpe {
		vals[i] = p.Value
		dates[i] = p.Date.Format("2006-01-02")
	}
	b.WriteString(`<section><h2>Rolling 12-month Sharpe</h2>
<p class="note">A single Sharpe over the whole run hides whether the edge was steady or came from one stretch.</p>`)
	writeLineChart(b, []namedSeries{{"Rolling Sharpe", vals, "#199e70"}}, dates,
		func(v float64) string { return fmt.Sprintf("%.2f", v) })
	b.WriteString("</section>\n")
}

func writeSplits(b *strings.Builder, r Result) {
	if len(r.Splits) == 0 {
		return
	}
	b.WriteString(`<section><h2>In-sample vs out-of-sample</h2>
<p class="note">The out-of-sample figure is the one that carries information about a swept or hand-tuned parameter set.</p>
<table><thead><tr><th>Segment</th><th>Window</th><th class="n">Days</th><th class="n">Total return %</th><th class="n">Sharpe</th><th class="n">Max DD %</th></tr></thead><tbody>`)
	for _, s := range r.Splits {
		fmt.Fprintf(b, `<tr><td>%s</td><td>%s → %s</td><td class="n">%d</td><td class="n %s">%+.2f</td><td class="n">%.2f</td><td class="n">%.2f</td></tr>`,
			esc(s.Label), esc(s.Start), esc(s.End), s.Days,
			sign(s.TotalReturn), s.TotalReturn, s.SharpeRatio, s.MaxDrawdown)
	}
	b.WriteString("</tbody></table></section>\n")
}

func writeWalkForward(b *strings.Builder, r Result) {
	if r.WalkForward == nil || len(r.WalkForward.Windows) == 0 {
		return
	}
	fmt.Fprintf(b, `<section><h2>Walk-forward</h2>
<p class="note">Re-selected on each training window and scored on the next, maximizing %s. Every figure below is out-of-sample.</p>
<table><thead><tr><th class="n">#</th><th>Test window</th><th>Selected</th><th class="n">Candidates</th><th class="n">Train score</th><th class="n">Test return %%</th></tr></thead><tbody>`,
		esc(r.WalkForward.Objective))
	for i, w := range r.WalkForward.Windows {
		fmt.Fprintf(b, `<tr><td class="n">%d</td><td>%s → %s</td><td class="mono">%s</td><td class="n">%d</td><td class="n">%.3f</td><td class="n %s">%+.2f</td></tr>`,
			i+1, esc(w.TestStart), esc(w.TestEnd), esc(w.Selected),
			w.Candidates, w.TrainScore, sign(w.TestReturn), w.TestReturn)
	}
	b.WriteString("</tbody></table></section>\n")
}

func writePeriodTables(b *strings.Builder, r Result) {
	if len(r.YearlyReturns) > 0 {
		b.WriteString(`<section><h2>Calendar-year returns</h2><table><thead><tr><th>Year</th><th class="n">Return %</th><th>Window</th></tr></thead><tbody>`)
		for _, p := range r.YearlyReturns {
			partial := ""
			if p.Partial {
				partial = ` <span class="tag">partial</span>`
			}
			fmt.Fprintf(b, `<tr><td>%s%s</td><td class="n %s">%+.2f</td><td class="dim">%s → %s</td></tr>`,
				esc(p.Period), partial, sign(p.ReturnPct), p.ReturnPct,
				p.Start.Format("2006-01-02"), p.End.Format("2006-01-02"))
		}
		b.WriteString("</tbody></table></section>\n")
	}
	if len(r.MonthlyReturns) > 0 {
		b.WriteString(`<section><h2>Monthly returns</h2><div class="months">`)
		for _, p := range r.MonthlyReturns {
			fmt.Fprintf(b, `<span class="m %s" title="%s">%s<br><b>%+.1f%%</b></span>`,
				sign(p.ReturnPct), esc(p.Period), esc(p.Period), p.ReturnPct)
		}
		b.WriteString("</div></section>\n")
	}
}

func writeDrawdownTable(b *strings.Builder, r Result) {
	if len(r.Drawdowns) == 0 {
		return
	}
	b.WriteString(`<section><h2>Deepest drawdowns</h2><table><thead><tr><th class="n">Depth %</th><th>Peak</th><th>Trough</th><th>Recovered</th><th class="n">Days down</th><th class="n">Days to recover</th></tr></thead><tbody>`)
	for _, d := range r.Drawdowns {
		recovered := d.Recovered.Format("2006-01-02")
		recovery := fmt.Sprintf("%d", d.RecoveryDays)
		if d.Ongoing {
			recovered, recovery = `<span class="tag">still open</span>`, "—"
		}
		fmt.Fprintf(b, `<tr><td class="n neg">-%.2f</td><td>%s</td><td>%s</td><td>%s</td><td class="n">%d</td><td class="n">%s</td></tr>`,
			d.DepthPct, d.Start.Format("2006-01-02"), d.Trough.Format("2006-01-02"),
			recovered, d.DurationDays, recovery)
	}
	b.WriteString("</tbody></table></section>\n")
}

func writeTradeStats(b *strings.Builder, r Result) {
	t := r.TradeStats
	if t.Trades == 0 {
		b.WriteString(`<section><h2>Trading</h2><p class="note">This portfolio never traded.</p></section>` + "\n")
		return
	}
	fmt.Fprintf(b, `<section><h2>Trading</h2><div class="grid">
%s%s%s%s%s%s%s%s
</div></section>
`,
		statCard("Trades", fmt.Sprintf("%d", t.Trades), fmt.Sprintf("%d buys / %d sells", t.Buys, t.Sells)),
		statCard("Round trips", fmt.Sprintf("%d", t.RoundTrips), fmt.Sprintf("%d won / %d lost", t.Wins, t.Losses)),
		statCard("Win rate", fmt.Sprintf("%.1f%%", t.WinRate), ""),
		statCard("Profit factor", fmt.Sprintf("%.2f", t.ProfitFactor), "gross win / gross loss"),
		statCard("Avg holding", fmt.Sprintf("%.0f days", t.AvgHoldingDays), ""),
		statCard("Turnover", fmt.Sprintf("%.2f×", r.Metrics.Turnover), "annualized, of portfolio value"),
		statCard("Realized P&L", money(t.TotalRealized), ""),
		statCard("Fees paid", money(t.TotalFees), "costs charged against turnover"),
	)
}

func writeFullMetrics(b *strings.Builder, r Result) {
	m := r.Metrics
	b.WriteString(`<section><h2>All metrics</h2><div class="grid">`)
	rows := []struct{ k, v, d string }{
		{"Annual return", fmt.Sprintf("%+.2f%%", m.AnnualReturn), "CAGR, 252-day year"},
		{"Sharpe", fmt.Sprintf("%.2f", m.SharpeRatio), ""},
		{"Sortino", fmt.Sprintf("%.2f", m.SortinoRatio), "downside deviation only"},
		{"Calmar", fmt.Sprintf("%.2f", m.CalmarRatio), "annual return / max drawdown"},
		{"Max drawdown", fmt.Sprintf("%.2f%%", m.MaxDrawdown), ""},
		{"Longest underwater", fmt.Sprintf("%d days", m.LongestUnderwaterDays), "peak to recovery"},
		{"Std dev", fmt.Sprintf("%.4f", m.StandardDev), "annualized"},
		{"Avg exposure", fmt.Sprintf("%.1f%%", m.AvgExposure), "mean invested fraction"},
		{"Time in market", fmt.Sprintf("%.1f%%", m.TimeInMarket), "days holding anything"},
		{"Avg correlation", fmt.Sprintf("%.2f", m.AvgCorrelation), "across held assets"},
		{"Cointegrated pairs", fmt.Sprintf("%d", m.CointegratedPairs), ""},
	}
	if r.BenchmarkStats.Ticker != "" {
		rows = append(rows,
			struct{ k, v, d string }{"Alpha", fmt.Sprintf("%+.2f%%", m.Alpha), "annualized, excess of beta"},
			struct{ k, v, d string }{"Beta", fmt.Sprintf("%.2f", m.Beta), "vs " + r.BenchmarkStats.Ticker},
			struct{ k, v, d string }{"Tracking error", fmt.Sprintf("%.4f", m.TrackingError), ""},
			struct{ k, v, d string }{"Information ratio", fmt.Sprintf("%.2f", m.InformationRatio), ""},
			struct{ k, v, d string }{"Up capture", fmt.Sprintf("%.1f%%", m.UpCapture), ""},
			struct{ k, v, d string }{"Down capture", fmt.Sprintf("%.1f%%", m.DownCapture), ""},
		)
	}
	for _, row := range rows {
		b.WriteString(statCard(row.k, row.v, row.d))
	}
	b.WriteString("</div></section>\n")
}

func statCard(label, value, detail string) string {
	d := ""
	if detail != "" {
		d = fmt.Sprintf(`<span class="d">%s</span>`, esc(detail))
	}
	return fmt.Sprintf(`<div class="s"><span class="l">%s</span><span class="v">%s</span>%s</div>`,
		esc(label), esc(value), d)
}

type namedSeries struct {
	name   string
	values []float64
	color  string
}

// writeLineChart renders one or more equal-length series as inline SVG, sharing
// one y-axis. Nothing here depends on a script running: the path is computed in
// Go and the axis labels are literal text.
func writeLineChart(b *strings.Builder, series []namedSeries, dates []string, fmtVal func(float64) string) {
	lo, hi := math.Inf(1), math.Inf(-1)
	n := 0
	for _, s := range series {
		if len(s.values) > n {
			n = len(s.values)
		}
		for _, v := range s.values {
			lo = math.Min(lo, v)
			hi = math.Max(hi, v)
		}
	}
	if n < 2 || math.IsInf(lo, 1) {
		return
	}
	if hi == lo {
		// A flat series still deserves a line rather than a divide-by-zero.
		hi, lo = hi+1, lo-1
	}

	plotW := float64(chartWidth - chartPadLeft - chartPadding)
	plotH := float64(chartHeight - 2*chartPadding)
	x := func(i int) float64 { return float64(chartPadLeft) + plotW*float64(i)/float64(n-1) }
	y := func(v float64) float64 {
		return float64(chartPadding) + plotH*(1-(v-lo)/(hi-lo))
	}

	fmt.Fprintf(b, `<svg class="chart" viewBox="0 0 %d %d" role="img">`, chartWidth, chartHeight)

	// Gridlines and y labels at five steps.
	for i := 0; i <= 4; i++ {
		v := lo + (hi-lo)*float64(i)/4
		yy := y(v)
		fmt.Fprintf(b, `<line class="grid" x1="%d" y1="%.1f" x2="%d" y2="%.1f"/>`,
			chartPadLeft, yy, chartWidth-chartPadding, yy)
		fmt.Fprintf(b, `<text class="ax" x="%d" y="%.1f" text-anchor="end">%s</text>`,
			chartPadLeft-8, yy+3, esc(fmtVal(v)))
	}
	// A zero line, when zero is inside the range — the reference that matters
	// on an underwater or Sharpe plot.
	if lo < 0 && hi > 0 {
		fmt.Fprintf(b, `<line class="zero" x1="%d" y1="%.1f" x2="%d" y2="%.1f"/>`,
			chartPadLeft, y(0), chartWidth-chartPadding, y(0))
	}

	for _, s := range series {
		if len(s.values) < 2 {
			continue
		}
		var p strings.Builder
		for i, v := range s.values {
			if i == 0 {
				fmt.Fprintf(&p, "M%.1f %.1f", x(i), y(v))
			} else {
				fmt.Fprintf(&p, " L%.1f %.1f", x(i), y(v))
			}
		}
		fmt.Fprintf(b, `<path d="%s" fill="none" stroke="%s" stroke-width="1.6"/>`,
			p.String(), s.color)
	}

	// x labels: first and last date only. More would collide at this width and
	// the chart is a shape, not a lookup table.
	if len(dates) >= 2 {
		fmt.Fprintf(b, `<text class="ax" x="%d" y="%d">%s</text>`,
			chartPadLeft, chartHeight-chartPadding+18, esc(dates[0]))
		fmt.Fprintf(b, `<text class="ax" x="%d" y="%d" text-anchor="end">%s</text>`,
			chartWidth-chartPadding, chartHeight-chartPadding+18, esc(dates[len(dates)-1]))
	}
	b.WriteString("</svg>")

	if len(series) > 1 {
		b.WriteString(`<div class="legend">`)
		for _, s := range series {
			fmt.Fprintf(b, `<span><i style="background:%s"></i>%s</span>`, s.color, esc(s.name))
		}
		b.WriteString("</div>")
	}
}

// money formats a signed dollar figure compactly, matching the UI's convention.
func money(v float64) string {
	sign := ""
	if v < 0 {
		sign, v = "-", -v
	}
	switch {
	case v >= 1e9:
		return fmt.Sprintf("%s$%.1fB", sign, v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%s$%.1fM", sign, v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%s$%.1fk", sign, v/1e3)
	default:
		return fmt.Sprintf("%s$%.0f", sign, v)
	}
}

// sign is the CSS class for a signed number, so red and green are decided in
// one place rather than at every call site.
func sign(v float64) string {
	if v < 0 {
		return "neg"
	}
	return "pos"
}

func esc(s string) string { return html.EscapeString(s) }

// pageCSS is deliberately small and literal. It renders light or dark from the
// reader's own preference and prints without a black background.
const pageCSS = `
:root{--bg:#fbfbfa;--fg:#1a1a19;--dim:#6b6b66;--line:#e2e2dd;--card:#fff;--pos:#0a7d4f;--neg:#c0392b}
@media(prefers-color-scheme:dark){:root{--bg:#131312;--fg:#e8e8e3;--dim:#8a8a85;--line:#2a2a28;--card:#1a1a19}}
*{box-sizing:border-box}
body{margin:0;padding:28px;background:var(--bg);color:var(--fg);
 font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif;max-width:980px}
h1{font-size:26px;margin:0}
h2{font-size:13px;text-transform:uppercase;letter-spacing:.08em;color:var(--dim);
 margin:32px 0 10px;font-weight:600}
header .sub{color:var(--dim);margin:4px 0 0;font-family:ui-monospace,monospace}
header .window{color:var(--dim);margin:2px 0 0;font-size:12px}
.headline{display:flex;flex-wrap:wrap;gap:28px;margin-top:18px;padding-bottom:18px;
 border-bottom:1px solid var(--line)}
.big{display:flex;flex-direction:column}
.big .v{font-size:28px;font-weight:600;line-height:1.1}
.big .l{font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:.05em}
.pos{color:var(--pos)}.neg{color:var(--neg)}
.cards{display:flex;flex-wrap:wrap;gap:12px}
.card{flex:1 1 240px;padding:12px 14px;border-radius:8px;background:var(--card);
 border:1px solid var(--line);border-left-width:4px}
.card.good{border-left-color:var(--pos)}
.card.bad{border-left-color:var(--neg)}
.card.warn{border-left-color:#c98500}
.card.unknown{border-left-color:var(--dim)}
.card .q{display:block;font-size:12px;color:var(--dim)}
.card .a{display:block;font-size:22px;font-weight:600;margin:2px 0}
.card .d{display:block;font-size:11px;color:var(--dim);font-family:ui-monospace,monospace}
.chart{width:100%;height:auto;display:block}
.grid{stroke:var(--line);stroke-width:1}
.zero{stroke:var(--dim);stroke-width:1;stroke-dasharray:3 3}
.ax{fill:var(--dim);font-size:10px;font-family:ui-monospace,monospace}
.legend{display:flex;gap:16px;font-size:12px;color:var(--dim);margin-top:4px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{padding:5px 10px;border-bottom:1px solid var(--line);text-align:left}
th{color:var(--dim);font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.04em}
td.n,th.n{text-align:right;font-family:ui-monospace,monospace}
td.dim{color:var(--dim);font-size:12px}
.tag{font-size:10px;padding:1px 5px;border-radius:3px;background:var(--line);color:var(--dim)}
div.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:10px;stroke:none}
.s{padding:9px 11px;background:var(--card);border:1px solid var(--line);border-radius:6px}
.s .l{display:block;font-size:11px;color:var(--dim)}
.s .v{display:block;font-size:17px;font-weight:600;font-family:ui-monospace,monospace}
.s .d{display:block;font-size:10px;color:var(--dim)}
.months{display:grid;grid-template-columns:repeat(auto-fill,minmax(86px,1fr));gap:6px}
.m{padding:6px;text-align:center;font-size:10px;color:var(--dim);background:var(--card);
 border:1px solid var(--line);border-radius:5px}
.m b{font-size:13px;font-family:ui-monospace,monospace}
.m.pos b{color:var(--pos)}.m.neg b{color:var(--neg)}
.note{color:var(--dim);font-size:12px;margin:-4px 0 10px}
footer{margin-top:40px;padding-top:12px;border-top:1px solid var(--line);
 color:var(--dim);font-size:11px}
`
