import { useEffect, useMemo, useRef, useState } from "react";
import { main } from "../wailsjs/go/models";

// Categorical palette (dark-surface steps), assigned by portfolio index in
// fixed slot order. Validated against the #1a1a19 card surface for CVD
// separation, normal-vision separation, and 3:1 contrast.
const COLORS = [
  "#3987e5",
  "#d95926",
  "#199e70",
  "#c98500",
  "#d55181",
  "#008300",
  "#9085e9",
  "#e66767",
];

// The benchmark is a reference line, not a competing series, so it is drawn
// in a muted neutral outside the categorical palette — it must never be
// mistaken for one of the portfolios.
const BENCHMARK_COLOR = "#8a8a85";

type PlotSeries = {
  name: string;
  pts: { t: number; v: number }[];
  benchmark?: boolean;
};

// Portfolios keep their palette slot by index; benchmarks are appended after
// every portfolio, so portfolio colours are unaffected by their presence.
function seriesColor(s: PlotSeries, i: number): string {
  return s.benchmark ? BENCHMARK_COLOR : COLORS[i % COLORS.length];
}

// Compact money label, e.g. 1234567 -> "$1.2M", 12345 -> "$12.3k".
function fmtMoney(v: number): string {
  const abs = Math.abs(v);
  if (abs >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `$${(v / 1e3).toFixed(1)}k`;
  return `$${v.toFixed(0)}`;
}

function fmtPct(v: number): string {
  return `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`;
}

// Exact whole-dollar amount, e.g. 184320.7 -> "$184,321". Used for the headline
// figures, where the rounded "$184.3k" of fmtMoney would hide real money.
function fmtDollars(v: number): string {
  const sign = v < 0 ? "-" : "";
  return `${sign}$${Math.round(Math.abs(v)).toLocaleString("en-US")}`;
}

// Profit in dollars and as a % of starting capital, measured against the cash
// the portfolio began with. Null when the run reported no starting capital.
// Go's time.Time arrives as an RFC3339 string, typed `any` by the wails
// binding generator. An ongoing drawdown has no recovery date and is sent
// as the zero time, which we render as a dash rather than as year 1.
function fmtDate(v: any): string {
  if (!v) return "—";
  const d = new Date(v as string);
  if (isNaN(d.getTime()) || d.getUTCFullYear() < 1900) return "—";
  return d.toISOString().slice(0, 10);
}

// The benchmark columns are only meaningful when a benchmark was set, and
// a wall of zeros is worse than no column at all.
function hasBenchmark(results: main.RunResult[]): boolean {
  return results.some(
    (r) => r.beta !== 0 || r.alpha !== 0 || r.trackingError !== 0,
  );
}

function profit(r: main.RunResult): { abs: number; pct: number } | null {
  if (!r.initialCapital) return null;
  const abs = r.finalValue - r.initialCapital;
  return { abs, pct: (abs / r.initialCapital) * 100 };
}

// Round tick values: pick a 1/2/2.5/5×10^n step and emit the multiples that
// fall inside [min, max], so axis labels land on clean dollar amounts.
function niceTicks(min: number, max: number, count: number): number[] {
  const span = max - min || 1;
  const step0 = span / count;
  const mag = Math.pow(10, Math.floor(Math.log10(step0)));
  const norm = step0 / mag;
  const step = (norm >= 5 ? 5 : norm >= 2.5 ? 2.5 : norm >= 2 ? 2 : 1) * mag;
  const out: number[] = [];
  for (let v = Math.ceil(min / step) * step; v <= max + step * 1e-6; v += step)
    out.push(v);
  return out;
}

const DAY = 86400000;
const MONTHS = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(" ");

// Calendar-aligned x ticks: year starts on multi-year spans, month starts on
// shorter ones, plain evenly-spaced days inside a couple of months.
function timeTicks(tMin: number, tMax: number): { t: number; label: string }[] {
  const days = (tMax - tMin) / DAY;
  const out: { t: number; label: string }[] = [];
  if (days > 900) {
    const y0 = new Date(tMin).getUTCFullYear() + 1;
    const y1 = new Date(tMax).getUTCFullYear();
    const step = Math.max(1, Math.ceil((y1 - y0 + 1) / 7));
    for (let yy = y0; yy <= y1; yy += step)
      out.push({ t: Date.UTC(yy, 0, 1), label: String(yy) });
  } else if (days > 50) {
    const stepM = days > 540 ? 3 : days > 270 ? 2 : 1;
    const d = new Date(tMin);
    let yy = d.getUTCFullYear();
    let mm = d.getUTCMonth() + 1; // first month start at/after tMin
    for (;;) {
      if (mm > 11) {
        mm -= 12;
        yy += 1;
      }
      const t = Date.UTC(yy, mm, 1);
      if (t > tMax) break;
      out.push({ t, label: `${MONTHS[mm]} ${String(yy).slice(2)}` });
      mm += stepM;
    }
  } else {
    for (let i = 0; i < 5; i++) {
      const t = tMin + ((tMax - tMin) * i) / 4;
      const d = new Date(t);
      out.push({ t, label: `${MONTHS[d.getUTCMonth()]} ${d.getUTCDate()}` });
    }
  }
  return out;
}

type Pt = { t: number; v: number };

// Index of the point with time closest to t (pts are date-ordered).
function nearestIdx(pts: Pt[], t: number): number {
  let lo = 0;
  let hi = pts.length - 1;
  if (hi < 0) return -1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (pts[mid].t < t) lo = mid + 1;
    else hi = mid;
  }
  if (lo > 0 && Math.abs(pts[lo - 1].t - t) <= Math.abs(pts[lo].t - t)) lo--;
  return lo;
}

type Hover = {
  px: number; // crosshair x in svg pixels
  date: string;
  rows: { name: string; color: string; v: number; x: number; y: number }[];
};

type Props = { results: main.RunResult[]; fontSize: number };

export default function ResultsView({ results, fontSize }: Props) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(800);
  const [hover, setHover] = useState<Hover | null>(null);

  // Only portfolios with a real curve can be plotted.
  const plottable = useMemo(
    () => results.filter((r) => r.equityCurve && r.equityCurve.length > 1),
    [results],
  );

  // Track the container width so the SVG is drawn in real pixels (crisp text,
  // no aspect-ratio distortion) and reflows when the panel is resized. Re-runs
  // when the chart appears/disappears because the observed div only exists
  // while there is something to plot.
  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    // Measure synchronously so the first paint is full-width; the observer's
    // initial delivery is async and would leave an 800px chart on screen.
    if (el.clientWidth) setWidth(el.clientWidth);
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width;
      if (w) setWidth(w);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [plottable.length > 0]);

  const chart = useMemo(() => {
    if (plottable.length === 0) return null;

    const series: PlotSeries[] = plottable.map((r) => ({
      name: r.portfolioName,
      pts: r.equityCurve
        .map((v, i) => ({ t: Date.parse(r.dates[i] ?? ""), v }))
        .filter((p) => !Number.isNaN(p.t)),
    }));

    // One line per distinct benchmark, appended after every portfolio so the
    // palette indices above stay put. Portfolios sharing a benchmark would
    // otherwise stack identical lines on top of each other.
    const seenBenchmarks = new Set<string>();
    for (const r of plottable) {
      const ticker = r.benchmarkStats?.ticker;
      if (!ticker || !r.benchmarkCurve || r.benchmarkCurve.length < 2) continue;
      if (seenBenchmarks.has(ticker)) continue;
      seenBenchmarks.add(ticker);
      series.push({
        name: ticker,
        benchmark: true,
        pts: r.benchmarkCurve
          .map((v, i) => ({ t: Date.parse(r.dates[i] ?? ""), v }))
          .filter((p) => !Number.isNaN(p.t)),
      });
    }

    let tMin = Infinity,
      tMax = -Infinity,
      yMin = Infinity,
      yMax = -Infinity;
    for (const s of series)
      for (const p of s.pts) {
        if (p.t < tMin) tMin = p.t;
        if (p.t > tMax) tMax = p.t;
        if (p.v < yMin) yMin = p.v;
        if (p.v > yMax) yMax = p.v;
      }
    if (yMin === yMax) {
      yMin -= 1;
      yMax += 1;
    }
    // Breathing room so lines don't touch the panel edges.
    const yPad = (yMax - yMin) * 0.05;
    yMin -= yPad;
    yMax += yPad;
    const tSpan = tMax - tMin || 1;
    const ySpan = yMax - yMin || 1;
    return { series, tMin, tMax, tSpan, yMin, yMax, ySpan };
  }, [plottable]);

  if (results.length === 0) {
    return (
      <div className="results-empty">
        Run a backtest to see equity curves and metrics here.
      </div>
    );
  }

  // The chart is laid out in pixels, so its height, paddings, label size and
  // stroke are all multiplied by the zoom factor to grow in step with the rest
  // of the (em-sized) UI. Width still comes from the container's real width.
  const scale = fontSize / 16;
  // Direct end-of-line labels for 2–4 series (beyond that the legend carries
  // identity alone); they need reserved room on the right.
  const endLabeled = plottable.length >= 2 && plottable.length <= 4;
  const H = Math.round(250 * scale);
  const pad = {
    top: 14 * scale,
    right: (endLabeled ? 100 : 18) * scale,
    bottom: 26 * scale,
    left: 62 * scale,
  };
  const labelSize = 10 * scale;
  const innerW = Math.max(10, width - pad.left - pad.right);
  const innerH = H - pad.top - pad.bottom;

  const x = (t: number) =>
    chart ? pad.left + ((t - chart.tMin) / chart.tSpan) * innerW : 0;
  const y = (v: number) =>
    chart ? pad.top + (1 - (v - chart.yMin) / chart.ySpan) * innerH : 0;

  const yTicks = chart ? niceTicks(chart.yMin, chart.yMax, 4) : [];
  const xTicks = chart ? timeTicks(chart.tMin, chart.tMax) : [];

  // End labels, nudged apart vertically so close curves don't collide.
  let endLabels: { name: string; color: string; y: number }[] = [];
  if (chart && endLabeled) {
    endLabels = chart.series
      .map((s, i) => {
        const last = s.pts[s.pts.length - 1];
        const name =
          s.name.length > 13 ? `${s.name.slice(0, 12)}…` : s.name;
        return { name, color: seriesColor(s, i), y: y(last.v) };
      })
      .sort((a, b) => a.y - b.y);
    const minGap = 13 * scale;
    for (let i = 1; i < endLabels.length; i++)
      if (endLabels[i].y - endLabels[i - 1].y < minGap)
        endLabels[i].y = endLabels[i - 1].y + minGap;
    const over = endLabels[endLabels.length - 1].y - (H - pad.bottom);
    if (over > 0) for (const l of endLabels) l.y -= over;
  }

  function onChartMove(e: React.MouseEvent<SVGSVGElement>) {
    if (!chart) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const px = e.clientX - rect.left;
    if (px < pad.left || px > width - pad.right) {
      setHover(null);
      return;
    }
    const t = chart.tMin + ((px - pad.left) / innerW) * chart.tSpan;
    let dateT = NaN;
    const rows: Hover["rows"] = [];
    chart.series.forEach((s, i) => {
      const idx = nearestIdx(s.pts, t);
      if (idx < 0) return;
      const p = s.pts[idx];
      if (Number.isNaN(dateT) || Math.abs(p.t - t) < Math.abs(dateT - t))
        dateT = p.t;
      rows.push({
        name: s.name,
        color: seriesColor(s, i),
        v: p.v,
        x: x(p.t),
        y: y(p.v),
      });
    });
    if (rows.length === 0 || Number.isNaN(dateT)) {
      setHover(null);
      return;
    }
    rows.sort((a, b) => b.v - a.v);
    const d = new Date(dateT);
    setHover({
      px: x(dateT),
      date: `${MONTHS[d.getUTCMonth()]} ${d.getUTCDate()}, ${d.getUTCFullYear()}`,
      rows,
    });
  }

  const tipOnLeft = hover !== null && hover.px > width * 0.62;

  const benchmarked = hasBenchmark(results);

  // The benchmark's own figures, deduped by ticker exactly as the chart does,
  // so N portfolios sharing $SP500 contribute one row rather than N identical
  // ones. Rebased to the portfolio's starting capital, so its "final value"
  // answers "what would that capital have become in the index".
  const benchmarkRows = useMemo(() => {
    const rows: {
      ticker: string;
      finalValue: number;
      profit: number;
      stats: main.RunResult["benchmarkStats"];
    }[] = [];
    const seen = new Set<string>();
    for (const r of results) {
      const ticker = r.benchmarkStats?.ticker;
      const curve = r.benchmarkCurve;
      if (!ticker || !curve || curve.length === 0 || seen.has(ticker)) continue;
      seen.add(ticker);
      const finalValue = curve[curve.length - 1];
      rows.push({
        ticker,
        finalValue,
        profit: finalValue - r.initialCapital,
        stats: r.benchmarkStats,
      });
    }
    return rows;
  }, [results]);

  return (
    <div className="results-view">
      <div className="stat-grid">
        {results.map((r, i) => {
          const p = profit(r);
          return (
            <div className="stat-card" key={i}>
              <div className="stat-name" title={r.portfolioName}>
                <span
                  className="swatch"
                  style={{ background: COLORS[i % COLORS.length] }}
                />
                {r.portfolioName}
              </div>
              <div className="stat-value">{fmtDollars(r.finalValue)}</div>
              {p && (
                <div className={`stat-gain ${p.abs < 0 ? "neg" : "pos"}`}>
                  {p.abs >= 0 ? "+" : "−"}
                  {fmtDollars(Math.abs(p.abs))} ({fmtPct(p.pct)})
                  <span className="stat-from">
                    from {fmtDollars(r.initialCapital)}
                  </span>
                </div>
              )}
              <div className="stat-sub">
                <span>
                  <span className="sub-k">Annual</span>
                  <span className="sub-v">{fmtPct(r.annualReturn)}</span>
                </span>
                <span>
                  <span className="sub-k">Max DD</span>
                  <span className="sub-v">{r.maxDrawdown.toFixed(1)}%</span>
                </span>
                <span>
                  <span className="sub-k">Sharpe</span>
                  <span className="sub-v">{r.sharpeRatio.toFixed(2)}</span>
                </span>
              </div>
            </div>
          );
        })}
      </div>

      {chart && (
        <div className="panel">
          <div className="panel-head">
            <span className="panel-title">Equity curve</span>
            {plottable.length >= 2 && (
              <div className="legend">
                {plottable.map((r, i) => (
                  <span
                    className="legend-item"
                    key={i}
                    title={r.portfolioName}
                  >
                    <span
                      className="swatch"
                      style={{ background: COLORS[i % COLORS.length] }}
                    />
                    {r.portfolioName}
                  </span>
                ))}
              </div>
            )}
          </div>
          <div className="chart-wrap" ref={wrapRef}>
            <svg
              className="equity-chart"
              width={width}
              height={H}
              style={{ fontSize: labelSize }}
              onMouseMove={onChartMove}
              onMouseLeave={() => setHover(null)}
            >
              {yTicks.map((v, i) => (
                <g key={i}>
                  <line
                    className="gridline"
                    x1={pad.left}
                    x2={width - pad.right}
                    y1={y(v)}
                    y2={y(v)}
                  />
                  <text
                    x={pad.left - 8 * scale}
                    y={y(v) + 3 * scale}
                    className="axis-label end"
                  >
                    {fmtMoney(v)}
                  </text>
                </g>
              ))}
              {xTicks.map((tick, i) => (
                <text
                  key={i}
                  x={x(tick.t)}
                  y={H - 8 * scale}
                  className="axis-label mid"
                >
                  {tick.label}
                </text>
              ))}
              {hover && (
                <line
                  className="crosshair"
                  x1={hover.px}
                  x2={hover.px}
                  y1={pad.top}
                  y2={H - pad.bottom}
                />
              )}
              {[...chart.series.entries()]
                // Benchmarks first so the portfolio lines paint over them.
                .sort((a, b) => Number(!!b[1].benchmark) - Number(!!a[1].benchmark))
                .map(([i, s]) => {
                  const d = s.pts
                    .map((p, j) => `${j === 0 ? "M" : "L"}${x(p.t)},${y(p.v)}`)
                    .join(" ");
                  return (
                    <path
                      key={i}
                      d={d}
                      fill="none"
                      stroke={seriesColor(s, i)}
                      strokeWidth={(s.benchmark ? 1.25 : 2) * scale}
                      strokeDasharray={s.benchmark ? `${5 * scale},${4 * scale}` : undefined}
                      strokeLinejoin="round"
                    />
                  );
                })}
              {endLabels.map((l, i) => (
                <g key={i}>
                  <line
                    x1={width - pad.right + 4 * scale}
                    x2={width - pad.right + 14 * scale}
                    y1={l.y}
                    y2={l.y}
                    stroke={l.color}
                    strokeWidth={2 * scale}
                  />
                  <text
                    className="end-label"
                    x={width - pad.right + 18 * scale}
                    y={l.y + 3 * scale}
                  >
                    {l.name}
                  </text>
                </g>
              ))}
              {/* Hover markers: series-colored dots ringed with the surface so
                  overlapping curves stay separable. */}
              {hover &&
                hover.rows.map((r, i) => (
                  <circle
                    key={i}
                    cx={r.x}
                    cy={r.y}
                    r={4 * scale}
                    fill={r.color}
                    stroke="#1a1a19"
                    strokeWidth={2 * scale}
                  />
                ))}
            </svg>
            {hover && (
              <div
                className="chart-tip"
                style={{
                  left: hover.px,
                  top: pad.top + 4 * scale,
                  transform: tipOnLeft
                    ? "translateX(calc(-100% - 0.9em))"
                    : "translateX(0.9em)",
                }}
              >
                <div className="tip-date">{hover.date}</div>
                {hover.rows.map((r, i) => (
                  <div className="tip-row" key={i}>
                    <span className="swatch" style={{ background: r.color }} />
                    <span className="tip-name">{r.name}</span>
                    <span className="tip-val">{fmtDollars(r.v)}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      <div className="panel">
        <div className="panel-head">
          <span className="panel-title">Metrics</span>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Portfolio</th>
                <th>Strategy</th>
                <th className="num">Final value</th>
                <th className="num">Profit</th>
                <th className="num">Sharpe</th>
                <th className="num">Sortino</th>
                <th className="num">Max DD %</th>
                <th className="num">Annual %</th>
                <th className="num">Std Dev</th>
                <th className="num">Turnover</th>
                <th className="num">Avg Corr</th>
                <th className="num">Coint Pairs</th>
                {benchmarked && (
                  <>
                    <th className="num">Alpha %</th>
                    <th className="num">Beta</th>
                    <th className="num">Track Err</th>
                    <th className="num">Info Ratio</th>
                    <th className="num">Up Cap %</th>
                    <th className="num">Down Cap %</th>
                  </>
                )}
              </tr>
            </thead>
            <tbody>
              {results.map((r, i) => {
                const p = profit(r);
                return (
                  <tr key={i}>
                    <td>
                      <span
                        className="swatch"
                        style={{
                          background: COLORS[i % COLORS.length],
                          display: "inline-block",
                          marginRight: "0.5em",
                        }}
                      />
                      {r.portfolioName}
                    </td>
                    <td className="strategy" title={r.strategy}>
                      {r.strategy}
                    </td>
                    <td className="num">{fmtDollars(r.finalValue)}</td>
                    <td className={`num ${p && p.abs < 0 ? "neg" : "pos"}`}>
                      {p
                        ? `${p.abs >= 0 ? "+" : "−"}${fmtDollars(Math.abs(p.abs))}`
                        : "—"}
                    </td>
                    <td className="num">{r.sharpeRatio.toFixed(2)}</td>
                    <td className="num">{r.sortinoRatio.toFixed(2)}</td>
                    <td className="num">{r.maxDrawdown.toFixed(2)}</td>
                    <td className="num">{r.annualReturn.toFixed(2)}</td>
                    <td className="num">{r.standardDev.toFixed(4)}</td>
                    <td className="num">{r.turnover.toFixed(2)}</td>
                    <td className="num">{r.avgCorrelation.toFixed(2)}</td>
                    <td className="num">{r.cointegratedPairs}</td>
                    {benchmarked && (
                      <>
                        <td className="num">{r.alpha.toFixed(2)}</td>
                        <td className="num">{r.beta.toFixed(2)}</td>
                        <td className="num">{r.trackingError.toFixed(4)}</td>
                        <td className="num">{r.informationRatio.toFixed(2)}</td>
                        <td className="num">{r.upCapture.toFixed(1)}</td>
                        <td className="num">{r.downCapture.toFixed(1)}</td>
                      </>
                    )}
                  </tr>
                );
              })}
              {/* One row per distinct benchmark, after the portfolios and
                  visually subordinate: it is the reference the rows above are
                  measured against, not another result. Only the columns that
                  describe a return series on its own terms are filled in —
                  a benchmark has no strategy, no turnover and no alpha
                  against itself. */}
              {benchmarkRows.map((b) => (
                <tr key={`bench-${b.ticker}`} className="benchmark-row">
                  <td>
                    <span
                      className="swatch"
                      style={{
                        background: BENCHMARK_COLOR,
                        display: "inline-block",
                        marginRight: "0.5em",
                      }}
                    />
                    {b.ticker}
                  </td>
                  <td className="strategy">benchmark</td>
                  <td className="num">{fmtDollars(b.finalValue)}</td>
                  <td className={`num ${b.profit < 0 ? "neg" : "pos"}`}>
                    {`${b.profit >= 0 ? "+" : "−"}${fmtDollars(Math.abs(b.profit))}`}
                  </td>
                  <td className="num">{b.stats.sharpeRatio.toFixed(2)}</td>
                  <td className="num">—</td>
                  <td className="num">{b.stats.maxDrawdown.toFixed(2)}</td>
                  <td className="num">{b.stats.annualReturn.toFixed(2)}</td>
                  <td className="num">{b.stats.standardDev.toFixed(4)}</td>
                  <td className="num">—</td>
                  <td className="num">—</td>
                  <td className="num">—</td>
                  {benchmarked && (
                    <>
                      <td className="num">—</td>
                      <td className="num">—</td>
                      <td className="num">—</td>
                      <td className="num">—</td>
                      <td className="num">—</td>
                      <td className="num">—</td>
                    </>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {results.some((r) => (r.drawdowns ?? []).length > 0) && (
        <div className="panel">
          <div className="panel-head">
            <span className="panel-title">Deepest drawdowns</span>
          </div>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Portfolio</th>
                  <th className="num">Depth %</th>
                  <th>Peak</th>
                  <th>Trough</th>
                  <th>Recovered</th>
                  <th className="num">Days down</th>
                  <th className="num">Days to recover</th>
                </tr>
              </thead>
              <tbody>
                {results.flatMap((r, i) =>
                  (r.drawdowns ?? []).slice(0, 5).map((d, j) => (
                    <tr key={`${i}-${j}`}>
                      <td>
                        <span
                          className="swatch"
                          style={{
                            background: COLORS[i % COLORS.length],
                            display: "inline-block",
                            marginRight: "0.5em",
                          }}
                        />
                        {r.portfolioName}
                      </td>
                      <td className="num neg">{d.depthPct.toFixed(2)}</td>
                      <td>{fmtDate(d.start)}</td>
                      <td>{fmtDate(d.trough)}</td>
                      <td>{d.ongoing ? "ongoing" : fmtDate(d.recovered)}</td>
                      <td className="num">{d.durationDays}</td>
                      <td className="num">
                        {d.ongoing ? "—" : d.recoveryDays}
                      </td>
                    </tr>
                  )),
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {results.some((r) => (r.yearlyReturns ?? []).length > 0) && (
        <div className="panel">
          <div className="panel-head">
            <span className="panel-title">Calendar year returns</span>
          </div>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Year</th>
                  {results.map((r, i) => (
                    <th className="num" key={i} title={r.portfolioName}>
                      <span
                        className="swatch"
                        style={{
                          background: COLORS[i % COLORS.length],
                          display: "inline-block",
                          marginRight: "0.5em",
                        }}
                      />
                      {r.portfolioName}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {Array.from(
                  new Set(
                    results.flatMap((r) =>
                      (r.yearlyReturns ?? []).map((y) => y.period),
                    ),
                  ),
                )
                  .sort()
                  .map((period) => (
                    <tr key={period}>
                      <td>{period}</td>
                      {results.map((r, i) => {
                        const y = (r.yearlyReturns ?? []).find(
                          (p) => p.period === period,
                        );
                        if (!y) return <td className="num" key={i}>—</td>;
                        return (
                          <td
                            className={`num ${y.returnPct < 0 ? "neg" : "pos"}`}
                            key={i}
                            title={y.partial ? "partial year" : undefined}
                          >
                            {y.returnPct.toFixed(2)}
                            {y.partial ? "*" : ""}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
          <div className="panel-note">
            * partial period — the run does not span the whole year. Shown as
            the plain return over the days present, not annualized.
          </div>
        </div>
      )}
    </div>
  );
}
