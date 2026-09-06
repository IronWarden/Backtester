import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { backtest, main } from "../wailsjs/go/models";
import { readStored, readStoredObject, writeStored } from "./storage";

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

// A portfolio's colour chip. Every panel that names a portfolio shows one, so
// this is the single place the palette lookup and the two spacing variants
// live: `inline` is the form that sits before a name inside a table cell.
function Swatch({
  index,
  color,
  inline,
}: {
  index?: number;
  color?: string;
  inline?: boolean;
}) {
  const background = color ?? COLORS[(index ?? 0) % COLORS.length];
  return (
    <span
      className="swatch"
      style={
        inline
          ? { background, display: "inline-block", marginRight: "0.5em" }
          : { background }
      }
    />
  );
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
// The engine reports 0 for "CPI does not cover both ends of this window", which
// is a different claim from "real growth was zero" and must not be rendered as
// 0.00. Exactly zero is unreachable for a real run, so the sentinel is safe.
function fmtReal(v: number | undefined): string {
  return v ? v.toFixed(2) : "—";
}

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

// Money the user put in is not money the strategy made. Contributing 500 a
// month for ten years grows the account by 60,000 that was never a gain, so
// the deposits are netted out — otherwise the Profit column reports the
// paying-in as performance. Zero without a [portfolio.Cashflow] schedule, so
// this is the old expression for every run that has none.
//
// The percentage stays against the starting capital rather than against
// capital-plus-contributions: a money-weighted figure is a different question
// (what did MY money earn) and answering it halfway here would be worse than
// not answering it.
function profit(r: main.RunResult): { abs: number; pct: number } | null {
  if (!r.initialCapital) return null;
  const abs = r.finalValue - r.initialCapital - (r.netContributions ?? 0);
  return { abs, pct: (abs / r.initialCapital) * 100 };
}

// Whether any portfolio in this run had a cashflow schedule. The columns it
// governs are blank for everyone else, and a wall of zeros is worse than no
// column — the same rule the sweep and benchmark column groups follow.
function hasCashflows(results: main.RunResult[]): boolean {
  return results.some(
    (r) => (r.netContributions ?? 0) !== 0 || (r.withdrawalsMissed ?? 0) > 0,
  );
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

// The mechanical judgement against a pre-registered criterion. Rendered above
// the standing objections because it is the question the user actually asked;
// the strip below is the set of questions asked on their behalf.
//
// Three verdicts, and the colours say what they mean rather than good/bad:
// rejected is a claim about the STRATEGY, inconclusive a claim about the
// EVIDENCE. They are not degrees of the same thing, so inconclusive is amber
// and neutral rather than a paler shade of failure.
function HypothesisVerdicts({ verdicts }: { verdicts: backtest.VerdictReport[] }) {
  if (verdicts.length === 0) return null;
  return (
    <div className="panel">
      <div className="panel-head">
        <span className="panel-title">Hypothesis verdict</span>
      </div>
      <div className="verdicts">
        {verdicts.map((v, i) => (
          <div className="verdict-group" key={i}>
            <div className={`vhead ${v.verdict}`}>
              <span className="vverdict">{v.verdict}</span>
              <span className="vid">{v.hypothesisId}</span>
              {v.portfolio && <span className="vport">{v.portfolio}</span>}
            </div>
            {v.claim && <div className="vclaim">{v.claim}</div>}
            <div className="vcriterion">
              criterion <code>{v.criterion}</code>
              {v.campaign
                ? ` · campaign ${v.campaign}: ${v.trialsSpent} of ${v.budget} trials spent`
                : ""}
            </div>
            {v.budgetSpent && (
              <div className="vbudget">
                Budget spent — further searching in this campaign is not evidence.
              </div>
            )}
            <ul className="vchecks">
              {(v.checks ?? []).map((c: backtest.Check, j: number) => (
                <li className={`vcheck ${c.status}`} key={j}>
                  <span className="vstatus">
                    {c.status === "pass"
                      ? "PASS"
                      : c.status === "fail"
                        ? "FAIL"
                        : "n/a"}
                  </span>
                  <span className="vname">{c.name}</span>
                  <span className="vdetail">{c.detail}</span>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    </div>
  );
}

// The three questions that kill an idea, in the same visual weight as the
// return above them. They are computed on every run and used to be visible
// only on the CLI, which is the same failure mode as burying them in a
// footnote: a candidate that looks good because it was the best of two hundred
// tries should not be able to hide the two hundred.
//
// Deliberately mirrors the -pages export (src/backtest/candidatepage.go) card
// for card, so a result read in the app and the same result read from a file
// say the same thing.
function VerdictStrip({ results }: { results: main.RunResult[] }) {
  const shown = results.filter(
    (r) => r.baseline?.computed || r.significance?.computed || (r.trials ?? 0) > 1,
  );
  if (shown.length === 0) return null;

  return (
    <div className="panel">
      <div className="panel-head">
        <span className="panel-title">Does this survive the obvious objections?</span>
      </div>
      <div className="verdicts">
        {shown.map((r, i) => (
          <div className="verdict-group" key={i}>
            {shown.length > 1 && (
              <div className="verdict-owner">{r.portfolioName}</div>
            )}
            <div className="verdict-cards">
              {r.baseline?.computed ? (
                <div className={`vcard ${r.baseline.beat ? "good" : "bad"}`}>
                  <span className="vq">Beat {r.baseline.name}?</span>
                  <span className="va">{r.baseline.beat ? "YES" : "NO"}</span>
                  <span className="vd">
                    {r.baseline.excessAnnualReturn >= 0 ? "+" : ""}
                    {r.baseline.excessAnnualReturn.toFixed(2)}%/yr ·{" "}
                    {fmtDollars(r.baseline.excessFinalValue)} in final value
                  </span>
                </div>
              ) : (
                <div className="vcard unknown">
                  <span className="vq">Beat the baseline?</span>
                  <span className="va">—</span>
                  <span className="vd">no baseline could be built</span>
                </div>
              )}

              {r.significance?.computed ? (
                <div className={`vcard ${r.significance.pValue <= 0.05 ? "good" : "bad"}`}>
                  <span className="vq">Better than random timing?</span>
                  <span className="va">
                    {r.significance.pValue <= 0.05 ? "YES" : "NO"}
                  </span>
                  <span className="vd">
                    p = {r.significance.pValue.toFixed(3)} ·{" "}
                    {r.significance.percentile.toFixed(0)}th percentile of{" "}
                    {r.significance.draws} draws
                  </span>
                </div>
              ) : (
                <div className="vcard unknown">
                  <span className="vq">Better than random timing?</span>
                  <span className="va">—</span>
                  <span className="vd">not computed</span>
                </div>
              )}

              <div className={`vcard ${trialClass(r)}`}>
                <span className="vq">How many trials was this picked from?</span>
                <span className="va">{Math.max(r.trials ?? 0, 1)}</span>
                <span className="vd">
                  deflated Sharpe {r.deflatedSharpe.toFixed(3)} · a no-edge
                  best-of-{Math.max(r.trials ?? 0, 1)} would show{" "}
                  {r.expectedMaxSharpe.toFixed(2)}
                </span>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// An unswept run is not being warned about at all; a swept one always is, and
// how loudly depends on whether the deflated Sharpe clears the bar. Same rule
// as writeVerdictStrip in candidatepage.go.
function trialClass(r: main.RunResult): string {
  if ((r.trials ?? 0) <= 1) return "good";
  return r.deflatedSharpe < 0.9 ? "bad" : "warn";
}

type Props = {
  results: main.RunResult[];
  fontSize: number;
  verdicts?: backtest.VerdictReport[];
};

// Panels below the headline ones open on click and remember it. Eleven panels
// rendering expanded at once is most of what "overwhelmed on first open"
// means, and the ones worth reading first — the verdicts, the curve, the
// metrics — are not the ones that take the most room.
//
// Presentation only: nothing here changes a number, and every panel that
// takes this treatment already hides itself when it has nothing to show, so a
// collapsed header never stands for an empty body.
const PANEL_STATE_KEY = "backtester-results-panels";

// Shape-guarded, not just parse-guarded: the indexing that follows happens in
// Collapsible, outside any try here, so a stored `"null"` would throw where
// nothing catches it. See storage.ts.
function readPanelState(): Record<string, boolean> {
  return readStoredObject<boolean>(PANEL_STATE_KEY);
}

function writePanelOpen(id: string, open: boolean) {
  // A browser that refuses storage still gets a working panel; it just
  // forgets the choice, which is the right way to degrade.
  writeStored(PANEL_STATE_KEY, JSON.stringify({ ...readPanelState(), [id]: open }));
}

// Which metric columns the table shows at rest.
//
// The table is honest and complete -- around twenty numeric columns, more once
// a benchmark, a sweep or a contribution schedule is involved -- and that is
// exactly what makes a first result unreadable: nothing on screen says which
// numbers answer the question that was asked. So the table has two views. The
// full one is what it has always shown; the basic one keeps the six columns a
// first run is actually about, and hides the rest behind one click. Nothing is
// removed, and a hidden column is still one hover from its definition.
//
// The keys are the single source of truth: the header, the portfolio rows and
// the benchmark rows all ask `show(key)`, so a column cannot appear in one and
// not the other. `TestMetricTableColumnsLineUp` in smoke.tsx counts the cells
// of both views to prove it.
// One rule decides membership: a column stays in Basics if hiding it would
// make a visible one misleading. Profit without Contributed reads as
// investment return when it is partly money paid in; a swept Sharpe without
// Confidence reads as edge when it may be selection; a plan that ran out of
// money says so in Missed W/D and nowhere else. The four conditional ones only
// render when their run produced them, so naming them here costs nothing on a
// plain run.
const BASIC_COLUMNS = new Set([
  "portfolio",
  "strategy",
  "finalValue",
  "profit",
  "sharpe",
  "maxDrawdown",
  "annualReturn",
  "contributed",
  "missedWD",
  "confidence",
  "alpha",
]);

const METRICS_VIEW_KEY = "backtester-metrics-view";

export type MetricsView = "basic" | "full";

// Basics on a first visit: someone who has never seen the table is exactly
// who the wall of columns costs the most.
function readMetricsView(): MetricsView {
  return readStored(METRICS_VIEW_KEY) === "full" ? "full" : "basic";
}

// visibleColumn is exported for the smoke run, which needs the same predicate
// the table uses rather than a copy of the rule.
export function visibleColumn(view: MetricsView, key: string): boolean {
  return view === "full" || BASIC_COLUMNS.has(key);
}

function Collapsible({
  id,
  title,
  defaultOpen = false,
  children,
}: {
  id: string;
  title: string;
  // What the panel does before the user has ever touched it. Every panel here
  // defaults closed except the reading guide, which is worth exactly one
  // showing: open the first time, and closed for good once it has been read
  // and collapsed.
  defaultOpen?: boolean;
  children: ReactNode;
}) {
  const [open, setOpen] = useState(() => readPanelState()[id] ?? defaultOpen);
  function toggle() {
    setOpen((prev) => {
      writePanelOpen(id, !prev);
      return !prev;
    });
  }
  return (
    <div className={`panel collapsible ${open ? "open" : "closed"}`}>
      <button
        type="button"
        className="panel-head panel-toggle"
        aria-expanded={open}
        onClick={toggle}
      >
        <span className="panel-chevron" aria-hidden="true">
          {open ? "\u25be" : "\u25b8"}
        </span>
        <span className="panel-title">{title}</span>
      </button>
      {open && children}
    </div>
  );
}

export default function ResultsView({ results, fontSize, verdicts }: Props) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(800);
  // Which metric columns to show. Remembered like the collapsible panels, so
  // a user who wants everything asks for it once.
  const [metricsView, setMetricsView] = useState<MetricsView>(readMetricsView);
  const show = (key: string) => visibleColumn(metricsView, key);
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

  // Every hook must run before the early return below: React counts hooks
  // per render, and ResultsView stays mounted across a run (App clears
  // results, then fills them), so a hook placed after the return would be
  // skipped on the empty render and appear on the next one -- "rendered
  // more hooks than during the previous render", which unmounts the tree.
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

  // Computed for the plotted set, not for every result: a portfolio with no
  // curve is not on the chart the note sits under.
  const windowNote = differingWindows(plottable);

  const benchmarked = hasBenchmark(results);

  const contributing = hasCashflows(results);
  // Only meaningful when something was actually searched: a single untried
  // portfolio has no selection bias to correct for, and showing a bar of 0.00
  // next to it would imply otherwise.
  const swept = results.some((r) => (r.trials ?? 0) > 1);

  return (
    <div className="results-view">
      {/* Open the first time, closed once collapsed. The tooltips explain
          every column, but a beginner does not know to hover, and nothing
          else on the page says which of these panels decides whether the idea
          was any good. Static text: it states no number, so it cannot
          disagree with one. */}
      <Collapsible id="how-to-read" title="How to read this" defaultOpen>
        <div className="how-to-read">
          <p>
            <b>The sentence at the top is the result.</b> Every figure in it
            comes from the table below — it is the same run said in words, not
            a summary that rounds or a second opinion.
          </p>
          <p>
            <b>“Does this survive the obvious objections?” is the part that
            decides things.</b> A strategy is not good because it made money;
            it is good because it beat the alternatives you already had.
          </p>
          <ul>
            <li>
              <b>Beat the baseline?</b> The baseline is an equal-weight
              buy-and-hold of <i>the same tickers</i>, over the same days, with
              the same costs. A <b>NO</b> here means the strategy lost to
              simply holding what it traded — the rules earned nothing, however
              good the return looks.
            </li>
            <li>
              <b>Better than random timing?</b> The same trades, shuffled onto
              random days, many times over. A <b>NO</b> means the timing was
              indistinguishable from luck.
            </li>
            <li>
              <b>How many trials was this picked from?</b> Search enough
              settings and one of them looks brilliant by accident. The
              deflated Sharpe beside it is what the number is worth after
              accounting for the search.
            </li>
          </ul>
          <p>
            <b>Of the numbers, three answer most questions:</b> the annual
            return, the max drawdown — the deepest hole the account was ever
            in, which is what actually decides whether a strategy gets
            abandoned partway through — and how both compare to the benchmark
            row at the bottom of the table.
          </p>
          <p>
            The metrics table opens on <b>Basics</b>. <b>Everything</b> is the
            full set, and every column explains itself on hover.
          </p>
        </div>
      </Collapsible>

      {/* What happened, in a sentence, before any of the numbers. Written by
          the engine (backtest.Sentence), not assembled here: the app, the
          -pages export and the assistant then say the same thing about the
          same run, and the sentence cannot drift from the table under it.
          A result that simulated nothing carries no headline and shows
          none -- silence rather than an empty quote. */}
      {results.some((r) => r.headline) && (
        <div className="panel headlines">
          {results.map((r, i) =>
            r.headline ? (
              <p className="headline-line" key={i}>
                {results.length > 1 && (
                  <span className="headline-who">
                    <Swatch index={i} inline />
                    {r.portfolioName}
                  </span>
                )}
                {r.headline}
              </p>
            ) : null,
          )}
        </div>
      )}
      <div className="stat-grid">
        {results.map((r, i) => {
          const p = profit(r);
          return (
            <div className="stat-card" key={i}>
              <div className="stat-name" title={r.portfolioName}>
                <Swatch index={i} />
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

      <HypothesisVerdicts verdicts={verdicts ?? []} />

      <VerdictStrip results={results} />

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
                    <Swatch index={i} />
                    {r.portfolioName}
                  </span>
                ))}
              </div>
            )}
          </div>
          {windowNote && <div className="panel-note">{windowNote}</div>}
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
                    <Swatch color={r.color} />
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
          {/* Not a "show more" link: both views are named, so the basic one
              reads as a choice rather than as the table failing to load. */}
          <div className="metrics-view" role="group" aria-label="Metric columns">
            {(["basic", "full"] as MetricsView[]).map((v) => (
              <button
                key={v}
                type="button"
                className={`metrics-view-btn ${metricsView === v ? "on" : ""}`}
                aria-pressed={metricsView === v}
                title={
                  v === "basic"
                    ? "The six numbers a first run is about"
                    : "Every metric the engine computed"
                }
                onClick={() => {
                  setMetricsView(v);
                  writeStored(METRICS_VIEW_KEY, v);
                }}
              >
                {v === "basic" ? "Basics" : "Everything"}
              </button>
            ))}
          </div>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              {/* Every cell in this table -- header, portfolio row, benchmark
                  row -- is gated on the same show(key), so a column cannot
                  appear in one and be missing from another. That failure is
                  not a gap in a row: it slides every later value one column
                  left, printing a number under the wrong label. It has
                  happened once already; see the benchmark row below. */}
              <tr>
                {show("portfolio") && <th>Portfolio</th>}
                {show("strategy") && <th>Strategy</th>}
                {show("finalValue") && <th className="num">Final value</th>}
                {show("profit") && <th className="num">Profit</th>}
                {show("sharpe") && <th className="num">Sharpe</th>}
                {show("sortino") && <th className="num">Sortino</th>}
                {show("calmar") && (
                  <th
                    className="num"
                    title="Annual return per unit of worst decline; 0 means it never declined"
                  >
                    Calmar
                  </th>
                )}
                {show("maxDrawdown") && <th className="num">Max DD %</th>}
                {show("underwater") && (
                  <th
                    className="num"
                    title="Longest run of calendar days below a previous peak"
                  >
                    Underwater
                  </th>
                )}
                {show("annualReturn") && <th className="num">Annual %</th>}
                {show("realAnnual") && (
                  <th
                    className="num"
                    title="Annual % with inflation divided out (CPI). An em dash means CPI does not cover both ends of the window — it is published a month in arrears — not that real growth was zero."
                  >
                    Real %
                  </th>
                )}
                {show("stdDev") && <th className="num">Std Dev</th>}
                {show("turnover") && <th className="num">Turnover</th>}
                {show("exposure") && (
                  <th className="num" title="Mean invested fraction of the book">
                    Exposure %
                  </th>
                )}
                {show("fundCov") && (
                  <th
                    className="num"
                    title="Share of day x ticker x declared-metric cells that had a published figure. Blank unless the config declares Fundamentals."
                  >
                    Fund Cov %
                  </th>
                )}
                {show("inMarket") && (
                  <th className="num" title="Share of simulated days holding anything">
                    In Market %
                  </th>
                )}
                {show("avgCorr") && <th className="num">Avg Corr</th>}
                {show("cointPairs") && <th className="num">Coint Pairs</th>}
                {contributing && (
                  <>
                    {show("contributed") && (
                      <th
                        className="num"
                        title="Net money paid in by the [portfolio.Cashflow] schedule — deposits positive, withdrawals negative. Already subtracted from Profit."
                      >
                        Contributed
                      </th>
                    )}
                    {show("missedWD") && (
                      <th
                        className="num"
                        title="Scheduled withdrawals that could not be funded even after selling everything. Non-zero means the portfolio ran out of money."
                      >
                        Missed W/D
                      </th>
                    )}
                  </>
                )}
                {swept && (
                  <>
                    {show("trials") && <th className="num">Trials</th>}
                    {show("luckBar") && <th className="num">Luck bar</th>}
                    {show("confidence") && <th className="num">Confidence</th>}
                  </>
                )}
                {benchmarked && (
                  <>
                    {show("alpha") && <th className="num">Alpha %</th>}
                    {show("beta") && <th className="num">Beta</th>}
                    {show("trackErr") && <th className="num">Track Err</th>}
                    {show("infoRatio") && <th className="num">Info Ratio</th>}
                    {show("upCap") && <th className="num">Up Cap %</th>}
                    {show("downCap") && <th className="num">Down Cap %</th>}
                  </>
                )}
              </tr>
            </thead>
            <tbody>
              {results.map((r, i) => {
                const p = profit(r);
                return (
                  <tr key={i}>
                    {show("portfolio") && (
                      <td>
                        <Swatch index={i} inline />
                        {r.portfolioName}
                      </td>
                    )}
                    {show("strategy") && (
                      <td className="strategy" title={r.strategy}>
                        {r.strategy}
                      </td>
                    )}
                    {show("finalValue") && (
                      <td className="num">{fmtDollars(r.finalValue)}</td>
                    )}
                    {show("profit") && (
                      <td className={`num ${p && p.abs < 0 ? "neg" : "pos"}`}>
                        {p
                          ? `${p.abs >= 0 ? "+" : "−"}${fmtDollars(Math.abs(p.abs))}`
                          : "—"}
                      </td>
                    )}
                    {show("sharpe") && (
                      <td className="num">{r.sharpeRatio.toFixed(2)}</td>
                    )}
                    {show("sortino") && (
                      <td className="num">{r.sortinoRatio.toFixed(2)}</td>
                    )}
                    {show("calmar") && (
                      <td className="num">{r.calmarRatio.toFixed(2)}</td>
                    )}
                    {show("maxDrawdown") && (
                      <td className="num">{r.maxDrawdown.toFixed(2)}</td>
                    )}
                    {show("underwater") && (
                      <td className="num">{r.longestUnderwaterDays}</td>
                    )}
                    {show("annualReturn") && (
                      <td className="num">{r.annualReturn.toFixed(2)}</td>
                    )}
                    {/* Zero means "CPI did not reach both ends of this
                        window", not "no real growth" — opposite claims, so it
                        must not render as 0.00. */}
                    {show("realAnnual") && (
                      <td className="num">{fmtReal(r.realAnnualReturn)}</td>
                    )}
                    {show("stdDev") && (
                      <td className="num">{r.standardDev.toFixed(4)}</td>
                    )}
                    {show("turnover") && (
                      <td className="num">{r.turnover.toFixed(2)}</td>
                    )}
                    {show("exposure") && (
                      <td className="num">{r.avgExposure.toFixed(1)}</td>
                    )}
                    {/* Blank rather than 0.0 when no Fundamentals were
                        declared: a zero here would read as "the data was
                        missing" rather than "this run never asked for it". */}
                    {show("fundCov") && (
                      <td className="num">
                        {(r.fundamentalCoverage ?? 0) > 0
                          ? r.fundamentalCoverage.toFixed(1)
                          : "—"}
                      </td>
                    )}
                    {show("inMarket") && (
                      <td className="num">{r.timeInMarket.toFixed(1)}</td>
                    )}
                    {show("avgCorr") && (
                      <td className="num">{r.avgCorrelation.toFixed(2)}</td>
                    )}
                    {show("cointPairs") && (
                      <td className="num">{r.cointegratedPairs}</td>
                    )}
                    {contributing && (
                      <>
                        {show("contributed") && (
                          <td className="num">{fmtDollars(r.netContributions ?? 0)}</td>
                        )}
                        {/* A missed withdrawal is not a statistic, it is the
                            result: the plan ran out of money. Coloured as the
                            negative it is. */}
                        {show("missedWD") && (
                          <td className={`num ${(r.withdrawalsMissed ?? 0) > 0 ? "neg" : ""}`}>
                            {r.withdrawalsMissed ?? 0}
                          </td>
                        )}
                      </>
                    )}
                    {swept && (
                      <>
                        {show("trials") && <td className="num">{r.trials}</td>}
                        {show("luckBar") && (
                          <td className="num">{r.expectedMaxSharpe.toFixed(2)}</td>
                        )}
                        {/* Confidence below 0.5 means the headline Sharpe is
                            more likely selection bias than edge, so it is
                            coloured as a negative rather than left neutral. */}
                        {show("confidence") && (
                          <td
                            className={`num ${r.deflatedSharpe < 0.5 ? "neg" : "pos"}`}
                          >
                            {r.deflatedSharpe.toFixed(2)}
                          </td>
                        )}
                      </>
                    )}
                    {benchmarked && (
                      <>
                        {show("alpha") && <td className="num">{r.alpha.toFixed(2)}</td>}
                        {show("beta") && <td className="num">{r.beta.toFixed(2)}</td>}
                        {show("trackErr") && (
                          <td className="num">{r.trackingError.toFixed(4)}</td>
                        )}
                        {show("infoRatio") && (
                          <td className="num">{r.informationRatio.toFixed(2)}</td>
                        )}
                        {show("upCap") && (
                          <td className="num">{r.upCapture.toFixed(1)}</td>
                        )}
                        {show("downCap") && (
                          <td className="num">{r.downCapture.toFixed(1)}</td>
                        )}
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
                  {show("portfolio") && (
                    <td>
                      <Swatch color={BENCHMARK_COLOR} inline />
                      {b.ticker}
                    </td>
                  )}
                  {show("strategy") && <td className="strategy">benchmark</td>}
                  {show("finalValue") && (
                    <td className="num">{fmtDollars(b.finalValue)}</td>
                  )}
                  {show("profit") && (
                    <td className={`num ${b.profit < 0 ? "neg" : "pos"}`}>
                      {`${b.profit >= 0 ? "+" : "−"}${fmtDollars(Math.abs(b.profit))}`}
                    </td>
                  )}
                  {/* Cell for cell against the header, in header order, and
                      gated on the same show(key) so the two cannot disagree.
                      The four BenchmarkStats carries land in their own columns
                      and everything else is an em dash. This row used to emit
                      five cells too few, which did not leave the row short —
                      it slid every value left, printing max drawdown under
                      Calmar and annual return under Max DD %. A number under
                      the wrong label is worse than no number. */}
                  {show("sharpe") && (
                    <td className="num">{b.stats.sharpeRatio.toFixed(2)}</td>
                  )}
                  {show("sortino") && <td className="num">—</td>}
                  {/* Calmar is annual return over worst decline, and the
                      engine has a rule for the zero-drawdown case. Deriving it
                      here would risk printing a figure that disagrees with the
                      column it sits in. */}
                  {show("calmar") && <td className="num">—</td>}
                  {show("maxDrawdown") && (
                    <td className="num">{b.stats.maxDrawdown.toFixed(2)}</td>
                  )}
                  {show("underwater") && <td className="num">—</td>}
                  {show("annualReturn") && (
                    <td className="num">{b.stats.annualReturn.toFixed(2)}</td>
                  )}
                  {show("realAnnual") && (
                    <td className="num">{fmtReal(b.stats.realAnnualReturn)}</td>
                  )}
                  {show("stdDev") && (
                    <td className="num">{b.stats.standardDev.toFixed(4)}</td>
                  )}
                  {show("turnover") && <td className="num">—</td>}
                  {show("exposure") && <td className="num">—</td>}
                  {show("fundCov") && <td className="num">—</td>}
                  {show("inMarket") && <td className="num">—</td>}
                  {show("avgCorr") && <td className="num">—</td>}
                  {show("cointPairs") && <td className="num">—</td>}
                  {contributing && (
                    <>
                      {show("contributed") && <td className="num">—</td>}
                      {show("missedWD") && <td className="num">—</td>}
                    </>
                  )}
                  {swept && (
                    <>
                      {show("trials") && <td className="num">—</td>}
                      {show("luckBar") && <td className="num">—</td>}
                      {show("confidence") && <td className="num">—</td>}
                    </>
                  )}
                  {benchmarked && (
                    <>
                      {show("alpha") && <td className="num">—</td>}
                      {show("beta") && <td className="num">—</td>}
                      {show("trackErr") && <td className="num">—</td>}
                      {show("infoRatio") && <td className="num">—</td>}
                      {show("upCap") && <td className="num">—</td>}
                      {show("downCap") && <td className="num">—</td>}
                    </>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <StrategyPlots results={results} />

      {results.some((r) => (r.walkForward?.windows ?? []).length > 0) && (
        <Collapsible id="walk-forward" title="Walk-forward windows">
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Portfolio</th>
                  <th>Train</th>
                  <th>Test</th>
                  <th>Selected</th>
                  <th className="num">Train score</th>
                  <th className="num">Test return %</th>
                  <th className="num">Capital</th>
                </tr>
              </thead>
              <tbody>
                {results.flatMap((r, i) =>
                  (r.walkForward?.windows ?? []).map((w, j) => (
                    <tr key={`${i}-wf-${j}`}>
                      <td>
                        <Swatch index={i} inline />
                        {j === 0 ? r.portfolioName : ""}
                      </td>
                      <td className="mono">
                        {w.trainStart} → {w.trainEnd}
                      </td>
                      <td className="mono">
                        {w.testStart} → {w.testEnd}
                      </td>
                      <td className="strategy" title={w.selected}>
                        {w.selected}
                      </td>
                      <td className="num">{w.trainScore.toFixed(2)}</td>
                      {/* The gap between these two columns is the whole
                          diagnostic: strong training scores next to weak test
                          returns is what overfitting looks like. */}
                      <td className={`num ${w.testReturn < 0 ? "neg" : "pos"}`}>
                        {w.testReturn.toFixed(2)}
                      </td>
                      <td className="num">{fmtDollars(w.capital)}</td>
                    </tr>
                  )),
                )}
              </tbody>
            </table>
          </div>
        </Collapsible>
      )}

      {results.some((r) => (r.splits ?? []).length > 0) && (
        <Collapsible id="splits" title="In-sample vs out-of-sample">
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Portfolio</th>
                  <th>Segment</th>
                  <th>From</th>
                  <th>To</th>
                  <th className="num">Days</th>
                  <th className="num">Return %</th>
                  <th className="num">Annual %</th>
                  <th className="num">Sharpe</th>
                  <th className="num">Sortino</th>
                  <th className="num">Max DD %</th>
                </tr>
              </thead>
              <tbody>
                {results.flatMap((r, i) =>
                  (r.splits ?? []).map((s) => (
                    <tr
                      key={`${i}-${s.label}`}
                      /* The out-of-sample row is the one that carries
                         information about a tuned parameter set; the
                         in-sample row is the number that was optimised and
                         is dimmed so it cannot be mistaken for evidence. */
                      className={
                        s.label === "in-sample" ? "benchmark-row" : undefined
                      }
                    >
                      <td>
                        <Swatch index={i} inline />
                        {r.portfolioName}
                      </td>
                      <td className="strategy">{s.label}</td>
                      <td className="mono">{s.start}</td>
                      <td className="mono">{s.end}</td>
                      <td className="num">{s.days}</td>
                      <td
                        className={`num ${s.totalReturn < 0 ? "neg" : "pos"}`}
                      >
                        {s.totalReturn.toFixed(2)}
                      </td>
                      <td className="num">{s.annualReturn.toFixed(2)}</td>
                      <td className="num">{s.sharpeRatio.toFixed(2)}</td>
                      <td className="num">{s.sortinoRatio.toFixed(2)}</td>
                      <td className="num">{s.maxDrawdown.toFixed(2)}</td>
                    </tr>
                  )),
                )}
              </tbody>
            </table>
          </div>
        </Collapsible>
      )}

      {results.some((r) => (r.drawdowns ?? []).length > 0) && (
        <Collapsible id="drawdowns" title="Deepest drawdowns">
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
                        <Swatch index={i} inline />
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
        </Collapsible>
      )}

      {results.some((r) => (r.tradeStats?.trades ?? 0) > 0) && (
        <Collapsible id="trading" title="Trading">
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Portfolio</th>
                  <th className="num">Trades</th>
                  <th className="num">Round trips</th>
                  <th className="num">Win rate %</th>
                  <th className="num">Avg win</th>
                  <th className="num">Avg loss</th>
                  <th className="num">Profit factor</th>
                  <th className="num">Avg hold (d)</th>
                  <th className="num">Realized</th>
                  <th className="num">Fees</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r, i) => {
                  const t = r.tradeStats;
                  if (!t || t.trades === 0) return null;
                  return (
                    <tr key={i}>
                      <td>
                        <Swatch index={i} inline />
                        {r.portfolioName}
                      </td>
                      <td className="num">{t.trades}</td>
                      <td className="num">{t.roundTrips}</td>
                      <td className="num">{t.winRate.toFixed(1)}</td>
                      <td className="num pos">{fmtDollars(t.avgWin)}</td>
                      {/* AvgLoss is a positive magnitude (blotter.go sums
                          -RealizedPnL), so the sign has to be written in or a
                          red "$540" reads as if the field were already signed. */}
                      <td className="num neg">
                        {t.avgLoss > 0 ? "−" : ""}
                        {fmtDollars(Math.abs(t.avgLoss))}
                      </td>
                      <td className="num">{t.profitFactor.toFixed(2)}</td>
                      <td className="num">{t.avgHoldingDays.toFixed(0)}</td>
                      <td className={`num ${t.totalRealized < 0 ? "neg" : "pos"}`}>
                        {fmtDollars(t.totalRealized)}
                      </td>
                      <td className="num">{fmtDollars(t.totalFees)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Collapsible>
      )}

      {results.some((r) => (r.tradeStats?.perTicker ?? []).length > 0) && (
        <Collapsible id="attribution" title="Attribution — which position made the money">
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Portfolio</th>
                  <th>Ticker</th>
                  <th className="num">Total P&amp;L</th>
                  <th className="num">Realized</th>
                  <th className="num">Unrealized</th>
                  <th className="num">Fees</th>
                  <th className="num">Trades</th>
                  <th className="num">Shares held</th>
                </tr>
              </thead>
              <tbody>
                {results.flatMap((r, i) =>
                  // Biggest contributors first: the decision-changing question
                  // is whether one position carried the whole result.
                  [...(r.tradeStats?.perTicker ?? [])]
                    .sort(
                      (a, b) =>
                        Math.abs(b.realized + b.unrealized) -
                        Math.abs(a.realized + a.unrealized),
                    )
                    .map((t, j) => {
                      const total = t.realized + t.unrealized;
                      return (
                        <tr key={`${i}-${j}`}>
                          <td>
                            <Swatch index={i} inline />
                            {r.portfolioName}
                          </td>
                          <td>{t.ticker}</td>
                          <td className={`num ${total < 0 ? "neg" : "pos"}`}>
                            {fmtDollars(total)}
                          </td>
                          <td className="num">{fmtDollars(t.realized)}</td>
                          <td className="num">{fmtDollars(t.unrealized)}</td>
                          <td className="num">{fmtDollars(t.fees)}</td>
                          <td className="num">{t.trades}</td>
                          <td className="num">
                            {t.sharesHeld > 0 ? t.sharesHeld.toFixed(2) : "—"}
                          </td>
                        </tr>
                      );
                    }),
                )}
              </tbody>
            </table>
          </div>
        </Collapsible>
      )}

      {results.some((r) => (r.yearlyReturns ?? []).length > 0) && (
        <Collapsible id="yearly" title="Calendar year returns">
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Year</th>
                  {results.map((r, i) => (
                    <th className="num" key={i} title={r.portfolioName}>
                      <Swatch index={i} inline />
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
        </Collapsible>
      )}

      {results.some((r) => (r.monthlyReturns ?? []).length > 0) && (
        <Collapsible id="monthly" title="Monthly returns">
          {results.map((r, i) =>
            (r.monthlyReturns ?? []).length === 0 ? null : (
              <MonthGrid key={i} result={r} color={COLORS[i % COLORS.length]} showName={results.length > 1} />
            ),
          )}
          <div className="panel-note">
            * partial month — the run does not span the whole month. Shown as
            the plain return over the days present.
          </div>
        </Collapsible>
      )}
    </div>
  );
}

const MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

// A year-by-month matrix. The calendar-year table above answers "was this a
// good year"; this answers whether the year was steady or was one good month,
// which is the same question the deflated Sharpe asks of a sweep and the one a
// single annual figure hides most effectively.
// What the strategy plotted.
//
// log() could always say things; nothing could show a NUMBER over time. The
// moving average a script is crossing, the z-score it is waiting on, the
// exposure it thinks it is carrying -- those are what explain a run, and
// reading them out of a thousand console lines is not seeing them.
//
// One chart per series rather than extra lines on the equity chart, which is
// in dollars: an RSI between 0 and 100 drawn on a $184,000 axis is a flat line
// at the bottom, and giving each series its own axis on a shared chart is how
// two lines end up looking like they cross when they do not.
function PlotChart({
  series,
  dates,
}: {
  series: backtest.PlotSeries;
  dates: string[];
}) {
  const pts = series.points ?? [];
  if (pts.length === 0) return null;

  const values = pts.map((p) => p.value);
  let lo = Math.min(...values);
  let hi = Math.max(...values);
  // A flat series is a real answer (an indicator that never moved), so it is
  // drawn down the middle rather than dividing by a zero range.
  if (hi - lo < 1e-12) {
    lo -= 1;
    hi += 1;
  }
  // The x axis is the run's days, not the plotted points: a series that starts
  // on day 200 must start 200 days in, not at the left edge.
  const lastDay = Math.max(dates.length - 1, pts[pts.length - 1].day, 1);
  const W = 600;
  const H = 64;
  const x = (day: number) => (day / lastDay) * W;
  const y = (v: number) => H - ((v - lo) / (hi - lo)) * H;
  const d = pts.map((p) => `${x(p.day)},${y(p.value)}`).join(" ");
  const fmt = (v: number) =>
    Math.abs(v) >= 1000 ? v.toFixed(0) : Math.abs(v) >= 1 ? v.toFixed(2) : v.toFixed(4);

  return (
    <div className="plot-chart">
      <div className="plot-head">
        <span className="plot-name">{series.name}</span>
        <span className="plot-range">
          {fmt(lo)} … {fmt(hi)} · last {fmt(values[values.length - 1])}
        </span>
      </div>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        preserveAspectRatio="none"
        className="plot-svg"
        role="img"
        aria-label={`${series.name}: ${pts.length} points`}
      >
        <polyline
          points={d}
          fill="none"
          // Without this the non-uniform viewBox stretch would thin the line
          // horizontally and fatten it vertically.
          vectorEffect="non-scaling-stroke"
        />
      </svg>
    </div>
  );
}

function StrategyPlots({ results }: { results: main.RunResult[] }) {
  const withPlots = results.filter((r) => (r.plots ?? []).length > 0);
  if (withPlots.length === 0) return null;
  return (
    <Collapsible id="plots" title="What the strategy plotted">
      <div className="plots">
        {withPlots.map((r) => (
          <div className="plot-group" key={r.portfolioName}>
            {withPlots.length > 1 && (
              <div className="plot-owner">
                <Swatch index={results.indexOf(r)} inline />
                {r.portfolioName}
              </div>
            )}
            {(r.plots ?? []).map((s) => (
              <PlotChart key={s.name} series={s} dates={r.dates ?? []} />
            ))}
          </div>
        ))}
      </div>
    </Collapsible>
  );
}

// Two portfolios drawn as two lines on one chart read as a comparison. Each
// genuinely runs over its own window, which is correct -- a portfolio's window
// is decided by what its own holdings cover -- but two return series over
// different periods are not comparable, and nothing on the panel said so. A
// 2009-2020 portfolio beating a 2015-2020 one has mostly told you that it held
// through the recovery.
//
// A note, not a refusal: running two windows on purpose is legitimate.
// Exported so the smoke run exercises the decision rather than a copy of it.
export function differingWindows(results: main.RunResult[]): string {
  const spans = results
    .filter((r) => (r.dates ?? []).length > 0)
    .map((r) => ({
      name: r.portfolioName,
      from: r.dates[0],
      to: r.dates[r.dates.length - 1],
    }));
  if (spans.length < 2) return "";
  const first = spans[0];
  if (spans.every((s) => s.from === first.from && s.to === first.to)) return "";
  const listed = spans.map((s) => `${s.name} ${s.from} → ${s.to}`).join(" · ");
  return (
    `These portfolios cover different windows — ${listed}. ` +
    "Two return series over different periods are not comparable: the longer " +
    "one has been through market conditions the other never saw."
  );
}

function MonthGrid({
  result,
  color,
  showName,
}: {
  result: main.RunResult;
  color: string;
  showName: boolean;
}) {
  // Period is "YYYY-MM"; anything else is skipped rather than guessed at.
  const byYear = new Map<string, (PeriodReturnLike | undefined)[]>();
  for (const m of result.monthlyReturns ?? []) {
    const [year, month] = (m.period ?? "").split("-");
    const idx = Number(month) - 1;
    if (!year || Number.isNaN(idx) || idx < 0 || idx > 11) continue;
    if (!byYear.has(year)) byYear.set(year, new Array(12).fill(undefined));
    byYear.get(year)![idx] = m;
  }
  const years = [...byYear.keys()].sort();
  if (years.length === 0) return null;

  return (
    <div className="table-scroll">
      {showName && (
        <div className="month-owner">
          <Swatch color={color} inline />
          {result.portfolioName}
        </div>
      )}
      <table>
        <thead>
          <tr>
            <th>Year</th>
            {MONTH_LABELS.map((m) => (
              <th className="num" key={m}>
                {m}
              </th>
            ))}
            <th className="num">Year</th>
          </tr>
        </thead>
        <tbody>
          {years.map((y) => {
            const months = byYear.get(y)!;
            // Compound the months present rather than summing them: returns
            // are multiplicative, and a year with -50% then +50% is -25%.
            const compounded =
              (months.reduce(
                (acc, m) => acc * (1 + (m ? m.returnPct : 0) / 100),
                1,
              ) -
                1) *
              100;
            return (
              <tr key={y}>
                <td>{y}</td>
                {months.map((m, j) => (
                  <td
                    key={j}
                    className={`num ${m ? (m.returnPct < 0 ? "neg" : "pos") : "dim"}`}
                  >
                    {m ? `${m.returnPct.toFixed(1)}${m.partial ? "*" : ""}` : "—"}
                  </td>
                ))}
                <td className={`num ${compounded < 0 ? "neg" : "pos"}`}>
                  <b>{compounded.toFixed(1)}</b>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// The shape MonthGrid needs from a period return, kept narrow so the grid does
// not depend on fields it never reads.
type PeriodReturnLike = { period: string; returnPct: number; partial: boolean };
