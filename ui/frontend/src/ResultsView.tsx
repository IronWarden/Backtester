import { useEffect, useMemo, useRef, useState } from "react";
import { main } from "../wailsjs/go/models";

const COLORS = [
  "#4a9eff",
  "#f59e0b",
  "#22c55e",
  "#ef4444",
  "#a855f7",
  "#14b8a6",
  "#eab308",
  "#ec4899",
];

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

// Total return from a portfolio's equity curve (last vs first day), %.
function totalReturn(curve: number[]): number | null {
  if (!curve || curve.length < 2 || curve[0] === 0) return null;
  return (curve[curve.length - 1] / curve[0] - 1) * 100;
}

type Props = { results: main.RunResult[]; fontSize: number };

export default function ResultsView({ results, fontSize }: Props) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(800);

  // Track the container width so the SVG is drawn in real pixels (crisp text,
  // no aspect-ratio distortion) and reflows when the panel is resized.
  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width;
      if (w) setWidth(w);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Only portfolios with a real curve can be plotted.
  const plottable = useMemo(
    () => results.filter((r) => r.equityCurve && r.equityCurve.length > 1),
    [results],
  );

  const chart = useMemo(() => {
    if (plottable.length === 0) return null;

    const series = plottable.map((r) => ({
      name: r.portfolioName,
      pts: r.equityCurve.map((v, i) => ({
        t: Date.parse(r.dates[i] ?? ""),
        v,
      })),
    }));

    let tMin = Infinity,
      tMax = -Infinity,
      yMin = Infinity,
      yMax = -Infinity;
    for (const s of series)
      for (const p of s.pts) {
        if (!Number.isNaN(p.t)) {
          if (p.t < tMin) tMin = p.t;
          if (p.t > tMax) tMax = p.t;
        }
        if (p.v < yMin) yMin = p.v;
        if (p.v > yMax) yMax = p.v;
      }
    if (yMin === yMax) {
      yMin -= 1;
      yMax += 1;
    }
    const tSpan = tMax - tMin || 1;
    const ySpan = yMax - yMin || 1;
    return { series, tMin, tSpan, yMin, yMax, ySpan };
  }, [plottable]);

  if (results.length === 0) {
    return <div className="empty">No results yet.</div>;
  }

  // The chart is laid out in pixels, so its height, paddings, label size and
  // stroke are all multiplied by the zoom factor to grow in step with the rest
  // of the (em-sized) UI. Width still comes from the container's real width.
  const scale = fontSize / 16;
  const H = Math.round(240 * scale);
  const pad = {
    top: 12 * scale,
    right: 16 * scale,
    bottom: 26 * scale,
    left: 60 * scale,
  };
  const labelSize = 10 * scale;
  const innerW = Math.max(10, width - pad.left - pad.right);
  const innerH = H - pad.top - pad.bottom;

  const x = (t: number) =>
    chart ? pad.left + ((t - chart.tMin) / chart.tSpan) * innerW : 0;
  const y = (v: number) =>
    chart ? pad.top + (1 - (v - chart.yMin) / chart.ySpan) * innerH : 0;

  const yTicks = chart
    ? Array.from({ length: 5 }, (_, i) => chart.yMin + (chart.ySpan * i) / 4)
    : [];
  const xTicks = chart
    ? Array.from({ length: 4 }, (_, i) => chart.tMin + (chart.tSpan * i) / 3)
    : [];

  return (
    <div className="results-view" ref={wrapRef}>
      <div className="headline-cards">
        {results.map((r, i) => {
          const tr = totalReturn(r.equityCurve);
          return (
            <div className="card" key={i}>
              <div className="card-name" title={r.portfolioName}>
                <span
                  className="card-swatch"
                  style={{ background: COLORS[i % COLORS.length] }}
                />
                {r.portfolioName}
              </div>
              <div
                className={`card-big ${tr != null && tr < 0 ? "neg" : "pos"}`}
              >
                {tr != null ? fmtPct(tr) : "—"}
              </div>
              <div className="card-sub">
                <span>Annual {fmtPct(r.annualReturn)}</span>
                <span>Max DD {r.maxDrawdown.toFixed(1)}%</span>
                <span>Sharpe {r.sharpeRatio.toFixed(2)}</span>
              </div>
            </div>
          );
        })}
      </div>

      {chart && (
        <svg
          className="equity-chart"
          width={width}
          height={H}
          style={{ fontSize: labelSize }}
        >
          {yTicks.map((v, i) => (
            <g key={i}>
              <line
                x1={pad.left}
                x2={width - pad.right}
                y1={y(v)}
                y2={y(v)}
                stroke="#2c2c2c"
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
          {xTicks.map((t, i) => (
            <text
              key={i}
              x={x(t)}
              y={H - 8 * scale}
              className="axis-label mid"
            >
              {new Date(t).toISOString().slice(0, 7)}
            </text>
          ))}
          {chart.series.map((s, i) => {
            const d = s.pts
              .filter((p) => !Number.isNaN(p.t))
              .map((p, j) => `${j === 0 ? "M" : "L"}${x(p.t)},${y(p.v)}`)
              .join(" ");
            return (
              <path
                key={i}
                d={d}
                fill="none"
                stroke={COLORS[i % COLORS.length]}
                strokeWidth={1.6 * scale}
              />
            );
          })}
        </svg>
      )}

      <table>
        <thead>
          <tr>
            <th>Portfolio</th>
            <th>Strategy</th>
            <th>Sharpe</th>
            <th>Sortino</th>
            <th>Max DD %</th>
            <th>Annual %</th>
            <th>Std Dev</th>
            <th>Avg Corr</th>
            <th>Coint Pairs</th>
          </tr>
        </thead>
        <tbody>
          {results.map((r, i) => (
            <tr key={i}>
              <td>{r.portfolioName}</td>
              <td>{r.strategy}</td>
              <td>{r.sharpeRatio.toFixed(2)}</td>
              <td>{r.sortinoRatio.toFixed(2)}</td>
              <td>{r.maxDrawdown.toFixed(2)}</td>
              <td>{r.annualReturn.toFixed(2)}</td>
              <td>{r.standardDev.toFixed(4)}</td>
              <td>{r.avgCorrelation.toFixed(2)}</td>
              <td>{r.cointegratedPairs}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
