import { createRoot } from "react-dom/client";
import "./src/style.css";
import "./src/App.css";
import ResultsView from "./src/ResultsView";
const base = {
  strategy: "lua:value.lua", sharpeRatio: 1.42, sortinoRatio: 1.98,
  maxDrawdown: -18.3, annualReturn: 16.4, standardDev: 0.115,
  avgCorrelation: 0.42, cointegratedPairs: 2, turnover: 3.1, avgExposure: 87,
  calmarRatio: 0.9, longestUnderwaterDays: 141, timeInMarket: 91, alpha: 3.1,
  beta: 0.88, trackingError: 0.06, informationRatio: 0.52, upCapture: 95,
  downCapture: 79, initialCapital: 100000, finalValue: 112318,
  equityCurve: [], dates: [], drawdowns: [], rollingSharpe: [],
  yearlyReturns: [], monthlyReturns: [], benchmarkCurve: [], benchmarkStats: {},
  splits: [], trials: 1, expectedMaxSharpe: 0.71, deflatedSharpe: 0.91,
  baseline: {}, significance: {}, tradeStats: { trades: 0, perTicker: [] },
};
// One run that declared Fundamentals (low coverage — the trap), one that did not.
const a: any = { ...base, portfolioName: "Value screen 2010-2025", fundamentalCoverage: 31.4 };
const b: any = { ...base, portfolioName: "Momentum (no fundamentals)", fundamentalCoverage: 0 };
createRoot(document.getElementById("root")!).render(
  <div style={{ padding: 16 }}><ResultsView results={[a, b] as any} fontSize={13} /></div>
);
