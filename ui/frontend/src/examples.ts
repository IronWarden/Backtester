// Starter examples for the Backtest tab.
//
// A beginner does not arrive short of compute. They arrive short of an IDEA,
// in front of a form asking for holdings, two dates, a strategy, an allocation
// and a success criterion -- five decisions they have no basis to make before
// anything has happened. One click here fills all five with a run that works,
// so their first act is EDITING something rather than assembling it.
//
// Pure on purpose: everything below is a function of the example and the
// database's own coverage table, so the smoke render can assert that every
// example produces a runnable portfolio without mounting anything.
import type { SimpleForm, HypothesisForm } from "./buildToml";
import type { Ranges } from "./portfolioCheck";
import { coverageOf } from "./portfolioCheck";

// Equal percentages that sum to exactly 100, matching what PortfolioCard fills
// in when even split is switched on. The app's rule is that even split carries
// REAL percentages -- an empty table means "no weights", not "equal weights",
// and portfolioRunnable refuses it -- so an example that skipped this would
// build a portfolio the form itself considers broken.
function evenAllocations(syms: string[]): Record<string, number> {
  if (syms.length === 0) return {};
  const base = Math.floor(100 / syms.length);
  const out: Record<string, number> = {};
  for (const s of syms) out[s] = base;
  let left = 100 - base * syms.length;
  for (let i = 0; left > 0; i++, left--) out[syms[i]] += 1;
  return out;
}

export type Example = {
  id: string;
  // What the button says, and under it the QUESTION the run asks. The
  // question is the part that teaches: a strategy name means nothing to
  // someone who has never run one.
  label: string;
  question: string;
  // Preferred holdings, best first. Symbols the database does not have are
  // dropped, and the example is withdrawn if fewer than `minTickers` survive
  // -- a starter example that opens with a red date error is worse than none.
  tickers: string[];
  minTickers: number;
  // "" is buy-and-hold, matching SimpleForm's own convention. Anything else
  // is a file name in the shipped library.
  strategy: string;
  // How long a window to ask for. The dates themselves come from what the
  // data actually covers, never from today's calendar.
  years: number;
  // Filled into the hypothesis box, so the first run is also the first
  // pre-registered question. Metric names must be ones CriterionMetrics
  // serves; the smoke render checks that.
  hypothesis: { metric: string; op: string; value: string };
};

// Deliberately four, and deliberately in this order: the first is the
// comparison every other idea is measured against, so it doubles as the
// explanation of what a baseline IS.
export const EXAMPLES: Example[] = [
  {
    id: "hold-big-tech",
    label: "Hold the big five",
    question: "What would simply holding the five biggest tech names have done?",
    tickers: ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"],
    minTickers: 3,
    strategy: "",
    years: 10,
    hypothesis: {
      metric: "annual_return",
      op: ">=",
      value: "8",
      // 8%/yr is roughly the long-run market average, so this asks the only
      // question a first run should: did it beat doing nothing clever?
    },
  },
  {
    id: "momentum-rotation",
    label: "Chase the winners",
    question:
      "Does rotating into whatever has been rising beat holding all of them?",
    tickers: ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"],
    minTickers: 3,
    strategy: "momentum_rotation.lua",
    years: 10,
    hypothesis: { metric: "sharpe", op: ">=", value: "1" },
  },
  {
    id: "trend-following",
    label: "Step out of falls",
    question:
      "Does selling when the market drops below its average beat sitting through it?",
    tickers: ["SPY"],
    minTickers: 1,
    strategy: "trend_following.lua",
    years: 15,
    hypothesis: { metric: "max_drawdown", op: "<=", value: "30" },
  },
  {
    id: "dca",
    label: "Drip money in",
    question: "Is paying in a bit each month better than investing it all at once?",
    tickers: ["SPY"],
    minTickers: 1,
    strategy: "dca.lua",
    years: 10,
    hypothesis: { metric: "annual_return", op: ">=", value: "5" },
  },
];

// YYYY-MM-DD arithmetic on the string, since that is the form the ranges
// table, the form and the engine all speak.
function minusYears(date: string, years: number): string {
  const [y, rest] = [Number(date.slice(0, 4)), date.slice(4)];
  return `${String(y - years).padStart(4, "0")}${rest}`;
}

// The symbols this database actually has, in the example's preferred order.
export function availableTickers(ex: Example, ranges: Ranges): string[] {
  return ex.tickers.filter((t) => !!ranges[t]);
}

// A minimal base form, so buildExample can be called before the user has one.
// Everything it carries is overwritten except the capital and the cashflow
// fields, which an example deliberately does not set: a starter run should not
// quietly turn on a contribution schedule.
export const BLANK: SimpleForm = {
  name: "",
  buyingPower: 10000,
  startDate: "",
  endDate: "",
  tickers: [],
  evenSplit: true,
  allocations: {},
  benchmark: "",
  cashflowDirection: "deposit",
  cashflowAmount: 0,
  cashflowFrequency: "never",
  strategy: "",
};

// Turn an example into a portfolio the engine will accept, or null when this
// database cannot support it.
//
// The window is the part that has to be right. It is the intersection of what
// every kept holding covers AND what the benchmark covers -- the engine drops
// every benchmark-relative metric unless the benchmark spans the whole window,
// and an example that silently loses the comparison it promised is exactly the
// wrong first impression. Dates come from the data, never from today.
export function buildExample(
  ex: Example,
  ranges: Ranges,
  benchmark: string,
  base: SimpleForm,
): SimpleForm | null {
  const tickers = availableTickers(ex, ranges);
  if (tickers.length < ex.minTickers) return null;

  const probe: SimpleForm = { ...base, tickers, allocations: {} };
  const coverage = coverageOf(probe, ranges);
  if (!coverage || !coverage.start || !coverage.end) return null;

  let start = coverage.start;
  let end = coverage.end;
  // The benchmark is scored over the same days, so it narrows the window
  // rather than being narrowed by it.
  const bench = benchmark ? ranges[benchmark] : undefined;
  if (benchmark && !bench) return null;
  if (bench) {
    if (bench[0] > start) start = bench[0];
    if (bench[1] < end) end = bench[1];
  }
  if (start > end) return null;

  // Ask for the example's window, but never earlier than the data begins.
  const wanted = minusYears(end, ex.years);
  if (wanted > start) start = wanted;

  return {
    ...base,
    name: ex.label,
    tickers,
    // Equal weight: an example must not open by asking for percentages.
    evenSplit: true,
    allocations: evenAllocations(tickers),
    startDate: start,
    endDate: end,
    benchmark,
    strategy: ex.strategy,
  };
}

// The hypothesis that goes with an example. Separate from buildExample because
// the form keeps one hypothesis per RUN, not per portfolio.
export function exampleHypothesis(ex: Example): HypothesisForm {
  return {
    claim: ex.question,
    metric: ex.hypothesis.metric,
    op: ex.hypothesis.op,
    value: ex.hypothesis.value,
  };
}
