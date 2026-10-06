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
import { REBALANCE_LUA } from "./buildToml";
import type { Ranges } from "./portfolioCheck";
import { coverageOf } from "./portfolioCheck";
import { RULES_STRATEGY, type RulesForm } from "./rules";

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
  // "" is buy-and-hold, matching SimpleForm's own convention. RULES_STRATEGY
  // runs `rules`. Anything else is a file name in the shipped library.
  strategy: string;
  // A rule set, when the example is one (T136): a beginner's first contact
  // should be a sentence they can read, not a script they cannot.
  rules?: RulesForm;
  // The library script these rules are the exact twin of. Its sentence (from
  // App.LibraryRules) is shown on the button, and the smoke render checks
  // `rules` against the engine's twin so the two cannot drift.
  twin?: string;
  // For an example that stays a script: why the rules menu cannot say it.
  scriptReason?: string;
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
    strategy: RULES_STRATEGY,
    rules: {
      rebalance: "monthly",
      rank: "return_126",
      pick: 2,
      keep: 0,
      filter: "rising_126",
      weight: "equal",
      riskOff: "",
    },
    twin: "momentum_rotation.lua",
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
    scriptReason:
      "A script: it buys and sells on the crossing itself, which the rules menu cannot state exactly.",
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
    scriptReason: "A script: it is driven by monthly deposits, which no rule describes.",
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

  const window = exampleWindow(tickers, ranges, benchmark, base);
  if (!window) return null;
  let { start } = window;
  const { end } = window;

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
    rules: ex.rules,
  };
}

// What every kept holding AND the benchmark cover, or null when they share no
// days. Shared by the examples and the investor portfolios, which differ only
// in where inside it they start.
function exampleWindow(
  tickers: string[],
  ranges: Ranges,
  benchmark: string,
  base: SimpleForm,
): { start: string; end: string } | null {
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
  return { start, end };
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

// ---------------------------------------------------------------------------
// Famous investors
//
// "What would Buffett's portfolio have done?" is the question a lot of people
// open a backtester to ask, and the naive answer is a trap: today's Berkshire
// holdings run backwards over ten years is a list of what WORKED, chosen with
// the ending already known. So every entry here is something a person could
// actually have bought on the window's first day:
//
//   - a published RECIPE (Buffett's 90/10, All Weather, the Permanent
//     Portfolio, Swensen) -- a fixed allocation, rebalanced, that does not
//     depend on knowing the future; and
//   - a 13F COPY, bought the day after the filing went public and held. The
//     holdings are the filing's, verbatim, not today's.
//
// The `caveat` is shown with each one, because the honest limits (a recipe
// tested before it was published, a holding the database cannot price) are
// exactly what a user comparing against a legend needs to know.

export type Investor = {
  id: string;
  // The person, and what the portfolio is called -- the latter becomes the
  // card's name, so a comparison run labels its line legibly.
  investor: string;
  name: string;
  // One line on what it is and where it comes from.
  note: string;
  caveat: string;
  question: string;
  // Relative weights, largest first. They are normalised over whatever this
  // database can price, so they need not sum to 100.
  holdings: Record<string, number>;
  // Withdrawn below this many priced holdings. For a recipe that is every
  // leg: a Permanent Portfolio without its gold is not one.
  minTickers: number;
  // "" is buy-and-hold at the weights; "rebalance.lua" holds them.
  strategy: string;
  // A 13F copy starts on a fixed day (the day after filing); a recipe asks
  // for `years` back from the end of the data, like the examples.
  startDate?: string;
  years?: number;
  hypothesis: { metric: string; op: string; value: string };
};

export const INVESTORS: Investor[] = [
  {
    id: "buffett-90-10",
    investor: "Warren Buffett",
    name: "Buffett 90/10",
    note: "90% S&P 500, 10% T-bills — his instructions for his own estate (2013 letter)",
    caveat:
      "Rebalanced quarterly. Buffett named the mix, not a rebalancing rule.",
    question:
      "Does Buffett's advice for his own heirs hold up over thirty years?",
    holdings: { $SP500: 90, $CASH: 10 },
    minTickers: 2,
    strategy: REBALANCE_LUA,
    years: 30,
    // The long-run market average; the 10% in bills should cost a little
    // return and buy a little less drawdown.
    hypothesis: { metric: "annual_return", op: ">=", value: "8" },
  },
  {
    id: "berkshire-13f-2015",
    investor: "Warren Buffett",
    name: "Berkshire 13F, Q4 2015",
    note: "Berkshire's largest stock holdings at 31 Dec 2015 (96% of the filing by value), bought the day after the filing (16 Feb 2016) and held",
    caveat:
      "Weights are the filing's dollar values. Precision Castparts (bought outright before the filing) and anything this database cannot price — USG, Visa on some databases — are left out and the rest scaled up. It holds the 2015 book; Berkshire kept trading.",
    question:
      "Would copying Berkshire's public filing have beaten simply holding the S&P 500?",
    // SEC EDGAR, accession 0000950123-16-015025, value in $M at 2015-12-31.
    holdings: {
      WFC: 26077, KHC: 23693, KO: 17184, IBM: 11152, AXP: 10545,
      PSX: 5030, PG: 4192, USB: 3630, WMT: 3444, DVA: 2688,
      MCO: 2475, GS: 1975, CHTR: 1883, DE: 1745, GM: 1701,
      T: 1603, VRSN: 1134, USG: 947, BK: 829, SU: 774,
      V: 767, COST: 700, VZ: 693, MTB: 652, AXTA: 622,
    },
    minTickers: 15,
    strategy: "",
    startDate: "2016-02-17",
    // The S&P 500 returned about 15.7%/yr over 2016-02-17..2026-06-26 on the
    // bundled database, so this is "did it beat the index", pre-registered.
    hypothesis: { metric: "annual_return", op: ">=", value: "15" },
  },
  {
    id: "dalio-all-weather",
    investor: "Ray Dalio",
    name: "All Weather",
    note: "30% stocks, 40% long Treasuries, 15% intermediate, 7.5% gold, 7.5% commodities",
    caveat:
      "The simplified retail version Dalio gave Tony Robbins (2014), not Bridgewater's leveraged fund. Rebalanced quarterly; years before 2014 test the recipe, not anyone following it.",
    question: "Does balancing risk across economic seasons cut the drawdowns?",
    holdings: { VTI: 30, TLT: 40, IEF: 15, GLD: 7.5, DBC: 7.5 },
    minTickers: 5,
    strategy: REBALANCE_LUA,
    years: 20,
    hypothesis: { metric: "max_drawdown", op: "<=", value: "25" },
  },
  {
    id: "browne-permanent",
    investor: "Harry Browne",
    name: "Permanent Portfolio",
    note: "25% each in stocks, long Treasuries, gold and cash",
    caveat:
      "Browne rebalanced when a leg left a 15–35% band; quarterly rebalancing approximates it. Cash is $CASH, the T-bill series.",
    question: "Can four assets that never all fall together keep losses small?",
    holdings: { VTI: 25, TLT: 25, GLD: 25, $CASH: 25 },
    minTickers: 4,
    strategy: REBALANCE_LUA,
    years: 20,
    hypothesis: { metric: "max_drawdown", op: "<=", value: "20" },
  },
  {
    id: "swensen-yale",
    investor: "David Swensen",
    name: "Swensen portfolio",
    note: "Yale's CIO's plan for individuals: US, foreign and emerging stocks, REITs, TIPS, Treasuries",
    caveat:
      "From Unconventional Success (2005). Rebalanced quarterly; the funds are today's ETFs for each asset class.",
    question: "Does a spread across six asset classes earn its keep per unit of risk?",
    holdings: { VTI: 30, VEA: 15, VWO: 5, VNQ: 20, TIP: 15, IEF: 15 },
    minTickers: 6,
    strategy: REBALANCE_LUA,
    years: 15,
    hypothesis: { metric: "sharpe", op: ">=", value: "0.5" },
  },
];

// Relative weights as two-decimal percentages summing to exactly 100, by the
// largest remainder, so the allocation boxes show what the run will use.
export function percentAllocations(
  weights: Record<string, number>,
  syms: string[],
): Record<string, number> {
  const total = syms.reduce((s, t) => s + (weights[t] ?? 0), 0);
  if (total <= 0) return evenAllocations(syms);
  const exact = syms.map((t) => ((weights[t] ?? 0) / total) * 10000);
  const cents = exact.map(Math.floor);
  let left = 10000 - cents.reduce((s, c) => s + c, 0);
  const order = exact
    .map((x, i) => [x - Math.floor(x), i] as const)
    .sort((a, b) => b[0] - a[0]);
  for (let k = 0; left > 0; k++, left--) cents[order[k % order.length][1]] += 1;
  const out: Record<string, number> = {};
  syms.forEach((t, i) => (out[t] = cents[i] / 100));
  return out;
}

// The holdings this database can price, largest weight first.
export function investorTickers(inv: Investor, ranges: Ranges): string[] {
  return Object.keys(inv.holdings).filter((t) => !!ranges[t]);
}

// Turn an investor portfolio into a runnable form, or null when this database
// cannot support it. A 13F copy whose filing date is outside the data is
// withdrawn rather than moved: starting it any other day is a different
// question from the one it asks.
export function buildInvestor(
  inv: Investor,
  ranges: Ranges,
  benchmark: string,
  base: SimpleForm,
): SimpleForm | null {
  const tickers = investorTickers(inv, ranges);
  if (tickers.length < inv.minTickers) return null;

  const window = exampleWindow(tickers, ranges, benchmark, base);
  if (!window) return null;
  let { start } = window;
  const { end } = window;

  if (inv.startDate) {
    if (inv.startDate < start || inv.startDate > end) return null;
    start = inv.startDate;
  } else if (inv.years) {
    const wanted = minusYears(end, inv.years);
    if (wanted > start) start = wanted;
  }

  return {
    ...base,
    name: inv.name,
    tickers,
    evenSplit: false,
    allocations: percentAllocations(inv.holdings, tickers),
    startDate: start,
    endDate: end,
    benchmark,
    strategy: inv.strategy,
  };
}

export function investorHypothesis(inv: Investor): HypothesisForm {
  return {
    claim: inv.question,
    metric: inv.hypothesis.metric,
    op: inv.hypothesis.op,
    value: inv.hypothesis.value,
  };
}

// ---------------------------------------------------------------------------
// Starter baskets
//
// The picker searches by company name, which helps someone who knows they want
// Apple. It does nothing for someone who does not know what to hold, and that
// is the more common beginner. These are the same idea as the examples above,
// one level down: a holdings list you can accept and then edit.
//
// Deliberately NOT offered on a form with nothing in it anywhere -- the
// starter examples own that moment, and two answers to the same question is
// worse than one. They appear on a card that has been emptied, and on the
// second portfolio of a comparison, which is exactly where someone knows they
// want "something to compare against" and not which symbols that means.

export type Basket = {
  id: string;
  label: string;
  // Why you would hold this, in the fewest words that are still true.
  note: string;
  tickers: string[];
  // Below this many surviving symbols the basket is not offered: a "spread
  // across sectors" that resolves to two names is a lie.
  minTickers: number;
};

export const BASKETS: Basket[] = [
  {
    id: "big-tech",
    label: "Big tech",
    note: "Five names that led the last decade",
    tickers: ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"],
    minTickers: 3,
  },
  {
    id: "whole-market",
    label: "The whole market",
    note: "One fund, the broad US index",
    tickers: ["SPY"],
    minTickers: 1,
  },
  {
    id: "sectors",
    label: "Spread across sectors",
    note: "Tech, health, energy, banks, staples",
    tickers: ["XLK", "XLV", "XLE", "XLF", "XLP"],
    minTickers: 4,
  },
];

// The symbols this database has, in the basket's preferred order. Empty when
// too few survive to be worth offering.
export function basketTickers(b: Basket, ranges: Ranges): string[] {
  const have = b.tickers.filter((t) => !!ranges[t]);
  return have.length >= b.minTickers ? have : [];
}

// ---------------------------------------------------------------------------
// Hypothesis presets
//
// Pre-registering a claim is the best idea in the app and the one a beginner
// is least able to use: the box asks for a metric, an operator and a number,
// and `sharpe >= 1.2` means nothing on a first run. The honest consequence is
// that they leave it blank and lose the discipline entirely.
//
// These fill the existing controls rather than replacing them, so they teach
// the vocabulary instead of hiding it -- after clicking "Steady enough to
// hold" you can see that it meant sharpe >= 1, and change the number.

export type CriterionPreset = {
  id: string;
  label: string;
  // Used only when the claim box is still empty: a preset should complete the
  // registration, never overwrite something the user wrote in their own words.
  claim: string;
  metric: string;
  op: string;
  value: string;
};

export const CRITERION_PRESETS: CriterionPreset[] = [
  {
    id: "not-luck",
    label: "Better than a coin flip",
    claim: "this strategy's timing beats being invested at random",
    metric: "p_value",
    op: "<=",
    value: "0.05",
  },
  {
    id: "steady",
    label: "Steady enough to hold",
    claim: "this strategy earns enough per unit of risk to be worth holding",
    metric: "sharpe",
    op: ">=",
    value: "1",
  },
  {
    id: "shallow",
    label: "Never fell more than 30%",
    claim: "this strategy avoids a fall I could not sit through",
    // MaxDrawdown is reported as a POSITIVE percentage (metrics.go returns
    // the depth, not a signed return), so the bound is an upper one.
    metric: "max_drawdown",
    op: "<=",
    value: "30",
  },
  {
    id: "beats-market",
    label: "Made at least 8% a year",
    claim: "this strategy beats what the market has returned on average",
    metric: "annual_return",
    op: ">=",
    value: "8",
  },
];
