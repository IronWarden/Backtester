// buildToml turns the simple-mode form into a TOML config string the
// existing runner already understands. With no strategy chosen it stays
// buy-and-hold: equal weights use the built-in strategy, custom weights
// reference the weighted Lua script. A library pick emits a lua: spec.

// Script paths are written the way a config writes them. The engine
// resolves them by walking up from the working directory, so the same
// spec works from the repo root (CLI) and from ui/ (the desktop app).
export const STRATEGY_DIR = "strategies";
export const WEIGHTED_LUA_PATH = `${STRATEGY_DIR}/buy_and_hold_weighted.lua`;

// The Lua tab's starter file. It is a copy of buy_and_hold.lua and lives in
// the library so there is one strategies directory, but offering it in the
// picker would list the same idea twice.
export const STARTER_SCRIPT = "example.lua";

// The benchmark choices simple mode offers, in plain language. Both are
// reconstructed total-return series that reach back to 1927, so neither
// constrains a realistic window at the start — only at the end, where they
// currently stop earlier than the priced tickers do.
export const BENCHMARKS: { value: string; label: string }[] = [
  { value: "$SP500", label: "S&P 500" },
  { value: "$USTOT", label: "Total US market" },
  { value: "", label: "Nothing — skip the comparison" },
];

export type SimpleForm = {
  name: string;
  buyingPower: number;
  startDate: string; // YYYY-MM-DD
  endDate: string; // YYYY-MM-DD
  tickers: string[];
  // evenSplit runs the built-in equal-weight strategy and ignores
  // allocations. Otherwise each ticker's percentage drives the weighted
  // Lua script.
  evenSplit: boolean;
  // Percentage of the portfolio allocated to each ticker (0–100). These are
  // relative, so the strategy normalizes by their sum — they need not add up
  // to exactly 100.
  allocations: Record<string, number>;
  // What the result is judged against. "$SP500" by default: the question
  // "did this beat just buying the market?" is the one a user actually has,
  // and leaving Benchmark unset makes it unanswerable. "" is no benchmark.
  benchmark: string;
  // The pre-registered question, or an empty claim for "do not judge this
  // run". Kept in the form so it survives a run and a mode switch, like
  // everything else here.
  hypothesis: HypothesisForm;
  // A script from the shipped library, as its bare file name ("rsi.lua").
  // EMPTY MEANS BUY-AND-HOLD — the original behaviour, and the reason an
  // untouched form still generates byte-identical TOML.
  strategy: string;
};

// Strategies that size their own positions read no weights, so the
// allocation percentages do not reach them. Only the buy-and-hold path
// (strategy === "") uses them.
export function usesAllocations(form: SimpleForm): boolean {
  return form.strategy === "";
}

// TOML floats must carry a decimal point, so 100000 -> "100000.0".
function toFloat(n: number): string {
  return Number.isInteger(n) ? `${n}.0` : String(n);
}

// Bare TOML keys allow [A-Za-z0-9_-]; quote anything else (e.g. a symbol
// that starts with a digit) to be safe.
function tomlKey(k: string): string {
  return /^[A-Za-z0-9_-]+$/.test(k) ? k : JSON.stringify(k);
}

export function buildToml(form: SimpleForm): string {
  const tickerList = form.tickers.map((t) => JSON.stringify(t)).join(", ");
  // The buy-and-hold wording is load-bearing: an untouched form has to
  // generate exactly the config it generated before the picker existed.
  // These name the Config tab, which T84 renamed from "Advanced". They are
  // TOML COMMENTS, so nothing parses them and no number moves — but a
  // generated file telling the user to click a button that no longer exists
  // is worse than the byte-identity this line previously had.
  const heading = form.strategy
    ? "# Generated from the Backtest form. Switch to Config to edit this"
    : "# Generated from the Backtest form. Switch to Config to edit this";
  const heading2 = form.strategy
    ? "# config directly, tune the strategy's parameters, or add more blocks."
    : "# config directly or add more [[Portfolio]] blocks.";
  const fallbackName = form.strategy
    ? form.strategy.replace(/\.lua$/, "")
    : "Buy and Hold";
  const lines: string[] = [
    heading,
    heading2,
    "",
    "[[Portfolio]]",
    `Name = ${JSON.stringify(form.name || fallbackName)}`,
    `BuyingPower = ${toFloat(form.buyingPower)}`,
    `StartDate = ${JSON.stringify(form.startDate)}`,
    `EndDate = ${JSON.stringify(form.endDate)}`,
    `Tickers = [${tickerList}]`,
  ];

  // Scored against, never traded: it stays out of Tickers so it receives no
  // capital and does not constrain the trading calendar.
  if (form.benchmark) {
    lines.push(`Benchmark = ${JSON.stringify(form.benchmark)}`);
  }

  // A library pick sizes itself and reads its own defaults, so no Params
  // block is emitted and the allocation percentages are not consulted.
  if (form.strategy) {
    lines.push(`Strategy = "lua:${STRATEGY_DIR}/${form.strategy}"`);
    return lines.join("\n") + "\n";
  }

  if (form.evenSplit) {
    lines.push('Strategy = "buyAndHold:equalWeights"');
  } else {
    const entries = form.tickers
      .map((t) => `${tomlKey(t)} = ${toFloat(form.allocations[t] ?? 0)}`)
      .join(", ");
    lines.push(`Strategy = "lua:${WEIGHTED_LUA_PATH}"`);
    lines.push(`Params = { weights = { ${entries} } }`);
  }

  return lines.join("\n") + "\n";
}

// ---------------------------------------------------------------------------
// Pre-registration
//
// The engine already parses a hypothesis from TOML and judges it mechanically;
// what stopped anyone using it was having to hand-write that TOML. These build
// the same document from a claim and a metric/operator/number triple, which is
// the whole grammar ParseCriterion accepts.

export type HypothesisForm = {
  // Plain English: what you think is true. Empty means no pre-registration,
  // and the run is simply not judged — which is different from judging it
  // against a default nobody chose.
  claim: string;
  metric: string;
  op: string;
  value: string;
};

export const CRITERION_OPS = [">=", ">", "<=", "<"];

// Friendly names for the engine's metric vocabulary. Any metric NOT listed
// here still works and shows under its own name — the list of valid metrics
// comes from Go (App.CriterionMetrics), so this map may only ever lag in
// prettiness, never in correctness.
export const CRITERION_LABELS: Record<string, string> = {
  sharpe: "Sharpe ratio",
  sortino: "Sortino ratio",
  calmar: "Calmar ratio",
  annual_return: "Annual return (%)",
  max_drawdown: "Max drawdown (%)",
  total_return: "Total profit ($)",
  turnover: "Turnover",
  exposure: "Average exposure (%)",
  time_in_market: "Time in market (%)",
  p_value: "p-value",
};

export function criterionLabel(metric: string): string {
  return CRITERION_LABELS[metric] ?? metric;
}

// A short, stable id derived from the question itself, so "a changed question
// is a new ID" is structural rather than a rule the user has to remember.
// Editing the claim or the criterion produces a different id on the next run;
// re-running the same question keeps it, which is what makes the research log
// able to group them.
function questionID(claim: string, criterion: string): string {
  let h = 5381;
  const s = `${claim.trim()}|${criterion}`;
  for (let i = 0; i < s.length; i++) {
    h = ((h << 5) + h + s.charCodeAt(i)) >>> 0;
  }
  // The WHOLE hash, not a prefix: base36 puts the fastest-changing digits
  // last, so truncating the front gave "sharpe >= 1.2" and "sharpe >= 1.3"
  // the same id — silently merging two different questions in the log.
  return `H-${h.toString(36).padStart(7, "0")}`;
}

// buildHypothesis renders the form as the TOML the engine registers, or ""
// when there is nothing to register. An incomplete form is "" rather than a
// partial document: half a hypothesis judged is worse than none.
export function buildHypothesis(h: HypothesisForm): string {
  const claim = h.claim.trim();
  const value = h.value.trim();
  if (!claim || !h.metric || !h.op || !value) return "";
  if (!Number.isFinite(Number(value))) return "";
  const criterion = `${h.metric} ${h.op} ${value}`;
  return [
    `ID          = ${JSON.stringify(questionID(claim, criterion))}`,
    `Claim       = ${JSON.stringify(claim)}`,
    `Criterion   = ${JSON.stringify(criterion)}`,
    "",
  ].join("\n");
}
