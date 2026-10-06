// buildToml turns the simple-mode form into a TOML config string the
// existing runner already understands. With no strategy chosen it stays
// buy-and-hold: buy_and_hold.lua, with a weights table when the split is not
// even. A library pick emits a lua: spec; a rules card emits [Portfolio.Rules].

import { RULES_STRATEGY, toRulesConfig, type RulesForm } from "./rules";

// Script paths are written the way a config writes them. The engine
// resolves them by walking up from the working directory, so the same
// spec works from the repo root (CLI) and from ui/ (the desktop app).
export const STRATEGY_DIR = "strategies";
// The script a form's strategy names. A bare file name means the shipped
// library ("rsi.lua"); a value carrying a separator is already a path.
export function strategySpec(strategy: string): string {
  return /[\\/]/.test(strategy) ? strategy : `${STRATEGY_DIR}/${strategy}`;
}

export const BUY_AND_HOLD_LUA_PATH = `${STRATEGY_DIR}/buy_and_hold.lua`;

// Where the Lua tab saves a script of your own. It lands in the library so
// there is one strategies directory, but it is a scratch file, not a strategy
// to offer in the picker.
export const STARTER_SCRIPT = "my_strategy.lua";

// The benchmark choices simple mode offers, in plain language. Both are
// reconstructed total-return series that reach back to 1927, so neither
// constrains a realistic window at the start — only at the end, where they
// currently stop earlier than the priced tickers do.
export type CashflowFrequency = "never" | "monthly" | "quarterly" | "annually";

// Labelled for the dropdown. "Never" is first because it is the default and
// doubles as the off switch, so no separate checkbox is needed.
export const CASHFLOW_FREQUENCIES: { value: CashflowFrequency; label: string }[] = [
  { value: "never", label: "Never" },
  { value: "monthly", label: "Monthly" },
  { value: "quarterly", label: "Quarterly" },
  { value: "annually", label: "Annually" },
];

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
  // Recurring deposit or withdrawal. "never" is the default and emits no
  // TOML at all, which is what keeps a form that has not touched this
  // generating byte-identical output.
  cashflowDirection: "deposit" | "withdraw";
  // Always POSITIVE here; the direction above carries the sign. A currency
  // field that means the opposite thing when you type a minus is a trap, and
  // a mistyped sign turns a contribution plan into a decumulation one.
  cashflowAmount: number;
  cashflowFrequency: CashflowFrequency;
  // A script from the shipped library, as its bare file name ("rsi.lua").
  // EMPTY MEANS BUY-AND-HOLD — the original behaviour, and the reason an
  // untouched form still generates byte-identical TOML. RULES_STRATEGY means
  // the card runs `rules` below instead of a script.
  strategy: string;
  // The rule set, read only when strategy is RULES_STRATEGY. Optional so a
  // session saved before rules existed still restores, and so every form
  // literal that never heard of rules stays valid.
  rules?: RulesForm;
};

// rebalance.lua's whole job is holding target weights, so it takes the
// allocation percentages as its `weights` param, the same table buy-and-hold
// reads. The famous-investor recipes depend on it: an All Weather that drifts
// for twenty years ends up a stock portfolio.
export const REBALANCE_LUA = "rebalance.lua";

// Strategies that size their own positions read no weights, so the
// allocation percentages do not reach them. Only buy-and-hold
// (strategy === "") and rebalance.lua use them.
export function usesAllocations(form: SimpleForm): boolean {
  if (form.strategy === RULES_STRATEGY) return form.rules?.weight === "allocations";
  return form.strategy === "" || form.strategy === REBALANCE_LUA;
}

export function usesRules(form: SimpleForm): boolean {
  return form.strategy === RULES_STRATEGY && !!form.rules;
}

// The weights table for a non-even split, or nothing.
function weightsParam(form: SimpleForm): string[] {
  if (form.evenSplit) return [];
  const entries = form.tickers
    .map((t) => `${tomlKey(t)} = ${toFloat(form.allocations[t] ?? 0)}`)
    .join(", ");
  return [`Params = { weights = { ${entries} } }`];
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

// One [[Portfolio]] block. Shared by the single- and multi-portfolio paths so
// they cannot drift: the whole point of the byte-identity guarantee is that
// adding the second portfolio did not change what the first one emits.
function portfolioBlock(form: SimpleForm): string[] {
  const tickerList = form.tickers.map((t) => JSON.stringify(t)).join(", ");
  const fallbackName = usesRules(form)
    ? "My rules"
    : form.strategy
      ? (form.strategy.split(/[\\/]/).pop() ?? form.strategy).replace(/\.lua$/, "")
      : "Buy and Hold";
  const lines: string[] = [
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

  // Emitted only when a schedule is actually set: "never" must produce no
  // block, so an untouched form generates exactly what it always did.
  const cashflow: string[] = [];
  if (form.cashflowFrequency && form.cashflowFrequency !== "never" && form.cashflowAmount > 0) {
    const signed =
      form.cashflowDirection === "withdraw" ? -form.cashflowAmount : form.cashflowAmount;
    cashflow.push(
      "",
      "  [Portfolio.Cashflow]",
      `  Amount = ${toFloat(signed)}`,
      `  Frequency = ${JSON.stringify(form.cashflowFrequency)}`,
    );
  }

  // A library pick sizes itself and reads its own defaults, so no Params
  // block is emitted and the allocation percentages are not consulted.
  //
  // A bare file name means the shipped library ("rsi.lua"). A value carrying a
  // separator is already a path and is emitted as written -- that is what lets
  // "Run this script" point a portfolio at the file open in the Lua tab,
  // wherever it lives, without the picker's names changing at all.
  // Rules replace Strategy outright (the engine refuses both), and come
  // after any cashflow table: sub-tables follow the portfolio's plain keys.
  if (usesRules(form)) {
    return lines.concat(cashflow, rulesBlock(form));
  }

  if (form.strategy) {
    lines.push(`Strategy = "lua:${strategySpec(form.strategy)}"`);
    if (form.strategy === REBALANCE_LUA) lines.push(...weightsParam(form));
    return lines.concat(cashflow);
  }

  lines.push(`Strategy = "lua:${BUY_AND_HOLD_LUA_PATH}"`);
  lines.push(...weightsParam(form));

  return lines.concat(cashflow);
}

// [Portfolio.Rules], with only the keys that are set: an unset key is the
// engine's default, and writing it out would only be noise in the Config tab.
function rulesBlock(form: SimpleForm): string[] {
  const rc = toRulesConfig(form.rules!, form.allocations, form.tickers);
  const out = ["", "  [Portfolio.Rules]", `  Rebalance = ${JSON.stringify(rc.rebalance)}`];
  if (rc.rank) {
    out.push(`  Rank = ${JSON.stringify(rc.rank)}`, `  Pick = ${rc.pick}`);
    if (rc.keep > rc.pick) out.push(`  Keep = ${rc.keep}`);
  }
  if (rc.filter) out.push(`  Filter = ${JSON.stringify(rc.filter)}`);
  if (rc.weight !== "equal") out.push(`  Weight = ${JSON.stringify(rc.weight)}`);
  if (rc.riskOff) out.push(`  RiskOff = ${JSON.stringify(rc.riskOff)}`);
  if (rc.weight === "allocations") {
    out.push("", "  [Portfolio.Rules.Allocations]");
    for (const t of form.tickers) out.push(`  ${tomlKey(t)} = ${toFloat(rc.allocations[t] ?? 0)}`);
  }
  return out;
}

// The whole config: a header comment, then one block per portfolio.
//
// A single-portfolio list must produce EXACTLY what this produced before the
// tab could hold more than one -- the generated TOML is the input to every run
// started from the form, so a stray blank line is a diff nobody asked for. The
// smoke render pins both shapes against goldens.
export function buildToml(forms: SimpleForm[]): string {
  // These name the Config tab, which T84 renamed from "Advanced". They are
  // TOML COMMENTS, so nothing parses them and no number moves.
  const anyStrategy = forms.some((f) => f.strategy);
  const heading = "# Generated from the Backtest form. Switch to Config to edit this";
  const heading2 = anyStrategy
    ? "# config directly, tune the strategy's parameters, or add more blocks."
    : "# config directly or add more [[Portfolio]] blocks.";

  const lines: string[] = [heading, heading2];
  for (const form of forms) {
    lines.push("", ...portfolioBlock(form));
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
