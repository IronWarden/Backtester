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
  const heading = form.strategy
    ? "# Generated from simple mode. Switch to Advanced to edit this config"
    : "# Generated from simple buy-and-hold mode. Switch to Advanced to edit";
  const heading2 = form.strategy
    ? "# directly, tune the strategy's parameters, or add more blocks."
    : "# this config directly or add more [[Portfolio]] blocks.";
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
