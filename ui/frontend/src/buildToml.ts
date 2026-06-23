// buildToml turns the simple buy-and-hold form into a TOML config string
// the existing runner already understands. Equal-weight runs use the
// built-in strategy; custom weights reference the weighted Lua script.

// Path is relative to the app's working directory (ui/), matching the
// existing strategies/example.lua convention.
export const WEIGHTED_LUA_PATH = "strategies/buy_and_hold_weighted.lua";

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
};

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
  const lines: string[] = [
    "# Generated from simple buy-and-hold mode. Switch to Advanced to edit",
    "# this config directly or add more [[Portfolio]] blocks.",
    "",
    "[[Portfolio]]",
    `Name = ${JSON.stringify(form.name || "Buy and Hold")}`,
    `BuyingPower = ${toFloat(form.buyingPower)}`,
    `StartDate = ${JSON.stringify(form.startDate)}`,
    `EndDate = ${JSON.stringify(form.endDate)}`,
    `Tickers = [${tickerList}]`,
  ];

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
