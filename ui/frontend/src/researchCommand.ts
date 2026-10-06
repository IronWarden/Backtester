// The Research tab's grammar and number formatting (T142a). Pure, so the
// smoke render pins both without a backend.
//
// The command line reads like a terminal's: a ticker, then a function.
//   AAPL         -> DES (what the company is)
//   AAPL FA      -> reported financials
//   aapl ern     -> earnings dates and the price's answer
//   apple fa     -> a name: resolved to the best-matching symbol
// The function may come first ("FA AAPL"); case never matters.

export const RESEARCH_FUNCTIONS = [
  { code: "DES", label: "Description", hint: "What the company is" },
  { code: "FA", label: "Financials", hint: "Reported figures, as first filed" },
  { code: "ERN", label: "Earnings", hint: "Report dates and the price's answer" },
] as const;

export type ResearchFn = (typeof RESEARCH_FUNCTIONS)[number]["code"];

export type ResearchCommand = {
  // What to look up: a symbol, or words to resolve to one by name.
  query: string;
  fn: ResearchFn;
};

const CODES = new Set<string>(RESEARCH_FUNCTIONS.map((f) => f.code));

export function parseResearchCommand(input: string): ResearchCommand | null {
  const words = input.trim().split(/\s+/).filter(Boolean);
  if (words.length === 0) return null;
  const isCode = (w: string | undefined) => !!w && CODES.has(w.toUpperCase());
  // The function is the last word ("AAPL FA") or the first ("FA AAPL");
  // anything else, including a code in the middle of a name, is the query.
  let fn: ResearchFn = "DES";
  if (words.length > 1 && isCode(words[words.length - 1])) {
    fn = words.pop()!.toUpperCase() as ResearchFn;
  } else if (words.length > 1 && isCode(words[0])) {
    fn = words.shift()!.toUpperCase() as ResearchFn;
  }
  return { query: words.join(" "), fn };
}

// The symbol a query names: itself when it is a listed symbol, otherwise the
// best match by symbol or company name (search is the picker's own ranking).
export function resolveQuery(
  query: string,
  pool: string[],
  search: (q: string) => string[],
): string {
  const up = query.trim().toUpperCase();
  if (pool.length === 0 || pool.includes(up)) return up;
  return search(query)[0] ?? up;
}

const PER_SHARE = new Set(["Diluted EPS", "Basic EPS"]);
const SHARE_COUNTS = new Set(["Diluted Average Shares", "Ordinary Shares Number"]);

function scaled(abs: number): string {
  if (abs >= 1e12) return `${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${(abs / 1e3).toFixed(1)}K`;
  return abs.toFixed(0);
}

// A reported figure as a reader wants it: dollars scaled to B/M, per-share
// figures to the cent, share counts without a currency. A minus sign is a
// real minus (U+2212), so a negative is never mistaken for a dash.
export function formatFigure(metric: string, value: number): string {
  const sign = value < 0 ? "−" : "";
  const abs = Math.abs(value);
  if (PER_SHARE.has(metric)) return `${sign}$${abs.toFixed(2)}`;
  if (SHARE_COUNTS.has(metric)) return `${sign}${scaled(abs)}`;
  return `${sign}$${scaled(abs)}`;
}

export function formatPrice(v: number): string {
  return v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function formatMove(v: number): string {
  const pct = (v * 100).toFixed(2);
  return v > 0 ? `+${pct}%` : v < 0 ? `−${pct.slice(1)}%` : "0.00%";
}
