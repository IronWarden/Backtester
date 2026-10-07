// The Research tab's grammar and number formatting (T142a). Pure, so the
// smoke render pins both without a backend.
//
// The command line reads like a terminal's: a ticker, then a function.
//   AAPL         -> DES (what the company is)
//   AAPL FA      -> reported financials
//   aapl ern     -> earnings dates and the price's answer
//   apple fa     -> a name: resolved to the best-matching symbol
//   ECO          -> the economy: needs no company (T142c)
//   MY           -> the watchlist's filings and earnings (T142d)
// The function may come first ("FA AAPL"); case never matters.

// market: the function is not about one company -- the economy, or the
// whole watchlist -- so it needs no ticker.
export const RESEARCH_FUNCTIONS = [
  { code: "DES", label: "Description", hint: "What the company is", market: false },
  { code: "FA", label: "Financials", hint: "Reported figures, as first filed", market: false },
  { code: "ERN", label: "Earnings", hint: "Report dates and the price's answer", market: false },
  { code: "CN", label: "Filings", hint: "The company's own news: its SEC filings, read as clean text", market: false },
  { code: "ECO", label: "Economy", hint: "Official readings, release dates and FOMC meetings", market: true },
  { code: "MY", label: "Watchlist", hint: "Filings and earnings across the companies you follow", market: true },
] as const;

export type ResearchFn = (typeof RESEARCH_FUNCTIONS)[number]["code"];

export type ResearchCommand = {
  // What to look up: a symbol, or words to resolve to one by name.
  query: string;
  fn: ResearchFn;
};

const CODES = new Set<string>(RESEARCH_FUNCTIONS.map((f) => f.code));
const MARKET_CODES = new Set<string>(RESEARCH_FUNCTIONS.filter((f) => f.market).map((f) => f.code));

export function isMarketFunction(fn: ResearchFn): boolean {
  return MARKET_CODES.has(fn);
}

export function parseResearchCommand(input: string): ResearchCommand | null {
  const words = input.trim().split(/\s+/).filter(Boolean);
  if (words.length === 0) return null;
  const isCode = (w: string | undefined) => !!w && CODES.has(w.toUpperCase());
  // A lone market function is the function -- it has nothing to look up. Any
  // other lone code is a symbol, so "ECO DES" still reaches the ticker ECO.
  if (words.length === 1 && MARKET_CODES.has(words[0].toUpperCase())) {
    return { query: "", fn: words[0].toUpperCase() as ResearchFn };
  }
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

// CN's filters. The default leaves out insider paperwork (Forms 3/4/5/144),
// which for a large company is most of what it files and buries the events.
export const FILING_FILTERS = [
  { id: "news", label: "Events & reports", kinds: ["event", "report", "ownership", "proxy"] },
  { id: "event", label: "Events (8-K)", kinds: ["event"] },
  { id: "report", label: "Reports (10-K, 10-Q)", kinds: ["report"] },
  { id: "insider", label: "Insiders", kinds: ["insider"] },
  { id: "all", label: "All", kinds: [] as string[] },
] as const;

export type FilingFilter = (typeof FILING_FILTERS)[number]["id"];

export function filterFilings<T extends { kind: string }>(rows: T[], filter: FilingFilter): T[] {
  const f = FILING_FILTERS.find((x) => x.id === filter);
  if (!f || f.kinds.length === 0) return rows;
  return rows.filter((r) => (f.kinds as readonly string[]).includes(r.kind));
}

// Which document the reader opens first: for an 8-K the press release
// (EX-99.1) is the substance and the 8-K a cover sheet; otherwise the
// filing's own main document.
export function firstDocument<T extends { type: string }>(docs: T[], form: string): T | undefined {
  if (form.startsWith("8-K") || form.startsWith("6-K")) {
    const release = docs.find((d) => /^EX-99/.test(d.type));
    if (release) return release;
  }
  return docs.find((d) => d.type === form) ?? docs[0];
}

// ECO's numbers. kind is the reading's: a rate changes by points, an index by
// percent, a count (payrolls) by its own units. Units come from the series.
function trimmed(v: number, places: number): string {
  return String(Number(v.toFixed(places)));
}

function inUnits(units: string, abs: number): string {
  if (units.startsWith("billions of dollars")) return `$${scaled(abs * 1e9)}`;
  if (units.startsWith("thousands")) return scaled(abs * 1e3);
  return abs.toFixed(1);
}

export function formatEcoValue(kind: string, units: string, v: number): string {
  const sign = v < 0 ? "−" : "";
  if (kind === "rate") return `${sign}${trimmed(Math.abs(v), 2)}%`;
  return `${sign}${inUnits(units, Math.abs(v))}`;
}

export function formatEcoChange(kind: string, units: string, c: number): string {
  if (Math.abs(c) < 1e-12) return "unchanged";
  const sign = c > 0 ? "+" : "−";
  if (kind === "rate") return `${sign}${trimmed(Math.abs(c), 2)} pt`;
  if (kind === "index") return formatMove(c);
  return `${sign}${inUnits(units, Math.abs(c))}`;
}

// The holdings a watchlist could still take: the form's tickers, upper-cased,
// that are not on it yet, in the form's order.
export function holdingsToAdd(holdings: string[], watchlist: string[]): string[] {
  const have = new Set(watchlist);
  const out: string[] = [];
  for (const h of holdings) {
    const t = h.trim().toUpperCase();
    if (t && !have.has(t)) {
      have.add(t);
      out.push(t);
    }
  }
  return out;
}
