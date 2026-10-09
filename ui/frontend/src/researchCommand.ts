// The Research tab's grammar and filing helpers (T142a). Pure, so the smoke
// render pins them without a backend.
//
// The command line reads like a terminal's: a ticker, then a function.
//   AAPL                   -> CN (the company's filings)
//   apple                  -> a name: resolved to the best-matching symbol
//   MY                     -> the watchlist's filings (T142d)
//   "going concern"        -> full-text search of every filing (T142e)
//   "buyback" AAPL         -> the same, within one company
// The function may come first ("CN AAPL"); case never matters.

// market: the function is not about one company -- the whole watchlist --
// so it needs no ticker.
export const RESEARCH_FUNCTIONS = [
  { code: "CN", label: "Filings", hint: "A company's SEC filings, read as sec.gov shows them", market: false },
  { code: "MY", label: "Watchlist", hint: "Filings across the companies you follow", market: true },
] as const;

// FTS is the full-text search: started by quotes, never by a button, so it
// is not in the list above.
export type ResearchFn = (typeof RESEARCH_FUNCTIONS)[number]["code"] | "FTS";

export type ResearchCommand = {
  // What to look up: a symbol, or words to resolve to one by name.
  query: string;
  fn: ResearchFn;
  // FTS only: the search as EDGAR takes it, quotes kept.
  text?: string;
};

const CODES = new Set<string>(RESEARCH_FUNCTIONS.map((f) => f.code));
const MARKET_CODES = new Set<string>(RESEARCH_FUNCTIONS.filter((f) => f.market).map((f) => f.code));

// Whether a function runs without a ticker: MY, and a search, which
// may or may not name one.
export function isMarketFunction(fn: ResearchFn): boolean {
  return MARKET_CODES.has(fn) || fn === "FTS";
}

export function parseResearchCommand(input: string): ResearchCommand | null {
  // Quotes make it a full-text search; what is left over, if anything, is
  // the company to search within. An unclosed quote is closed.
  const phrases = (input.match(/"[^"]*"?/g) ?? []).filter((q) => q.replace(/"/g, "").trim());
  if (phrases.length > 0) {
    const text = phrases.map((q) => (q.length > 1 && q.endsWith('"') ? q : `${q}"`)).join(" ");
    const rest = input.replace(/"[^"]*"?/g, " ").trim().split(/\s+/).filter(Boolean);
    return { query: rest.join(" "), fn: "FTS", text };
  }
  const words = input.trim().split(/\s+/).filter(Boolean);
  if (words.length === 0) return null;
  const isCode = (w: string | undefined) => !!w && CODES.has(w.toUpperCase());
  // A lone market function is the function -- it has nothing to look up. Any
  // other lone code is a symbol, so "MY CN" still reaches the ticker MY.
  if (words.length === 1 && MARKET_CODES.has(words[0].toUpperCase())) {
    return { query: "", fn: words[0].toUpperCase() as ResearchFn };
  }
  // The function is the last word ("AAPL CN") or the first ("CN AAPL");
  // anything else, including a code in the middle of a name, is the query.
  let fn: ResearchFn = "CN";
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

// The search's date ranges, as the first day they cover ("" = since 2001).
export const SEARCH_RANGES = [
  { id: "month", label: "Past 30 days", days: 30 },
  { id: "year", label: "Past year", days: 365 },
  { id: "all", label: "Since 2001", days: 0 },
] as const;

export type SearchRange = (typeof SEARCH_RANGES)[number]["id"];

export function searchSince(range: SearchRange, today: Date): string {
  const r = SEARCH_RANGES.find((x) => x.id === range);
  if (!r || r.days === 0) return "";
  const d = new Date(today.getTime() - r.days * 86400000);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}
