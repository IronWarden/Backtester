// Ranking for the ticker picker.
//
// Pure and separate from the component so it can be asserted on directly --
// the ordering IS the feature, and "does typing apple find AAPL" is not a
// question a render test can answer.

// Enough to scan at a glance, few enough that the list never becomes the page.
export const MAX_SUGGESTIONS = 8;

// Symbol beats name, and a prefix beats a substring. Someone typing "AA" wants
// AA and AAPL before Alcoa and before every company with "aa" buried in its
// name. Matching the name at all is the point: "apple" has to find AAPL for a
// user who does not know the symbol, which is exactly the user who could not
// use this form before.
const EXACT = 0;
const SYMBOL_PREFIX = 1;
const NAME_PREFIX = 2;
const SYMBOL_SUBSTRING = 3;
const NAME_SUBSTRING = 4;

export function rankTicker(
  ticker: string,
  name: string,
  query: string,
): number {
  if (ticker === query) return EXACT;
  if (ticker.startsWith(query)) return SYMBOL_PREFIX;
  if (name.startsWith(query)) return NAME_PREFIX;
  if (ticker.includes(query)) return SYMBOL_SUBSTRING;
  if (name.includes(query)) return NAME_SUBSTRING;
  return -1;
}

// searchTickers returns at most `limit` symbols from `pool`, best match first.
// An empty query is not a search -- it is the list before you have narrowed
// it -- so it returns the head of the pool in its existing (alphabetical)
// order rather than nothing.
export function searchTickers(
  pool: string[],
  names: Record<string, string>,
  query: string,
  limit: number = MAX_SUGGESTIONS,
): string[] {
  const q = query.trim().toUpperCase();
  if (!q) return pool.slice(0, limit);

  const scored: { ticker: string; rank: number }[] = [];
  for (const t of pool) {
    const rank = rankTicker(t, (names[t] ?? "").toUpperCase(), q);
    if (rank >= 0) scored.push({ ticker: t, rank });
    // The pool is ten thousand symbols and this runs on every keystroke.
    // Once there are far more candidates than can be shown, the tail cannot
    // change the top of the list by more than ties already do.
    if (scored.length > limit * 20) break;
  }
  scored.sort((a, b) => a.rank - b.rank || (a.ticker < b.ticker ? -1 : 1));
  return scored.slice(0, limit).map((s) => s.ticker);
}
