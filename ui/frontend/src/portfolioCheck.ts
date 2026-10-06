// Per-portfolio validation for the Backtest tab.
//
// Extracted from SimpleForm when the tab learned to hold more than one
// portfolio: the card renders these messages and the container needs the same
// verdicts to decide whether Run is allowed, and a rule that exists in two
// places is a rule that will disagree with itself. Pure, so the smoke render
// can assert on them directly.
//
// The engine enforces all of this server-side too. These exist so the user
// reads it while the dates are still in front of them, not after a run.
import type { SimpleForm } from "./buildToml";
import { usesAllocations } from "./buildToml";

export type Ranges = Record<string, string[]>;

export type Coverage = {
  start: string;
  end: string;
  startTicker: string;
  endTicker: string;
  // Selected symbols the database has never heard of.
  unknown: string[];
};

// Whether the ranges have loaded. Every card asks this several times per
// render, and on a real database the object holds ~10,000 symbols: both
// Object.keys(...).length and a for-in are O(n) there (V8 enumerates every key
// of a dictionary-mode object before yielding the first), and together they
// were over half the form's render time in a profile. The object is replaced,
// never mutated, when a database loads, so the answer is cached per object.
const loadedCache = new WeakMap<Ranges, boolean>();

export function hasRanges(ranges: Ranges): boolean {
  let loaded = loadedCache.get(ranges);
  if (loaded === undefined) {
    loaded = Object.keys(ranges).length > 0;
    loadedCache.set(ranges, loaded);
  }
  return loaded;
}

export function isValidDate(s: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const d = new Date(`${s}T00:00:00Z`);
  return !Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

// The window every selected ticker covers: the latest of their first dates to
// the earliest of their last dates. Null until at least one ticker is chosen
// and the ranges have loaded.
export function coverageOf(form: SimpleForm, ranges: Ranges): Coverage | null {
  if (form.tickers.length === 0 || !hasRanges(ranges)) return null;
  let start = "";
  let end = "";
  let startTicker = "";
  let endTicker = "";
  const unknown: string[] = [];
  for (const t of form.tickers) {
    const r = ranges[t];
    if (!r) {
      unknown.push(t);
      continue;
    }
    if (!start || r[0] > start) [start, startTicker] = [r[0], t];
    if (!end || r[1] < end) [end, endTicker] = [r[1], t];
  }
  return { start, end, startTicker, endTicker, unknown };
}

// A human-readable reason this portfolio cannot be backtested, or "".
export function dateErrorOf(form: SimpleForm, coverage: Coverage | null): string {
  if (form.startDate && !isValidDate(form.startDate))
    return "Start date must be a real date in YYYY-MM-DD form.";
  if (form.endDate && !isValidDate(form.endDate))
    return "End date must be a real date in YYYY-MM-DD form.";
  if (!coverage) return "";
  if (coverage.unknown.length > 0)
    return `Not in this database: ${coverage.unknown.join(", ")}. Remove it or pick a listed symbol.`;
  if (coverage.start > coverage.end)
    return `${coverage.startTicker} (data from ${coverage.start}) and ${coverage.endTicker} (until ${coverage.end}) have no overlapping dates.`;
  if (form.startDate < coverage.start)
    return `Data for ${coverage.startTicker} starts ${coverage.start} — move the start date on or after that.`;
  if (form.endDate > coverage.end)
    return `Data for ${coverage.endTicker} ends ${coverage.end} — move the end date on or before that.`;
  if (form.startDate > form.endDate) return "Start date is after the end date.";
  return "";
}

// The engine skips every benchmark-relative metric unless the benchmark has a
// bar for EVERY trading day in the window, and says so only in the run
// console. The benchmark series currently end earlier than the priced tickers,
// so the end date is the edge that bites.
export function benchmarkWarningOf(form: SimpleForm, ranges: Ranges): string {
  // Nothing to warn about before there is anything to run: an empty form
  // should not greet a new user with a date complaint.
  if (form.tickers.length === 0) return "";
  if (!form.benchmark || !hasRanges(ranges)) return "";
  const r = ranges[form.benchmark];
  if (!r) {
    return `${form.benchmark} is not in this database, so Alpha, Beta and the rest of the comparison will be empty.`;
  }
  if (form.startDate && form.startDate < r[0]) {
    return `${form.benchmark} starts ${r[0]} — the comparison needs a start date on or after that, or it is skipped entirely.`;
  }
  if (form.endDate && form.endDate > r[1]) {
    return `${form.benchmark} ends ${r[1]} — move the end date on or before that, or the comparison is skipped entirely.`;
  }
  return "";
}

export function allocationTotal(form: SimpleForm): number {
  return form.tickers.reduce((sum, t) => sum + (form.allocations[t] ?? 0), 0);
}

// Whether this one portfolio is in a runnable state. The container ANDs this
// across every portfolio: a run carries all of them, so one broken card has to
// stop the whole run rather than quietly contributing a bad block.
export function portfolioRunnable(form: SimpleForm, ranges: Ranges): boolean {
  return portfolioBlocker(form, ranges) === "";
}

// Why this portfolio cannot run, as the end of a sentence, or "" when it can.
// portfolioRunnable is defined from this, so the reason shown beside a
// disabled Run button can never disagree with the button.
export function portfolioBlocker(form: SimpleForm, ranges: Ranges): string {
  if (form.tickers.length === 0) return "add at least one holding";
  // A library strategy sizes its own positions, so an all-zero allocation
  // table is not a reason to refuse the run.
  if (usesAllocations(form) && allocationTotal(form) <= 0) {
    return "give at least one holding a percentage above 0";
  }
  // dateErrorOf speaks in sentences; this continues one ("To run, ..."), so
  // drop the full stop and lower-case a leading WORD -- never a ticker, which
  // is how "AAPL (data from ...)" begins.
  const dateErr = dateErrorOf(form, coverageOf(form, ranges)).replace(/\.$/, "");
  return /^[A-Z][a-z]/.test(dateErr) ? dateErr[0].toLowerCase() + dateErr.slice(1) : dateErr;
}

// Why the whole run cannot start, or "" when it can. The first reason only:
// fixing it may well fix the rest, and a list reads as a scolding. A reason
// names its portfolio when there is more than one card to look at.
export function runBlocker(
  forms: SimpleForm[],
  ranges: Ranges,
  dbPath: string,
  running: boolean,
): string {
  if (running) return "a run is already going";
  if (!dbPath) return "choose a database first (the database chip in the top bar)";
  if (forms.length === 0) return "add a portfolio";
  const dupes = duplicateNames(forms);
  if (dupes.length > 0) return `two portfolios are both called "${dupes[0]}" — rename one`;
  for (const [i, f] of forms.entries()) {
    const why = portfolioBlocker(f, ranges);
    if (!why) continue;
    if (forms.length === 1) return why;
    return `${f.name.trim() || `Portfolio ${i + 1}`}: ${why}`;
  }
  return "";
}

// Portfolio names reach the results table, the blotter and every verdict, so
// two blocks sharing one are two rows nobody can tell apart. Returns the
// duplicated names, in first-seen order.
export function duplicateNames(forms: SimpleForm[]): string[] {
  const seen = new Set<string>();
  const dupes: string[] = [];
  for (const f of forms) {
    const name = f.name.trim();
    if (!name) continue;
    if (seen.has(name) && !dupes.includes(name)) dupes.push(name);
    seen.add(name);
  }
  return dupes;
}

// ---------------------------------------------------------------------------
// Guards: stop a bad value getting INTO the form, rather than reporting it
// after the fact. Every control on the Backtest tab routes its input through
// one of these, so the error messages above become a backstop rather than the
// primary interface.

// The window a portfolio's dates may occupy: every holding's coverage, and
// the benchmark's too when the two overlap — a window past the benchmark's
// last bar silently costs every comparison metric. A benchmark that does not
// overlap the holdings at all is ignored here rather than allowed to produce
// an empty window; benchmarkWarningOf still says so.
export type Bounds = { min: string; max: string };

export function dateBounds(form: SimpleForm, ranges: Ranges): Bounds | null {
  const cov = coverageOf(form, ranges);
  if (!cov || !cov.start || !cov.end || cov.start > cov.end) return null;
  let { start: min, end: max } = cov;
  const b = form.benchmark ? ranges[form.benchmark] : undefined;
  if (b) {
    const bmin = b[0] > min ? b[0] : min;
    const bmax = b[1] < max ? b[1] : max;
    if (bmin <= bmax) [min, max] = [bmin, bmax];
  }
  return { min, max };
}

// Keystroke mask for a YYYY-MM-DD box: digits only, dashes inserted where
// they belong, never longer than a date. Typing "20150102" reads 2015-01-02.
export function maskDate(raw: string): string {
  const d = raw.replace(/[^0-9]/g, "").slice(0, 8);
  if (d.length <= 4) return d;
  if (d.length <= 6) return `${d.slice(0, 4)}-${d.slice(4)}`;
  return `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6)}`;
}

function clampTo(d: string, b: Bounds): string {
  return d < b.min ? b.min : d > b.max ? b.max : d;
}

// The dates this portfolio should hold, or null when they are already fine.
// A partial or impossible date falls back to the last good one, then to the
// window's edge; anything outside the window is pulled inside it; and a start
// after the end moves the side the user did NOT just edit, so what they typed
// is what they keep.
export function normalizeDates(
  form: SimpleForm,
  bounds: Bounds | null,
  lastGood: { start: string; end: string },
  edited: "start" | "end" | null,
): { startDate: string; endDate: string } | null {
  const pick = (v: string, prev: string, edge: string) =>
    isValidDate(v) ? v : isValidDate(prev) ? prev : edge;
  let s = pick(form.startDate, lastGood.start, bounds?.min ?? "");
  let e = pick(form.endDate, lastGood.end, bounds?.max ?? "");
  if (bounds) {
    s = clampTo(s, bounds);
    e = clampTo(e, bounds);
    if (s > e) {
      if (edited === "end") s = bounds.min;
      else e = bounds.max;
    }
  }
  if (s === form.startDate && e === form.endDate) return null;
  return { startDate: s, endDate: e };
}

// Whether adding a symbol keeps the portfolio runnable: it must be priced,
// and its history must overlap what is already held. Offering one that does
// not is offering an error.
//
// Returns a predicate rather than taking the symbol, because it runs over the
// whole universe (~10,000 symbols) on every form change: the coverage and the
// "have the ranges loaded" check are computed ONCE here, so each symbol costs a
// lookup and two string compares. Computing them per symbol made the filter
// quadratic -- 15 seconds of blocked UI per keystroke on a real database.
export function tickerFilter(form: SimpleForm, ranges: Ranges): (ticker: string) => boolean {
  if (!hasRanges(ranges)) return () => true;
  const cov = coverageOf(form, ranges);
  const held = new Set(form.tickers);
  return (ticker) => {
    if (held.has(ticker)) return false;
    const r = ranges[ticker];
    if (!r) return false;
    if (!cov || !cov.start) return true;
    const start = r[0] > cov.start ? r[0] : cov.start;
    const end = r[1] < cov.end ? r[1] : cov.end;
    return start <= end;
  };
}

// What Enter or Add should add: the highlighted suggestion, else an exact
// symbol match, else the best suggestion — never free text the database has
// not heard of.
export function resolveTicker(
  typed: string,
  highlight: number,
  suggestions: string[],
): string {
  if (highlight >= 0 && highlight < suggestions.length) return suggestions[highlight];
  const q = typed.trim().toUpperCase();
  if (!q) return "";
  return suggestions.includes(q) ? q : (suggestions[0] ?? "");
}

// A name no other portfolio has. Blank falls back to the given default; a
// clash gets the first free " (n)" suffix.
export function uniqueName(name: string, others: string[], fallback: string): string {
  const base = name.trim() || fallback;
  const taken = new Set(others.map((o) => o.trim()));
  if (!taken.has(base)) return base;
  let n = 2;
  while (taken.has(`${base} (${n})`)) n++;
  return `${base} (${n})`;
}

// Keystroke mask for a signed decimal: one leading minus, one point, digits.
export function maskDecimal(raw: string): string {
  const neg = raw.trim().startsWith("-");
  const [whole, ...rest] = raw.replace(/[^0-9.]/g, "").split(".");
  const num = rest.length > 0 ? `${whole}.${rest.join("")}` : whole;
  return neg ? `-${num}` : num;
}
