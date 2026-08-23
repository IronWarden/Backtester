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

export function isValidDate(s: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const d = new Date(`${s}T00:00:00Z`);
  return !Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

// The window every selected ticker covers: the latest of their first dates to
// the earliest of their last dates. Null until at least one ticker is chosen
// and the ranges have loaded.
export function coverageOf(form: SimpleForm, ranges: Ranges): Coverage | null {
  if (form.tickers.length === 0 || Object.keys(ranges).length === 0) return null;
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
  if (!form.benchmark || Object.keys(ranges).length === 0) return "";
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
  if (form.tickers.length === 0) return false;
  // A library strategy sizes its own positions, so an all-zero allocation
  // table is not a reason to refuse the run.
  if (usesAllocations(form) && allocationTotal(form) <= 0) return false;
  return dateErrorOf(form, coverageOf(form, ranges)) === "";
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
