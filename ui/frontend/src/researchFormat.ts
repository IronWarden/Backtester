// How the Research tab writes dates, times and moves (T144). Pure, so the
// smoke render pins every format without a backend.
//
// The bindings send dates as "2026-07-30" and times as "2026-07-30 16:30"
// (US Eastern, already). They are read by their parts, never through
// Date.parse, which would shift them into the viewer's own time zone.

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

function parts(s: string): { y: number; m: number; d: number; hh: number; mm: number } | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}))?/.exec(s.trim());
  if (!m) return null;
  return { y: +m[1], m: +m[2], d: +m[3], hh: m[4] ? +m[4] : -1, mm: m[5] ? +m[5] : 0 };
}

// "2026-07-30" or "2026-07-30 16:30" -> "Jul 30, 2026". Anything else is
// returned as it came.
export function fmtDate(s: string): string {
  const p = parts(s);
  return p ? `${MONTHS[p.m - 1]} ${p.d}, ${p.y}` : s;
}

// "2026-07-30" -> "Jul 30": for ranges inside one table.
export function fmtShortDate(s: string): string {
  const p = parts(s);
  return p ? `${MONTHS[p.m - 1]} ${p.d}` : s;
}

// "2026-07-30 16:30" -> "4:30 pm"; "" when the string carries no time.
export function fmtTime(s: string): string {
  const p = parts(s);
  if (!p || p.hh < 0) return "";
  const h12 = p.hh % 12 === 0 ? 12 : p.hh % 12;
  return `${h12}:${String(p.mm).padStart(2, "0")} ${p.hh < 12 ? "am" : "pm"}`;
}

// The day a date string names, as a day number for subtraction.
function dayNumber(y: number, m: number, d: number): number {
  return Math.round(Date.UTC(y, m - 1, d) / 86400000);
}

// How far a date is from today: "today", "tomorrow", "in 22 days",
// "yesterday", "22 days ago". today is the viewer's calendar date.
export function countdown(s: string, today: Date): string {
  const p = parts(s);
  if (!p) return "";
  const n = dayNumber(p.y, p.m, p.d) - dayNumber(today.getFullYear(), today.getMonth() + 1, today.getDate());
  if (n === 0) return "today";
  if (n === 1) return "tomorrow";
  if (n === -1) return "yesterday";
  return n > 0 ? `in ${n} days` : `${-n} days ago`;
}

// ERN's timing ("after close", "before open", "during the session") as a
// tag: its label and the class that colours it.
export function timingTag(timing: string): { label: string; cls: string } {
  switch (timing) {
    case "after close":
      return { label: "After close", cls: "tag-after" };
    case "before open":
      return { label: "Before open", cls: "tag-before" };
    case "during the session":
      return { label: "During session", cls: "tag-during" };
    default:
      return { label: timing, cls: "" };
  }
}

// A move bar's width, as a percentage of the largest move in view.
export function barWidth(move: number, largest: number): number {
  if (largest <= 0) return 0;
  return Math.min(100, Math.round((Math.abs(move) / largest) * 100));
}

// The summary above an earnings table: how big the moves were, and which
// way.
export function moveSummary(moves: number[]): { avgAbs: number; up: number; down: number } | null {
  if (moves.length === 0) return null;
  const avgAbs = moves.reduce((s, m) => s + Math.abs(m), 0) / moves.length;
  return { avgAbs, up: moves.filter((m) => m > 0).length, down: moves.filter((m) => m < 0).length };
}
