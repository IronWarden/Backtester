// How the Research tab writes dates (T144). Pure, so the
// smoke render pins every format without a backend.
//
// The bindings send dates as "2026-07-30" and times as "2026-07-30 16:30"
// (US Eastern, already). They are read by their parts, never through
// Date.parse, which would shift them into the viewer's own time zone.

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

function parts(s: string): { y: number; m: number; d: number } | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}))?/.exec(s.trim());
  if (!m) return null;
  return { y: +m[1], m: +m[2], d: +m[3] };
}

// "2026-07-30" or "2026-07-30 16:30" -> "Jul 30, 2026". Anything else is
// returned as it came.
export function fmtDate(s: string): string {
  const p = parts(s);
  return p ? `${MONTHS[p.m - 1]} ${p.d}, ${p.y}` : s;
}
