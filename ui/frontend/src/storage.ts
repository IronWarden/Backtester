// Every read and write of persisted UI state goes through here.
//
// Two things make raw `localStorage` a hazard in this app, and both have
// already cost it a blank window once (T89):
//
//  1. **Access itself can throw.** A browser told to block site data throws a
//     SecurityError from the `localStorage` getter, not from `getItem`. Two of
//     these reads used to sit in `useState` initializers in the ROOT
//     component, so a throw there is not a lost preference -- it is React
//     failing to mount and an empty page.
//  2. **The stored value is input.** It survives reloads, upgrades and schema
//     changes, and nobody thinks of it as input. `JSON.parse("null")` is
//     `null`, and `"3"` parses to a number; code that then indexes the result
//     throws somewhere the try block is not.
//
// So: reading never throws and never returns something of the wrong shape;
// writing never throws. Losing a remembered panel or zoom level is the correct
// failure. Losing the app is not.

export function readStored(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

export function writeStored(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch {
    // Storage refused (blocked, full, private mode). The setting is not
    // remembered across launches, which is a strictly better outcome than the
    // UI refusing to work in this one.
  }
}

// A stored JSON object, or {} for anything else. Guards the shape, not just
// the parse: `"null"` is a truthy string that parses to null, and an array
// parses to an object that indexes wrong.
export function readStoredObject<T>(key: string): Record<string, T> {
  const raw = readStored(key);
  if (!raw) return {};
  try {
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      return {};
    }
    return parsed as Record<string, T>;
  } catch {
    return {};
  }
}

// A stored number, or `fallback` unless it is finite and passes `valid`.
// `Number(null)` is 0 and `Number("")` is 0, so a missing key must be rejected
// before the numeric check rather than by it.
export function readStoredNumber(
  key: string,
  fallback: number,
  valid: (n: number) => boolean,
): number {
  const raw = readStored(key);
  if (raw === null || raw.trim() === "") return fallback;
  const n = Number(raw);
  return Number.isFinite(n) && valid(n) ? n : fallback;
}
