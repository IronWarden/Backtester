// What the Backtest tab remembers between launches.
//
// The app persisted the font size and the assistant's width and nothing else,
// so every holding, date, allocation, benchmark, contribution schedule and
// pre-registered claim died with the process. A first session is exploratory
// by definition -- build something, run it, close the app to think -- and the
// work lost was exactly the work that took the longest to do.
//
// Two rules decide the shape of this file:
//
//  1. **A stored value is input.** It outlives upgrades and shape changes, and
//     nothing about it is trustworthy. Everything is validated field by field
//     and the whole session is dropped if any part of it fails -- a
//     half-restored form (holdings without their allocations, a criterion
//     without its metric) is worse than an empty one, because the user cannot
//     see what is missing.
//  2. **Losing the memory must never cost the app.** Reads and writes go
//     through storage.ts, which never throws; this file adds no access of its
//     own. See the T89 note there.
//
// Deliberately NOT remembered: the results (a stale result beside a fresh form
// reads as today's numbers), and the database and config paths (they may have
// moved, and Go resolves them properly -- see src/apppath).
import { CASHFLOW_FREQUENCIES } from "./buildToml";
import type { CashflowFrequency, HypothesisForm, SimpleForm } from "./buildToml";
import { readStored, writeStored } from "./storage";

// Versioned: a shape change bumps the key, so an old session is simply not
// found rather than validated field by field against a shape it predates.
export const SESSION_KEY = "backtester-session-v1";

export type Session = {
    forms: SimpleForm[];
    hypothesis: HypothesisForm;
};

const isString = (v: unknown): v is string => typeof v === "string";
// Rejects NaN and Infinity, which JSON.parse produces from no input but
// Number() produces from plenty.
const isNumber = (v: unknown): v is number =>
    typeof v === "number" && Number.isFinite(v);
const isObject = (v: unknown): v is Record<string, unknown> =>
    typeof v === "object" && v !== null && !Array.isArray(v);

function validAllocations(v: unknown): v is Record<string, number> {
    if (!isObject(v)) return false;
    return Object.values(v).every(isNumber);
}

function validFrequency(v: unknown): v is CashflowFrequency {
    return CASHFLOW_FREQUENCIES.some((f) => f.value === v);
}

// Every field of SimpleForm, checked. A field added to the type without a
// check here would be restored as undefined and reach buildToml, so the
// round-trip smoke case pins the field list too.
function validForm(v: unknown): v is SimpleForm {
    if (!isObject(v)) return false;
    return (
        isString(v.name) &&
        isNumber(v.buyingPower) &&
        isString(v.startDate) &&
        isString(v.endDate) &&
        Array.isArray(v.tickers) &&
        v.tickers.every(isString) &&
        typeof v.evenSplit === "boolean" &&
        validAllocations(v.allocations) &&
        isString(v.benchmark) &&
        (v.cashflowDirection === "deposit" || v.cashflowDirection === "withdraw") &&
        isNumber(v.cashflowAmount) &&
        validFrequency(v.cashflowFrequency) &&
        isString(v.strategy)
    );
}

function validHypothesis(v: unknown): v is HypothesisForm {
    if (!isObject(v)) return false;
    return (
        isString(v.claim) &&
        isString(v.metric) &&
        isString(v.op) &&
        isString(v.value)
    );
}

// parseSession is the whole of the validation, kept pure so the smoke run can
// exercise it without a localStorage to write to.
export function parseSession(raw: string | null, fallback: Session): Session {
    if (!raw) return fallback;
    let parsed: unknown;
    try {
        parsed = JSON.parse(raw);
    } catch {
        return fallback;
    }
    if (!isObject(parsed)) return fallback;
    const { forms, hypothesis } = parsed;
    // An empty form list is not a valid session: it would restore a Backtest
    // tab with no card at all, which no code path can produce and no button
    // recovers from.
    if (!Array.isArray(forms) || forms.length === 0 || !forms.every(validForm)) {
        return fallback;
    }
    if (!validHypothesis(hypothesis)) return fallback;
    return { forms, hypothesis };
}

export function readSession(fallback: Session): Session {
    return parseSession(readStored(SESSION_KEY), fallback);
}

// Called on every edit, so it is a stringify and a setItem -- both cheap
// against a handful of portfolios, and both already guarded against a storage
// that refuses to cooperate.
export function writeSession(session: Session): void {
    try {
        writeStored(SESSION_KEY, JSON.stringify(session));
    } catch {
        // JSON.stringify throws only on a cycle, which this shape cannot hold;
        // caught anyway because failing to remember must never reach the user.
    }
}
