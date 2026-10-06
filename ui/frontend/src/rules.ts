// The rules card's model: a [portfolio.Rules] block as form state, and the
// pure helpers that turn a menu entry ("return_126") into controls and back.
//
// The menu itself comes from the engine (App.RulesMenu), so nothing here
// knows which entries exist. What lives here is only the grammar every entry
// shares: a base name, then its numbers in order, joined by underscores. The
// guards from 2026-09-27 hold by construction: a control can only produce a
// menu entry, every number is clamped to its entry's range, and Keep is never
// below Pick. The engine still checks the whole set (App.ReadRules) and its
// answer is what blocks Run -- these guards only stop the card from offering
// a state the engine would refuse for a reason the card could have known.

import type { backtest } from "../wailsjs/go/models";

// The strategy value a card carries when it runs rules instead of a script.
// Not a file name, so it cannot collide with one: library entries are bare
// "*.lua" names and paths carry a separator.
export const RULES_STRATEGY = ":rules";

export type RulesForm = {
  rebalance: string;
  rank: string; // "" = no ranking: hold everything that passes the filter
  pick: number;
  keep: number; // 0 = same as pick
  filter: string;
  weight: string; // "equal" | "inverse_vol_N" | "allocations"
  riskOff: string;
};

// What choosing "Describe my own idea…" starts from: rebalance monthly and
// hold everything equally -- a real strategy the sentence can read back, so
// the card is never blank, and the smallest one the user can edit into theirs.
export const DEFAULT_RULES: RulesForm = {
  rebalance: "monthly",
  rank: "",
  pick: 0,
  keep: 0,
  filter: "",
  weight: "equal",
  riskOff: "",
};

export type MenuEntry = backtest.RuleEntry;

// An entry's numbers. Go sends an entry that takes none as `args: null`, not
// an empty list, so every reader goes through this.
export function argsOf(e: MenuEntry): backtest.RuleArg[] {
  return e.args ?? [];
}

export function entriesOf(menu: MenuEntry[], kind: string): MenuEntry[] {
  return menu.filter((e) => e.kind === kind);
}

// The entry a written rule names, with its numbers, or null when it names
// nothing on the menu (a typo, or an entry this engine dropped).
export function parseRule(
  menu: MenuEntry[],
  kind: string,
  written: string,
): { entry: MenuEntry; args: number[] } | null {
  const s = written.trim();
  if (!s) return null;
  for (const e of entriesOf(menu, kind)) {
    const n = argsOf(e).length;
    if (n === 0) {
      if (s === e.base) return { entry: e, args: [] };
      continue;
    }
    if (!s.startsWith(e.base + "_")) continue;
    const parts = s.slice(e.base.length + 1).split("_");
    if (parts.length !== n || !parts.every((p) => /^\d+$/.test(p))) continue;
    return { entry: e, args: parts.map(Number) };
  }
  return null;
}

export function clampArg(arg: backtest.RuleArg, v: number): number {
  if (!Number.isFinite(v)) return arg.default;
  return Math.min(arg.max, Math.max(arg.min, Math.round(v)));
}

// Writes an entry with its numbers, each clamped to its range; a missing
// number takes the entry's default.
export function formatRule(entry: MenuEntry, args: number[] = []): string {
  const args0 = argsOf(entry);
  if (args0.length === 0) return entry.base;
  const nums = args0.map((a, i) => clampArg(a, args[i] ?? a.default));
  return [entry.base, ...nums].join("_");
}

// A first choice of an entry fills in its defaults.
export function defaultRule(entry: MenuEntry): string {
  return formatRule(entry, []);
}

// Pick and Keep as the engine accepts them: Pick in 1..tickers when there is
// a rank, Keep at least Pick (0 meaning "same as Pick"), and both 0 without a
// rank, which the engine requires.
export function settleCounts(r: RulesForm, tickerCount: number): RulesForm {
  if (!r.rank) return { ...r, pick: 0, keep: 0 };
  const most = Math.max(1, tickerCount);
  const pick = Math.min(most, Math.max(1, Math.round(r.pick || 1)));
  let keep = Math.round(r.keep || 0);
  if (keep !== 0) keep = Math.min(Math.max(keep, pick), Math.max(most, pick));
  return { ...r, pick, keep };
}

// The form's rules as the engine's RulesConfig. Allocations ride along only
// for Weight = "allocations", taken from the card's own percentages, so the
// card has one place to type weights rather than two.
export function toRulesConfig(
  r: RulesForm,
  allocations: Record<string, number>,
  tickers: string[],
): backtest.RulesConfig {
  const alloc: Record<string, number> = {};
  if (r.weight === "allocations") {
    for (const t of tickers) alloc[t] = allocations[t] ?? 0;
  }
  return {
    rebalance: r.rebalance,
    rank: r.rank,
    pick: r.rank ? r.pick : 0,
    keep: r.rank ? r.keep : 0,
    filter: r.filter,
    weight: r.weight || "equal",
    allocations: alloc,
    riskOff: r.riskOff,
  };
}

// And back: rules the assistant wrote, as form state.
export function fromRulesConfig(rc: backtest.RulesConfig): RulesForm {
  return {
    rebalance: rc.rebalance ?? "",
    rank: rc.rank ?? "",
    pick: rc.pick ?? 0,
    keep: rc.keep ?? 0,
    filter: rc.filter ?? "",
    weight: rc.weight || "equal",
    riskOff: rc.riskOff ?? "",
  };
}

// A cache key for one reading: everything ReadRules is asked.
export function readingKey(
  rc: backtest.RulesConfig,
  tickers: string[],
  start: string,
  end: string,
  benchmark: string,
): string {
  return JSON.stringify([rc, tickers, start, end, benchmark]);
}

// Session restore: a stored rules object must have every field, typed.
export function validRules(v: unknown): v is RulesForm {
  if (typeof v !== "object" || v === null) return false;
  const o = v as Record<string, unknown>;
  const str = (k: string) => typeof o[k] === "string";
  const num = (k: string) => typeof o[k] === "number" && Number.isFinite(o[k] as number);
  return (
    str("rebalance") && str("rank") && num("pick") && num("keep") &&
    str("filter") && str("weight") && str("riskOff")
  );
}

// Does a TOML snippet carry a rules block? Mirrors ui/rules_form.go's
// HasRulesBlock, so the chat offers "Use these rules" only where
// App.RulesFromToml will find some.
export function hasRulesBlock(toml: string): boolean {
  return /^\s*\[\s*portfolio\.rules\s*\]\s*$/im.test(toml);
}

// Why the rules stop a run, or "". readings is index-aligned with forms:
// undefined for a card that does not run rules, null while the engine is
// still being asked. The engine's refusal is quoted, not reworded.
export function rulesBlocker(
  forms: { name: string }[],
  readings: (RulesReadingLike | null | undefined)[],
): string {
  for (const [i, r] of readings.entries()) {
    if (r === undefined) continue;
    const who = forms.length > 1 ? `${forms[i]?.name.trim() || `Portfolio ${i + 1}`}: ` : "";
    if (r === null) return `${who}the rules are still being checked`;
    if (r.error) return `${who}fix the rules — ${r.error.replace(/\.$/, "")}`;
  }
  return "";
}

// The Run button carries the sentence's promise when there is one portfolio
// running rules: confirming the sentence IS pressing the button. The words are
// the engine's headline, a verbatim piece of the sentence above it.
export function runLabel(readings: (RulesReadingLike | null | undefined)[]): string {
  if (readings.length === 1 && readings[0]?.headline) {
    return `Run: ${readings[0].headline}`;
  }
  return "Run backtest";
}

type RulesReadingLike = { sentence: string; headline: string; error: string };
