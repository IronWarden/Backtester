// The rules card (T135): a strategy as a sentence the user confirms, with
// each rule beneath it as a control.
//
// Presentational on purpose -- no hooks, no Go calls. The container asks the
// engine for the menu and for the reading (App.RulesMenu / App.ReadRules) and
// passes both in, which is what lets the smoke run render every state of this
// card without a backend, and what keeps the sentence the ENGINE's: nothing
// here composes English from the rules.

import type { main } from "../wailsjs/go/models";
import {
  argsOf,
  clampArg,
  defaultRule,
  entriesOf,
  formatRule,
  parseRule,
  settleCounts,
  type MenuEntry,
  type RulesForm,
} from "./rules";

type Props = {
  menu: MenuEntry[];
  rules: RulesForm;
  // null while the engine is still being asked.
  reading: main.RulesReading | null;
  tickerCount: number;
  hasBenchmark: boolean;
  onChange: (next: RulesForm) => void;
  // Opens the assistant to describe the idea in words instead.
  onAskAssistant?: () => void;
};

// A menu entry as a dropdown label: its phrase with the numbers named rather
// than filled ("rose most over the last N trading days").
function entryLabel(e: MenuEntry): string {
  const s = e.phrase.replace(/\{B\}/g, "the benchmark").replace(/\{(\w)\}/g, "$1");
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function NumberBox({
  label,
  value,
  min,
  max,
  title,
  onCommit,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  title?: string;
  onCommit: (v: number) => void;
}) {
  // Uncontrolled and committed on blur/Enter: clamping on every keystroke
  // would turn the "1" of "126" into the minimum before the rest is typed.
  // Keyed by value, so an outside change (or the clamp) resets the box.
  const commit = (raw: string) => {
    const n = Number(raw);
    onCommit(Number.isFinite(n) ? Math.min(max, Math.max(min, Math.round(n))) : value);
  };
  return (
    <label className="rules-num" title={title}>
      {label}
      <input
        key={value}
        type="number"
        min={min}
        max={max}
        defaultValue={value}
        onBlur={(e) => commit(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") commit((e.target as HTMLInputElement).value);
        }}
      />
    </label>
  );
}

// One rule: a dropdown of its kind's entries (plus an "off" choice when the
// rule is optional), and a box for each number the chosen entry takes.
function RuleControl({
  label,
  menu,
  kind,
  value,
  offLabel,
  disabled,
  onChange,
}: {
  label: string;
  menu: MenuEntry[];
  kind: string;
  value: string;
  offLabel?: string;
  disabled?: boolean;
  onChange: (next: string) => void;
}) {
  const entries = entriesOf(menu, kind);
  const parsed = parseRule(menu, kind, value);
  return (
    <div className="rules-row">
      <label className="strategy-pick">
        {label}
        <select
          value={parsed?.entry.base ?? ""}
          disabled={disabled}
          onChange={(e) => {
            const next = entries.find((x) => x.base === e.target.value);
            onChange(next ? defaultRule(next) : "");
          }}
        >
          {offLabel !== undefined && <option value="">{offLabel}</option>}
          {entries.map((e) => (
            <option key={e.base} value={e.base} title={e.summary}>
              {entryLabel(e)}
            </option>
          ))}
        </select>
      </label>
      {parsed &&
        !disabled &&
        argsOf(parsed.entry).map((arg, i) => (
          <NumberBox
            key={arg.name}
            label={arg.name}
            value={parsed.args[i]}
            min={arg.min}
            max={arg.max}
            title={`${arg.min}–${arg.max}`}
            onCommit={(v) => {
              const args = [...parsed.args];
              args[i] = clampArg(arg, v);
              onChange(formatRule(parsed.entry, args));
            }}
          />
        ))}
    </div>
  );
}

export default function RulesCard({
  menu,
  rules,
  reading,
  tickerCount,
  hasBenchmark,
  onChange,
  onAskAssistant,
}: Props) {
  const set = (p: Partial<RulesForm>) => onChange(settleCounts({ ...rules, ...p }, tickerCount));

  if (menu.length === 0) {
    return (
      <div className="strategy-card rules-card">
        <p className="hint">Loading the rules menu…</p>
      </div>
    );
  }

  return (
    <div className="strategy-card rules-card">
      {reading === null ? (
        <p className="rules-sentence pending">Reading the rules…</p>
      ) : reading.error ? (
        <div className="coverage-msg err rules-error">These rules cannot run: {reading.error}</div>
      ) : (
        <p className="rules-sentence">{reading.sentence}</p>
      )}

      <div className="rules-controls">
        <RuleControl
          label="How often to decide"
          menu={menu}
          kind="rebalance"
          value={rules.rebalance}
          onChange={(v) => set({ rebalance: v })}
        />
        <RuleControl
          label="Rank the holdings by"
          menu={menu}
          kind="rank"
          value={rules.rank}
          offLabel="No ranking — hold every one that qualifies"
          onChange={(v) => set({ rank: v })}
        />
        {rules.rank && (
          <div className="rules-row">
            <NumberBox
              label="Hold the top"
              value={rules.pick}
              min={1}
              max={Math.max(1, tickerCount)}
              onCommit={(v) => set({ pick: v })}
            />
            <NumberBox
              label="Keep while in the top"
              title="A holding is kept while it stays this high, so a name slipping one place is not sold and bought back. Never below the number held."
              value={rules.keep || rules.pick}
              min={rules.pick}
              max={Math.max(rules.pick, tickerCount)}
              onCommit={(v) => set({ keep: v === rules.pick ? 0 : v })}
            />
          </div>
        )}
        <RuleControl
          label="Only hold names"
          menu={menu}
          kind="filter"
          value={rules.filter}
          offLabel="Any of them"
          onChange={(v) => set({ filter: v })}
        />
        <RuleControl
          label="Split the money"
          menu={menu}
          kind="weight"
          value={rules.weight}
          onChange={(v) => set({ weight: v })}
        />
        <RuleControl
          label="Step aside to cash when"
          menu={menu}
          kind="riskoff"
          value={rules.riskOff}
          offLabel={hasBenchmark ? "Never" : "Never — pick a comparison above to enable"}
          // Left enabled while a risk-off is still set, so the engine's
          // "needs a Benchmark" refusal can be cleared from here.
          disabled={!hasBenchmark && !rules.riskOff}
          onChange={(v) => set({ riskOff: v })}
        />
      </div>

      {onAskAssistant && (
        <button type="button" className="link-button" onClick={onAskAssistant}>
          Describe your idea to the assistant instead →
        </button>
      )}
    </div>
  );
}
