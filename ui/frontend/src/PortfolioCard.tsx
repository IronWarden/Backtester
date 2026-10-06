import { useEffect, useMemo, useRef, useState } from "react";
import type { backtest, main } from "../wailsjs/go/models";
import {
    BENCHMARKS,
    CASHFLOW_FREQUENCIES,
    STRATEGY_DIR,
    usesAllocations,
    usesRules,
} from "./buildToml";
import RulesCard from "./RulesCard";
import {
    DEFAULT_RULES,
    RULES_STRATEGY,
    fromRulesConfig,
    settleCounts,
    type MenuEntry,
} from "./rules";
import type { SimpleForm as Form } from "./buildToml";
import { searchTickers } from "./tickerSearch";
import { BASKETS, INVESTORS, basketTickers, buildInvestor } from "./examples";
import {
    allocationTotal,
    benchmarkWarningOf,
    coverageOf,
    dateBounds,
    dateErrorOf,
    hasRanges,
    isValidDate,
    maskDate,
    maskDecimal,
    normalizeDates,
    resolveTicker,
    tickerFilter,
    uniqueName,
} from "./portfolioCheck";
import type { Ranges } from "./portfolioCheck";
import { ImportHoldings } from "../wailsjs/go/main/App";
import { applyImport, importSummary } from "./holdingsImport";
import type { HoldingsImport } from "./holdingsImport";

// One [[Portfolio]] block, as a card.
//
// This was the whole of SimpleForm until the Backtest tab learned to run more
// than one portfolio at a time -- which is the comparison the app exists for,
// and which previously meant hand-writing TOML. Everything per-portfolio lives
// here so each card carries its own ticker picker, its own coverage verdict
// and its own dates; everything shared (the symbol universe, the names, the
// gallery) is fetched ONCE by the container and passed down, because five
// identical database queries per card is not a feature.

// Split 100% across the tickers as whole percentages that sum to exactly 100
// (e.g. 3 tickers -> 34/33/33).
function evenAllocations(syms: string[]): Record<string, number> {
    if (syms.length === 0) return {};
    const base = Math.floor(100 / syms.length);
    const out: Record<string, number> = {};
    for (const s of syms) out[s] = base;
    let left = 100 - base * syms.length;
    for (let i = 0; left > 0; i++, left--) out[syms[i]] += 1;
    return out;
}

function clampPct(n: number): number {
    if (Number.isNaN(n)) return 0;
    return Math.min(100, Math.max(0, n));
}

function formatPct(n: number): string {
    return Number.isInteger(n) ? String(n) : n.toFixed(1);
}

function formatMoney(n: number): string {
    return n.toLocaleString("en-US");
}

function strategyLabel(file: string): string {
    if (!file) return "Buy and hold";
    if (file === RULES_STRATEGY) return "Your rules";
    return file
        .replace(/\.lua$/, "")
        .replace(/[_-]+/g, " ")
        .replace(/\b\w/g, (c) => c.toUpperCase());
}

function laterDate(a: string, b: string): string {
    return a > b ? a : b;
}

// Shift a YYYY-MM-DD date back by whole years, clamping Feb 29 to Feb 28.
function minusYears(date: string, years: number): string {
    const [y, m, d] = date.split("-").map(Number);
    const year = y - years;
    const day = m === 2 && d === 29 ? 28 : d;
    return `${String(year).padStart(4, "0")}-${String(m).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

// Starting capital restored when the box is left empty or at zero, and the
// longest dollar figure either money box accepts ($999 billion): past that,
// float64 cash arithmetic stops being exact to the cent.
const DEFAULT_CAPITAL = 10000;
const MONEY_DIGITS = 12;

type Props = {
    form: Form;
    onChange: (next: Form) => void;
    // Null for the only portfolio: a run with nothing in it is not a state the
    // form should be able to reach by clicking Remove.
    onRemove: (() => void) | null;
    // Every OTHER portfolio's name, so this one can refuse to collide.
    otherNames: string[];
    // What a blank name becomes: the card's position-based default.
    defaultName: string;
    dbPath: string;
    // Shared, fetched once by the container.
    available: string[];
    names: Record<string, string>;
    ranges: Ranges;
    pickable: backtest.StrategyCard[];
    // Whether to offer starter baskets in an empty holdings box. False while
    // the whole form is empty, because the starter examples own that moment --
    // two answers to the same question is worse than one.
    showBaskets: boolean;
    // Opens the chosen strategy's script in the Lua tab, read-only.
    onViewScript?: (strategy: string) => void;
    // The engine's rules menu, and its reading of THIS card's rules (null
    // while it is being asked). Only consulted when the card runs rules.
    rulesMenu?: MenuEntry[];
    rulesReading?: main.RulesReading | null;
    // "Describe my own idea…": opens the assistant with this card's holdings
    // and window in its first message.
    onDescribeIdea?: () => void;
    // The chosen library script restated as rules, when the menu can say it
    // exactly (App.LibraryRules): shown on its card, one click to switch.
    twin?: main.LibraryTwin;
};

export default function PortfolioCard({
    form,
    onChange,
    onRemove,
    otherNames,
    defaultName,
    dbPath,
    available,
    names,
    ranges,
    pickable,
    showBaskets,
    onViewScript,
    rulesMenu = [],
    rulesReading = null,
    onDescribeIdea,
    twin,
}: Props) {
    const [tickerInput, setTickerInput] = useState("");
    // Which suggestion the keyboard is on, or -1 for "none: Enter takes what
    // was typed". Reset whenever the query changes, because the list under it
    // has changed.
    const [highlight, setHighlight] = useState(-1);
    const [pickerOpen, setPickerOpen] = useState(false);
    // What the last import changed or left out, shown until dismissed.
    const [importNote, setImportNote] = useState<{ lines: string[]; err: boolean } | null>(
        null,
    );
    // The allocation box being typed in, as typed. The form holds a number,
    // and "4." or "4.0" do not survive a round trip through one.
    const [pctDraft, setPctDraft] = useState<{ t: string; text: string } | null>(null);
    // Which date box has focus. A half-typed date is left alone while the
    // user is still typing it; it is normalized when they leave the box.
    const editingDate = useRef<"start" | "end" | null>(null);
    // The last dates that were real dates, for a typo to fall back to.
    const lastGood = useRef({ start: form.startDate, end: form.endDate });
    if (isValidDate(form.startDate)) lastGood.current.start = form.startDate;
    if (isValidDate(form.endDate)) lastGood.current.end = form.endDate;

    // Shallow-merge a patch into the form data owned by the container.
    const patch = (p: Partial<Form>) => onChange({ ...form, ...p });

    // Only symbols that keep the portfolio runnable are offered: priced, not
    // already held, and overlapping what is. Keyed on the holdings, not the
    // whole form, so typing a name or a date does not re-filter ten thousand
    // symbols; searchTickers then ranks the survivors per keystroke.
    const pool = useMemo(
        () => available.filter(tickerFilter(form, ranges)),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [available, ranges, form.tickers],
    );
    const suggestions = useMemo(
        () => searchTickers(pool, names, tickerInput),
        [pool, names, tickerInput],
    );

    const resolved = resolveTicker(tickerInput, highlight, suggestions);

    // The symbol is the identity and the name is the gloss, so a missing name
    // degrades to exactly the old display rather than to an empty cell.
    const nameOf = (t: string) => names[t] ?? "";

    // Takes only a symbol resolveTicker vouched for, so free text the
    // database has never heard of cannot reach the form.
    function addTicker(t: string) {
        if (!t || form.tickers.includes(t)) return;
        const tickers = [...form.tickers, t];
        const allocations = form.evenSplit
            ? evenAllocations(tickers)
            : { ...form.allocations, [t]: form.allocations[t] ?? 0 };
        patch({ tickers, allocations });
        setTickerInput("");
        setHighlight(-1);
        setPickerOpen(false);
    }

    // A whole basket at once. Not addTicker in a loop: each call patches from
    // the form it closed over, so the second would drop the first.
    function addBasket(syms: string[]) {
        const tickers = [...form.tickers];
        for (const t of syms) if (!tickers.includes(t)) tickers.push(t);
        if (tickers.length === form.tickers.length) return;
        const allocations = form.evenSplit
            ? evenAllocations(tickers)
            : Object.fromEntries(
                  tickers.map((t) => [t, form.allocations[t] ?? 0]),
              );
        patch({ tickers, allocations });
    }

    // A famous investor's portfolio replaces the card's holdings, weights,
    // window and strategy; capital and any cashflow schedule are kept. The
    // card's benchmark is kept too unless it is empty, in which case the
    // market is the comparison the portfolio's whole point rests on.
    function investorForm(id: string) {
        const inv = INVESTORS.find((e) => e.id === id);
        if (!inv) return null;
        const benchmark =
            form.benchmark || (ranges["$SP500"] ? "$SP500" : "");
        return buildInvestor(inv, ranges, benchmark, form);
    }

    // Every investor portfolio, with the form it would build here or null
    // when this database cannot support it -- those stay in the dropdown,
    // disabled, so a missing one says why rather than vanishing.
    const investorChoices = INVESTORS.map((inv) => ({
        inv,
        built: investorForm(inv.id),
    }));
    // The one this card was filled from, recognised by name, so its caveat
    // stays on screen while the card still is that portfolio.
    const copiedInvestor = INVESTORS.find((inv) => inv.name === form.name);

    function removeTicker(t: string) {
        const tickers = form.tickers.filter((x) => x !== t);
        let allocations: Record<string, number>;
        if (form.evenSplit) {
            allocations = evenAllocations(tickers);
        } else {
            const { [t]: _removed, ...rest } = form.allocations;
            allocations = rest;
        }
        patch({ tickers, allocations });
    }

    // Typing a percentage means the user wants control: drop even-split mode.
    function setAllocation(t: string, value: number) {
        patch({
            evenSplit: false,
            allocations: { ...form.allocations, [t]: clampPct(value) },
        });
    }

    async function importHoldings() {
        try {
            const imp = (await ImportHoldings(dbPath)) as unknown as HoldingsImport;
            if (!imp || !imp.file) return; // picker cancelled
            onChange(applyImport(form, imp));
            setImportNote({ lines: importSummary(imp), err: false });
        } catch (e) {
            setImportNote({ lines: [String(e)], err: true });
        }
    }

    function distributeEvenly() {
        patch({ evenSplit: true, allocations: evenAllocations(form.tickers) });
    }

    // Snap the date range to a quick preset, always ending at the last date the
    // selected tickers cover and starting no earlier than the first they cover.
    function applyPreset(kind: "max" | "ytd" | "1y" | "5y") {
        if (!coverage || !coverage.start || !coverage.end) return;
        // The $-benchmark series currently end weeks earlier than the priced
        // tickers, and a window past that end silently costs every
        // benchmark-relative metric. Snapping to the earlier of the two keeps
        // the comparison the presets promise.
        const benchEnd = form.benchmark ? ranges[form.benchmark]?.[1] : "";
        const end =
            benchEnd && benchEnd < coverage.end ? benchEnd : coverage.end;
        const start =
            kind === "max"
                ? coverage.start
                : kind === "ytd"
                  ? laterDate(`${end.slice(0, 4)}-01-01`, coverage.start)
                  : laterDate(minusYears(end, kind === "1y" ? 1 : 5), coverage.start);
        patch({ startDate: start, endDate: end });
    }

    const chosen = useMemo(
        () => pickable.find((c) => c.File === form.strategy) ?? null,
        [pickable, form.strategy],
    );
    const allocationsApply = usesAllocations(form);
    const total = allocationTotal(form);
    const totalsTo100 = Math.abs(total - 100) < 0.01;

    const coverage = useMemo(() => coverageOf(form, ranges), [form, ranges]);
    const bounds = useMemo(() => dateBounds(form, ranges), [form, ranges]);

    // Keep the dates inside the window the holdings (and benchmark) cover.
    // Runs after every render, not on a dependency list: sibling cards patch
    // from the same snapshot of the form list, so one card's fix can be
    // overwritten by another's in the same commit. Re-checking each render
    // converges instead, and normalizeDates returns null once nothing is
    // wrong, so a settled card does not patch at all.
    useEffect(() => {
        if (editingDate.current) return;
        const fix = normalizeDates(form, bounds, lastGood.current, null);
        if (fix) patch(fix);
    });

    function leaveDate(which: "start" | "end") {
        editingDate.current = null;
        const fix = normalizeDates(form, bounds, lastGood.current, which);
        if (fix) patch(fix);
    }
    const dateError = useMemo(() => dateErrorOf(form, coverage), [form, coverage]);
    const benchmarkEnd = form.benchmark ? (ranges[form.benchmark]?.[1] ?? "") : "";
    const benchmarkWarning = useMemo(
        () => benchmarkWarningOf(form, ranges),
        [form, ranges],
    );

    // Preset chips need a known data window (i.e. at least one ticker chosen
    // and its ranges loaded) to snap the dates to.
    // Named in plain words, with the real start date: "monthly" hides the
    // first-trading-day-on-or-after rule, and that is exactly the detail
    // someone reconciling against a spreadsheet will trip on. It also states
    // the one-period delay, which surprises people otherwise.
    const cashflowNote = useMemo(() => {
        if (form.cashflowFrequency === "never") return "";
        if (form.cashflowAmount <= 0) {
            return "Set an amount, or leave the frequency on Never — as it stands nothing is scheduled.";
        }
        const verb = form.cashflowDirection === "withdraw" ? "taken out" : "added";
        const every = { monthly: "month", quarterly: "quarter", annually: "year" }[
            form.cashflowFrequency
        ];
        const start = form.startDate || "the start date";
        return `$${formatMoney(form.cashflowAmount)} ${verb} on the first trading day of each ${every}, beginning one ${every} after ${start}. The opening capital is already invested, so the first one lands a ${every} in.`;
    }, [form.cashflowFrequency, form.cashflowAmount, form.cashflowDirection, form.startDate]);

    const presetsReady =
        !!coverage &&
        !!coverage.start &&
        !!coverage.end &&
        coverage.start <= coverage.end;

    return (
        <div className="portfolio-card">
            <div className="portfolio-card-bar">
                <span className="portfolio-card-name">
                    {form.name.trim() || "Untitled portfolio"}
                </span>
                {onRemove && (
                    <button
                        type="button"
                        className="ghost portfolio-remove"
                        onClick={onRemove}
                        title="Remove this portfolio from the run"
                    >
                        Remove
                    </button>
                )}
            </div>
            <section className="form-card">
                <h3 className="form-card-title">Portfolio</h3>
                <div className="field-row">
                    <label>
                        Name
                        <input
                            value={form.name}
                            maxLength={60}
                            onChange={(e) => patch({ name: e.target.value })}
                            onBlur={() => {
                                const name = uniqueName(form.name, otherNames, defaultName);
                                if (name !== form.name) patch({ name });
                            }}
                        />
                    </label>
                    <label>
                        Starting capital
                        <div className="money-field">
                            <span className="money-sign">$</span>
                            <input
                                type="text"
                                inputMode="numeric"
                                placeholder="10,000"
                                value={formatMoney(form.buyingPower)}
                                onChange={(e) => {
                                    const digits = e.target.value
                                        .replace(/[^0-9]/g, "")
                                        .slice(0, MONEY_DIGITS);
                                    patch({ buyingPower: digits ? Number(digits) : 0 });
                                }}
                                // Nothing to invest is not a backtest.
                                onBlur={() => {
                                    if (form.buyingPower <= 0)
                                        patch({ buyingPower: DEFAULT_CAPITAL });
                                }}
                            />
                        </div>
                    </label>
                </div>

                {/* Direction is a dropdown rather than a minus sign. The TOML
                    field is a signed Amount, but a currency box that means the
                    opposite thing when you type "-" is a trap, and a mistyped
                    sign turns a contribution plan into a decumulation one.
                    "Never" is the default and emits no config at all, which is
                    also why there is no enable checkbox. */}
                <div className="criterion-row cashflow-row">
                    <select
                        aria-label="Deposit or withdraw"
                        value={form.cashflowDirection}
                        onChange={(e) =>
                            patch({
                                cashflowDirection: e.target.value as Form["cashflowDirection"],
                            })
                        }
                    >
                        <option value="deposit">Deposit</option>
                        <option value="withdraw">Withdraw</option>
                    </select>
                    <div className="money-field">
                        <span className="money-sign">$</span>
                        <input
                            type="text"
                            inputMode="numeric"
                            placeholder="500"
                            aria-label="Recurring amount"
                            value={formatMoney(form.cashflowAmount)}
                            onChange={(e) => {
                                const digits = e.target.value
                                    .replace(/[^0-9]/g, "")
                                    .slice(0, MONEY_DIGITS);
                                patch({ cashflowAmount: digits ? Number(digits) : 0 });
                            }}
                        />
                    </div>
                    <select
                        aria-label="How often"
                        value={form.cashflowFrequency}
                        onChange={(e) =>
                            patch({
                                cashflowFrequency: e.target.value as Form["cashflowFrequency"],
                            })
                        }
                    >
                        {CASHFLOW_FREQUENCIES.map((f) => (
                            <option key={f.value} value={f.value}>
                                {f.label}
                            </option>
                        ))}
                    </select>
                </div>
                {cashflowNote && <div className="hint">{cashflowNote}</div>}
            </section>

            <section className="form-card">
                <h3 className="form-card-title">Backtest period</h3>
                <div className="field-row">
                    <label>
                        Start date
                        <input
                            type="text"
                            inputMode="numeric"
                            placeholder="YYYY-MM-DD"
                            maxLength={10}
                            disabled={!bounds}
                            title={
                                bounds
                                    ? `${bounds.min} to ${bounds.max}`
                                    : "Add a holding first — the dates it covers set the range"
                            }
                            value={form.startDate}
                            onFocus={() => (editingDate.current = "start")}
                            onChange={(e) => patch({ startDate: maskDate(e.target.value) })}
                            onBlur={() => leaveDate("start")}
                            onKeyDown={(e) => {
                                if (e.key === "Enter") e.currentTarget.blur();
                            }}
                        />
                    </label>
                    <label>
                        End date
                        <input
                            type="text"
                            inputMode="numeric"
                            placeholder="YYYY-MM-DD"
                            maxLength={10}
                            disabled={!bounds}
                            title={
                                bounds
                                    ? `${bounds.min} to ${bounds.max}`
                                    : "Add a holding first — the dates it covers set the range"
                            }
                            value={form.endDate}
                            onFocus={() => (editingDate.current = "end")}
                            onChange={(e) => patch({ endDate: maskDate(e.target.value) })}
                            onBlur={() => leaveDate("end")}
                            onKeyDown={(e) => {
                                if (e.key === "Enter") e.currentTarget.blur();
                            }}
                        />
                    </label>
                </div>

                <div className="date-presets">
                    <span className="presets-label">Quick range</span>
                    {([
                        ["max", "Max"],
                        ["ytd", "YTD"],
                        ["1y", "1Y"],
                        ["5y", "5Y"],
                    ] as const).map(([kind, label]) => (
                        <button
                            key={kind}
                            type="button"
                            className="preset-chip"
                            disabled={!presetsReady}
                            onClick={() => applyPreset(kind)}
                        >
                            {label}
                        </button>
                    ))}
                </div>
            </section>

            <section className="form-card wide">
                <h3 className="form-card-title">Strategy</h3>
                <label className="strategy-pick">
                    What should it do?
                    <select
                        value={form.strategy}
                        onChange={(e) => {
                            const strategy = e.target.value;
                            if (strategy !== RULES_STRATEGY) {
                                patch({ strategy });
                                return;
                            }
                            // Kept when switching away and back, so an edited
                            // rule set is not lost to a stray click.
                            patch({
                                strategy,
                                rules: settleCounts(
                                    form.rules ?? DEFAULT_RULES,
                                    form.tickers.length,
                                ),
                            });
                            onDescribeIdea?.();
                        }}
                    >
                        <option value="">
                            Buy and hold — buy on day 1, never sell
                        </option>
                        <option value={RULES_STRATEGY}>
                            Describe my own idea… (read back as a sentence)
                        </option>
                        {pickable.map((c) => (
                            <option key={c.File} value={c.File}>
                                {strategyLabel(c.File)}
                            </option>
                        ))}
                    </select>
                </label>

                <label className="strategy-pick">
                    Compare it against
                    <select
                        value={form.benchmark}
                        onChange={(e) => patch({ benchmark: e.target.value })}
                    >
                        {BENCHMARKS.map((b) => (
                            <option
                                key={b.value}
                                value={b.value}
                                // A benchmark this database does not hold would
                                // only produce an empty comparison.
                                disabled={
                                    !!b.value &&
                                    hasRanges(ranges) &&
                                    !ranges[b.value]
                                }
                            >
                                {b.label}
                            </option>
                        ))}
                    </select>
                </label>
                {benchmarkWarning && (
                    <div className="coverage-msg err">
                        {benchmarkWarning}
                        {benchmarkEnd && form.endDate > benchmarkEnd && (
                            <button
                                type="button"
                                className="preset-chip"
                                onClick={() => patch({ endDate: benchmarkEnd })}
                            >
                                End on {benchmarkEnd}
                            </button>
                        )}
                    </div>
                )}

                {/* The failure mode is the most useful sentence here, and it
                    has to be readable BEFORE the run, not explained after a
                    bad result. */}
                {usesRules(form) ? (
                    <RulesCard
                        menu={rulesMenu}
                        rules={form.rules!}
                        reading={rulesReading}
                        tickerCount={form.tickers.length}
                        hasBenchmark={!!form.benchmark}
                        onChange={(rules) => patch({ rules })}
                        onAskAssistant={onDescribeIdea}
                    />
                ) : chosen ? (
                    <div className="strategy-card">
                        <p className="strategy-idea">{chosen.Idea}</p>
                        <dl className="strategy-when">
                            <dt className="works">Works when</dt>
                            <dd>{chosen.Works}</dd>
                            <dt className="fails">Fails when</dt>
                            <dd>{chosen.Fails}</dd>
                        </dl>
                        <p className="strategy-baseline">
                            Compare it against: {chosen.Baseline}
                        </p>
                        {/* The same strategy as a sentence, held to the cent
                            against the script by the engine's tests. */}
                        {twin && (
                            <div className="strategy-twin">
                                <p>
                                    <strong>As rules:</strong> {twin.sentence}
                                </p>
                                <button
                                    type="button"
                                    className="link-button"
                                    onClick={() => {
                                        const rules = fromRulesConfig(twin.rules);
                                        // Fixed percentages on the card are
                                        // what the script's weights would be.
                                        if (form.strategy === "rebalance.lua" && !form.evenSplit) {
                                            rules.weight = "allocations";
                                        }
                                        patch({
                                            strategy: RULES_STRATEGY,
                                            rules: settleCounts(rules, form.tickers.length),
                                        });
                                    }}
                                    title="Switch this card to the same strategy written as rules you can read and edit"
                                >
                                    Use as rules →
                                </button>
                            </div>
                        )}
                        {onViewScript && (
                            <button
                                type="button"
                                className="link-button"
                                onClick={() => onViewScript(form.strategy)}
                                title="Open the script in the Config tab's Lua editor, read-only"
                            >
                                Read the script →
                            </button>
                        )}
                    </div>
                ) : (
                    <div className="strategy-card">
                        <p className="strategy-idea">
                            Open a position in every holding on the first day and
                            keep it to the end.
                        </p>
                        <dl className="strategy-when">
                            <dt className="works">Works when</dt>
                            <dd>
                                the holdings rise over your window — the default
                                answer, and the one most strategies fail to beat.
                            </dd>
                            <dt className="fails">Fails when</dt>
                            <dd>
                                you cannot sit through the drawdown it takes on the
                                way; it has no exit and never goes to cash.
                            </dd>
                        </dl>
                    </div>
                )}
            </section>

            <section className="form-card wide">
                <h3 className="form-card-title">Holdings &amp; allocation</h3>

                {/* A real listbox rather than a <datalist>: the company name
                    is the whole point, and WebKit -- which is the only engine
                    this app ever runs in -- does not reliably render a
                    datalist option's label beside its value. Owning the list
                    also means owning the ranking and the keyboard. */}
                <div className="ticker-add">
                    <div className="ticker-combo">
                        <input
                            role="combobox"
                            data-shortcut="search"
                            aria-expanded={pickerOpen && suggestions.length > 0}
                            aria-controls="ticker-listbox"
                            aria-autocomplete="list"
                            placeholder={
                                dbPath
                                    ? "Symbol or company name   ( / )"
                                    : "Choose a DB first"
                            }
                            disabled={!dbPath}
                            value={tickerInput}
                            onChange={(e) => {
                                setTickerInput(e.target.value);
                                setHighlight(-1);
                                setPickerOpen(true);
                            }}
                            onFocus={() => setPickerOpen(true)}
                            // A click lands before blur, so closing has to wait
                            // one tick or the row unmounts before it is chosen.
                            onBlur={() => setTimeout(() => setPickerOpen(false), 120)}
                            onKeyDown={(e) => {
                                const n = suggestions.length;
                                if (e.key === "ArrowDown" && n > 0) {
                                    e.preventDefault();
                                    setPickerOpen(true);
                                    setHighlight((h) => (h + 1) % n);
                                } else if (e.key === "ArrowUp" && n > 0) {
                                    e.preventDefault();
                                    setPickerOpen(true);
                                    setHighlight((h) => (h <= 0 ? n - 1 : h - 1));
                                } else if (e.key === "Enter") {
                                    e.preventDefault();
                                    // Nothing highlighted takes the exact
                                    // symbol, else the best match -- never
                                    // free text the database cannot price.
                                    addTicker(resolved);
                                } else if (e.key === "Escape") {
                                    setPickerOpen(false);
                                    setHighlight(-1);
                                }
                            }}
                        />
                        {pickerOpen && suggestions.length > 0 && (
                            <ul className="ticker-listbox" id="ticker-listbox" role="listbox">
                                {suggestions.map((t, i) => (
                                    <li
                                        key={t}
                                        role="option"
                                        aria-selected={i === highlight}
                                        className={i === highlight ? "on" : undefined}
                                        onMouseEnter={() => setHighlight(i)}
                                        onMouseDown={(e) => {
                                            // mousedown, not click: blur would
                                            // otherwise close the list first.
                                            e.preventDefault();
                                            addTicker(t);
                                        }}
                                    >
                                        <span className="opt-sym">{t}</span>
                                        <span className="opt-name">{nameOf(t)}</span>
                                    </li>
                                ))}
                            </ul>
                        )}
                    </div>
                    <button
                        disabled={!resolved}
                        title={resolved ? `Add ${resolved}` : "Pick a symbol from the list"}
                        onClick={() => addTicker(resolved)}
                    >
                        Add
                    </button>
                    <button
                        disabled={!dbPath}
                        title="Fill this portfolio from a CSV or Excel (.xlsx) file of assets and amounts"
                        onClick={importHoldings}
                    >
                        Import…
                    </button>
                </div>
                {/* Always offered, on every card: picking one REPLACES the
                    holdings, weights, dates and strategy, the same way Import…
                    replaces the holdings. */}
                <label className="strategy-pick investor-pick">
                    Copy a famous investor
                    <select
                        value={copiedInvestor?.id ?? ""}
                        onChange={(e) => {
                            const choice = investorChoices.find(
                                (c) => c.inv.id === e.target.value,
                            );
                            if (choice?.built) onChange(choice.built);
                        }}
                    >
                        <option value="">
                            {form.tickers.length > 0
                                ? "Choose one — replaces these holdings"
                                : "Choose one"}
                        </option>
                        {investorChoices.map(({ inv, built }) => (
                            <option key={inv.id} value={inv.id} disabled={!built}>
                                {inv.name} — {inv.investor}
                                {built ? "" : " (not in this database)"}
                            </option>
                        ))}
                    </select>
                </label>
                {copiedInvestor && (
                    <div className="hint investor-note">
                        {copiedInvestor.note}. {copiedInvestor.caveat}
                    </div>
                )}
                {importNote && (
                    <div className={`hint import-note${importNote.err ? " err" : ""}`}>
                        {importNote.lines.map((l, i) => (
                            <div key={i}>{l}</div>
                        ))}
                        <button
                            className="link"
                            title="Dismiss"
                            onClick={() => setImportNote(null)}
                        >
                            Dismiss
                        </button>
                    </div>
                )}

                {!allocationsApply && form.tickers.length > 0 && (
                    <div className="hint">
                        {usesRules(form)
                            ? "Your rules split the money themselves (see Split the money above)"
                            : `${strategyLabel(form.strategy)} sizes its own positions`}, so
                        these percentages are not used — they apply to buy and
                        hold. The symbols below are still the universe it trades.
                    </div>
                )}
                {form.tickers.length === 0 ? (
                    <div className="empty">
                        No holdings yet — add a ticker to allocate to it, or Import… a CSV or Excel file of what you hold.
                        {showBaskets && (
                            <div className="baskets">
                                {BASKETS.map((b) => {
                                    const syms = basketTickers(b, ranges);
                                    if (syms.length === 0) return null;
                                    return (
                                        <button
                                            key={b.id}
                                            className="basket"
                                            onClick={() => addBasket(syms)}
                                            title={syms.join(", ")}
                                        >
                                            <span className="basket-label">
                                                {b.label}
                                            </span>
                                            <span className="basket-note">
                                                {b.note}
                                            </span>
                                        </button>
                                    );
                                })}
                            </div>
                        )}
                    </div>
                ) : (
                    <div className="alloc-list">
                        <div className="alloc-row alloc-head">
                            <span>Symbol</span>
                            <span>Allocation</span>
                            <span />
                        </div>
                        {form.tickers.map((t) => (
                            <div className="alloc-row" key={t}>
                                <span className="alloc-sym">
                                    {t}
                                    {nameOf(t) && (
                                        <span className="alloc-name" title={nameOf(t)}>
                                            {nameOf(t)}
                                        </span>
                                    )}
                                </span>
                                <div className="alloc-pct">
                                    <input
                                        type="text"
                                        inputMode="decimal"
                                        value={
                                            pctDraft?.t === t
                                                ? pctDraft.text
                                                : (form.allocations[t] ?? 0)
                                        }
                                        onChange={(e) => {
                                            const text = maskDecimal(
                                                e.target.value.replace("-", ""),
                                            );
                                            setPctDraft({ t, text });
                                            setAllocation(t, text ? Number(text) || 0 : 0);
                                        }}
                                        onBlur={() => setPctDraft(null)}
                                    />
                                    <span className="pct-sign">%</span>
                                </div>
                                <button
                                    className="alloc-x"
                                    title={`Remove ${t}`}
                                    onClick={() => removeTicker(t)}
                                >
                                    ×
                                </button>
                            </div>
                        ))}
                        <div className={`alloc-total ${totalsTo100 ? "ok" : "warn"}`}>
                            <span>Total</span>
                            <span className="total-val">{formatPct(total)}%</span>
                            <span />
                        </div>
                    </div>
                )}
                {allocationsApply && form.tickers.length > 0 && !totalsTo100 && (
                    <div className="hint">
                        Allocations add up to {formatPct(total)}%. They're relative, so the
                        backtest still splits your capital by these shares — but they
                        usually total 100%.
                    </div>
                )}
            </section>

            {coverage &&
                (dateError ? (
                    <div className="coverage-msg err">{dateError}</div>
                ) : (
                    <div className="coverage-msg ok">
                        Data available for these tickers: {coverage.start} to{" "}
                        {coverage.end}
                    </div>
                ))}
        </div>
    );
}
