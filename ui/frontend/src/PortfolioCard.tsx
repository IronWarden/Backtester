import { useMemo, useState } from "react";
import type { backtest } from "../wailsjs/go/models";
import {
    BENCHMARKS,
    CASHFLOW_FREQUENCIES,
    STRATEGY_DIR,
    usesAllocations,
} from "./buildToml";
import type { SimpleForm as Form } from "./buildToml";
import { searchTickers } from "./tickerSearch";
import { BASKETS, basketTickers } from "./examples";
import {
    allocationTotal,
    benchmarkWarningOf,
    coverageOf,
    dateErrorOf,
} from "./portfolioCheck";
import type { Ranges } from "./portfolioCheck";

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

type Props = {
    form: Form;
    onChange: (next: Form) => void;
    // Null for the only portfolio: a run with nothing in it is not a state the
    // form should be able to reach by clicking Remove.
    onRemove: (() => void) | null;
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
};

export default function PortfolioCard({
    form,
    onChange,
    onRemove,
    dbPath,
    available,
    names,
    ranges,
    pickable,
    showBaskets,
}: Props) {
    const [tickerInput, setTickerInput] = useState("");
    // Which suggestion the keyboard is on, or -1 for "none: Enter takes what
    // was typed". Reset whenever the query changes, because the list under it
    // has changed.
    const [highlight, setHighlight] = useState(-1);
    const [pickerOpen, setPickerOpen] = useState(false);

    // Shallow-merge a patch into the form data owned by the container.
    const patch = (p: Partial<Form>) => onChange({ ...form, ...p });

    const suggestions = useMemo(() => {
        const sel = new Set(form.tickers);
        return searchTickers(
            available.filter((t) => !sel.has(t)),
            names,
            tickerInput,
        );
    }, [available, names, form.tickers, tickerInput]);

    // The symbol is the identity and the name is the gloss, so a missing name
    // degrades to exactly the old display rather than to an empty cell.
    const nameOf = (t: string) => names[t] ?? "";

    function addTicker(raw: string) {
        const t = raw.trim().toUpperCase();
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
                            onChange={(e) => patch({ name: e.target.value })}
                        />
                    </label>
                    <label>
                        Starting capital
                        <div className="money-field">
                            <span className="money-sign">$</span>
                            <input
                                type="text"
                                inputMode="numeric"
                                placeholder="100,000"
                                value={formatMoney(form.buyingPower)}
                                onChange={(e) => {
                                    const digits = e.target.value.replace(/[^0-9]/g, "");
                                    patch({ buyingPower: digits ? Number(digits) : 0 });
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
                                const digits = e.target.value.replace(/[^0-9]/g, "");
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
                            value={form.startDate}
                            onChange={(e) => patch({ startDate: e.target.value.trim() })}
                        />
                    </label>
                    <label>
                        End date
                        <input
                            type="text"
                            inputMode="numeric"
                            placeholder="YYYY-MM-DD"
                            value={form.endDate}
                            onChange={(e) => patch({ endDate: e.target.value.trim() })}
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
                        onChange={(e) => patch({ strategy: e.target.value })}
                    >
                        <option value="">
                            Buy and hold — buy on day 1, never sell
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
                            <option key={b.value} value={b.value}>
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
                {chosen ? (
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
                            aria-expanded={pickerOpen && suggestions.length > 0}
                            aria-controls="ticker-listbox"
                            aria-autocomplete="list"
                            placeholder={
                                dbPath
                                    ? "Symbol or company name"
                                    : "Choose a DB first"
                            }
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
                                    // Nothing highlighted means take what was
                                    // typed: a symbol the DB has not heard of
                                    // is still worth attempting, and the
                                    // coverage check below reports it properly.
                                    addTicker(
                                        highlight >= 0 && highlight < n
                                            ? suggestions[highlight]
                                            : tickerInput,
                                    );
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
                    <button onClick={() => addTicker(tickerInput)}>Add</button>
                </div>

                {!allocationsApply && form.tickers.length > 0 && (
                    <div className="hint">
                        {strategyLabel(form.strategy)} sizes its own positions, so
                        these percentages are not used — they apply to buy and
                        hold. The symbols below are still the universe it trades.
                    </div>
                )}
                {form.tickers.length === 0 ? (
                    <div className="empty">
                        No holdings yet — add a ticker to allocate to it.
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
                                        inputMode="numeric"
                                        value={form.allocations[t] ?? 0}
                                        onChange={(e) => {
                                            const digits = e.target.value.replace(/[^0-9]/g, "");
                                            setAllocation(t, digits ? Number(digits) : 0);
                                        }}
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
