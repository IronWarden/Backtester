import { useEffect, useMemo, useState } from "react";
import { ListTickers, TickerDateRanges } from "../wailsjs/go/main/App";
import { EventsOn } from "../wailsjs/runtime/runtime";
import { buildToml, SimpleForm as Form } from "./buildToml";

type Props = {
    dbPath: string;
    running: boolean;
    // Form data is owned by App so it persists across runs and mode switches.
    form: Form;
    onChange: (next: Form) => void;
    // Persist + run the generated config (App owns the run pipeline).
    onRun: (toml: string) => void;
    // Hand the generated config off to the Advanced TOML editor.
    onEditAsToml: (toml: string) => void;
};

// Split 100% across the tickers as whole percentages that sum to exactly
// 100, handing the leftover one point at a time to the first few rows
// (e.g. 3 tickers -> 34/33/33).
function evenAllocations(syms: string[]): Record<string, number> {
    const out: Record<string, number> = {};
    const n = syms.length;
    if (n === 0) return out;
    const base = Math.floor(100 / n);
    let remainder = 100 - base * n;
    for (const s of syms) {
        out[s] = base + (remainder > 0 ? 1 : 0);
        if (remainder > 0) remainder--;
    }
    return out;
}

// Keep a single field in [0, 100]; treat blanks/NaN as 0.
function clampPct(n: number): number {
    if (!Number.isFinite(n)) return 0;
    return Math.max(0, Math.min(100, n));
}

// Trim float noise (33.33000001 -> 33.33) for display.
function formatPct(n: number): string {
    return String(Math.round(n * 100) / 100);
}

// Whole-dollar amount with thousands separators for display (100000 ->
// "100,000"); blank at 0 so the placeholder shows instead of a bare "0".
function formatMoney(n: number): string {
    return n > 0 ? n.toLocaleString("en-US") : "";
}

// A YYYY-MM-DD string that names a real calendar date. ISO dates also sort
// lexically, so plain string comparison orders them correctly elsewhere.
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;
function isValidDate(s: string): boolean {
    return ISO_DATE.test(s) && Number.isFinite(Date.parse(`${s}T00:00:00Z`));
}

// The later of two YYYY-MM-DD strings.
function laterDate(a: string, b: string): string {
    return a > b ? a : b;
}

// Shift a YYYY-MM-DD date back by whole years, rolling to a valid calendar
// date via UTC math, returned as YYYY-MM-DD.
function minusYears(date: string, years: number): string {
    const [y, m, d] = date.split("-").map(Number);
    return new Date(Date.UTC(y - years, m - 1, d)).toISOString().slice(0, 10);
}

export default function SimpleForm({
    dbPath,
    running,
    form,
    onChange,
    onRun,
    onEditAsToml,
}: Props) {
    const [tickerInput, setTickerInput] = useState("");
    const [available, setAvailable] = useState<string[]>([]);
    // Per-ticker [firstDate, lastDate], keyed by symbol, from the chosen DB.
    const [ranges, setRanges] = useState<Record<string, string[]>>({});
    // Bumped when the assistant's fetch_history tool writes new bars, so
    // the ticker list and coverage ranges reload without switching DBs.
    const [dbVersion, setDbVersion] = useState(0);

    useEffect(
        () => EventsOn("chat:db-changed", () => setDbVersion((v) => v + 1)),
        [],
    );

    // Shallow-merge a patch into the form data owned by App.
    const patch = (p: Partial<Form>) => onChange({ ...form, ...p });

    // Pull the real ticker universe + each ticker's available date range from
    // the chosen DB, for autocomplete and for constraining the date pickers.
    useEffect(() => {
        if (!dbPath) {
            setAvailable([]);
            setRanges({});
            return;
        }
        let cancelled = false;
        ListTickers(dbPath)
            .then((ts) => {
                if (!cancelled) setAvailable(ts ?? []);
            })
            .catch(() => {
                if (!cancelled) setAvailable([]);
            });
        TickerDateRanges(dbPath)
            .then((r) => {
                if (!cancelled) setRanges(r ?? {});
            })
            .catch(() => {
                if (!cancelled) setRanges({});
            });
        return () => {
            cancelled = true;
        };
    }, [dbPath, dbVersion]);

    const suggestions = useMemo(() => {
        const sel = new Set(form.tickers);
        return available.filter((t) => !sel.has(t));
    }, [available, form.tickers]);

    function addTicker(raw: string) {
        const t = raw.trim().toUpperCase();
        if (!t || form.tickers.includes(t)) return;
        const tickers = [...form.tickers, t];
        const allocations = form.evenSplit
            ? evenAllocations(tickers)
            : { ...form.allocations, [t]: form.allocations[t] ?? 0 };
        patch({ tickers, allocations });
        setTickerInput("");
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
        const end = coverage.end;
        const start =
            kind === "max"
                ? coverage.start
                : kind === "ytd"
                  ? laterDate(`${end.slice(0, 4)}-01-01`, coverage.start)
                  : laterDate(minusYears(end, kind === "1y" ? 1 : 5), coverage.start);
        patch({ startDate: start, endDate: end });
    }

    const total = form.tickers.reduce(
        (sum, t) => sum + (form.allocations[t] ?? 0),
        0,
    );
    const totalsTo100 = Math.abs(total - 100) < 0.01;

    // The window every selected ticker covers: the latest of their first dates
    // to the earliest of their last dates. `unknown` lists any selected symbol
    // not in the DB. Null until at least one ticker is chosen.
    const rangesLoaded = Object.keys(ranges).length > 0;
    const coverage = useMemo(() => {
        if (form.tickers.length === 0 || !rangesLoaded) return null;
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
    }, [ranges, rangesLoaded, form.tickers]);

    // A human-readable reason the current dates/tickers can't be backtested, or
    // "" when they're valid. The engine enforces the same rule server-side; this
    // just stops the user reaching that error and explains how to fix it.
    const dateError = useMemo(() => {
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
        if (form.startDate > form.endDate)
            return "Start date is after the end date.";
        return "";
    }, [coverage, form.startDate, form.endDate]);

    const canRun =
        form.tickers.length > 0 &&
        !!dbPath &&
        !running &&
        total > 0 &&
        !dateError;

    // Preset chips need a known data window (i.e. at least one ticker chosen
    // and its ranges loaded) to snap the dates to.
    const presetsReady =
        !!coverage &&
        !!coverage.start &&
        !!coverage.end &&
        coverage.start <= coverage.end;

    return (
        <div className="simple-form">
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
                <h3 className="form-card-title">Holdings &amp; allocation</h3>

                <div className="ticker-add">
                    <input
                        list="ticker-options"
                        placeholder={
                            dbPath ? "Type a symbol and press Enter" : "Choose a DB first"
                        }
                        value={tickerInput}
                        onChange={(e) => setTickerInput(e.target.value)}
                        onKeyDown={(e) => {
                            if (e.key === "Enter") {
                                e.preventDefault();
                                addTicker(tickerInput);
                            }
                        }}
                    />
                    <datalist id="ticker-options">
                        {suggestions.map((t) => (
                            <option key={t} value={t} />
                        ))}
                    </datalist>
                    <button onClick={() => addTicker(tickerInput)}>Add</button>
                </div>

                {form.tickers.length === 0 ? (
                    <div className="empty">
                        No holdings yet — add a ticker to allocate to it.
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
                                <span className="alloc-sym">{t}</span>
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
                {form.tickers.length > 0 && !totalsTo100 && (
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

            <div className="field-row actions">
                <button
                    className="run"
                    disabled={!canRun}
                    onClick={() => onRun(buildToml(form))}
                >
                    {running ? "Running…" : "Run backtest"}
                </button>
                <button
                    disabled={form.tickers.length === 0}
                    onClick={() => onEditAsToml(buildToml(form))}
                >
                    Edit as TOML →
                </button>
            </div>
        </div>
    );
}
