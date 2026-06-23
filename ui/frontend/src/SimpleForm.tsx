import { useEffect, useMemo, useState } from "react";
import { ListTickers } from "../wailsjs/go/main/App";
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

    // Shallow-merge a patch into the form data owned by App.
    const patch = (p: Partial<Form>) => onChange({ ...form, ...p });

    // Pull the real ticker universe from the chosen DB for autocomplete.
    useEffect(() => {
        if (!dbPath) {
            setAvailable([]);
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
        return () => {
            cancelled = true;
        };
    }, [dbPath]);

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

    const total = form.tickers.reduce(
        (sum, t) => sum + (form.allocations[t] ?? 0),
        0,
    );
    const totalsTo100 = Math.abs(total - 100) < 0.01;

    const canRun = form.tickers.length > 0 && !!dbPath && !running && total > 0;

    return (
        <div className="simple-form">
            <div className="field-row">
                <label>
                    Portfolio name
                    <input
                        value={form.name}
                        onChange={(e) => patch({ name: e.target.value })}
                    />
                </label>
                <label>
                    Starting capital ($)
                    <input
                        type="number"
                        min={0}
                        value={form.buyingPower}
                        onChange={(e) => patch({ buyingPower: Number(e.target.value) })}
                    />
                </label>
                <label>
                    Start date
                    <input
                        type="date"
                        value={form.startDate}
                        onChange={(e) => patch({ startDate: e.target.value })}
                    />
                </label>
                <label>
                    End date
                    <input
                        type="date"
                        value={form.endDate}
                        onChange={(e) => patch({ endDate: e.target.value })}
                    />
                </label>
            </div>

            <div className="field-block">
                <div className="block-head">
                    <label className="block-label">Holdings &amp; allocation</label>
                </div>

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
                                        type="number"
                                        min={0}
                                        max={100}
                                        step={1}
                                        value={form.allocations[t] ?? 0}
                                        onChange={(e) => setAllocation(t, Number(e.target.value))}
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
            </div>

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
