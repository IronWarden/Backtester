import { useEffect, useMemo, useState } from "react";
import {
    CriterionMetrics,
    ListStrategies,
    ListTickers,
    TickerDateRanges,
} from "../wailsjs/go/main/App";
import { backtest } from "../wailsjs/go/models";
import { EventsOn } from "../wailsjs/runtime/runtime";
import {
    BENCHMARKS,
    buildHypothesis,
    buildToml,
    CRITERION_OPS,
    criterionLabel,
    STARTER_SCRIPT,
    SimpleForm as Form,
    usesAllocations,
} from "./buildToml";

type Props = {
    dbPath: string;
    running: boolean;
    // Form data is owned by App so it persists across runs and mode switches.
    form: Form;
    onChange: (next: Form) => void;
    // Persist + run the generated config (App owns the run pipeline). The
    // hypothesis rides along rather than living in App's raw box: registering
    // it is part of starting THIS run, not a mode-wide setting.
    onRun: (toml: string, hypothesis: string) => void;
    // Hand the generated config off to the Advanced TOML editor, with the
    // registered question, so switching modes does not quietly drop it.
    onEditAsToml: (toml: string, hypothesis: string) => void;
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

// "momentum_rotation.lua" -> "Momentum rotation". The file name is the
// identity the config uses; this is only what the user reads.
function strategyLabel(file: string): string {
    const bare = file.replace(/\.lua$/, "").replace(/_/g, " ");
    return bare.charAt(0).toUpperCase() + bare.slice(1);
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
    // The shipped library's gallery cards, for the strategy picker.
    const [gallery, setGallery] = useState<backtest.StrategyCard[]>([]);
    // Criterion metrics, served by the engine so the picker cannot offer one
    // ParseCriterion would reject.
    const [metrics, setMetrics] = useState<string[]>([]);

    useEffect(
        () => EventsOn("chat:db-changed", () => setDbVersion((v) => v + 1)),
        [],
    );

    // The library is on disk, not in the database, so this runs once. An
    // empty gallery (no strategies/ directory) simply leaves buy-and-hold as
    // the only option rather than breaking the form.
    useEffect(() => {
        let cancelled = false;
        ListStrategies()
            .then((cards) => {
                if (!cancelled) setGallery(cards ?? []);
            })
            .catch(() => {
                if (!cancelled) setGallery([]);
            });
        CriterionMetrics()
            .then((ms) => {
                if (!cancelled) setMetrics(ms ?? []);
            })
            .catch(() => {
                if (!cancelled) setMetrics([]);
            });
        return () => {
            cancelled = true;
        };
    }, []);

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

    // example.lua is the Lua tab's starter and a copy of buy_and_hold, so
    // listing it would offer the same idea twice.
    const pickable = useMemo(
        () => gallery.filter((c) => c.File !== STARTER_SCRIPT),
        [gallery],
    );
    const chosen = useMemo(
        () => pickable.find((c) => c.File === form.strategy) ?? null,
        [pickable, form.strategy],
    );
    const allocationsApply = usesAllocations(form);
    const hypothesisToml = buildHypothesis(form.hypothesis);
    // A claim typed with no threshold is a half-registration: say so rather
    // than running unjudged and letting the user believe it was tested.
    const hypothesisIncomplete =
        form.hypothesis.claim.trim() !== "" && hypothesisToml === "";

    const patchHypothesis = (p: Partial<Form["hypothesis"]>) =>
        patch({ hypothesis: { ...form.hypothesis, ...p } });


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
    // The engine skips every benchmark-relative metric unless the benchmark
    // has a bar for EVERY trading day in the window, and says so only in the
    // run console. Catching it here means the user reads it while the dates
    // are still in front of them. The benchmark series currently end earlier
    // than the priced tickers do, so the end date is the edge that bites.
    const benchmarkEnd = form.benchmark
        ? (ranges[form.benchmark]?.[1] ?? "")
        : "";

    const benchmarkWarning = useMemo(() => {
        // Nothing to warn about before there is anything to run: an empty
        // form should not greet a new user with a date complaint.
        if (form.tickers.length === 0) return "";
        if (!form.benchmark || !rangesLoaded) return "";
        const r = ranges[form.benchmark];
        if (!r) {
            return `${form.benchmark} is not in this database, so Alpha, Beta and the rest of the comparison will be empty.`;
        }
        if (form.startDate && form.startDate < r[0]) {
            return `${form.benchmark} starts ${r[0]} — the comparison needs a start date on or after that, or it is skipped entirely.`;
        }
        if (form.endDate && form.endDate > r[1]) {
            return `${form.benchmark} ends ${r[1]} — move the end date on or before that, or the comparison is skipped entirely.`;
        }
        return "";
    }, [
        form.benchmark,
        form.startDate,
        form.endDate,
        form.tickers,
        ranges,
        rangesLoaded,
    ]);

    const canRun =
        form.tickers.length > 0 &&
        !!dbPath &&
        !running &&
        // A library strategy sizes its own positions, so an all-zero
        // allocation table is not a reason to refuse the run.
        (!allocationsApply || total > 0) &&
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

            {/* Above the run button on purpose: a criterion is a
                pre-registration, and one written after seeing a result is not
                one. Advanced mode places its raw box the same way. */}
            <section className="form-card wide">
                <h3 className="form-card-title">
                    Before you run <span className="optional">(optional)</span>
                </h3>
                <label className="strategy-pick">
                    What do you think is true?
                    <input
                        placeholder="e.g. momentum beats buying and holding these names"
                        value={form.hypothesis.claim}
                        onChange={(e) => patchHypothesis({ claim: e.target.value })}
                    />
                </label>

                <div className="criterion-row">
                    <span className="criterion-lead">Call it a success if</span>
                    <select
                        value={form.hypothesis.metric}
                        onChange={(e) => patchHypothesis({ metric: e.target.value })}
                    >
                        {metrics.map((m) => (
                            <option key={m} value={m}>
                                {criterionLabel(m)}
                            </option>
                        ))}
                    </select>
                    <select
                        value={form.hypothesis.op}
                        onChange={(e) => patchHypothesis({ op: e.target.value })}
                    >
                        {CRITERION_OPS.map((o) => (
                            <option key={o} value={o}>
                                {o}
                            </option>
                        ))}
                    </select>
                    <input
                        className="criterion-value"
                        inputMode="decimal"
                        placeholder="1.2"
                        value={form.hypothesis.value}
                        onChange={(e) => patchHypothesis({ value: e.target.value })}
                    />
                </div>

                {hypothesisIncomplete ? (
                    <div className="coverage-msg err">
                        Add a number to finish the criterion, or clear the claim.
                        As it stands the run will not be judged.
                    </div>
                ) : (
                    <div className="hint">
                        {hypothesisToml
                            ? "Registered before the run and judged mechanically afterwards. Change the question and it becomes a new one — never edit it after seeing a result."
                            : "Leave this empty to run without a verdict. Writing it down first is what stops a result being explained after the fact."}
                    </div>
                )}
            </section>

            <div className="field-row actions">
                <button
                    className="run"
                    disabled={!canRun}
                    onClick={() => onRun(buildToml(form), hypothesisToml)}
                >
                    {running ? "Running…" : "Run backtest"}
                </button>
                <button
                    disabled={form.tickers.length === 0}
                    onClick={() => onEditAsToml(buildToml(form), hypothesisToml)}
                >
                    Edit as TOML →
                </button>
            </div>
        </div>
    );
}
