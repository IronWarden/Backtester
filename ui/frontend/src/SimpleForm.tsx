import { useEffect, useMemo, useState } from "react";
import { keysFor } from "./shortcuts";
import {
    ListStrategies,
    ListTickers,
    TickerNames,
    TickerDateRanges,
    CriterionMetrics,
    ReadRules,
    RulesMenu,
} from "../wailsjs/go/main/App";
import { EventsOn } from "../wailsjs/runtime/runtime";
import { backtest, main } from "../wailsjs/go/models";
import {
    CRITERION_OPS,
    STARTER_SCRIPT,
    buildHypothesis,
    buildToml,
    criterionLabel,
    usesRules,
} from "./buildToml";
import { readingKey, rulesBlocker, runLabel, toRulesConfig } from "./rules";
import type { SimpleForm as Form, HypothesisForm } from "./buildToml";
import { duplicateNames, isValidDate, maskDecimal, runBlocker } from "./portfolioCheck";
import {
    BLANK,
    CRITERION_PRESETS,
    EXAMPLES,
    INVESTORS,
    buildExample,
    buildInvestor,
    exampleHypothesis,
    investorHypothesis,
} from "./examples";
import PortfolioCard from "./PortfolioCard";
import { readStored, writeStored } from "./storage";

// The Backtest tab.
//
// It holds a LIST of portfolios now. Comparing two configurations is the
// question the engine exists to answer -- the runner has always taken any
// number of [[Portfolio]] blocks and the results view has always drawn a line
// per portfolio -- and until now the form emitted exactly one, so the only way
// to compare anything was to hand-write TOML. That is a strange thing to
// require of the tab a new user opens the app on.
//
// This file owns what is shared across portfolios: the symbol universe and its
// names, the date ranges, the strategy gallery, the criterion vocabulary, the
// pre-registered hypothesis, and the run itself. PortfolioCard owns everything
// that is per-portfolio. The shared data is fetched once here rather than per
// card, because N cards must not mean N copies of the same query.
//
// The hypothesis is deliberately NOT per-portfolio: the engine registers one
// claim per RUN and judges every result against it, so a box per card would be
// a UI inventing a concept the engine does not have.

type Props = {
    dbPath: string;
    running: boolean;
    // Opens the assistant panel. A new user has no way to know it can write
    // the whole config from one sentence, which makes it the fastest path
    // through this form and the least discoverable thing in the app.
    onOpenAssistant: () => void;
    // Form data is owned by App so it persists across runs and mode switches.
    forms: Form[];
    onChange: (next: Form[]) => void;
    hypothesis: HypothesisForm;
    onHypothesisChange: (next: HypothesisForm) => void;
    // Persist + run the generated config (App owns the run pipeline). The
    // hypothesis rides along rather than living in App's raw box: registering
    // it is part of starting THIS run, not a mode-wide setting.
    onRun: (toml: string, hypothesis: string) => void;
    // Hand the generated config off to the Config tab's TOML editor, with the
    // registered question, so switching modes does not quietly drop it.
    onEditAsToml: (toml: string, hypothesis: string) => void;
    // Opens a picked strategy's script in the Lua tab. Optional so a host
    // without an editor (the smoke harness) can leave it out.
    onViewScript?: (strategy: string) => void;
    // "Describe my own idea…" on card i: App opens the assistant seeded with
    // that card's holdings and window, and routes proposed rules back to it.
    onDescribeIdea?: (index: number) => void;
};


// The first run explains itself, once. Named for the workflow rather than the
// UI: a backtest is a question you ask, a run you make, and a verdict you read
// — and the app previously opened on a form that said none of that.
//
// Dismissed for good on click. NOT a modal: it sits above the form, blocks
// nothing, and never reappears once the user has said they have it.
const ONBOARDED_KEY = "backtester-onboarded";

function readOnboarded(): boolean {
    return readStored(ONBOARDED_KEY) === "1";
}

function FirstRun({
    onOpenAssistant,
    onDismiss,
}: {
    onOpenAssistant: () => void;
    onDismiss: () => void;
}) {
    return (
        <section className="form-card wide first-run">
            <h3 className="form-card-title">
                Three steps, and the third is the one that matters
            </h3>
            <ol className="first-run-steps">
                <li>
                    <b>Ask something.</b> Add the holdings you want to trade and
                    pick a strategy. Each one says what it works on and how it
                    fails before you run it.
                </li>
                <li>
                    <b>Test it.</b> Write down what would count as success —
                    "Sharpe at least 1.2" — then hit Run. Writing it first is
                    what stops a result being explained after the fact.
                </li>
                <li>
                    <b>See if it held.</b> The verdict at the top of the results
                    answers the question you registered, mechanically, against
                    the market and against plain buy-and-hold.
                </li>
            </ol>
            <p className="first-run-aside">
                In a hurry? The assistant can write the whole thing from one
                sentence — "test momentum on the big five tech names since
                2015" — and check the data covers it.
            </p>
            <div className="first-run-actions">
                <button className="primary" onClick={onOpenAssistant}>
                    ✦ Ask the assistant
                </button>
                <button className="ghost" onClick={onDismiss}>
                    Got it
                </button>
            </div>
        </section>
    );
}

// Starter examples.
//
// The form asks for holdings, two dates, a strategy, an allocation and a
// success criterion before anything has happened, and a beginner has no basis
// for any of them. One click here answers all five with a run that works, so
// the first act is editing something rather than inventing it.
//
// Shown only while no portfolio has holdings, so it is the empty form's
// content rather than a permanent banner -- and it comes back if the user
// clears everything, which is exactly when they want it again.
function Examples({
    ranges,
    onPick,
}: {
    ranges: Record<string, string[]>;
    onPick: (id: string) => void;
}) {
    // Built against the loaded database: an example whose symbols are missing,
    // or whose window cannot be made to work, is withdrawn rather than shown
    // broken. A starter example that opens with a red date error is worse than
    // no starter example.
    const benchmark = ranges["$SP500"] ? "$SP500" : "";
    const usable = EXAMPLES.filter(
        (ex) => buildExample(ex, ranges, benchmark, BLANK) !== null,
    );
    if (usable.length === 0) return null;

    return (
        <section className="form-card wide examples">
            <h3 className="form-card-title">Start from a question</h3>
            <p className="examples-lead">
                Each one fills the form with a run that works on this database.
                Change anything you like before hitting Run — that is the point
                of them.
            </p>
            <div className="example-list">
                {usable.map((ex) => (
                    <button
                        key={ex.id}
                        className="example"
                        onClick={() => onPick(ex.id)}
                    >
                        <span className="example-label">{ex.label}</span>
                        <span className="example-question">{ex.question}</span>
                    </button>
                ))}
            </div>
        </section>
    );
}

// Famous investors' portfolios, on the same empty-form footing as the examples
// above: one click fills a runnable portfolio. Each shows its caveat on the
// button, not in a tooltip -- "what this is NOT" is the part of copying a
// legend that a user most needs and is least likely to hover for.
export function Investors({
    ranges,
    onPick,
}: {
    ranges: Record<string, string[]>;
    onPick: (id: string) => void;
}) {
    const benchmark = ranges["$SP500"] ? "$SP500" : "";
    const usable = INVESTORS.filter(
        (inv) => buildInvestor(inv, ranges, benchmark, BLANK) !== null,
    );
    if (usable.length === 0) return null;

    return (
        <section className="form-card wide examples investors">
            <h3 className="form-card-title">Or copy a famous investor</h3>
            <p className="examples-lead">
                Each is something you could actually have bought on the first
                day of the window — a published recipe, or a public filing
                bought the day after it came out — never the current holdings run
                backwards.
            </p>
            <div className="example-list">
                {usable.map((inv) => (
                    <button
                        key={inv.id}
                        className="example"
                        onClick={() => onPick(inv.id)}
                    >
                        <span className="example-investor">{inv.investor}</span>
                        <span className="example-label">{inv.name}</span>
                        <span className="example-question">{inv.note}</span>
                        <span className="example-caveat">{inv.caveat}</span>
                    </button>
                ))}
            </div>
        </section>
    );
}

// A new portfolio starts as a copy of the last one's window and capital -- two
// portfolios over different date ranges are not a comparison -- but with no
// holdings and a name that does not collide.
function nextPortfolio(forms: Form[]): Form {
    const last = forms[forms.length - 1];
    const used = new Set(forms.map((f) => f.name.trim()));
    let n = forms.length + 1;
    while (used.has(`Portfolio ${n}`)) n++;
    return {
        ...last,
        name: `Portfolio ${n}`,
        tickers: [],
        allocations: {},
        evenSplit: true,
    };
}

export default function SimpleForm({
    dbPath,
    running,
    onOpenAssistant,
    forms,
    onChange,
    hypothesis,
    onHypothesisChange,
    onRun,
    onEditAsToml,
    onViewScript,
    onDescribeIdea,
}: Props) {
    const [available, setAvailable] = useState<string[]>([]);
    // {"AAPL": "Apple Inc."} for every symbol we can name. Partial by nature
    // -- see App.TickerNames -- so every read of it falls back to the symbol.
    const [names, setNames] = useState<Record<string, string>>({});
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
    const [onboarded, setOnboarded] = useState(readOnboarded);
    // The engine's rules menu, and its readings of rule sets, keyed by
    // everything ReadRules was asked -- so an edit asks once and switching
    // back to an earlier state is instant.
    const [rulesMenu, setRulesMenu] = useState<backtest.RuleEntry[]>([]);
    const [readings, setReadings] = useState<Record<string, main.RulesReading>>({});

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
        RulesMenu()
            .then((m) => {
                if (!cancelled) setRulesMenu(m ?? []);
            })
            .catch(() => {
                if (!cancelled) setRulesMenu([]);
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

    // Pull the real ticker universe, each ticker's available date range, and
    // the company names from the chosen DB. Once, for every card.
    useEffect(() => {
        if (!dbPath) {
            setAvailable([]);
            setRanges({});
            setNames({});
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
        // Optional: a database with no company_profile table simply leaves
        // this empty and the picker shows bare symbols, as it always did.
        TickerNames(dbPath)
            .then((n) => {
                if (!cancelled) setNames(n ?? {});
            })
            .catch(() => {
                if (!cancelled) setNames({});
            });
        return () => {
            cancelled = true;
        };
    }, [dbPath, dbVersion]);

    // my_strategy.lua is the Lua tab's scratch file, not a strategy to offer.
    const pickable = useMemo(
        () => gallery.filter((c) => c.File !== STARTER_SCRIPT),
        [gallery],
    );

    // A preset naming a metric this engine does not serve would produce a
    // criterion the run refuses, so the vocabulary check happens here rather
    // than in the click handler.
    const presets = useMemo(
        () => CRITERION_PRESETS.filter((p) => metrics.includes(p.metric)),
        [metrics],
    );

    const hypothesisToml = buildHypothesis(hypothesis);
    // A claim typed with no threshold is a half-registration: say so rather
    // than running unjudged and letting the user believe it was tested.
    const hypothesisIncomplete =
        hypothesis.claim.trim() !== "" && hypothesisToml === "";
    const patchHypothesis = (p: Partial<HypothesisForm>) =>
        onHypothesisChange({ ...hypothesis, ...p });

    // Two blocks sharing a name are two rows nobody can tell apart in the
    // results table, the blotter or a verdict. Refuse rather than explain.
    const dupes = useMemo(() => duplicateNames(forms), [forms]);

    // Every portfolio has to be runnable: a run carries all of them, so one
    // broken card stops the run rather than quietly contributing a bad block.
    // runBlocker says WHY when it cannot, and the reason is shown beside the
    // button: a disabled button with no explanation reads as broken.
    // What to ask the engine about each card that runs rules. Dates go along
    // only once both are real dates; until then the rules alone are read.
    const rulesAsks = useMemo(
        () =>
            forms.map((f) => {
                if (!usesRules(f)) return null;
                const rc = toRulesConfig(f.rules!, f.allocations, f.tickers);
                const dated = isValidDate(f.startDate) && isValidDate(f.endDate);
                const start = dated ? f.startDate : "";
                const end = dated ? f.endDate : "";
                return {
                    key: readingKey(rc, f.tickers, start, end, f.benchmark),
                    rc,
                    tickers: f.tickers,
                    start,
                    end,
                    benchmark: f.benchmark,
                };
            }),
        [forms],
    );
    useEffect(() => {
        for (const ask of rulesAsks) {
            if (!ask || readings[ask.key]) continue;
            // Keyed by the question, so a late answer to an old question
            // lands under its own key and is simply never read.
            ReadRules(ask.rc, ask.tickers, ask.start, ask.end, ask.benchmark)
                .then((r) => setReadings((m) => ({ ...m, [ask.key]: r })))
                .catch((e) =>
                    setReadings((m) => ({
                        ...m,
                        [ask.key]: { sentence: "", headline: "", error: String(e) },
                    })),
                );
        }
    }, [rulesAsks, readings]);
    const cardReadings = rulesAsks.map((ask) =>
        ask ? (readings[ask.key] ?? null) : undefined,
    );

    const blocker =
        runBlocker(forms, ranges, dbPath, running) || rulesBlocker(forms, cardReadings);
    const canRun = blocker === "";

    const anyTickers = forms.some((f) => f.tickers.length > 0);

    function patchAt(i: number, next: Form) {
        onChange(forms.map((f, j) => (j === i ? next : f)));
    }

    function removeAt(i: number) {
        onChange(forms.filter((_, j) => j !== i));
    }

    // Replace the (empty) form with the example, and register its question as
    // the hypothesis: the discipline the app is built around is worth nothing
    // if the first run silently skips it. Safe to replace outright because the
    // examples are only offered while no portfolio holds anything.
    function applyExample(id: string) {
        const ex = EXAMPLES.find((e) => e.id === id);
        if (!ex) return;
        const benchmark = ranges["$SP500"] ? "$SP500" : "";
        const built = buildExample(ex, ranges, benchmark, forms[0] ?? BLANK);
        if (!built) return;
        onChange([built]);
        onHypothesisChange(exampleHypothesis(ex));
    }

    // Same footing as applyExample: only offered on an empty form.
    function applyInvestor(id: string) {
        const inv = INVESTORS.find((e) => e.id === id);
        if (!inv) return;
        const benchmark = ranges["$SP500"] ? "$SP500" : "";
        const built = buildInvestor(inv, ranges, benchmark, forms[0] ?? BLANK);
        if (!built) return;
        onChange([built]);
        onHypothesisChange(investorHypothesis(inv));
    }

    function dismissFirstRun() {
        setOnboarded(true);
        // Storage refused: the card reappears next launch, which is a better
        // failure than the form refusing to render.
        writeStored(ONBOARDED_KEY, "1");
    }

    return (
        <div className="simple-form">
            {!onboarded && (
                <FirstRun
                    onOpenAssistant={() => {
                        onOpenAssistant();
                        dismissFirstRun();
                    }}
                    onDismiss={dismissFirstRun}
                />
            )}

            {!anyTickers && (
                <Examples ranges={ranges} onPick={applyExample} />
            )}
            {!anyTickers && (
                <Investors ranges={ranges} onPick={applyInvestor} />
            )}

            {forms.map((f, i) => (
                <PortfolioCard
                    key={i}
                    form={f}
                    onChange={(next) => patchAt(i, next)}
                    // The last portfolio cannot be removed: a run with nothing
                    // in it is not a state the form should reach by clicking.
                    onRemove={forms.length > 1 ? () => removeAt(i) : null}
                    otherNames={forms.filter((_, j) => j !== i).map((o) => o.name)}
                    defaultName={`Portfolio ${i + 1}`}
                    dbPath={dbPath}
                    available={available}
                    names={names}
                    ranges={ranges}
                    // Only once something is in the form: an untouched form
                    // gets the starter examples above instead.
                    showBaskets={anyTickers}
                    pickable={pickable}
                    onViewScript={onViewScript}
                    rulesMenu={rulesMenu}
                    rulesReading={cardReadings[i] ?? null}
                    onDescribeIdea={onDescribeIdea ? () => onDescribeIdea(i) : undefined}
                />
            ))}

            <div className="field-row add-portfolio">
                <button type="button" onClick={() => onChange([...forms, nextPortfolio(forms)])}>
                    + Add portfolio
                </button>
                <span className="hint">
                    {forms.length === 1
                        ? "A second portfolio runs beside this one, on the same chart and in the same table — which is what makes a result a comparison rather than a number."
                        : `${forms.length} portfolios run together in one backtest.`}
                </span>
            </div>

            {dupes.length > 0 && (
                <div className="coverage-msg err">
                    Two portfolios are both called {dupes.map((d) => `"${d}"`).join(", ")}.
                    The name is how every result, trade and verdict is labelled — give
                    them different ones.
                </div>
            )}

            {/* Above the run button on purpose: a criterion is a
                pre-registration, and one written after seeing a result is not
                one. The Config tab places its raw box the same way. */}
            <section className="form-card wide">
                <h3 className="form-card-title">
                    Before you run <span className="optional">(optional)</span>
                </h3>
                <label className="strategy-pick">
                    What do you think is true?
                    <input
                        placeholder="e.g. momentum beats buying and holding these names"
                        value={hypothesis.claim}
                        onChange={(e) => patchHypothesis({ claim: e.target.value })}
                    />
                </label>

                <div className="criterion-row">
                    <span className="criterion-lead">Call it a success if</span>
                    <select
                        value={hypothesis.metric}
                        onChange={(e) => patchHypothesis({ metric: e.target.value })}
                    >
                        {metrics.map((m) => (
                            <option key={m} value={m}>
                                {criterionLabel(m)}
                            </option>
                        ))}
                    </select>
                    <select
                        value={hypothesis.op}
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
                        value={hypothesis.value}
                        maxLength={16}
                        onChange={(e) => patchHypothesis({ value: maskDecimal(e.target.value) })}
                    />
                </div>

                {/* The presets fill the controls above rather than replacing
                    them, so the vocabulary is visible after one click instead
                    of hidden behind a friendly label. Filtered against the
                    metric list the ENGINE serves, so the form cannot offer a
                    criterion ParseCriterion would reject. */}
                {presets.length > 0 && (
                    <div className="criterion-presets">
                        <span className="presets-label">Or start from</span>
                        {presets.map((pre) => (
                            <button
                                key={pre.id}
                                className="preset-chip"
                                onClick={() =>
                                    patchHypothesis({
                                        // Never overwrite a claim the user
                                        // wrote in their own words.
                                        claim: hypothesis.claim.trim()
                                            ? hypothesis.claim
                                            : pre.claim,
                                        metric: pre.metric,
                                        op: pre.op,
                                        value: pre.value,
                                    })
                                }
                            >
                                {pre.label}
                            </button>
                        ))}
                    </div>
                )}

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
                    data-shortcut="run"
                    disabled={!canRun}
                    data-blocker={blocker || undefined}
                    onClick={() => onRun(buildToml(forms), hypothesisToml)}
                    title={`Run the backtest (${keysFor("run")})`}
                >
                    {running ? "Running…" : runLabel(cardReadings)}
                </button>
                <button
                    disabled={!anyTickers}
                    onClick={() => onEditAsToml(buildToml(forms), hypothesisToml)}
                >
                    Edit as TOML →
                </button>
                {/* Not while running: the button already says so. And not on a
                    wholly empty form, where the starter examples above are
                    the answer and "add a holding" would only nag. */}
                {!canRun && !running && anyTickers && (
                    <span className="hint run-blocker">To run, {blocker}.</span>
                )}
            </div>
        </div>
    );
}
