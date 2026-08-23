/**
 * Smoke render. Not a unit test suite -- the project has no test runner (T87)
 * -- but the one thing that was missing entirely: code under `src/` that
 * actually EXECUTES during the gate.
 *
 * `tsc` type-checks components and `vite build` bundles them; neither has ever
 * mounted one. That is how T90 shipped -- a hook below an early return is
 * well-typed, bundles cleanly, and unmounts the whole app on the first run.
 *
 * Every fixture here is generated from the real Go structs by
 * `ui/fixtures_test.go`, so it cannot drift from the `json` tags. The
 * degenerate ones carry their weight: `plain` is the ordinary run where
 * walk-forward, splits, trades and the benchmark all arrive as null.
 *
 * Run with `npm run smoke`, or let `go test ./ui` do it.
 */
import { renderToString } from "react-dom/server";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import ResultsView from "./ResultsView";
import ConsolePanel from "./ConsolePanel";
import SimpleFormView from "./SimpleForm";
import { buildToml, buildHypothesis } from "./buildToml";
import { searchTickers } from "./tickerSearch";
import { duplicateNames, portfolioRunnable } from "./portfolioCheck";
import { readStored, readStoredNumber, readStoredObject, writeStored } from "./storage";
import type { SimpleForm, HypothesisForm } from "./buildToml";
import { backtest, main } from "../wailsjs/go/models";

// Set by scripts/smoke.mjs. The bundle it builds does not live next to this
// file, so the path cannot be derived from import.meta.url.
const testdata = process.env.SMOKE_TESTDATA ?? "testdata";

const failures: string[] = [];

function check(what: string, fn: () => void) {
    try {
        fn();
        console.log(`  ok    ${what}`);
    } catch (e) {
        const err = e as Error;
        failures.push(`${what}\n${err.stack ?? err.message}`);
        console.log(`  FAIL  ${what}`);
    }
}

function load(name: string) {
    const out = JSON.parse(readFileSync(join(testdata, `${name}.json`), "utf8"));
    return {
        results: (out.results ?? []).map((r: unknown) => main.RunResult.createFrom(r)),
        verdicts: (out.verdicts ?? []).map((v: unknown) =>
            backtest.VerdictReport.createFrom(v),
        ),
        diagnostics: (out.diagnostics ?? []).map((d: unknown) =>
            backtest.Diagnostic.createFrom(d),
        ),
    };
}

const form: SimpleForm = JSON.parse(
    readFileSync(join(testdata, "simple-form.json"), "utf8"),
);
const hypothesis: HypothesisForm = JSON.parse(
    readFileSync(join(testdata, "hypothesis-form.json"), "utf8"),
);

console.log("frontend smoke render");

// RealAnnualReturn uses 0 as "CPI does not cover this window", which is a
// different claim from "real growth was zero". Rendering it as 0.00 would be a
// number the engine never asserted.
check("an unavailable real return renders as a dash, not zero", () => {
    const { results, verdicts } = load("full");
    const unavailable = results.map((r: main.RunResult) =>
        main.RunResult.createFrom({ ...r, realAnnualReturn: 0 }),
    );
    const html = renderToString(
        <ResultsView results={unavailable} fontSize={16} verdicts={verdicts} />,
    );
    // The metrics table always renders, so the cell is definitely in there.
    if (html.includes(">0.00<")) {
        throw new Error("a zero real return rendered as 0.00 rather than an em dash");
    }
});

// Seven of the eleven result panels are COLLAPSIBLE and default to closed
// (T85), and a closed panel renders no body: `{open && children}`. So every
// render check above exercises four panels and silently skips walk-forward,
// splits, drawdowns, trading, attribution, calendar-year and monthly returns
// -- which are most of the table code in the file.
//
// The open/closed set comes from localStorage, so opening them here means
// installing one. Node has none, which is the same reason the storage guards
// exist; a stub is both how these panels get rendered and a check that the
// stored state is actually honoured.
const ALL_PANELS = {
    "walk-forward": true,
    splits: true,
    drawdowns: true,
    trading: true,
    attribution: true,
    yearly: true,
    monthly: true,
};

function withPanelsOpen<T>(fn: () => T): T {
    const store = new Map<string, string>([
        ["backtester-results-panels", JSON.stringify(ALL_PANELS)],
    ]);
    Object.defineProperty(globalThis, "localStorage", {
        value: {
            getItem: (k: string) => store.get(k) ?? null,
            setItem: (k: string, v: string) => {
                store.set(k, v);
            },
        },
        configurable: true,
        writable: true,
    });
    try {
        return fn();
    } finally {
        delete (globalThis as unknown as { localStorage?: unknown }).localStorage;
    }
}

check("every collapsible panel renders when opened", () => {
    const { results, verdicts } = load("full");
    const html = withPanelsOpen(() =>
        renderToString(<ResultsView results={results} fontSize={16} verdicts={verdicts} />),
    );
    // Each panel's own heading proves its body was reached, not just its title.
    for (const marker of [
        "Walk-forward windows",
        "In-sample vs out-of-sample",
        "Deepest drawdowns",
        "Attribution",
        "Calendar year returns",
        "Monthly returns",
    ]) {
        if (!html.includes(marker)) throw new Error(`panel missing: ${marker}`);
    }
    // Closed by default, so the same render without the stub is much smaller.
    const closed = renderToString(
        <ResultsView results={results} fontSize={16} verdicts={verdicts} />,
    );
    if (html.length <= closed.length) {
        throw new Error(
            `opening the panels did not render more (${html.length} vs ${closed.length}) ` +
                "-- the stored panel state is being ignored",
        );
    }
});

// Every row in a table must have exactly as many cells as the table has
// columns. Obvious, unenforceable by tsc, and it silently shipped: the
// benchmark row emitted twelve cells against a seventeen-column header, so max
// drawdown was printed under CALMAR, annual return under MAX DD %, and std dev
// under UNDERWATER. Every number in that row was real and every label was
// wrong, which is worse than a blank row.
function tableCellCounts(html: string): { headers: number; rows: number[] }[] {
    const tables: { headers: number; rows: number[] }[] = [];
    for (const table of html.split("<table").slice(1)) {
        const headers = (table.match(/<th[\s>]/g) ?? []).length;
        const rows: number[] = [];
        // Body rows only: the header row's <th>s are counted separately.
        for (const row of table.split("<tr").slice(1)) {
            const cells = (row.match(/<td[\s>]/g) ?? []).length;
            if (cells > 0) rows.push(cells);
        }
        tables.push({ headers, rows });
    }
    return tables;
}

// Across every fixture, because the metrics table's column count is
// conditional: the sweep columns and the benchmark columns each appear only
// when some result carries them, and a row that is right in one combination
// can be wrong in another.
for (const name of ["full", "plain", "no-curve"]) {
    check(`every table row matches its header — ${name}`, () => {
        const { results, verdicts } = load(name);
        const html = withPanelsOpen(() =>
            renderToString(<ResultsView results={results} fontSize={16} verdicts={verdicts} />),
        );
        const tables = tableCellCounts(html);
        if (tables.length === 0) throw new Error("no tables rendered");
        tables.forEach((t, i) => {
            for (const cells of t.rows) {
                if (cells !== t.headers) {
                    throw new Error(
                        `table ${i}: a row has ${cells} cells but the header has ` +
                            `${t.headers} columns — every value after the short row's ` +
                            "first gap is printed under the wrong label",
                    );
                }
            }
        });
    });
}

// The seven time.Time fields cross the wire as `any` -- wails has no
// TypeScript mapping for them and says so on every binding regeneration (T96).
// `any` means tsc cannot catch a raw RFC3339 string being rendered, so assert
// it at runtime instead: no "T00:00:00Z" may reach the DOM. The fixture dates
// are all midnight UTC, which makes them a reliable canary.
check("no raw timestamp reaches the DOM", () => {
    const { results, verdicts } = load("full");
    // Opened, because the drawdowns table -- where these dates are rendered --
    // is one of the panels that is closed by default.
    const html = withPanelsOpen(() =>
        renderToString(<ResultsView results={results} fontSize={16} verdicts={verdicts} />),
    );
    if (html.includes("T00:00:00Z")) {
        const at = html.indexOf("T00:00:00Z");
        throw new Error(
            "a time.Time field was rendered raw instead of through fmtDate: ..." +
                html.slice(Math.max(0, at - 120), at + 20),
        );
    }
});

for (const name of ["full", "plain", "no-curve", "empty"]) {
    const { results, verdicts, diagnostics } = load(name);
    // Both zoom extremes: the chart lays itself out in real pixels scaled by
    // fontSize, and a divide-by-zero there would only show at one end.
    for (const fontSize of [12, 22]) {
        check(`ResultsView / ${name} @ ${fontSize}px`, () => {
            renderToString(
                <ResultsView results={results} fontSize={fontSize} verdicts={verdicts} />,
            );
        });
    }
    check(`ConsolePanel / ${name}`, () => {
        renderToString(<ConsolePanel diagnostics={diagnostics} fontSize={16} />);
    });
}

// The Backtest tab itself. Its effects (ListTickers, TickerNames, the gallery)
// do not run under SSR, so this renders the no-database state -- which is also
// the state a new user opens the app in, and the one where every optional
// lookup is empty. A component that throws on empty data is the failure mode
// this catches.
const noop = () => {};
const emptyHypothesis: HypothesisForm = { claim: "", metric: "sharpe", op: ">=", value: "" };
for (const [label, fs] of [
    ["one empty portfolio", [{ ...form, tickers: [], allocations: {} }]],
    ["one populated portfolio", [form]],
    // The multi-portfolio path: a second card, and a Remove button that only
    // exists once there is more than one.
    ["three portfolios", [form, { ...form, name: "B" }, { ...form, name: "C" }]],
    // Two blocks sharing a name is the state the form must refuse to run.
    ["duplicate names", [form, { ...form }]],
] as [string, SimpleForm[]][]) {
    check(`SimpleForm / ${label}`, () => {
        renderToString(
            <SimpleFormView
                dbPath=""
                running={false}
                onOpenAssistant={noop}
                forms={fs}
                onChange={noop}
                hypothesis={emptyHypothesis}
                onHypothesisChange={noop}
                onRun={noop}
                onEditAsToml={noop}
            />,
        );
    });
}

// buildToml is what turns the Backtest tab into a config, so a change here
// silently changes every run started from the form. Pure function, no DOM --
// the cheapest thing in the app to pin down, and it had no cover at all.
check("buildToml matches the golden config", () => {
    const want = readFileSync(join(testdata, "golden.toml"), "utf8");
    const got = buildToml([form]);
    if (got !== want) {
        throw new Error(
            "generated TOML changed. If that was intended, refresh " +
                "ui/frontend/testdata/golden.toml -- but a config generated by the " +
                "form is the input to every run started from it, so read the diff " +
                "first:\n\n--- want\n" +
                want +
                "\n--- got\n" +
                got,
        );
    }
});

// One portfolio must still emit byte-for-byte what it emitted before the tab
// could hold several. golden.toml predates the multi-portfolio work and is not
// regenerated with it, so this is a real guarantee rather than a restatement.
check("buildToml emits one block per portfolio", () => {
    const want = readFileSync(join(testdata, "golden-multi.toml"), "utf8");
    const second: SimpleForm = {
        ...form,
        name: "Momentum",
        tickers: ["NVDA", "AMD"],
        evenSplit: true,
        allocations: {},
        strategy: "momentum.lua",
    };
    const got = buildToml([form, second]);
    if (got !== want) {
        throw new Error(
            "multi-portfolio TOML changed:\n\n--- want\n" + want + "\n--- got\n" + got,
        );
    }
    // The single-portfolio config is a prefix of nothing and a special case of
    // nothing: it is just the same builder with a one-element list.
    if (!buildToml([form, second]).includes("[[Portfolio]]")) {
        throw new Error("no portfolio blocks emitted");
    }
    const blocks = (got.match(/\[\[Portfolio\]\]/g) ?? []).length;
    if (blocks !== 2) throw new Error(`expected 2 [[Portfolio]] blocks, got ${blocks}`);
});

check("buildHypothesis matches the golden hypothesis", () => {
    const want = readFileSync(join(testdata, "golden-hypothesis.toml"), "utf8");
    // The ID is generated fresh per registration -- that is the point of it --
    // so it is the one line that cannot be snapshotted.
    const got = buildHypothesis(hypothesis).replace(
        /^ID(\s*)= ".*"$/m,
        'ID$1= "H-<generated>"',
    );
    if (got !== want) {
        throw new Error(
            "generated hypothesis changed:\n\n--- want\n" + want + "\n--- got\n" + got,
        );
    }
});

// Persisted UI state. Node has no working localStorage, which makes this the
// exact environment the guards exist for: a browser told to block site data
// throws from the accessor, and two of these reads run in the ROOT component's
// useState initializer, where a throw is a blank window rather than a lost
// preference.
check("reading persisted state never throws without localStorage", () => {
    eq("readStored", readStored("anything"), null);
    eq("readStoredObject", readStoredObject("anything"), {});
    eq("readStoredNumber", readStoredNumber("anything", 16, (n) => n > 0), 16);
    // Writing must be just as quiet.
    writeStored("anything", "value");
});

check("readStoredNumber rejects what Number() would accept", () => {
    // Number(null) is 0 and Number("") is 0, so a missing key has to be
    // rejected before the numeric check rather than by it.
    eq("missing", readStoredNumber("nope", 16, (n) => n >= 10), 16);
});

// Multi-portfolio gating. A run carries every portfolio, so one bad card has
// to stop the whole run rather than quietly contributing a broken block.
const RANGES = { AAPL: ["2000-01-03", "2026-08-07"], MSFT: ["2000-01-03", "2026-08-07"] };

check("a portfolio with no holdings is not runnable", () => {
    eq("empty", portfolioRunnable({ ...form, tickers: [], allocations: {} }, RANGES), false);
});

check("a portfolio outside its data window is not runnable", () => {
    // evenSplit still carries real percentages -- addTicker fills them -- so
    // an all-zero allocation table means "no weights", not "equal weights".
    const ok = { ...form, tickers: ["AAPL"], evenSplit: true, allocations: { AAPL: 100 },
        startDate: "2010-01-01", endDate: "2020-01-01" };
    eq("inside", portfolioRunnable(ok, RANGES), true);
    // Starts before AAPL has bars.
    eq("before first bar", portfolioRunnable({ ...ok, startDate: "1990-01-01" }, RANGES), false);
    // Ends after the last bar.
    eq("past last bar", portfolioRunnable({ ...ok, endDate: "2030-01-01" }, RANGES), false);
    // A symbol the database has never heard of.
    eq("unknown symbol", portfolioRunnable({ ...ok, tickers: ["NOPE"] }, RANGES), false);
    // A library strategy sizes its own positions, so a zero allocation table
    // is not a reason to refuse it -- only the buy-and-hold path reads them.
    eq("no weights, buy and hold", portfolioRunnable({ ...ok, allocations: {} }, RANGES), false);
    eq("no weights, library strategy",
        portfolioRunnable({ ...ok, allocations: {}, strategy: "momentum.lua" }, RANGES), true);
});

check("duplicate portfolio names are reported", () => {
    eq("none", duplicateNames([form, { ...form, name: "B" }]), []);
    eq("one pair", duplicateNames([form, { ...form }]), [form.name]);
    // Reported once however many times it repeats, and blanks are not names.
    eq("triple", duplicateNames([form, { ...form }, { ...form }]), [form.name]);
    eq("blank", duplicateNames([{ ...form, name: "" }, { ...form, name: "  " }]), []);
});

// The ticker picker's ordering. This is the whole of what the company-name
// feature adds, and no render test can see it.
const POOL = ["AA", "AAPL", "AMD", "APLE", "MSFT", "$SP500"];
const NAMES: Record<string, string> = {
    AA: "Alcoa Corp",
    AAPL: "Apple Inc.",
    AMD: "Advanced Micro Devices, Inc.",
    APLE: "Apple Hospitality REIT, Inc.",
    MSFT: "MICROSOFT CORP",
    $SP500: "S&P 500",
};

function eq(what: string, got: unknown, want: unknown) {
    const g = JSON.stringify(got);
    const w = JSON.stringify(want);
    if (g !== w) throw new Error(`${what}: got ${g}, want ${w}`);
}

check("ticker search ranks symbols before names", () => {
    // AA is exact, AAPL is a symbol prefix; Apple Inc. and Apple Hospitality
    // only match on the name and must come after both.
    eq("AA", searchTickers(POOL, NAMES, "AA"), ["AA", "AAPL"]);
});

check("ticker search finds a symbol by company name", () => {
    // The point of the feature: a user who knows "apple" but not "AAPL".
    eq("apple", searchTickers(POOL, NAMES, "apple"), ["AAPL", "APLE"]);
    // Case-folded on both sides -- profile names are inconsistently cased.
    eq("microsoft", searchTickers(POOL, NAMES, "microsoft"), ["MSFT"]);
});

check("ticker search survives a missing name table", () => {
    // No company_profile in the database: symbols still match, nothing throws.
    eq("AA/no names", searchTickers(POOL, {}, "AA"), ["AA", "AAPL"]);
    eq("apple/no names", searchTickers(POOL, {}, "apple"), []);
});

check("ticker search caps and does not treat empty as no matches", () => {
    const many = Array.from({ length: 500 }, (_, i) => `T${i}`);
    eq("cap", searchTickers(many, {}, "T").length, 8);
    eq("empty query", searchTickers(POOL, NAMES, "  "), POOL);
});

if (process.env.SMOKE_UPDATE === "1") {
    // golden.toml is deliberately NOT rewritten here -- it is the pre-multi
    // output and refreshing it would erase the guarantee it exists to hold.
    writeFileSync(
        join(testdata, "golden-multi.toml"),
        buildToml([
            form,
            {
                ...form,
                name: "Momentum",
                tickers: ["NVDA", "AMD"],
                evenSplit: true,
                allocations: {},
                strategy: "momentum.lua",
            },
        ]),
    );
    writeFileSync(
        join(testdata, "golden-hypothesis.toml"),
        buildHypothesis(hypothesis).replace(/^ID(\s*)= ".*"$/m, 'ID$1= "H-<generated>"'),
    );
    console.log("\ngoldens refreshed");
    process.exit(0);
}

if (failures.length > 0) {
    console.error(`\n${failures.length} smoke failure(s):\n`);
    for (const f of failures) console.error(`${f}\n`);
    process.exit(1);
}
console.log("\nall clear");
