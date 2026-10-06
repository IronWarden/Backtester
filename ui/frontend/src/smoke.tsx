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

import ResultsView, { benchmarkLines, differingWindows } from "./ResultsView";
import SimpleFormView, { Examples, Investors } from "./SimpleForm";
import PortfolioCard from "./PortfolioCard";
import {
    addBuffer,
    copyPath,
    copyText,
    isDirty,
    markSaved,
    setBufferText,
} from "./luaBuffers";
import type { LuaBuffer } from "./luaBuffers";
import ChatPanel, { ChatMessage } from "./ChatPanel";
import { CRITERION_OPS, buildToml, buildHypothesis } from "./buildToml";
import { searchTickers } from "./tickerSearch";
import { applyImport, importSummary } from "./holdingsImport";
import type { HoldingsImport } from "./holdingsImport";
import {
    dateBounds,
    duplicateNames,
    maskDate,
    maskDecimal,
    normalizeDates,
    portfolioRunnable,
    portfolioBlocker,
    resolveTicker,
    runBlocker,
    tickerFilter,
    uniqueName,
} from "./portfolioCheck";
import {
    BASKETS,
    BLANK,
    CRITERION_PRESETS,
    EXAMPLES,
    INVESTORS,
    basketTickers,
    buildExample,
    buildInvestor,
    exampleHypothesis,
    investorHypothesis,
    percentAllocations,
} from "./examples";
import { chipLabel } from "./paths";
import { perFrame, trackDrag } from "./frame";
import Pane, { skipRender } from "./Pane";
import ResultsBar, { cycleResultsLayout, nextResultsLayout, parseResultsLayout } from "./ResultsBar";
import { SHORTCUTS, ShortcutsHelp, displayKeys, keysFor, shortcutFor, typingInField, zoomFor } from "./shortcuts";
import { CommandPalette, filterCommands } from "./commandPalette";
import RunTimer, { formatElapsed } from "./RunTimer";
import type { Command } from "./commandPalette";
import { ResultPanel, DockProvider, clampGeo, defaultGeo, readFloats } from "./ResultPanel";
import type { Geo } from "./ResultPanel";
import {
    argsOf,
    detailFor,
    documentation,
    findGlobal,
    luaSuggestions,
    snippetFor,
} from "./luaApi";
import { parseSession } from "./session";
import RulesCard from "./RulesCard";
import {
    DEFAULT_RULES,
    RULES_STRATEGY,
    argsOf as ruleArgs,
    entriesOf,
    formatRule,
    fromRulesConfig,
    hasRulesBlock,
    parseRule,
    rulesBlocker,
    runLabel,
    settleCounts,
    toRulesConfig,
} from "./rules";
import type { RulesForm } from "./rules";
import type { Session } from "./session";
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

// The same form with a schedule on it. Shared by the golden and the sign
// check so the file on disk is the document those assertions describe.
const cashflowForm: SimpleForm = {
    ...form,
    cashflowFrequency: "monthly",
    cashflowAmount: 500,
    cashflowDirection: "deposit",
};

console.log("frontend smoke render");

// The metric table has two views, and every cell in it -- header, portfolio
// row, benchmark row -- is gated on the same show(key). If one of the three
// disagrees, the row does not come out short: every later value slides one
// column left and prints under the wrong label, which has happened here once
// already. So both views are rendered and their cells counted.
//
// The view is read from storage, which node does not provide, so a fake one is
// installed for the duration -- the same path the app takes, not a way around
// it.
function withStoredView<T>(view: string, fn: () => T): T {
    const had = Object.getOwnPropertyDescriptor(globalThis, "localStorage");
    Object.defineProperty(globalThis, "localStorage", {
        value: { getItem: () => view, setItem: () => {} },
        configurable: true,
    });
    try {
        return fn();
    } finally {
        if (had) Object.defineProperty(globalThis, "localStorage", had);
        else delete (globalThis as Record<string, unknown>).localStorage;
    }
}

// The metrics table is the first one on the page; the other tables live in
// collapsible panels that render nothing while closed.
function firstTable(html: string): string {
    const start = html.indexOf("<table");
    const end = html.indexOf("</table>", start);
    if (start < 0 || end < 0) throw new Error("no metrics table was rendered");
    return html.slice(start, end);
}

function cellCounts(table: string): { headers: number; rows: number[] } {
    const rows = table
        .split("<tr")
        .slice(1)
        .map((row) => (row.match(/<td[ >]/g) ?? []).length)
        .filter((n) => n > 0);
    // <th[ >] and not <th, because <thead> matches the latter and quietly
    // adds a header that no row can have a cell for.
    return { headers: (table.match(/<th[ >]/g) ?? []).length, rows };
}

check("both metric views line up cell for cell", () => {
    const { results, verdicts } = load("full");
    for (const view of ["basic", "full"]) {
        const html = withStoredView(view, () =>
            renderToString(
                <ResultsView results={results} fontSize={16} verdicts={verdicts} />,
            ),
        );
        const { headers, rows } = cellCounts(firstTable(html));
        if (headers === 0 || rows.length === 0) {
            throw new Error(`${view}: rendered a table with no ${headers === 0 ? "headers" : "rows"}`);
        }
        for (const [i, n] of rows.entries()) {
            if (n !== headers) {
                throw new Error(
                    `${view} view: row ${i} has ${n} cells against ${headers} headers — ` +
                        "a column is gated in one place and not the other, so every " +
                        "value after it prints under the wrong label",
                );
            }
        }
    }
});

// Basics has to be genuinely shorter, and Everything has to be the whole
// table: a toggle that changes nothing is worse than no toggle.
check("Basics hides columns that Everything shows", () => {
    const { results, verdicts } = load("full");
    const count = (view: string) =>
        cellCounts(
            firstTable(
                withStoredView(view, () =>
                    renderToString(
                        <ResultsView results={results} fontSize={16} verdicts={verdicts} />,
                    ),
                ),
            ),
        ).headers;
    const basic = count("basic");
    const full = count("full");
    if (basic >= full) {
        throw new Error(`Basics shows ${basic} columns and Everything ${full}`);
    }
    if (basic < 6) {
        throw new Error(`Basics shows only ${basic} columns; the six a first run is about should survive`);
    }
});

// Money paid in is not money the strategy made. Without netting it out, the
// Profit column reports a decade of contributions as performance.
check("profit excludes contributions", () => {
    const { results, verdicts } = load("full");
    const base = results[0] as main.RunResult;
    const render = (netContributions: number) =>
        renderToString(
            <ResultsView
                results={[main.RunResult.createFrom({ ...base, netContributions })]}
                fontSize={16}
                verdicts={verdicts}
            />,
        );
    if (render(0) === render(50000)) {
        throw new Error(
            "50,000 of contributions changed nothing on screen — the Profit " +
                "column is counting deposits as gains",
        );
    }
});

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

// Renders with a fake localStorage holding `entries`, which is how a panel's
// remembered state is exercised: node has no storage, so without this every
// render is a first visit.
function withStorage<T>(entries: Record<string, string>, fn: () => T): T {
    const store = new Map<string, string>(Object.entries(entries));
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

function withPanelsOpen<T>(fn: () => T): T {
    return withStorage(
        { "backtester-results-panels": JSON.stringify(ALL_PANELS) },
        fn,
    );
}

// "Run this script" points every portfolio at the file open in the Lua tab,
// which is a path, while the strategy picker names a library file. buildToml
// has to emit both correctly: a bare name means the shipped library, a path is
// already a path.
check("a strategy may be a library name or a path", () => {
    const libraryPick = buildToml([{ ...form, strategy: "rsi.lua" }]);
    if (!libraryPick.includes('Strategy = "lua:strategies/rsi.lua"')) {
        throw new Error(`a library pick emitted:\n${libraryPick}`);
    }
    const scriptPath = buildToml([{ ...form, strategy: "strategies/my_strategy.lua" }]);
    if (!scriptPath.includes('Strategy = "lua:strategies/my_strategy.lua"')) {
        throw new Error(`a path emitted:\n${scriptPath}`);
    }
    // The bug this prevents: a path run through the library rule becomes
    // "lua:strategies/strategies/my_strategy.lua", which resolves to nothing.
    if (scriptPath.includes("strategies/strategies")) {
        throw new Error("a path was prefixed with the library directory again");
    }
    const absolute = buildToml([{ ...form, name: "", strategy: "/home/u/scripts/idea.lua" }]);
    if (!absolute.includes('Strategy = "lua:/home/u/scripts/idea.lua"')) {
        throw new Error(`an absolute path emitted:\n${absolute}`);
    }
    // And the portfolio's fallback name is the script's, not the whole path.
    if (!absolute.includes('Name = "idea"')) {
        throw new Error(`an unnamed portfolio was named from the path:\n${absolute}`);
    }
});

// Two lines on one chart read as a comparison, and two return series over
// different periods are not one. The note says so; it does not refuse the run,
// because running two windows on purpose is legitimate.
check("differing windows are called out, identical ones are not", () => {
    const span = (name: string, from: string, to: string) =>
        main.RunResult.createFrom({
            portfolioName: name,
            dates: [from, "2016-06-01", to],
            equityCurve: [1, 2, 3],
        });

    const same = differingWindows([
        span("A", "2015-01-02", "2020-12-31"),
        span("B", "2015-01-02", "2020-12-31"),
    ]);
    if (same !== "") {
        throw new Error(`identical windows produced a note: ${same}`);
    }

    const differing = differingWindows([
        span("Long", "2009-01-02", "2020-12-31"),
        span("Short", "2015-01-02", "2020-12-31"),
    ]);
    for (const want of ["Long 2009-01-02", "Short 2015-01-02", "not comparable"]) {
        if (!differing.includes(want)) {
            throw new Error(`the note is missing ${want}: ${differing}`);
        }
    }

    // A different END is just as incomparable as a different start, and is the
    // easier one to miss: two runs "to today" that ended on different days.
    const endsDiffer = differingWindows([
        span("A", "2015-01-02", "2020-12-31"),
        span("B", "2015-01-02", "2021-06-30"),
    ]);
    if (endsDiffer === "") {
        throw new Error("two windows ending on different days produced no note");
    }

    // One portfolio has nothing to be incomparable with, and a result with no
    // days is not on the chart at all.
    if (differingWindows([span("A", "2015-01-02", "2020-12-31")]) !== "") {
        throw new Error("a single portfolio was compared with itself");
    }
    const noDates = main.RunResult.createFrom({ portfolioName: "empty", dates: [] });
    if (differingWindows([noDates, span("A", "2015-01-02", "2020-12-31")]) !== "") {
        throw new Error("a result with no days was treated as a window");
    }
});

// What the strategy plotted. The panel is collapsible, so the render has to
// open it; the shape checks are that a series starting late starts late and a
// flat series does not divide by a zero range.
check("plotted series are drawn, late starts and all", () => {
    const { results, verdicts } = load("plain");
    const base = results[0] as main.RunResult;
    const withPlots = main.RunResult.createFrom({
        ...base,
        dates: ["2020-01-02", "2020-01-03", "2020-01-06", "2020-01-07"],
        plots: [
            { name: "sma50", points: [{ day: 2, value: 10 }, { day: 3, value: 12 }] },
            // A flat series is a real answer: an indicator that never moved.
            { name: "flat", points: [{ day: 0, value: 5 }, { day: 3, value: 5 }] },
        ],
    });
    const html = withStorage(
        { "backtester-results-panels": JSON.stringify({ plots: true }) },
        () =>
            renderToString(
                <ResultsView results={[withPlots]} fontSize={16} verdicts={verdicts} />,
            ),
    );
    if (!html.includes("What the strategy plotted")) {
        throw new Error("the plots panel did not render");
    }
    for (const name of ["sma50", "flat"]) {
        if (!html.includes(name)) throw new Error(`series ${name} is missing`);
    }
    if (html.includes("NaN")) {
        throw new Error("a NaN reached the DOM — a flat series divided by its own range");
    }
    // The late series must start two thirds of the way along (day 2 of 3),
    // not at the left edge.
    const poly = html.match(/points="([^"]+)"/g) ?? [];
    if (poly.length < 2) throw new Error(`drew ${poly.length} polylines, want 2`);
    const late = poly[0] ?? "";
    if (!late.includes('points="400,')) {
        throw new Error(`a series starting on day 2 of 3 was drawn from ${late}`);
    }

    // A result that plotted nothing shows no panel rather than an empty one.
    // Spelled out rather than taken from the fixture: fixtures.go fills every
    // field, so the "plain" result plots two series like everything else.
    const without = renderToString(
        <ResultsView
            results={[main.RunResult.createFrom({ ...base, plots: [] })]}
            fontSize={16}
            verdicts={verdicts}
        />,
    );
    if (without.includes("What the strategy plotted")) {
        throw new Error("the plots panel rendered for a run that plotted nothing");
    }
});

// The Lua editor's completion. The descriptions come from Go and a test there
// pins them to the engine's own SetGlobal calls; what is checked here is the
// part that turns a signature into something an editor can insert -- because a
// snippet with the arguments in the wrong order teaches the wrong API more
// convincingly than no completion at all.
const luaApiFixture = [
    backtest.LuaGlobal.createFrom({
        name: "sma",
        signature: "sma(ticker, day, period)",
        kind: "function",
        returns: "number",
        summary: "Mean close over [day-period, day).",
        group: "Indicators",
    }),
    backtest.LuaGlobal.createFrom({
        name: "cash",
        signature: "cash()",
        kind: "function",
        returns: "number",
        summary: "Uninvested buying power.",
        group: "Account and orders",
    }),
    backtest.LuaGlobal.createFrom({
        name: "log",
        signature: "log(...)",
        kind: "function",
        summary: "Say something the run console will show.",
        group: "Context",
    }),
    backtest.LuaGlobal.createFrom({
        name: "tickers",
        signature: "tickers",
        kind: "value",
        summary: "1-indexed table of the portfolio's ticker strings.",
        group: "Context",
    }),
];

check("a signature becomes a snippet with the arguments in order", () => {
    const cases: [string, string][] = [
        ["sma", "sma(${1:ticker}, ${2:day}, ${3:period})"],
        ["cash", "cash()"],
        // Any number of values, so there is no argument to tab to.
        ["log", "log($0)"],
        // A value is inserted as itself: `tickers()` would be a call on a table.
        ["tickers", "tickers"],
    ];
    for (const [name, want] of cases) {
        const g = findGlobal(luaApiFixture, name);
        if (!g) throw new Error(`${name} missing from the fixture`);
        const got = snippetFor(g);
        if (got !== want) {
            throw new Error(`snippetFor(${name}) = ${got}, want ${want}`);
        }
    }
    if (argsOf("corr(a, b, day, n)").join("|") !== "a|b|day|n") {
        throw new Error(`argsOf did not split the arguments: ${argsOf("corr(a, b, day, n)")}`);
    }
    if (argsOf("tickers").length !== 0 || argsOf("cash()").length !== 0) {
        throw new Error("a value and a no-argument call must yield no arguments");
    }
});

check("hover explains exactly the name under the cursor", () => {
    if (findGlobal(luaApiFixture, "sm") || findGlobal(luaApiFixture, "smaa")) {
        throw new Error("hover matched a name that is not a global");
    }
    const doc = documentation(findGlobal(luaApiFixture, "sma")!);
    for (const want of ["sma(ticker, day, period)", "number", "Mean close"]) {
        if (!doc.includes(want)) throw new Error(`hover text is missing ${want}:\n${doc}`);
    }
    // A function with nothing to return says nothing about returns rather than
    // claiming an empty one.
    if (documentation(findGlobal(luaApiFixture, "log")!).includes("Returns")) {
        throw new Error("a function with no return value advertised one");
    }
    if (!detailFor(findGlobal(luaApiFixture, "cash")!).includes("→ number")) {
        throw new Error("the completion detail line lost its return type");
    }
});

check("the completion list leads with step and keeps engine order", () => {
    const items = luaSuggestions(luaApiFixture);
    if (items[0].label !== "step") {
        throw new Error(`first suggestion is ${items[0].label}; a blank file needs step first`);
    }
    if (items.length !== luaApiFixture.length + 1) {
        throw new Error(`${items.length} suggestions for ${luaApiFixture.length} globals`);
    }
    const sorted = [...items].sort((a, b) => a.sortText.localeCompare(b.sortText));
    if (sorted.map((i) => i.label).join(",") !== items.map((i) => i.label).join(",")) {
        throw new Error(
            "sortText does not reproduce the engine's grouping — the list would " +
                "come out alphabetised instead of grouped",
        );
    }
    if (luaSuggestions([]).length !== 1) {
        throw new Error("with no API loaded the list should still offer step, and nothing else");
    }
    // With four entries any sortText scheme looks right; the real API has 34,
    // and an unpadded index sorts 10 before 9.
    const many = Array.from({ length: 12 }, (_, i) =>
        backtest.LuaGlobal.createFrom({
            name: `g${i}`,
            signature: `g${i}()`,
            kind: "function",
            summary: "x",
            group: "Context",
        }),
    );
    const order = luaSuggestions(many);
    const resorted = [...order].sort((a, b) => a.sortText.localeCompare(b.sortText));
    if (resorted.map((i) => i.label).join(",") !== order.map((i) => i.label).join(",")) {
        throw new Error(
            `sortText breaks past nine entries: ${resorted.map((i) => i.label).join(",")}`,
        );
    }
});

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
    const { results, verdicts } = load(name);
    // Both zoom extremes: the chart lays itself out in real pixels scaled by
    // fontSize, and a divide-by-zero there would only show at one end.
    for (const fontSize of [12, 22]) {
        check(`ResultsView / ${name} @ ${fontSize}px`, () => {
            renderToString(
                <ResultsView results={results} fontSize={fontSize} verdicts={verdicts} />,
            );
        });
    }
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
    // An empty card beside a populated one: the only state that renders the
    // starter baskets, since a wholly empty form gets the examples instead.
    ["one populated, one empty", [form, { ...form, name: "B", tickers: [], allocations: {} }]],
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

// A library pick says what it does, and — with an editor to open it in —
// offers the script itself, so a user can read what they are about to run.
const qualityCard = backtest.StrategyCard.createFrom({
    File: "quality_roe.lua",
    Idea: "Hold the most profitable companies.",
    Works: "quality persists",
    Fails: "junk rallies",
    Sweep: "top_n = [5, 10]",
    Baseline: "buy_and_hold.lua",
});
for (const [label, withViewer] of [
    ["with an editor", true],
    ["without one", false],
] as [string, boolean][]) {
    check(`PortfolioCard / a library pick, ${label}`, () => {
        const html = renderToString(
            <PortfolioCard
                form={{ ...form, strategy: "quality_roe.lua" }}
                onChange={noop}
                onRemove={null}
                otherNames={[]}
                defaultName="Portfolio 1"
                dbPath=""
                available={[]}
                names={{}}
                ranges={{}}
                pickable={[qualityCard]}
                showBaskets={false}
                onViewScript={withViewer ? noop : undefined}
            />,
        );
        if (!html.includes("Hold the most profitable companies.")) {
            throw new Error("the chosen strategy's card did not render");
        }
        if (html.includes("Read the script") !== withViewer) {
            throw new Error(`"Read the script" shown=${!withViewer}, want ${withViewer}`);
        }
    });
}

check("SimpleForm / with a script viewer", () => {
    renderToString(
        <SimpleFormView
            dbPath=""
            running={false}
            onOpenAssistant={noop}
            forms={[{ ...form, strategy: "quality_roe.lua" }]}
            onChange={noop}
            hypothesis={emptyHypothesis}
            onHypothesisChange={noop}
            onRun={noop}
            onEditAsToml={noop}
            onViewScript={noop}
        />,
    );
});

// The Lua tab's buffers: a library script is never changed or saved, and a
// copy of one is the user's, under its own name.
check("a library buffer is read-only and a copy is the user's", () => {
    const lib: LuaBuffer = {
        path: "strategies/quality_roe.lua",
        text: "-- quality_roe.lua\nfunction step(day) end\n",
        pristine: "-- quality_roe.lua\nfunction step(day) end\n",
        library: true,
        isNew: false,
    };
    eq("copy path", copyPath(lib.path), "strategies/my_quality_roe.lua");
    eq("copy of a bare name", copyPath("rsi.lua"), "my_rsi.lua");
    const copied = copyText(lib.path, lib.text);
    if (!copied.startsWith("-- my_quality_roe.lua — your copy of quality_roe.lua")) {
        throw new Error(`copy label: ${copied.split("\n")[0]}`);
    }
    if (!copied.endsWith("function step(day) end\n")) {
        throw new Error("the copy lost the script");
    }

    let bufs = addBuffer([], lib);
    bufs = addBuffer(bufs, { ...lib, text: "a second load" });
    eq("opening twice keeps one buffer", bufs.length, 1);
    bufs = setBufferText(bufs, lib.path, "-- gutted");
    eq("a library buffer ignores edits", bufs[0].text, lib.text);
    eq("a library buffer is never dirty", isDirty({ ...lib, text: "x" }), false);

    const copy: LuaBuffer = {
        path: copyPath(lib.path), text: copied, pristine: "", library: false, isNew: true,
    };
    bufs = addBuffer(bufs, copy);
    eq("a new copy is unsaved", isDirty(bufs[1]), true);
    bufs = setBufferText(bufs, copy.path, "-- mine");
    eq("a copy takes edits", bufs[1].text, "-- mine");
    bufs = markSaved(bufs, [bufs[1]]);
    eq("saved means clean", isDirty(bufs[1]), false);
});

// An imported holdings file, shaped as App.ImportHoldings returns it: decimal
// percentages, a Cash line mapped to $CASH, and two symbols left out.
const imported: HoldingsImport = {
    file: "my holdings.csv",
    tickers: ["VOO", "$CASH", "BRK.B"],
    allocations: { VOO: 12.35, "$CASH": 65.4, "BRK.B": 22.25 },
    total: 92617.4,
    percent: false,
    mapped: [{ from: "Cash", to: "$CASH", reason: "uninvested cash" }],
    unmatched: [
        { symbol: "SOL", pct: 0.51 },
        { symbol: "ETH", pct: 0.68 },
    ],
    unmatchedPct: 1.19,
    skipped: null,
};

check("import replaces the holdings and sets capital from the file", () => {
    const got = applyImport({ ...form, name: "", evenSplit: true }, imported);
    eq("tickers", got.tickers, ["VOO", "$CASH", "BRK.B"]);
    eq("allocations", got.allocations, imported.allocations);
    eq("evenSplit", got.evenSplit, false);
    eq("buyingPower", got.buyingPower, 92617);
    eq("name", got.name, "my holdings");
    // A file of weights says nothing about capital.
    eq(
        "percent file keeps capital",
        applyImport(form, { ...imported, percent: true, total: 0 }).buyingPower,
        form.buyingPower,
    );
    const toml = buildToml([got]);
    if (!toml.includes('"$CASH" = 65.4') || !toml.includes('"BRK.B" = 22.25')) {
        throw new Error(`weights not emitted:\n${toml}`);
    }
});

check("import summary names what was mapped and what was left out", () => {
    const lines = importSummary(imported).join("\n");
    for (const want of ["Cash → $CASH", "SOL (0.51%)", "1.19% of the file", "$92,617"]) {
        if (!lines.includes(want)) throw new Error(`missing ${want}:\n${lines}`);
    }
});

check("SimpleForm / an imported portfolio with decimal weights", () => {
    const html = renderToString(
        <SimpleFormView
            dbPath=""
            running={false}
            onOpenAssistant={noop}
            forms={[applyImport(form, imported)]}
            onChange={noop}
            hypothesis={emptyHypothesis}
            onHypothesisChange={noop}
            onRun={noop}
            onEditAsToml={noop}
        />,
    );
    if (!html.includes('value="22.25"')) throw new Error("decimal allocation not rendered");
    if (!html.includes("Import…")) throw new Error("no Import button");
});

// What the app remembers between launches. A stored session outlives shape
// changes and upgrades, so it is INPUT: the check is that a session which no
// longer matches the current form is dropped whole, rather than restoring a
// card with holdings and no allocations.
const savedSession: Session = {
    forms: [form, { ...form, name: "B", tickers: ["AAPL"], allocations: { AAPL: 100 } }],
    hypothesis: { claim: "it beats the market", metric: "sharpe", op: ">=", value: "1" },
};
const fallbackSession: Session = { forms: [form], hypothesis: emptyHypothesis };

check("a saved session round-trips", () => {
    const got = parseSession(JSON.stringify(savedSession), fallbackSession);
    if (JSON.stringify(got) !== JSON.stringify(savedSession)) {
        throw new Error(
            `restored session differs:\n${JSON.stringify(got)}\n` +
                `want\n${JSON.stringify(savedSession)}`,
        );
    }
});

check("a session that is not the current shape is dropped whole", () => {
    const bad: [string, string | null][] = [
        ["nothing stored", null],
        ["not JSON", "{oh no"],
        ["JSON null", "null"],
        ["an array", "[]"],
        ["no forms", JSON.stringify({ hypothesis: emptyHypothesis })],
        ["an empty form list", JSON.stringify({ forms: [], hypothesis: emptyHypothesis })],
        ["no hypothesis", JSON.stringify({ forms: [form] })],
        [
            "a non-numeric allocation",
            JSON.stringify({
                forms: [{ ...form, allocations: { AAPL: "half" } }],
                hypothesis: emptyHypothesis,
            }),
        ],
        [
            "a cashflow frequency the engine does not have",
            JSON.stringify({
                forms: [{ ...form, cashflowFrequency: "fortnightly" }],
                hypothesis: emptyHypothesis,
            }),
        ],
        [
            "one bad form among good ones",
            JSON.stringify({
                forms: [form, { ...form, tickers: "AAPL" }],
                hypothesis: emptyHypothesis,
            }),
        ],
    ];
    for (const [what, raw] of bad) {
        const got = parseSession(raw, fallbackSession);
        if (got !== fallbackSession) {
            throw new Error(`${what}: restored ${JSON.stringify(got)} instead of the defaults`);
        }
    }
});

// The validator has to name every field of SimpleForm. One it forgets is one
// restored as undefined, which reaches buildToml and generates a config with a
// hole in it -- so dropping any single field must invalidate the session.
check("every field of the form is validated", () => {
    for (const field of Object.keys(form)) {
        const partial: Record<string, unknown> = { ...form };
        delete partial[field];
        const raw = JSON.stringify({ forms: [partial], hypothesis: emptyHypothesis });
        if (parseSession(raw, fallbackSession) !== fallbackSession) {
            throw new Error(
                `a form with no "${field}" was restored; session.ts does not check that field`,
            );
        }
    }
});

// The header chips. The database one can now read "Choose DB…" for real: the
// path comes from Go (DefaultDBPath), and an app launched with no database
// beside it gets an empty string rather than the "../stock_data.db" the
// frontend used to hardcode. A chip that rendered that empty path would name
// a file that does not exist and offer no way out.
check("chipLabel names a file, or asks for one", () => {
    const cases: [string, string, string][] = [
        ["/home/u/data/stock_data.db", "Choose DB…", "stock_data.db"],
        ["C:\\Users\\u\\stock_data.db", "Choose DB…", "stock_data.db"],
        ["stock_data.db", "Choose DB…", "stock_data.db"],
        ["", "Choose DB…", "Choose DB…"],
        ["", "unsaved", "unsaved"],
    ];
    for (const [path, empty, want] of cases) {
        const got = chipLabel(path, empty);
        if (got !== want) {
            throw new Error(
                `chipLabel(${JSON.stringify(path)}, ${JSON.stringify(empty)}) ` +
                    `= ${JSON.stringify(got)}, want ${JSON.stringify(want)}`,
            );
        }
    }
});

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

// There is one buy-and-hold script. An even split runs it with no weights --
// the script's own equal split -- and a custom split hands it the weights;
// neither may name a script the library no longer ships.
check("buy-and-hold always runs buy_and_hold.lua", () => {
    const even = buildToml([{ ...form, evenSplit: true, strategy: "" }]);
    if (!even.includes('Strategy = "lua:strategies/buy_and_hold.lua"')) {
        throw new Error("even split does not run buy_and_hold.lua:\n" + even);
    }
    if (even.includes("Params")) {
        throw new Error("even split emitted weights:\n" + even);
    }
    const custom = buildToml([{ ...form, evenSplit: false, strategy: "" }]);
    if (!custom.includes('Strategy = "lua:strategies/buy_and_hold.lua"') ||
        !custom.includes("Params = { weights = {")) {
        throw new Error("custom split does not pass weights to buy_and_hold.lua:\n" + custom);
    }
});

// A schedule adds a sub-table, which is the one shape in this document whose
// header the decoder can reject outright. TestGeneratedConfigsParse feeds this
// file to the real TOML decoder, so the golden is a parse test, not just a
// snapshot.
check("buildToml matches the golden cashflow config", () => {
    const want = readFileSync(join(testdata, "golden-cashflow.toml"), "utf8");
    const got = buildToml([cashflowForm]);
    if (got !== want) {
        throw new Error(
            "generated cashflow TOML changed:\n\n--- want\n" + want + "\n--- got\n" + got,
        );
    }
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

// The cashflow block, and above all its SIGN. The form carries a positive
// amount plus a direction; getting the sign wrong turns a contribution plan
// into a decumulation one, which is a completely different backtest.
check("a cashflow schedule emits a correctly signed block", () => {
    const none = buildToml([form]);
    if (none.includes("Cashflow")) {
        throw new Error("frequency 'never' emitted a block; it must emit nothing");
    }

    const dep = buildToml([cashflowForm]);
    // The sub-table header must name the array header EXACTLY: TOML keys are
    // case-sensitive, so "[portfolio.Cashflow]" under "[[Portfolio]]" is a
    // second, unrelated top-level table -- which the decoder rejects with
    // "incompatible types" rather than ignoring. That shipped once.
    if (!dep.includes("[Portfolio.Cashflow]") || !dep.includes("Amount = 500.0")) {
        throw new Error("deposit did not emit a positive Amount:\n" + dep);
    }
    for (const header of dep.match(/^\s*\[[A-Za-z0-9_.-]+\]$/gm) ?? []) {
        const parent = header.trim().slice(1, -1).split(".")[0];
        if (parent !== "Portfolio") {
            throw new Error(
                `sub-table ${header.trim()} does not hang off [[Portfolio]]; ` +
                    "TOML keys are case-sensitive:\n" + dep,
            );
        }
    }
    if (!dep.includes('Frequency = "monthly"')) {
        throw new Error("frequency missing:\n" + dep);
    }

    const wd = buildToml([
        { ...form, cashflowFrequency: "annually", cashflowAmount: 500, cashflowDirection: "withdraw" },
    ]);
    if (!wd.includes("Amount = -500.0")) {
        throw new Error("withdraw did not negate the amount:\n" + wd);
    }

    // An amount with no frequency, and a frequency with no amount, both do
    // nothing rather than emitting a half-schedule the engine would refuse.
    const halfA = buildToml([{ ...form, cashflowAmount: 500 }]);
    const halfB = buildToml([{ ...form, cashflowFrequency: "monthly" }]);
    if (halfA.includes("Cashflow") || halfB.includes("Cashflow")) {
        throw new Error("a half-filled schedule emitted a block");
    }
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

// Input guards. The form refuses bad input rather than reporting it, so these
// are what stands between a user and an error message -- and none of them is
// visible to a render test, which never types.
const GUARD_RANGES = {
    AAPL: ["2000-01-03", "2026-08-07"],
    NEWCO: ["2021-06-01", "2026-08-07"],
    GONE: ["1995-01-03", "1999-12-31"],
    $SP500: ["1990-01-02", "2026-07-31"],
};

check("date boxes accept only the shape of a date", () => {
    eq("digits", maskDate("20150102"), "2015-01-02");
    eq("partial", maskDate("20151"), "2015-1");
    eq("letters dropped", maskDate("2o15-O1-02x"), "2151-02");
    eq("too long", maskDate("2015-01-0299"), "2015-01-02");
});

check("dates are pulled inside the covered window", () => {
    const f = { ...form, tickers: ["AAPL"], benchmark: "$SP500",
        startDate: "1990-01-01", endDate: "2030-01-01" };
    const b = dateBounds(f, GUARD_RANGES);
    // The benchmark ends earlier than AAPL, and a window past it would
    // silently lose every comparison metric.
    eq("bounds", b, { min: "2000-01-03", max: "2026-07-31" });
    eq("clamped", normalizeDates(f, b, { start: "", end: "" }, null),
        { startDate: "2000-01-03", endDate: "2026-07-31" });
    const fine = { ...f, startDate: "2010-01-01", endDate: "2020-01-01" };
    eq("already fine", normalizeDates(fine, b, { start: "", end: "" }, null), null);
    // A typo falls back to the last good date, not to the window's edge.
    eq("typo", normalizeDates({ ...fine, startDate: "2010-13-45" }, b,
        { start: "2011-05-05", end: "" }, "start"),
        { startDate: "2011-05-05", endDate: "2020-01-01" });
    // Start after end: what the user just typed is kept, the other side moves.
    eq("start past end", normalizeDates({ ...fine, startDate: "2021-01-01" }, b,
        { start: "", end: "" }, "start"),
        { startDate: "2021-01-01", endDate: "2026-07-31" });
    eq("end before start", normalizeDates({ ...fine, endDate: "2005-01-01" }, b,
        { start: "", end: "" }, "end"),
        { startDate: "2000-01-03", endDate: "2005-01-01" });
    // Empty dates on a portfolio with holdings are filled, not left to fail.
    eq("empty filled", normalizeDates({ ...f, startDate: "", endDate: "" }, b,
        { start: "", end: "" }, null),
        { startDate: "2000-01-03", endDate: "2026-07-31" });
    // Whatever it returns is runnable -- that is the whole contract.
    const fixed = { ...f, allocations: { AAPL: 100 },
        ...normalizeDates(f, b, { start: "", end: "" }, null)! };
    eq("result runs", portfolioRunnable(fixed, GUARD_RANGES), true);
});

check("a benchmark that misses the holdings does not empty the window", () => {
    const f = { ...form, tickers: ["NEWCO"], benchmark: "GONE" };
    eq("ignored", dateBounds(f, GUARD_RANGES), { min: "2021-06-01", max: "2026-08-07" });
});

check("the picker offers only symbols that keep the portfolio runnable", () => {
    const fits = tickerFilter({ ...form, tickers: ["AAPL"] }, GUARD_RANGES);
    eq("overlaps", fits("NEWCO"), true);
    eq("no overlap", fits("GONE"), false);
    eq("unpriced", fits("NOPE"), false);
    eq("already held", fits("AAPL"), false);
    eq("first pick", tickerFilter({ ...form, tickers: [] }, GUARD_RANGES)("GONE"), true);
    // Ranges not loaded yet: nothing to judge against, so nothing is hidden.
    eq("no ranges", tickerFilter({ ...form, tickers: ["AAPL"] }, {})("NOPE"), true);
});

// The filter runs over the whole symbol universe on every holdings change,
// on the UI thread. It was once quadratic -- 15 s per keystroke at 10,000
// symbols -- so this pins it linear with a budget no quadratic version meets.
check("the picker's filter stays linear in the size of the universe", () => {
    const big: Record<string, string[]> = {};
    const syms: string[] = [];
    for (let i = 0; i < 20000; i++) {
        syms.push(`S${i}`);
        big[`S${i}`] = ["2000-01-03", "2026-08-07"];
    }
    const t0 = performance.now();
    const kept = syms.filter(tickerFilter({ ...form, tickers: ["S1", "S2"] }, big));
    const ms = performance.now() - t0;
    eq("kept", kept.length, 19998);
    if (ms > 250) throw new Error(`filtering 20,000 symbols took ${ms.toFixed(0)} ms`);
});

check("Enter and Add take a listed symbol, never free text", () => {
    const sugg = ["AAPL", "AAP"];
    eq("highlighted", resolveTicker("aa", 1, sugg), "AAP");
    eq("exact", resolveTicker("aap", -1, sugg), "AAP");
    eq("best match", resolveTicker("apple", -1, sugg), "AAPL");
    eq("nothing listed", resolveTicker("zzzz", -1, []), "");
    eq("blank", resolveTicker("  ", -1, sugg), "");
});

check("portfolio names cannot collide or go blank", () => {
    eq("free", uniqueName("Growth", ["Value"], "Portfolio 2"), "Growth");
    eq("clash", uniqueName("Value", ["Value"], "Portfolio 2"), "Value (2)");
    eq("next free", uniqueName("Value", ["Value", "Value (2)"], "P"), "Value (3)");
    eq("blank", uniqueName("  ", ["Value"], "Portfolio 2"), "Portfolio 2");
});

check("the criterion box accepts only a number", () => {
    eq("plain", maskDecimal("1.2"), "1.2");
    eq("negative", maskDecimal("-0.5"), "-0.5");
    eq("junk", maskDecimal("abc1.2.3%"), "1.23");
    eq("inner minus", maskDecimal("1-2"), "12");
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

// Starter examples. The whole promise is that clicking one produces a RUN --
// an example that opens with a red date error is worse than no example -- so
// what gets pinned is runnability against a realistic coverage table, not the
// wording.
const EXAMPLE_RANGES = {
    AAPL: ["1990-01-02", "2026-08-07"],
    MSFT: ["1990-01-02", "2026-08-07"],
    GOOGL: ["2004-08-19", "2026-08-07"],
    AMZN: ["1997-05-15", "2026-08-07"],
    NVDA: ["1999-01-22", "2026-08-07"],
    SPY: ["1993-01-29", "2026-08-07"],
    // The benchmark ends BEFORE the priced tickers, which is the real
    // database's state and the thing that silently drops every
    // benchmark-relative metric. An example has to survive it.
    $SP500: ["1927-12-30", "2026-07-02"],
};

check("every starter example produces a runnable portfolio", () => {
    for (const ex of EXAMPLES) {
        const built = buildExample(ex, EXAMPLE_RANGES, "$SP500", BLANK);
        if (!built) throw new Error(`${ex.id} could not be built at all`);
        if (!portfolioRunnable(built, EXAMPLE_RANGES)) {
            throw new Error(
                `${ex.id} builds a portfolio the engine would refuse: ` +
                    JSON.stringify(built),
            );
        }
        // Inside the benchmark's window too, or the run quietly loses the
        // comparison the example's question is asking about.
        if (built.endDate > EXAMPLE_RANGES.$SP500[1]) {
            throw new Error(
                `${ex.id} ends ${built.endDate}, past the benchmark's last bar`,
            );
        }
        if (built.tickers.length === 0) throw new Error(`${ex.id} has no holdings`);
        // Equal weight: an example must never open by demanding percentages.
        if (!built.evenSplit) throw new Error(`${ex.id} did not use equal weight`);
    }
});

check("an example is withdrawn rather than broken when symbols are missing", () => {
    // A database with only SPY: the two five-name examples cannot be built,
    // the single-ticker ones still can.
    const thin = { SPY: ["1993-01-29", "2026-08-07"], $SP500: ["1927-12-30", "2026-07-02"] };
    for (const ex of EXAMPLES) {
        const built = buildExample(ex, thin, "$SP500", BLANK);
        const enough = ex.tickers.filter((t) => t in thin).length >= ex.minTickers;
        eq(`${ex.id} offered`, built !== null, enough);
    }
    // And an empty database offers nothing at all, rather than throwing.
    for (const ex of EXAMPLES) {
        eq(`${ex.id} on empty db`, buildExample(ex, {}, "$SP500", BLANK), null);
    }
});

check("starter baskets are filtered to what the database has", () => {
    const ranges = {
        AAPL: ["1990-01-02", "2026-08-07"],
        MSFT: ["1990-01-02", "2026-08-07"],
        GOOGL: ["2004-08-19", "2026-08-07"],
        SPY: ["1993-01-29", "2026-08-07"],
    };
    for (const b of BASKETS) {
        const got = basketTickers(b, ranges);
        const have = b.tickers.filter((t) => t in ranges);
        // Offered only when enough of it survives -- a "spread across sectors"
        // that resolves to two names is a lie, so it is withdrawn instead.
        eq(`${b.id}`, got, have.length >= b.minTickers ? have : []);
        // Never a symbol this database does not have.
        for (const t of got) {
            if (!(t in ranges)) throw new Error(`${b.id} offered missing ${t}`);
        }
    }
    // An empty database offers nothing rather than throwing.
    for (const b of BASKETS) eq(`${b.id} on empty db`, basketTickers(b, {}), []);
});

check("every criterion preset names a metric the engine serves", () => {
    // Same list App.CriterionMetrics returns. A preset the engine would reject
    // has to fail here rather than at run time, where it costs a whole run.
    const METRICS = [
        "sharpe", "sortino", "calmar", "annual_return", "max_drawdown",
        "total_return", "turnover", "exposure", "time_in_market", "p_value",
    ];
    if (CRITERION_PRESETS.length === 0) throw new Error("no presets offered");
    for (const p of CRITERION_PRESETS) {
        if (!METRICS.includes(p.metric)) {
            throw new Error(`${p.id} uses unknown metric ${p.metric}`);
        }
        if (!CRITERION_OPS.includes(p.op)) {
            throw new Error(`${p.id} uses unknown operator ${p.op}`);
        }
        // A preset must COMPLETE a registration, claim included, or clicking
        // it leaves the run unjudged while looking like it did something.
        const built = buildHypothesis({
            claim: p.claim, metric: p.metric, op: p.op, value: p.value,
        });
        if (built === "") throw new Error(`${p.id} produced no hypothesis`);
        if (!built.includes(`${p.metric} ${p.op} ${p.value}`)) {
            throw new Error(`${p.id} did not register its own criterion: ${built}`);
        }
    }
});

check("every starter example registers a criterion the engine accepts", () => {
    // The metric vocabulary is served by Go (App.CriterionMetrics); this is
    // the list it returns, so a preset naming something else fails here rather
    // than at run time.
    const METRICS = [
        "sharpe", "sortino", "calmar", "annual_return", "max_drawdown",
        "total_return", "turnover", "exposure", "time_in_market", "p_value",
    ];
    for (const ex of EXAMPLES) {
        const h = exampleHypothesis(ex);
        if (!METRICS.includes(h.metric)) {
            throw new Error(`${ex.id} uses unknown metric ${h.metric}`);
        }
        if (!CRITERION_OPS.includes(h.op)) {
            throw new Error(`${ex.id} uses unknown operator ${h.op}`);
        }
        if (buildHypothesis(h) === "") {
            throw new Error(`${ex.id} produced no hypothesis: ${JSON.stringify(h)}`);
        }
    }
});

// Every famous-investor holding, at its real first bar, ending where the
// bundled database does -- BK's feed stops early on it, which is the case that
// has to clamp the window rather than break it.
const INVESTOR_RANGES: Record<string, string[]> = {
    $SP500: ["1927-12-30", "2026-09-25"],
    $CASH: ["1927-12-30", "2026-09-24"],
    VTI: ["2001-06-15", "2026-09-25"],
    TLT: ["2002-07-30", "2026-09-25"],
    IEF: ["2002-07-30", "2026-09-23"],
    GLD: ["2004-11-18", "2026-09-25"],
    DBC: ["2006-02-06", "2026-09-25"],
    VEA: ["2007-07-26", "2026-09-25"],
    VWO: ["2005-03-10", "2026-09-25"],
    VNQ: ["2004-09-29", "2026-09-25"],
    TIP: ["2003-12-05", "2026-09-25"],
    BK: ["1972-06-15", "2026-06-26"],
    KHC: ["2015-07-06", "2026-09-25"],
    PSX: ["2012-04-12", "2026-09-25"],
    AXTA: ["2014-11-12", "2026-09-25"],
};
for (const t of [
    "WFC", "KO", "IBM", "AXP", "PG", "USB", "WMT", "DVA", "MCO", "GS", "CHTR",
    "DE", "GM", "T", "VRSN", "SU", "COST", "VZ", "MTB",
]) {
    INVESTOR_RANGES[t] = ["2012-01-03", "2026-09-25"];
}

check("every famous-investor portfolio produces a runnable portfolio", () => {
    for (const inv of INVESTORS) {
        const built = buildInvestor(inv, INVESTOR_RANGES, "$SP500", BLANK);
        if (!built) throw new Error(`${inv.id} could not be built at all`);
        if (!portfolioRunnable(built, INVESTOR_RANGES)) {
            throw new Error(`${inv.id} builds a portfolio the engine would refuse`);
        }
        if (built.evenSplit) throw new Error(`${inv.id} lost its weights`);
        const total = built.tickers.reduce((s, t) => s + built.allocations[t], 0);
        eq(`${inv.id} allocations`, Math.round(total * 100), 10000);
        // Nothing the database cannot price, and the largest holding first.
        for (const t of built.tickers) {
            if (!(t in INVESTOR_RANGES)) throw new Error(`${inv.id} kept ${t}`);
        }
        eq(`${inv.id} name`, built.name, inv.name);
        // The TOML carries the weights whichever strategy holds them, or the
        // run is an equal-weight portfolio wearing the investor's name.
        const toml = buildToml([built]);
        if (!toml.includes("Params = { weights = {")) {
            throw new Error(`${inv.id} emitted no weights:\n${toml}`);
        }
    }
});

check("a 13F copy starts the day after its filing, never earlier", () => {
    const inv = INVESTORS.find((i) => i.id === "berkshire-13f-2015")!;
    const built = buildInvestor(inv, INVESTOR_RANGES, "$SP500", BLANK)!;
    eq("start", built.startDate, "2016-02-17");
    // BK's short feed clamps the end; it does not move the start.
    eq("end", built.endDate, "2026-06-26");
    // USG and V are not in the table: dropped and the rest scaled up.
    if (built.tickers.includes("USG") || built.tickers.includes("V")) {
        throw new Error("kept a holding the database cannot price");
    }
    eq("largest first", built.tickers[0], "WFC");
    // A database whose data starts after the filing cannot honestly run it.
    const late = { ...INVESTOR_RANGES, WFC: ["2017-01-03", "2026-09-25"] };
    eq("withdrawn when the filing day is not covered",
        buildInvestor(inv, late, "$SP500", BLANK), null);
});

check("a recipe is withdrawn when one of its legs is missing", () => {
    const { GLD: _gold, ...noGold } = INVESTOR_RANGES;
    for (const id of ["dalio-all-weather", "browne-permanent"]) {
        const inv = INVESTORS.find((i) => i.id === id)!;
        eq(`${id} without gold`, buildInvestor(inv, noGold, "$SP500", BLANK), null);
    }
    for (const inv of INVESTORS) {
        eq(`${inv.id} on empty db`, buildInvestor(inv, {}, "$SP500", BLANK), null);
    }
});

check("percentAllocations sums to exactly 100 at two decimals", () => {
    const got = percentAllocations({ A: 1, B: 1, C: 1 }, ["A", "B", "C"]);
    eq("thirds", got, { A: 33.34, B: 33.33, C: 33.33 });
    eq("relative", percentAllocations({ A: 7.5, B: 22.5 }, ["A", "B"]), { A: 25, B: 75 });
});

check("every famous investor registers a criterion the engine accepts", () => {
    const METRICS = [
        "sharpe", "sortino", "calmar", "annual_return", "max_drawdown",
        "total_return", "turnover", "exposure", "time_in_market", "p_value",
    ];
    for (const inv of INVESTORS) {
        const h = investorHypothesis(inv);
        if (!METRICS.includes(h.metric)) throw new Error(`${inv.id}: ${h.metric}`);
        if (buildHypothesis(h) === "") throw new Error(`${inv.id} produced no hypothesis`);
    }
});

check("rebalance.lua takes the allocations; other library picks do not", () => {
    const weighted: SimpleForm = {
        ...BLANK, name: "W", startDate: "2015-01-02", endDate: "2020-01-02",
        tickers: ["SPY", "TLT"], evenSplit: false, allocations: { SPY: 60, TLT: 40 },
    };
    const rebal = buildToml([{ ...weighted, strategy: "rebalance.lua" }]);
    if (!rebal.includes("Params = { weights = { SPY = 60.0, TLT = 40.0 } }")) {
        throw new Error(`rebalance.lua lost its weights:\n${rebal}`);
    }
    // Even split stays the script's own equal weighting: no Params at all.
    const even = buildToml([{ ...weighted, evenSplit: true, strategy: "rebalance.lua" }]);
    if (even.includes("Params")) throw new Error(`even split emitted Params:\n${even}`);
    const rsi = buildToml([{ ...weighted, strategy: "rsi.lua" }]);
    if (rsi.includes("Params")) throw new Error(`rsi.lua was handed weights:\n${rsi}`);
});

check("the famous-investor cards render, caveats included", () => {
    const html = renderToString(<Investors ranges={INVESTOR_RANGES} onPick={noop} />);
    for (const inv of INVESTORS) {
        if (!html.includes(inv.name)) throw new Error(`${inv.id} not offered`);
    }
    if (!html.includes("Rebalanced quarterly")) throw new Error("caveats not shown");
    // A database with none of it offers no section at all.
    eq("empty db", renderToString(<Investors ranges={{}} onPick={noop} />), "");
});

check("PortfolioCard / every card offers the famous investors", () => {
    const card = (f: SimpleForm, ranges: Record<string, string[]>) =>
        renderToString(
            <PortfolioCard
                form={f}
                onChange={noop}
                onRemove={null}
                otherNames={[]}
                defaultName="Portfolio 2"
                dbPath=""
                available={[]}
                names={{}}
                ranges={ranges}
                pickable={[]}
                showBaskets={false}
            />,
        );
    // Empty or populated, baskets off: the dropdown is there regardless.
    for (const f of [{ ...BLANK, name: "B" }, form]) {
        const html = card(f, INVESTOR_RANGES);
        if (!html.includes("Copy a famous investor")) {
            throw new Error(`no investor dropdown on ${JSON.stringify(f.tickers)}`);
        }
        for (const inv of INVESTORS) {
            if (!html.includes(inv.name)) throw new Error(`${inv.id} not offered`);
        }
    }
    if (!card(form, INVESTOR_RANGES).includes("replaces these holdings")) {
        throw new Error("a populated card does not warn that picking replaces it");
    }
    // A portfolio this database cannot build is listed, disabled, with why.
    if (!card(form, {}).includes("(not in this database)")) {
        throw new Error("an unbuildable investor was not marked");
    }
    // A card filled from one keeps its caveat on screen.
    const built = buildInvestor(INVESTORS[1], INVESTOR_RANGES, "$SP500", BLANK)!;
    if (!card(built, INVESTOR_RANGES).includes(INVESTORS[1].caveat.slice(0, 20))) {
        throw new Error("the copied portfolio's caveat is not shown");
    }
});

check("a benchmark line per deposit schedule, not one per ticker", () => {
    const mk = (name: string, curve: number[], netContributions = 0) =>
        main.RunResult.createFrom({
            portfolioName: name,
            initialCapital: 10000,
            netContributions,
            benchmarkCurve: curve,
            benchmarkStats: { ticker: "$SP500" },
        });
    // Two lump-sum portfolios share the index's line, unlabelled.
    const shared = benchmarkLines([mk("A", [10000, 11000]), mk("B", [10000, 11000])]);
    eq("shared", shared.map((l) => l.name), ["$SP500"]);
    // A monthly-deposit card's benchmark received its deposits, so it is its
    // own line, and both are named after their portfolios.
    const split = benchmarkLines([
        mk("Lump sum", [10000, 11000]),
        mk("Monthly", [10000, 17000], 6000),
    ]);
    eq("split", split.map((l) => l.name), ["$SP500 · Lump sum", "$SP500 · Monthly"]);
});

check("a chat message renders its markdown and insert actions", () => {
    const noop = () => {};
    const html = renderToString(
        <ChatMessage
            m={{
                role: "assistant",
                content: "Try **this**:\n```toml\n[Output]\n```\nor `sma(50)`.",
                toolNotes: ["SELECT 1"],
            }}
            busy={false}
            onApplyToml={noop}
            onApplyLua={noop}
        />,
    );
    const text = html.replace(/<!-- -->/g, "");
    for (const want of ["<strong>this</strong>", "Insert into TOML editor", "[Output]", "SELECT 1"]) {
        if (!text.includes(want)) throw new Error(`missing ${want}:\n${html}`);
    }
    // Streaming hides the insert action, so a half-written block cannot be
    // applied; the empty reply shows the thinking placeholder instead.
    const streaming = renderToString(
        <ChatMessage
            m={{ role: "assistant", content: "", streaming: true }}
            busy={true}
            onApplyToml={noop}
            onApplyLua={noop}
        />,
    );
    if (!streaming.includes("thinking")) throw new Error(streaming);
    const panel = renderToString(
        <ChatPanel
            width={320}
            dbPath=""
            cfgText=""
            luaText=""
            onApplyToml={noop}
            onApplyLua={noop}
            onClose={noop}
        />,
    );
    if (!panel.includes("chat-scroll") && !panel.includes("chat-setup")) {
        throw new Error(panel);
    }
});

check("the assistant maximises and restores from its header", () => {
    const noop = () => {};
    const render = (maximized: boolean) =>
        renderToString(
            <ChatPanel
                width={320}
                dbPath=""
                cfgText=""
                luaText=""
                onApplyToml={noop}
                onApplyLua={noop}
                onClose={noop}
                maximized={maximized}
                onToggleMax={noop}
            />,
        );
    const normal = render(false);
    if (!normal.includes("Maximise assistant")) throw new Error("no maximise button");
    if (!normal.includes("width:320px")) throw new Error("normal panel lost its width");
    const max = render(true);
    if (!max.includes("chat-panel maximized") || !max.includes("Restore assistant")) {
        throw new Error(`maximised panel does not say so:\n${max}`);
    }
    // Maximised, the panel's width is the row's, never the stored pixels.
    if (max.includes("width:320px")) throw new Error("maximised panel kept a fixed width");
});

check("keyboard shortcuts map to one action each", () => {
    const k = (key: string, mods: { ctrl?: boolean; shift?: boolean; alt?: boolean; meta?: boolean } = {}) =>
        shortcutFor({
            key,
            ctrlKey: !!mods.ctrl,
            metaKey: !!mods.meta,
            shiftKey: !!mods.shift,
            altKey: !!mods.alt,
        });
    eq("ctrl+enter", k("Enter", { ctrl: true }), "run");
    eq("cmd+enter", k("Enter", { meta: true }), "run");
    eq("ctrl+1", k("1", { ctrl: true }), "tab-backtest");
    eq("ctrl+3", k("3", { ctrl: true }), "tab-data");
    eq("ctrl+j", k("j", { ctrl: true }), "toggle-assistant");
    // Shift changes the key's case; the binding must not care.
    eq("ctrl+shift+J", k("J", { ctrl: true, shift: true }), "max-assistant");
    eq("ctrl+m", k("m", { ctrl: true }), "toggle-results");
    eq("ctrl+shift+M", k("M", { ctrl: true, shift: true }), "max-results");
    eq("ctrl+s", k("s", { ctrl: true }), "save");
    eq("f1", k("F1"), "help");
    eq("esc", k("Escape"), "escape");
    eq("slash", k("/"), "focus-search");
    eq("ctrl+/ is not it", k("/", { ctrl: true }), null);
    // A bare key never fires from a field: typingInField is App's guard.
    const el = (tag: string, inMonaco = false) =>
        ({ tagName: tag, isContentEditable: false, closest: () => (inMonaco ? {} : null) }) as unknown as Element;
    eq("input types", typingInField(el("INPUT")), true);
    eq("textarea types", typingInField(el("TEXTAREA")), true);
    eq("monaco types", typingInField(el("DIV", true)), true);
    eq("a button does not", typingInField(el("BUTTON")), false);
    eq("nothing focused", typingInField(null), false);
    // Typing is never a shortcut, and AltGr (Ctrl+Alt) types characters.
    eq("plain j", k("j"), null);
    eq("plain enter", k("Enter"), null);
    eq("altgr", k("2", { ctrl: true, alt: true }), null);
    // Zoom stays with its own handler.
    eq("ctrl+0", k("0", { ctrl: true }), null);
    // Every action is listed once, so the overlay is complete.
    eq("unique", new Set(SHORTCUTS.map((s) => s.action)).size, SHORTCUTS.length);
    eq("keysFor", keysFor("run", false), "Ctrl+Enter");
    const html = renderToString(<ShortcutsHelp onClose={() => {}} />);
    for (const sc of SHORTCUTS) {
        if (!html.includes(sc.label.replace(/[()]/g, "").slice(0, 12))) {
            throw new Error(`help overlay omits ${sc.action}`);
        }
    }
});

check("the command palette finds commands by any of their words", () => {
    const cmd = (id: string, label: string, extra: Partial<Command> = {}): Command => ({
        id, label, run: () => {}, ...extra,
    });
    const all = [
        cmd("run", "Run backtest", { keys: "Ctrl+Enter" }),
        cmd("tab-config", "Go to Config tab"),
        cmd("check", "Check config"),
        cmd("save", "Save config and scripts", { disabled: true, hint: "Config tab only" }),
        cmd("zoom-reset", "Reset zoom"),
    ];
    const ids = (q: string) => filterCommands(all, q).map((c) => c.id);
    eq("empty lists everything", ids(""), all.map((c) => c.id));
    eq("prefix first", ids("check"), ["check"]);
    // Every word must match, in any order and any case.
    eq("words in any order", ids("CONFIG go"), ["tab-config"]);
    // A label starting with the query outranks one merely containing it.
    eq("ranked", ids("config"), ["tab-config", "check", "save"]);
    eq("word start", ids("zoom"), ["zoom-reset"]);
    eq("nothing", ids("xyzzy"), []);
    const key = (k: string, mods: { ctrl?: boolean; meta?: boolean; shift?: boolean }) =>
        shortcutFor({ key: k, ctrlKey: !!mods.ctrl, metaKey: !!mods.meta, shiftKey: !!mods.shift, altKey: false });
    eq("ctrl+shift+P", key("P", { ctrl: true, shift: true }), "palette");
    // On a Mac it is Cmd+Shift+P, and WebKit reports the key lower-case there.
    eq("cmd+shift+p", key("p", { meta: true, shift: true }), "palette");
    // The Slack/Linear habit still works.
    eq("ctrl+k", key("k", { ctrl: true }), "palette");
    // Printed the way each platform presses it.
    eq("palette keys", keysFor("palette", false), "Ctrl+Shift+P");
    eq("palette keys on a mac", keysFor("palette", true), "Cmd+Shift+P");
    eq("run on a mac", keysFor("run", true), "Cmd+Enter");
    eq("only the modifier", displayKeys("Ctrl+1", true), "Cmd+1");

    const html = renderToString(<CommandPalette commands={all} onClose={() => {}} />);
    for (const want of ["Type a command", "Run backtest", "Ctrl+Enter", "Config tab only"]) {
        if (!html.includes(want)) throw new Error(`palette lacks ${want}`);
    }
    // A disabled command is listed, marked, and says why.
    if (!html.includes('aria-disabled="true"')) throw new Error("disabled command not marked");
});

check("zoom keys work with Ctrl everywhere and Cmd on a Mac", () => {
    const z = (key: string, mods: { ctrl?: boolean; meta?: boolean; alt?: boolean } = {}) =>
        zoomFor({ key, ctrlKey: !!mods.ctrl, metaKey: !!mods.meta, shiftKey: false, altKey: !!mods.alt });
    eq("ctrl+=", z("=", { ctrl: true }), "in");
    eq("cmd+=", z("=", { meta: true }), "in");
    eq("cmd++ (numpad)", z("+", { meta: true }), "in");
    eq("cmd+-", z("-", { meta: true }), "out");
    eq("cmd+0", z("0", { meta: true }), "reset");
    eq("ctrl+up", z("ArrowUp", { ctrl: true }), "in");
    // Cmd+arrow moves the caret on a Mac; never a zoom.
    eq("cmd+up", z("ArrowUp", { meta: true }), null);
    eq("plain -", z("-"), null);
    eq("altgr", z("0", { ctrl: true, alt: true }), null);
    // No shortcut action shadows a zoom key.
    eq("ctrl+0 not a shortcut", shortcutFor({ key: "0", ctrlKey: true, metaKey: false, shiftKey: false, altKey: false }), null);
});

check("a disabled Run says why, and the reason agrees with the button", () => {
    const ranges = { AAPL: ["2010-01-04", "2026-08-07"], MSFT: ["2010-01-04", "2026-08-07"] };
    const ok: SimpleForm = {
        ...BLANK, name: "A", tickers: ["AAPL"], evenSplit: true, allocations: { AAPL: 100 },
        startDate: "2015-01-02", endDate: "2020-01-02",
    };
    eq("runnable", runBlocker([ok], ranges, "db", false), "");
    eq("running", runBlocker([ok], ranges, "db", true), "a run is already going");
    if (!runBlocker([ok], ranges, "", false).startsWith("choose a database")) throw new Error("no db");
    eq("empty", runBlocker([{ ...ok, tickers: [], allocations: {} }], ranges, "db", false), "add at least one holding");
    eq("zero weights",
        runBlocker([{ ...ok, evenSplit: false, allocations: { AAPL: 0 } }], ranges, "db", false),
        "give at least one holding a percentage above 0");
    // A date problem continues the sentence: lower-case word, no full stop...
    eq("reversed", runBlocker([{ ...ok, startDate: "2021-01-04" }], ranges, "db", false),
        "start date is after the end date");
    // ...but a ticker keeps its case.
    const early = runBlocker([{ ...ok, startDate: "2005-01-03" }], ranges, "db", false);
    if (!early.startsWith("data for AAPL starts")) throw new Error(early);
    // With several cards, it names the one to look at.
    eq("named", runBlocker([ok, { ...ok, name: "B", tickers: [], allocations: {} }], ranges, "db", false),
        "B: add at least one holding");
    if (!runBlocker([ok, { ...ok }], ranges, "db", false).includes('both called "A"')) {
        throw new Error("duplicate names not explained");
    }
    // portfolioRunnable is defined from the blocker, so they cannot disagree.
    for (const f of [ok, { ...ok, tickers: [] }, { ...ok, startDate: "2021-01-04" }]) {
        eq(`agree ${JSON.stringify(f.tickers)} ${f.startDate}`,
            portfolioRunnable(f, ranges), portfolioBlocker(f, ranges) === "");
    }
});

check("the run timer counts whole seconds, then minutes", () => {
    eq("0", formatElapsed(0), "0s");
    eq("7.9s", formatElapsed(7900), "7s");
    eq("59s", formatElapsed(59999), "59s");
    eq("1m", formatElapsed(60000), "1m 00s");
    eq("1m05", formatElapsed(65000), "1m 05s");
    eq("12m", formatElapsed(720000), "12m 00s");
    // A clock that went backwards never shows a negative time.
    eq("negative", formatElapsed(-500), "0s");
    const html = renderToString(<RunTimer since={Date.now() - 3000} />);
    if (!/run-timer">[34]s</.test(html)) throw new Error(html);
});

check("double-clicking the results bar steps min -> normal -> max -> normal", () => {
    eq("min", cycleResultsLayout("min"), "normal");
    eq("normal", cycleResultsLayout("normal"), "max");
    eq("max", cycleResultsLayout("max"), "normal");
});

check("a drag applies at most once per frame, and its last position", () => {
    const g = globalThis as any;
    const saved = [g.requestAnimationFrame, g.cancelAnimationFrame];
    const queued = new Map<number, () => void>();
    let next = 1;
    g.requestAnimationFrame = (cb: () => void) => (queued.set(next, cb), next++);
    g.cancelAnimationFrame = (id: number) => queued.delete(id);
    try {
        const seen: number[] = [];
        const f = perFrame((v: number) => seen.push(v));
        f.push(1);
        f.push(2);
        f.push(3);
        eq("frames requested for three moves", queued.size, 1);
        for (const [id, cb] of [...queued]) (queued.delete(id), cb());
        eq("applied after one frame", seen, [3]);
        f.push(4);
        f.flush();
        eq("mouseup applies the pending move at once", seen, [3, 4]);
        eq("and cancels its frame", queued.size, 0);
        f.flush();
        eq("a second flush applies nothing", seen, [3, 4]);
    } finally {
        [g.requestAnimationFrame, g.cancelAnimationFrame] = saved;
    }
});

check("a drag ends on release wherever it happens, and never sticks", () => {
    const g = globalThis as any;
    const saved = [g.requestAnimationFrame, g.cancelAnimationFrame, g.document];
    // Frames run immediately here; coalescing is covered by the perFrame case.
    g.requestAnimationFrame = (cb: () => void) => (cb(), 0);
    g.cancelAnimationFrame = () => {};
    g.document = { body: { style: {} as Record<string, string> } };
    try {
        const listeners = new Map<string, (e: any) => void>();
        let captured = -1;
        const el = {
            setPointerCapture: (id: number) => (captured = id),
            addEventListener: (t: string, f: (e: any) => void) => listeners.set(t, f),
            removeEventListener: (t: string) => listeners.delete(t),
        };
        const moves: number[][] = [];
        let ended = 0;
        trackDrag(
            { currentTarget: el as any, pointerId: 7, clientX: 100, clientY: 500 },
            "ns-resize",
            (dx, dy) => moves.push([dx, dy]),
            () => ended++,
        );
        eq("pointer captured", captured, 7);
        eq("cursor held for the drag", g.document.body.style.cursor, "ns-resize");
        listeners.get("pointermove")!({ clientX: 110, clientY: 350 });
        eq("delta reported", moves, [[10, -150]]);
        // Released far outside the handle: capture still routes it here.
        listeners.get("pointerup")!({ clientX: 1399, clientY: 899 });
        eq("ended once", ended, 1);
        eq("every listener removed", listeners.size, 0);
        eq("cursor restored", g.document.body.style.cursor, "");
    } finally {
        [g.requestAnimationFrame, g.cancelAnimationFrame, g.document] = saved;
    }
});

check("every result panel can be popped out, and a popped-out one leaves a stub", () => {
    const { results, verdicts } = load("full");
    const docked = renderToString(
        <ResultsView results={results} fontSize={16} verdicts={verdicts} />,
    );
    // One pop-out button per panel header, headline and collapsible alike.
    const panels = (docked.match(/class="panel[ "]/g) ?? []).length;
    const buttons = (docked.match(/aria-label="Open [^"]+ in its own window"/g) ?? []).length;
    if (panels === 0 || buttons !== panels) {
        throw new Error(`${panels} panels but ${buttons} pop-out buttons`);
    }
    if (docked.includes("float-win")) throw new Error("a window rendered with nothing floated");

    const out = { equity: { x: 10, y: 20, w: 700, h: 400, max: false } };
    const floated = withStorage(
        { "backtester-float-panels": JSON.stringify(out) },
        () => renderToString(<ResultsView results={results} fontSize={16} verdicts={verdicts} />),
    );
    const text = floated.replace(/<!-- -->/g, "");
    if (!text.includes("in its own window")) throw new Error("no stub left in the strip");
    if (!/class="float-win"[^>]*aria-label="Equity curve"/.test(text)) {
        throw new Error(`the equity curve has no window:\n${text.slice(0, 400)}`);
    }
    // The window holds the real chart, not a copy or a placeholder.
    const win = text.slice(text.indexOf('class="float-win"'));
    if (!win.includes("equity-chart")) throw new Error("the window is empty");
});

check("a collapsed panel renders its header only, and opens from storage", () => {
    const body = <p>body</p>;
    const closed = renderToString(<ResultPanel id="t" title="T" collapsible>{body}</ResultPanel>);
    if (closed.includes("<p>body</p>")) throw new Error("a closed panel rendered its body");
    const open = withStorage({ "backtester-results-panels": '{"t":true}' }, () =>
        renderToString(<ResultPanel id="t" title="T" collapsible>{body}</ResultPanel>),
    );
    if (!open.includes("<p>body</p>")) throw new Error("stored open state ignored");
    // Outside a dock (no ResultsView) there is nothing to pop out into.
    if (closed.includes("in its own window")) throw new Error(closed);
    const withDock = renderToString(
        <DockProvider
            value={{
                floats: {},
                order: [],
                float: () => {},
                dock: () => {},
                focus: () => {},
                setGeo: () => {},
            }}
        >
            <ResultPanel id="t" title="T">{body}</ResultPanel>
        </DockProvider>,
    );
    if (!withDock.includes("Open T in its own window")) throw new Error(withDock);
});

check("a window can never be lost off screen or shrunk ungrabbable", () => {
    const g = (x: number, y: number, w: number, h: number): Geo => ({ x, y, w, h, max: false });
    // Placed on a big monitor, restored on a small one.
    eq("pulled back on screen", clampGeo(g(3000, 2000, 900, 600), 1280, 720), g(1160, 680, 900, 600));
    eq("never negative", clampGeo(g(-400, -50, 900, 600), 1280, 720), g(0, 0, 900, 600));
    eq("never tiny", clampGeo(g(10, 10, 20, 10), 1280, 720), g(10, 10, 280, 160));
    eq("never bigger than the screen", clampGeo(g(0, 0, 5000, 5000), 1280, 720), g(0, 0, 1280, 720));
    const a = defaultGeo(0, 1400, 900);
    const b = defaultGeo(1, 1400, 900);
    eq("first window centred", [a.x + a.w / 2, a.y + a.h / 2], [700, 450]);
    eq("second window offset from the first", [b.x - a.x, b.y - a.y], [28, 28]);
});

check("stored window positions are input: bad entries are dropped", () => {
    const stored = JSON.stringify({
        equity: { x: 1, y: 2, w: 300, h: 200, max: true },
        metrics: { x: "1", y: 2, w: 300, h: 200, max: false },
        drawdowns: { x: 1, y: 2, w: 300, h: 200 },
        yearly: null,
    });
    const got = withStorage({ "backtester-float-panels": stored }, readFloats);
    eq("only the valid entry survives", Object.keys(got), ["equity"]);
    eq("garbage is empty, not a throw", withStorage({ "backtester-float-panels": "{[" }, readFloats), {});
});

check("a kept-mounted tab hides, and repaints only when it shows", () => {
    const shown = renderToString(<Pane active={true} className="data-pane"><i>q</i></Pane>);
    const hidden = renderToString(<Pane active={false} className="data-pane"><i>q</i></Pane>);
    if (shown.includes("display:none")) throw new Error(shown);
    if (!hidden.includes("display:none") || !hidden.includes("<i>q</i>")) {
        throw new Error(`a hidden pane must keep its content, just not show it:\n${hidden}`);
    }
    const p = (active: boolean) => ({ active, className: "x", children: null });
    eq("hidden -> hidden skips", skipRender(p(false), p(false)), true);
    eq("hidden -> shown renders", skipRender(p(false), p(true)), false);
    eq("shown -> hidden renders", skipRender(p(true), p(false)), false);
    eq("shown -> shown renders", skipRender(p(true), p(true)), false);
});

check("the results bar minimises, maximises and restores", () => {
    // Each button toggles its own state off and switches straight across.
    eq("min from normal", nextResultsLayout("normal", "min"), "min");
    eq("min again restores", nextResultsLayout("min", "min"), "normal");
    eq("max from min", nextResultsLayout("min", "max"), "max");
    eq("max again restores", nextResultsLayout("max", "max"), "normal");
    // Stored state is input; maximised never comes back at launch.
    eq("stored min", parseResultsLayout("min"), "min");
    eq("stored max", parseResultsLayout("max"), "normal");
    eq("junk", parseResultsLayout("sideways"), "normal");
    eq("absent", parseResultsLayout(null), "normal");

    const noop = () => {};
    const bar = (view: "normal" | "min" | "max", count = 0) =>
        renderToString(
            <ResultsBar view={view} onView={noop} onResizeStart={noop} count={count} />,
        );
    const normal = bar("normal", 3);
    for (const want of ["Minimise results", "Maximise results", "Results · 3", "resize-handle"]) {
        if (!normal.includes(want)) throw new Error(`normal bar lacks ${want}:\n${normal}`);
    }
    if (!bar("min").includes("Restore results") || bar("min").includes("Minimise results")) {
        throw new Error(`a minimised bar must offer restore:\n${bar("min")}`);
    }
    if (!bar("max").includes("Restore results") || bar("max").includes("Maximise results")) {
        throw new Error(`a maximised bar must offer restore:\n${bar("max")}`);
    }
    if (bar("normal", 0).includes("Results ·")) throw new Error("no count before a run");
});


// ---------------------------------------------------------------------------
// The rules card (T135). Menu and readings are the engine's, written by
// ui/rules_form_test.go, so these render the sentences the app will show.

const RULES_MENU: backtest.RuleEntry[] = JSON.parse(
    readFileSync(join(testdata, "rules-menu.json"), "utf8"),
).map((e: unknown) => backtest.RuleEntry.createFrom(e));
const RULES_READINGS: Record<string, main.RulesReading> = JSON.parse(
    readFileSync(join(testdata, "rules-readings.json"), "utf8"),
);
const PROPOSED_RULES: RulesForm = {
    rebalance: "monthly",
    rank: "return_126",
    pick: 2,
    keep: 0,
    filter: "above_sma_200",
    weight: "equal",
    riskOff: "benchmark_below_sma_200",
};

check("every rules menu entry round-trips through the card's controls", () => {
    for (const e of RULES_MENU) {
        // ruleArgs: Go sends null, not [], for an entry with no numbers --
        // which this case first caught as a crash in the card.
        const written = formatRule(e, ruleArgs(e).map((a) => a.default));
        const back = parseRule(RULES_MENU, e.kind, written);
        if (!back || back.entry.base !== e.base) {
            throw new Error(`${e.kind} ${written} does not parse back to ${e.base}`);
        }
        // Out of range is clamped, never written.
        if (ruleArgs(e).length > 0) {
            const big = formatRule(e, ruleArgs(e).map((a) => a.max + 1000));
            const nums = parseRule(RULES_MENU, e.kind, big)!.args;
            ruleArgs(e).forEach((a, i) => eq(`${e.form} clamp`, nums[i], a.max));
        }
    }
    if (parseRule(RULES_MENU, "rank", "retrun_126") !== null) {
        throw new Error("a typo parsed as a menu entry");
    }
    if (parseRule(RULES_MENU, "rank", "momentum_126") !== null) {
        throw new Error("momentum_126 is not on the menu (momentum_12_1 is)");
    }
});

check("Keep never falls below Pick, and Pick never exceeds the holdings", () => {
    eq("keep raised", settleCounts({ ...PROPOSED_RULES, pick: 3, keep: 2 }, 5).keep, 3);
    eq("pick capped", settleCounts({ ...PROPOSED_RULES, pick: 9 }, 4).pick, 4);
    eq("pick at least 1", settleCounts({ ...PROPOSED_RULES, pick: 0 }, 4).pick, 1);
    const unranked = settleCounts({ ...PROPOSED_RULES, rank: "", pick: 3, keep: 5 }, 4);
    eq("no rank, no counts", [unranked.pick, unranked.keep], [0, 0]);
});

check("RulesCard / empty, proposed, edited and invalid", () => {
    const card = (rules: RulesForm, reading: main.RulesReading | null, hasBenchmark = true) =>
        renderToString(
            <RulesCard
                menu={RULES_MENU}
                rules={rules}
                reading={reading}
                tickerCount={4}
                hasBenchmark={hasBenchmark}
                onChange={noop}
                onAskAssistant={noop}
            />,
        ).replace(/<!-- -->/g, "");
    const unescape = (h: string) => h.replace(/&amp;/g, "&");

    const empty = unescape(card(DEFAULT_RULES, RULES_READINGS.default));
    for (const want of [RULES_READINGS.default.sentence, "How often to decide",
        "No ranking", "Describe your idea to the assistant"]) {
        if (!empty.includes(want)) throw new Error(`empty card lacks ${want}`);
    }

    const proposed = unescape(card(PROPOSED_RULES, RULES_READINGS.proposed));
    if (!proposed.includes(RULES_READINGS.proposed.sentence)) {
        throw new Error("the proposed card does not show the engine's sentence");
    }
    // The chosen entries are selected, with their numbers in boxes.
    for (const want of ['value="126"', 'value="200"', "Hold the top", "Keep while in the top"]) {
        if (!proposed.includes(want)) throw new Error(`proposed card lacks ${want}`);
    }

    const edited = unescape(card({ ...PROPOSED_RULES, rebalance: "weekly", keep: 3 },
        RULES_READINGS.edited));
    if (!edited.includes("stays in the top 3") || !edited.includes('value="3"')) {
        throw new Error("the edited card does not show the edit");
    }

    const invalid = card({ ...DEFAULT_RULES, riskOff: "benchmark_below_sma_200" },
        RULES_READINGS.invalid, false);
    if (!invalid.includes("These rules cannot run") || !invalid.includes("Benchmark")) {
        throw new Error(`invalid card does not say why:\n${invalid}`);
    }
    // Still set, so still editable: the refusal has to be clearable here.
    if (/<select[^>]*disabled[^>]*>(?:(?!<\/select>).)*benchmark_below_sma/s.test(invalid)) {
        throw new Error("a set risk-off was locked with no way to clear it");
    }
    if (!card(DEFAULT_RULES, null).includes("Reading the rules")) {
        throw new Error("a pending reading is not shown as pending");
    }
    if (!card(DEFAULT_RULES, RULES_READINGS.default, false).includes("pick a comparison above")) {
        throw new Error("risk-off without a benchmark does not say why it is off");
    }
});

check("PortfolioCard / offers rules and shows the rules card when chosen", () => {
    const card = (f: SimpleForm) =>
        renderToString(
            <PortfolioCard
                form={f}
                onChange={noop}
                onRemove={null}
                otherNames={[]}
                defaultName="Portfolio 1"
                dbPath=""
                available={[]}
                names={{}}
                ranges={{}}
                pickable={[]}
                showBaskets={false}
                rulesMenu={RULES_MENU}
                rulesReading={RULES_READINGS.proposed}
                onDescribeIdea={noop}
            />,
        ).replace(/&amp;/g, "&");
    if (!card(form).includes("Describe my own idea")) {
        throw new Error("the strategy picker does not offer rules");
    }
    const ruled = card({ ...form, strategy: RULES_STRATEGY, rules: PROPOSED_RULES });
    if (!ruled.includes(RULES_READINGS.proposed.sentence)) {
        throw new Error("a rules card does not show the sentence");
    }
    if (ruled.includes("Open a position in every holding")) {
        throw new Error("a rules card still shows buy-and-hold's description");
    }
});

check("buildToml writes rules instead of a strategy, and nothing unset", () => {
    const tickers = ["AAPL", "MSFT", "JNJ"];
    const toml = buildToml([{ ...form, tickers, strategy: RULES_STRATEGY,
        rules: { ...PROPOSED_RULES, keep: 3 } }]);
    for (const want of ["[Portfolio.Rules]", 'Rebalance = "monthly"', 'Rank = "return_126"',
        "Pick = 2", "Keep = 3", 'Filter = "above_sma_200"',
        'RiskOff = "benchmark_below_sma_200"']) {
        if (!toml.includes(want)) throw new Error(`missing ${want}:\n${toml}`);
    }
    for (const never of ["Strategy =", "Weight =", "Params ="]) {
        if (toml.includes(never)) throw new Error(`rules config writes ${never}:\n${toml}`);
    }
    // Fixed weights come from the card's own percentages.
    const fixed = buildToml([{ ...form, strategy: RULES_STRATEGY,
        rules: { ...DEFAULT_RULES, weight: "allocations" } }]);
    for (const want of ['Weight = "allocations"', "[Portfolio.Rules.Allocations]", "AAPL = 50.0",
        '"9988.HK" = 20.0']) {
        if (!fixed.includes(want)) throw new Error(`missing ${want}:\n${fixed}`);
    }
    eq("allocations only for that weight", Object.keys(toRulesConfig(DEFAULT_RULES,
        form.allocations, form.tickers).allocations).length, 0);
    // A rules value with no rules object stays buy-and-hold's output shape
    // rather than emitting an empty table.
    if (!buildToml([{ ...form, strategy: RULES_STRATEGY }]).includes("Strategy =")) {
        throw new Error("a rules strategy with no rules wrote no strategy at all");
    }
});

// What a rules card generates, pinned -- and ui/rules_form_test.go runs the
// same golden through the engine's config check, so the TOML this writes is
// TOML the engine accepts, not just TOML that looks right.
const RULES_GOLDEN_FORMS: SimpleForm[] = [
    { ...form, name: "Momentum rules", tickers: ["AAPL", "MSFT", "JNJ"],
        strategy: RULES_STRATEGY, rules: { ...PROPOSED_RULES, keep: 3 } },
    { ...form, name: "Fixed rules", strategy: RULES_STRATEGY,
        rules: { ...DEFAULT_RULES, rebalance: "quarterly", weight: "allocations" } },
];

check("buildToml matches the golden rules config", () => {
    const want = readFileSync(join(testdata, "golden-rules.toml"), "utf8");
    eq("golden-rules.toml", buildToml(RULES_GOLDEN_FORMS), want);
});

check("a refused rule set stops the run, and the Run button carries the promise", () => {
    const two = [{ name: "A" }, { name: "B" }];
    eq("plain", rulesBlocker(two, [undefined, undefined]), "");
    eq("pending", rulesBlocker([{ name: "A" }], [null]), "the rules are still being checked");
    const why = rulesBlocker(two, [undefined, RULES_READINGS.invalid]);
    if (!why.startsWith("B: fix the rules") || !why.includes("Benchmark")) throw new Error(why);
    eq("label", runLabel([RULES_READINGS.proposed]),
        `Run: ${RULES_READINGS.proposed.headline}`);
    eq("label, two portfolios", runLabel([RULES_READINGS.proposed, undefined]), "Run backtest");
    eq("label, no rules", runLabel([undefined]), "Run backtest");
    if (!RULES_READINGS.proposed.sentence.includes(RULES_READINGS.proposed.headline)) {
        throw new Error("the button's words are not a piece of the sentence");
    }
});

check("a session keeps its rules, and drops malformed ones", () => {
    const withRules = { ...form, strategy: RULES_STRATEGY, rules: PROPOSED_RULES };
    const ok: Session = { forms: [withRules], hypothesis: emptyHypothesis };
    if (JSON.stringify(parseSession(JSON.stringify(ok), fallbackSession)) !== JSON.stringify(ok)) {
        throw new Error("a session with rules did not round-trip");
    }
    const bad = { forms: [{ ...withRules, rules: { rebalance: "monthly" } }], hypothesis: emptyHypothesis };
    if (parseSession(JSON.stringify(bad), fallbackSession) !== fallbackSession) {
        throw new Error("a half rules object was restored");
    }
});

check("only a rules block offers to go to the form", () => {
    const msg = (code: string) =>
        renderToString(
            <ChatMessage
                m={{ role: "assistant", content: "```toml\n" + code + "\n```" }}
                busy={false}
                onApplyToml={noop}
                onApplyLua={noop}
            />,
        );
    if (!msg('[portfolio.Rules]\nRebalance = "monthly"').includes("Use these rules in the form")) {
        throw new Error("a rules block has no Use-these-rules action");
    }
    if (msg("[Output]\npath = \"x\"").includes("Use these rules")) {
        throw new Error("a block with no rules offers to use them");
    }
    if (hasRulesBlock("# [portfolio.Rules] mentioned in a comment")) {
        throw new Error("a comment was taken for a rules block");
    }
});

check("rules entries the card lists are exactly the engine's", () => {
    for (const kind of ["rebalance", "rank", "filter", "weight", "riskoff"]) {
        if (entriesOf(RULES_MENU, kind).length === 0) throw new Error(`no ${kind} entries`);
    }
});


// The library and the starter examples, restated as rules (T136). The twins
// are the engine's (App.LibraryRules, written by ui/rules_form_test.go), and
// src/backtest holds each one to the cent against its script.
// Served as a list (Wails emits no class for a map-valued struct); keyed by
// file here exactly as SimpleForm keys it.
const LIBRARY_TWINS: Record<string, main.LibraryTwin> = Object.fromEntries(
    (JSON.parse(readFileSync(join(testdata, "library-rules.json"), "utf8")) as main.LibraryTwin[])
        .map((t) => [t.file, t]),
);

check("a rules example is exactly its library twin", () => {
    const ruled = EXAMPLES.filter((ex) => ex.rules);
    if (ruled.length === 0) throw new Error("no starter example is written as rules");
    for (const ex of ruled) {
        const twin = ex.twin ? LIBRARY_TWINS[ex.twin] : undefined;
        if (!twin) throw new Error(`${ex.id}: twin ${ex.twin} is not one the engine serves`);
        eq(`${ex.id} rules`, ex.rules, fromRulesConfig(twin.rules));
        eq(`${ex.id} strategy`, ex.strategy, RULES_STRATEGY);
    }
    // An example that is not rules says why it stays a script.
    for (const ex of EXAMPLES) {
        if (!ex.rules && ex.strategy && !ex.scriptReason) {
            throw new Error(`${ex.id} is a script and does not say why`);
        }
    }
});

check("the starter examples show their sentence, or why they are a script", () => {
    const html = renderToString(
        <Examples ranges={EXAMPLE_RANGES} onPick={noop} twins={LIBRARY_TWINS} />,
    ).replace(/&amp;/g, "&");
    const momentum = LIBRARY_TWINS["momentum_rotation.lua"].sentence;
    if (!html.includes(momentum)) throw new Error("the rules example does not show its sentence");
    for (const ex of EXAMPLES.filter((e) => e.scriptReason)) {
        if (!html.includes(ex.scriptReason!)) throw new Error(`${ex.id} hides why it is a script`);
    }
    // Without the twins loaded (no backend yet), nothing breaks.
    renderToString(<Examples ranges={EXAMPLE_RANGES} onPick={noop} />);
});

check("a library card offers its twin as rules", () => {
    const card = (strategy: string, twin?: main.LibraryTwin) =>
        renderToString(
            <PortfolioCard
                form={{ ...form, strategy }}
                onChange={noop}
                onRemove={null}
                otherNames={[]}
                defaultName="Portfolio 1"
                dbPath=""
                available={[]}
                names={{}}
                ranges={{}}
                pickable={[backtest.StrategyCard.createFrom({ File: strategy, Idea: "idea",
                    Works: "w", Fails: "f", Sweep: "", Baseline: "b" })]}
                showBaskets={false}
                twin={twin}
            />,
        ).replace(/&amp;/g, "&");
    const html = card("momentum_rotation.lua", LIBRARY_TWINS["momentum_rotation.lua"]);
    if (!html.includes("As rules:") || !html.includes("Use as rules")) {
        throw new Error("the momentum card does not offer its rules");
    }
    if (card("trend_following.lua").includes("As rules:")) {
        throw new Error("a script with no exact twin claims one");
    }
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
    writeFileSync(join(testdata, "golden-cashflow.toml"), buildToml([cashflowForm]));
    writeFileSync(join(testdata, "golden-rules.toml"), buildToml(RULES_GOLDEN_FORMS));
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
