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

import ResultsView, { differingWindows } from "./ResultsView";
import ConsolePanel from "./ConsolePanel";
import SimpleFormView from "./SimpleForm";
import { CRITERION_OPS, buildToml, buildHypothesis } from "./buildToml";
import { searchTickers } from "./tickerSearch";
import { duplicateNames, portfolioRunnable } from "./portfolioCheck";
import {
    BASKETS,
    BLANK,
    CRITERION_PRESETS,
    EXAMPLES,
    basketTickers,
    buildExample,
    exampleHypothesis,
} from "./examples";
import { chipLabel } from "./paths";
import {
    argsOf,
    detailFor,
    documentation,
    findGlobal,
    luaSuggestions,
    snippetFor,
} from "./luaApi";
import { parseSession } from "./session";
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

// A console row that knows where it happened offers to go there. The message
// has always said "sma_cross.lua:41"; the reader had to find line 41 by hand.
check("a console row with a position offers to open it", () => {
    const positioned = backtest.Diagnostic.createFrom({
        level: "error",
        portfolio: "P",
        message: "lua strategy: strategies/sma_cross.lua:41: attempt to compare nil with number",
        script: "strategies/sma_cross.lua",
        line: 41,
    });
    const plain = backtest.Diagnostic.createFrom({
        level: "warn",
        portfolio: "P",
        message: "benchmark $SP500 covers only part of the window",
        script: "",
        line: 0,
    });

    const withJump = renderToString(
        <ConsolePanel
            diagnostics={[positioned, plain]}
            fontSize={16}
            onJump={() => {}}
        />,
    );
    if (!withJump.includes("sma_cross.lua:41")) {
        throw new Error("the position chip never rendered");
    }
    if ((withJump.match(/console-jump/g) ?? []).length !== 1) {
        throw new Error(
            "a diagnostic with no script offered a jump — it would open " +
                "whatever file happened to be in the editor",
        );
    }

    // Without a handler there is nothing to jump to, and a dead link is worse
    // than plain text.
    const withoutJump = renderToString(
        <ConsolePanel diagnostics={[positioned]} fontSize={16} />,
    );
    if (withoutJump.includes("console-jump")) {
        throw new Error("a jump chip rendered with no handler behind it");
    }
    if (!withoutJump.includes("attempt to compare nil")) {
        throw new Error("the message itself went missing");
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
