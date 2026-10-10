// The Lua editor's knowledge of the engine.
//
// A strategy has 34 globals available and the editor used to know none of
// them: plain syntax highlighting, no completion, no hover. The only way to
// learn that it is `sma(t, day, period)` and not `sma(t, period)` was to read
// strategies/README.md in another window, and getting it wrong was a runtime
// error on day 0 of a full backtest.
//
// The descriptions come from Go (App.LuaAPI), which is pinned by a test to the
// SetGlobal calls themselves, so what the editor offers is what the engine
// registers. Everything here is pure and takes the list as an argument, so the
// smoke run can exercise it without Monaco or a backend.
import type { backtest } from "../wailsjs/go/models";

export type LuaGlobal = backtest.LuaGlobal;

// argsOf pulls the argument names out of a signature: "sma(ticker, day,
// period)" -> ["ticker", "day", "period"]. A value ("tickers") and a
// no-argument call ("cash()") both give [].
export function argsOf(signature: string): string[] {
    const open = signature.indexOf("(");
    if (open < 0 || !signature.endsWith(")")) return [];
    const inner = signature.slice(open + 1, -1).trim();
    if (inner === "") return [];
    return inner.split(",").map((a) => a.trim()).filter(Boolean);
}

// snippetFor turns a signature into Monaco snippet syntax, so accepting a
// completion drops the cursor on the first argument and Tab walks the rest:
// "sma(ticker, day, period)" -> "sma(${1:ticker}, ${2:day}, ${3:period})".
//
// `...` is left as a literal placeholder rather than a tab stop -- log takes
// any number of values, so there is no argument to walk to.
export function snippetFor(g: LuaGlobal): string {
    if (g.kind !== "function") return g.name;
    const args = argsOf(g.signature);
    if (args.length === 0) return `${g.name}()`;
    if (args.length === 1 && args[0] === "...") return `${g.name}($0)`;
    const body = args
        .map((a, i) => `\${${i + 1}:${a}}`)
        .join(", ");
    return `${g.name}(${body})`;
}

// documentation is what shows on hover and beside a completion: the signature,
// what comes back, and the sentence that says what it is for. Markdown,
// because Monaco renders it in both places.
export function documentation(g: LuaGlobal): string {
    const lines = ["```lua", g.signature, "```", ""];
    if (g.returns) lines.push(`**Returns** \`${g.returns}\``, "");
    lines.push(g.summary);
    return lines.join("\n");
}

// detailFor is the grey line beside the name in the completion list: the
// return type where there is one, and the group otherwise, so a list of 34
// names is scannable by what each one gives you.
export function detailFor(g: LuaGlobal): string {
    return g.returns ? `${g.signature} → ${g.returns}` : g.signature;
}

// findGlobal is the hover lookup: an exact name match, or nothing. Deliberately
// not fuzzy -- hovering `sma` must never explain `sma_something`.
export function findGlobal(api: LuaGlobal[], word: string): LuaGlobal | undefined {
    return api.find((g) => g.name === word);
}

// A Lua strategy is one function, and the blank file gives no clue what shape
// it takes. Offered as a completion like everything else, so it is found by
// typing rather than by being told.
export const STEP_SNIPPET = {
    name: "step",
    detail: "step(day) — the function the engine calls once per trading day",
    documentation:
        "Every strategy defines this one global function. `day` counts from 0, " +
        "and the last day is `num_days() - 1`.",
    snippet: [
        "function step(day)",
        "\t${1:-- one ticker at a time; `tickers` is 1-indexed}",
        "\tfor i = 1, #tickers do",
        "\t\tlocal t = tickers[i]",
        "\t\t$0",
        "\tend",
        "end",
    ].join("\n"),
};

// A suggestion, in the editor-independent shape: everything the completion
// list needs, with none of Monaco's enums. luaEditor.ts maps these onto
// Monaco's types, which keeps the decisions -- what is offered, in what order,
// with what text -- testable in the smoke run.
export type LuaSuggestion = {
    label: string;
    detail: string;
    documentation: string;
    snippet: string;
    kind: "function" | "value" | "snippet";
    // sortText orders the list. `step` first because a blank file needs it
    // before anything else, then the globals in the order the engine groups
    // them, so completion reads like the reference rather than like an
    // alphabetised dump.
    sortText: string;
};

export function luaSuggestions(api: LuaGlobal[]): LuaSuggestion[] {
    const out: LuaSuggestion[] = [
        {
            label: STEP_SNIPPET.name,
            detail: STEP_SNIPPET.detail,
            documentation: STEP_SNIPPET.documentation,
            snippet: STEP_SNIPPET.snippet,
            kind: "snippet",
            sortText: "0",
        },
    ];
    api.forEach((g, i) => {
        out.push({
            label: g.name,
            detail: detailFor(g),
            documentation: documentation(g),
            snippet: snippetFor(g),
            kind: g.kind === "value" ? "value" : "function",
            // Padded so 10 sorts after 9 rather than after 1.
            sortText: `1${String(i).padStart(3, "0")}`,
        });
    });
    return out;
}

