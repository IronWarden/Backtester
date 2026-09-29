import { useEffect, useRef, useState } from "react";

const BASE_FONT_SIZE = 16;
const MIN_FONT_SIZE = 9;
const MAX_FONT_SIZE = 40;
const clampFont = (n: number) =>
    Math.max(MIN_FONT_SIZE, Math.min(MAX_FONT_SIZE, n));

// DPI-aware default zoom. On a hi-res display without OS scaling (e.g. 4K
// at devicePixelRatio 1) screen.width reports ~3840 CSS px and a 16px base
// font renders tiny, with the em-based max-widths leaving the screen mostly
// empty. Scale the default so the UI occupies a 4K screen the way it
// occupies a 1080p one; with OS scaling active, screen.width is already
// divided down and this returns the plain base size.
function defaultFontSize(): number {
    const s = Math.min(
        window.screen.width / 1920,
        window.screen.height / 1080,
    );
    return clampFont(Math.round(BASE_FONT_SIZE * Math.max(1, s)));
}
import Editor from "@monaco-editor/react";
import {
    CheckLuaScript,
    DatabaseNotice,
    LuaAPI,
    LuaSkeletons,
    Version,
    DefaultDBPath,
    FileExists,
    FindLuaPathInConfig,
    PickConfigFile,
    PickDBFile,
    ReadFile,
    RunBacktest,
    CheckConfig,
    WriteFile,
} from "../wailsjs/go/main/App";
import { backtest, main } from "../wailsjs/go/models";
import SimpleForm from "./SimpleForm";
import { buildToml } from "./buildToml";
import type {
    SimpleForm as SimpleFormData,
    HypothesisForm as HypothesisFormData,
} from "./buildToml";
import { baseName, chipLabel } from "./paths";
import { perFrame, trackDrag } from "./frame";
import ResultsBar, { parseResultsLayout, type ResultsLayout } from "./ResultsBar";
import { registerLuaLanguageSupport } from "./luaEditor";
import ResultsView from "./ResultsView";
import Pane from "./Pane";
import ErrorBoundary from "./ErrorBoundary";
import { readStored, readStoredNumber, writeStored } from "./storage";
import { readSession, writeSession } from "./session";
import type { Session } from "./session";
import ChatPanel from "./ChatPanel";
import QueryConsole from "./QueryConsole";
import "./App.css";

const today = new Date().toISOString().slice(0, 10);

// Remember the zoom level across app restarts. v2: bumped when the default
// became DPI-aware, so previously stored pre-4K-fix sizes re-default once.
const FONT_STORAGE_KEY = "backtester-font-size-v2";

// Assistant panel width in base pixels (at zoom 1), persisted like the font.
const CHAT_WIDTH_STORAGE_KEY = "backtester-chat-width";
const DEFAULT_CHAT_WIDTH = 432;
const RESULTS_HEIGHT_STORAGE_KEY = "backtester-results-height";
// Minimised / normal, remembered across launches. Maximised is not (see
// parseResultsLayout).
const RESULTS_LAYOUT_STORAGE_KEY = "backtester-results-layout";
// Tall enough to show the headline cards and the top of the equity curve
// without scrolling; the old 240 showed a single card.
const DEFAULT_RESULTS_HEIGHT = 380;
// Base pixels kept above the results panel. Config needs its toolbar, tabs
// and the editor's 200px minimum on top of the topbar and status line; the
// other tabs scroll, so they only need enough to stay usable.
const RESERVED_TOP_CONFIG = 440;
const RESERVED_TOP = 260;

type Tab = "toml" | "lua";
type Mode = "simple" | "advanced" | "data";

// Fallback DB path, relative to the app's working directory (ui/). The real
// answer comes from Go (DefaultDBPath), which knows where the binary lives;
// this is only what the field holds for the instant before that resolves, and
// it is the path a checkout has always used.
const DEFAULT_DB_PATH = "../stock_data.db";

// Where a script started from a skeleton is saved. A name of its own so
// picking a shape can never overwrite example.lua or a library script.
const SKELETON_LUA_PATH = "strategies/my_strategy.lua";

// Path used for the Lua tab when the TOML doesn't pin a specific lua: script.
// Portfolios that omit Strategy fall back to this file at run time.
const DEFAULT_LUA_PATH = "strategies/example.lua";

// The Backtest tab as it opens when there is nothing to restore. A function,
// not a constant, because the state initializer must not share one object
// with anything else -- and because `today` moves.
function defaultSession(): Session {
    return {
        forms: [
            {
                name: "Buy and Hold",
                buyingPower: 100000,
                startDate: "2020-01-01",
                endDate: today,
                tickers: [],
                evenSplit: true,
                allocations: {},
                // Empty is buy-and-hold, so an untouched form behaves exactly
                // as it did before the picker existed.
                strategy: "",
                // Deliberately NOT a zero value: an unset Benchmark leaves
                // "did this beat the market?" unanswerable, which is the
                // question simple mode exists to answer.
                benchmark: "$SP500",
                // "never" emits no [portfolio.Cashflow] block at all, so an
                // untouched form generates exactly the config it always did.
                cashflowDirection: "deposit",
                cashflowAmount: 0,
                cashflowFrequency: "never",
            },
        ],
        hypothesis: { claim: "", metric: "sharpe", op: ">=", value: "" },
    };
}

// Every key the engine reads appears below, active or commented out, so the
// Config tab doubles as the reference for what can be configured. Only the two
// portfolios' first six keys are live: the commented options are the zero
// values, and an untouched tab runs exactly what it always ran
// (ui/default_toml_test.go holds both promises). Nothing commented may read
// `Strategy = "lua:...`: FindLuaPathInConfig matches that text even behind a #,
// and would swap the script the Lua tab opens.
const DEFAULT_TOML = `# Edit your portfolio config here. Add as many [[portfolio]] blocks as
# you like — they all run and their metrics appear in the results table.
# Omit Strategy to use the Lua script open in the Lua tab.
#
# Every setting the engine understands is listed in the first block, turned
# off. Delete the leading "# " on a line to turn it on. A [portfolio.X]
# section belongs to the [[portfolio]] above it, and must come after that
# portfolio's plain settings.

[[portfolio]]
Name = "Default Lua"
BuyingPower = 100000.0
StartDate = "2020-01-01"
EndDate = "2024-01-01"
Tickers = ["AAPL", "MSFT"]

# Strategy        = ""     # empty: the Lua tab's script. Or a library script
#                          #   ("lua:strategies/rsi.lua"), "buyAndHold:equalWeights",
#                          #   "buyAndHold:greedy", "smaCross:50:200:equalWeights"
# Benchmark       = ""     # ticker to compare against, e.g. "$SP500"; never traded
# Fundamentals    = []     # reported figures a Lua strategy reads with
#                          #   fundamental(), e.g. ["Net Income", "Total Revenue"]
# AllowShort      = false  # true: a sell past zero opens a short
# DelistingReturn = 0.0    # loss taken if a holding goes bankrupt, -1.0 to 0.0

# [portfolio.Params]       # values a Lua strategy reads as params.<name>
# lookback = 126

# [portfolio.Costs]        # trading costs; all zero = no costs
# commission_per_trade = 0.0  # flat dollars per order
# commission_bps       = 0.0  # share of each order, in basis points (1 bp = 0.01%)
# slippage_bps         = 0.0  # price lost to the spread on each fill, in bp
# borrow_bps_annual    = 0.0  # yearly cost of borrowing shares you are short

# [portfolio.Margin]       # only with AllowShort; 0 = Reg T (50% / 30%)
# initial_pct     = 0.0    # collateral to open a short, e.g. 0.5 = 50%
# maintenance_pct = 0.0    # collateral to keep it open, e.g. 0.3 = 30%

# [portfolio.Cashflow]     # a recurring deposit (+) or withdrawal (-)
# Amount    = 0.0
# Frequency = ""           # "monthly", "quarterly" or "annually"
# Start     = ""           # YYYY-MM-DD; empty = StartDate

# [portfolio.Sweep]        # run every combination of these Params values
# lookback = [63, 126, 252]

# [portfolio.Validation]   # judge the end of the window as unseen data
# in_sample_end = ""       # YYYY-MM-DD between StartDate and EndDate

# [portfolio.WalkForward]  # re-pick the best Sweep setting on rolling windows
# train_days = 0           # trading days each choice is made on
# test_days  = 0           # trading days each choice is then scored on
# step_days  = 0           # 0 = test_days
# objective  = ""          # "SharpeRatio" (default), "SortinoRatio" or "AnnualReturn"

[[portfolio]]
Name = "Buy and Hold"
BuyingPower = 100000.0
StartDate = "2020-01-01"
EndDate = "2024-01-01"
Tickers = ["AAPL", "MSFT"]
Strategy = "buyAndHold:equalWeights"

# [Output]                 # also write the results to a file (the app shows
#                          #   them either way); goes after every portfolio
# path    = ""             # e.g. "results.csv"
# format  = ""             # "txt" (default), "csv" or "json"
# fields  = []             # columns, e.g. ["PortfolioName", "SharpeRatio"]
# filter  = ""             # e.g. "Trades > 0 && SharpeRatio > 0.5"
# sort_by = ""             # e.g. "SharpeRatio"
# order   = ""             # "desc" (default) or "asc"
# limit   = 0              # at most this many rows; 0 = all
`;

// There is no console panel: the Backtest form refuses bad input up front, so
// what is left to say after a run is rare enough for the status bar. The first
// error is the one worth reading; the rest are counted, not listed. Lua errors
// that name a line are also drawn as markers in the editor.
function firstError(diags: backtest.Diagnostic[]): string {
    const errs = diags.filter((d) => d.level === "error");
    if (errs.length === 0) return "";
    const more = errs.length > 1 ? ` (+${errs.length - 1} more)` : "";
    const who = errs[0].portfolio ? `${errs[0].portfolio}: ` : "";
    return `${who}${errs[0].message}${more}`;
}

function App() {
    const [mode, setMode] = useState<Mode>("simple");
    const [tab, setTab] = useState<Tab>("toml");
    const visited = useRef(new Set<Mode>());
    const [tomlText, setTomlText] = useState<string>(DEFAULT_TOML);
    const [luaText, setLuaText] = useState<string>("");
    const [tomlPath, setTomlPath] = useState<string>("");
    const [luaPath, setLuaPath] = useState<string>(DEFAULT_LUA_PATH);
    const [dbPath, setDbPath] = useState<string>(DEFAULT_DB_PATH);
    const [running, setRunning] = useState(false);
    const [chatOpen, setChatOpen] = useState(false);
    const [results, setResults] = useState<main.RunResult[]>([]);
    // Pre-registration. Kept as raw TOML so the app and the CLI validate one
    // identical thing; an empty box means the run is simply not judged, which
    // is different from judging it against a default nobody registered.
    const [hypothesis, setHypothesis] = useState("");
    const [verdicts, setVerdicts] = useState<backtest.VerdictReport[]>([]);
    const [hypothesisError, setHypothesisError] = useState("");
    const [diagnostics, setDiagnostics] = useState<backtest.Diagnostic[]>(
        [],
    );
    const [errorMsg, setErrorMsg] = useState<string>("");
    const [statusMsg, setStatusMsg] = useState<string>("");
    // Through storage.ts: this runs in the ROOT component's initializer, so a
    // browser that throws on localStorage access would take the whole app with
    // it rather than lose a zoom level.
    const [fontSize, setFontSize] = useState<number>(() =>
        clampFont(
            readStoredNumber(FONT_STORAGE_KEY, defaultFontSize(), (n) => n >= MIN_FONT_SIZE),
        ),
    );
    // Assistant panel width in base pixels; rendered at chatWidth * scale so
    // it grows and shrinks with the zoom, like the results panel height.
    const [chatWidth, setChatWidth] = useState<number>(() =>
        readStoredNumber(CHAT_WIDTH_STORAGE_KEY, DEFAULT_CHAT_WIDTH, (n) => n >= 200),
    );
    // Zoom factor relative to the base size. The whole UI sizes itself in `em`
    // off .app's font-size, but a few pixel-valued dimensions (the results
    // panel, the SVG chart) have to be multiplied by this to scale in step.
    const scale = fontSize / BASE_FONT_SIZE;
    // Results-panel height in *base* pixels (i.e. at scale 1). It's rendered at
    // resultsHeight * scale so the panel grows and shrinks with the zoom.
    // Remembered, like the assistant's width: someone who made room for the
    // results once should not have to again on every launch.
    const [resultsHeight, setResultsHeight] = useState<number>(() =>
        readStoredNumber(RESULTS_HEIGHT_STORAGE_KEY, DEFAULT_RESULTS_HEIGHT, (n) => n >= 60),
    );
    // Minimised to its bar, normal, or maximised over the tabs. Orthogonal to
    // resultsHeight, which is kept so restoring returns to the same size.
    const [resultsLayout, setResultsLayout] = useState<ResultsLayout>(() =>
        parseResultsLayout(readStored(RESULTS_LAYOUT_STORAGE_KEY)),
    );
    const resultsRef = useRef<HTMLDivElement>(null);
    // The window's height, tracked so the results panel's bound follows it.
    const [winHeight, setWinHeight] = useState(() => window.innerHeight);
    useEffect(() => {
        const frame = perFrame(setWinHeight);
        const onResize = () => frame.push(window.innerHeight);
        window.addEventListener("resize", onResize);
        return () => window.removeEventListener("resize", onResize);
    }, []);
    // The results panel may take the window down to RESERVED_TOP* of room
    // above it and no further. The old bound left 220px for the topbar,
    // status line AND the tab above, so a tall results panel squeezed the
    // Config tab below its editor's own minimum and the two overlapped.
    const clampResults = (px: number) =>
        Math.max(
            60 * scale,
            Math.min(
                winHeight -
                    (mode === "advanced" ? RESERVED_TOP_CONFIG : RESERVED_TOP) * scale,
                px,
            ),
        );
    const resultsRendered = clampResults(resultsHeight * scale);
    // Simple-mode form data lives here (not inside SimpleForm) so it survives
    // running a backtest and round-tripping through the Advanced editor.
    // A LIST since the Backtest tab learned to run several portfolios at once.
    // It opens with exactly one, so an untouched app generates exactly the
    // config it generated before.
    // Restored from the last session if there is a valid one, and the
    // untouched defaults otherwise. Read once, in the ROOT component's
    // initializer, so both pieces of state come from the same session rather
    // than from two reads that could disagree.
    const [restored] = useState<Session>(() => readSession(defaultSession()));
    const [simpleForms, setSimpleForms] = useState<SimpleFormData[]>(
        () => restored.forms,
    );
    // One per RUN, not one per portfolio: the engine registers a single claim
    // and judges every result against it, so a box per card would invent a
    // concept the engine does not have. Empty claim = not pre-registered = the
    // run is not judged; a default criterion would be judging against a bar
    // nobody chose.
    const [simpleHypothesis, setSimpleHypothesis] = useState<HypothesisFormData>(
        () => restored.hypothesis,
    );

    // Remember the form as it is edited. Not the results, and not the file
    // paths -- see session.ts for why each is left out.
    useEffect(() => {
        writeSession({ forms: simpleForms, hypothesis: simpleHypothesis });
    }, [simpleForms, simpleHypothesis]);

    // Drag the handle between editor and results to resize. The pointer moves in
    // real screen pixels, but resultsHeight is stored in base pixels, so the
    // delta is converted through `scale`. The bounds are applied at render
    // (see resultsRendered), so a window that shrinks after the drag cannot
    // leave the results taller than the room there is for them.
    // Dragging a minimised or maximised panel restores it and starts from the
    // height it actually has on screen (0 minimised, the full room maximised),
    // so the panel follows the pointer instead of jumping to the stored size.
    function startResize(e: React.PointerEvent) {
        if (e.button !== 0) return;
        e.preventDefault();
        const start = resultsRef.current?.getBoundingClientRect().height ?? resultsRendered;
        trackDrag(e, "ns-resize", (_dx, dy) => {
            setResultsLayout("normal");
            setResultsHeight(clampResults(start - dy) / scale);
        });
    }

    // Same idea horizontally, for the handle on the assistant panel's left
    // edge: dragging left widens the panel. Bounds scale with the zoom.
    function startChatResize(e: React.PointerEvent) {
        if (e.button !== 0) return;
        e.preventDefault();
        const startRendered = chatWidth * scale;
        const maxRendered = window.innerWidth * 0.7;
        const minRendered = 220 * scale;
        trackDrag(e, "ew-resize", (dx) => {
            const clamped = Math.max(minRendered, Math.min(maxRendered, startRendered - dx));
            setChatWidth(clamped / scale);
        });
    }

    // Ctrl + scroll wheel / Ctrl + ArrowUp/Down adjust font size for the
    // whole UI (Monaco picks it up via its fontSize option; the rest of the
    // app uses em units that cascade from .app's inline font-size). Ctrl+0
    // resets. Listeners are attached in capture phase so they win against
    // Monaco's own Ctrl+ArrowUp scroll-by-line and the webview's zoom.
    useEffect(() => {
        const onWheel = (e: WheelEvent) => {
            if (!e.ctrlKey) return;
            e.preventDefault();
            setFontSize((s) => clampFont(s + (e.deltaY < 0 ? 1 : -1)));
        };
        const onKey = (e: KeyboardEvent) => {
            if (!e.ctrlKey) return;
            if (e.key === "ArrowUp") {
                e.preventDefault();
                setFontSize((s) => clampFont(s + 1));
            } else if (e.key === "ArrowDown") {
                e.preventDefault();
                setFontSize((s) => clampFont(s - 1));
            } else if (e.key === "0") {
                e.preventDefault();
                setFontSize(defaultFontSize());
            }
        };
        document.addEventListener("wheel", onWheel, {
            passive: false,
            capture: true,
        });
        document.addEventListener("keydown", onKey, true);
        return () => {
            document.removeEventListener("wheel", onWheel, { capture: true });
            document.removeEventListener("keydown", onKey, true);
        };
    }, []);

    // The Lua API the engine actually registers, for the editor's completion
    // and hover. Fetched once: it cannot change while the app is running, and
    // an editor that completes a global the engine does not have is worse than
    // one that completes nothing.
    // A ref, not state: nothing renders from it. The completion provider is
    // registered once when Monaco mounts and reads this at the moment a list
    // is requested, so the API arriving later needs no re-render and no
    // re-registration.
    // The Monaco editor and namespace, for the two things that need to reach
    // into the editor from outside it: jumping to a line, and marking one.
    const editorRef = useRef<any>(null);
    const monacoRef = useRef<any>(null);
    const luaApiRef = useRef<backtest.LuaGlobal[]>([]);
    // Starting shapes for a blank file. State rather than a ref: the buttons
    // that offer them are rendered, so their arrival has to re-render.
    const [skeletons, setSkeletons] = useState<backtest.LuaSkeleton[]>([]);
    // The text as last loaded from or saved to disk. A buffer equal to it has
    // no unsaved work in it, which is what makes replacing it safe.
    const [luaPristine, setLuaPristine] = useState<string>("");
    // What this build calls itself, for the header. A beta bug report that
    // cannot name a build is very hard to act on, so it is on screen rather
    // than in an About box nobody opens.
    const [appVersion, setAppVersion] = useState<string>("");
    useEffect(() => {
        let cancelled = false;
        LuaAPI()
            .then((api) => {
                if (!cancelled) luaApiRef.current = api ?? [];
            })
            .catch(() => {});
        return () => {
            cancelled = true;
        };
    }, []);

    useEffect(() => {
        let cancelled = false;
        Version()
            .then((v) => {
                if (!cancelled) setAppVersion(v);
            })
            .catch(() => {});
        return () => {
            cancelled = true;
        };
    }, []);

    useEffect(() => {
        let cancelled = false;
        LuaSkeletons()
            .then((s) => {
                if (!cancelled) setSkeletons(s ?? []);
            })
            .catch(() => {});
        return () => {
            cancelled = true;
        };
    }, []);

    // Ask Go where the bundled database actually is. The frontend cannot know:
    // "../stock_data.db" is its location relative to ui/ and nowhere else, so
    // a binary launched from anywhere but the checkout opened a path that did
    // not exist. An empty answer means no database was found, and the header
    // then asks for one instead of naming a file that isn't there.
    useEffect(() => {
        let cancelled = false;
        DefaultDBPath()
            .then((p) => {
                if (!cancelled) setDbPath(p);
            })
            .catch(() => {});
        return () => {
            cancelled = true;
        };
    }, []);

    // How the database opened, once there is one. Read-write in place says
    // nothing; the app running against its own copy, or read-only because the
    // file cannot be written, says so once rather than letting the user
    // discover it when a fetch fails. Failing to open at all is an error --
    // usually another program holding the file, which the message names.
    useEffect(() => {
        if (!dbPath) return;
        let cancelled = false;
        DatabaseNotice(dbPath)
            .then((notice) => {
                if (!cancelled && notice) setStatusMsg(notice);
            })
            .catch((e: any) => {
                if (!cancelled) setErrorMsg(`${e?.message ?? e}`);
            });
        return () => {
            cancelled = true;
        };
    }, [dbPath]);

    useEffect(() => {
        writeStored(FONT_STORAGE_KEY, String(fontSize));
    }, [fontSize]);

    useEffect(() => {
        writeStored(CHAT_WIDTH_STORAGE_KEY, String(Math.round(chatWidth)));
    }, [chatWidth]);

    useEffect(() => {
        writeStored(RESULTS_HEIGHT_STORAGE_KEY, String(Math.round(resultsHeight)));
    }, [resultsHeight]);

    useEffect(() => {
        writeStored(RESULTS_LAYOUT_STORAGE_KEY, resultsLayout);
    }, [resultsLayout]);

    // Whenever the TOML changes (or its on-disk path), look for a lua:<path>
    // strategy and reload the referenced script into the Lua tab. When no
    // lua: strategy is present, fall back to DEFAULT_LUA_PATH so the tab is
    // always populated — that script is the default for any portfolio whose
    // Strategy field is omitted.
    useEffect(() => {
        let cancelled = false;
        (async () => {
            const found = await FindLuaPathInConfig(tomlText, tomlPath);
            const resolved = found || DEFAULT_LUA_PATH;
            if (cancelled) return;
            if (resolved === luaPath && luaText !== "") return;
            setLuaPath(resolved);
            const exists = await FileExists(resolved);
            if (!exists) {
                if (!cancelled) {
                    setLuaText("");
                    setLuaPristine("");
                    setStatusMsg(`${resolved} — new file, will be created on Save`);
                }
                return;
            }
            try {
                const content = await ReadFile(resolved);
                if (!cancelled) {
                    setLuaText(content);
                    setLuaPristine(content);
                }
            } catch (e: any) {
                if (!cancelled) {
                    setLuaText("");
                    setErrorMsg(`read ${resolved}: ${e?.message ?? e}`);
                }
            }
        })();
        return () => {
            cancelled = true;
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [tomlText, tomlPath]);

    async function chooseConfig() {
        setErrorMsg("");
        try {
            const path = await PickConfigFile();
            if (!path) return;
            const content = await ReadFile(path);
            setTomlPath(path);
            setTomlText(content);
            setStatusMsg(`Loaded ${path}`);
        } catch (e: any) {
            setErrorMsg(String(e?.message ?? e));
        }
    }

    async function chooseDB() {
        setErrorMsg("");
        try {
            const path = await PickDBFile();
            if (path) {
                setDbPath(path);
                setStatusMsg(`DB: ${path}`);
            }
        } catch (e: any) {
            setErrorMsg(String(e?.message ?? e));
        }
    }

    async function save() {
        setErrorMsg("");
        const saved: string[] = [];
        try {
            if (tomlPath) {
                await WriteFile(tomlPath, tomlText);
                saved.push(tomlPath);
            }
            if (luaPath) {
                await WriteFile(luaPath, luaText);
                saved.push(luaPath);
            }
            setStatusMsg(
                saved.length === 0 ? "Nothing to save" : `Saved: ${saved.join(", ")}`,
            );
        } catch (e: any) {
            setErrorMsg(String(e?.message ?? e));
        }
    }

    // runConfig executes a TOML config against the chosen DB. When persist is
    // true (advanced mode) the open TOML/Lua buffers are written to disk first,
    // since the Lua strategy reads its script off disk at runtime. Simple mode
    // passes its generated config and skips persistence — its weighted strategy
    // ships on disk and equal-weight uses a built-in strategy.
    // hypothesisText overrides the Advanced-mode box, so simple mode can
    // register the question its own form built without the two modes fighting
    // over one piece of state.
    async function runConfig(
        cfgText: string,
        persist: boolean,
        hypothesisText?: string,
    ) {
        setErrorMsg("");
        setDiagnostics([]);
        // The previous results stay on screen, dimmed, until the new ones
        // arrive -- clearing them up front collapsed the panel to its empty
        // state for the whole run and then popped the chart back in. They are
        // cleared on every path that will not replace them, so a stale result
        // is never left looking like the answer to a run that failed.
        if (!dbPath) {
            setResults([]);
            setErrorMsg("Pick a DuckDB file first.");
            return;
        }
        if (persist) {
            try {
                if (tomlPath) await WriteFile(tomlPath, tomlText);
                if (luaPath) await WriteFile(luaPath, luaText);
            } catch (e: any) {
                setResults([]);
                setErrorMsg(`save failed: ${e?.message ?? e}`);
                return;
            }
        }
        setRunning(true);
        setStatusMsg("Running…");
        const started = performance.now();
        try {
            const res = await RunBacktest(
                cfgText,
                dbPath,
                luaPath,
                hypothesisText ?? hypothesis,
            );
            setResults(res.results ?? []);
            // A run's answer should be seen: a minimised panel reopens.
            // A maximised one stays as it is.
            if ((res.results ?? []).length > 0) {
                setResultsLayout((v) => (v === "min" ? "normal" : v));
            }
            setDiagnostics(res.diagnostics ?? []);
            setErrorMsg(firstError(res.diagnostics ?? []));
            setVerdicts(res.verdicts ?? []);
            setHypothesisError(res.hypothesisError ?? "");
            const secs = (performance.now() - started) / 1000;
            setStatusMsg(
                `Done — ${(res.results ?? []).length} result(s) in ${secs.toFixed(secs < 10 ? 1 : 0)}s`,
            );
        } catch (e: any) {
            setResults([]);
            setErrorMsg(String(e?.message ?? e));
            setStatusMsg("");
        } finally {
            setRunning(false);
        }
    }

    // Validate the config without running it. Opens no database and simulates
    // nothing, so it answers immediately even for a config that would take
    // minutes — the point is to catch a typo before waiting for one.
    async function checkConfig() {
        setErrorMsg("");
        setResults([]);
        setDiagnostics([]);
        setStatusMsg("Checking…");
        try {
            const res = await CheckConfig(tomlText, luaPath);
            const diags = res.diagnostics ?? [];
            setDiagnostics(diags);
            setErrorMsg(firstError(diags));
            const errors = diags.filter((d) => d.level === "error").length;
            const warnings = diags.length - errors;
            if (errors > 0) {
                setStatusMsg(
                    `Config check failed — ${errors} error(s), ${warnings} warning(s)`,
                );
            } else if (warnings > 0) {
                // Warnings do not stop a run; they mean it may not do what
                // was meant.
                setStatusMsg(`Config check passed — ${warnings} warning(s)`);
            } else {
                setStatusMsg("Config check passed");
            }
        } catch (e: any) {
            setErrorMsg(String(e?.message ?? e));
            setStatusMsg("");
        }
    }

    // The Lua half of ✓ Check: compile the script, require its step(day), and
    // run that for a few days against synthetic prices — so a typo inside the
    // body is found here rather than on day 0 of a real backtest. Opens no
    // database. The config text goes along only for [portfolio.Params], so a
    // script reading params.period is checked with the values it would get.
    async function checkScript() {
        setErrorMsg("");
        setResults([]);
        setDiagnostics([]);
        setStatusMsg("Checking the script…");
        try {
            const res = await CheckLuaScript(luaText, luaPath, tomlText);
            const diags = res.diagnostics ?? [];
            setDiagnostics(diags);
            setErrorMsg(firstError(diags));
            const errors = diags.filter((d) => d.level === "error").length;
            const warnings = diags.length - errors;
            if (errors > 0) {
                setStatusMsg(
                    `Script check failed — ${errors} error(s), ${warnings} warning(s)`,
                );
            } else if (warnings > 0) {
                setStatusMsg(`Script check passed — ${warnings} warning(s)`);
            } else {
                setStatusMsg("Script check passed");
            }
        } catch (e: any) {
            setErrorMsg(String(e?.message ?? e));
            setStatusMsg("");
        }
    }

    // Start from a shape rather than from a blank file (or from buy-and-hold,
    // which is what example.lua is and which teaches exactly one shape).
    //
    // Only offered while the buffer holds no unsaved work — an unmodified file
    // is still on disk, so replacing it loses nothing, and this way there is
    // no "are you sure" to get wrong. The path moves to a new file so Save
    // cannot overwrite the script that was open.
    function startFromSkeleton(sk: backtest.LuaSkeleton) {
        setLuaText(sk.source);
        setLuaPristine("");
        setLuaPath(SKELETON_LUA_PATH);
        setTab("lua");
        setStatusMsg(
            `${sk.name} — Save writes ${SKELETON_LUA_PATH}; ✓ Check script tells you if it runs`,
        );
    }

    // Run the script that is open, against the holdings already on the
    // Backtest tab. The detour it removes is real: without it, trying a script
    // means going to the Config tab, writing a portfolio around it, and coming
    // back — which is a lot of TOML to learn before finding out whether an
    // idea does anything.
    async function runScript() {
        const usable = simpleForms.filter((f) => f.tickers.length > 0);
        if (usable.length === 0) {
            setErrorMsg(
                "Add holdings on the Backtest tab first — Run this script " +
                    "runs your script against them.",
            );
            return;
        }
        if (!luaPath) {
            setErrorMsg("Save the script first, so the engine has a file to read.");
            return;
        }
        // Every portfolio, pointed at this script instead of whatever it was
        // going to run. Names are left alone so the results are recognisable.
        const forms = usable.map((f) => ({ ...f, strategy: luaPath }));
        await runConfig(buildToml(forms), true);
    }

    // Mark the lines the last run complained about, so the editor shows where
    // a script went wrong. Only diagnostics belonging to the script actually
    // open: a marker on line 41 of a different file would point at whatever
    // happens to be on line 41 of this one.
    useEffect(() => {
        const monaco = monacoRef.current;
        const editor = editorRef.current;
        if (!monaco || !editor) return;
        const model = editor.getModel();
        if (!model || tab !== "lua") return;
        const mine = diagnostics.filter(
            (d) => d.line > 0 && d.script && baseName(d.script) === baseName(luaPath),
        );
        monaco.editor.setModelMarkers(
            model,
            "backtester",
            mine.map((d) => ({
                startLineNumber: d.line,
                endLineNumber: d.line,
                startColumn: 1,
                endColumn: 1000,
                message: d.message,
                severity:
                    d.level === "error"
                        ? monaco.MarkerSeverity.Error
                        : monaco.MarkerSeverity.Warning,
            })),
        );
    }, [diagnostics, luaPath, luaText, tab]);

    // Load a simple-mode-generated config into the Advanced editor and switch.
    function editAsToml(cfgText: string) {
        setTomlText(cfgText);
        setTomlPath("");
        setTab("toml");
        setMode("advanced");
        setStatusMsg("Loaded generated config — edit freely.");
    }

    // Claude's "Insert into … editor" buttons land here: swap the buffer in
    // and jump to the right tab so the user sees what changed before running.
    function applyClaudeToml(toml: string) {
        setTomlText(toml);
        setTab("toml");
        setMode("advanced");
        setStatusMsg("Claude's config loaded into the TOML editor.");
    }
    function applyClaudeLua(lua: string) {
        setLuaText(lua);
        setTab("lua");
        setMode("advanced");
        setStatusMsg(`Claude's strategy loaded — Save writes ${luaPath}.`);
    }

    // The tabs visited so far, which are the ones kept mounted. Added to
    // during render: idempotent, and it must hold before the JSX below reads it.
    visited.current.add(mode);

    const language = tab === "toml" ? "ini" : "lua"; // monaco doesn't ship TOML, ini is close
    const value = tab === "toml" ? tomlText : luaText;
    const setValue = tab === "toml" ? setTomlText : setLuaText;

    return (
        <div className="app" style={{ fontSize: `${fontSize}px` }}>
            <header className="topbar">
                <div className="brand">
                    <svg className="brand-mark" viewBox="0 0 16 16" aria-hidden>
                        <rect x="1" y="9" width="3.5" height="6" rx="1" />
                        <rect x="6.25" y="5" width="3.5" height="10" rx="1" />
                        <rect x="11.5" y="1" width="3.5" height="14" rx="1" />
                    </svg>
                    <span className="brand-name">Backtester</span>
                    {appVersion && (
                        <span
                            className="brand-version"
                            title="Quote this in a bug report"
                        >
                            {appVersion}
                        </span>
                    )}
                </div>
                {/* Named for what each mode operates on, not for how hard it
                    is: "Simple/Advanced" reads as a hierarchy, and a user who
                    needs the TOML editor is not thereby an advanced user. The
                    internal Mode values are unchanged — they key persisted
                    state and every conditional below. */}
                <div className="mode-toggle">
                    <button
                        className={mode === "simple" ? "active" : ""}
                        onClick={() => setMode("simple")}
                        title="Build a portfolio from a form and run it"
                    >
                        Backtest
                    </button>
                    <button
                        className={mode === "advanced" ? "active" : ""}
                        onClick={() => setMode("advanced")}
                        title="Edit the TOML config and Lua strategy directly"
                    >
                        Config
                    </button>
                    <button
                        className={mode === "data" ? "active" : ""}
                        onClick={() => setMode("data")}
                        title="Query the database with SQL"
                    >
                        Data
                    </button>
                </div>
                <div className="topbar-spacer" />
                <button
                    className="db-chip"
                    onClick={chooseDB}
                    title={dbPath ? `Database: ${dbPath}` : "Choose a database"}
                >
                    <span className="db-ico">⛁</span>
                    <span className="db-name">
                        {chipLabel(dbPath, "Choose DB…")}
                    </span>
                </button>
                <button
                    className={chatOpen ? "chat-toggle active" : "chat-toggle"}
                    onClick={() => setChatOpen((o) => !o)}
                >
                    ✦ Assistant
                </button>
            </header>

            <div className={resultsLayout === "max" ? "mid-row hidden" : "mid-row"}>
            <div className="mid-main">
            {/* Each tab mounts on its first visit and then stays mounted --
                see Pane. First visits keep startup as cheap as before. */}
            {visited.current.has("simple") && (
                <Pane active={mode === "simple"} className="main-scroll">
                    <SimpleForm
                        dbPath={dbPath}
                        running={running}
                        forms={simpleForms}
                        onChange={setSimpleForms}
                        hypothesis={simpleHypothesis}
                        onHypothesisChange={setSimpleHypothesis}
                        onRun={(cfg, hyp) => runConfig(cfg, false, hyp)}
                        onOpenAssistant={() => setChatOpen(true)}
                        onEditAsToml={(cfg, hyp) => {
                            if (hyp) setHypothesis(hyp);
                            editAsToml(cfg);
                        }}
                    />
                </Pane>
            )}
            {visited.current.has("data") && (
                <Pane active={mode === "data"} className="data-pane">
                    <QueryConsole dbPath={dbPath} fontSize={fontSize} />
                </Pane>
            )}
            {visited.current.has("advanced") && (
                <Pane active={mode === "advanced"} className="config-pane">
                    <div className="toolbar">
                        <button onClick={chooseConfig}>Open config…</button>
                        <button onClick={save} disabled={!tomlPath && !luaPath}>
                            Save
                        </button>
                        <div className="paths">
                            <span
                                className="path-chip"
                                title={tomlPath || "Config not saved to disk yet"}
                            >
                                toml · {chipLabel(tomlPath, "unsaved")}
                            </span>
                            <span
                                className="path-chip"
                                title={luaPath || "No Lua script"}
                            >
                                lua · {chipLabel(luaPath, "none")}
                            </span>
                        </div>
                        {/* One button, checking whichever thing is open. Two
                            buttons would ask the user which of "check" they
                            meant while looking straight at the answer. */}
                        {tab === "lua" ? (
                            <>
                                <button
                                    onClick={checkScript}
                                    disabled={running}
                                    title="Compile the script and run its step(day) against synthetic prices, without a backtest"
                                >
                                    ✓ Check script
                                </button>
                                <button
                                    onClick={runScript}
                                    disabled={running}
                                    className="run"
                                    title="Run this script against the holdings on the Backtest tab"
                                >
                                    {running ? "Running…" : "▶ Run this script"}
                                </button>
                            </>
                        ) : (
                            <button
                                onClick={checkConfig}
                                disabled={running}
                                title="Validate the config without running it"
                            >
                                ✓ Check
                            </button>
                        )}
                        <button
                            onClick={() => runConfig(tomlText, true)}
                            disabled={running}
                            className="run"
                        >
                            {running ? "Running…" : "▶ Run backtest"}
                        </button>
                    </div>

                    {/* Deliberately above the run button: a criterion is a
                        pre-registration, and one typed after seeing results is
                        not one. Leaving it empty runs unjudged rather than
                        judging against a default nobody chose. */}
                    <details className="prereg">
                        <summary>
                            Pre-register a hypothesis
                            {hypothesis.trim() ? " — registered" : " (optional)"}
                        </summary>
                        <textarea
                            className="prereg-input"
                            spellCheck={false}
                            placeholder={
                                'ID          = "H-1"\n' +
                                'Claim       = "12-1 momentum beats buy-and-hold"\n' +
                                'Criterion   = "sharpe >= 1.2"\n' +
                                "TrialBudget = 200"
                            }
                            value={hypothesis}
                            onChange={(e) => setHypothesis(e.target.value)}
                        />
                        <div className="prereg-note">
                            One metric, one operator, one number — e.g.{" "}
                            <code>sharpe &gt;= 1.2</code>. Write it before you run,
                            and never edit it after seeing a result: a changed
                            question is a new <code>ID</code>.
                        </div>
                        {hypothesisError && (
                            <div className="prereg-error">{hypothesisError}</div>
                        )}
                    </details>

                    <div className="tabs">
                        <button
                            className={tab === "toml" ? "tab active" : "tab"}
                            onClick={() => setTab("toml")}
                        >
                            TOML config
                        </button>
                        <button
                            className={tab === "lua" ? "tab active" : "tab"}
                            onClick={() => setTab("lua")}
                            title="Default Lua strategy — used when a portfolio omits Strategy"
                        >
                            Lua script
                        </button>
                    </div>

                    {/* Starting shapes, offered only while the buffer holds no
                        unsaved work — an unmodified file is still on disk, so
                        replacing it loses nothing, and nothing here needs an
                        "are you sure". Same rule as the Backtest tab's starter
                        examples: the offer appears exactly where there is
                        nothing to lose by taking it. */}
                    {tab === "lua" &&
                        skeletons.length > 0 &&
                        luaText === luaPristine && (
                            <div className="skeletons">
                                <span className="skeletons-label">
                                    Start from a shape
                                </span>
                                {skeletons.map((sk) => (
                                    <button
                                        key={sk.name}
                                        className="preset-chip"
                                        title={sk.summary}
                                        onClick={() => startFromSkeleton(sk)}
                                    >
                                        {sk.name}
                                    </button>
                                ))}
                            </div>
                        )}

                    <div className="editor">
                        <Editor
                            height="100%"
                            // One Monaco model per file. Without a path the
                            // two tabs shared a model, and every switch
                            // replaced its whole text: slow on a long script,
                            // and it lost the cursor, scroll and undo history
                            // of the file being left. Keyed by path, each tab
                            // keeps its own and switching is a model swap.
                            path={tab === "toml" ? "backtester/config.toml" : "backtester/strategy.lua"}
                            language={language}
                            theme="backtester-dark"
                            value={value}
                            onChange={(v) => setValue(v ?? "")}
                            // The providers are registered against the Monaco
                            // instance, not the editor, so this runs on every
                            // mount and registerLuaLanguageSupport is
                            // idempotent -- registering twice would offer
                            // every completion twice.
                            onMount={(editor, monaco) => {
                                editorRef.current = editor;
                                monacoRef.current = monaco;
                                registerLuaLanguageSupport(
                                    monaco,
                                    () => luaApiRef.current,
                                );
                            }}
                            options={{
                                minimap: { enabled: false },
                                fontSize,
                                scrollBeyondLastLine: false,
                            }}
                        />
                    </div>
                </Pane>
            )}
            </div>
            {chatOpen && (
                <>
                    <div
                        className="vresize-handle"
                        onPointerDown={startChatResize}
                        onDoubleClick={() => setChatWidth(DEFAULT_CHAT_WIDTH)}
                        title="Drag to resize the assistant panel · double-click to reset"
                    />
                    <ChatPanel
                        width={chatWidth * scale}
                        dbPath={dbPath}
                        cfgText={tomlText}
                        luaText={luaText}
                        onApplyToml={applyClaudeToml}
                        onApplyLua={applyClaudeLua}
                        onClose={() => setChatOpen(false)}
                    />
                </>
            )}
            </div>

            <ResultsBar
                view={resultsLayout}
                onView={setResultsLayout}
                onResizeStart={startResize}
                onReset={() => {
                    setResultsLayout("normal");
                    setResultsHeight(DEFAULT_RESULTS_HEIGHT);
                }}
                count={results.length}
            />

            <div
                ref={resultsRef}
                className={
                    "results" +
                    (running && results.length > 0 ? " stale" : "") +
                    (resultsLayout === "normal" ? "" : ` ${resultsLayout}`)
                }
                style={resultsLayout === "normal" ? { height: resultsRendered } : undefined}
                aria-busy={running}
            >
                <ErrorBoundary label="The results view">
                    <ResultsView results={results} fontSize={fontSize} verdicts={verdicts} />
                </ErrorBoundary>
            </div>

            <footer className="status">
                {errorMsg ? (
                    <span className="err">⚠ {errorMsg}</span>
                ) : (
                    <span>{statusMsg}</span>
                )}
                <span className="status-spacer" />
                <span className="zoom-hint" title="Ctrl+scroll or Ctrl+↑/↓ to zoom, Ctrl+0 to reset">
                    {Math.round(scale * 100)}%
                </span>
            </footer>
        </div>
    );
}

export default App;
