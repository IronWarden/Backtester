import { useEffect, useState } from "react";

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
    FileExists,
    FindLuaPathInConfig,
    PickConfigFile,
    PickDBFile,
    ReadFile,
    RunBacktest,
    WriteFile,
} from "../wailsjs/go/main/App";
import { main } from "../wailsjs/go/models";
import SimpleForm from "./SimpleForm";
import type { SimpleForm as SimpleFormData } from "./buildToml";
import ResultsView from "./ResultsView";
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

// Last path segment, for compact chips ("/a/b/config.toml" -> "config.toml").
const baseName = (p: string) => p.split(/[\\/]/).pop() || p;

type Tab = "toml" | "lua";
type Mode = "simple" | "advanced" | "data";

// Default DB path, relative to the app's working directory (ui/). Points at
// the bundled database so simple-mode users can run without picking a file.
const DEFAULT_DB_PATH = "../stock_data.db";

// Path used for the Lua tab when the TOML doesn't pin a specific lua: script.
// Portfolios that omit Strategy fall back to this file at run time.
const DEFAULT_LUA_PATH = "strategies/example.lua";

const DEFAULT_TOML = `# Edit your portfolio config here. Add as many [[portfolio]] blocks as
# you like — they all run and their metrics appear in the results table.
# Omit Strategy to use the Lua script open in the Lua tab.

[[portfolio]]
Name = "Default Lua"
BuyingPower = 100000.0
StartDate = "2020-01-01"
EndDate = "2024-01-01"
Tickers = ["AAPL", "MSFT"]

[[portfolio]]
Name = "Buy and Hold"
BuyingPower = 100000.0
StartDate = "2020-01-01"
EndDate = "2024-01-01"
Tickers = ["AAPL", "MSFT"]
Strategy = "buyAndHold:equalWeights"
`;

function App() {
    const [mode, setMode] = useState<Mode>("simple");
    const [tab, setTab] = useState<Tab>("toml");
    const [tomlText, setTomlText] = useState<string>(DEFAULT_TOML);
    const [luaText, setLuaText] = useState<string>("");
    const [tomlPath, setTomlPath] = useState<string>("");
    const [luaPath, setLuaPath] = useState<string>(DEFAULT_LUA_PATH);
    const [dbPath, setDbPath] = useState<string>(DEFAULT_DB_PATH);
    const [running, setRunning] = useState(false);
    const [chatOpen, setChatOpen] = useState(false);
    const [results, setResults] = useState<main.RunResult[]>([]);
    const [errorMsg, setErrorMsg] = useState<string>("");
    const [statusMsg, setStatusMsg] = useState<string>("");
    const [fontSize, setFontSize] = useState<number>(() => {
        const stored = Number(localStorage.getItem(FONT_STORAGE_KEY));
        return Number.isFinite(stored) && stored >= MIN_FONT_SIZE
            ? clampFont(stored)
            : defaultFontSize();
    });
    // Assistant panel width in base pixels; rendered at chatWidth * scale so
    // it grows and shrinks with the zoom, like the results panel height.
    const [chatWidth, setChatWidth] = useState<number>(() => {
        const stored = Number(localStorage.getItem(CHAT_WIDTH_STORAGE_KEY));
        return Number.isFinite(stored) && stored >= 200
            ? stored
            : DEFAULT_CHAT_WIDTH;
    });
    // Zoom factor relative to the base size. The whole UI sizes itself in `em`
    // off .app's font-size, but a few pixel-valued dimensions (the results
    // panel, the SVG chart) have to be multiplied by this to scale in step.
    const scale = fontSize / BASE_FONT_SIZE;
    // Results-panel height in *base* pixels (i.e. at scale 1). It's rendered at
    // resultsHeight * scale so the panel grows and shrinks with the zoom.
    const [resultsHeight, setResultsHeight] = useState<number>(240);
    // Simple-mode form data lives here (not inside SimpleForm) so it survives
    // running a backtest and round-tripping through the Advanced editor.
    const [simpleForm, setSimpleForm] = useState<SimpleFormData>({
        name: "Buy and Hold",
        buyingPower: 100000,
        startDate: "2020-01-01",
        endDate: today,
        tickers: [],
        evenSplit: true,
        allocations: {},
    });

    // Drag the handle between editor and results to resize. The pointer moves in
    // real screen pixels, but resultsHeight is stored in base pixels, so the
    // delta and bounds are converted through `scale`. The reserved chrome and
    // the floor scale with the zoom too, keeping the feel consistent at any size.
    function startResize(e: React.MouseEvent) {
        e.preventDefault();
        const startRendered = resultsHeight * scale;
        const startY = e.clientY;
        const maxRendered = window.innerHeight - 220 * scale;
        const minRendered = 60 * scale;
        const onMove = (ev: MouseEvent) => {
            const next = startRendered + (startY - ev.clientY);
            const clamped = Math.max(minRendered, Math.min(maxRendered, next));
            setResultsHeight(clamped / scale);
        };
        const onUp = () => {
            document.removeEventListener("mousemove", onMove);
            document.removeEventListener("mouseup", onUp);
            document.body.style.cursor = "";
            document.body.style.userSelect = "";
        };
        document.body.style.cursor = "ns-resize";
        document.body.style.userSelect = "none";
        document.addEventListener("mousemove", onMove);
        document.addEventListener("mouseup", onUp);
    }

    // Same idea horizontally, for the handle on the assistant panel's left
    // edge: dragging left widens the panel. Bounds scale with the zoom.
    function startChatResize(e: React.MouseEvent) {
        e.preventDefault();
        const startRendered = chatWidth * scale;
        const startX = e.clientX;
        const maxRendered = window.innerWidth * 0.7;
        const minRendered = 220 * scale;
        const onMove = (ev: MouseEvent) => {
            const next = startRendered + (startX - ev.clientX);
            const clamped = Math.max(minRendered, Math.min(maxRendered, next));
            setChatWidth(clamped / scale);
        };
        const onUp = () => {
            document.removeEventListener("mousemove", onMove);
            document.removeEventListener("mouseup", onUp);
            document.body.style.cursor = "";
            document.body.style.userSelect = "";
        };
        document.body.style.cursor = "ew-resize";
        document.body.style.userSelect = "none";
        document.addEventListener("mousemove", onMove);
        document.addEventListener("mouseup", onUp);
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

    useEffect(() => {
        try {
            localStorage.setItem(FONT_STORAGE_KEY, String(fontSize));
        } catch {}
    }, [fontSize]);

    useEffect(() => {
        try {
            localStorage.setItem(
                CHAT_WIDTH_STORAGE_KEY,
                String(Math.round(chatWidth)),
            );
        } catch {}
    }, [chatWidth]);

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
                    setStatusMsg(`${resolved} — new file, will be created on Save`);
                }
                return;
            }
            try {
                const content = await ReadFile(resolved);
                if (!cancelled) setLuaText(content);
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
    async function runConfig(cfgText: string, persist: boolean) {
        setErrorMsg("");
        setResults([]);
        if (!dbPath) {
            setErrorMsg("Pick a DuckDB file first.");
            return;
        }
        if (persist) {
            try {
                if (tomlPath) await WriteFile(tomlPath, tomlText);
                if (luaPath) await WriteFile(luaPath, luaText);
            } catch (e: any) {
                setErrorMsg(`save failed: ${e?.message ?? e}`);
                return;
            }
        }
        setRunning(true);
        setStatusMsg("Running…");
        try {
            const res = await RunBacktest(cfgText, dbPath, luaPath);
            setResults(res);
            setStatusMsg(`Done — ${res.length} result(s)`);
        } catch (e: any) {
            setErrorMsg(String(e?.message ?? e));
            setStatusMsg("");
        } finally {
            setRunning(false);
        }
    }

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
                </div>
                <div className="mode-toggle">
                    <button
                        className={mode === "simple" ? "active" : ""}
                        onClick={() => setMode("simple")}
                    >
                        Simple
                    </button>
                    <button
                        className={mode === "advanced" ? "active" : ""}
                        onClick={() => setMode("advanced")}
                    >
                        Advanced
                    </button>
                    <button
                        className={mode === "data" ? "active" : ""}
                        onClick={() => setMode("data")}
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
                        {dbPath ? baseName(dbPath) : "Choose DB…"}
                    </span>
                </button>
                <button
                    className={chatOpen ? "chat-toggle active" : "chat-toggle"}
                    onClick={() => setChatOpen((o) => !o)}
                >
                    ✦ Assistant
                </button>
            </header>

            <div className="mid-row">
            <div className="mid-main">
            {mode === "simple" ? (
                <div className="main-scroll">
                    <SimpleForm
                        dbPath={dbPath}
                        running={running}
                        form={simpleForm}
                        onChange={setSimpleForm}
                        onRun={(cfg) => runConfig(cfg, false)}
                        onEditAsToml={editAsToml}
                    />
                </div>
            ) : mode === "data" ? (
                <QueryConsole dbPath={dbPath} fontSize={fontSize} />
            ) : (
                <>
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
                                toml · {tomlPath ? baseName(tomlPath) : "unsaved"}
                            </span>
                            <span
                                className="path-chip"
                                title={luaPath || "No Lua script"}
                            >
                                lua · {luaPath ? baseName(luaPath) : "none"}
                            </span>
                        </div>
                        <button
                            onClick={() => runConfig(tomlText, true)}
                            disabled={running}
                            className="run"
                        >
                            {running ? "Running…" : "▶ Run backtest"}
                        </button>
                    </div>

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

                    <div className="editor">
                        <Editor
                            height="100%"
                            language={language}
                            theme="backtester-dark"
                            value={value}
                            onChange={(v) => setValue(v ?? "")}
                            options={{
                                minimap: { enabled: false },
                                fontSize,
                                scrollBeyondLastLine: false,
                            }}
                        />
                    </div>
                </>
            )}
            </div>
            {chatOpen && (
                <>
                    <div
                        className="vresize-handle"
                        onMouseDown={startChatResize}
                        title="Drag to resize assistant panel"
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

            <div
                className="resize-handle"
                onMouseDown={startResize}
                title="Drag to resize results panel"
            />

            <div className="results" style={{ height: resultsHeight * scale }}>
                <ResultsView results={results} fontSize={fontSize} />
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
