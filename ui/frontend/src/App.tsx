import { useEffect, useState } from "react";

const BASE_FONT_SIZE = 16;
const MIN_FONT_SIZE = 9;
const MAX_FONT_SIZE = 28;
const clampFont = (n: number) =>
    Math.max(MIN_FONT_SIZE, Math.min(MAX_FONT_SIZE, n));
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
import "./App.css";

const today = new Date().toISOString().slice(0, 10);

type Tab = "toml" | "lua";
type Mode = "simple" | "advanced";

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
    const [results, setResults] = useState<main.RunResult[]>([]);
    const [errorMsg, setErrorMsg] = useState<string>("");
    const [statusMsg, setStatusMsg] = useState<string>("");
    const [fontSize, setFontSize] = useState<number>(BASE_FONT_SIZE);
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
                setFontSize(BASE_FONT_SIZE);
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

    const language = tab === "toml" ? "ini" : "lua"; // monaco doesn't ship TOML, ini is close
    const value = tab === "toml" ? tomlText : luaText;
    const setValue = tab === "toml" ? setTomlText : setLuaText;

    return (
        <div className="app" style={{ fontSize: `${fontSize}px` }}>
            <div className="topbar">
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
                </div>
                <button onClick={chooseDB}>Choose DB…</button>
                <div className="paths">
                    <div>db: {dbPath || "(not chosen)"}</div>
                </div>
            </div>

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
            ) : (
                <>
                    <div className="toolbar">
                        <button onClick={chooseConfig}>Open config…</button>
                        <button onClick={save} disabled={!tomlPath && !luaPath}>
                            Save
                        </button>
                        <button
                            onClick={() => runConfig(tomlText, true)}
                            disabled={running}
                            className="run"
                        >
                            {running ? "Running…" : "Run backtest"}
                        </button>
                        <div className="paths">
                            <div>config: {tomlPath || "(unsaved)"}</div>
                            <div>lua: {luaPath || "(none)"}</div>
                        </div>
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
                            theme="vs-dark"
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

            <div
                className="resize-handle"
                onMouseDown={startResize}
                title="Drag to resize results panel"
            />

            <div className="status">
                {errorMsg ? (
                    <span className="err">{errorMsg}</span>
                ) : (
                    <span>{statusMsg}</span>
                )}
            </div>

            <div className="results" style={{ height: resultsHeight * scale }}>
                <ResultsView results={results} fontSize={fontSize} />
            </div>
        </div>
    );
}

export default App;
