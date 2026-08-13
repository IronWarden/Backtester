import { useEffect, useMemo, useRef, useState } from "react";
import Editor, { OnMount } from "@monaco-editor/react";
import { RunUserQuery } from "../wailsjs/go/main/App";
import { main } from "../wailsjs/go/models";

// Survive app restarts: people build queries up incrementally.
const SQL_STORAGE_KEY = "backtester-console-sql";
const DEFAULT_SQL = "SELECT * FROM stock_data_optimized LIMIT 100";

// Values arrive pre-rendered as strings; a column is right-aligned only when
// every non-NULL cell looks like a number.
const NUMERIC = /^-?[0-9][0-9,]*\.?[0-9]*%?$/;

type Props = { dbPath: string; fontSize: number };

export default function QueryConsole({ dbPath, fontSize }: Props) {
    const [sql, setSql] = useState<string>(
        () => localStorage.getItem(SQL_STORAGE_KEY) || DEFAULT_SQL,
    );
    const [result, setResult] = useState<main.QueryResult | null>(null);
    const [error, setError] = useState<string>("");
    const [busy, setBusy] = useState(false);
    const [tables, setTables] = useState<string[]>([]);
    // Monaco's Ctrl+Enter command binds once at mount; the ref keeps it
    // pointed at the current run() closure instead of a stale one.
    const runRef = useRef<() => void>(() => {});

    useEffect(() => {
        try {
            localStorage.setItem(SQL_STORAGE_KEY, sql);
        } catch {}
    }, [sql]);

    // Table chips come from the DB itself, so clicking one always queries a
    // table that exists in the chosen file.
    useEffect(() => {
        if (!dbPath) {
            setTables([]);
            return;
        }
        let cancelled = false;
        RunUserQuery(dbPath, "SHOW TABLES")
            .then((r) => {
                if (cancelled) return;
                setTables((r.rows ?? []).map((row) => row[0]).filter(Boolean));
            })
            .catch(() => {
                if (!cancelled) setTables([]);
            });
        return () => {
            cancelled = true;
        };
    }, [dbPath]);

    async function run() {
        if (busy || !dbPath) return;
        setBusy(true);
        setError("");
        try {
            const r = await RunUserQuery(dbPath, sql);
            setResult(r);
        } catch (e: any) {
            setError(String(e?.message ?? e));
            setResult(null);
        } finally {
            setBusy(false);
        }
    }
    runRef.current = run;

    const onMount: OnMount = (editor, monaco) => {
        editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () =>
            runRef.current(),
        );
    };

    const numCols = useMemo(() => {
        if (!result) return [] as boolean[];
        return result.columns.map(
            (_, c) =>
                result.rows.length > 0 &&
                result.rows.every((r) => {
                    const v = r[c];
                    return v === "NULL" || v === "" || NUMERIC.test(v);
                }),
        );
    }, [result]);

    return (
        <div className="query-console">
            <div className="toolbar">
                <button
                    className="run"
                    onClick={run}
                    disabled={busy || !dbPath}
                >
                    {busy ? "Running…" : "▶ Run query"}
                </button>
                <span className="query-hint">
                    {dbPath
                        ? "Ctrl+Enter to run · read-only (SELECT, DESCRIBE, SHOW, SUMMARIZE…)"
                        : "Choose a DB first"}
                </span>
                {tables.length > 0 && (
                    <div className="table-chips">
                        {tables.map((t) => (
                            <button
                                key={t}
                                className="preset-chip"
                                title={`SELECT * FROM "${t}" LIMIT 100`}
                                onClick={() =>
                                    setSql(`SELECT * FROM "${t}" LIMIT 100`)
                                }
                            >
                                {t}
                            </button>
                        ))}
                    </div>
                )}
            </div>

            <div className="query-editor">
                <Editor
                    height="100%"
                    language="sql"
                    theme="backtester-dark"
                    value={sql}
                    onChange={(v) => setSql(v ?? "")}
                    onMount={onMount}
                    options={{
                        minimap: { enabled: false },
                        fontSize,
                        scrollBeyondLastLine: false,
                        lineNumbers: "off",
                        folding: false,
                        wordWrap: "on",
                    }}
                />
            </div>

            <div className="query-results">
                {error ? (
                    <div className="query-error">⚠ {error}</div>
                ) : !result ? (
                    <div className="query-empty">
                        Results appear here — pick a table chip above or write
                        a query and hit Run.
                    </div>
                ) : (
                    <div className="panel query-panel">
                        <div className="panel-head">
                            <span className="panel-title">Results</span>
                            <span className="query-meta">
                                {result.rows.length.toLocaleString("en-US")} row
                                {result.rows.length === 1 ? "" : "s"}
                                {result.truncated ? " · truncated at 1,000" : ""}
                                {" · "}
                                {result.elapsedMs} ms
                            </span>
                        </div>
                        <div className="table-scroll">
                            <table>
                                <thead>
                                    <tr>
                                        {result.columns.map((c, i) => (
                                            <th
                                                key={i}
                                                className={
                                                    numCols[i] ? "num" : ""
                                                }
                                            >
                                                {c}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {result.rows.map((row, ri) => (
                                        <tr key={ri}>
                                            {row.map((v, ci) => (
                                                <td
                                                    key={ci}
                                                    className={
                                                        numCols[ci]
                                                            ? "num"
                                                            : ""
                                                    }
                                                >
                                                    {v}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
