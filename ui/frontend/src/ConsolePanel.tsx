import { useEffect, useState } from "react";
import { backtest } from "../wailsjs/go/models";

// The console shows what the engine had to say about a run — a Lua script that
// raised, a window that could not be fully simulated, a benchmark with no data.
// It deliberately does not show the engine's own logging: timings, row-scan
// warnings and metric dumps stay in the log, because a console that reports
// everything is one nobody reads. What reaches here is decided in Go, at the
// call site, by backtest.diagf.

type Props = { diagnostics: backtest.Diagnostic[]; fontSize: number };

export default function ConsolePanel({ diagnostics, fontSize }: Props) {
    const [open, setOpen] = useState(false);
    const errors = diagnostics.filter((d) => d.level === "error").length;
    const warnings = diagnostics.length - errors;

    // An error means the run did not do what was asked, so it opens itself.
    // Warnings are worth knowing and not worth interrupting for.
    useEffect(() => {
        setOpen(errors > 0);
    }, [diagnostics, errors]);

    if (diagnostics.length === 0) return null;

    const summary = [
        errors > 0 ? `${errors} error${errors === 1 ? "" : "s"}` : "",
        warnings > 0 ? `${warnings} warning${warnings === 1 ? "" : "s"}` : "",
    ]
        .filter(Boolean)
        .join(", ");

    return (
        <div className="console-panel" style={{ fontSize }}>
            <button
                className="console-head"
                onClick={() => setOpen(!open)}
                title={open ? "Hide run messages" : "Show run messages"}
            >
                <span className="console-caret">{open ? "▾" : "▸"}</span>
                <span className="console-title">Console</span>
                <span className={errors > 0 ? "console-count err" : "console-count"}>
                    {summary}
                </span>
            </button>
            {open && (
                <ul className="console-list">
                    {diagnostics.map((d, i) => (
                        <li key={i} className={`console-row ${d.level}`}>
                            <span className={`console-level ${d.level}`}>
                                {d.level}
                            </span>
                            {d.portfolio && (
                                <span className="console-portfolio">
                                    {d.portfolio}
                                </span>
                            )}
                            <span className="console-msg">{d.message}</span>
                        </li>
                    ))}
                </ul>
            )}
        </div>
    );
}
