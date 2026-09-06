import { useEffect, useState } from "react";
import { backtest } from "../wailsjs/go/models";
import { baseName } from "./paths";

// The console shows what the engine had to say about a run — a Lua script that
// raised, a window that could not be fully simulated, a benchmark with no data —
// plus whatever a strategy chose to say for itself through log().
// It deliberately does not show the engine's own logging: timings, row-scan
// warnings and metric dumps stay in the log, because a console that reports
// everything is one nobody reads. What reaches here is decided in Go, at the
// call site, by backtest.diagf.

type Props = {
    diagnostics: backtest.Diagnostic[];
    fontSize: number;
    // Called when a row that knows where it happened is clicked. Optional, so
    // a console rendered without an editor behind it (the smoke run, a future
    // read-only view) still renders every row -- just not as a link.
    onJump?: (script: string, line: number) => void;
};

export default function ConsolePanel({ diagnostics, fontSize, onJump }: Props) {
    const errors = diagnostics.filter((d) => d.level === "error").length;
    // Seeded, not only reacted to: a run that errored should render open on
    // the first paint rather than flashing closed and then opening. The effect
    // below still handles every LATER set of diagnostics.
    const [open, setOpen] = useState(errors > 0);
    const warnings = diagnostics.filter((d) => d.level === "warn").length;
    const notes = diagnostics.filter((d) => d.level === "info").length;

    // An error means the run did not do what was asked, so it opens itself.
    // Warnings are worth knowing and not worth interrupting for.
    useEffect(() => {
        setOpen(errors > 0);
    }, [diagnostics, errors]);

    if (diagnostics.length === 0) return null;

    const summary = [
        errors > 0 ? `${errors} error${errors === 1 ? "" : "s"}` : "",
        warnings > 0 ? `${warnings} warning${warnings === 1 ? "" : "s"}` : "",
        // "note" rather than "info": these are the strategy's own log() calls.
        notes > 0 ? `${notes} note${notes === 1 ? "" : "s"}` : "",
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
                    {diagnostics.map((d, i) => {
                        // A row knows where it happened when the engine could
                        // parse a position out of the message. The message
                        // already said "sma_cross.lua:41:"; until now the
                        // reader had to go and find line 41 themselves.
                        const jumpable = Boolean(onJump && d.script && d.line > 0);
                        return (
                            <li key={i} className={`console-row ${d.level}`}>
                                <span className={`console-level ${d.level}`}>
                                    {d.level}
                                </span>
                                {d.portfolio && (
                                    <span className="console-portfolio">
                                        {d.portfolio}
                                    </span>
                                )}
                                {jumpable && (
                                    <button
                                        type="button"
                                        className="console-jump"
                                        title={`Open ${d.script} at line ${d.line}`}
                                        onClick={() => onJump!(d.script, d.line)}
                                    >
                                        {baseName(d.script)}:{d.line}
                                    </button>
                                )}
                                <span className="console-msg">{d.message}</span>
                            </li>
                        );
                    })}
                </ul>
            )}
        </div>
    );
}
