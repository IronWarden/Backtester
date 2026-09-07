import { useCallback, useEffect, useState } from "react";
import { StopSync, SyncData, SyncStatus } from "../wailsjs/go/main/App";
import { EventsOff, EventsOn } from "../wailsjs/runtime/runtime";
import { main } from "../wailsjs/go/models";

// The Data tab's "is this database current?" strip.
//
// Before this existed the only way to refresh prices was to ask the assistant,
// which drives fetch_history three symbols at a time and gives up long before
// ten thousand tickers are done. The panel answers the question directly —
// how far behind is the data, and one button to fix it.

type Props = { dbPath: string };

export type Progress = {
    batch: number;
    batches: number;
    stored: number;
    rows: number;
    failed: number;
    current: string;
};

const num = (n: number) => n.toLocaleString();

export default function SyncPanel({ dbPath }: Props) {
    const [status, setStatus] = useState<main.SyncReport | null>(null);
    const [report, setReport] = useState<main.SyncReport | null>(null);
    const [progress, setProgress] = useState<Progress | null>(null);
    const [running, setRunning] = useState(false);
    const [error, setError] = useState("");

    const refresh = useCallback(() => {
        if (!dbPath) {
            setStatus(null);
            return;
        }
        SyncStatus(dbPath)
            .then((s) => {
                setStatus(s);
                setError("");
            })
            .catch((e) => setError(String(e)));
    }, [dbPath]);

    useEffect(() => {
        refresh();
    }, [refresh]);

    // Progress and completion arrive as events, because a full sync runs for
    // minutes and a bound call that long would block the frontend.
    useEffect(() => {
        EventsOn("sync:progress", (p: Progress) => setProgress(p));
        EventsOn("sync:done", (r: main.SyncReport) => {
            setReport(r);
            setRunning(false);
            setProgress(null);
            refresh();
        });
        return () => {
            EventsOff("sync:progress");
            EventsOff("sync:done");
        };
    }, [refresh]);

    async function start() {
        if (!dbPath || running) return;
        setRunning(true);
        setReport(null);
        setError("");
        try {
            await SyncData(dbPath);
        } catch (e) {
            setError(String(e));
            setRunning(false);
        }
    }

    return (
        <SyncPanelView
            dbPath={dbPath}
            status={status}
            report={report}
            progress={progress}
            running={running}
            error={error}
            onStart={start}
            onStop={() => StopSync()}
        />
    );
}

// The panel's markup, with every input passed in. Split from the container
// above so the smoke render can mount each state — stale, current, running,
// finished, failed — which is impossible through a component that only ever
// reaches those states via effects and Wails events.
export type ViewProps = {
    dbPath: string;
    status: main.SyncReport | null;
    report: main.SyncReport | null;
    progress: Progress | null;
    running: boolean;
    error: string;
    onStart: () => void;
    onStop: () => void;
};

export function SyncPanelView({
    dbPath,
    status,
    report,
    progress,
    running,
    error,
    onStart,
    onStop,
}: ViewProps) {
    const stale = status?.stale ?? 0;
    const pct =
        progress && progress.batches > 0
            ? Math.round((progress.batch / progress.batches) * 100)
            : 0;

    return (
        <div className="sync-panel">
            <div className="sync-row">
                <span className="sync-label">Price data</span>
                {!dbPath ? (
                    <span className="sync-note">Choose a database first.</span>
                ) : !status ? (
                    <span className="sync-note">Checking…</span>
                ) : stale === 0 ? (
                    <span className="sync-note sync-ok">
                        Current through {status.through}.
                    </span>
                ) : (
                    <span className="sync-note sync-stale">
                        {num(stale)} of {num(status.considered)} tickers are
                        behind {status.through}.
                    </span>
                )}

                <div className="sync-spacer" />

                {running ? (
                    <button className="sync-stop" onClick={onStop}>
                        Stop
                    </button>
                ) : (
                    <button
                        className="sync-go"
                        onClick={onStart}
                        disabled={!dbPath || stale === 0}
                        title={
                            stale === 0
                                ? "Every tradable ticker is already current"
                                : "Fetch each stale ticker's missing bars"
                        }
                    >
                        Sync now
                    </button>
                )}
            </div>

            {running && (
                <div className="sync-progress">
                    <div className="sync-bar">
                        <div className="sync-bar-fill" style={{ width: `${pct}%` }} />
                    </div>
                    <span className="sync-note">
                        {progress
                            ? `Batch ${progress.batch} of ${progress.batches} · ` +
                              `${progress.current} · ${num(progress.stored)} tickers, ` +
                              `${num(progress.rows)} bars stored` +
                              (progress.failed > 0
                                  ? ` · ${num(progress.failed)} failed`
                                  : "")
                            : "Starting…"}
                    </span>
                </div>
            )}

            {report && !running && (
                <div className="sync-note sync-result">
                    Updated {num(report.updated)}{" "}
                    {report.updated === 1 ? "ticker" : "tickers"} with{" "}
                    {num(report.rows)} new daily bars
                    {report.noNewBars > 0 &&
                        `; ${num(report.noNewBars)} had not traded since their last bar`}
                    .
                    {Object.keys(report.failed ?? {}).length > 0 &&
                        ` ${num(Object.keys(report.failed).length)} failed.`}
                    {report.incomplete &&
                        ` Stopped early (${report.reason}) — ${num(
                            report.remaining,
                        )} not attempted. Everything fetched is saved; syncing again resumes from here.`}
                </div>
            )}

            {error && <div className="sync-note sync-error">{error}</div>}
        </div>
    );
}
