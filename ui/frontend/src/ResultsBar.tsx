// The strip between the tabs and the results: drag it to resize, double-click
// to reset, and its two buttons minimise the results to this strip or maximise
// them over the tabs. A small component of its own because App cannot be
// smoke-rendered (Monaco), and this one can.

import type React from "react";

export type ResultsLayout = "normal" | "min" | "max";

// What pressing a button does from a given state: the same button toggles back
// to normal, the other one switches straight across.
export function nextResultsLayout(cur: ResultsLayout, pressed: "min" | "max"): ResultsLayout {
    return cur === pressed ? "normal" : pressed;
}

// Stored state is input. Maximised is deliberately not restored: opening the
// app with the tabs hidden reads as a broken layout rather than a preference.
export function parseResultsLayout(raw: string | null): ResultsLayout {
    return raw === "min" ? "min" : "normal";
}

type Props = {
    view: ResultsLayout;
    onView: (v: ResultsLayout) => void;
    onResizeStart: (e: React.PointerEvent) => void;
    onReset: () => void;
    count: number;
};

export default function ResultsBar({ view, onView, onResizeStart, onReset, count }: Props) {
    // The buttons sit on the drag handle, so a press on one must not also
    // start a resize, and a double-click on one must not also reset.
    const keep = (e: React.SyntheticEvent) => e.stopPropagation();
    return (
        <div
            className={`resize-handle results-bar ${view}`}
            onPointerDown={onResizeStart}
            onDoubleClick={onReset}
            title="Drag to resize the results panel · double-click to reset"
        >
            <span className="results-bar-title">
                {count > 0 ? `Results · ${count}` : "Results"}
            </span>
            <span className="panel-head-spacer" />
            <button
                type="button"
                className="panel-icon"
                onPointerDown={keep}
                onDoubleClick={keep}
                onClick={() => onView(nextResultsLayout(view, "min"))}
                title={view === "min" ? "Restore the results" : "Minimise the results"}
                aria-label={view === "min" ? "Restore results" : "Minimise results"}
            >
                {view === "min" ? "▴" : "▁"}
            </button>
            <button
                type="button"
                className="panel-icon"
                onPointerDown={keep}
                onDoubleClick={keep}
                onClick={() => onView(nextResultsLayout(view, "max"))}
                title={view === "max" ? "Restore the results" : "Maximise the results"}
                aria-label={view === "max" ? "Restore results" : "Maximise results"}
            >
                {view === "max" ? "❐" : "□"}
            </button>
        </div>
    );
}
