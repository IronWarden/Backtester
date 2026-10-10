import { createContext, useCallback, useContext, useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";
import { createPortal } from "react-dom";
import { trackDrag } from "./frame";
import { readStoredObject, writeStored } from "./storage";

// Every panel in the results -- the verdicts, the equity curve, the metrics
// table, each collapsible -- is its own entity: double-click its header (or
// press ⤢) and it leaves the results strip for a window of its own, which can
// be dragged by its header, resized from its corner, maximised with another
// double-click, and sent back with ×. The strip keeps a one-line stub where the
// panel was, so nothing seems to have vanished.
//
// The results strip is a few hundred pixels at the bottom of the app, and the
// panels in it were designed to be read at full width and height. Popping one
// out is how a chart or a table gets the room it needs without the rest of the
// app giving up theirs.
//
// Presentation only: a floating panel renders the very same component tree,
// through a portal, so nothing it shows can differ from the docked copy.

// --- Collapsed / expanded --------------------------------------------------

// Panels below the headline ones open on click and remember it. Eleven panels
// rendering expanded at once is most of what "overwhelmed on first open"
// means, and the ones worth reading first — the verdicts, the curve, the
// metrics — are not the ones that take the most room.
//
// Presentation only: nothing here changes a number, and every panel that
// takes this treatment already hides itself when it has nothing to show, so a
// collapsed header never stands for an empty body.
const PANEL_STATE_KEY = "backtester-results-panels";

// Shape-guarded, not just parse-guarded: the indexing that follows happens in
// render, outside any try here, so a stored `"null"` would throw where nothing
// catches it. See storage.ts.
function readPanelState(): Record<string, boolean> {
    return readStoredObject<boolean>(PANEL_STATE_KEY);
}

function writePanelOpen(id: string, open: boolean) {
    // A browser that refuses storage still gets a working panel; it just
    // forgets the choice, which is the right way to degrade.
    writeStored(PANEL_STATE_KEY, JSON.stringify({ ...readPanelState(), [id]: open }));
}

// --- Floating geometry -----------------------------------------------------

export type Geo = { x: number; y: number; w: number; h: number; max: boolean };

const FLOAT_STATE_KEY = "backtester-float-panels";

function validGeo(v: unknown): v is Geo {
    if (typeof v !== "object" || v === null) return false;
    const g = v as Record<string, unknown>;
    return (
        ["x", "y", "w", "h"].every(
            (k) => typeof g[k] === "number" && Number.isFinite(g[k] as number),
        ) && typeof g.max === "boolean"
    );
}

// Which panels are out, and where. Remembered, so a layout someone arranged
// survives the next run and the next launch; a stored entry of the wrong shape
// is dropped rather than trusted.
export function readFloats(): Record<string, Geo> {
    const raw = readStoredObject<unknown>(FLOAT_STATE_KEY);
    const out: Record<string, Geo> = {};
    for (const [k, v] of Object.entries(raw)) if (validGeo(v)) out[k] = v;
    return out;
}

// A window is kept reachable whatever happened to the screen since it was
// placed: its header must stay inside the viewport, and it is never smaller
// than something that can be grabbed. Pure, so the smoke run can pin it.
export function clampGeo(g: Geo, vw: number, vh: number): Geo {
    const w = Math.max(280, Math.min(g.w, vw));
    const h = Math.max(160, Math.min(g.h, vh));
    return {
        ...g,
        w,
        h,
        x: Math.max(0, Math.min(g.x, vw - Math.min(w, 120))),
        y: Math.max(0, Math.min(g.y, vh - 40)),
    };
}

// Where a newly popped-out panel appears: centred, 70% of the window, and
// nudged down-right per window already out so a second one never lands
// exactly on the first.
export function defaultGeo(openCount: number, vw: number, vh: number): Geo {
    const w = Math.min(960, Math.round(vw * 0.7));
    const h = Math.min(640, Math.round(vh * 0.7));
    const step = 28 * (openCount % 6);
    return {
        x: Math.round((vw - w) / 2) + step,
        y: Math.round((vh - h) / 2) + step,
        w,
        h,
        max: false,
    };
}

type Dock = {
    floats: Record<string, Geo>;
    order: string[];
    float: (id: string) => void;
    dock: (id: string) => void;
    focus: (id: string) => void;
    setGeo: (id: string, g: Geo) => void;
};

const DockContext = createContext<Dock | null>(null);
export const DockProvider = DockContext.Provider;

// The one piece of state behind every floating panel, owned by ResultsView so
// that it outlives any single panel (a panel unmounts whenever a run has
// nothing for it to show, and its window has to be there again next time).
export function useDock(): Dock {
    const [floats, setFloats] = useState<Record<string, Geo>>(readFloats);
    // Stacking order, last = front. Not persisted: which window was on top is
    // not a layout anyone arranged.
    const [order, setOrder] = useState<string[]>(() => Object.keys(floats));

    useEffect(() => {
        writeStored(FLOAT_STATE_KEY, JSON.stringify(floats));
    }, [floats]);

    const focus = useCallback((id: string) => {
        setOrder((o) => (o[o.length - 1] === id ? o : [...o.filter((x) => x !== id), id]));
    }, []);
    const float = useCallback(
        (id: string) => {
            setFloats((f) =>
                f[id]
                    ? f
                    : {
                          ...f,
                          [id]: defaultGeo(
                              Object.keys(f).length,
                              window.innerWidth,
                              window.innerHeight,
                          ),
                      },
            );
            focus(id);
        },
        [focus],
    );
    const dock = useCallback((id: string) => {
        setFloats((f) => {
            const { [id]: _gone, ...rest } = f;
            return rest;
        });
        setOrder((o) => o.filter((x) => x !== id));
    }, []);
    const setGeo = useCallback((id: string, g: Geo) => {
        setFloats((f) => (f[id] ? { ...f, [id]: g } : f));
    }, []);
    return { floats, order, float, dock, focus, setGeo };
}

// --- The panel -------------------------------------------------------------

type PanelProps = {
    id: string;
    title: string;
    // Header controls beside the title: the legend, the metric-view toggle.
    head?: ReactNode;
    // Collapsible panels start closed and remember their state; the headline
    // ones are always open.
    collapsible?: boolean;
    // Extra class on the body when it floats, for panels whose content lays
    // itself out against the window (the equity curve fills it).
    fillClass?: string;
    children: ReactNode;
};

export function ResultPanel({ id, title, head, collapsible, fillClass, children }: PanelProps) {
    const dockState = useContext(DockContext);
    const [open, setOpen] = useState(() =>
        collapsible ? (readPanelState()[id] ?? false) : true,
    );
    const floating = dockState?.floats[id];

    function setOpenAndRemember(next: boolean) {
        setOpen(next);
        if (collapsible) writePanelOpen(id, next);
    }

    function popOut() {
        if (!dockState) return;
        // A panel is popped out to be read, so it opens on the way.
        if (!open) setOpenAndRemember(true);
        dockState.float(id);
    }

    if (floating && dockState) {
        return (
            <>
                <div className="panel panel-away">
                    <div className="panel-head">
                        <span className="panel-title">{title}</span>
                        <span className="panel-away-note">in its own window</span>
                        <span className="panel-head-spacer" />
                        <button
                            type="button"
                            className="panel-icon"
                            onClick={() => dockState.focus(id)}
                            title="Bring its window to the front"
                        >
                            Show
                        </button>
                        <button
                            type="button"
                            className="panel-icon"
                            onClick={() => dockState.dock(id)}
                            title="Put it back in the results"
                        >
                            Dock
                        </button>
                    </div>
                </div>
                <FloatWindow
                    id={id}
                    title={title}
                    head={head}
                    geo={floating}
                    z={dockState.order.indexOf(id)}
                    dock={dockState}
                    fillClass={fillClass}
                >
                    {children}
                </FloatWindow>
            </>
        );
    }

    return (
        <div className={`panel${collapsible ? ` collapsible ${open ? "open" : "closed"}` : ""}`}>
            <div
                className="panel-head"
                // Double-click anywhere on the header pops the panel out. On
                // a collapsible header the first click of the pair has already
                // toggled it, which popOut() undoes by opening it.
                onDoubleClick={(e) => {
                    if ((e.target as HTMLElement).closest("button:not(.panel-toggle), select, input"))
                        return;
                    popOut();
                }}
                title={dockState ? "Double-click to open in its own window" : undefined}
            >
                {collapsible ? (
                    <button
                        type="button"
                        className="panel-toggle"
                        aria-expanded={open}
                        onClick={(e) => {
                            // The second click of a double-click is the pop-out
                            // gesture, not a second toggle -- toggling twice
                            // is what made the panels look like they flickered
                            // open and shut on their own.
                            if (e.detail > 1) return;
                            setOpenAndRemember(!open);
                        }}
                    >
                        <span className="panel-chevron" aria-hidden="true">
                            {open ? "▾" : "▸"}
                        </span>
                        <span className="panel-title">{title}</span>
                    </button>
                ) : (
                    <span className="panel-title">{title}</span>
                )}
                {head}
                <span className="panel-head-spacer" />
                {dockState && (
                    <button
                        type="button"
                        className="panel-icon"
                        onClick={popOut}
                        title="Open in its own window (or double-click the header)"
                        aria-label={`Open ${title} in its own window`}
                    >
                        ⤢
                    </button>
                )}
            </div>
            {open && children}
        </div>
    );
}

// --- The window ------------------------------------------------------------

function FloatWindow({
    id,
    title,
    head,
    geo,
    z,
    dock,
    fillClass,
    children,
}: {
    id: string;
    title: string;
    head?: ReactNode;
    geo: Geo;
    z: number;
    dock: Dock;
    fillClass?: string;
    children: ReactNode;
}) {
    const winRef = useRef<HTMLDivElement>(null);
    const vw = typeof window === "undefined" ? 1400 : window.innerWidth;
    const vh = typeof window === "undefined" ? 900 : window.innerHeight;
    const g = clampGeo(geo, vw, vh);

    // Moving and resizing write the element's style directly and commit to
    // state once, on release. Committing every frame would re-render the whole
    // results view -- every table in it -- sixty times a second for a drag.
    function startMove(e: React.PointerEvent) {
        if (e.button !== 0 || g.max) return;
        if ((e.target as HTMLElement).closest("button, select, input")) return;
        e.preventDefault();
        dock.focus(id);
        const el = winRef.current;
        let last = g;
        trackDrag(
            e,
            "grabbing",
            (dx, dy) => {
                last = clampGeo({ ...g, x: g.x + dx, y: g.y + dy }, vw, vh);
                if (el) {
                    el.style.left = `${last.x}px`;
                    el.style.top = `${last.y}px`;
                }
            },
            () => dock.setGeo(id, last),
        );
    }

    function startResize(e: React.PointerEvent) {
        if (e.button !== 0 || g.max) return;
        e.preventDefault();
        e.stopPropagation();
        dock.focus(id);
        const el = winRef.current;
        let last = g;
        trackDrag(
            e,
            "nwse-resize",
            (dx, dy) => {
                last = clampGeo({ ...g, w: g.w + dx, h: g.h + dy }, vw, vh);
                if (el) {
                    el.style.width = `${last.w}px`;
                    el.style.height = `${last.h}px`;
                }
            },
            () => dock.setGeo(id, last),
        );
    }

    const toggleMax = () => dock.setGeo(id, { ...geo, max: !geo.max });

    const win = (
        <div
            ref={winRef}
            className={`float-win${g.max ? " max" : ""}`}
            style={
                g.max
                    ? { zIndex: 50 + z }
                    : { left: g.x, top: g.y, width: g.w, height: g.h, zIndex: 50 + z }
            }
            role="dialog"
            aria-label={title}
            onPointerDown={() => dock.focus(id)}
            onKeyDown={(e) => {
                if (e.key === "Escape") {
                    e.stopPropagation();
                    if (g.max) toggleMax();
                    else dock.dock(id);
                }
            }}
            tabIndex={-1}
        >
            <div
                className="float-head"
                onPointerDown={startMove}
                onDoubleClick={(e) => {
                    if ((e.target as HTMLElement).closest("button, select, input")) return;
                    toggleMax();
                }}
                title="Drag to move · double-click to maximise"
            >
                <span className="panel-title">{title}</span>
                {head}
                <span className="panel-head-spacer" />
                <button
                    type="button"
                    className="panel-icon"
                    onClick={toggleMax}
                    title={g.max ? "Restore" : "Maximise"}
                    aria-label={g.max ? "Restore" : "Maximise"}
                >
                    {g.max ? "❐" : "□"}
                </button>
                <button
                    type="button"
                    className="panel-icon"
                    onClick={() => dock.dock(id)}
                    title="Put it back in the results (Esc)"
                    aria-label="Dock"
                >
                    ×
                </button>
            </div>
            <div className={`float-body${fillClass ? ` ${fillClass}` : ""}`}>{children}</div>
            {!g.max && (
                <div
                    className="float-grip"
                    onPointerDown={startResize}
                    title="Drag to resize"
                    aria-hidden="true"
                />
            )}
        </div>
    );

    // Into .app, not <body>: the theme's tokens and the zoom's font-size are
    // set on .app, and a window outside it would render unstyled at 16px.
    // Server rendering (the smoke run) has no document to portal into; the
    // window renders in place there, which is all a string render can check.
    if (typeof document === "undefined") return win;
    return createPortal(win, document.querySelector(".app") ?? document.body);
}
