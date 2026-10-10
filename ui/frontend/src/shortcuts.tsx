// Keyboard shortcuts: the one table every part of the app reads, so the help
// overlay, the button tooltips and the handler in App cannot drift apart.
//
// Pure on purpose -- shortcutFor() maps a key event to an action and nothing
// else -- so the smoke render can pin the bindings without a DOM, and App's
// handler is a switch over actions rather than a tangle of key tests.

export type ShortcutAction =
    | "run"
    | "tab-backtest"
    | "tab-config"
    | "tab-data"
    | "tab-research"
    | "toggle-assistant"
    | "max-assistant"
    | "toggle-results"
    | "max-results"
    | "save"
    | "help"
    | "palette"
    | "focus-search"
    | "escape";

export type Shortcut = {
    action: ShortcutAction;
    // What the help overlay and tooltips print.
    keys: string;
    label: string;
};

// In the order the help overlay lists them. Zoom is handled separately (it
// predates this table) but is listed so the overlay is the whole story.
export const SHORTCUTS: Shortcut[] = [
    { action: "palette", keys: "Ctrl+Shift+P", label: "Command palette: every action, by name" },
    { action: "run", keys: "Ctrl+Enter", label: "Run the backtest (on the Data tab: run the query)" },
    { action: "focus-search", keys: "/", label: "Jump to the ticker search (Backtest tab)" },
    { action: "tab-backtest", keys: "Ctrl+1", label: "Backtest tab" },
    { action: "tab-config", keys: "Ctrl+2", label: "Config tab" },
    { action: "tab-data", keys: "Ctrl+3", label: "Data tab" },
    { action: "tab-research", keys: "Ctrl+4", label: "Research tab (SEC filings and economic releases: CN, MY, NEWS, search)" },
    { action: "toggle-assistant", keys: "Ctrl+J", label: "Show or hide the assistant" },
    { action: "max-assistant", keys: "Ctrl+Shift+J", label: "Maximise or restore the assistant" },
    { action: "toggle-results", keys: "Ctrl+M", label: "Minimise or restore the results" },
    { action: "max-results", keys: "Ctrl+Shift+M", label: "Maximise or restore the results" },
    { action: "save", keys: "Ctrl+S", label: "Save the config and scripts (Config tab)" },
    { action: "help", keys: "F1", label: "Show this list" },
    { action: "escape", keys: "Esc", label: "Close this list or the palette, or restore a maximised pane" },
];

// Listed in the overlay only; App's existing wheel/key handler owns them.
// `mac` marks the ones pressed with Cmd on a Mac (see zoomFor); the arrows
// stay Ctrl, since Cmd+arrow moves the caret there.
export const ZOOM_SHORTCUTS: { keys: string; label: string; mac?: boolean }[] = [
    { keys: "Ctrl+= / Ctrl+-", label: "Zoom in / out", mac: true },
    { keys: "Ctrl+0", label: "Reset zoom", mac: true },
    { keys: "Ctrl+↑ / Ctrl+↓", label: "Zoom in / out (also Ctrl+scroll)" },
    { keys: "Double-click a pane's bar", label: "Minimise / maximise it" },
];

// The zoom a key event asks for, or null. Ctrl/Cmd with = + - 0 is the
// browser and macOS convention, and the only one a Mac user can press:
// Ctrl+arrow there is Mission Control. Ctrl+arrow is kept for everyone else,
// as it was. Alt is never part of it (AltGr types characters).
export function zoomFor(e: KeyLike): "in" | "out" | "reset" | null {
    if (e.altKey || !(e.ctrlKey || e.metaKey)) return null;
    switch (e.key) {
        case "=":
        case "+":
            return "in";
        case "-":
        case "_":
            return "out";
        case "0":
            return "reset";
    }
    if (e.ctrlKey && !e.metaKey) {
        if (e.key === "ArrowUp") return "in";
        if (e.key === "ArrowDown") return "out";
    }
    return null;
}

// Whether this is a Mac, where the bindings are pressed with Cmd and should be
// printed that way. navigator.platform is deprecated but is what WebKit -- the
// only engine this app runs in -- still fills in; the user agent covers the
// day it does not.
export const IS_MAC =
    typeof navigator !== "undefined" &&
    /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent || "");

// A binding as this platform presses it: "Ctrl+Shift+P" is "Cmd+Shift+P" on a
// Mac. Only for SHORTCUTS entries, which accept Cmd; the zoom keys are read
// from ctrlKey alone and are printed as they are.
export function displayKeys(keys: string, mac: boolean = IS_MAC): string {
    return mac ? keys.replace(/\bCtrl\+/g, "Cmd+") : keys;
}

// The printed keys for an action, for a button's title.
export function keysFor(action: ShortcutAction, mac: boolean = IS_MAC): string {
    return displayKeys(SHORTCUTS.find((s) => s.action === action)?.keys ?? "", mac);
}

type KeyLike = {
    key: string;
    ctrlKey: boolean;
    metaKey: boolean;
    shiftKey: boolean;
    altKey: boolean;
};

// The action a key event asks for, or null. Cmd counts as Ctrl so the same
// bindings work on a Mac (Cmd+Shift+P, Cmd+Enter, ...). Ctrl+K is kept as a
// second way to the palette -- the Slack/Linear/GitHub habit -- but is not
// advertised: one printed binding per action. Alt is never part of a binding, so Ctrl+Alt+... --
// AltGr on European layouts, which types characters -- is left alone.
export function shortcutFor(e: KeyLike): ShortcutAction | null {
    if (e.altKey) return null;
    if (e.key === "F1") return "help";
    if (e.key === "Escape") return "escape";
    // Bare "/", the GitHub/YouTube habit. Whether it applies -- never while
    // typing in a field -- is App's call (see typingInField), not this map's.
    if (e.key === "/" && !e.ctrlKey && !e.metaKey) return "focus-search";
    if (!(e.ctrlKey || e.metaKey)) return null;
    const k = e.key.toLowerCase();
    if (e.shiftKey) {
        if (k === "j") return "max-assistant";
        if (k === "m") return "max-results";
        if (k === "p") return "palette";
        return null;
    }
    switch (k) {
        case "enter":
            return "run";
        case "1":
            return "tab-backtest";
        case "2":
            return "tab-config";
        case "3":
            return "tab-data";
        case "4":
            return "tab-research";
        case "j":
            return "toggle-assistant";
        case "m":
            return "toggle-results";
        case "s":
            return "save";
        case "k":
            return "palette";
    }
    return null;
}

// Whether keyboard focus is somewhere that types: a text field, a select, an
// editable element or the Monaco editor. A bare-key shortcut must not steal a
// character from any of them.
export function typingInField(el: Element | null): boolean {
    if (!el) return false;
    const tag = el.tagName;
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return true;
    if ((el as HTMLElement).isContentEditable) return true;
    return !!el.closest(".monaco-editor");
}

// The help overlay. Click outside, Esc or F1 closes it.
export function ShortcutsHelp({ onClose }: { onClose: () => void }) {
    return (
        <div className="shortcuts-backdrop" onClick={onClose}>
            <div
                className="shortcuts-card"
                role="dialog"
                aria-label="Keyboard shortcuts"
                onClick={(e) => e.stopPropagation()}
            >
                <div className="shortcuts-head">
                    <span className="shortcuts-title">Keyboard shortcuts</span>
                    <span className="panel-head-spacer" />
                    <button type="button" className="panel-icon" onClick={onClose} title="Close (Esc)">
                        ×
                    </button>
                </div>
                <dl className="shortcuts-list">
                    {[
                        ...SHORTCUTS.map((s) => ({ ...s, keys: displayKeys(s.keys) })),
                        ...ZOOM_SHORTCUTS.map((z) => (z.mac ? { ...z, keys: displayKeys(z.keys) } : z)),
                    ].map((s) => (
                        <div className="shortcuts-row" key={s.keys}>
                            <dt>
                                {s.keys.split(" / ").map((combo, i) => (
                                    <span key={combo}>
                                        {i > 0 && " / "}
                                        <kbd>{combo}</kbd>
                                    </span>
                                ))}
                            </dt>
                            <dd>{s.label}</dd>
                        </div>
                    ))}
                </dl>
            </div>
        </div>
    );
}
