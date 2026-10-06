// The command palette (Ctrl+K): every action the app has, by name, one
// keystroke away. The shortcuts make the common actions fast for someone who
// has learned them; the palette is for everyone else, and for the actions that
// have no key at all.
//
// filterCommands is pure so the smoke render can pin the matching; the
// component only owns the query, the highlight and the keyboard.

import { useState } from "react";

export type Command = {
    id: string;
    label: string;
    // Shown at the right, so the palette also teaches the shortcuts.
    keys?: string;
    // A disabled command is listed but not runnable, with the reason as its
    // hint -- "Save" on the Backtest tab should say why, not vanish.
    disabled?: boolean;
    hint?: string;
    run: () => void;
};

// Every word of the query must appear in the label, in any order, ignoring
// case. Ranked: a label starting with the query first, then one where a word
// starts with the first query word, then the rest, each in list order.
export function filterCommands(commands: Command[], query: string): Command[] {
    const words = query.toLowerCase().split(/\s+/).filter(Boolean);
    if (words.length === 0) return commands;
    const q = query.trim().toLowerCase();
    const rank = (c: Command): number => {
        const label = c.label.toLowerCase();
        if (label.startsWith(q)) return 0;
        if (label.split(/[\s/·:-]+/).some((w) => w.startsWith(words[0]))) return 1;
        return 2;
    };
    return commands
        .map((c, i) => ({ c, i }))
        .filter(({ c }) => {
            const label = c.label.toLowerCase();
            return words.every((w) => label.includes(w));
        })
        .sort((a, b) => rank(a.c) - rank(b.c) || a.i - b.i)
        .map(({ c }) => c);
}

export function CommandPalette({
    commands,
    onClose,
}: {
    commands: Command[];
    onClose: () => void;
}) {
    const [query, setQuery] = useState("");
    const [highlight, setHighlight] = useState(0);
    const shown = filterCommands(commands, query);
    const at = Math.min(highlight, Math.max(0, shown.length - 1));

    // Close first, then run: a command that opens a dialog or moves focus
    // should not have the palette sitting on top of it.
    function pick(c: Command | undefined) {
        if (!c || c.disabled) return;
        onClose();
        c.run();
    }

    // Wrapping, and skipping nothing: a disabled row can be highlighted so
    // its hint can be read, it just cannot be run.
    function move(by: number) {
        if (shown.length === 0) return;
        setHighlight((at + by + shown.length) % shown.length);
    }

    return (
        <div className="shortcuts-backdrop palette-backdrop" onMouseDown={onClose}>
            <div
                className="palette"
                role="dialog"
                aria-label="Command palette"
                onMouseDown={(e) => e.stopPropagation()}
            >
                <input
                    className="palette-input"
                    autoFocus
                    placeholder="Type a command…"
                    value={query}
                    role="combobox"
                    aria-expanded={shown.length > 0}
                    aria-controls="palette-list"
                    aria-activedescendant={shown[at] ? `palette-${shown[at].id}` : undefined}
                    onChange={(e) => {
                        setQuery(e.target.value);
                        setHighlight(0);
                    }}
                    onKeyDown={(e) => {
                        if (e.key === "ArrowDown") {
                            e.preventDefault();
                            move(1);
                        } else if (e.key === "ArrowUp") {
                            e.preventDefault();
                            move(-1);
                        } else if (e.key === "Enter") {
                            e.preventDefault();
                            pick(shown[at]);
                        }
                    }}
                />
                <ul className="palette-list" id="palette-list" role="listbox">
                    {shown.length === 0 && <li className="palette-empty">No matching command</li>}
                    {shown.map((c, i) => (
                        <li
                            key={c.id}
                            id={`palette-${c.id}`}
                            role="option"
                            aria-selected={i === at}
                            aria-disabled={c.disabled || undefined}
                            className={
                                (i === at ? "on" : "") + (c.disabled ? " disabled" : "")
                            }
                            onMouseEnter={() => setHighlight(i)}
                            onMouseDown={(e) => {
                                e.preventDefault();
                                pick(c);
                            }}
                        >
                            <span className="palette-label">{c.label}</span>
                            {c.disabled && c.hint && (
                                <span className="palette-hint">{c.hint}</span>
                            )}
                            {c.keys && <kbd className="palette-keys">{c.keys}</kbd>}
                        </li>
                    ))}
                </ul>
            </div>
        </div>
    );
}
