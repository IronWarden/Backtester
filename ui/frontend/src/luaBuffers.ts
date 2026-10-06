// The Lua tab's open scripts, one buffer each.
//
// A shipped library script opens READ-ONLY: it is what the gallery describes
// and what every config naming it runs, so an edit to it would quietly change
// strategies other people (and other portfolios) rely on. To change one, the
// user edits a copy, saved under its own my_ name — the same prefix Go's
// strategies.IsLibrary uses to tell a user's script from the library's.

import { baseName } from "./paths";

export type LuaBuffer = {
    path: string;
    text: string;
    // The text as last loaded from or saved to disk.
    pristine: string;
    // A shipped library script: shown, run and checked, never edited or saved.
    library: boolean;
    // Not on disk yet; Save creates it.
    isNew: boolean;
};

export const USER_PREFIX = "my_";

// Where "Edit a copy" of a library script is saved: beside it, as my_<name>.
export function copyPath(path: string): string {
    const base = baseName(path);
    const dir = path.slice(0, path.length - base.length);
    return `${dir}${USER_PREFIX}${base}`;
}

// The copy's text: the library's, with its first line — the "-- name.lua"
// label every library script opens with — saying whose copy it is.
export function copyText(libraryPath: string, text: string): string {
    const base = baseName(libraryPath);
    const label = `-- ${USER_PREFIX}${base} — your copy of ${base}; edit freely.`;
    const lines = text.split("\n");
    if (lines[0]?.trim() === `-- ${base}`) {
        lines[0] = label;
        return lines.join("\n");
    }
    return `${label}\n${text}`;
}

// Unsaved work: an edited buffer, or one that is not on disk yet.
export function isDirty(b: LuaBuffer): boolean {
    return !b.library && (b.isNew || b.text !== b.pristine);
}

// Adds b unless a buffer for its path is already open, which keeps two
// overlapping loads of one file from opening it twice.
export function addBuffer(buffers: LuaBuffer[], b: LuaBuffer): LuaBuffer[] {
    return buffers.some((o) => o.path === b.path) ? buffers : [...buffers, b];
}

// Sets one buffer's text. A library buffer never changes.
export function setBufferText(
    buffers: LuaBuffer[],
    path: string,
    text: string,
): LuaBuffer[] {
    return buffers.map((b) => (b.path === path && !b.library ? { ...b, text } : b));
}

// Marks the given paths as saved with the text they had.
export function markSaved(buffers: LuaBuffer[], saved: LuaBuffer[]): LuaBuffer[] {
    return buffers.map((b) => {
        const s = saved.find((o) => o.path === b.path);
        return s ? { ...b, pristine: s.text, isNew: false } : b;
    });
}
