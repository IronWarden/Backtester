// Path labels for the header chips.
//
// Extracted from App.tsx so the smoke run can exercise them: since the app
// asks Go where the bundled database is (DefaultDBPath), an empty path is a
// state the header can really be in -- no database was found, and the chip
// has to ask for one rather than name a file that is not there.

// Last path segment ("/a/b/config.toml" -> "config.toml"). Handles both
// separators because a Windows path reaches the frontend as it was written.
export const baseName = (p: string) => p.split(/[\\/]/).pop() || p;

// Label for a chip that names a file, or invites one to be chosen. `empty` is
// what the chip reads when nothing is set -- "Choose DB…", "unsaved", "none".
export const chipLabel = (path: string, empty: string) =>
    path ? baseName(path) : empty;
