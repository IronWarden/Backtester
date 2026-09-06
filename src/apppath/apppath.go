// Package apppath locates the files the application ships with -- the Lua
// strategy library, the bundled database, the Python loaders -- without
// assuming the process was started from inside its own source checkout.
//
// Every path in this project used to be resolved from the working directory.
// The CLI runs from src/ and the desktop app from ui/, so "../strategies" was
// right for both and wrong for everything else: a binary launched from
// ~/Downloads, from a desktop launcher, or installed under /opt found no
// strategy library at all, which made the gallery empty and every
// Strategy = "lua:strategies/..." unresolvable.
//
// Resolution therefore walks three places in order:
//
//  1. the working directory and its parents, so a checkout behaves exactly as
//     it did before this package existed;
//  2. the per-user directory a packaged app writes to, so a script saved from
//     the Lua tab is found again next launch;
//  3. the executable's own directory and its parent, so an installed binary
//     finds what was installed beside it.
//
// Nothing here creates anything except ConfigDir, and nothing here knows what
// a resource means -- callers ask for a relative path and get the first one
// that exists.
package apppath

import (
	"fmt"
	"os"
	"path/filepath"
)

// appDirName is the per-user directory this app owns, under the platform's
// configuration directory (~/.config/backtester on Linux).
const appDirName = "backtester"

// executable is os.Executable, indirected so tests can stand the binary
// somewhere they control without moving the test binary itself.
var executable = os.Executable

// Roots returns the directories a bundled resource may live under, nearest
// first. The first three are relative on purpose: they are the working
// directory walk this project has always done, and returning them unchanged
// keeps a resolved path looking the way it always looked ("../strategies",
// not an absolute path into the user's home).
func Roots() []string {
	roots := []string{".", "..", filepath.Join("..", "..")}
	// The per-user directory is a place to LOOK even before anything has been
	// written there, so this is the non-creating spelling: merely resolving a
	// path must not leave a directory behind.
	if dir, err := configDirPath(); err == nil {
		roots = append(roots, dir)
	}
	if exe, err := executable(); err == nil {
		// EvalSymlinks so a symlink on $PATH (or an AppImage's mount point)
		// resolves to where the payload actually sits, not to the link.
		if real, err := filepath.EvalSymlinks(exe); err == nil {
			exe = real
		}
		dir := filepath.Dir(exe)
		roots = append(roots, dir, filepath.Dir(dir))
	}
	return dedupe(roots)
}

// dedupe drops repeated roots, comparing by absolute path so "." and the
// executable's directory do not both appear when the app is launched from
// beside its own binary. The first spelling of a root wins, which is what
// keeps the relative entries relative.
func dedupe(roots []string) []string {
	seen := make(map[string]bool, len(roots))
	out := make([]string, 0, len(roots))
	for _, r := range roots {
		key := r
		if abs, err := filepath.Abs(r); err == nil {
			key = abs
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, r)
	}
	return out
}

// Resolve returns the first root under which rel exists, and whether one did.
// An absolute rel is returned untouched if it exists -- second-guessing a
// path the user picked from a file dialog would be wrong.
func Resolve(rel string) (string, bool) {
	if rel == "" {
		return "", false
	}
	if filepath.IsAbs(rel) {
		if _, err := os.Stat(rel); err == nil {
			return rel, true
		}
		return "", false
	}
	for _, root := range Roots() {
		p := join(root, rel)
		if _, err := os.Stat(p); err == nil {
			return p, true
		}
	}
	return "", false
}

// ResolveDir is Resolve restricted to directories, so a file that happens to
// share a library's name cannot answer for it.
func ResolveDir(rel string) (string, bool) {
	if rel == "" {
		return "", false
	}
	for _, root := range Roots() {
		p := join(root, rel)
		if info, err := os.Stat(p); err == nil && info.IsDir() {
			return p, true
		}
	}
	return "", false
}

// Candidates returns rel joined to every root, existing or not. Callers that
// have to choose a place to CREATE a file need the whole list; Resolve only
// answers for files that are already there.
func Candidates(rel string) []string {
	roots := Roots()
	out := make([]string, 0, len(roots))
	for _, root := range roots {
		out = append(out, join(root, rel))
	}
	return out
}

// join keeps a "." root from turning "strategies" into "./strategies", which
// would change every path string the UI shows and every test that pins one.
func join(root, rel string) string {
	if root == "." {
		return rel
	}
	return filepath.Join(root, rel)
}

// Root returns the directory the app's bundled files sit under: the first
// root holding a strategies/ library. Falls back to "..", the answer this
// project assumed unconditionally before, so a checkout with no library
// present behaves exactly as it used to.
func Root() string {
	if dir, ok := ResolveDir("strategies"); ok {
		return filepath.Dir(dir)
	}
	return ".."
}

// ConfigDir returns the per-user configuration directory for this app,
// creating it on demand. This is where the API key and any script saved from
// a packaged app go: an installed binary may sit on a read-only filesystem,
// and a user's home directory is the one place it can always write.
func ConfigDir() (string, error) {
	dir, err := configDirPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating %s: %w", dir, err)
	}
	return dir, nil
}

// configDirPath names the directory without creating it.
func configDirPath() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("no user config directory: %w", err)
	}
	return filepath.Join(base, appDirName), nil
}

// DataDir returns the per-user directory for large files the app maintains
// rather than the user -- a writable copy of a shipped database, above all.
// Kept apart from ConfigDir because these are gigabytes, not settings, and on
// Linux the convention puts them in different places
// ($XDG_DATA_HOME/~/.local/share rather than $XDG_CONFIG_HOME/~/.config).
func DataDir() (string, error) {
	base := os.Getenv("XDG_DATA_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("no home directory: %w", err)
		}
		base = filepath.Join(home, ".local", "share")
	}
	dir := filepath.Join(base, appDirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating %s: %w", dir, err)
	}
	return dir, nil
}

// UserStrategiesDir is the library a packaged app writes to, under ConfigDir.
// It is created only when something is about to be written into it.
func UserStrategiesDir() (string, error) {
	dir, err := ConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "strategies"), nil
}
