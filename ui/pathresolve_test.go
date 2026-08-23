package main

import (
	"os"
	"path/filepath"
	"testing"
)

// T95. The go tests run with cwd=ui, which is exactly where the desktop app
// runs, so these assert the real thing rather than a simulation of it.

// The bug as reported: from ui/, the Lua tab could not see the library, said
// the default script was a new file, and saving created a zero-byte
// ui/strategies/example.lua that then shadowed the real one.
func TestDefaultLuaScriptIsVisibleFromUI(t *testing.T) {
	const spec = "strategies/example.lua" // DEFAULT_LUA_PATH in App.tsx

	if _, err := os.Stat(spec); err == nil {
		t.Fatalf("%s resolved from cwd — ui/strategies exists again, which is "+
			"the shadowing T86 removed", spec)
	}

	a := &App{}
	if !a.FileExists(spec) {
		t.Errorf("FileExists(%q) is false from cwd=ui, so the Lua tab reports "+
			"a new file and Save creates a shadowing copy", spec)
	}
	got, err := a.ReadFile(spec)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", spec, err)
	}
	if len(got) == 0 {
		t.Errorf("%s read as empty — an empty script is what got written into "+
			"ui/strategies last time", spec)
	}
}

// Writing must land on the library, not create a second copy beside the
// binary. The assertion that matters is the absence of ui/strategies.
func TestWriteResolvesToTheLibraryNotCwd(t *testing.T) {
	if got := resolveAppPath("strategies/example.lua"); got != filepath.Join("..", "strategies", "example.lua") {
		t.Errorf("existing library script resolved to %q, want ../strategies/example.lua", got)
	}
	// A script that does not exist yet still belongs in the library the
	// gallery lists — its parent directory is what decides.
	if got := resolveAppPath("strategies/brand-new-idea.lua"); got != filepath.Join("..", "strategies", "brand-new-idea.lua") {
		t.Errorf("new library script resolved to %q, want ../strategies/brand-new-idea.lua", got)
	}
	if _, err := os.Stat("strategies"); err == nil {
		t.Error("resolving a script path created ui/strategies")
	}
}

func TestResolveAppPathLeavesEverythingElseAlone(t *testing.T) {
	// Absolute paths come from the file dialogs; second-guessing them is wrong.
	abs := filepath.Join(t.TempDir(), "config.toml")
	if got := resolveAppPath(abs); got != abs {
		t.Errorf("absolute path rewritten to %q", got)
	}
	if got := resolveAppPath(""); got != "" {
		t.Errorf("empty path rewritten to %q", got)
	}
	// cwd still shadows a parent when the file is actually there: nearest wins.
	if got := resolveAppPath("app.go"); got != "app.go" {
		t.Errorf("a file present in cwd resolved to %q, want app.go", got)
	}
	// A new file in an existing directory stays put — this must not start
	// scattering ordinary saves into parent directories.
	if got := resolveAppPath("zz-not-a-real-file.txt"); got != "zz-not-a-real-file.txt" {
		t.Errorf("new file in cwd resolved to %q", got)
	}
	// Nothing to go on: unchanged, so a write fails where it always failed
	// rather than somewhere surprising.
	const nowhere = "no-such-dir/deeper/file.txt"
	if got := resolveAppPath(nowhere); got != nowhere {
		t.Errorf("unresolvable path rewritten to %q", got)
	}
}

// WriteFile still does what it always did for an ordinary path.
func TestWriteFileRoundTrips(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "script.lua")
	a := &App{}
	if err := a.WriteFile(path, "-- hello\n"); err != nil {
		t.Fatal(err)
	}
	got, err := a.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != "-- hello\n" {
		t.Errorf("round-trip gave %q", got)
	}
}
