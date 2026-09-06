package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"my-backtester/src/apppath"
)

// tempConfigHome redirects the per-user config directory into the test's own
// temporary directory, so nothing here writes into the developer's home.
func tempConfigHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	dir, err := apppath.ConfigDir()
	if err != nil || !strings.HasPrefix(dir, home) {
		t.Skipf("user config directory is not redirectable here (%q, %v)", dir, err)
	}
	return dir
}

// The gallery must survive being launched from somewhere else. Bound to the
// UI, an empty list is not a missing feature -- it is the strategy picker
// offering nothing at all, which is what an installed app used to show.
func TestListStrategiesFromAnUnrelatedDirectory(t *testing.T) {
	tempConfigHome(t)
	t.Chdir(t.TempDir())

	a := &App{}
	cards := a.ListStrategies()
	if len(cards) == 0 {
		t.Fatal("no strategies listed with no library on disk")
	}
	for _, c := range cards {
		if c.File == "" || !c.Complete() {
			t.Errorf("card %+v came back without its description", c)
		}
	}
}

// The Lua tab opens the script a portfolio would run. In a packaged app that
// script lives in the binary, and an empty editor is how a user concludes the
// library is missing.
func TestLuaTabServesTheEmbeddedScript(t *testing.T) {
	tempConfigHome(t)
	t.Chdir(t.TempDir())

	a := &App{}
	const spec = "strategies/example.lua"
	if !a.FileExists(spec) {
		t.Fatal("FileExists says the shipped script is not there")
	}
	body, err := a.ReadFile(spec)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", spec, err)
	}
	if !strings.Contains(body, "function step") {
		t.Errorf("read something that is not the script:\n%s", body)
	}
	if a.FileExists("strategies/definitely_not_here.lua") {
		t.Error("a script that exists nowhere reported as present")
	}
}

// Saving a script from a packaged app has to land somewhere the user can
// write AND somewhere the engine looks. Beside a binary in /opt satisfies
// neither; the per-user library satisfies both.
func TestSavingAScriptWithNoLibraryOnDiskUsesTheUserDirectory(t *testing.T) {
	config := tempConfigHome(t)
	t.Chdir(t.TempDir())

	a := &App{}
	const spec = "strategies/mine.lua"
	if err := a.WriteFile(spec, "function step(day) end\n"); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	want := filepath.Join(config, "strategies", "mine.lua")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("script did not land in the per-user library: %v", err)
	}

	// And it is found again: the saved copy has to win over the embedded
	// library, or an edit appears to save and never takes effect.
	got, err := a.ReadFile(spec)
	if err != nil {
		t.Fatalf("ReadFile after save: %v", err)
	}
	if got != "function step(day) end\n" {
		t.Errorf("read back %q, want the saved script", got)
	}
	if resolved := resolveAppPath(spec); resolved != want {
		t.Errorf("resolveAppPath = %q, want the saved copy %q", resolved, want)
	}
}

// A checkout is unchanged: a save still lands in the repository's own
// library, not in the user's home. This is the case that would silently
// break the development loop.
func TestSavingAScriptInACheckoutStaysInTheCheckout(t *testing.T) {
	tempConfigHome(t)
	// cwd is ui/, so "../strategies" is the real library -- the same
	// directory resolveAppPath has always chosen.
	got := writablePath("strategies/brand-new-idea.lua")
	want := filepath.Join("..", "strategies", "brand-new-idea.lua")
	if got != want {
		t.Fatalf("writablePath = %q, want %q", got, want)
	}
}

// DefaultDBPath answers "" rather than a path that does not exist, because
// the header turns "" into "Choose DB…" and a wrong path into an error on
// every panel.
func TestDefaultDBPath(t *testing.T) {
	tempConfigHome(t)
	a := &App{}
	if got := a.DefaultDBPath(); got == "" {
		t.Error("the checkout's own database was not found from ui/")
	} else if _, err := os.Stat(got); err != nil {
		t.Errorf("DefaultDBPath = %q which does not exist: %v", got, err)
	}

	t.Chdir(t.TempDir())
	if got := a.DefaultDBPath(); got != "" {
		t.Errorf("DefaultDBPath = %q with no database anywhere, want \"\"", got)
	}
}
