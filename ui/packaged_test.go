package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"my-backtester/src/apppath"
	"my-backtester/src/data"
	"my-backtester/strategies"
)

// userScript reports whether a script is the user's own (my_…) rather than
// the library's; the library's description rules do not apply to it.
func userScript(file string) bool {
	return strings.HasPrefix(file, strategies.UserPrefix)
}

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
		if userScript(c.File) {
			continue
		}
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
	const spec = "strategies/buy_and_hold.lua"
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

// DatabaseNotice is the frontend's only way to learn how the database opened.
// Silence is the ordinary case; a sentence means the app is not running
// against the file the user pointed at, or cannot write to it.
func TestDatabaseNoticeIsSilentOnAnOrdinaryDatabase(t *testing.T) {
	if got, err := (&App{}).DatabaseNotice(""); got != "" || err != nil {
		t.Fatalf("DatabaseNotice(\"\") = %q, %v; want no notice and no error", got, err)
	}

	path := filepath.Join(t.TempDir(), "own.db")
	if _, err := data.InitDB(path); err != nil {
		t.Fatalf("creating a scratch database: %v", err)
	}
	got, err := (&App{}).DatabaseNotice(path)
	if err != nil {
		t.Fatalf("DatabaseNotice: %v", err)
	}
	if got != "" {
		t.Errorf("DatabaseNotice = %q, want silence for a database opened as asked", got)
	}
}

// A database that cannot be opened at all reaches the user as an error, not
// as an empty notice -- otherwise every panel fails separately with DuckDB's
// own wording.
func TestDatabaseNoticeReportsAnUnopenableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-database.db")
	if err := os.WriteFile(path, []byte("this is not a duckdb file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := (&App{}).DatabaseNotice(path); err == nil {
		t.Fatal("a file that is not a database opened without complaint")
	}
}

// The Lua tab shows library scripts read-only; WriteFile is the backstop that
// keeps any other path from overwriting one.
func TestWriteFileRefusesALibraryScript(t *testing.T) {
	tempConfigHome(t)
	t.Chdir(t.TempDir())

	a := &App{}
	err := a.WriteFile("strategies/rsi.lua", "-- gutted\n")
	if err == nil || !strings.Contains(err.Error(), "my_rsi.lua") {
		t.Fatalf("WriteFile over a library script: err = %v, want a refusal "+
			"naming the copy", err)
	}
	if !a.IsLibraryScript("strategies/rsi.lua") || a.IsLibraryScript("strategies/my_rsi.lua") {
		t.Error("IsLibraryScript disagrees with strategies.IsLibrary")
	}
	if err := a.WriteFile("strategies/my_rsi.lua", "function step(day) end\n"); err != nil {
		t.Errorf("a user's copy must save: %v", err)
	}
}
