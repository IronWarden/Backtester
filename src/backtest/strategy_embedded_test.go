package backtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"my-backtester/strategies"
)

// The packaged case: a binary launched from a directory that has no
// strategies/ next to it, or above it, or above that. Every config in the
// app says Strategy = "lua:strategies/<file>.lua", so before the library was
// embedded this was not a degraded experience -- it was every shipped
// strategy failing to load.
func TestShippedStrategyLoadsWithNoLibraryOnDisk(t *testing.T) {
	t.Chdir(t.TempDir())

	spec := filepath.Join("strategies", "sma_cross.lua")
	if _, err := resolveLuaPath(spec); err == nil {
		t.Fatal("a library was found on disk; this test needs the case where none is")
	}

	got, source, err := loadLuaScript(spec)
	if err != nil {
		t.Fatalf("loadLuaScript(%q) with no library on disk: %v", spec, err)
	}
	if got != spec {
		t.Errorf("display path = %q, want the spec as written %q", got, spec)
	}
	if !strings.Contains(string(source), "function step") {
		t.Errorf("embedded source does not look like a strategy:\n%s", source)
	}

	// And it runs: an embedded script has no file for DoFile to open, so the
	// loading path is genuinely different from the on-disk one.
	p, logs := runStrategyScript(t, spec, map[string]any{
		"short": int64(20), "long": int64(50),
	})
	if logs != "" {
		t.Errorf("embedded sma_cross logged errors:\n%s", logs)
	}
	if len(p.DailyReturns) == 0 && len(p.Positions) == 0 && p.BuyingPower == libCash {
		t.Error("embedded strategy never traded; it was loaded but did nothing")
	}
}

// The on-disk copy wins. Editing a script in the Lua tab is the development
// loop, and a stale copy compiled into the binary silently answering instead
// would be the worst possible failure: the run succeeds with the wrong code.
func TestOnDiskScriptShadowsTheEmbeddedOne(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.MkdirAll("strategies", 0o755); err != nil {
		t.Fatal(err)
	}
	marker := "-- edited on disk\nfunction step(day) end\n"
	spec := filepath.Join("strategies", "sma_cross.lua")
	if err := os.WriteFile(spec, []byte(marker), 0o644); err != nil {
		t.Fatal(err)
	}

	got, source, err := loadLuaScript(spec)
	if err != nil {
		t.Fatalf("loadLuaScript: %v", err)
	}
	if got != spec {
		t.Errorf("display path = %q, want %q", got, spec)
	}
	if string(source) != marker {
		t.Errorf("loaded the embedded copy over the one on disk:\n%s", source)
	}
}

// A script that exists nowhere is still an error. The embedded library must
// not turn a typo into a silent fallback.
func TestMissingScriptStaysAnError(t *testing.T) {
	t.Chdir(t.TempDir())
	if _, _, err := loadLuaScript("strategies/definitely_not_here.lua"); err == nil {
		t.Fatal("a missing script should be an error, not a silent pass")
	}
	if _, _, err := loadLuaScript("../../../etc/passwd.lua"); err == nil {
		t.Fatal("a path outside the library resolved to something")
	}
}

// The embedded library must be the whole library. A script added to
// strategies/ but excluded from the embed (a name the //go:embed pattern
// misses, say) would work in the checkout and vanish in the package.
func TestEmbeddedLibraryMatchesTheDirectory(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join("..", "..", "strategies"))
	if err != nil {
		t.Fatalf("read strategies dir: %v", err)
	}
	embedded := map[string]bool{}
	for _, name := range strategies.Names() {
		embedded[name] = true
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".lua") {
			continue
		}
		if !embedded[e.Name()] {
			t.Errorf("%s is in strategies/ but not embedded: it would work in "+
				"a checkout and be missing from a packaged app", e.Name())
		}
		delete(embedded, e.Name())
	}
	for name := range embedded {
		t.Errorf("%s is embedded but no longer in strategies/", name)
	}
}

// userScript reports whether a file in strategies/ is the user's own — the
// scratch file or a copy, named my_ — rather than part of the library. The
// library's rules (gallery tags, a test entry, a declared fundamentals
// table) are for what ships, not for what a user is writing.
func userScript(file string) bool {
	return strings.HasPrefix(file, strategies.UserPrefix)
}

// The gallery has to come from the same place a run does, or the app
// describes scripts it cannot load.
func TestLoadLibraryWorksWithNoLibraryOnDisk(t *testing.T) {
	t.Chdir(t.TempDir())
	cards, err := LoadLibrary()
	if err != nil {
		t.Fatalf("LoadLibrary: %v", err)
	}
	if len(cards) != len(strategies.Names()) {
		t.Fatalf("LoadLibrary returned %d cards, want %d -- the gallery is "+
			"empty or partial in a packaged app", len(cards), len(strategies.Names()))
	}
	for _, c := range cards {
		if userScript(c.File) {
			continue
		}
		if c.Works == "" || c.Fails == "" {
			t.Errorf("%s: embedded card lost its tags (works=%q fails=%q)",
				c.File, c.Works, c.Fails)
		}
	}
}
