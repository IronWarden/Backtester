package main

import (
	"os"
	"path/filepath"
	"testing"
)

// A packaged install may ship without strategies/, and the UI must still
// start. An empty gallery is the degraded state; an error is not.
func TestLoadStrategyGalleryMissingDir(t *testing.T) {
	got := loadStrategyGallery(filepath.Join(t.TempDir(), "nope"))
	if got == nil {
		t.Fatal("missing directory returned nil, want an empty slice — nil " +
			"marshals to JSON null and the frontend cannot map over it")
	}
	if len(got) != 0 {
		t.Errorf("missing directory returned %d cards, want 0", len(got))
	}
}

// The cards are the whole point: a picker shows what a strategy is for and
// how it fails, so parsing has to reach those fields, not just the names.
func TestLoadStrategyGalleryParsesCards(t *testing.T) {
	dir := t.TempDir()
	script := `-- demo.lua
-- Buys the dip and sells the rip.
--
-- @works: a range-bound market
-- @fails: a sustained downtrend
-- @sweep: period = [7, 14]
-- @baseline: buy_and_hold.lua
`
	if err := os.WriteFile(filepath.Join(dir, "demo.lua"),
		[]byte(script), 0644); err != nil {
		t.Fatal(err)
	}
	cards := loadStrategyGallery(dir)
	if len(cards) != 1 {
		t.Fatalf("got %d cards, want 1", len(cards))
	}
	c := cards[0]
	if c.File != "demo.lua" {
		t.Errorf("File = %q", c.File)
	}
	if c.Works == "" || c.Fails == "" {
		t.Errorf("card lost its @works/@fails lines: %+v", c)
	}
	if !c.Complete() {
		t.Errorf("card incomplete, missing %v", c.Missing())
	}
}

// The shipped library is what the binding will actually serve, so assert it
// parses rather than trusting that it does.
func TestShippedGalleryLoads(t *testing.T) {
	cards := loadStrategyGallery(filepath.Join(repoRoot(), "strategies"))
	if len(cards) == 0 {
		t.Skip("no strategies/ directory next to the test")
	}
	for _, c := range cards {
		if userScript(c.File) {
			continue
		}
		if !c.Complete() {
			t.Errorf("%s is missing %v", c.File, c.Missing())
		}
	}
}

// The frontend hardcodes a library script path — BUY_AND_HOLD_LUA_PATH in
// buildToml.ts — as "strategies/<name>", resolved by the engine's
// parent-walk. Such paths used to be satisfied by a second copy in
// ui/strategies/, which SHADOWED the library: the copy of the weighted
// buy-and-hold script there was an older one that floored to whole shares. With that directory gone these must resolve to the library, and
// this test is what says so — the go tests run with cwd=ui, exactly where
// the desktop app runs.
func TestUIDefaultScriptsResolveToTheLibrary(t *testing.T) {
	for _, name := range []string{"buy_and_hold.lua"} {
		path := filepath.Join(repoRoot(), "strategies", name)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("%s is referenced by the frontend but not in the "+
				"library: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join("strategies")); err == nil {
		t.Error("ui/strategies exists again — it shadows the root library " +
			"for any filename it holds, which is the bug T86 removed")
	}
}
