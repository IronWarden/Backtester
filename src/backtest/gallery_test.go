package backtest

// The gallery's value is that it cannot go stale silently: a strategy added
// without its metadata fails this test rather than joining the library
// undocumented. That enforcement is the feature — the prose is just the payload.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every shipped strategy must carry all four tags. @fails is the one that
// earns its place: every strategy works somewhere, and the useful question is
// where it does not.
func TestEveryShippedStrategyDocumentsItself(t *testing.T) {
	cards, err := LoadGallery(filepath.Join("..", "..", "strategies"))
	if err != nil {
		t.Fatalf("LoadGallery: %v", err)
	}
	if len(cards) < 11 {
		t.Fatalf("found %d strategies, want at least the 11 shipped ones",
			len(cards))
	}

	for _, c := range cards {
		if !c.Complete() {
			t.Errorf("%s is missing %v — a strategy without its failure mode "+
				"is one a user discovers the expensive way", c.File, c.Missing())
		}
	}
}

// The sweep line must name parameters the script actually reads, or it sends a
// user to search a knob that does nothing. Checked by looking for each name in
// the source itself.
func TestSweepRangesNameRealParameters(t *testing.T) {
	dir := filepath.Join("..", "..", "strategies")
	cards, err := LoadGallery(dir)
	if err != nil {
		t.Fatalf("LoadGallery: %v", err)
	}

	for _, c := range cards {
		// Some strategies have nothing numeric to sweep and say so in prose
		// rather than naming parameters.
		if !strings.Contains(c.Sweep, "=") {
			continue
		}
		source := readStrategySource(t, filepath.Join(dir, c.File))
		// Only the names on the left of an "=", so the numbers inside a range
		// list are not mistaken for parameters. Splitting on commas would
		// treat "[10, 20, 50]" as three parameter names.
		for _, m := range sweepParam.FindAllStringSubmatch(c.Sweep, -1) {
			name := m[1]
			if !strings.Contains(source, "params."+name) {
				t.Errorf("%s: @sweep names %q, which the script never reads",
					c.File, name)
			}
		}
	}
}

// sweepParam matches the "name =" of each range in a @sweep line.
var sweepParam = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*=`)

func readStrategySource(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(raw)
}

// The parser is the enforcement mechanism, so its own behaviour is pinned
// against hand-written headers rather than against the shipped files.
func TestParseStrategyCard(t *testing.T) {
	card := ParseStrategyCard("example.lua", `-- example.lua
-- Buys things. Sells them later.
--
-- Params:
--   period  how long                 (default 20)
--
-- @works: trends persist
-- @fails: chop, where every crossing
--   is a round trip
-- @sweep: period = [10, 20, 50]
-- @baseline: buy_and_hold.lua

local period = params.period or 20
-- @works: this line is code, not header
`)

	if !card.Complete() {
		t.Fatalf("card is incomplete: missing %v", card.Missing())
	}
	if !strings.HasPrefix(card.Idea, "Buys things") {
		t.Errorf("idea = %q, want the prose line after the filename", card.Idea)
	}
	if card.Works != "trends persist" {
		t.Errorf("works = %q", card.Works)
	}
	// The continuation line must be joined, not dropped.
	if card.Fails != "chop, where every crossing is a round trip" {
		t.Errorf("fails = %q — a wrapped tag lost its continuation", card.Fails)
	}
	if card.Sweep != "period = [10, 20, 50]" {
		t.Errorf("sweep = %q", card.Sweep)
	}
	// Parsing must stop at the first line of code: a tag in the body is not
	// documentation, and treating it as such would let a comment inside a
	// function silently redefine the card.
	if strings.Contains(card.Works, "code, not header") {
		t.Error("parsing continued past the first line of code")
	}
}

// A script with no tags reports every one as missing, so the failure message
// tells an author what to write.
func TestMissingNamesEveryAbsentField(t *testing.T) {
	card := ParseStrategyCard("bare.lua", "-- bare.lua\n\nlocal x = 1\n")
	missing := card.Missing()
	if len(missing) != 5 {
		t.Errorf("missing = %v, want all five fields", missing)
	}
	if card.Complete() {
		t.Error("an undocumented script reported itself complete")
	}
}

// The idea is the first paragraph, not the whole header. Every shipped script
// continues into a Params list and often an implementation note, and a gallery
// entry that reprints all of it is unreadable — which is what a listing is for.
func TestIdeaStopsAtTheFirstParagraph(t *testing.T) {
	card := ParseStrategyCard("example.lua", `-- example.lua
-- The idea, which is one sentence.
-- It may wrap onto a second line.
--
-- Params:
--   period  how long  (default 20)
--
-- @works: trends persist
-- @fails: chop
-- @sweep: period = [10, 20]
-- @baseline: buy_and_hold.lua
`)

	want := "The idea, which is one sentence. It may wrap onto a second line."
	if card.Idea != want {
		t.Errorf("idea = %q, want %q", card.Idea, want)
	}
	if strings.Contains(card.Idea, "Params") {
		t.Error("the idea swallowed the Params block")
	}
}

// And the shipped gallery must stay readable: an entry whose idea runs to a
// paragraph is a listing nobody reads.
func TestShippedIdeasAreShort(t *testing.T) {
	cards, err := LoadGallery(filepath.Join("..", "..", "strategies"))
	if err != nil {
		t.Fatalf("LoadGallery: %v", err)
	}
	for _, c := range cards {
		if len(c.Idea) > 320 {
			t.Errorf("%s: idea is %d chars — the first paragraph should be a "+
				"summary, not the whole header", c.File, len(c.Idea))
		}
		if c.Idea == "" {
			t.Errorf("%s: no idea line", c.File)
		}
	}
}

// @fails is required, not optional. It is the field the whole gallery exists
// for — every strategy works somewhere, and a library that only says what each
// one is good at is the kind users learn from expensively.
func TestFailsIsRequired(t *testing.T) {
	card := ParseStrategyCard("example.lua", `-- example.lua
-- An idea.
--
-- @works: trends persist
-- @sweep: period = [10, 20]
-- @baseline: buy_and_hold.lua
`)
	if card.Complete() {
		t.Error("a card with no @fails reported itself complete")
	}
	missing := card.Missing()
	if len(missing) != 1 || missing[0] != "@fails" {
		t.Errorf("missing = %v, want exactly [@fails]", missing)
	}
}
