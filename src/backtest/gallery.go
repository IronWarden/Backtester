package backtest

// The strategy gallery: what each shipped script is for, and how it fails.
//
// `strategies/` is a good library and was a poor starting point. A user opening
// it had to read eleven scripts to learn what each does, guess which parameters
// were worth varying, and had no way to know what each one is bad at — which is
// the part that decides whether it suits their universe. The sweep machinery was
// invisible unless you already knew it existed.
//
// So every shipped strategy now carries four tagged lines in its header:
//
//	-- @works:    the market condition it needs
//	-- @fails:    how it is known to lose money
//	-- @sweep:    parameter ranges worth searching, ready to paste
//	-- @baseline: what to compare it against
//
// Prose in a comment goes stale silently, so this parses those tags and a test
// requires all four on every script. A strategy added without them fails the
// build rather than quietly joining the library undocumented.
//
// @fails is the one that earns its place. Every strategy here works somewhere;
// the useful question is where it does not, and that is exactly what a library
// of examples usually omits.

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// StrategyCard is one gallery entry, parsed from a script's header.
type StrategyCard struct {
	// File is the script's base name, as a config's Strategy field would name
	// it: "lua:strategies/<File>".
	File string
	// Idea is the first prose line of the header — what the strategy does.
	Idea string
	// Works, Fails, Sweep and Baseline come from the tagged lines.
	Works    string
	Fails    string
	Sweep    string
	Baseline string
}

// Complete reports whether the card has everything a gallery entry needs. The
// idea is not enough: a strategy whose failure mode is undocumented is one a
// user will discover the expensive way.
func (c StrategyCard) Complete() bool {
	return c.Idea != "" && c.Works != "" && c.Fails != "" &&
		c.Sweep != "" && c.Baseline != ""
}

// Missing names the absent fields, so a failing test says what to write rather
// than only that something is wrong.
func (c StrategyCard) Missing() []string {
	var missing []string
	for _, f := range []struct {
		name  string
		value string
	}{
		{"idea", c.Idea}, {"@works", c.Works}, {"@fails", c.Fails},
		{"@sweep", c.Sweep}, {"@baseline", c.Baseline},
	} {
		if f.value == "" {
			missing = append(missing, f.name)
		}
	}
	return missing
}

// String renders one gallery entry for the CLI listing.
func (c StrategyCard) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "lua:strategies/%s\n", c.File)
	fmt.Fprintf(&b, "  %s\n", c.Idea)
	fmt.Fprintf(&b, "  works when : %s\n", c.Works)
	fmt.Fprintf(&b, "  fails when : %s\n", c.Fails)
	fmt.Fprintf(&b, "  sweep      : %s\n", c.Sweep)
	fmt.Fprintf(&b, "  compare to : %s\n", c.Baseline)
	return b.String()
}

// ParseStrategyCard reads the header of a Lua script. Pure, so the parsing is
// testable without files.
//
// The idea is the first run of prose comment lines after the filename line;
// tagged lines are "-- @tag: value" and may wrap onto continuation lines that
// are indented under them. Parsing stops at the first non-comment line, so
// nothing in the body can be mistaken for documentation.
func ParseStrategyCard(file, source string) StrategyCard {
	card := StrategyCard{File: file}

	var ideaLines []string
	var current *string
	// The idea is the FIRST paragraph only. These headers continue into a
	// Params list and often a note on implementation, none of which belongs in
	// a one-line gallery entry — so collection stops at the first bare "--",
	// which is the paragraph break every one of them already uses.
	ideaDone := false
	for _, raw := range strings.Split(source, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "--") {
			break // the header ends at the first line of code
		}
		body := strings.TrimSpace(strings.TrimPrefix(line, "--"))

		// The filename line is a label, not prose.
		if strings.HasSuffix(body, ".lua") && len(ideaLines) == 0 {
			continue
		}

		if tag, value, ok := parseTag(body); ok {
			switch tag {
			case "works":
				card.Works = value
				current = &card.Works
			case "fails":
				card.Fails = value
				current = &card.Fails
			case "sweep":
				card.Sweep = value
				current = &card.Sweep
			case "baseline":
				card.Baseline = value
				current = &card.Baseline
			default:
				current = nil
			}
			continue
		}

		// A continuation of the tag above, if there is one.
		if current != nil {
			if body == "" {
				current = nil
				continue
			}
			*current += " " + body
			continue
		}
		if body == "" {
			if len(ideaLines) > 0 {
				ideaDone = true
			}
			continue
		}
		if !ideaDone {
			ideaLines = append(ideaLines, body)
		}
	}

	card.Idea = strings.Join(ideaLines, " ")
	return card
}

// parseTag splits "@name: value".
func parseTag(body string) (tag, value string, ok bool) {
	if !strings.HasPrefix(body, "@") {
		return "", "", false
	}
	colon := strings.Index(body, ":")
	if colon < 0 {
		return "", "", false
	}
	return strings.TrimSpace(body[1:colon]),
		strings.TrimSpace(body[colon+1:]), true
}

// LoadGallery parses every .lua in a directory, sorted by file name.
func LoadGallery(dir string) ([]StrategyCard, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.lua"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)

	cards := make([]StrategyCard, 0, len(paths))
	for _, path := range paths {
		source, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}
		cards = append(cards, ParseStrategyCard(filepath.Base(path), string(source)))
	}
	return cards, nil
}

// GalleryReport renders the whole gallery for `-list-strategies`.
func GalleryReport(cards []StrategyCard) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d strategies in the library.\n\n", len(cards))
	for _, c := range cards {
		b.WriteString(c.String())
		b.WriteString("\n")
	}
	b.WriteString("Point a portfolio at one with Strategy = \"lua:strategies/" +
		"<file>\" and put its\nparameters in [portfolio.Params]. The sweep " +
		"ranges above go in [portfolio.Sweep]\nto search them; -scan-signals " +
		"first will tell you which signals have any\npredictive power on your " +
		"universe before you pick.\n")
	return b.String()
}
