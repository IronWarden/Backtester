package main

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The design system, enforced.
//
// `App.css` opens by stating two rules: the whole UI reads from the tokens, and
// sizes are in em so the app-wide Ctrl+scroll zoom scales everything. Both were
// true of most of the file and false of one region, and nothing noticed —
// stylesheets have no compiler, so a rule a file states about itself is worth
// exactly as much as a comment claiming a function is thread-safe.
//
// What had drifted, measured before the fix: 16 `font-size` declarations in
// hard px (so the verdict strip was the one part of the app that did not
// respond to zoom), three tokens referenced but never defined — `--muted`,
// `--surface-2`, `--line` — each falling back to a hardcoded hex, so a whole
// region painted from a shadow palette no theme change could reach, and a
// second set of green/amber/red sitting inches from the first.

var (
	cssVarUse    = regexp.MustCompile(`var\(\s*(--[a-zA-Z0-9-]+)`)
	cssVarDefine = regexp.MustCompile(`(?m)^\s*(--[a-zA-Z0-9-]+)\s*:`)
	// Deliberately NOT anchored to the start of a line. The region this check
	// exists to catch was written with several declarations per line
	// (`.prereg { margin: 8px 0 0; font-size: 12px; }`), so an anchored
	// pattern would have missed the very thing it was built for — which the
	// self-test below is what caught.
	cssFontPx  = regexp.MustCompile(`font-size:\s*[0-9.]+px`)
	cssComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
)

func appCSS(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("frontend", "src", "App.css"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// A token that is referenced but never defined is not a token. It silently
// resolves to its fallback, which means that corner of the UI is painted from
// a literal no theme change can reach — and a reader has no way to tell the
// difference from a real one.
func TestEveryCSSTokenIsDefined(t *testing.T) {
	css := appCSS(t)

	defined := map[string]bool{}
	for _, m := range cssVarDefine.FindAllStringSubmatch(css, -1) {
		defined[m[1]] = true
	}
	if len(defined) == 0 {
		t.Fatal("no custom properties found — has App.css moved?")
	}

	missing := map[string]bool{}
	for _, m := range cssVarUse.FindAllStringSubmatch(css, -1) {
		if !defined[m[1]] {
			missing[m[1]] = true
		}
	}
	names := make([]string, 0, len(missing))
	for n := range missing {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		t.Errorf("%s is used but never defined, so every use falls back to a "+
			"literal and that part of the UI is outside the theme", n)
	}
}

// The zoom is implemented by setting font-size in px on the .app root and
// sizing everything else in em. A px font-size anywhere below that root is a
// element that does not zoom — which is not a style preference, it is a
// feature quietly not working on part of the screen.
func TestNoPixelFontSizes(t *testing.T) {
	// Comments blanked first, preserving newlines, so prose describing the
	// old px values does not read as the old px values.
	css := cssComment.ReplaceAllStringFunc(appCSS(t), func(c string) string {
		return strings.Repeat("\n", strings.Count(c, "\n"))
	})
	for i, line := range strings.Split(css, "\n") {
		if cssFontPx.MatchString(line) {
			t.Errorf("App.css:%d: %s — px font sizes do not respond to the "+
				"app's Ctrl+scroll zoom, which is applied as a font-size on "+
				".app and inherited in em. Use one of the --fs-* steps.",
				i+1, strings.TrimSpace(line))
		}
	}
}

// The checker has to be able to fail, or it is a green light wired to nothing.
func TestDesignTokenChecksCatchTheirOwnFailures(t *testing.T) {
	t.Run("undefined token", func(t *testing.T) {
		css := ":root { --a: 1px; }\n.x { color: var(--nope, #fff); }"
		defined := map[string]bool{}
		for _, m := range cssVarDefine.FindAllStringSubmatch(css, -1) {
			defined[m[1]] = true
		}
		found := false
		for _, m := range cssVarUse.FindAllStringSubmatch(css, -1) {
			if !defined[m[1]] {
				found = true
			}
		}
		if !found {
			t.Error("an undefined token was not detected")
		}
	})

	t.Run("px font size", func(t *testing.T) {
		if !cssFontPx.MatchString("  font-size: 11px;") {
			t.Error("a px font-size was not detected")
		}
		// The shape that actually shipped: several declarations on one line.
		// An anchored pattern passes this file and misses that, which is how
		// the first version of this check was useless.
		if !cssFontPx.MatchString(".prereg { margin: 8px 0 0; font-size: 12px; }") {
			t.Error("a px font-size inside a single-line rule was not detected")
		}
		if cssFontPx.MatchString("  font-size: 0.85em;") {
			t.Error("an em font-size was flagged")
		}
		// Not every px is wrong: drag handles and layout floors are fixed
		// affordances and must stay px. Only font-size is zoom-critical.
		if cssFontPx.MatchString("  min-width: 200px;") {
			t.Error("a non-font px value was flagged")
		}
	})
}
