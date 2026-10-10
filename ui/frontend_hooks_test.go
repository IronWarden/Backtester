package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The rules of hooks, enforced without a dependency.
//
// This exists because of a real bug that shipped and stayed shipped for a
// week: `benchmarkRows`, a useMemo in ResultsView.tsx, sat below the
// `if (results.length === 0) return` early return. React counts hooks per
// render and ResultsView never unmounts, so a run went six hooks (empty) then
// seven (populated), React refused, and it tore down the whole root -- which
// on this app's dark surfaces is a black window with no message in it.
//
// Nothing we run could see it. `tsc` types a conditional hook happily, `vite
// build` bundles it happily, and the Go suite never mounts a component. The
// tool built for this is eslint-plugin-react-hooks; adopting it is T90 and
// needs a dependency decision. Until then this catches the same class from the
// Go suite, which already runs on every change.
//
// It is deliberately conservative. A gate that cries wolf gets switched off,
// so this flags only two shapes, both of which are unambiguously wrong:
//
//   1. a hook called at anything other than the top level of its function --
//      inside an if, a loop, a callback;
//   2. a hook called after a `return` at the top level of its function.
//
// Both are "this hook may not run on every render", which is the entire bug.

var (
	// A hook call: use<Uppercase>( ... ). Matches useState, useMemo, and any
	// custom hook, which is the naming convention React itself relies on.
	hookCall = regexp.MustCompile(`^use[A-Z]\w*\s*\(`)
	// A function that may contain hooks: a component (Uppercase) or a custom
	// hook (useSomething), declared at the top level of the module. Anything
	// else calling a hook is already a violation the compiler cannot see, but
	// none exists here and flagging it needs a different message.
	funcDecl = regexp.MustCompile(`^(?:export\s+default\s+|export\s+)?function\s+([A-Z]\w*|use[A-Z]\w*)\s*\(`)
	// `return` as a statement, not `returned` or `.return`.
	returnStmt = regexp.MustCompile(`^return\b`)
)

type hookViolation struct {
	file, fn, reason string
	line             int
}

func (v hookViolation) String() string {
	return fmt.Sprintf("%s:%d  %s() -- %s", v.file, v.line, v.fn, v.reason)
}

// stripNoise blanks out comments and string/template literals in place,
// preserving line structure and byte offsets so reported line numbers stay
// true. Braces inside a comment or a string must not move the nesting depth,
// and a `//` in a URL must not eat the rest of the line.
func stripNoise(src string) string {
	out := []byte(src)
	blank := func(i int) {
		if out[i] != '\n' {
			out[i] = ' '
		}
	}
	for i := 0; i < len(src); i++ {
		switch {
		case strings.HasPrefix(src[i:], "//"):
			for ; i < len(src) && src[i] != '\n'; i++ {
				blank(i)
			}
		case strings.HasPrefix(src[i:], "/*"):
			for ; i < len(src) && !strings.HasPrefix(src[i:], "*/"); i++ {
				blank(i)
			}
			for j := 0; j < 2 && i < len(src); j, i = j+1, i+1 {
				blank(i)
			}
			i--
		case src[i] == '"' || src[i] == '\'' || src[i] == '`':
			quote := src[i]
			blank(i)
			for i++; i < len(src); i++ {
				if src[i] == '\\' {
					blank(i)
					if i+1 < len(src) {
						i++
						blank(i)
					}
					continue
				}
				if src[i] == quote {
					blank(i)
					break
				}
				blank(i)
			}
		}
	}
	return string(out)
}

// scanHooks walks one module and reports every hook call that is not
// unconditionally reached on every render of its function.
//
// The one distinction that makes this work: a `{` either opens a nested
// FUNCTION (a callback) or a nested BLOCK (an if, a loop). `return` inside a
// callback is ordinary control flow -- every useEffect in this app has one --
// while `return` inside a block, or at the top, is an early return that some
// renders take and others do not. Only the second kind makes a later hook
// conditional.
func scanHooks(path, src string) []hookViolation {
	clean := stripNoise(src)
	lines := strings.Split(clean, "\n")

	// Line numbers by walking, not by re-counting newlines at every offset.
	lineOf := make([]int, len(clean)+1)
	ln := 1
	for i := 0; i < len(clean); i++ {
		lineOf[i] = ln
		if clean[i] == '\n' {
			ln++
		}
	}
	lineOf[len(clean)] = ln

	var out []hookViolation
	off := 0
	for _, line := range lines {
		lineStart := off
		off += len(line) + 1
		m := funcDecl.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		name := m[1]
		open := findBodyOpen(clean, lineStart)
		if open < 0 {
			continue
		}

		// frames[k] is true when brace k opens a function body. frames[0] is
		// the component itself.
		frames := []bool{true}
		earlyReturn := 0 // line of the first early return seen, 0 = none

		inCallback := func() bool {
			for _, isFn := range frames[1:] {
				if isFn {
					return true
				}
			}
			return false
		}

		for p := open; p < len(clean) && len(frames) > 0; p++ {
			switch clean[p] {
			case '{':
				if p != open {
					frames = append(frames, opensFunction(clean[:p]))
				}
				continue
			case '}':
				frames = frames[:len(frames)-1]
				continue
			}
			if !isWordStart(clean, p) {
				continue
			}
			switch {
			case matchAt(clean, p, returnStmt) > 0:
				// A return that some renders take and others do not. Inside a
				// callback it says nothing about the render's hook sequence.
				if earlyReturn == 0 && !inCallback() {
					earlyReturn = lineOf[p]
				}
			case matchAt(clean, p, hookCall) > 0:
				switch {
				case len(frames) > 1:
					out = append(out, hookViolation{path, name,
						"hook is nested inside a block or callback; it must be called " +
							"at the top level of the function so it runs on every render",
						lineOf[p]})
				case earlyReturn > 0:
					out = append(out, hookViolation{path, name, fmt.Sprintf(
						"hook is below the early return on line %d, so it is skipped on "+
							"renders that take that return and appears on the ones that do "+
							"not -- React unmounts the tree with \"rendered more hooks than "+
							"during the previous render\". Move it above the return",
						earlyReturn), lineOf[p]})
				}
			}
		}
	}
	return out
}

// findBodyOpen returns the offset of the `{` that opens a function body
// declared at from. It cannot simply be the next `{`: every component here
// destructures its props, so `function Results({ items }: Props)` puts a brace
// inside the parameter list. Match the parameter parens first, then take the
// brace after them, stepping over any `: ReturnType` annotation.
func findBodyOpen(s string, from int) int {
	i := strings.IndexByte(s[from:], '(')
	if i < 0 {
		return -1
	}
	i += from
	depth := 0
	for ; i < len(s); i++ {
		if s[i] == '(' {
			depth++
		} else if s[i] == ')' {
			depth--
			if depth == 0 {
				break
			}
		}
	}
	if i >= len(s) {
		return -1
	}
	brace := strings.IndexByte(s[i:], '{')
	if brace < 0 {
		return -1
	}
	return i + brace
}

// opensFunction decides whether the `{` at the end of head begins a function
// body rather than a plain block, by looking at what precedes it: `=>` for an
// arrow, or a `)` closing a parameter list whose `function` keyword is on the
// same line. Everything else -- if, else, for, while, try, switch, an object
// literal -- is a block.
func opensFunction(head string) bool {
	tail := strings.TrimRight(head, " \t\n")
	if strings.HasSuffix(tail, "=>") {
		return true
	}
	if !strings.HasSuffix(tail, ")") {
		return false
	}
	// Walk back to the matching `(`, then look at the token before it.
	depth := 0
	i := len(tail) - 1
	for ; i >= 0; i-- {
		if tail[i] == ')' {
			depth++
		} else if tail[i] == '(' {
			depth--
			if depth == 0 {
				break
			}
		}
	}
	if i < 0 {
		return false
	}
	before := tail[:i]
	if nl := strings.LastIndexByte(before, '\n'); nl >= 0 {
		before = before[nl+1:]
	}
	before = strings.TrimSpace(before)
	return strings.HasSuffix(before, "function") ||
		regexp.MustCompile(`\bfunction\s+\w*$`).MatchString(before)
}

// isWordStart reports whether p begins an identifier rather than continuing
// one, so `usedRows` is not read as a `use` hook and `noreturn` is not a
// `return`.
func isWordStart(s string, p int) bool {
	c := s[p]
	if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_') {
		return false
	}
	if p == 0 {
		return true
	}
	b := s[p-1]
	return !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '$' || b == '.')
}

// matchAt reports the length of re's match if it starts exactly at off.
// The patterns are anchored, so this fails at the first byte and the walk
// stays linear.
func matchAt(s string, off int, re *regexp.Regexp) int {
	loc := re.FindStringIndex(s[off:])
	if loc == nil || loc[0] != 0 {
		return 0
	}
	return loc[1]
}

func frontendSources(t *testing.T) map[string]string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("frontend", "src", "*.tsx"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no frontend components found -- has the tree moved?")
	}
	srcs := make(map[string]string, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		srcs[p] = string(b)
	}
	return srcs
}

func TestNoConditionalHooks(t *testing.T) {
	for path, src := range frontendSources(t) {
		for _, v := range scanHooks(path, src) {
			t.Errorf("%s", v)
		}
	}
}

// The checker has to be able to fail, or it is a green light wired to nothing.
// These are the two shapes it exists to catch, written out literally.
func TestConditionalHookCheckerCatchesBothShapes(t *testing.T) {
	cases := []struct {
		name, src, want string
	}{{
		name: "hook below an early return",
		src: `
export default function Results({ items }: Props) {
  const [w, setW] = useState(0);
  if (items.length === 0) {
    return <div>nothing yet</div>;
  }
  const rows = useMemo(() => items.map(f), [items]);
  return <div>{rows}</div>;
}`,
		want: "below the early return",
	}, {
		name: "hook nested inside a block",
		src: `
export default function Results({ items }: Props) {
  if (items.length > 0) {
    const rows = useMemo(() => items.map(f), [items]);
  }
  return null;
}`,
		want: "nested inside a block",
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := scanHooks("sample.tsx", c.src)
			if len(got) != 1 {
				t.Fatalf("want exactly 1 violation, got %d: %v", len(got), got)
			}
			if !strings.Contains(got[0].reason, c.want) {
				t.Errorf("reason %q does not mention %q", got[0].reason, c.want)
			}
		})
	}
}

// The shapes that are FINE must stay fine, or the gate starts blocking correct
// code and someone deletes it. A `return` inside a hook's own callback is the
// one that matters: every useEffect in this app has one.
func TestConditionalHookCheckerAllowsValidCode(t *testing.T) {
	src := `
export default function Results({ items }: Props) {
  const ref = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(0);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(() => setW(1));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  const chart = useMemo(() => {
    if (items.length === 0) return null;
    return items.map(f);
  }, [items]);
  const rows = useMemo(() => items.filter(g), [items]);
  if (items.length === 0) {
    return <div>nothing yet</div>;
  }
  return <div>{chart}{rows}</div>;
}`
	if got := scanHooks("sample.tsx", src); len(got) != 0 {
		t.Errorf("false positive on valid code: %v", got)
	}
}

// Comments and strings must not be read as code: a brace in either would move
// the nesting depth and a `//` would swallow the rest of the line.
func TestStripNoisePreservesLineNumbers(t *testing.T) {
	src := "const a = 1; // {{{ not a block\nconst b = \"}}}\";\n/* {\n} */\nconst c = 3;\n"
	got := stripNoise(src)
	if strings.Count(got, "\n") != strings.Count(src, "\n") {
		t.Fatalf("line count changed: %q", got)
	}
	if len(got) != len(src) {
		t.Fatalf("byte length changed: %d != %d", len(got), len(src))
	}
	if strings.ContainsAny(got, "{}") {
		t.Errorf("braces survived inside a comment or string: %q", got)
	}
}
