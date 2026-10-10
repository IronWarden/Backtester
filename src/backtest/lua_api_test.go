package backtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// registeredGlobals collects the names the package hands to Lua, from the
// package's own source. Reading the source is the only mechanical way to ask
// this question -- the registrations happen across five files, inside
// functions that need a live lua.LState -- and a hand-copied list is exactly
// what this test exists to prevent.
//
// Parsed rather than grepped: the first version of this matched
// `SetGlobal("name"` inside a doc comment in lua_api.go and reported a global
// called "name". An AST walk sees calls and only calls.
func registeredGlobals(t *testing.T) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		// Test files register their own fixtures ("data", "prevLong") that no
		// strategy can rely on.
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parsing the package: %v", err)
	}

	names := map[string]bool{}
	for _, pkg := range pkgs {
		ast.Inspect(pkg, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "SetGlobal" {
				return true
			}
			lit, ok := call.Args[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			if s, err := strconv.Unquote(lit.Value); err == nil {
				names[s] = true
			}
			return true
		})
	}
	if len(names) == 0 {
		t.Fatal("found no SetGlobal calls at all; the scan is broken, not the API")
	}
	return names
}

// The editor's knowledge of the engine and the engine itself must be the same
// set, in both directions. A global added without an entry ships an editor
// that cannot complete it; an entry without a global completes a name that
// does not exist, which is worse -- the user writes it, and finds out on day 0
// of a run.
func TestLuaAPICoversEveryGlobal(t *testing.T) {
	registered := registeredGlobals(t)
	documented := map[string]bool{}
	for _, g := range LuaAPI() {
		if documented[g.Name] {
			t.Errorf("%s is listed twice in LuaAPI", g.Name)
		}
		documented[g.Name] = true
	}

	for name := range registered {
		if !documented[name] {
			t.Errorf("the engine registers %q but LuaAPI does not describe it: "+
				"the editor will not complete it and the assistant will not know "+
				"it exists", name)
		}
	}
	for name := range documented {
		if !registered[name] {
			t.Errorf("LuaAPI describes %q but no SetGlobal registers it: the "+
				"editor is offering a global that does not exist", name)
		}
	}
}

// Every entry has to be usable by an editor: a signature to complete, a
// summary to hover, and a group to file it under.
func TestLuaAPIEntriesAreComplete(t *testing.T) {
	groups := map[string]bool{
		LuaGroupContext: true, LuaGroupPrices: true, LuaGroupIndicators: true,
		LuaGroupFundamentals: true, LuaGroupAccount: true,
	}
	for _, g := range LuaAPI() {
		if g.Name == "" || g.Signature == "" || g.Summary == "" {
			t.Errorf("incomplete entry: %+v", g)
			continue
		}
		if !groups[g.Group] {
			t.Errorf("%s: unknown group %q", g.Name, g.Group)
		}
		switch g.Kind {
		case "function":
			// The signature is what an editor turns into a snippet, so it has
			// to start with the name and carry its parentheses.
			if !strings.HasPrefix(g.Signature, g.Name+"(") ||
				!strings.HasSuffix(g.Signature, ")") {
				t.Errorf("%s: signature %q is not a call of that name",
					g.Name, g.Signature)
			}
		case "value":
			if g.Signature != g.Name {
				t.Errorf("%s: a value's signature is just its name, got %q",
					g.Name, g.Signature)
			}
			if g.Returns != "" {
				t.Errorf("%s: a value has no return type, got %q", g.Name, g.Returns)
			}
		default:
			t.Errorf("%s: kind %q is neither function nor value", g.Name, g.Kind)
		}
	}
}

// LuaAPI hands its slice across a package boundary; a shared backing array
// would let a caller's sort reorder everyone else's copy.
func TestLuaAPIReturnsACopy(t *testing.T) {
	first := LuaAPI()
	if len(first) == 0 {
		t.Fatal("LuaAPI is empty")
	}
	first[0] = LuaGlobal{Name: "clobbered"}
	if LuaAPI()[0].Name == "clobbered" {
		t.Error("LuaAPI shares its backing array with the caller")
	}
}
