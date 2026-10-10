package backtest

// A strategy script says which reported figures it reads in its own code:
//
//	fundamentals = {
//	    source  = "sec",
//	    metrics = { "Net Income", "Stockholders Equity" },
//	}
//
// The figures are fetched once, before the run starts and before any Lua has
// executed, so the declaration is READ, not run: the script is parsed and the
// top-level assignment is taken from the syntax tree. That is why it has to be
// a plain table of literals — a computed metric name could only be known by
// running the script, which happens too late.
//
// It used to be a comment (`-- @fundamentals: ...`), and a comment is the
// first thing a user tidying a copy of a script deletes. Deleting it turned the
// strategy off without a word: nothing was loaded and every fundamental() call
// returned nil. As code it reads as code, and a malformed one is an error that
// names its line rather than a silent nil.

import (
	"bytes"
	"fmt"

	"github.com/yuin/gopher-lua/ast"
	"github.com/yuin/gopher-lua/parse"
)

// ScriptFundamentals returns the source ("sec", or "" for the default
// `financials` table) and metrics a script's top-level `fundamentals` table
// declares. All empty, with no error, when the script declares nothing — or
// does not parse, which the Lua compiler reports on its own with a better
// message than this could.
//
// An error means the declaration is there but cannot be read: not a table,
// a computed value, an unknown field, or assigned twice. It is prefixed
// "name:line:" like a Lua error, so the editor can jump to it.
func ScriptFundamentals(name string, source []byte) (string, []string, error) {
	chunk, err := parse.Parse(bytes.NewReader(source), name)
	if err != nil {
		return "", nil, nil
	}
	var found *ast.AssignStmt
	for _, stmt := range chunk {
		assign, ok := stmt.(*ast.AssignStmt)
		if !ok {
			continue
		}
		for _, lhs := range assign.Lhs {
			if id, ok := lhs.(*ast.IdentExpr); ok && id.Value == "fundamentals" {
				if found != nil {
					return "", nil, fmt.Errorf("%s:%d: fundamentals is assigned "+
						"twice (first on line %d); declare it once",
						name, assign.Line(), found.Line())
				}
				if len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
					return "", nil, fmt.Errorf("%s:%d: assign fundamentals on "+
						"its own line: fundamentals = { ... }", name, assign.Line())
				}
				found = assign
			}
		}
	}
	if found == nil {
		return "", nil, nil
	}
	return readFundamentalsTable(name, found)
}

func readFundamentalsTable(name string, assign *ast.AssignStmt) (string, []string, error) {
	fail := func(line int, format string, args ...any) (string, []string, error) {
		return "", nil, fmt.Errorf("%s:%d: %s", name, line, fmt.Sprintf(format, args...))
	}
	const shape = `fundamentals = { source = "sec", metrics = { "Net Income", ... } }`

	table, ok := assign.Rhs[0].(*ast.TableExpr)
	if !ok {
		return fail(assign.Line(), "fundamentals must be a table, read before "+
			"the script runs: %s", shape)
	}
	var source string
	var metrics []string
	for _, field := range table.Fields {
		key, ok := field.Key.(*ast.StringExpr)
		if !ok {
			return fail(field.Value.Line(), "every entry in fundamentals needs "+
				"a name, source or metrics: %s", shape)
		}
		switch key.Value {
		case "source":
			s, ok := field.Value.(*ast.StringExpr)
			if !ok {
				return fail(field.Value.Line(), `fundamentals.source must be a `+
					`quoted string such as "sec"`)
			}
			source = s.Value
		case "metrics":
			list, ok := field.Value.(*ast.TableExpr)
			if !ok {
				return fail(field.Value.Line(), "fundamentals.metrics must be a "+
					`list of quoted names: { "Net Income", "Total Assets" }`)
			}
			for _, item := range list.Fields {
				s, ok := item.Value.(*ast.StringExpr)
				if item.Key != nil || !ok {
					return fail(item.Value.Line(), "fundamentals.metrics must "+
						"list quoted metric names and nothing else — they are "+
						"read before the script runs, so a computed name "+
						"cannot be known")
				}
				metrics = append(metrics, s.Value)
			}
		default:
			return fail(field.Value.Line(), "fundamentals has no field %q; "+
				"it takes source and metrics", key.Value)
		}
	}
	return source, metrics, nil
}
