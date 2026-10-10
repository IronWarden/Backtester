package backtest

// User-facing diagnostics, and the line between them and the log.
//
// The engine writes about sixty log lines, and only a handful of them are
// anything a user could act on. The rest are timings, row-scan warnings and
// metric dumps — useful when debugging the engine, noise when you are trying
// to find out why your backtest traded nothing. Filtering that stream after
// the fact does not work: the two kinds do not differ in shape, only in
// intent, and intent is known at the call site and nowhere else.
//
// So the call site declares it. diagf writes to the log exactly as before AND
// offers the message to Diagnostics, which is nil unless something installed a
// collector. That nil default is what keeps this change inert: the CLI and
// every existing test see byte-identical log output and no new behaviour.
//
// This mirrors TransactionLogger in portfolio.go — a package-level sink that
// defaults to discarding, swapped out by whoever wants the records.

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
)

// Diagnostic is one thing that went wrong in a way the user can act on.
type Diagnostic struct {
	// Level is "info" when a strategy script called log() and is simply
	// talking, "warn" when the run continued with less than it wanted, and
	// "error" when something the user wrote did not work at all.
	Level string `json:"level"`
	// Portfolio is the portfolio the diagnostic belongs to, or "" when it
	// belongs to the run as a whole. Message keeps its own `portfolio %q:`
	// prefix rather than being trimmed to suit this field — the log line must
	// stay exactly what it was, and a reader of one message in isolation
	// should still know which portfolio it came from.
	Portfolio string `json:"portfolio"`
	Message   string `json:"message"`
	// Script and Line are where in a Lua script this happened, parsed out of
	// the message when it carries a position. Zero for everything else, which
	// is most diagnostics -- a config warning belongs to no line of anything.
	//
	// They exist so the console row can be clicked: the message already said
	// "sma_cross.lua:41:", and the reader still had to find line 41 by hand.
	Script string `json:"script"`
	Line   int    `json:"line"`
}

// luaPositionRe finds a Lua error's position: the interpreter writes
// "path/to/script.lua:41:" at the front of what it raises, and the same string
// appears mid-message when the engine wraps it ("lua strategy %q step day=3:
// sma_cross.lua:41: attempt to ...").
//
// It insists on the .lua suffix. Without that it would match a bare "12:30:"
// in some future message and send the editor to line 30 of nothing.
var luaPositionRe = regexp.MustCompile(`([^\s"'()]+\.lua):(\d+):`)

// luaParsePositionRe is the OTHER format, and the one that matters most: a
// compile error is written "mine.lua line:3(column:3) near 'end': syntax
// error", which shares no punctuation with a raised error. Missing it would
// mean the one finding whose line is certain -- the parser's -- is the one
// nothing could jump to.
var luaParsePositionRe = regexp.MustCompile(`([^\s"'()]+\.lua) line:(\d+)`)

// ScriptPosition extracts the script and line a message points at. The FIRST
// position wins: gopher-lua appends a stack traceback whose frames carry the
// same file at other lines, and the first is where it actually went wrong.
func ScriptPosition(message string) (script string, line int, ok bool) {
	for _, re := range []*regexp.Regexp{luaPositionRe, luaParsePositionRe} {
		m := re.FindStringSubmatch(message)
		if m == nil {
			continue
		}
		n, err := strconv.Atoi(m[2])
		if err != nil || n <= 0 {
			continue
		}
		return m[1], n, true
	}
	return "", 0, false
}

// Diagnostics receives user-facing diagnostics. Nil by default, so nothing is
// collected unless a host asks for it.
//
// It is called from the worker pool, concurrently, from as many goroutines as
// there are portfolios in flight. **The installed callback owns its own
// locking** — the engine deliberately does not lock around this, so that a
// host wanting to fan out to several sinks is not paying for a mutex it does
// not need. Install and restore it around a run, not during one.
var Diagnostics func(Diagnostic)

// diagf records a user-facing diagnostic. The message goes to the log whatever
// happens, so `-debug` loses nothing by this being routed.
func diagf(level, portfolio, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	log.Print(msg)
	if sink := Diagnostics; sink != nil {
		d := Diagnostic{Level: level, Portfolio: portfolio, Message: msg}
		// Parsed once, here, rather than by each host that displays a
		// diagnostic: the position is a property of the message, and a second
		// parser in the frontend would be a second thing to get wrong.
		d.Script, d.Line, _ = ScriptPosition(msg)
		sink(d)
	}
}
