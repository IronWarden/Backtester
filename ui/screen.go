package main

// screen_stocks tool: fundamentals-based screening of the US market via
// Yahoo Finance's screener (screen_stocks.py helper). This is the tool
// for "find/list companies where ..." questions — the local database has
// no fundamentals, so query_db cannot answer them.

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

const screenStocksToolName = "screen_stocks"

const screenStocksToolDescription = "Screen the US stock market by " +
	"fundamentals via Yahoo Finance: filter by trailing P/E, market cap, " +
	"dividend yield, and/or sector, sorted by market cap (default) or " +
	"P/E. Returns the top matches plus the total match count. Use this " +
	"for \"find/list companies where ...\" questions — the local database " +
	"has NO fundamentals data. At least one filter is required. " +
	"Needs internet."

func screenStocksToolSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"pe_max": map[string]any{
				"type":        "number",
				"description": "Only companies with trailing P/E below this.",
			},
			"pe_min": map[string]any{
				"type":        "number",
				"description": "Only companies with trailing P/E above this.",
			},
			"market_cap_min": map[string]any{
				"type":        "number",
				"description": "Market cap floor in USD, e.g. 1e10 for $10B.",
			},
			"dividend_yield_min": map[string]any{
				"type":        "number",
				"description": "Forward dividend yield floor, in percent.",
			},
			"sector": map[string]any{
				"type": "string",
				"description": "Yahoo sector name, e.g. Technology, " +
					"Healthcare, Financial Services, Energy, Utilities.",
			},
			"sort_by": map[string]any{
				"type":        "string",
				"enum":        []string{"market_cap", "pe"},
				"description": "market_cap: largest first (default). pe: cheapest first.",
			},
			"limit": map[string]any{
				"type":        "integer",
				"description": "Max results to return (default 25, max 100).",
			},
		},
	}
}

const (
	screenTimeout      = 60 * time.Second
	screenHelperScript = "screen_stocks.py"
)

var sectorRe = regexp.MustCompile(`^[A-Za-z &-]{1,40}$`)

// screenFilters converts validated tool input into helper argv flags plus
// a human preview for the chat chip. An error string (tool convention)
// is returned for invalid input; empty means OK.
func screenFilters(input map[string]any) (args []string, preview []string, errMsg string) {
	numFlag := func(key, flag, short string) string {
		v, ok := input[key]
		if !ok || v == nil {
			return ""
		}
		f, isNum := v.(float64)
		if !isNum || f < 0 {
			return fmt.Sprintf("ERROR: %s must be a non-negative number.", key)
		}
		args = append(args, flag, fmtFilterNum(f))
		preview = append(preview, short+fmtFilterNum(f))
		return ""
	}
	for _, spec := range [][3]string{
		{"pe_max", "--pe-max", "pe<"},
		{"pe_min", "--pe-min", "pe>"},
		{"market_cap_min", "--cap-min", "cap>"},
		{"dividend_yield_min", "--div-yield-min", "yield>"},
	} {
		if msg := numFlag(spec[0], spec[1], spec[2]); msg != "" {
			return nil, nil, msg
		}
	}
	if sector, ok := input["sector"].(string); ok && sector != "" {
		if !sectorRe.MatchString(sector) {
			return nil, nil, fmt.Sprintf("ERROR: %q is not a valid sector name.", sector)
		}
		args = append(args, "--sector", sector)
		preview = append(preview, "sector="+sector)
	}
	if len(args) == 0 {
		return nil, nil, "ERROR: give at least one filter (pe_max, pe_min, " +
			"market_cap_min, dividend_yield_min, or sector)."
	}
	if sortBy, ok := input["sort_by"].(string); ok && sortBy == "pe" {
		args = append(args, "--sort", "pe")
	}
	if lim, ok := input["limit"].(float64); ok && lim >= 1 {
		if lim > 100 {
			lim = 100
		}
		args = append(args, "--limit", fmt.Sprintf("%d", int(lim)))
	}
	return args, preview, ""
}

// fmtFilterNum prints a filter number compactly (20, 1.5, 1e10 → 10000000000).
func fmtFilterNum(f float64) string {
	if f == float64(int64(f)) {
		return fmt.Sprintf("%d", int64(f))
	}
	return fmt.Sprintf("%g", f)
}

// screenPreview renders the tool-note chip, e.g. "pe<20 cap>10000000000".
func screenPreview(input map[string]any) string {
	_, preview, errMsg := screenFilters(input)
	if errMsg != "" {
		return "invalid filters"
	}
	return strings.Join(preview, " ")
}

// runScreenTool executes one screen_stocks call end to end.
func runScreenTool(ctx context.Context, input map[string]any) string {
	flags, _, errMsg := screenFilters(input)
	if errMsg != "" {
		return errMsg
	}
	py, script, errMsg := locateHelper(screenHelperScript)
	if errMsg != "" {
		return errMsg
	}

	ctx, cancel := context.WithTimeout(ctx, screenTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, py, append([]string{script}, flags...)...)
	cmd.Dir = repoRoot()
	stdout, stderr := &strings.Builder{}, &strings.Builder{}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	runErr := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Sprintf("ERROR: screener timed out after %s "+
			"(Yahoo Finance slow or network down).", screenTimeout)
	}
	if runErr != nil {
		msg := strings.TrimSpace(stderr.String())
		if len(msg) > 400 {
			msg = msg[:400] + "…"
		}
		if msg == "" {
			msg = runErr.Error()
		}
		return "ERROR: screener helper failed: " + msg
	}

	var parsed struct {
		Total  float64          `json:"total"`
		Quotes []map[string]any `json:"quotes"`
	}
	if err := json.Unmarshal([]byte(stdout.String()), &parsed); err != nil {
		return fmt.Sprintf("ERROR: could not parse helper output: %v", err)
	}
	return formatScreenResult(int(parsed.Total), parsed.Quotes)
}

// formatScreenResult renders the match list, one line per company.
func formatScreenResult(total int, quotes []map[string]any) string {
	if len(quotes) == 0 {
		return "0 matches — relax the filters (or the sector name is not " +
			"a Yahoo sector)."
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d total matches; showing %d:\n", total, len(quotes))
	for _, q := range quotes {
		sym, _ := q["symbol"].(string)
		b.WriteString("\n" + sym)
		if name, ok := q["name"].(string); ok {
			b.WriteString(" — " + name)
		}
		if v, ok := num(q["pe"]); ok {
			fmt.Fprintf(&b, " | PE %.1f", v)
		}
		if v, ok := num(q["market_cap"]); ok {
			b.WriteString(" | cap " + fmtBig(v))
		}
		if v, ok := num(q["price"]); ok {
			b.WriteString(" | $" + fmtNum(v))
		}
		if v, ok := num(q["dividend_yield"]); ok {
			fmt.Fprintf(&b, " | yield %.2f%%", v)
		}
		if s, ok := q["sector"].(string); ok {
			b.WriteString(" | " + s)
		}
	}
	if total > len(quotes) {
		fmt.Fprintf(&b, "\n\n(%d more matches — raise limit, tighten "+
			"filters, or sort differently to see others)",
			total-len(quotes))
	}
	return b.String()
}
