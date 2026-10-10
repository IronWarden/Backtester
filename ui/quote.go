package main

// lookup_quote tool: live quotes & fundamentals for the chat assistant,
// backed by the quote_lookup.py yfinance helper at the repo root. The
// helper prints a JSON contract on stdout; this side validates symbols,
// runs the subprocess with a deadline, and formats the result as compact
// key-value text for the model. Like query_db, every failure is returned
// as "ERROR: ..." tool-result text so the model can self-correct.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

const lookupQuoteToolName = "lookup_quote"

const lookupQuoteToolDescription = "Look up live quote and fundamentals " +
	"from Yahoo Finance for up to 5 stock/ETF symbols: current price, day " +
	"change, market cap, P/E, EPS, dividend yield, beta, 52-week range, " +
	"sector/industry and a one-line business profile. Real symbols only — " +
	"$-prefixed benchmark series ($SP500 etc.) are local synthetics; use " +
	"query_db for those. Needs internet; quotes may be ~15 min delayed."

func lookupQuoteToolSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"symbols": map[string]any{
				"type":     "array",
				"items":    map[string]any{"type": "string"},
				"maxItems": maxQuoteSymbols,
				"description": "Ticker symbols as used on Yahoo Finance, " +
					`e.g. ["AAPL","BRK-B"].`,
			},
		},
		"required": []string{"symbols"},
	}
}

const (
	maxQuoteSymbols    = 5
	quoteLookupTimeout = 45 * time.Second
	quoteHelperScript  = "quote_lookup.py"
)

// Covers ordinary tickers plus class shares (BRK-B, BF.B), indices (^GSPC)
// and FX pairs (EURUSD=X). The first-character rule blocks a leading "-"
// so no symbol can ever be parsed as a flag by the helper.
var quoteSymbolRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.\-=^]{0,11}$`)

// quoteSymbols coerces the tool-call argument (JSON array → []any) into a
// cleaned, uppercased, deduplicated symbol list.
func quoteSymbols(raw []any) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		s, _ := v.(string)
		s = strings.ToUpper(strings.TrimSpace(s))
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// symbolsPreview renders the tool-note chip shown in the chat panel.
func symbolsPreview(raw []any) string {
	return strings.Join(quoteSymbols(raw), ", ")
}

// runQuoteTool executes one lookup_quote call end to end.
func runQuoteTool(ctx context.Context, rawSymbols []any) string {
	symbols := quoteSymbols(rawSymbols)
	if len(symbols) == 0 {
		return "ERROR: no symbols given."
	}
	truncNote := ""
	if len(symbols) > maxQuoteSymbols {
		truncNote = fmt.Sprintf(
			"(note: %d symbols requested, only the first %d were looked up)\n\n",
			len(symbols), maxQuoteSymbols)
		symbols = symbols[:maxQuoteSymbols]
	}
	for _, s := range symbols {
		if strings.HasPrefix(s, "$") {
			return fmt.Sprintf("ERROR: %q is a local benchmark series in the "+
				"backtester's database, not a real Yahoo symbol — use query_db "+
				"for its history; there is no live quote.", s)
		}
		if !quoteSymbolRe.MatchString(s) {
			return fmt.Sprintf("ERROR: %q is not a valid ticker symbol.", s)
		}
	}

	py, script, errMsg := locateHelper(quoteHelperScript)
	if errMsg != "" {
		return errMsg
	}

	ctx, cancel := context.WithTimeout(ctx, quoteLookupTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, py, append([]string{script}, symbols...)...)
	cmd.Dir = repoRoot()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Sprintf("ERROR: quote lookup timed out after %s "+
			"(Yahoo Finance slow or network down).", quoteLookupTimeout)
	}
	if runErr != nil {
		msg := strings.TrimSpace(stderr.String())
		if len(msg) > 400 {
			msg = msg[:400] + "…"
		}
		if msg == "" {
			msg = runErr.Error()
		}
		return "ERROR: quote helper failed: " + msg
	}

	var parsed struct {
		Quotes []map[string]any `json:"quotes"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &parsed); err != nil {
		return fmt.Sprintf("ERROR: could not parse helper output: %v", err)
	}
	if len(parsed.Quotes) == 0 {
		return "ERROR: helper returned no quotes."
	}
	return truncNote + formatQuoteResult(parsed.Quotes)
}

// formatQuoteResult renders the helper's JSON as key-value text. Records
// are sparse (ETFs have no P/E or sector), so missing fields are simply
// skipped rather than shown as empty columns.
func formatQuoteResult(quotes []map[string]any) string {
	var b strings.Builder
	for i, q := range quotes {
		if i > 0 {
			b.WriteString("\n\n")
		}
		sym, _ := q["symbol"].(string)
		if errMsg, ok := q["error"].(string); ok {
			fmt.Fprintf(&b, "%s: ERROR: %s", sym, errMsg)
			continue
		}
		b.WriteString(sym)
		if name, ok := q["name"].(string); ok {
			b.WriteString(" — " + name)
		}
		sector, _ := q["sector"].(string)
		industry, _ := q["industry"].(string)
		if sector != "" || industry != "" {
			fmt.Fprintf(&b, " (%s)", joinNonEmpty(" / ", sector, industry))
		}
		b.WriteString("\n")

		line1 := []string{}
		if p, ok := num(q["price"]); ok {
			cur, _ := q["currency"].(string)
			s := fmt.Sprintf("price: %s %s", fmtNum(p), cur)
			if c, ok := num(q["change_pct"]); ok {
				s += fmt.Sprintf(" (%+.2f%% vs prev close)", c)
			}
			line1 = append(line1, strings.TrimSpace(s))
		}
		if mc, ok := num(q["market_cap"]); ok {
			line1 = append(line1, "market cap: "+fmtBig(mc))
		}
		if v, ok := num(q["last_volume"]); ok {
			line1 = append(line1, "volume: "+fmtBig(v))
		}
		writeIndented(&b, line1)

		line2 := []string{}
		lo, okLo := num(q["year_low"])
		hi, okHi := num(q["year_high"])
		if okLo && okHi {
			line2 = append(line2,
				fmt.Sprintf("52w range: %s – %s", fmtNum(lo), fmtNum(hi)))
		}
		if a, ok := num(q["fifty_day_avg"]); ok {
			line2 = append(line2, "50d avg: "+fmtNum(a))
		}
		if a, ok := num(q["two_hundred_day_avg"]); ok {
			line2 = append(line2, "200d avg: "+fmtNum(a))
		}
		writeIndented(&b, line2)

		line3 := []string{}
		if v, ok := num(q["trailing_pe"]); ok {
			line3 = append(line3, fmt.Sprintf("P/E (ttm): %.1f", v))
		}
		if v, ok := num(q["forward_pe"]); ok {
			line3 = append(line3, fmt.Sprintf("fwd P/E: %.1f", v))
		}
		if v, ok := num(q["eps_ttm"]); ok {
			line3 = append(line3, fmt.Sprintf("EPS: %.2f", v))
		}
		if v, ok := num(q["dividend_yield"]); ok {
			line3 = append(line3, fmt.Sprintf("div yield: %.2f%%", v))
		}
		if v, ok := num(q["beta"]); ok {
			line3 = append(line3, fmt.Sprintf("beta: %.2f", v))
		}
		writeIndented(&b, line3)

		if p, ok := q["profile"].(string); ok {
			fmt.Fprintf(&b, "  profile: %s\n", p)
		}
		if n, ok := q["note"].(string); ok {
			fmt.Fprintf(&b, "  note: %s\n", n)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

func writeIndented(b *strings.Builder, parts []string) {
	if len(parts) > 0 {
		fmt.Fprintf(b, "  %s\n", strings.Join(parts, "   "))
	}
}

func joinNonEmpty(sep string, parts ...string) string {
	out := parts[:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, sep)
}

func num(v any) (float64, bool) {
	f, ok := v.(float64)
	return f, ok
}

// fmtNum prints a price-like value with two decimals, dropping them for
// large round numbers is not worth the complexity — 2dp everywhere.
func fmtNum(v float64) string {
	return fmt.Sprintf("%.2f", v)
}

// fmtBig humanizes large magnitudes: 3.19T, 42.1M, 132.28M.
func fmtBig(v float64) string {
	switch {
	case v >= 1e12:
		return fmt.Sprintf("%.2fT", v/1e12)
	case v >= 1e9:
		return fmt.Sprintf("%.2fB", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1fk", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}
