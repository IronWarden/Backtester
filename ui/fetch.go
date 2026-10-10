package main

// fetch_history tool: downloads daily OHLCV history from Yahoo Finance
// into the backtester's database. The fetch_history.py helper only fetches
// and prints JSON — it never opens the DuckDB, because this process holds
// the write lock. The upsert happens here through data.UpsertBars on the
// app's own handle, so the tool works while the UI is running.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"my-backtester/src/data"
)

const fetchHistoryToolName = "fetch_history"

const fetchHistoryToolDescription = "Download daily OHLCV history " +
	"(split/dividend-adjusted) from Yahoo Finance and store it in the " +
	"backtester's database, for up to 3 symbols per call. Omit start for " +
	"the full available history; the write is an idempotent upsert, so " +
	"refreshing a stale ticker is safe. Use when a ticker the user wants " +
	"to backtest is missing or stale — verify with query_db first. " +
	"$-prefixed benchmark series are built locally and cannot be fetched."

func fetchHistoryToolSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"symbols": map[string]any{
				"type":     "array",
				"items":    map[string]any{"type": "string"},
				"maxItems": maxFetchSymbols,
				"description": "Ticker symbols as used on Yahoo Finance, " +
					`e.g. ["TSLA","NVDA"].`,
			},
			"start": map[string]any{
				"type": "string",
				"description": "YYYY-MM-DD. Omit to fetch the full " +
					"available history.",
			},
			"end": map[string]any{
				"type":        "string",
				"description": "YYYY-MM-DD, exclusive. Omit for today.",
			},
		},
		"required": []string{"symbols"},
	}
}

const (
	maxFetchSymbols     = 3
	fetchHistoryTimeout = 120 * time.Second
	fetchHelperScript   = "fetch_history.py"
)

var isoDateRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// locateHelper finds python3 and the named helper script at the repo root.
// errMsg is non-empty (and py/script empty) when either is missing, phrased
// per the tool convention so it can be returned to the model directly.
func locateHelper(name string) (py, script, errMsg string) {
	py, err := exec.LookPath("python3")
	if err != nil {
		return "", "", "ERROR: python3 not found on PATH — this tool " +
			"requires Python 3 with yfinance installed."
	}
	// Absolute path: the argv is resolved by the child process against
	// cmd.Dir, which differs from this process's cwd.
	script, err = filepath.Abs(filepath.Join(repoRoot(), name))
	if err != nil {
		return "", "", fmt.Sprintf("ERROR: resolving helper path: %v", err)
	}
	if _, err := os.Stat(script); err != nil {
		return "", "", fmt.Sprintf("ERROR: %s not found at %s.", name, script)
	}
	return py, script, ""
}

// fetchPreview renders the tool-note chip, e.g. "TSLA, NVDA (full history)".
func fetchPreview(input map[string]any) string {
	raw, _ := input["symbols"].([]any)
	s := symbolsPreview(raw)
	start, _ := input["start"].(string)
	end, _ := input["end"].(string)
	switch {
	case start == "" && end == "":
		return s + " (full history)"
	case end == "":
		return fmt.Sprintf("%s (%s → today)", s, start)
	default:
		return fmt.Sprintf("%s (%s → %s)", s, start, end)
	}
}

// runFetchTool executes one fetch_history call end to end: subprocess
// fetch, in-process upsert, cache invalidation, frontend notification.
func runFetchTool(
	ctx context.Context, dbPath string, input map[string]any, em *chatEmitter,
) string {
	if dbPath == "" {
		return "ERROR: no database chosen — pick a DB first."
	}
	raw, _ := input["symbols"].([]any)
	symbols := quoteSymbols(raw)
	if len(symbols) == 0 {
		return "ERROR: no symbols given."
	}
	truncNote := ""
	if len(symbols) > maxFetchSymbols {
		truncNote = fmt.Sprintf(
			"(note: %d symbols requested, only the first %d were fetched)\n\n",
			len(symbols), maxFetchSymbols)
		symbols = symbols[:maxFetchSymbols]
	}
	for _, s := range symbols {
		if strings.HasPrefix(s, "$") {
			return fmt.Sprintf("ERROR: %q is a local benchmark series built "+
				"by add_collections.py, not a Yahoo symbol — it cannot be "+
				"fetched. Its history is already in the database.", s)
		}
		if !quoteSymbolRe.MatchString(s) {
			return fmt.Sprintf("ERROR: %q is not a valid ticker symbol.", s)
		}
	}
	start, _ := input["start"].(string)
	end, _ := input["end"].(string)
	for _, d := range []string{start, end} {
		if d != "" && !isoDateRe.MatchString(d) {
			return fmt.Sprintf("ERROR: %q is not a YYYY-MM-DD date.", d)
		}
	}

	py, script, errMsg := locateHelper(fetchHelperScript)
	if errMsg != "" {
		return errMsg
	}

	args := append([]string{script}, symbols...)
	if start != "" {
		args = append(args, "--start", start)
	}
	if end != "" {
		args = append(args, "--end", end)
	}

	ctx, cancel := context.WithTimeout(ctx, fetchHistoryTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, py, args...)
	cmd.Dir = repoRoot()
	stdout, stderr := &strings.Builder{}, &strings.Builder{}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	runErr := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Sprintf("ERROR: history fetch timed out after %s "+
			"(Yahoo Finance slow or network down).", fetchHistoryTimeout)
	}
	if runErr != nil {
		msg := strings.TrimSpace(stderr.String())
		if len(msg) > 400 {
			msg = msg[:400] + "…"
		}
		if msg == "" {
			msg = runErr.Error()
		}
		return "ERROR: fetch helper failed: " + msg
	}

	var parsed struct {
		Results []struct {
			Symbol string  `json:"symbol"`
			Error  string  `json:"error"`
			Rows   [][]any `json:"rows"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(stdout.String()), &parsed); err != nil {
		return fmt.Sprintf("ERROR: could not parse helper output: %v", err)
	}
	if len(parsed.Results) == 0 {
		return "ERROR: helper returned no results."
	}

	if _, err := data.InitDB(dbPath); err != nil {
		return fmt.Sprintf("ERROR: open db %q: %v", dbPath, err)
	}

	var b strings.Builder
	b.WriteString(truncNote)
	stored := 0
	for i, res := range parsed.Results {
		if i > 0 {
			b.WriteString("\n")
		}
		if res.Error != "" {
			fmt.Fprintf(&b, "%s: ERROR: %s", res.Symbol, res.Error)
			continue
		}
		bars, err := rowsToBars(res.Rows)
		if err != nil {
			fmt.Fprintf(&b, "%s: ERROR: %v", res.Symbol, err)
			continue
		}
		if err := data.UpsertBars(res.Symbol, bars); err != nil {
			fmt.Fprintf(&b, "%s: ERROR: storing rows: %v", res.Symbol, err)
			continue
		}
		stored++
		fmt.Fprintf(&b, "%s: stored %d rows, %s → %s",
			res.Symbol, len(bars), bars[0].Date, bars[len(bars)-1].Date)
	}

	if stored > 0 {
		// The system prompt's DB overview and the frontend's ticker list
		// are both stale now.
		dbOverviewCache.Delete(dbPath)
		if em != nil {
			em.dbChanged()
		}
		b.WriteString("\n\nThe database now includes this data — it is " +
			"immediately usable in backtests and query_db.")
	}
	return b.String()
}

// rowsToBars converts the helper's compact row arrays
// ([date, open, high, low, close, volume]) into data.Bar values.
func rowsToBars(rows [][]any) ([]data.Bar, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("helper returned no rows")
	}
	bars := make([]data.Bar, 0, len(rows))
	for _, r := range rows {
		if len(r) != 6 {
			return nil, fmt.Errorf("malformed row (%d fields)", len(r))
		}
		date, ok := r[0].(string)
		if !ok || !isoDateRe.MatchString(date) {
			return nil, fmt.Errorf("malformed row date %v", r[0])
		}
		vals := [5]float64{}
		for i := 0; i < 5; i++ {
			f, ok := r[i+1].(float64)
			if !ok {
				return nil, fmt.Errorf("malformed row value %v", r[i+1])
			}
			vals[i] = f
		}
		bars = append(bars, data.Bar{
			Date: date, Open: vals[0], High: vals[1], Low: vals[2],
			Close: vals[3], Volume: vals[4],
		})
	}
	return bars, nil
}
