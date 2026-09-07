package main

// sync_data: bring every tradable ticker in the price table up to the latest
// session, in one action.
//
// The fetch_history tool refreshes up to three symbols per call and defaults
// to full history. That is right for "add TSLA", and hopeless for "the
// database is two months behind": ten thousand tickers is three thousand tool
// calls, and the assistant reliably gives up or times out partway through.
// This path exists so that keeping the data current is one operation rather
// than a loop the model has to drive.
//
// Two properties do the work:
//
//   - Only the missing tail is fetched. Each ticker's start is the day after
//     its last stored bar, so nothing already held is downloaded again.
//   - Tickers sharing a start date are fetched in one request. After any
//     previous sync almost all of them do, so a full refresh is a hundred
//     requests rather than ten thousand.
//
// Each batch is upserted as it arrives, so an interrupted sync keeps what it
// already stored — and because the next run derives its plan from the
// database's own last bars, it resumes exactly where this one stopped.

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"

	"my-backtester/src/data"
)

const syncDataToolName = "sync_data"

const syncDataToolDescription = "Bring every tradable ticker in the " +
	"database up to the latest trading session in one call. Fetches only " +
	"each ticker's missing tail, batched, so it is far faster than repeated " +
	"fetch_history calls — use this, not fetch_history, when the database " +
	"is broadly stale. Delisted symbols and locally-built $-prefixed " +
	"benchmark series are skipped. Safe to re-run: it resumes where it " +
	"stopped. Set dry_run to report what is stale without downloading."

func syncDataToolSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"dry_run": map[string]any{
				"type": "boolean",
				"description": "Report how many tickers are stale and " +
					"through what date, without fetching anything.",
			},
			"symbols": map[string]any{
				"type":  "array",
				"items": map[string]any{"type": "string"},
				"description": "Restrict the sync to these tickers. Omit " +
					"to sync every stale ticker in the database.",
			},
		},
	}
}

const (
	// One request carries this many symbols. Yahoo accepts more, but a larger
	// batch means a longer window in which a single network failure loses
	// everything in flight, and the per-request overhead is already amortized
	// a hundredfold at this size.
	syncBatchSize = 100

	// Per-batch subprocess timeout. Generous because it covers a hundred
	// symbols, and a slow batch is retried by the next run rather than lost.
	syncBatchTimeout = 180 * time.Second

	// How long the assistant's sync_data call may run before it stops and
	// reports what it managed. The UI button has no such budget: it runs
	// until it finishes or the user cancels.
	syncToolBudget = 10 * time.Minute

	syncHelperScript = "sync_prices.py"
)

// SyncProgress is one step of a running sync, for the UI's progress bar.
type SyncProgress struct {
	Batch   int    `json:"batch"`
	Batches int    `json:"batches"`
	Symbols int    `json:"symbols"`
	Stored  int    `json:"stored"`
	Rows    int    `json:"rows"`
	Failed  int    `json:"failed"`
	Current string `json:"current"`
}

// SyncReport is the outcome of a sync, shaped for both the chat tool's text
// summary and the frontend's result panel.
type SyncReport struct {
	// Through is the session the sync aimed at.
	Through string `json:"through"`
	// Considered is every ticker in the price table.
	Considered int `json:"considered"`
	// UpToDate already reached Through before the sync ran.
	UpToDate int `json:"upToDate"`
	// Stale needed data; Updated actually received some.
	Stale   int `json:"stale"`
	Updated int `json:"updated"`
	// NoNewBars were fetched but had not traded since their last bar.
	NoNewBars int `json:"noNewBars"`
	// Rows is the total number of daily bars written.
	Rows int `json:"rows"`
	// Skipped is delisted and locally-built symbols, by reason.
	Skipped map[string]int `json:"skipped"`
	// Failed names the tickers that errored, with the first reason each.
	Failed map[string]string `json:"failed"`
	// Incomplete is set when the sync stopped before working the whole plan.
	Incomplete bool   `json:"incomplete"`
	Reason     string `json:"reason,omitempty"`
	// Remaining counts targets never attempted, when Incomplete.
	Remaining int `json:"remaining"`
	DryRun    bool `json:"dryRun"`
}

// planSync opens the database and works out what needs fetching. Split from
// the fetch loop so the dry run, the UI's "what's stale?" panel and the real
// sync all decide identically.
func planSync(dbPath string, symbols []string) (data.SyncPlan, error) {
	if dbPath == "" {
		return data.SyncPlan{}, fmt.Errorf("no database chosen — pick a DB first")
	}
	if _, err := data.InitDB(dbPath); err != nil {
		return data.SyncPlan{}, fmt.Errorf("open db %q: %w", dbPath, err)
	}

	ranges, err := data.TickerDateRanges(symbols)
	if err != nil {
		return data.SyncPlan{}, fmt.Errorf("reading ticker coverage: %w", err)
	}
	if len(ranges) == 0 {
		return data.SyncPlan{}, fmt.Errorf("no tickers found in %s", dbPath)
	}

	tickers := make([]string, 0, len(ranges))
	for t := range ranges {
		tickers = append(tickers, t)
	}
	// A missing registry or profile table is not an error: PlanSync treats a
	// nil coverage map as "no evidence of death", which is what a database
	// without add_delistings.py honestly has.
	delistings, _ := data.LoadDelistings(tickers)
	profiles, _ := data.LoadCompanyProfiles(tickers)
	var coverage map[string]data.TickerCoverage
	if len(delistings) > 0 {
		coverage = data.ClassifyTickers(ranges, delistings, profiles)
	}

	return data.PlanSync(ranges, coverage, time.Now()), nil
}

// runSync executes a plan, upserting each batch as it arrives. onProgress may
// be nil. The context bounds the whole run; when it expires the report comes
// back Incomplete rather than as an error, because the batches already stored
// are real data the caller should be told about.
func runSync(
	ctx context.Context, plan data.SyncPlan, onProgress func(SyncProgress),
) SyncReport {
	rep := SyncReport{
		Through:  plan.Through,
		UpToDate: plan.UpToDate,
		Stale:    len(plan.Targets),
		Skipped:  map[string]int{},
		Failed:   map[string]string{},
	}
	rep.Considered = rep.UpToDate + rep.Stale + len(plan.Skipped)
	for _, s := range plan.Skipped {
		rep.Skipped[skipBucket(s.Reason)]++
	}
	if len(plan.Targets) == 0 {
		return rep
	}

	py, script, errMsg := locateHelper(syncHelperScript)
	if errMsg != "" {
		rep.Incomplete, rep.Reason = true, errMsg
		rep.Remaining = len(plan.Targets)
		return rep
	}

	// yfinance's end is exclusive, so the day after the target session is
	// what asks for the target session itself.
	through, err := time.Parse(data.ISODate, plan.Through)
	if err != nil {
		rep.Incomplete, rep.Reason = true, "unparseable target date "+plan.Through
		rep.Remaining = len(plan.Targets)
		return rep
	}
	end := through.AddDate(0, 0, 1).Format(data.ISODate)

	batches := data.SyncBatches(plan.Targets, syncBatchSize)
	prog := SyncProgress{Batches: len(batches)}

	for i, batch := range batches {
		if err := ctx.Err(); err != nil {
			rep.Incomplete = true
			rep.Reason = "stopped after " + itoa(i) + " of " +
				itoa(len(batches)) + " batches (" + ctxReason(err) + ")"
			for _, t := range batches[i:] {
				rep.Remaining += len(t)
			}
			break
		}

		prog.Batch = i + 1
		prog.Symbols += len(batch)
		prog.Current = batch[0].Ticker
		if len(batch) > 1 {
			prog.Current += "…" + batch[len(batch)-1].Ticker
		}
		if onProgress != nil {
			onProgress(prog)
		}

		results, err := fetchBatch(ctx, py, script, batch, end)
		if err != nil {
			// A whole batch failing is usually the network, not the symbols;
			// record it against each and carry on to the next batch.
			for _, t := range batch {
				rep.Failed[t.Ticker] = err.Error()
			}
			prog.Failed = len(rep.Failed)
			continue
		}

		for _, res := range results {
			switch {
			case res.Error != "":
				rep.Failed[res.Symbol] = res.Error
			case len(res.Rows) == 0:
				// Not an error: the ticker simply has not traded since its
				// last stored bar.
				rep.NoNewBars++
			default:
				bars, err := rowsToBars(res.Rows)
				if err != nil {
					rep.Failed[res.Symbol] = err.Error()
					continue
				}
				if err := data.UpsertBars(res.Symbol, bars); err != nil {
					rep.Failed[res.Symbol] = err.Error()
					continue
				}
				rep.Updated++
				rep.Rows += len(bars)
			}
		}
		prog.Stored, prog.Rows, prog.Failed = rep.Updated, rep.Rows, len(rep.Failed)
		if onProgress != nil {
			onProgress(prog)
		}
	}
	return rep
}

// syncResult is one symbol's slice of the helper's JSON output. It matches
// fetch_history.py's contract, so rowsToBars is shared between the two paths.
type syncResult struct {
	Symbol string  `json:"symbol"`
	Error  string  `json:"error"`
	Rows   [][]any `json:"rows"`
}

// fetchBatch runs the helper for one batch of same-start symbols.
func fetchBatch(
	ctx context.Context, py, script string, batch []data.SyncTarget, end string,
) ([]syncResult, error) {
	args := []string{script, "--start", batch[0].Start, "--end", end}
	for _, t := range batch {
		args = append(args, t.Ticker)
	}

	ctx, cancel := context.WithTimeout(ctx, syncBatchTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, py, args...)
	cmd.Dir = repoRoot()
	stdout, stderr := &strings.Builder{}, &strings.Builder{}
	cmd.Stdout, cmd.Stderr = stdout, stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("batch timed out after %s", syncBatchTimeout)
		}
		msg := strings.TrimSpace(stderr.String())
		if len(msg) > 200 {
			msg = msg[:200] + "…"
		}
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("helper failed: %s", msg)
	}

	var parsed struct {
		Results []syncResult `json:"results"`
	}
	if err := json.Unmarshal([]byte(stdout.String()), &parsed); err != nil {
		return nil, fmt.Errorf("could not parse helper output: %w", err)
	}
	return parsed.Results, nil
}

// skipBucket collapses a per-ticker skip reason into a countable category.
// Delisting reasons carry a date, which is worth showing for one ticker and
// useless in a tally of four hundred.
func skipBucket(reason string) string {
	if strings.HasPrefix(reason, data.SkipDelisted) {
		return data.SkipDelisted
	}
	return reason
}

func ctxReason(err error) string {
	if err == context.DeadlineExceeded {
		return "time budget reached"
	}
	return "cancelled"
}

func itoa(n int) string { return fmt.Sprintf("%d", n) }

// plural picks the noun form for n, so a one-ticker sync does not report
// "Updated 1 tickers" in text the user reads.
func plural(n int, word string) string {
	if n == 1 {
		return word
	}
	return word + "s"
}

// runSyncTool is the assistant's entry point: plan, fetch within a budget,
// and render the outcome as text the model can relay.
func runSyncTool(
	ctx context.Context, dbPath string, input map[string]any, em *chatEmitter,
) string {
	symbols := quoteSymbols(asAnySlice(input["symbols"]))
	plan, err := planSync(dbPath, symbols)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	dry, _ := input["dry_run"].(bool)
	if dry {
		rep := SyncReport{
			Through:    plan.Through,
			UpToDate:   plan.UpToDate,
			Stale:      len(plan.Targets),
			Considered: plan.UpToDate + len(plan.Targets) + len(plan.Skipped),
			Skipped:    map[string]int{},
			Failed:     map[string]string{},
			DryRun:     true,
		}
		for _, s := range plan.Skipped {
			rep.Skipped[skipBucket(s.Reason)]++
		}
		return formatSyncReport(rep)
	}

	ctx, cancel := context.WithTimeout(ctx, syncToolBudget)
	defer cancel()
	rep := runSync(ctx, plan, nil)

	if rep.Updated > 0 {
		// The system prompt's DB overview and the frontend's ticker list are
		// both stale now.
		dbOverviewCache.Delete(dbPath)
		if em != nil {
			em.dbChanged()
		}
	}
	return formatSyncReport(rep)
}

// syncPreview renders the tool-note chip.
func syncPreview(input map[string]any) string {
	if dry, _ := input["dry_run"].(bool); dry {
		return "check what is stale"
	}
	if syms := quoteSymbols(asAnySlice(input["symbols"])); len(syms) > 0 {
		return strings.Join(syms, ", ")
	}
	return "every stale ticker"
}

// asAnySlice normalizes an optional JSON array argument, which is absent as
// nil rather than as an empty slice.
func asAnySlice(v any) []any {
	s, _ := v.([]any)
	return s
}

// formatSyncReport renders a report as the text the assistant sees and relays.
func formatSyncReport(rep SyncReport) string {
	var b strings.Builder

	if rep.DryRun {
		fmt.Fprintf(&b, "Dry run — nothing was downloaded.\n")
	}
	fmt.Fprintf(&b, "Target session: %s\n", rep.Through)
	fmt.Fprintf(&b, "%d tickers in the database: %d already current, "+
		"%d stale, %d skipped.\n",
		rep.Considered, rep.UpToDate, rep.Stale,
		rep.Considered-rep.UpToDate-rep.Stale)

	if len(rep.Skipped) > 0 {
		reasons := make([]string, 0, len(rep.Skipped))
		for r := range rep.Skipped {
			reasons = append(reasons, r)
		}
		sort.Strings(reasons)
		for _, r := range reasons {
			fmt.Fprintf(&b, "  skipped %d: %s\n", rep.Skipped[r], r)
		}
	}

	if !rep.DryRun {
		fmt.Fprintf(&b, "Updated %d %s with %d new daily bars",
			rep.Updated, plural(rep.Updated, "ticker"), rep.Rows)
		if rep.NoNewBars > 0 {
			fmt.Fprintf(&b, "; %d had not traded since their last bar",
				rep.NoNewBars)
		}
		b.WriteString(".\n")

		if n := len(rep.Failed); n > 0 {
			fmt.Fprintf(&b, "%d failed: %s\n", n, failedPreview(rep.Failed))
		}
		if rep.Incomplete {
			fmt.Fprintf(&b, "INCOMPLETE: %s. %d tickers were not attempted — "+
				"everything fetched so far is stored, and running sync_data "+
				"again resumes from here.\n", rep.Reason, rep.Remaining)
		}
		if rep.Updated > 0 {
			b.WriteString("The new data is immediately usable in backtests " +
				"and query_db.\n")
		}
	} else if rep.Stale > 0 {
		b.WriteString("Run sync_data without dry_run to fetch them.\n")
	}

	return strings.TrimRight(b.String(), "\n")
}

// failedPreview names a handful of failures rather than all of them: a list
// of four hundred symbols buries the summary it is attached to.
func failedPreview(failed map[string]string) string {
	names := make([]string, 0, len(failed))
	for t := range failed {
		names = append(names, t)
	}
	sort.Strings(names)

	const show = 5
	if len(names) <= show {
		return strings.Join(names, ", ") + " (" + failed[names[0]] + ")"
	}
	return fmt.Sprintf("%s and %d more (%s)",
		strings.Join(names[:show], ", "), len(names)-show, failed[names[0]])
}
