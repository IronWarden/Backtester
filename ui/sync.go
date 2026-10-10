package main

// Price sync: bring every tradable ticker in the price table up to the latest
// finished session, in one action. Run headless as `ui --sync <db>` (see
// synccmd.go); the app itself no longer offers it, from the Data tab or the
// assistant.
//
// The fetch_history tool refreshes up to three symbols per call and defaults
// to full history. That is right for "add TSLA", and hopeless for "the
// database is two months behind": ten thousand tickers is three thousand
// calls. This path makes keeping the data current one operation.
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
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"

	"my-backtester/src/data"
)

const (
	// One request carries this many symbols. Yahoo accepts more, but a larger
	// batch means a longer window in which a single network failure loses
	// everything in flight, and the per-request overhead is already amortized
	// a hundredfold at this size.
	syncBatchSize = 100

	// How long a killed helper's children may keep its output pipes open
	// before Run gives up on them. yfinance downloads on worker threads, and
	// without a bound a timed-out batch could hang on the pipe indefinitely.
	syncWaitDelay = 2 * time.Second

	// Breathing room between batches. A full refresh is over a hundred
	// requests, and firing them back to back is what gets the whole run
	// throttled -- which used to be invisible, because a throttled batch came
	// back looking like a hundred tickers that simply had not traded.
	syncBatchPause = 2 * time.Second

	// A throttled batch is retried after this long, doubling each time. Yahoo
	// does not say how long its limit lasts, so the wait is the only lever.
	syncRateLimitBackoff = 30 * time.Second

	// How many times a throttled batch is retried before the run gives up.
	// Giving up is the point: marching on through the remaining batches while
	// throttled fetches nothing and deepens the limit.
	syncRateLimitRetries = 4

	syncHelperScript = "sync_prices.py"
)

// Per-batch subprocess timeout. Generous because it covers a hundred symbols;
// a batch that still times out is split and retried once (fetchBatchResilient).
// A var so tests can shorten it.
var syncBatchTimeout = 180 * time.Second

// errBatchTimeout marks a batch that exceeded syncBatchTimeout, as opposed to
// one cut short by the caller's own deadline or cancellation.
var errBatchTimeout = errors.New("batch timed out")

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
	Remaining int  `json:"remaining"`
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

// planReport renders a plan as a SyncReport without fetching anything. The
// CLI's --dry-run goes through it, as does the live run's opening tally, so
// the two cannot describe the same plan differently.
func planReport(plan data.SyncPlan) SyncReport {
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
	return rep
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

		results, err, throttled := fetchBatchResilient(ctx, py, script, batch, end)
		if ctx.Err() != nil {
			// The run's own deadline or a Stop landed mid-batch. The batch is
			// unattempted, not failed: nothing about its symbols went wrong,
			// and the next run fetches it again.
			rep.Incomplete = true
			rep.Reason = "stopped during batch " + itoa(i+1) + " of " +
				itoa(len(batches)) + " (" + ctxReason(ctx.Err()) + ")"
			for _, t := range batches[i:] {
				rep.Remaining += len(t)
			}
			break
		}
		if throttled {
			// Every retry came back throttled. Stop rather than spend the
			// remaining batches confirming it.
			rep.Incomplete = true
			rep.Reason = "Yahoo rate limited the run after " + itoa(i) +
				" of " + itoa(len(batches)) + " batches"
			for _, t := range batches[i:] {
				rep.Remaining += len(t)
			}
			break
		}
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

		if i < len(batches)-1 && !sleepCtx(ctx, syncBatchPause) {
			rep.Incomplete = true
			rep.Reason = "stopped after " + itoa(i+1) + " of " +
				itoa(len(batches)) + " batches (" + ctxReason(ctx.Err()) + ")"
			for _, t := range batches[i+1:] {
				rep.Remaining += len(t)
			}
			break
		}
	}
	return rep
}

// fetchBatchResilient is fetchBatchPatiently with one retry for a batch that
// timed out: it is split in half and each half is fetched on its own. A
// timeout is usually one slow symbol or a slow network stretch, and failing a
// hundred tickers for it is the expensive answer. The split happens once, not
// recursively, so a genuinely dead network costs at most three timeouts per
// batch rather than a cascade of them.
func fetchBatchResilient(
	ctx context.Context, py, script string, batch []data.SyncTarget, end string,
) ([]syncResult, error, bool) {
	results, err, throttled := fetchBatchPatiently(ctx, py, script, batch, end)
	if !errors.Is(err, errBatchTimeout) || len(batch) < 2 || ctx.Err() != nil {
		return results, err, throttled
	}

	mid := len(batch) / 2
	var merged []syncResult
	for _, half := range [][]data.SyncTarget{batch[:mid], batch[mid:]} {
		res, herr, hthrottled := fetchBatchPatiently(ctx, py, script, half, end)
		if hthrottled {
			return nil, herr, true
		}
		if herr != nil {
			// This half failed again; name it against its own symbols so the
			// other half's data is not thrown away with it.
			for _, t := range half {
				merged = append(merged, syncResult{Symbol: t.Ticker, Error: herr.Error()})
			}
			continue
		}
		merged = append(merged, res...)
	}
	return merged, nil, false
}

// fetchBatchPatiently is fetchBatch with a wait-and-retry for a throttled
// response. It reports throttled only when every attempt was refused, which
// is the caller's cue to stop the run rather than to record failures.
func fetchBatchPatiently(
	ctx context.Context, py, script string, batch []data.SyncTarget, end string,
) (results []syncResult, err error, throttled bool) {
	wait := syncRateLimitBackoff
	for attempt := 0; ; attempt++ {
		results, err = fetchBatch(ctx, py, script, batch, end)
		if !rateLimited(results, err) {
			return results, err, false
		}
		if attempt >= syncRateLimitRetries {
			return nil, err, true
		}
		// A backoff the run's deadline will not outlast is not a retry, only
		// a slower way to report the same limit — give up now, while the
		// caller still has time to relay it.
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) < wait {
			return nil, err, true
		}
		if !sleepCtx(ctx, wait) {
			return nil, ctx.Err(), false
		}
		wait *= 2
	}
}

// rateLimited reports whether a batch was throttled rather than answered.
// Yahoo refuses the whole request, so a throttled batch is one where nothing
// came back and every symbol that spoke said so.
func rateLimited(results []syncResult, err error) bool {
	if err != nil {
		return strings.Contains(err.Error(), "RateLimit")
	}
	if len(results) == 0 {
		return false
	}
	for _, r := range results {
		if !strings.Contains(r.Error, "RateLimit") {
			return false
		}
	}
	return true
}

// sleepCtx waits for d, reporting false if the context ended first. A plain
// time.Sleep would make Stop take up to a batch-pause to be noticed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
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

	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, syncBatchTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, py, args...)
	cmd.Dir = repoRoot()
	cmd.WaitDelay = syncWaitDelay
	stdout, stderr := &strings.Builder{}, &strings.Builder{}
	cmd.Stdout, cmd.Stderr = stdout, stderr

	if err := cmd.Run(); err != nil {
		if perr := parent.Err(); perr != nil {
			// The run was stopped or ran out of budget; that is not this
			// batch's fault and must not be reported as its timeout.
			return nil, perr
		}
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("%w after %s", errBatchTimeout, syncBatchTimeout)
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

// formatSyncReport renders a report as the text `ui --sync` prints.
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
				"everything fetched so far is stored, and running ui --sync "+
				"again resumes from here.\n", rep.Reason, rep.Remaining)
		}
		if rep.Updated > 0 {
			b.WriteString("The new data is immediately usable in backtests " +
				"and query_db.\n")
		}
	} else if rep.Stale > 0 {
		b.WriteString("Run ui --sync without --dry-run to fetch them.\n")
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
