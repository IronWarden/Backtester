package main

// The headless half of sync_data.
//
// The button and the assistant tool both need a running app, and there are
// two occasions when there isn't one: keeping a machine's database current
// without opening a GUI, and the first sync of a freshly downloaded database,
// which is exactly when it is furthest behind. `ui --sync <db>` runs the same
// plan-fetch-upsert the button runs, prints the same summary, and exits.

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// runSyncCommand handles `--sync <db>`, returning false when the flag was not
// given so main falls through to launching the GUI.
func runSyncCommand(args []string) (handled bool, code int) {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	dbPath := fs.String("sync", "", "sync the price data in this database and exit")
	dryRun := fs.Bool("dry-run", false, "report what is stale without downloading")
	if err := fs.Parse(args); err != nil || *dbPath == "" {
		return false, 0
	}

	plan, err := planSync(*dbPath, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "sync:", err)
		return true, 1
	}

	if *dryRun {
		fmt.Println(formatSyncReport(planReport(plan)))
		return true, 0
	}

	// Ctrl-C stops the run the way the Stop button does: the batch in flight
	// is lost, everything already stored is kept, and the next run resumes.
	ctx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rep := runSync(ctx, plan, func(p SyncProgress) {
		// One line per batch, overwritten in place: a full sync is a hundred
		// batches, and a hundred lines of scrollback buries the summary.
		fmt.Fprintf(os.Stderr, "\r\033[Kbatch %d/%d  %s  %d tickers, %d bars",
			p.Batch, p.Batches, p.Current, p.Stored, p.Rows)
	})
	fmt.Fprintln(os.Stderr)

	fmt.Println(formatSyncReport(rep))
	if rep.Incomplete {
		return true, 1
	}
	return true, 0
}
