package main

// The frontend's half of sync_data: a bound method to start a sync, one to
// ask what is stale without fetching, and one to stop a running one.
//
// A sync is long — minutes, for a database months behind — so it cannot be a
// blocking bound call. SyncData returns as soon as the work is scheduled and
// reports through the "sync:progress" and "sync:done" events; only one runs
// at a time, because two concurrent syncs would fight over the same DuckDB
// write lock and re-fetch each other's work.

import (
	"context"
	"errors"
	"sync"

	wruntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

// syncRunner serializes syncs and carries the cancel func for the running one.
type syncRunner struct {
	mu     sync.Mutex
	cancel context.CancelFunc
}

var activeSync syncRunner

// SyncStatus reports what a sync would do, without fetching anything. Backs
// the Data panel's "N tickers are stale" line.
func (a *App) SyncStatus(dbPath string) (SyncReport, error) {
	plan, err := planSync(dbPath, nil)
	if err != nil {
		return SyncReport{}, err
	}
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
	return rep, nil
}

// SyncData starts a background sync of every stale ticker. It returns once
// the run is scheduled; progress arrives as "sync:progress" events and the
// outcome as "sync:done". Returns an error if a sync is already running, so
// a double-click cannot start two.
func (a *App) SyncData(dbPath string) error {
	plan, err := planSync(dbPath, nil)
	if err != nil {
		return err
	}

	activeSync.mu.Lock()
	if activeSync.cancel != nil {
		activeSync.mu.Unlock()
		return errSyncInProgress
	}
	// Deliberately not bounded by a deadline: the UI's sync runs until it is
	// done or the user stops it. The assistant's sync_data has a budget
	// because a chat turn cannot wait ten minutes; a button press can.
	ctx, cancel := context.WithCancel(a.ctx)
	activeSync.cancel = cancel
	activeSync.mu.Unlock()

	go func() {
		defer func() {
			activeSync.mu.Lock()
			activeSync.cancel = nil
			activeSync.mu.Unlock()
			cancel()
		}()

		rep := runSync(ctx, plan, func(p SyncProgress) {
			wruntime.EventsEmit(a.ctx, "sync:progress", p)
		})
		if rep.Updated > 0 {
			// The assistant's cached DB overview and the frontend's ticker
			// list both describe the pre-sync database now.
			dbOverviewCache.Delete(dbPath)
			wruntime.EventsEmit(a.ctx, "db:changed")
		}
		wruntime.EventsEmit(a.ctx, "sync:done", rep)
	}()
	return nil
}

// StopSync cancels a running sync. Everything already fetched stays written,
// and the next sync resumes from it. A no-op when nothing is running.
func (a *App) StopSync() {
	activeSync.mu.Lock()
	defer activeSync.mu.Unlock()
	if activeSync.cancel != nil {
		activeSync.cancel()
	}
}

// SyncSummary renders a report as the same text the assistant is given, so
// the panel and the chat cannot describe one run differently.
func (a *App) SyncSummary(rep SyncReport) string {
	return formatSyncReport(rep)
}

// errSyncInProgress is returned rather than queueing a second run: two syncs
// would contend for the same write lock and duplicate each other's fetches.
var errSyncInProgress = errors.New(
	"a sync is already running — wait for it to finish or stop it")
