package data

import (
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// makeDB writes a throwaway DuckDB with one row in it and closes it again,
// leaving a file on disk to be reopened under the mode being tested.
func makeDB(t *testing.T, path string) {
	t.Helper()
	d, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	if _, err := d.Exec(`CREATE TABLE t(a INTEGER); INSERT INTO t VALUES (7)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// resetDB drops the package handle so each test opens from scratch and does
// not inherit another's decision.
func resetDB(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		initMu.Lock()
		defer initMu.Unlock()
		if db != nil {
			db.Close()
		}
		db, dbDSN, dbStat = nil, "", DBStatus{}
	})
	initMu.Lock()
	if db != nil {
		db.Close()
	}
	db, dbDSN, dbStat = nil, "", DBStatus{}
	initMu.Unlock()
}

// The ordinary case, and the one that must not change: a database the user
// owns opens in place, read-write, with nothing to report and nothing copied.
func TestOpenWritableDatabaseInPlace(t *testing.T) {
	resetDB(t)
	data := t.TempDir()
	t.Setenv("XDG_DATA_HOME", data)

	path := filepath.Join(t.TempDir(), "own.db")
	makeDB(t, path)
	if _, err := InitDB(path); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	st := Status()
	if st.Mode != ModeReadWrite || st.Reason != "" || st.Path != path {
		t.Fatalf("status = %+v, want a silent read-write open of %s", st, path)
	}
	if entries, _ := os.ReadDir(filepath.Join(data, "backtester")); len(entries) != 0 {
		t.Errorf("a writable database was copied anyway: %v", entries)
	}
	if err := UpsertBars("TEST", []Bar{{Date: "2024-01-02", Close: 1}}); err != nil {
		t.Errorf("writes refused on a writable database: %v", err)
	}
}

// A database installed where it cannot be written -- /usr/share, /opt, an
// AppImage mount, or plain 444 -- is copied once into the per-user data
// directory and opened there, so fetching new bars still works.
func TestReadOnlyFileIsCopiedAndOpenedWritable(t *testing.T) {
	resetDB(t)
	dataHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)

	dir := t.TempDir()
	path := filepath.Join(dir, "shipped.db")
	makeDB(t, path)
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })

	if _, err := InitDB(path); err != nil {
		t.Fatalf("InitDB on a read-only file: %v", err)
	}
	st := Status()
	if st.Mode != ModeReadWrite {
		t.Fatalf("status = %+v, want the writable copy", st)
	}
	copyPath := filepath.Join(dataHome, "backtester", "shipped.db")
	if st.Path != copyPath {
		t.Fatalf("opened %s, want the copy at %s", st.Path, copyPath)
	}
	if st.Source != path || !strings.Contains(st.Reason, copyPath) {
		t.Errorf("status does not say what happened: %+v", st)
	}
	// The copy is a real database, and it takes writes.
	if err := UpsertBars("TEST", []Bar{{Date: "2024-01-02", Close: 1}}); err != nil {
		t.Fatalf("the copy refused a write: %v", err)
	}

	// Reopening reuses the copy rather than remaking it -- anything fetched
	// into it would otherwise be wiped by the next launch.
	written, err := storedBarCount()
	if err != nil {
		t.Fatal(err)
	}
	resetDB(t)
	if _, err := InitDB(path); err != nil {
		t.Fatalf("second InitDB: %v", err)
	}
	if got := Status().Path; got != copyPath {
		t.Fatalf("second open used %s, want the same copy", got)
	}
	again, err := storedBarCount()
	if err != nil {
		t.Fatal(err)
	}
	if again != written {
		t.Fatalf("the copy was remade: %s rows became %s", written, again)
	}
}

// storedBarCount counts the bars stored by the test above, through the shared
// handle, so "did the copy survive" is asked of the database the app opened.
func storedBarCount() (string, error) {
	_, rows, _, err := RunQuery(
		`SELECT COUNT(*) FROM stock_data_optimized`, 10)
	if err != nil || len(rows) == 0 {
		return "", err
	}
	return rows[0][0], nil
}

// When the copy cannot be made either, the database still opens -- read-only.
// A database you can read is worth far more than an error message, and the
// only thing lost is fetching new data.
func TestReadOnlyFallbackWhenNoCopyIsPossible(t *testing.T) {
	resetDB(t)
	// A data directory that cannot be created: its parent is a file.
	blocked := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocked, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_DATA_HOME", blocked)

	dir := t.TempDir()
	path := filepath.Join(dir, "shipped.db")
	makeDB(t, path)
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })

	if _, err := InitDB(path); err != nil {
		t.Fatalf("InitDB fell over instead of opening read-only: %v", err)
	}
	st := Status()
	if !st.ReadOnly() {
		t.Fatalf("status = %+v, want read-only", st)
	}
	if !strings.Contains(st.Reason, path) || !strings.Contains(st.Reason, "read-only") {
		t.Errorf("reason does not say what happened: %q", st.Reason)
	}
	// Reading works.
	_, rows, _, err := RunQuery(`SELECT a FROM t`, 10)
	if err != nil {
		t.Fatalf("read-only database refused a SELECT: %v", err)
	}
	if len(rows) != 1 || rows[0][0] != "7" {
		t.Fatalf("read-only query returned %v", rows)
	}
	// Writing is refused with the reason, not with a DuckDB attach error.
	err = UpsertBars("TEST", []Bar{{Date: "2024-01-02", Close: 1}})
	if err == nil {
		t.Fatal("a write succeeded against a read-only database")
	}
	if !strings.Contains(err.Error(), "read-only") {
		t.Errorf("write error does not explain itself: %v", err)
	}
}

// The second opener. DuckDB refuses the attach outright -- read-only does not
// help -- and its own message is a lock path and a PID. The app has to turn
// that into a sentence naming the program to close.
func TestLockConflictNamesTheOtherProcess(t *testing.T) {
	resetDB(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "held.db")
	makeDB(t, path)

	held := path + ".held"
	holder := exec.Command(os.Args[0], "-test.run=TestHoldsTheLockForAnotherTest")
	holder.Env = append(os.Environ(), "BACKTESTER_HOLD_DB="+path)
	if err := holder.Start(); err != nil {
		t.Fatalf("starting the holder: %v", err)
	}
	t.Cleanup(func() {
		holder.Process.Kill()
		holder.Wait()
	})
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(held); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Skip("the holding process never took the lock")
		}
		time.Sleep(20 * time.Millisecond)
	}

	_, err := InitDB(path)
	if err == nil {
		t.Fatal("opening a database held by another process succeeded")
	}
	msg := err.Error()
	if strings.Contains(msg, "Conflicting lock") || strings.Contains(msg, "IO Error") {
		t.Errorf("DuckDB's own message reached the user verbatim: %v", msg)
	}
	for _, want := range []string{path, "PID", "already open"} {
		if !strings.Contains(msg, want) {
			t.Errorf("lock message %q does not mention %q", msg, want)
		}
	}
}

// TestHoldsTheLockForAnotherTest is not a test: it is the second process
// TestLockConflictNamesTheOtherProcess needs, re-entering this binary because
// a lock conflict cannot be produced inside one process.
func TestHoldsTheLockForAnotherTest(t *testing.T) {
	path := os.Getenv("BACKTESTER_HOLD_DB")
	if path == "" {
		t.Skip("only runs as the lock holder")
	}
	d, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("holder could not open %s: %v", path, err)
	}
	defer d.Close()
	if err := os.WriteFile(path+".held", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Second)
}

// The message is assembled from DuckDB's, so a driver upgrade that reworded
// it must not silently degrade the sentence to "another program".
func TestLockHolderParsesDuckDBsMessage(t *testing.T) {
	err := &stringError{`database/sql/driver: could not connect to database: ` +
		`duckdb error: IO Error: Could not set lock on file "/opt/app/stock_data.db": ` +
		`Conflicting lock is held in /home/u/app/ui (PID 33601) by user u. ` +
		`See also https://duckdb.org/docs/connect/concurrency`}
	holder, ok := lockHolder(err)
	if !ok {
		t.Fatal("a conflicting-lock error was not recognised")
	}
	if holder != "ui (PID 33601)" {
		t.Fatalf("holder = %q, want %q", holder, "ui (PID 33601)")
	}
	if _, ok := lockHolder(&stringError{"IO Error: Cannot open file: Permission denied"}); ok {
		t.Error("a permission error was read as a lock conflict")
	}
}

type stringError struct{ s string }

func (e *stringError) Error() string { return e.s }
