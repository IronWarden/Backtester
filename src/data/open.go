package data

// How the database file is opened, and what to say when it cannot be.
//
// `sql.Open("duckdb", path)` is read-write and connects eagerly, which gives
// a shipped database two ways to be dead on arrival:
//
//   - the file is not writable -- installed to /usr/share, /opt, or an
//     AppImage mount, or simply chmod 444. DuckDB refuses outright:
//     `IO Error: Cannot open file "…": Permission denied`.
//   - something else already has it open. DuckDB is single-writer AND
//     single-opener: a second window, a CLI run or a Python loader gets
//     `IO Error: Could not set lock on file "…": Conflicting lock is held
//     in <path> (PID N) by user <u>`, verbatim, at the user. Read-only does
//     not help -- an attach is refused while the lock is held either way.
//
// So opening walks three steps: the file in place, a writable per-user copy
// of it, and finally read-only. Only the first can happen to a database the
// user already owns, which is why nothing changes for an existing checkout.

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"my-backtester/src/apppath"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// OpenMode reports what the shared handle can do.
type OpenMode string

const (
	ModeReadWrite OpenMode = "read-write"
	ModeReadOnly  OpenMode = "read-only"
)

// DBStatus describes the open database: which file is really behind the
// handle, what it can do, and -- when that is not what was asked for -- why.
// The UI shows Reason verbatim, so it is written as a sentence.
type DBStatus struct {
	// Path is the file actually opened. Source is the file that was asked
	// for; they differ only when a writable copy was made.
	Path   string
	Source string
	Mode   OpenMode
	// Reason is empty when the database opened as asked, and otherwise says
	// what happened in one sentence.
	Reason string
}

// ReadOnly reports whether writes will fail. Loaders check it rather than
// discovering it from a DuckDB error halfway through an upsert.
func (s DBStatus) ReadOnly() bool { return s.Mode == ModeReadOnly }

// readOnlyDSN appends DuckDB's read-only access mode to a path.
func readOnlyDSN(path string) string {
	if strings.Contains(path, "?") {
		return path + "&access_mode=read_only"
	}
	return path + "?access_mode=read_only"
}

// openDatabase opens path the best way it can and says what it did.
//
// The order matters: in place first, so a database the user owns is opened
// exactly as it always was and no gigabytes are copied behind their back; a
// per-user copy second, because a read-only database cannot fetch new bars
// and copying is the only way to give it back; read-only last, because a
// database you can read is worth far more than an error message.
func openDatabase(path string) (*sql.DB, DBStatus, error) {
	st := DBStatus{Path: path, Source: path, Mode: ModeReadWrite}

	db, err := sql.Open("duckdb", path)
	if err == nil {
		return db, st, nil
	}
	// A held lock is not solved by copying or by read-only: DuckDB refuses
	// the attach itself. Say who has it instead.
	if holder, ok := lockHolder(err); ok {
		return nil, st, fmt.Errorf("%s", lockMessage(path, holder))
	}
	// Anything other than a permission problem is a real failure -- a
	// corrupt file, a missing directory -- and hiding it behind a read-only
	// retry would report the wrong thing.
	if !isPermissionDenied(err) {
		return nil, st, err
	}

	copied, copyErr := writableCopy(path)
	if copyErr == nil {
		if db, err := sql.Open("duckdb", copied); err == nil {
			st.Path = copied
			st.Reason = fmt.Sprintf(
				"%s cannot be written to, so the app is using its own copy at %s.",
				path, copied)
			log.Printf("data: %s", st.Reason)
			return db, st, nil
		} else if holder, ok := lockHolder(err); ok {
			return nil, st, fmt.Errorf("%s", lockMessage(copied, holder))
		}
	}

	db, roErr := sql.Open("duckdb", readOnlyDSN(path))
	if roErr != nil {
		// Read-only failed too: the first error is the one that explains it.
		return nil, st, err
	}
	st.Mode = ModeReadOnly
	st.Reason = readOnlyReason(path, copyErr)
	log.Printf("data: %s", st.Reason)
	return db, st, nil
}

// readOnlyReason explains, in one sentence, why writes are off: the file
// could not be written and a copy could not be made either.
func readOnlyReason(path string, copyErr error) string {
	msg := fmt.Sprintf(
		"%s is open read-only because the file cannot be written to", path)
	if copyErr != nil {
		msg += fmt.Sprintf(" and a writable copy could not be made (%v)", copyErr)
	}
	return msg + ". Everything reads normally; fetching new data is what stops working."
}

// isPermissionDenied reports whether DuckDB refused the file because it may
// not write it. Matched on the message because the driver returns a plain
// error, not a wrapped *fs.PathError.
func isPermissionDenied(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "Permission denied") ||
		strings.Contains(msg, "Read-only file system") ||
		strings.Contains(msg, "read-only file system")
}

// lockPattern pulls the holder out of DuckDB's conflicting-lock error, which
// already names the executable and PID -- it is the sentence around them that
// is missing.
var lockPattern = regexp.MustCompile(
	`Conflicting lock is held in (.+?) \(PID (\d+)\)`)

// lockHolder returns "<program> (PID n)" when err is a lock conflict.
func lockHolder(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	m := lockPattern.FindStringSubmatch(err.Error())
	if m == nil {
		if strings.Contains(err.Error(), "Conflicting lock") {
			return "another program", true
		}
		return "", false
	}
	return fmt.Sprintf("%s (PID %s)", filepath.Base(m[1]), m[2]), true
}

// lockMessage turns the conflict into something a user can act on. DuckDB
// allows one process to hold a database at a time, so the fix is always the
// same: close the other one.
func lockMessage(path, holder string) string {
	return fmt.Sprintf(
		"%s is already open in %s. DuckDB lets one program hold a database at "+
			"a time, so close that one -- the desktop app, another window, or "+
			"a Python loader -- and try again.", path, holder)
}

// writableCopy returns a copy of path in the per-user data directory,
// creating it if there is not already a usable one. The copy is what a
// packaged app runs against: a database installed read-only can still gain
// new bars this way.
//
// A previous copy is reused whenever the source it was made from has not
// changed, which is what keeps data fetched INTO the copy from being wiped by
// the next launch. The source's size and modification time are recorded
// beside it, since the copy's own size drifts as soon as anything is written.
func writableCopy(path string) (string, error) {
	src, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	dir, err := apppath.DataDir()
	if err != nil {
		return "", err
	}
	dest := filepath.Join(dir, filepath.Base(path))
	stamp := dest + ".source"
	want := sourceStamp(path, src)

	if _, err := os.Stat(dest); err == nil {
		if got, err := os.ReadFile(stamp); err == nil && string(got) == want {
			return dest, nil
		}
	}

	// Copy through a temporary file in the destination directory and rename,
	// so an interrupted copy of a multi-gigabyte database never looks like a
	// finished one.
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".partial-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())

	in, err := os.Open(path)
	if err != nil {
		tmp.Close()
		return "", err
	}
	log.Printf("data: copying %s (%d MB) to %s so it can be written to",
		path, src.Size()/(1<<20), dest)
	_, copyErr := io.Copy(tmp, in)
	in.Close()
	closeErr := tmp.Close()
	if copyErr != nil {
		return "", copyErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if err := os.Rename(tmp.Name(), dest); err != nil {
		return "", err
	}
	// The stamp is written last: without it the copy is reused only after
	// being remade, which is the safe way round.
	if err := os.WriteFile(stamp, []byte(want), 0o644); err != nil {
		return "", err
	}
	return dest, nil
}

// sourceStamp identifies the file a copy was made from.
func sourceStamp(path string, info os.FileInfo) string {
	return fmt.Sprintf("%s\n%d\n%d\n", path, info.Size(), info.ModTime().UnixNano())
}
