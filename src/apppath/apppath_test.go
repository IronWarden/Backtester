package apppath

import (
	"os"
	"path/filepath"
	"testing"
)

// withExecutable stands the "binary" somewhere the test controls, so the
// installed-app case can be exercised without moving the test binary.
func withExecutable(t *testing.T, path string) {
	t.Helper()
	prev := executable
	executable = func() (string, error) { return path, nil }
	t.Cleanup(func() { executable = prev })
}

// The case nothing covered before: a process started from a directory that
// has nothing to do with the app. Everything used to resolve from the working
// directory, so this found nothing at all.
func TestResolveFromAnUnrelatedDirectory(t *testing.T) {
	install := t.TempDir()
	if err := os.MkdirAll(filepath.Join(install, "strategies"), 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(install, "strategies", "mine.lua")
	if err := os.WriteFile(script, []byte("-- hi\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	withExecutable(t, filepath.Join(install, "backtester"))
	t.Chdir(t.TempDir())

	got, ok := Resolve(filepath.Join("strategies", "mine.lua"))
	if !ok {
		t.Fatal("a script beside the executable was not found")
	}
	if got != script {
		t.Fatalf("Resolve = %q, want %q", got, script)
	}
	if dir, ok := ResolveDir("strategies"); !ok || dir != filepath.Dir(script) {
		t.Fatalf("ResolveDir = %q, %v; want %q", dir, ok, filepath.Dir(script))
	}
	if root := Root(); root != install {
		t.Fatalf("Root = %q, want %q", root, install)
	}
}

// The binary may be reached through a symlink on $PATH; what matters is where
// the real file sits, because that is what the resources were installed
// beside.
func TestResolveFollowsASymlinkedExecutable(t *testing.T) {
	install := t.TempDir()
	real := filepath.Join(install, "backtester")
	if err := os.WriteFile(real, []byte("#!/bin/true\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(install, "config.toml"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "backtester")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	withExecutable(t, link)
	t.Chdir(t.TempDir())

	if _, ok := Resolve("config.toml"); !ok {
		t.Fatal("config.toml beside the real binary was not found through the symlink")
	}
}

// The working directory still wins. A checkout must behave exactly as it did
// before this package existed -- editing a script in place is the whole
// development loop -- so a file in cwd beats the same name beside the binary.
func TestWorkingDirectoryWinsOverTheExecutable(t *testing.T) {
	install := t.TempDir()
	if err := os.WriteFile(filepath.Join(install, "config.toml"), []byte("installed"), 0o644); err != nil {
		t.Fatal(err)
	}
	withExecutable(t, filepath.Join(install, "backtester"))

	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "config.toml"), []byte("local"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(work)

	got, ok := Resolve("config.toml")
	if !ok {
		t.Fatal("config.toml in the working directory was not found")
	}
	if got != "config.toml" {
		t.Fatalf("Resolve = %q, want the relative %q -- a resolved path that "+
			"suddenly turns absolute changes every path the UI displays", got, "config.toml")
	}
	b, err := os.ReadFile(got)
	if err != nil || string(b) != "local" {
		t.Fatalf("resolved the installed copy, not the local one: %q, %v", b, err)
	}
}

// Root falls back to what this project assumed unconditionally before, so a
// checkout with no library present is no worse off than it was.
func TestRootFallsBackToParent(t *testing.T) {
	withExecutable(t, filepath.Join(t.TempDir(), "backtester"))
	t.Chdir(t.TempDir())
	if root := Root(); root != ".." {
		t.Fatalf("Root = %q, want %q", root, "..")
	}
}

// Candidates is what a caller choosing where to CREATE a file walks, so it
// must list every root, existing or not, and keep the near ones relative.
func TestCandidatesCoverEveryRoot(t *testing.T) {
	install := t.TempDir()
	withExecutable(t, filepath.Join(install, "bin", "backtester"))
	t.Chdir(t.TempDir())

	got := Candidates(filepath.Join("strategies", "x.lua"))
	if len(got) < 5 {
		t.Fatalf("Candidates = %v, want the cwd walk plus the executable's directories", got)
	}
	if got[0] != filepath.Join("strategies", "x.lua") {
		t.Fatalf("first candidate = %q, want the path as given", got[0])
	}
	want := filepath.Join(install, "bin", "strategies", "x.lua")
	found := false
	for _, c := range got {
		if c == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("Candidates = %v, missing the executable's own directory %q", got, want)
	}
}

// A duplicate root would make every caller stat the same directory twice and,
// worse, would let an absolute spelling of the working directory shadow the
// relative one the UI displays.
func TestRootsAreDeduplicated(t *testing.T) {
	work := t.TempDir()
	withExecutable(t, filepath.Join(work, "backtester"))
	t.Chdir(work)

	seen := map[string]bool{}
	for _, r := range Roots() {
		abs, err := filepath.Abs(r)
		if err != nil {
			t.Fatal(err)
		}
		if seen[abs] {
			t.Fatalf("Roots repeats %q: %v", abs, Roots())
		}
		seen[abs] = true
	}
	if Roots()[0] != "." {
		t.Fatalf("Roots[0] = %q, want the working directory first", Roots()[0])
	}
}
