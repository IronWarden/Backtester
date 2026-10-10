package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Go infers build constraints from FILE NAMES: anything ending `_windows.go`,
// `_darwin.go`, `_arm64.go` and so on is silently constrained to that platform
// and never compiled here. There is no error and no warning — `go vet` is
// clean, `go test` prints ok, and the file simply does not exist as far as the
// build is concerned.
//
// This cost real time: a new test named `portfolio_windows_test.go` — for date
// windows, nothing to do with Microsoft — was ignored on Linux, and `go test`
// reported PASS for a package whose new tests had never run. That is the
// green-light-wired-to-nothing failure this suite exists to prevent, arriving
// through the filename rather than the code.
//
// This project builds for Linux only, so ANY such suffix is a mistake.
var platformSuffixes = []string{
	"aix", "android", "darwin", "dragonfly", "freebsd", "hurd", "illumos",
	"ios", "js", "linux", "nacl", "netbsd", "openbsd", "plan9", "solaris",
	"wasip1", "windows", "zos",
	"386", "amd64", "arm", "arm64", "loong64", "mips", "mips64", "mips64le",
	"mipsle", "ppc64", "ppc64le", "riscv64", "s390x", "sparc64", "wasm",
}

func TestNoAccidentalPlatformSuffixes(t *testing.T) {
	root := repoRoot()
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable corners are not this test's business
		}
		if d.IsDir() {
			switch d.Name() {
			case "node_modules", ".git", "build", "dist", "wailsjs":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		// Strip .go and a _test suffix, then look at the final _word.
		base := strings.TrimSuffix(d.Name(), ".go")
		base = strings.TrimSuffix(base, "_test")
		for _, suffix := range platformSuffixes {
			if strings.HasSuffix(base, "_"+suffix) {
				t.Errorf("%s ends in _%s, which Go reads as a build constraint: "+
					"the file is silently excluded from this build and its tests "+
					"never run. Rename it.", path, suffix)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
