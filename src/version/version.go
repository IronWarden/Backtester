// Package version is what a build calls itself.
//
// A beta bug report that cannot name a build is very hard to act on: "the
// contribution figures are wrong" is a different conversation depending on
// whether the reporter has the build from before T113 (where a lowercase key
// made every contribution config unparseable) or after it. Nothing in the
// binary reported anything at all, and ui/frontend/package.json said 0.0.0.
//
// So: one constant, read by the CLI's -version, by the desktop app's header
// and by the assistant's system prompt, and stamped into the package the
// release script builds.
package version

import (
	"fmt"
	"runtime"
	"strings"
)

// Version is the release this source tree is. Semver with a pre-release tag
// while the beta lasts; the LICENSE's terms end at 1.0, so the tag is load
// bearing rather than decorative.
const Version = "0.1.0-beta.1"

// Commit is set at link time by the release script:
//
//	go build -ldflags "-X my-backtester/src/version.Commit=$(git rev-parse --short HEAD)"
//
// A build from an ordinary `go build` leaves it empty, which is honest: it
// says "not a release" rather than naming a commit nobody can check out.
var Commit = ""

// String is what a user reads back to you in a bug report: the version, the
// commit when there is one, and the platform, because the beta is Linux-only
// and a report from anywhere else is answered differently.
func String() string {
	var b strings.Builder
	b.WriteString("Backtester ")
	b.WriteString(Version)
	if Commit != "" {
		fmt.Fprintf(&b, " (%s)", Commit)
	}
	fmt.Fprintf(&b, " · %s/%s · %s",
		runtime.GOOS, runtime.GOARCH, runtime.Version())
	return b.String()
}

// Short is the version alone, for a header chip that has no room for the rest.
func Short() string {
	if Commit == "" {
		return Version
	}
	return Version + " (" + Commit + ")"
}
