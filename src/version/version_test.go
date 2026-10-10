package version

import (
	"regexp"
	"strings"
	"testing"
)

// The version is read back to us in bug reports and stamped on package
// directories, so it has to be a version rather than a mood.
var semver = regexp.MustCompile(`^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$`)

func TestVersionIsSemver(t *testing.T) {
	if !semver.MatchString(Version) {
		t.Errorf("Version = %q, which is not semver — npm, the package name "+
			"and every download link depend on it being one", Version)
	}
}

// The whole point is that a report can name the build. Anything missing here
// is a question someone has to ask before they can act on the report.
func TestStringNamesTheBuild(t *testing.T) {
	got := String()
	for _, want := range []string{"Backtester", Version, "linux", "go1."} {
		if !strings.Contains(got, want) {
			t.Errorf("version string %q does not mention %q", got, want)
		}
	}
	// An unstamped build must not invent a commit.
	if strings.Contains(got, "()") {
		t.Errorf("an empty commit was rendered anyway: %q", got)
	}
}

func TestShortCarriesTheCommitWhenThereIsOne(t *testing.T) {
	if Short() != Version {
		t.Errorf("Short() = %q on an unstamped build, want %q", Short(), Version)
	}
	prev := Commit
	Commit = "abc1234"
	defer func() { Commit = prev }()
	if Short() != Version+" (abc1234)" {
		t.Errorf("Short() = %q with a commit stamped", Short())
	}
}
