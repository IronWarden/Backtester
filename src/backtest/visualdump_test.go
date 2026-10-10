package backtest

import (
	"os"
	"testing"
)

// TestCandidatePageVisualDump writes a fully-populated candidate page to the
// path in PAGE_DUMP and skips otherwise, so it costs nothing in a normal run.
//
// It exists because a unit test cannot see a layout. The first version of the
// chart clipped the leading "$" off every y-axis label — the padding was
// narrower than the text — and every assertion in candidatepage_test.go passed
// while it did. Rendering the page and looking at it is the only thing that
// catches that class of defect:
//
//	PAGE_DUMP=/tmp/page.html go test ./src/backtest/ -run VisualDump
func TestCandidatePageVisualDump(t *testing.T) {
	out := os.Getenv("PAGE_DUMP")
	if out == "" {
		t.Skip("PAGE_DUMP not set; this test renders a page for human inspection")
	}
	if err := os.WriteFile(out, []byte(CandidatePage(pageFixture())), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", out)
}
