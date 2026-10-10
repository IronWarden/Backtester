package main

import (
	"os"
	"os/exec"
	"testing"
)

// Runs the frontend smoke render as part of the Go suite, so `go test ./...`
// stays the one command the gate needs and nobody has to remember a second
// one. The npm script is `cd ui/frontend && npm run smoke` if you want it on
// its own.
//
// Skips rather than fails when the frontend is not installed: a checkout that
// has never run `npm install` should not report a red Go suite for it.
func TestFrontendSmokeRender(t *testing.T) {
	if testing.Short() {
		t.Skip("smoke render bundles the frontend; skipped under -short")
	}
	if _, err := os.Stat("frontend/node_modules"); err != nil {
		t.Skip("frontend/node_modules missing -- run `npm install` in ui/frontend")
	}
	if _, err := exec.LookPath("npm"); err != nil {
		t.Skip("npm not on PATH")
	}

	cmd := exec.Command("npm", "run", "--silent", "smoke")
	cmd.Dir = "frontend"
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("frontend smoke render failed: %v\n\n%s\n\n"+
			"A component threw while rendering, or a generated config changed. "+
			"This is the check that runs frontend code; `tsc` and `vite build` "+
			"only compile it.", err, out)
	}
	t.Logf("\n%s", out)
}
