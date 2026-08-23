package main

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"my-backtester/src/backtest"
)

// Fixtures for the frontend smoke render, generated from the Go structs
// rather than written by hand.
//
// A hand-written fixture drifts from the `json` tags the moment a field is
// renamed, and it drifts silently: the test keeps passing against a shape the
// app never sends. Every field here comes from reflection over the real
// RunOutput, so a renamed tag changes the fixture, the fixture no longer
// matches what is checked in, and this test says so.
//
// Regenerate with:  go test ./ui -run TestResultFixtures -update

const fixtureDir = "frontend/testdata"

var updateFixtures = flag.Bool("update", false,
	"rewrite the frontend render fixtures from the current Go structs")

// fillValue populates every exported field of v with a plausible non-zero
// value, so the fixture exercises every branch that asks "is this set?".
func fillValue(v reflect.Value, depth int) {
	switch v.Kind() {
	case reflect.Struct:
		if v.Type() == reflect.TypeOf(time.Time{}) {
			v.Set(reflect.ValueOf(time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)))
			return
		}
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).PkgPath == "" {
				fillValue(v.Field(i), depth+1)
			}
		}
	case reflect.Ptr:
		if depth > 4 {
			return
		}
		p := reflect.New(v.Type().Elem())
		fillValue(p.Elem(), depth+1)
		v.Set(p)
	case reflect.Slice:
		if depth > 4 {
			return
		}
		s := reflect.MakeSlice(v.Type(), 2, 2)
		for i := 0; i < 2; i++ {
			fillValue(s.Index(i), depth+1)
		}
		v.Set(s)
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
	case reflect.String:
		v.SetString("2021-03-04")
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1.25)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(7)
	case reflect.Bool:
		v.SetBool(true)
	}
}

// fixtures returns the shapes the results view has to survive. The degenerate
// ones matter more than the full one: a run with no trades, no walk-forward
// and no benchmark is the ordinary case, and every one of those fields arrives
// as JSON null rather than an empty array.
func fixtures() map[string]RunOutput {
	var full RunOutput
	fillValue(reflect.ValueOf(&full).Elem(), 0)

	plain := RunOutput{Results: []RunResult{full.Results[0]}}
	p := &plain.Results[0]
	p.Drawdowns = nil
	p.RollingSharpe = nil
	p.YearlyReturns = nil
	p.MonthlyReturns = nil
	p.BenchmarkCurve = nil
	p.Splits = nil
	p.WalkForward = nil
	p.TradeStats = backtest.TradeStats{}
	p.BenchmarkStats = backtest.BenchmarkStats{}

	noCurve := RunOutput{Results: []RunResult{plain.Results[0]}}
	noCurve.Results[0].EquityCurve = nil
	noCurve.Results[0].Dates = nil

	return map[string]RunOutput{
		"full":     full,    // every panel populated, two portfolios, verdicts
		"plain":    plain,   // one portfolio, every optional section nil
		"no-curve": noCurve, // nothing plottable at all
		"empty":    {},      // the pre-run state
	}
}

func TestResultFixturesMatchTheWireShape(t *testing.T) {
	if err := os.MkdirAll(fixtureDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, out := range fixtures() {
		want, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, '\n')
		path := filepath.Join(fixtureDir, name+".json")
		if *updateFixtures {
			if err := os.WriteFile(path, want, 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v -- regenerate with `go test ./ui -run TestResultFixtures -update`", path, err)
		}
		if string(got) != string(want) {
			t.Errorf("%s is stale: the Go wire shape changed but the fixture the "+
				"frontend renders against did not. The smoke render is now testing a "+
				"shape the app never sends. Regenerate with "+
				"`go test ./ui -run TestResultFixtures -update`, then re-run the smoke "+
				"render -- if it breaks, the frontend needs updating for the new shape.", path)
		}
	}
}
