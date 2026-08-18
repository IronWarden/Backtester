package backtest

import (
	"go/parser"
	"reflect"
	"strings"
	"testing"
)

// resultFields is only useful if it agrees with resultValue, which is the
// authority. It had already drifted once — missing Trials, ExpectedMaxSharpe
// and DeflatedSharpe — while nothing read it, so nothing noticed.
func TestResultFieldsMatchResultValue(t *testing.T) {
	for _, name := range resultFields {
		if _, ok := resultValue(Result{}, name); !ok {
			t.Errorf("resultFields lists %q, which resultValue cannot resolve",
				name)
		}
	}

	// The other direction: anything resolvable must be advertised, or a user
	// reading the error message is told the field does not exist when it does.
	for _, name := range knownButUnlisted(t) {
		t.Errorf("resultValue resolves %q but resultFields omits it, so the "+
			"error message hides a field that works", name)
	}
}

// knownButUnlisted finds fields resultValue resolves that resultFields does
// not advertise. resultValue is a switch, so the names have to come from
// somewhere — the metric and result structs are the source of truth for what
// a user would plausibly try.
func knownButUnlisted(t *testing.T) []string {
	t.Helper()
	listed := make(map[string]bool, len(resultFields))
	for _, f := range resultFields {
		listed[f] = true
	}
	var missing []string
	for _, name := range candidateFieldNames() {
		if listed[name] {
			continue
		}
		if _, ok := resultValue(Result{}, name); ok {
			missing = append(missing, name)
		}
	}
	return missing
}

// candidateFieldNames is every name a user could plausibly write: the fields
// of Result and of the Metrics it embeds. resultValue is a switch, so there is
// no way to enumerate it directly — this approximates it from the structs the
// switch reads, which is where a new field would be added first.
func candidateFieldNames() []string {
	var names []string
	for _, typ := range []reflect.Type{
		reflect.TypeOf(Result{}),
		reflect.TypeOf(Metrics{}),
	} {
		for i := range typ.NumField() {
			names = append(names, typ.Field(i).Name)
		}
	}
	return names
}

// Every numeric field must come back as a float64: evalBinary compares
// float64s and lessByField sorts them, so an int silently breaks both. Trials
// was an int, which made `filter = "Trials > 5"` fail with a type error and
// take the whole output file down with it.
func TestEveryNumericFieldIsFloat64(t *testing.T) {
	textual := map[string]bool{"PortfolioName": true, "Strategy": true}
	for _, name := range resultFields {
		v, ok := resultValue(Result{}, name)
		if !ok {
			continue // reported by the sync test above
		}
		if textual[name] {
			if _, isString := v.(string); !isString {
				t.Errorf("%s = %T, want string", name, v)
			}
			continue
		}
		if _, isFloat := v.(float64); !isFloat {
			t.Errorf("%s = %T, want float64 — comparisons and sorting both "+
				"require float64", name, v)
		}
	}
}

// The end-to-end version of the same bug: a filter over any advertised numeric
// field has to evaluate, not error. Write wraps an eval failure as an error, so
// one bad field takes down the whole output file rather than one row.
func TestEveryNumericFieldIsFilterable(t *testing.T) {
	for _, name := range resultFields {
		if name == "PortfolioName" || name == "Strategy" {
			continue
		}
		expr, err := parser.ParseExpr(name + " > 0")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := evalFilter(expr, Result{Trials: 3}); err != nil {
			t.Errorf("filter %q > 0 failed: %v", name, err)
		}
	}
}

// A typo should say what the user could have meant. Naming the alternatives is
// the whole reason resultFields is no longer dead.
func TestUnknownFieldErrorNamesTheAlternatives(t *testing.T) {
	cases := []struct {
		name string
		cfg  OutputConfig
	}{
		{"fields", OutputConfig{Path: "x.txt", Fields: []string{"Sharp"}}},
		{"sort_by", OutputConfig{Path: "x.txt", SortBy: "Sharp"}},
		{"filter", OutputConfig{Path: "x.txt", Filter: "Sharp > 1"}},
	}
	for _, tc := range cases {
		_, err := NewReporter(&tc.cfg)
		if err == nil {
			t.Errorf("%s: a typo'd field name was accepted", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), "SharpeRatio") {
			t.Errorf("%s: error does not name the field they meant: %v",
				tc.name, err)
		}
	}
}
