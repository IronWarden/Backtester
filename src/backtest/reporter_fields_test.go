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
		reflect.TypeOf(TradeStats{}),
	} {
		for i := range typ.NumField() {
			names = append(names, typ.Field(i).Name)
		}
	}
	return names
}

// textualFields and boolFields are the exceptions to "everything is a
// float64". Listing them explicitly means adding a field of either kind is a
// deliberate act rather than something that quietly slips past the type check
// below.
var textualFields = map[string]bool{"PortfolioName": true, "Strategy": true}

var boolFields = map[string]bool{
	"BeatBaseline":         true,
	"BaselineComputed":     true,
	"SignificanceComputed": true,
}

// Every numeric field must come back as a float64: evalBinary compares
// float64s and lessByField sorts them, so an int silently breaks both. Trials
// was an int, which made `filter = "Trials > 5"` fail with a type error and
// take the whole output file down with it.
func TestEveryNumericFieldIsFloat64(t *testing.T) {
	for _, name := range resultFields {
		v, ok := resultValue(Result{}, name)
		if !ok {
			continue // reported by the sync test above
		}
		switch {
		case textualFields[name]:
			if _, isString := v.(string); !isString {
				t.Errorf("%s = %T, want string", name, v)
			}
		case boolFields[name]:
			if _, isBool := v.(bool); !isBool {
				t.Errorf("%s = %T, want bool", name, v)
			}
		default:
			if _, isFloat := v.(float64); !isFloat {
				t.Errorf("%s = %T, want float64 — comparisons and sorting "+
					"both require float64", name, v)
			}
		}
	}
}

// The end-to-end version of the same bug: a filter over any advertised numeric
// field has to evaluate, not error. Write wraps an eval failure as an error, so
// one bad field takes down the whole output file rather than one row.
func TestEveryNumericFieldIsFilterable(t *testing.T) {
	for _, name := range resultFields {
		if textualFields[name] {
			continue
		}
		// A bool field is a filter on its own; a number needs a comparison.
		src := name + " > 0"
		if boolFields[name] {
			src = name
		}
		expr, err := parser.ParseExpr(src)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := evalFilter(expr, Result{Trials: 3}); err != nil {
			t.Errorf("filter %q failed: %v", src, err)
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

// Resolving a name is not the same as returning the right number. This pins
// the blotter fields to the struct they read, so a copy-paste slip in the
// switch — the easiest mistake to make in a thirty-case mapping — shows up.
func TestBlotterFieldsReadTheBlotter(t *testing.T) {
	res := Result{TradeStats: TradeStats{
		Trades: 12, Buys: 7, Sells: 5, RoundTrips: 5,
		Wins: 3, Losses: 2, WinRate: 0.6,
		AvgWin: 210.5, AvgLoss: 90.25, ProfitFactor: 3.5,
		TotalRealized: 431.0, TotalFees: 18.75, AvgHoldingDays: 41.5,
	}}
	want := map[string]float64{
		"Trades": 12, "Buys": 7, "Sells": 5, "RoundTrips": 5,
		"Wins": 3, "Losses": 2, "WinRate": 0.6,
		"AvgWin": 210.5, "AvgLoss": 90.25, "ProfitFactor": 3.5,
		"TotalRealized": 431.0, "TotalFees": 18.75, "AvgHoldingDays": 41.5,
	}
	for name, expect := range want {
		v, ok := resultValue(res, name)
		if !ok {
			t.Errorf("%s does not resolve", name)
			continue
		}
		if got, isFloat := v.(float64); !isFloat || got != expect {
			t.Errorf("%s = %v (%T), want %v", name, v, v, expect)
		}
	}
}

// The reason this is worth having at all: a swept config produces candidates
// that never traded, and they do not look like failures — they report a flat
// curve, not an error, so a filter written in terms of returns keeps them.
// "Trades > 0" is the one that throws them out.
func TestFilteringOutStrategiesThatNeverTraded(t *testing.T) {
	expr, err := parser.ParseExpr("Trades > 0 && ProfitFactor > 1.5")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		res  Result
		want bool
	}{
		{"traded and profitable",
			Result{TradeStats: TradeStats{Trades: 8, ProfitFactor: 2.0}}, true},
		{"never traded",
			Result{TradeStats: TradeStats{}}, false},
		{"traded but not profitable enough",
			Result{TradeStats: TradeStats{Trades: 8, ProfitFactor: 1.1}}, false},
	}
	for _, tc := range cases {
		got, err := evalFilter(expr, tc.res)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: kept = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The Computed flags are not bookkeeping — without them the figures they
// guard lie by omission. An uncomputed Significance reports PValue 0, which
// passes "PValue < 0.05" and reads as maximally significant when the truth is
// that nothing was tested. The same shape applies to Baseline: uncomputed
// means Beat false and Excess 0, which reads as "tied with buy-and-hold".
//
// Nothing can stop a user writing the naive filter, so this pins the trap in
// place: the naive form keeps the untested result, and the guarded form does
// not. If a change ever makes the naive filter safe, this test says so.
func TestUncomputedSignificanceLooksSignificant(t *testing.T) {
	untested := Result{} // Significance.Computed false, PValue 0

	naive, err := parser.ParseExpr("PValue < 0.05")
	if err != nil {
		t.Fatal(err)
	}
	kept, err := evalFilter(naive, untested)
	if err != nil {
		t.Fatal(err)
	}
	if !kept {
		t.Error("the trap is gone: an untested result no longer passes " +
			"PValue < 0.05 — update the docs, this is now safe")
	}

	guarded, err := parser.ParseExpr("SignificanceComputed && PValue < 0.05")
	if err != nil {
		t.Fatal(err)
	}
	kept, err = evalFilter(guarded, untested)
	if err != nil {
		t.Fatal(err)
	}
	if kept {
		t.Error("guarding with SignificanceComputed did not exclude an " +
			"untested result, which is the only reason the flag is addressable")
	}

	// And the guard must not throw away a real one.
	real := Result{Significance: Significance{Computed: true, PValue: 0.01}}
	if kept, err = evalFilter(guarded, real); err != nil || !kept {
		t.Errorf("the guarded filter dropped a genuinely significant "+
			"result: kept=%v err=%v", kept, err)
	}
}

// The same trap on the baseline side, plus the thing a user actually wants to
// write: did it beat buy-and-hold, and did it trade to get there.
func TestBeatBaselineFilter(t *testing.T) {
	expr, err := parser.ParseExpr("BaselineComputed && BeatBaseline && Trades > 0")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		res  Result
		want bool
	}{
		{"beat it, having traded", Result{
			Baseline:   BaselineStats{Computed: true, Beat: true},
			TradeStats: TradeStats{Trades: 4},
		}, true},
		{"no baseline was built", Result{
			Baseline:   BaselineStats{Computed: false, Beat: false},
			TradeStats: TradeStats{Trades: 4},
		}, false},
		{"beat it without trading", Result{
			Baseline:   BaselineStats{Computed: true, Beat: true},
			TradeStats: TradeStats{Trades: 0},
		}, false},
	}
	for _, tc := range cases {
		got, err := evalFilter(expr, tc.res)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: kept = %v, want %v", tc.name, got, tc.want)
		}
	}
}
