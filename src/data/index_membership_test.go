package data

import (
	"reflect"
	"testing"
	"time"
)

// `day` is defined in fundamentals_test.go.

var openEnd = day("9999-12-31")

// The real spans the 2026-08-17 reconstruction produced for four companies whose
// index exits are public knowledge. Using real ones means a boundary bug shows
// up as "the backtest held SVB after it failed" rather than as an abstract
// off-by-one.
func realSpans() []MembershipSpan {
	return []MembershipSpan{
		{Index: "SP500", Ticker: "ATVI", Start: day("2015-08-28"),
			End: day("2023-10-18")},
		{Index: "SP500", Ticker: "SIVB", Start: day("2018-03-19"),
			End: day("2023-03-15")},
		{Index: "SP500", Ticker: "TWTR", Start: day("2018-06-07"),
			End: day("2022-11-01")},
		{Index: "SP500", Ticker: "AAPL", Start: day("2007-01-01"),
			End: openEnd, StartIsHorizon: true},
	}
}

func TestMembersOn(t *testing.T) {
	spans := realSpans()

	cases := []struct {
		when string
		want []string
	}{
		// Before any of the three joined.
		{"2015-01-01", []string{"AAPL"}},
		// ATVI's first day as a member is inclusive.
		{"2015-08-28", []string{"AAPL", "ATVI"}},
		{"2015-08-27", []string{"AAPL"}},
		// All four in.
		{"2020-01-02", []string{"AAPL", "ATVI", "SIVB", "TWTR"}},
		// SVB's last day is inclusive; the day after, it is gone. A backtest
		// that starts the day after a failure must not hold it.
		{"2023-03-15", []string{"AAPL", "ATVI", "SIVB"}},
		{"2023-03-16", []string{"AAPL", "ATVI"}},
		// After every exit, only the current member is left.
		{"2026-08-17", []string{"AAPL"}},
	}

	for _, tc := range cases {
		t.Run(tc.when, func(t *testing.T) {
			got := MembersOn(spans, day(tc.when))
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("MembersOn(%s) = %v, want %v", tc.when, got, tc.want)
			}
		})
	}
}

// A ticker that leaves and rejoins has two spans, and must be absent in the
// gap between them. Common in the real index and the reason spans are a list.
func TestMembersOnHandlesRejoining(t *testing.T) {
	spans := []MembershipSpan{
		{Ticker: "RJN", Start: day("2010-01-01"), End: day("2014-06-30")},
		{Ticker: "RJN", Start: day("2019-01-01"), End: openEnd},
	}
	for _, tc := range []struct {
		when string
		in   bool
	}{
		{"2012-01-01", true},
		{"2014-06-30", true},
		{"2014-07-01", false},
		{"2018-12-31", false},
		{"2019-01-01", true},
		{"2025-01-01", true},
	} {
		got := MembersOn(spans, day(tc.when))
		isIn := len(got) == 1 && got[0] == "RJN"
		if isIn != tc.in {
			t.Errorf("on %s: member=%v, want %v", tc.when, isIn, tc.in)
		}
	}
}

func TestMembershipSpanContains(t *testing.T) {
	s := MembershipSpan{Start: day("2020-01-01"), End: day("2020-12-31")}
	for _, tc := range []struct {
		when string
		want bool
	}{
		{"2019-12-31", false},
		{"2020-01-01", true},
		{"2020-07-01", true},
		{"2020-12-31", true},
		{"2021-01-01", false},
	} {
		if got := s.Contains(day(tc.when)); got != tc.want {
			t.Errorf("Contains(%s) = %v, want %v", tc.when, got, tc.want)
		}
	}
}

// No spans is an empty universe, not a nil-map panic, and MembersOn must return
// a sorted slice so a caller can diff two dates' universes directly.
func TestMembersOnEmptyAndSorted(t *testing.T) {
	if got := MembersOn(nil, time.Now()); len(got) != 0 {
		t.Errorf("MembersOn(nil) = %v, want empty", got)
	}

	spans := []MembershipSpan{
		{Ticker: "ZZZ", Start: day("2020-01-01"), End: openEnd},
		{Ticker: "AAA", Start: day("2020-01-01"), End: openEnd},
		{Ticker: "MMM", Start: day("2020-01-01"), End: openEnd},
		// A duplicate ticker with a second overlapping span must not be
		// reported twice.
		{Ticker: "AAA", Start: day("2019-01-01"), End: openEnd},
	}
	got := MembersOn(spans, day("2021-01-01"))
	want := []string{"AAA", "MMM", "ZZZ"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("MembersOn = %v, want %v", got, want)
	}
}
