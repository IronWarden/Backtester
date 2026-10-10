package main

import "testing"

// The benchmark labels are the picker's only source for the ten $-prefixed
// series -- no SEC filing exists for a synthetic index, so company_profile
// will never carry them. If add_collections.py grows a collection and this map
// does not, that symbol shows up in the picker as a bare ticker.
func TestBenchmarkNamesCoverEveryCollection(t *testing.T) {
	// The symbols add_collections.py builds, as of 2026-08-23.
	want := []string{
		"$SP500", "$USTOT", "$SP100", "$SP400", "$SP600",
		"$RUS2K", "$EXUS", "$EM", "$WORLD", "$CASH",
	}
	for _, sym := range want {
		if benchmarkNames[sym] == "" {
			t.Errorf("%s has no label -- the picker will show it as a bare symbol", sym)
		}
	}
	if len(benchmarkNames) != len(want) {
		t.Errorf("benchmarkNames has %d entries, expected %d: a new collection needs a "+
			"label here and in add_collections.py's COLLECTIONS",
			len(benchmarkNames), len(want))
	}
}
