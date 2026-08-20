package data

import (
	"testing"
	"time"
)

// `day` is defined in fundamentals_test.go.

func TestClassifyCoverageBuckets(t *testing.T) {
	delisted := day("2020-01-01")

	cases := []struct {
		name    string
		lastBar time.Time
		want    Coverage
	}{
		{"bars stop years early", day("2015-06-30"), CoverageClean},
		{"bars stop on the delisting date", delisted, CoverageClean},
		{"bars stop at the stale-tail edge", day("2020-01-06"), CoverageClean},
		{"one day past the edge", day("2020-01-07"), CoverageStaleTail},
		{"just inside the stale tail", day("2020-03-30"), CoverageStaleTail},
		{"just inside suspect", day("2020-04-01"), CoverageSuspect},
		{"at the recycled boundary", day("2021-12-31"), CoverageSuspect},
		{"past the recycled boundary", day("2022-01-02"), CoverageRecycled},
		{"years past", day("2026-07-02"), CoverageRecycled},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyCoverage(
				DateRange{Min: day("2010-01-04"), Max: tc.lastBar}, Delisting{
					Symbol:        "TEST",
					DelistingDate: delisted,
				})
			if got.Verdict != tc.want {
				t.Errorf("last bar %s: got %q, want %q (%d days past)",
					tc.lastBar.Format("2006-01-02"), got.Verdict, tc.want,
					got.DaysPastDelisting)
			}
		})
	}
}

// The four tickers below are real findings from the 2026-08-16 registry pull,
// each corroborated against company_profile in 2026-08-20: one price series in
// stock_data_optimized carrying two genuinely different companies. If a change
// ever lets one of these read as tradable while unsplit, the engine is back to
// paying a strategy for holding a company it does not hold.
func TestClassifyCoverageCatchesRealSplicedSeries(t *testing.T) {
	spliced := []struct {
		ticker   string
		delisted string
		lastBar  string
		what     string
	}{
		{"ACI", "2020-09-15", "2026-07-02", "Arch Coal, then Albertsons"},
		{"SUNE", "2016-04-21", "2026-07-02", "SunEdison went bankrupt in 2016"},
		{"COR", "2021-12-28", "2026-07-02", "CoreSite Realty, then Cencora"},
		{"DRS", "2009-10-30", "2026-07-02", "DRS Technologies, then Leonardo DRS"},
	}

	for _, s := range spliced {
		got := ClassifyCoverage(
			DateRange{Min: day("2005-01-03"), Max: day(s.lastBar)},
			Delisting{
				Symbol:        s.ticker,
				DelistingDate: day(s.delisted),
			})
		if got.Verdict != CoverageRecycled {
			t.Errorf("%s (%s): got %q, want %q",
				s.ticker, s.what, got.Verdict, CoverageRecycled)
		}
		if got.Verdict.Trustworthy() {
			t.Errorf("%s (%s): spliced series reported as trustworthy",
				s.ticker, s.what)
		}
	}
}

func TestClassifyCoverageWithoutDelistingDate(t *testing.T) {
	got := ClassifyCoverage(
		DateRange{Min: day("1980-12-12"), Max: day("2026-07-02")},
		Delisting{Symbol: "AAPL"})
	if got.Verdict != CoverageUnknown {
		t.Errorf("no delisting date: got %q, want %q", got.Verdict, CoverageUnknown)
	}
	if !got.Verdict.Trustworthy() {
		t.Error("unknown must not be treated as untrustworthy: it is the " +
			"verdict for every living company")
	}
	if got.DaysPastDelisting != 0 {
		t.Errorf("no delisting date: DaysPastDelisting = %d, want 0",
			got.DaysPastDelisting)
	}
}

func TestTrustworthy(t *testing.T) {
	trusted := []Coverage{CoverageUnknown, CoverageClean, CoverageStaleTail}
	for _, c := range trusted {
		if !c.Trustworthy() {
			t.Errorf("%q should be trustworthy", c)
		}
	}
	for _, c := range []Coverage{CoverageSuspect, CoverageRecycled} {
		if c.Trustworthy() {
			t.Errorf("%q must not be trustworthy", c)
		}
	}
}

func TestClassifyTickersCoversEveryTicker(t *testing.T) {
	ranges := map[string]DateRange{
		"AAPL": {Min: day("1980-12-12"), Max: day("2026-07-02")},
		"DD":   {Min: day("1972-06-01"), Max: day("2026-07-02")},
		"LUX":  {Min: day("2021-01-04"), Max: day("2025-08-08")},
	}
	delistings := map[string]Delisting{
		"DD":  {Symbol: "DD", DelistingDate: day("2019-05-31")},
		"LUX": {Symbol: "LUX", DelistingDate: day("2025-08-06")},
		// A registry entry for a ticker with no price data must not invent a
		// row in the result.
		"ENRNQ": {Symbol: "ENRNQ", DelistingDate: day("2001-11-29")},
	}

	got := ClassifyTickers(ranges, delistings)

	if len(got) != len(ranges) {
		t.Fatalf("got %d verdicts, want %d (one per priced ticker)",
			len(got), len(ranges))
	}
	want := map[string]Coverage{
		"AAPL": CoverageUnknown,
		"DD":   CoverageRecycled,
		"LUX":  CoverageClean,
	}
	for ticker, w := range want {
		if got[ticker].Verdict != w {
			t.Errorf("%s: got %q, want %q", ticker, got[ticker].Verdict, w)
		}
	}
	if got["AAPL"].LastBar != day("2026-07-02") {
		t.Errorf("unknown verdict lost its last bar: %v", got["AAPL"].LastBar)
	}
}

func TestUntrustworthyTickersSorted(t *testing.T) {
	cov := map[string]TickerCoverage{
		"SUNE": {Ticker: "SUNE", Verdict: CoverageRecycled},
		"AAPL": {Ticker: "AAPL", Verdict: CoverageUnknown},
		"COR":  {Ticker: "COR", Verdict: CoverageRecycled},
		"LUX":  {Ticker: "LUX", Verdict: CoverageClean},
		"ZZZ":  {Ticker: "ZZZ", Verdict: CoverageSuspect},
		"JAMF": {Ticker: "JAMF", Verdict: CoverageStaleTail},
	}

	got := UntrustworthyTickers(cov)
	want := []string{"COR", "SUNE", "ZZZ"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v (sorted)", got, want)
		}
	}
}

// The audit that found the defect is the test that it is fixed. `DD` unsplit is
// two companies in one series and must stay condemned; `DD` and `DD.1` after
// the split are one company each and must both read clean, without the registry
// ever having heard of `DD.1`.
func TestClassifySplitPairIsClean(t *testing.T) {
	registry := map[string]Delisting{
		"DD": {Symbol: "DD", Name: "DowDuPont Inc", DelistingDate: day("2019-05-31")},
	}

	unsplit := ClassifyTickers(map[string]DateRange{
		"DD": {Min: day("1972-06-01"), Max: day("2026-07-02")},
	}, registry)
	if got := unsplit["DD"].Verdict; got != CoverageRecycled {
		t.Fatalf("unsplit DD: got %q, want %q — the regression guard for the "+
			"whole feature", got, CoverageRecycled)
	}

	split := ClassifyTickers(map[string]DateRange{
		// What is left under the live symbol begins after the death.
		"DD": {Min: day("2019-06-03"), Max: day("2026-07-02")},
		// The dead occupant ends at it.
		"DD.1": {Min: day("1972-06-01"), Max: day("2019-05-31")},
	}, registry)

	for _, ticker := range []string{"DD", "DD.1"} {
		tc := split[ticker]
		if tc.Verdict != CoverageClean {
			t.Errorf("split %s: got %q, want %q", ticker, tc.Verdict, CoverageClean)
		}
		if !tc.Verdict.Trustworthy() {
			t.Errorf("split %s: a split half must be tradable", ticker)
		}
	}
	// The generation half must be judged against its base company's death
	// date, not reported as an unknown symbol.
	if split["DD.1"].Delisting.DelistingDate.IsZero() {
		t.Error("DD.1 was classified without resolving DD's delisting date")
	}
}

func TestParseGenerationRoundTrips(t *testing.T) {
	cases := []struct {
		symbol string
		base   string
		gen    int
	}{
		{"DD.1", "DD", 1},
		{"DD.12", "DD", 12},
		{"DD", "DD", 0},
		{"$SP500", "$SP500", 0},
		// A class share is not a generation and must not be mangled into BRK.
		{"BRK.B", "BRK.B", 0},
		{"DD.", "DD.", 0},
		{".1", ".1", 0},
		{"DD.01", "DD.01", 0},
		{"DD.0", "DD.0", 0},
		{"DD.1.2", "DD.1", 2},
	}

	for _, tc := range cases {
		base, gen := ParseGeneration(tc.symbol)
		if base != tc.base || gen != tc.gen {
			t.Errorf("ParseGeneration(%q) = (%q, %d), want (%q, %d)",
				tc.symbol, base, gen, tc.base, tc.gen)
		}
		if round := GenerationSymbol(base, gen); round != tc.symbol {
			t.Errorf("round trip of %q produced %q", tc.symbol, round)
		}
	}
}

// DD used to head the list above, and it does not belong there. "DowDuPont Inc"
// is a FORMER NAME of the filer now called DuPont de Nemours -- the same CIK,
// renamed, not a symbol re-issued to somebody else. The registry records the
// rename as a delisting, so the date-based classifier still condemns the series,
// and split_recycled.py's corroboration step is the only thing that knows
// better.
//
// This test pins that gap rather than papering over it: the verdict IS recycled
// today, and a reader who assumes a recycled verdict means two companies will be
// wrong about 57 symbols. Closing it needs the corroboration fed back into the
// classifier, which is filed as its own task.
func TestRenameIsStillReportedRecycled(t *testing.T) {
	got := ClassifyCoverage(
		DateRange{Min: day("1972-06-01"), Max: day("2026-07-02")},
		Delisting{
			Symbol:        "DD",
			Name:          "DowDuPont Inc",
			DelistingDate: day("2019-05-31"),
		})
	if got.Verdict != CoverageRecycled {
		t.Fatalf("DD: got %q, want %q — if this now passes, the classifier "+
			"learned about renames and this test should become the assertion "+
			"that DD reads clean", got.Verdict, CoverageRecycled)
	}
}
