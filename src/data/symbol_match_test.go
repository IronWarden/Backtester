package data

import (
	"testing"
)

func TestNormalizeCompanyName(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// The two real pairs this file exists to reconcile.
		{"SUNEDISON, INC.", "SUNEDISON INC"},
		{"SunEdison Inc", "SUNEDISON INC"},
		{"CoreSite Realty Corp", "CORESITE REALTY CORP"},
		{"CoreSite Realty Corporation", "CORESITE REALTY CORPORATION"},

		{"  Activision   Blizzard, Inc.  ", "ACTIVISION BLIZZARD INC"},
		{"The Kroger Co.", "KROGER CO"},
		// A bare "The" is the whole name; stripping it would leave nothing.
		{"The", "THE"},
		{"Barnes & Noble", "BARNES AND NOBLE"},
		{"Barnes and Noble", "BARNES AND NOBLE"},
		// Punctuation separates rather than vanishing.
		{"SUNEDISON,INC", "SUNEDISON INC"},
		{"", ""},
	}
	for _, c := range cases {
		if got := NormalizeCompanyName(c.in); got != c.want {
			t.Errorf("NormalizeCompanyName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestStripSuffixesReducesVariantsAlike(t *testing.T) {
	same := [][2]string{
		{"FOO HOLDINGS INC", "FOO INC"},
		{"CORESITE REALTY CORP", "CORESITE REALTY CORPORATION"},
		{"ACME CO", "ACME COMPANY LIMITED"},
	}
	for _, p := range same {
		a, b := stripSuffixes(p[0]), stripSuffixes(p[1])
		if a != b {
			t.Errorf("stripSuffixes(%q)=%q != stripSuffixes(%q)=%q", p[0], a, p[1], b)
		}
	}
	// Never strips the last token: a filer named only by a suffix keeps a name.
	if got := stripSuffixes("HOLDINGS INC"); got != "HOLDINGS" {
		t.Errorf("stripSuffixes(HOLDINGS INC) = %q, want HOLDINGS", got)
	}
	// Distinguishing words are not suffixes.
	if got := stripSuffixes("FIRST REPUBLIC BANCORP"); got != "FIRST REPUBLIC BANCORP" {
		t.Errorf("stripped a distinguishing token: %q", got)
	}
}

func TestNameSimilarityEmptyIsNotAgreement(t *testing.T) {
	if s := NameSimilarity("", ""); s != 0 {
		t.Errorf("two empty names scored %v, want 0 — absence is not agreement", s)
	}
	if s := NameSimilarity("SunEdison Inc", ""); s != 0 {
		t.Errorf("empty against a real name scored %v, want 0", s)
	}
	if s := NameSimilarity("SUNEDISON, INC.", "SunEdison Inc"); s != 1 {
		t.Errorf("spelling variants scored %v, want 1", s)
	}
}

// The four companies T42 verified against public knowledge, as they actually
// appear on each side of the join.
func verifiedFixtures() ([]Delisting, []DelistingReason) {
	registry := []Delisting{
		{Symbol: "SUNE", Name: "SunEdison Inc", DelistingDate: day("2016-05-06")},
		{Symbol: "COR", Name: "CoreSite Realty Corp", DelistingDate: day("2021-12-28")},
		{Symbol: "ATVI", Name: "Activision Blizzard Inc", DelistingDate: day("2023-10-13")},
		{Symbol: "SIVB", Name: "SVB Financial Group", DelistingDate: day("2023-03-15")},
	}
	reasons := []DelistingReason{
		{CIK: "0000945436", CompanyName: "SUNEDISON, INC.", DelistingDate: day("2016-05-06"),
			Evidence: ReasonEvidence{BankruptcyReport: day("2016-04-27"), ExchangeFiled25: true}},
		{CIK: "0001490892", CompanyName: "CoreSite Realty Corporation", DelistingDate: day("2021-12-28"),
			Evidence: ReasonEvidence{AcquisitionReport: day("2021-12-28"), ExchangeFiled25: true}},
		{CIK: "0000718877", CompanyName: "Activision Blizzard, Inc.", DelistingDate: day("2023-10-13"),
			Evidence: ReasonEvidence{AcquisitionReport: day("2023-10-13"), ExchangeFiled25: true}},
		{CIK: "0000719739", CompanyName: "SVB FINANCIAL GROUP", DelistingDate: day("2023-03-15"),
			Evidence: ReasonEvidence{
				AcquisitionReport: day("2023-03-14"),
				BankruptcyReport:  day("2023-03-17"),
				ExchangeFiled25:   true,
			}},
	}
	return registry, reasons
}

func TestMatchDelistingSymbolsOnVerifiedCompanies(t *testing.T) {
	registry, reasons := verifiedFixtures()
	got := MatchDelistingSymbols(registry, reasons)

	want := map[string]struct {
		cik    string
		method MatchMethod
	}{
		"SUNE": {"0000945436", MatchExact},  // SUNEDISON, INC. == SunEdison Inc
		"COR":  {"0001490892", MatchSuffix}, // Corp vs Corporation
		"ATVI": {"0000718877", MatchExact},
		"SIVB": {"0000719739", MatchExact}, // GROUP is a suffix but both carry it
	}
	for symbol, w := range want {
		m, ok := got[symbol]
		if !ok {
			t.Fatalf("%s missing from the result", symbol)
		}
		if m.MatchedBy != w.method {
			t.Errorf("%s matched by %q, want %q", symbol, m.MatchedBy, w.method)
		}
		if m.CIK != w.cik {
			t.Errorf("%s -> CIK %q, want %q", symbol, m.CIK, w.cik)
		}
		if !m.MatchedBy.Resolved() {
			t.Errorf("%s did not resolve", symbol)
		}
	}
}

// The payoff, end to end: the symbol a backtest held now carries the reason,
// including SVB's both-filings case where bankruptcy has to win.
func TestReasonsByMatchedSymbolCarriesTheReason(t *testing.T) {
	registry, reasons := verifiedFixtures()
	for i := range reasons {
		reasons[i].Reason = ClassifyReason(reasons[i].Evidence)
	}
	bySymbol := ReasonsByMatchedSymbol(MatchDelistingSymbols(registry, reasons), reasons)

	want := map[string]Reason{
		"SUNE": ReasonBankruptcy,
		"COR":  ReasonAcquired,
		"ATVI": ReasonAcquired,
		"SIVB": ReasonBankruptcy, // filed 2.01 and 1.03; the holder was wiped out
	}
	for symbol, w := range want {
		r, ok := bySymbol[symbol]
		if !ok {
			t.Fatalf("%s did not survive the join", symbol)
		}
		if r.Reason != w {
			t.Errorf("%s -> %q, want %q", symbol, r.Reason, w)
		}
	}
	if got := bySymbol["SIVB"].Reason.TerminalForHolders(); !got {
		t.Error("SIVB should be terminal for holders")
	}
	if got := bySymbol["COR"].Reason.TerminalForHolders(); got {
		t.Error("an acquisition is not terminal for holders")
	}
}

func TestDateGateRejectsANameMatchAtTheWrongTime(t *testing.T) {
	registry := []Delisting{
		{Symbol: "SUNE", Name: "SunEdison Inc", DelistingDate: day("2016-05-06")},
	}
	// Same company name, delisting a year later: a different event.
	reasons := []DelistingReason{
		{CIK: "0000945436", CompanyName: "SUNEDISON, INC.", DelistingDate: day("2017-05-06")},
	}
	m := MatchDelistingSymbols(registry, reasons)["SUNE"]
	if m.MatchedBy != MatchNone {
		t.Errorf("matched across a 365-day gap as %q; the date gate should have refused", m.MatchedBy)
	}
	if m.CIK != "" {
		t.Errorf("unmatched result carries CIK %q", m.CIK)
	}

	// Just inside the gate still matches, and records the gap as evidence.
	reasons[0].DelistingDate = day("2016-05-26") // 20 days
	m = MatchDelistingSymbols(registry, reasons)["SUNE"]
	if m.MatchedBy != MatchExact {
		t.Errorf("a 20-day gap was refused: %q", m.MatchedBy)
	}
	if m.DateGapDays != 20 {
		t.Errorf("DateGapDays = %d, want 20", m.DateGapDays)
	}
}

func TestAmbiguityIsRecordedNotResolved(t *testing.T) {
	registry := []Delisting{
		{Symbol: "ACME", Name: "Acme Corp", DelistingDate: day("2020-01-15")},
	}
	// Two distinct filers reducing to the same name AT THE SAME TIER, delisting
	// together. They must tie: a tie broken by tier is not ambiguity, which is
	// what TestAmbiguousStrongTierDoesNotFallThrough covers.
	reasons := []DelistingReason{
		{CIK: "0000000111", CompanyName: "Acme Corp", DelistingDate: day("2020-01-15")},
		{CIK: "0000000222", CompanyName: "ACME CORP.", DelistingDate: day("2020-01-16")},
	}
	m := MatchDelistingSymbols(registry, reasons)["ACME"]
	if m.MatchedBy != MatchAmbiguous {
		t.Fatalf("MatchedBy = %q, want ambiguous", m.MatchedBy)
	}
	if m.CIK != "" {
		t.Errorf("an ambiguous match picked CIK %q; it must not choose", m.CIK)
	}
	if len(m.Candidates) != 2 {
		t.Errorf("Candidates = %v, want both contenders recorded", m.Candidates)
	}
	if m.MatchedBy.Resolved() {
		t.Error("ambiguous must not report Resolved")
	}
	// And it must not leak into the payoff map.
	if _, ok := ReasonsByMatchedSymbol(map[string]SymbolMatch{"ACME": m}, reasons)["ACME"]; ok {
		t.Error("an ambiguous symbol reached ReasonsByMatchedSymbol")
	}
}

// A strong tier that ties is ambiguous outright; it must not fall through to a
// weaker tier, which would manufacture confidence from a genuine conflict.
func TestAmbiguousStrongTierDoesNotFallThrough(t *testing.T) {
	registry := []Delisting{
		{Symbol: "ACME", Name: "Acme Corp", DelistingDate: day("2020-01-15")},
	}
	reasons := []DelistingReason{
		{CIK: "0000000111", CompanyName: "Acme Corp", DelistingDate: day("2020-01-15")},
		{CIK: "0000000222", CompanyName: "ACME CORP", DelistingDate: day("2020-01-15")},
		// A lone suffix-tier candidate that would win if the tie fell through.
		{CIK: "0000000333", CompanyName: "Acme Corporation", DelistingDate: day("2020-01-15")},
	}
	m := MatchDelistingSymbols(registry, reasons)["ACME"]
	if m.MatchedBy != MatchAmbiguous {
		t.Errorf("MatchedBy = %q, want ambiguous", m.MatchedBy)
	}
	if m.CIK == "0000000333" {
		t.Error("fell through an ambiguous exact tier into the suffix tier")
	}
}

func TestUnmatchableInputIsReportedNotDropped(t *testing.T) {
	registry := []Delisting{
		{Symbol: "AAA", Name: "Wholly Unrelated Industries", DelistingDate: day("2020-01-15")},
		{Symbol: "BBB", Name: "", DelistingDate: day("2020-01-15")},          // no name
		{Symbol: "CCC", Name: "Acme Corp"},                                   // no date
		{Symbol: "DDD", Name: "Acme Corp", DelistingDate: day("2020-01-15")}, // filer has no date
	}
	reasons := []DelistingReason{
		{CIK: "0000000111", CompanyName: "Acme Corp"}, // zero delisting date
	}
	got := MatchDelistingSymbols(registry, reasons)
	if len(got) != len(registry) {
		t.Fatalf("got %d results for %d registry rows; every symbol must be reported", len(got), len(registry))
	}
	for _, s := range []string{"AAA", "BBB", "CCC", "DDD"} {
		if got[s].MatchedBy != MatchNone {
			t.Errorf("%s = %q, want none", s, got[s].MatchedBy)
		}
		if got[s].Symbol != s {
			t.Errorf("result for %s lost its symbol: %q", s, got[s].Symbol)
		}
	}
}

func TestFuzzyTierMatchesSpellingDriftOnly(t *testing.T) {
	registry := []Delisting{
		{Symbol: "XYZ", Name: "Transocean Sedco Forex Inc", DelistingDate: day("2020-06-01")},
	}
	reasons := []DelistingReason{
		// One character of drift: above the floor.
		{CIK: "0000000111", CompanyName: "Transocean Sedco Forex, Inc", DelistingDate: day("2020-06-01")},
		// A different company in the same industry: well below it.
		{CIK: "0000000222", CompanyName: "Transocean Partners LLC", DelistingDate: day("2020-06-01")},
	}
	m := MatchDelistingSymbols(registry, reasons)["XYZ"]
	if m.CIK != "0000000111" {
		t.Errorf("CIK = %q, want the spelling variant 0000000111", m.CIK)
	}
	if m.MatchedBy == MatchAmbiguous {
		t.Error("a different company was treated as a tie")
	}
	if NameSimilarity("Transocean Sedco Forex Inc", "Transocean Partners LLC") >= MinSimilarity {
		t.Error("MinSimilarity is too loose: it admits a different company")
	}
}
