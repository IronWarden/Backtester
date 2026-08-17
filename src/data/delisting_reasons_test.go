package data

import "testing"

// `day` is defined in fundamentals_test.go.

func TestClassifyReasonPrecedence(t *testing.T) {
	cases := []struct {
		name string
		ev   ReasonEvidence
		want Reason
	}{
		{"nothing found", ReasonEvidence{}, ReasonUnknown},
		{"exchange filed only", ReasonEvidence{ExchangeFiled25: true},
			ReasonCompliance},
		{"company filed only", ReasonEvidence{CompanyFiled25: true},
			ReasonVoluntary},
		{"deregistered only", ReasonEvidence{Deregistered: true},
			ReasonVoluntary},
		{"bankruptcy report", ReasonEvidence{
			BankruptcyReport: day("2016-04-27"), ExchangeFiled25: true},
			ReasonBankruptcy},
		{"acquisition report", ReasonEvidence{
			AcquisitionReport: day("2021-12-28"), ExchangeFiled25: true},
			ReasonAcquired},

		// The case that decides the precedence, and it is a real one: SVB
		// Financial filed item 2.01 on 2023-03-14 and item 1.03 on 2023-03-17.
		// Reading that as an acquisition would credit a holder who was wiped
		// out, so bankruptcy has to win.
		{"both reports, bankruptcy wins", ReasonEvidence{
			BankruptcyReport:  day("2023-03-17"),
			AcquisitionReport: day("2023-03-14"),
			CompanyFiled25:    true},
			ReasonBankruptcy},

		// A company that left of its own accord is voluntary even though the
		// exchange also filed: the company's own action is the more specific
		// signal, and calling it a compliance failure would be wrong.
		{"company and exchange both filed", ReasonEvidence{
			CompanyFiled25: true, ExchangeFiled25: true},
			ReasonVoluntary},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyReason(tc.ev); got != tc.want {
				t.Errorf("ClassifyReason(%+v) = %q, want %q", tc.ev, got, tc.want)
			}
		})
	}
}

// Only a bankruptcy destroys the position. A compliance delisting leaves the
// stock trading over the counter, and both acquisition routes pay something —
// so treating them alike would apply a -100% to a holder who was paid a premium.
func TestReasonTerminalForHolders(t *testing.T) {
	if !ReasonBankruptcy.TerminalForHolders() {
		t.Error("bankruptcy must be terminal for holders")
	}
	for _, r := range []Reason{ReasonAcquired, ReasonCompliance,
		ReasonVoluntary, ReasonUnknown} {
		if r.TerminalForHolders() {
			t.Errorf("%q must not be treated as terminal", r)
		}
	}
}

// The four real classifications from the 2026-08-17 verification run. If a
// change ever flips one of these, the reason column has stopped meaning what
// the README says it means.
func TestClassifyReasonRealCases(t *testing.T) {
	real := []struct {
		who  string
		ev   ReasonEvidence
		want Reason
	}{
		{"SunEdison (CIK 945436)", ReasonEvidence{
			BankruptcyReport: day("2016-04-27"), ExchangeFiled25: true},
			ReasonBankruptcy},
		{"CoreSite Realty (1490892)", ReasonEvidence{
			AcquisitionReport: day("2021-12-28"), ExchangeFiled25: true},
			ReasonAcquired},
		{"Activision Blizzard (718877)", ReasonEvidence{
			AcquisitionReport: day("2023-10-13"), ExchangeFiled25: true,
			Deregistered: true},
			ReasonAcquired},
		{"SVB Financial (719739)", ReasonEvidence{
			BankruptcyReport:  day("2023-03-17"),
			AcquisitionReport: day("2023-03-14"), CompanyFiled25: true},
			ReasonBankruptcy},
	}
	for _, tc := range real {
		t.Run(tc.who, func(t *testing.T) {
			if got := ClassifyReason(tc.ev); got != tc.want {
				t.Errorf("%s: got %q, want %q", tc.who, got, tc.want)
			}
		})
	}
}

func TestReasonsBySymbol(t *testing.T) {
	reasons := map[string]DelistingReason{
		"0000945436": {CIK: "0000945436", Tickers: nil,
			Reason: ReasonBankruptcy},
		"0000718877": {CIK: "0000718877", Tickers: []string{"ATVI"},
			Reason: ReasonAcquired},
		"0000000002": {CIK: "0000000002", Tickers: []string{"AAA", "AAA.B"},
			Reason: ReasonCompliance},
	}
	got := ReasonsBySymbol(reasons)

	// Three symbols across two filers; the tickerless filer contributes none,
	// which is the normal case for a delisted company and must not become an
	// empty-string key.
	if len(got) != 3 {
		t.Fatalf("re-keyed %d symbols, want 3: %v", len(got), got)
	}
	if _, ok := got[""]; ok {
		t.Error("a filer with no tickers produced an empty-string key")
	}
	if got["ATVI"].Reason != ReasonAcquired {
		t.Errorf("ATVI = %q, want %q", got["ATVI"].Reason, ReasonAcquired)
	}
	if got["AAA.B"].CIK != "0000000002" {
		t.Errorf("AAA.B mapped to CIK %q", got["AAA.B"].CIK)
	}
}
