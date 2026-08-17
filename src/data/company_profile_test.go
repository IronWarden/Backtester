package data

import (
	"reflect"
	"testing"
)

// The division boundaries are the whole of the classification, and an off-by-one
// at an edge silently moves an entire industry into the wrong sector. Every case
// here is an edge or a gap, plus the real codes of companies in the price table
// so the mapping is checked against something recognisable rather than only
// against itself.
func TestSectorForSIC(t *testing.T) {
	cases := []struct {
		sic  string
		want string
		note string
	}{
		// Division edges, low and high.
		{"0100", "Agriculture, Forestry & Fishing", "first agriculture code"},
		{"0999", "Agriculture, Forestry & Fishing", "last agriculture code"},
		{"1000", "Mining", "first mining code"},
		{"1499", "Mining", "last mining code"},
		{"1500", "Construction", "first construction code"},
		{"1799", "Construction", "last construction code"},
		{"2000", "Manufacturing", "first manufacturing code"},
		{"3999", "Manufacturing", "last manufacturing code"},
		{"4000", "Transportation & Public Utilities", "first transport code"},
		{"4999", "Transportation & Public Utilities", "last transport code"},
		{"5000", "Wholesale Trade", "wholesale starts"},
		{"5199", "Wholesale Trade", "wholesale ends"},
		{"5200", "Retail Trade", "retail starts one past wholesale"},
		{"5999", "Retail Trade", "retail ends"},
		{"6000", "Finance, Insurance & Real Estate", "finance starts"},
		{"6799", "Finance, Insurance & Real Estate", "finance ends"},
		{"7000", "Services", "services starts"},
		{"8999", "Services", "services ends"},
		{"9100", "Public Administration", "public administration starts"},
		{"9729", "Public Administration", "public administration ends"},

		// The gaps between divisions are real gaps in the SEC's scheme.
		{"1800", SectorUnclassified, "between construction and manufacturing"},
		{"6900", SectorUnclassified, "between finance and services"},
		{"9000", SectorUnclassified, "between services and public admin"},
		{"9995", SectorUnclassified, "nonclassifiable, past every division"},

		// Absent or unusable values.
		{"", SectorUnclassified, "no code at all"},
		{"0", SectorUnclassified, "zero is not a code"},
		{"n/a", SectorUnclassified, "unparseable"},

		// Real codes, verified against EDGAR on 2026-08-17.
		{"3571", "Manufacturing", "AAPL, Electronic Computers"},
		{"7372", "Services", "MSFT, Prepackaged Software"},
		{"6022", "Finance, Insurance & Real Estate", "state commercial banks"},
		{"5812", "Retail Trade", "eating places"},
	}

	for _, tc := range cases {
		t.Run(tc.sic+" "+tc.note, func(t *testing.T) {
			if got := SectorForSIC(tc.sic); got != tc.want {
				t.Errorf("SectorForSIC(%q) = %q, want %q (%s)",
					tc.sic, got, tc.want, tc.note)
			}
		})
	}
}

// No division may overlap another, or a code's sector would depend on the order
// the table happens to be written in. Checked as a property rather than a case,
// because the failure mode appears when someone adds a division later.
func TestSICDivisionsDoNotOverlap(t *testing.T) {
	for i, a := range sicDivisions {
		if a.Low > a.High {
			t.Errorf("division %q is inverted: %d-%d", a.Sector, a.Low, a.High)
		}
		for j, b := range sicDivisions {
			if i >= j {
				continue
			}
			if a.Low <= b.High && b.Low <= a.High {
				t.Errorf("divisions %q (%d-%d) and %q (%d-%d) overlap",
					a.Sector, a.Low, a.High, b.Sector, b.Low, b.High)
			}
		}
	}
}

// The rename trail is context, and losing it must never cost the caller the
// rest of the profile — hence a parse failure that degrades to no names rather
// than to an error.
func TestParseFormerNames(t *testing.T) {
	cases := []struct {
		raw  string
		want []string
	}{
		{`["DowDuPont Inc.","E I Du Pont De Nemours & Co"]`,
			[]string{"DowDuPont Inc.", "E I Du Pont De Nemours & Co"}},
		{`[]`, []string{}},
		{"", nil},
		{"not json", nil},
		{`{"name":"x"}`, nil},
	}
	for _, tc := range cases {
		got := parseFormerNames(tc.raw)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("parseFormerNames(%q) = %#v, want %#v", tc.raw, got, tc.want)
		}
	}
}
