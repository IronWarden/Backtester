package data

// A rename is not a recycling, and the delisting registry cannot tell them
// apart.
//
// Alpha Vantage records a symbol leaving an exchange. It records it the same
// way whether the company died and somebody else took the symbol, or the
// company simply changed its name and kept trading. Dates alone therefore
// condemn both: `DD` reads `recycled` because its bars run seven years past
// "DowDuPont Inc"'s delisting date, when "DowDuPont Inc" is a FORMER NAME of
// the filer now called DuPont de Nemours — the same CIK, renamed.
//
// company_profile carries what settles it: the filer's current name and its
// rename history. So corroborate before condemning, using the same
// normalisation and the same tier order as symbol_match.go, because the audit
// and split_recycled.py disagreeing about which symbols are recycled would
// make both of them useless.
//
// Pure, and inert without the table: an absent company_profile yields an empty
// profile, IsRename answers false, and every verdict is exactly what it was
// before this existed.

import "strings"

// RenameMethod records which tier recognised a rename, so a caller can weigh an
// exact match against the loosest one.
type RenameMethod string

const (
	// RenameNone means nothing the filer has ever been called matches.
	RenameNone RenameMethod = "none"

	// RenameExact means the normalised names were identical with their
	// corporate suffixes intact.
	RenameExact RenameMethod = "exact"

	// RenameSuffix means they matched only after the corporate-suffix set was
	// stripped — "Coherent Inc" against "COHERENT CORP.".
	RenameSuffix RenameMethod = "suffix"

	// RenameCore means one name's words are a prefix of the other's once the
	// suffixes are gone — "Ads-Tec Energy Plc" against "Ads-Tec Energy Public
	// Ltd Co". The loosest tier, and the reason MinRenameWords exists.
	RenameCore RenameMethod = "core"
)

// MinRenameWords is how many words a core-tier prefix match must share. Two,
// because a single shared word like AMERICAN or NATIONAL corroborates nothing
// and would quietly clear a genuinely recycled symbol.
const MinRenameWords = 2

// MatchRename reports whether `delistedName` names the same company as the
// filer trading under the symbol today, and which of that filer's names it
// matched.
//
// The matched name is returned because it is routinely NOT the current one, and
// that difference is the whole finding: DD's evidence is the former name
// "DowDuPont Inc.", not the current "DuPont de Nemours, Inc.".
//
// The tiers err toward declaring a rename, which is the safe direction here:
// wrongly calling a rename a recycling condemns a continuous series, while
// wrongly calling a recycling a rename leaves it in the state it is in today.
// The known cost is an acquirer that takes its target's name — Coherent Inc was
// bought by II-VI, which renamed itself Coherent Corp, so COHR matches at the
// suffix tier although it really does carry two companies.
func MatchRename(delistedName string, p CompanyProfile) (RenameMethod, string) {
	target := NormalizeCompanyName(delistedName)
	if target == "" {
		return RenameNone, ""
	}

	candidates := make([]string, 0, len(p.FormerNames)+1)
	if p.Name != "" {
		candidates = append(candidates, p.Name)
	}
	for _, f := range p.FormerNames {
		if f != "" {
			candidates = append(candidates, f)
		}
	}
	if len(candidates) == 0 {
		return RenameNone, ""
	}

	for _, c := range candidates {
		if NormalizeCompanyName(c) == target {
			return RenameExact, c
		}
	}

	bare := stripSuffixes(target)
	if bare == "" {
		return RenameNone, ""
	}
	for _, c := range candidates {
		if stripSuffixes(NormalizeCompanyName(c)) == bare {
			return RenameSuffix, c
		}
	}

	words := strings.Fields(bare)
	for _, c := range candidates {
		other := strings.Fields(stripSuffixes(NormalizeCompanyName(c)))
		n := len(words)
		if len(other) < n {
			n = len(other)
		}
		if n < MinRenameWords {
			continue
		}
		same := true
		for i := 0; i < n; i++ {
			if words[i] != other[i] {
				same = false
				break
			}
		}
		if same {
			return RenameCore, c
		}
	}
	return RenameNone, ""
}

// IsRename is MatchRename reduced to the question a classifier asks.
func IsRename(delistedName string, p CompanyProfile) bool {
	m, _ := MatchRename(delistedName, p)
	return m != RenameNone
}
