package data

// The last link between "what happened to this company" and "what happened to
// the symbol my backtest held".
//
// delisting_reasons.go is keyed by CIK, because EDGAR is: a filer's submissions
// document lists `tickers` only while it has a CURRENT listing, which a delisted
// company by definition does not. Measured 2026-08-17: SunEdison's document
// reports `tickers: []`. delistings.go is keyed by symbol, because Alpha Vantage
// is. Neither carries the other's key, so the join has to go through the one
// field they share — the company name — and company names differ in exactly the
// ways company names always differ:
//
//	SUNEDISON, INC.            vs  SunEdison Inc
//	CoreSite Realty Corp       vs  CoreSite Realty Corporation
//
// So: normalise, then match in tiers, and record which tier fired so a caller
// can trust an exact match more than a fuzzy one.
//
// Two rules matter more than the matching itself, and both are about refusing
// to guess:
//
//  1. THE DATES MUST AGREE. Two companies can share a name; the same company
//     delisting on the same date is far stronger evidence than the name alone.
//     A candidate whose delisting date is more than MaxDateGapDays from the
//     registry's is not a match at any similarity.
//  2. AMBIGUITY IS NOT A MATCH. If two filers reduce to one registry name, the
//     result records both and reports MatchAmbiguous rather than picking one.
//     A wrong join here silently attributes a bankruptcy to the wrong symbol,
//     which is worse than no join at all — an unmatched symbol degrades to
//     ReasonUnknown, which is honest.
//
// Pure, so the precedence and the refusals can be tested without a database,
// in the shape of ClassifyReason and ClassifyCoverage.

import (
	"sort"
	"strings"
	"time"
)

// MaxDateGapDays is how far apart the two sources' delisting dates may be and
// still describe the same event. The registry records the last day the symbol
// traded; EDGAR records when a form was filed, and the exchange's Form 25 comes
// days either side of the final tape. Thirty days absorbs that without being
// wide enough to swallow a different company's delisting.
const MaxDateGapDays = 30

// MinSimilarity is the floor for a fuzzy match. Set high on purpose: below it
// the pairs that survive the date gate stop being spelling variants and start
// being different companies in the same industry.
const MinSimilarity = 0.90

// MatchMethod records which rule produced a match, so a caller can weigh it.
type MatchMethod string

const (
	// MatchNone means no filer cleared both the name and the date gate.
	MatchNone MatchMethod = "none"

	// MatchExact means the normalised names were identical with their
	// corporate suffixes intact — the strongest evidence available here.
	MatchExact MatchMethod = "exact"

	// MatchSuffix means they matched only after the INC/CORP/CO/LTD/LLC/PLC/
	// HOLDINGS/GROUP set was stripped, i.e. "CoreSite Realty Corp" against
	// "CoreSite Realty Corporation".
	MatchSuffix MatchMethod = "suffix"

	// MatchFuzzy means an edit-distance similarity at or above MinSimilarity.
	MatchFuzzy MatchMethod = "fuzzy"

	// MatchAmbiguous means two or more filers tied at the best tier. Recorded,
	// deliberately not resolved.
	MatchAmbiguous MatchMethod = "ambiguous"
)

// Resolved reports whether the match names exactly one filer. Ambiguous and
// none both answer false: the caller has no CIK it may safely use.
func (m MatchMethod) Resolved() bool {
	return m == MatchExact || m == MatchSuffix || m == MatchFuzzy
}

// SymbolMatch is one symbol's join result, with the evidence behind it.
type SymbolMatch struct {
	Symbol string
	// RegistryName is the name Alpha Vantage carries for the symbol.
	RegistryName string
	// CIK is the matched filer, empty unless MatchedBy.Resolved().
	CIK string
	// FilerName is EDGAR's name for that filer, empty unless resolved.
	FilerName string

	MatchedBy MatchMethod
	// Similarity is the normalised-name similarity of the chosen candidate,
	// 1 for exact and suffix matches, 0 when nothing matched.
	Similarity float64
	// DateGapDays is the absolute gap between the two sources' delisting
	// dates. Small is corroboration; the gate already rejected anything over
	// MaxDateGapDays.
	DateGapDays int
	// Candidates lists every CIK that tied at the best tier. It has one entry
	// for a resolved match and two or more when MatchedBy is MatchAmbiguous,
	// so an ambiguous result still says who the contenders were.
	Candidates []string
}

// corporateSuffixes is the set stripped for MatchSuffix. Only forms that carry
// no distinguishing information: "HOLDINGS" and "GROUP" are here because
// registries drop them freely, while words like TRUST, PARTNERS or BANCORP are
// deliberately absent — they distinguish real, differently-named entities.
var corporateSuffixes = map[string]bool{
	"INC": true, "INCORPORATED": true,
	"CORP": true, "CORPORATION": true,
	"CO": true, "COMPANY": true,
	"LTD": true, "LIMITED": true,
	"LLC": true, "PLC": true,
	"HOLDINGS": true, "HOLDING": true,
	"GROUP": true,
}

// NormalizeCompanyName folds the differences that are spelling rather than
// identity: case, punctuation, "&" against "AND", a leading "THE", and runs of
// whitespace. Corporate suffixes are NOT stripped here — that is the separate,
// weaker tier, and collapsing the two would lose the distinction.
func NormalizeCompanyName(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range strings.ToUpper(s) {
		switch {
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '&':
			b.WriteString(" AND ")
		default:
			// Punctuation becomes a separator rather than vanishing, so
			// "SUNEDISON,INC" cannot fuse into one token.
			b.WriteByte(' ')
		}
	}
	fields := strings.Fields(b.String())
	if len(fields) > 1 && fields[0] == "THE" {
		fields = fields[1:]
	}
	return strings.Join(fields, " ")
}

// stripSuffixes removes trailing corporate suffixes, repeatedly — "FOO HOLDINGS
// INC" and "FOO INC" have to reduce alike. Never strips the last remaining
// token: a filer literally named "Holdings Inc" keeps a name.
func stripSuffixes(normalized string) string {
	fields := strings.Fields(normalized)
	for len(fields) > 1 && corporateSuffixes[fields[len(fields)-1]] {
		fields = fields[:len(fields)-1]
	}
	return strings.Join(fields, " ")
}

// NameSimilarity scores two raw names in [0,1] as 1 minus their normalised edit
// distance, after NormalizeCompanyName. Two empty names score 0, not 1 — an
// absent name is missing evidence, never agreement.
func NameSimilarity(a, b string) float64 {
	na, nb := NormalizeCompanyName(a), NormalizeCompanyName(b)
	if na == "" || nb == "" {
		return 0
	}
	if na == nb {
		return 1
	}
	longest := len(na)
	if len(nb) > longest {
		longest = len(nb)
	}
	return 1 - float64(levenshtein(na, nb))/float64(longest)
}

// levenshtein is the ordinary two-row edit distance. Company names are short
// enough that nothing cleverer earns its complexity.
func levenshtein(a, b string) int {
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min(prev[j]+1, min(curr[j-1]+1, prev[j-1]+cost))
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

// dayGap is the absolute whole-day difference between two dates.
func dayGap(a, b time.Time) int {
	d := a.Sub(b)
	if d < 0 {
		d = -d
	}
	return int(d.Hours() / 24)
}

// MatchDelistingSymbols joins the CIK-keyed reasons onto the symbol-keyed
// registry by company name, returning one SymbolMatch per registry entry —
// including the ones that did not match, because "no filer explains this
// symbol" is a result the caller needs to see rather than an absence.
//
// Pure: it takes both sides as slices and touches no database.
func MatchDelistingSymbols(registry []Delisting, reasons []DelistingReason) map[string]SymbolMatch {
	out := make(map[string]SymbolMatch, len(registry))
	for _, d := range registry {
		out[d.Symbol] = matchOne(d, reasons)
	}
	return out
}

// matchOne resolves a single symbol. The tiers are tried in order and the first
// that produces any candidate wins outright — a weaker tier never breaks a tie
// in a stronger one, because a tie at the strong tier is genuine ambiguity and
// falling through would silently manufacture confidence.
func matchOne(d Delisting, reasons []DelistingReason) SymbolMatch {
	m := SymbolMatch{
		Symbol:       d.Symbol,
		RegistryName: d.Name,
		MatchedBy:    MatchNone,
	}
	if d.Name == "" || d.DelistingDate.IsZero() {
		return m
	}

	normalized := NormalizeCompanyName(d.Name)
	stripped := stripSuffixes(normalized)

	type candidate struct {
		reason     DelistingReason
		similarity float64
		gap        int
	}
	var exact, suffix, fuzzy []candidate

	for _, r := range reasons {
		// The date gate comes first and applies to every tier: a name is not
		// evidence of an event, and a filer with no delisting date at all
		// carries no corroboration to offer.
		if r.DelistingDate.IsZero() {
			continue
		}
		gap := dayGap(d.DelistingDate, r.DelistingDate)
		if gap > MaxDateGapDays {
			continue
		}

		rn := NormalizeCompanyName(r.CompanyName)
		if rn == "" {
			continue
		}
		switch {
		case rn == normalized:
			exact = append(exact, candidate{r, 1, gap})
		case stripSuffixes(rn) == stripped:
			suffix = append(suffix, candidate{r, 1, gap})
		default:
			if s := NameSimilarity(d.Name, r.CompanyName); s >= MinSimilarity {
				fuzzy = append(fuzzy, candidate{r, s, gap})
			}
		}
	}

	var tier []candidate
	var method MatchMethod
	switch {
	case len(exact) > 0:
		tier, method = exact, MatchExact
	case len(suffix) > 0:
		tier, method = suffix, MatchSuffix
	case len(fuzzy) > 0:
		tier, method = fuzzy, MatchFuzzy
	default:
		return m
	}

	m.Candidates = make([]string, 0, len(tier))
	for _, c := range tier {
		m.Candidates = append(m.Candidates, c.reason.CIK)
	}
	sort.Strings(m.Candidates)

	if len(tier) > 1 {
		// Two filers, one name, both delisting at the same time. Recorded and
		// left unresolved on purpose; see the note at the top of this file.
		m.MatchedBy = MatchAmbiguous
		return m
	}

	m.MatchedBy = method
	m.CIK = tier[0].reason.CIK
	m.FilerName = tier[0].reason.CompanyName
	m.Similarity = tier[0].similarity
	m.DateGapDays = tier[0].gap
	return m
}

// ReasonsByMatchedSymbol is the join's payoff: symbol -> why that symbol died,
// for the symbols whose match resolved. Ambiguous and unmatched symbols are
// absent, so a caller that looks a symbol up and finds nothing gets
// ReasonUnknown, which is the honest answer.
//
// Distinct from ReasonsBySymbol in delisting_reasons.go, which re-keys by the
// `tickers` EDGAR itself lists. That path is free and exact but empty for
// precisely the companies this file exists for; this one earns its coverage by
// matching names. Prefer EDGAR's own answer where it has one.
func ReasonsByMatchedSymbol(matches map[string]SymbolMatch, reasons []DelistingReason) map[string]DelistingReason {
	byCIK := make(map[string]DelistingReason, len(reasons))
	for _, r := range reasons {
		byCIK[r.CIK] = r
	}
	out := make(map[string]DelistingReason)
	for symbol, m := range matches {
		if !m.MatchedBy.Resolved() {
			continue
		}
		if r, ok := byCIK[m.CIK]; ok {
			out[symbol] = r
		}
	}
	return out
}
