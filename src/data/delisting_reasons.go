package data

// Why a company stopped trading, which decides what a holder actually got.
//
// The delisting registry (delistings.go) knows the date and not the cause, and
// the two causes point in opposite directions: an acquisition usually pays a
// premium, a Chapter 11 usually pays nothing. The literature's answer to not
// knowing is a blanket haircut — Shumway & Warther (1999) put it at −55% for
// Nasdaq, Shumway (1997) nearer −30% for NYSE/AMEX — which overcorrects every
// acquisition. Knowing the reason is what makes a delisting return honest
// rather than a guess applied uniformly.
//
// SEC EDGAR answers it for free. `add_delisting_reasons.py` harvests the
// quarterly filing indexes for the forms that end a listing, then reads each
// filer's submissions document for the 8-K item that explains it:
//
//	item 1.03  Bankruptcy or Receivership
//	item 2.01  Completion of Acquisition or Disposition of Assets
//
// Verified against cases whose answers are public knowledge, 2026-08-17:
// SunEdison (CIK 945436) filed 8-K item 1.03 on 2016-04-27, nine days before
// the NYSE filed 25-NSE → bankruptcy. CoreSite Realty and Activision Blizzard
// each filed item 2.01 on the same day as their 25-NSE → acquired. SVB
// Financial filed BOTH (2.01 on 2023-03-14, 1.03 on 2023-03-17), which is why
// bankruptcy has to outrank acquisition: a company sold out of Chapter 11 files
// both, and the shareholder was still wiped out.
//
// Inert until the table exists, like every other optional dataset here.
//
// The unresolved half, stated plainly: this is keyed by CIK, and mapping a dead
// company's CIK back to its TICKER is not solved. EDGAR lists `tickers` only
// for companies with a current listing, which these by definition lack. Until
// that join exists (name-matching against `delistings.name` is the obvious
// route), this table answers "what happened to this company" and not yet "what
// happened to the symbol my backtest held".

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// Reason is why a listing ended.
type Reason string

const (
	// ReasonUnknown means no delisting filing was found. Absence of evidence:
	// EDGAR's indexes are complete from 2001 but a company may have left by a
	// route these forms do not capture.
	ReasonUnknown Reason = "unknown"

	// ReasonBankruptcy means an 8-K reporting bankruptcy or receivership was
	// filed around the delisting. Equity is usually worthless.
	ReasonBankruptcy Reason = "bankruptcy"

	// ReasonAcquired means an 8-K reporting a completed acquisition was filed
	// around the delisting. The holder was usually paid, often at a premium.
	ReasonAcquired Reason = "acquired"

	// ReasonCompliance means the exchange filed the delisting notice (Form
	// 25-NSE) with no explanatory 8-K nearby: a listing-standards failure —
	// price, float, market cap, or delinquent filings. The equity keeps
	// trading over the counter, usually much lower.
	ReasonCompliance Reason = "compliance"

	// ReasonVoluntary means the company filed its own Form 25, or deregistered,
	// without an 8-K saying why. Usually a merger or a going-private deal, but
	// the filings do not say so, so it is not called an acquisition.
	ReasonVoluntary Reason = "voluntary"
)

// TerminalForHolders reports whether the reason implies the position was
// destroyed rather than paid out. Only bankruptcy is: a compliance delisting
// leaves a stock trading over the counter, and both acquisition routes pay
// something.
//
// Deliberately not a return figure. What number to apply on a delisting is a
// modelling choice with a config knob attached, and it belongs to whoever adds
// it, not here.
func (r Reason) TerminalForHolders() bool { return r == ReasonBankruptcy }

// ReasonEvidence is the flags a reason is derived from — one boolean or date per
// signal found in EDGAR, so the verdict can be recomputed and argued with.
type ReasonEvidence struct {
	// BankruptcyReport and AcquisitionReport are the dates of the 8-K filings
	// carrying items 1.03 and 2.01 near the delisting, zero when absent.
	BankruptcyReport  time.Time
	AcquisitionReport time.Time
	// ExchangeFiled25 is a Form 25-NSE: the exchange removed the listing.
	ExchangeFiled25 bool
	// CompanyFiled25 is a Form 25 filed by the company itself.
	CompanyFiled25 bool
	// Deregistered is a Form 15 of any flavour.
	Deregistered bool
}

// ClassifyReason turns the evidence into a reason, most conclusive signal first.
// Pure, so the precedence can be tested without a database.
//
// Bankruptcy outranks acquisition because SVB Financial filed both three days
// apart; treating that as an acquisition would credit a holder who was wiped
// out.
func ClassifyReason(ev ReasonEvidence) Reason {
	switch {
	case !ev.BankruptcyReport.IsZero():
		return ReasonBankruptcy
	case !ev.AcquisitionReport.IsZero():
		return ReasonAcquired
	case ev.CompanyFiled25 || ev.Deregistered:
		return ReasonVoluntary
	case ev.ExchangeFiled25:
		return ReasonCompliance
	default:
		return ReasonUnknown
	}
}

// DelistingReason is one row of the `delisting_reasons` table.
type DelistingReason struct {
	// CIK is the SEC's permanent filer identifier, zero-padded to ten digits.
	// It is the key here because a dead company has no reliable ticker.
	CIK         string
	CompanyName string
	SIC         string
	// Tickers is what EDGAR lists for the filer, which for a delisted company
	// is usually empty — see the note at the top of this file.
	Tickers       []string
	DelistingDate time.Time
	// Reason is recomputed from Evidence rather than read from the table's own
	// column, so a precedence change takes effect without re-running the
	// harvest.
	Reason   Reason
	Evidence ReasonEvidence
	// Confidence is the loader's own summary of how direct the evidence was:
	// high (an 8-K said so), medium (the exchange filed and nothing explains
	// it), low (the company left quietly), none.
	Confidence string
	Note       string
}

// LoadDelistingReasons reads the table, keyed by CIK. Pass CIKs to narrow it, or
// nothing for every row.
//
// Returns nil without error when the `delisting_reasons` table does not exist,
// which is the state of any database where `add_delisting_reasons.py` has not
// been run.
func LoadDelistingReasons(ciks []string) (map[string]DelistingReason, error) {
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	if !tableExists("delisting_reasons") {
		return nil, nil
	}

	query := `SELECT cik, company_name, sic, tickers, delisting_date,
	                 confidence, evidence, exchange_filed_25, company_filed_25,
	                 deregistered, bankruptcy_8k, acquisition_8k
	          FROM delisting_reasons`
	args := make([]any, 0, len(ciks))
	if len(ciks) > 0 {
		query += ` WHERE cik IN (` + placeholderList(len(ciks)) + `)`
		for _, c := range ciks {
			args = append(args, c)
		}
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]DelistingReason)
	for rows.Next() {
		var d DelistingReason
		var name, sic, tickers, confidence, note sql.NullString
		var delisted, bankruptcy, acquisition sql.NullTime
		var exch, company, dereg sql.NullBool
		if err := rows.Scan(
			&d.CIK, &name, &sic, &tickers, &delisted, &confidence, &note,
			&exch, &company, &dereg, &bankruptcy, &acquisition,
		); err != nil {
			log.Printf("scanning delisting reason: %v", err)
			continue
		}
		d.CompanyName = name.String
		d.SIC = sic.String
		d.Confidence = confidence.String
		d.Note = note.String
		d.DelistingDate = delisted.Time
		if tickers.String != "" {
			if err := json.Unmarshal([]byte(tickers.String), &d.Tickers); err != nil {
				d.Tickers = nil
			}
		}
		d.Evidence = ReasonEvidence{
			BankruptcyReport:  bankruptcy.Time,
			AcquisitionReport: acquisition.Time,
			ExchangeFiled25:   exch.Bool,
			CompanyFiled25:    company.Bool,
			Deregistered:      dereg.Bool,
		}
		d.Reason = ClassifyReason(d.Evidence)
		out[d.CIK] = d
	}
	return out, rows.Err()
}

// ReasonsBySymbol re-keys the table by whatever tickers EDGAR listed for each
// filer. Usually thin for exactly the companies this is about — a delisted
// filer has no current listing and so no tickers — which is why it returns what
// it can find rather than claiming coverage.
func ReasonsBySymbol(reasons map[string]DelistingReason) map[string]DelistingReason {
	out := make(map[string]DelistingReason)
	for _, d := range reasons {
		for _, t := range d.Tickers {
			out[t] = d
		}
	}
	return out
}
