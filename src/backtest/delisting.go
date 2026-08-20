package backtest

// What a position is worth when the company stops existing.
//
// The engine marks every holding at its last printed close. For a company that
// was acquired, that is roughly right — the tape's final price is close to what
// the holder was paid. For a company that went bankrupt it is a fiction: the
// last trade before a Chapter 11 delisting is not what the shareholder got,
// which was usually nothing.
//
// The literature's convention is a fixed haircut on PERFORMANCE-RELATED
// delistings — Shumway (1997) for NYSE/AMEX, Shumway & Warther (1999) for
// Nasdaq — and the word "performance-related" is doing all the work. Applying a
// haircut to every delisting would penalise the acquisitions, and on the joined
// data acquisitions are the MAJORITY of the delistings whose reason is known
// (1,352 of 1,893 as of 2026-08-20). So this applies the haircut only where the
// reason says the company failed, leaves acquisitions at their last close, and
// reports what it could not classify rather than guessing.
//
// The whole feature is off unless a config sets DelistingReturn, and its zero
// value reproduces previous behaviour exactly.

import (
	"time"

	"my-backtester/src/data"
)

// Shumway haircuts, exported so a config can name a researched figure rather
// than a number someone liked the look of. Both are the delisting RETURN, i.e.
// the fraction of the position's value lost on the delisting day.
const (
	// ShumwayNYSEReturn is the NYSE/AMEX figure from Shumway (1997).
	ShumwayNYSEReturn = -0.30

	// ShumwayNasdaqReturn is the Nasdaq figure from Shumway & Warther (1999).
	ShumwayNasdaqReturn = -0.55
)

// delistingEvent is what the engine needs to know about one ticker's death:
// when, and whether the company failed. Reason is deliberately kept rather than
// reduced to the boolean, so a diagnostic can say which of them applied.
type delistingEvent struct {
	Date   time.Time
	Reason data.Reason
}

// performanceRelated reports whether this death is the kind the haircut is for.
// Bankruptcy is the clear case; a compliance delisting (price, market cap or
// filing failures) is the exchange throwing a company out, which is also a
// failure. An acquisition and a voluntary deregistration are not, and an
// unknown reason is not treated as one either — see haircutPrice.
func (e delistingEvent) performanceRelated() bool {
	return e.Reason == data.ReasonBankruptcy || e.Reason == data.ReasonCompliance
}

// loadDelistingEvents joins the delisting registry to the reasons harvested
// from EDGAR, for the tickers a run actually holds.
//
// Returns nil when either optional table is absent, which is the state of any
// database where the loaders have not been run — the feature is then inert and
// results are identical to before it shipped.
func loadDelistingEvents(tickers []string) map[string]delistingEvent {
	registry, err := data.LoadDelistings(tickers)
	if err != nil || len(registry) == 0 {
		return nil
	}

	// The registry keys on symbol, EDGAR keys on CIK, and only the name join
	// (T51) connects them. It resolves about a quarter of symbols; the rest
	// keep ReasonUnknown, which is honest and is why unknown never takes a
	// haircut.
	byCIK, err := data.LoadDelistingReasons(nil)
	var bySymbol map[string]data.DelistingReason
	if err == nil && len(byCIK) > 0 {
		all := make([]data.Delisting, 0, len(registry))
		for _, d := range registry {
			all = append(all, d)
		}
		reasons := make([]data.DelistingReason, 0, len(byCIK))
		for _, r := range byCIK {
			reasons = append(reasons, r)
		}
		bySymbol = data.ReasonsByMatchedSymbol(
			data.MatchDelistingSymbols(all, reasons), reasons)
	}

	out := make(map[string]delistingEvent, len(registry))
	for symbol, d := range registry {
		ev := delistingEvent{Date: d.DelistingDate, Reason: data.ReasonUnknown}
		if r, ok := bySymbol[symbol]; ok {
			ev.Reason = r.Reason
		}
		out[symbol] = ev
	}
	return out
}

// applyDelistingReturns liquidates any position still open on the final
// simulated day whose company had already delisted, at a haircut price.
//
// It runs as a SALE rather than a silent mark-down so the blotter, the realised
// P&L and the per-ticker attribution all see it: a delisting is a liquidation,
// and hiding it from the trade record would make the attribution panel lie
// about where the money went.
//
// Only the final day, because that is the only reachable case. validateCoverage
// refuses a portfolio whose window runs past a ticker's last bar, so a holding
// cannot die in the middle of a run — it can only still be held when the run
// ends on or after the day it died.
func (p *Portfolio) applyDelistingReturns(
	windowed map[string][]data.AssetData, dataLen int,
) {
	if p.DelistingReturn == 0 || len(p.DelistingEvents) == 0 || dataLen == 0 {
		return
	}
	last := dataLen - 1

	var applied, unclassified []string
	for _, ticker := range p.Tickers {
		pos, ok := p.Positions[ticker]
		if !ok || pos.Amount <= 0 {
			continue
		}
		bars := windowed[ticker]
		if last >= len(bars) {
			continue
		}
		day := bars[last].Date
		// A generation symbol carries its base company's death: DD.1 is the
		// series that ended when DD died.
		base, _ := data.ParseGeneration(ticker)
		ev, ok := p.DelistingEvents[ticker]
		if !ok {
			ev, ok = p.DelistingEvents[base]
		}
		if !ok || ev.Date.IsZero() || ev.Date.After(day) {
			continue
		}
		if !ev.performanceRelated() {
			if ev.Reason == data.ReasonUnknown {
				unclassified = append(unclassified, ticker)
			}
			continue
		}
		// Sell at the haircut price. Costs still apply on top: a forced
		// liquidation is not a free one.
		price := bars[last].Close * (1 + p.DelistingReturn)
		if price < 0 {
			price = 0
		}
		p.Sell(ticker, pos.Amount, price, day)
		applied = append(applied, ticker)
	}

	p.Metrics.DelistedHoldings = len(applied)
	p.Metrics.UnclassifiedDelistings = len(unclassified)
	if len(applied) > 0 {
		diagf("warn", p.Pname,
			"portfolio %q: %d holding(s) were liquidated at a %.0f%% delisting "+
				"return because the company failed: %v",
			p.Pname, len(applied), p.DelistingReturn*100, applied)
	}
	if len(unclassified) > 0 {
		diagf("warn", p.Pname,
			"portfolio %q: %d holding(s) delisted for a reason EDGAR could not "+
				"be matched to, so they were marked at their last close and NOT "+
				"haircut — this result is optimistic by however many of them "+
				"failed: %v",
			p.Pname, len(unclassified), unclassified)
	}
}
