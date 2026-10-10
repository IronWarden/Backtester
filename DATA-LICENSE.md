# Terms for the bundled market-data database

`stock_data.db` is distributed **with** Backtester but is not part of it. The
software is covered by [`LICENSE`](LICENSE); this file covers the data, and it
is the more restrictive of the two.

Read the [Known defects](#known-defects) section before you trust a number that
comes out of this database. Every item in it can move a result.

---

## What you may do with it

Use it, locally, to evaluate Backtester and to run your own backtests.

## What you may not do with it

Redistribute it, republish it, resell it, or make it available to others — in
whole, in part, or as a derived dataset. This is not a preference. Some of the
data below comes from providers whose own terms forbid redistribution, and the
copy shipped with Backtester is provided to you on the same basis.

If you need data you can redistribute, rebuild the database from the loader
scripts against sources you have your own rights to. `build_db.py` builds a
complete database of your own in one command (see "Build your own database" in
README.md); the same terms apply to what it downloads, since it uses the same
sources.

## No warranty of accuracy

The data is provided as is. It is assembled from free public sources by the
scripts in this repository, without a commercial data vendor's validation, and
it is known to contain errors — see below. Nothing here is warranted to be
accurate, complete or current, and no liability is accepted for any decision
made using it.

---

## Where each table came from

Every table's real provenance — the loader, the commit that ran it, and when —
is recorded in the database itself, in the `data_sources` table. Query it; do
not trust this list to stay current.

| Data | Upstream source | Their terms, in short |
|---|---|---|
| `stock_data_optimized` (daily bars) | Yahoo Finance, via `yfinance` | **Personal use; redistribution not permitted.** The binding constraint on this whole file. |
| `delistings` | Alpha Vantage | Free-tier API under their terms of service; redistribution restricted. |
| `3MTreasuryYields`, `"10YrTreasuryYields"`, `economic_indicators` | FRED (Federal Reserve Bank of St. Louis) | Underlying US government series are public domain; FRED's own terms of use apply to the service. |
| `sec_financials`, `company_profile`, `delisting_reasons`, `earnings_calendar` | SEC EDGAR | US government work, public domain. EDGAR requires a declared User-Agent and rate limits automated access. |
| `index_membership` (S&P 500 members by date) | English Wikipedia | **CC BY-SA 4.0** — attribution *and* share-alike. Redistributing this table, or a dataset derived from it, carries obligations the rest of this file does not. |
| `$`-prefixed benchmark series | Reconstructed locally from the above plus the Ken French Data Library (Dartmouth) and Robert Shiller's dataset (Yale) | Academic datasets, free to use, customarily cited. |
| `financials` | Yahoo Finance, via `yfinance` | As above. |
| `data_quality`, `data_sources` | Computed locally by this project's own scripts | — |

---

## Known defects

These are measured, not suspected: the per-ticker verdicts ship with the
database, in the `data_quality` table.

**The universe contains only survivors.** `stock_data_optimized` holds no
company that stopped trading before 2025. Of the 1,792 tickers trading in 2000,
all 1,792 are still present — a 100% twenty-five-year survival rate, against a
real figure nearer 40–50%. Enron, Lehman, WorldCom, Bear Stearns, Washington
Mutual, SVB and Bed Bath & Beyond are simply absent, and `GM` and `HTZ` are the
post-bankruptcy re-listings with the collapse excised. **Any backtest that
picks among these tickers is choosing among known winners, and its results are
optimistic by an amount this data cannot measure.** The `$`-prefixed benchmark
series are not affected — they are reconstructed index series that already
contain the companies that failed out of the index.

**273 tickers cannot support a backtest, and nothing refuses to trade them.**
`VHI`, `SVA`, `SAFE` and `DEC` carry *negative* closing prices (11,931 bars,
`VHI` to −639.30) from back-adjustment; `HUBB` has 1,862 consecutive identical
closes and synthetic 1972 bars; 281 tickers have a flat run of 60 days or more;
263 have a single-day move over 500%. A further 1,053 are usable but carry
defects that can move a metric on their own.

**Prices are split- and dividend-adjusted.** They are not the prices anything
traded at on the day. A market capitalisation computed from an adjusted close
is understated for any dividend payer, by more the further back you go.

**Some series end before others.** The `$`-benchmarks and the CPI series lag
the price data. Benchmark-relative metrics (alpha, beta, tracking error,
information ratio, capture) are dropped entirely for any window the benchmark
does not fully cover, and inflation-adjusted returns are blank past the last
CPI reading. The run console says so when it happens.

**One symbol can be two companies.** Recycled tickers have been separated where
they were found — the current company keeps the bare symbol and earlier
occupants take a generation suffix (`COR.1`) — but the separation is
corroborated heuristically and is not guaranteed complete.

---

## Attribution

If you publish anything derived from this data, cite the upstream sources
rather than this project: they produced the data, and this project only
assembled it.
