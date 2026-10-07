// The Research tab (T142a): look a company up the way a terminal does --
// a ticker, then a function -- and read facts from primary data only.
//
//   DES  what the company is (SEC EDGAR profile, index membership, the
//        database's own price history and its quality audit)
//   FA   reported financials, each figure as first FILED, with the date
//   ERN  report dates and how the price met each one
//   CN   the company's own SEC filings, in a clean reader
//   ECO  the economy's official readings and the FOMC's meetings (no ticker)
//   MY   the watchlist: its companies' filings and earnings, newest first
//   "…"  full-text search of every filing since 2001, optionally one company's
//
// Nothing here is a live quote or a headline: every number says where it came
// from and as of when. The three views are exported and take plain data, so
// the smoke render exercises them without a backend; the default export owns
// the command line and the fetching.

import { useEffect, useMemo, useRef, useState } from "react";
import {
    CompanyEarnings,
    CompanyFilings,
    CompanyFinancials,
    CompanyOverview,
    EconomicReleases,
    FilingDocuments,
    ListTickers,
    LoadWatchlist,
    ReadFiling,
    SaveWatchlist,
    SearchFilings,
    TickerNames,
    WatchlistFeed,
} from "../wailsjs/go/main/App";
import { BrowserOpenURL } from "../wailsjs/runtime/runtime";
import type { main } from "../wailsjs/go/models";
import { searchTickers } from "./tickerSearch";
import { ResearchTable, type Column } from "./ResearchTable";
import {
    barWidth,
    countdown,
    fmtDate,
    fmtShortDate,
    fmtTime,
    moveSummary,
    timingTag,
} from "./researchFormat";
import {
    FILING_FILTERS,
    RESEARCH_FUNCTIONS,
    SEARCH_RANGES,
    filterFilings,
    firstDocument,
    isMarketFunction,
    formatEcoChange,
    formatEcoValue,
    formatFigure,
    formatMove,
    formatPrice,
    holdingsToAdd,
    orderHits,
    parseResearchCommand,
    resolveQuery,
    searchSince,
    type FilingFilter,
    type ResearchFn,
    type SearchRange,
} from "./researchCommand";

function Notes({ notes }: { notes: string[] }) {
    if (!notes || notes.length === 0) return null;
    return (
        <ul className="research-notes">
            {notes.map((n) => (
                <li key={n}>{n}</li>
            ))}
        </ul>
    );
}

// EDGAR's own page for a filer: the primary source behind DES and FA.
export function edgarURL(cik: string): string {
    return `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${encodeURIComponent(cik)}`;
}

function openExternal(url: string) {
    try {
        BrowserOpenURL(url);
    } catch {
        // No Wails runtime (the smoke harness): nothing to open.
    }
}

export function DesView({ d }: { d: main.CompanyOverview }) {
    const current = d.index.find((s) => !s.end);
    return (
        <div className="research-view">
            <h2 className="research-title">
                {d.name || d.ticker} <span className="research-ticker">{d.ticker}</span>
            </h2>
            {(d.sector || d.exchange) && (
                <dl className="research-facts">
                    {d.sicDescription && (
                        <>
                            <dt>Industry</dt>
                            <dd>
                                {d.sicDescription}
                                {d.sector && <span className="research-dim"> · {d.sector} (SIC division)</span>}
                            </dd>
                        </>
                    )}
                    {d.exchange && (
                        <>
                            <dt>Exchange</dt>
                            <dd>{d.exchange}</dd>
                        </>
                    )}
                    {d.incorporated && (
                        <>
                            <dt>Incorporated</dt>
                            <dd>{d.incorporated}</dd>
                        </>
                    )}
                    {d.cik && (
                        <>
                            <dt>SEC CIK</dt>
                            <dd>
                                {d.cik}{" "}
                                <button
                                    type="button"
                                    className="link-button"
                                    onClick={() => openExternal(edgarURL(d.cik))}
                                    title="Open the company's filings on sec.gov in your browser"
                                >
                                    Filings on EDGAR ↗
                                </button>
                            </dd>
                        </>
                    )}
                    {d.formerNames.length > 0 && (
                        <>
                            <dt>Formerly</dt>
                            <dd>{d.formerNames.join(" · ")}</dd>
                        </>
                    )}
                </dl>
            )}

            {d.price && (
                <section className="research-section">
                    <h3>Price history in this database</h3>
                    <p>
                        Last close <strong>{formatPrice(d.price.lastClose)}</strong> on{" "}
                        {d.price.lastDate}. 52-week closing range {formatPrice(d.price.low52)}–
                        {formatPrice(d.price.high52)}.
                    </p>
                    <p className="research-dim">
                        {d.price.bars.toLocaleString("en-US")} sessions since {d.price.firstDate}. From
                        the database's own bars, not a live quote.
                    </p>
                </section>
            )}

            {d.index.length > 0 && (
                <section className="research-section">
                    <h3>S&amp;P 500</h3>
                    <p>
                        {current
                            ? `A member ${current.atLeastSince ? "at least since" : "since"} ${current.start}.`
                            : "Not a member today."}
                    </p>
                    {d.index.length > (current ? 1 : 0) && (
                        <ul className="research-list">
                            {d.index
                                .filter((s) => s !== current)
                                .map((s) => (
                                    <li key={s.start}>
                                        {s.atLeastSince ? "At least " : ""}
                                        {s.start} to {s.end}
                                    </li>
                                ))}
                        </ul>
                    )}
                    <p className="research-dim">
                        Reconstructed from the index's published changes
                        {current?.confidence ? `; ${current.confidence} confidence for that year` : ""}.
                    </p>
                </section>
            )}

            {d.quality && (
                <section className="research-section">
                    <h3>Data quality</h3>
                    <p>
                        The price series audits as <strong>{d.quality}</strong>
                        {d.qualityReason ? ` — ${d.qualityReason}` : "."}
                    </p>
                </section>
            )}
            <Notes notes={d.notes} />
        </div>
    );
}

export function FaView({
    f,
    onFrequency,
}: {
    f: main.CompanyFinancials;
    onFrequency: (freq: "annual" | "quarterly") => void;
}) {
    return (
        <div className="research-view">
            <h2 className="research-title">
                {f.company || f.ticker} <span className="research-ticker">{f.ticker} FA</span>
            </h2>
            <div className="research-toggle" role="group" aria-label="Period">
                {(["annual", "quarterly"] as const).map((freq) => (
                    <button
                        key={freq}
                        type="button"
                        className={f.frequency === freq ? "active" : ""}
                        onClick={() => onFrequency(freq)}
                    >
                        {freq === "annual" ? "Annual" : "Quarterly"}
                    </button>
                ))}
            </div>
            {f.periods.length > 0 && (
                <div className="research-table-wrap">
                    <table className="research-table">
                        <thead>
                            <tr>
                                <th>Period ending</th>
                                {f.periods.map((p) => (
                                    <th key={p}>{p}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {f.rows.map((r) => (
                                <tr key={r.metric}>
                                    <th scope="row">{r.metric}</th>
                                    {r.cells.map((c, i) =>
                                        c ? (
                                            <td key={i} title={`Filed ${c.filed}${c.form ? ` in a ${c.form}` : ""}`}>
                                                {formatFigure(r.metric, c.value)}
                                            </td>
                                        ) : (
                                            <td key={i} className="research-dim">
                                                —
                                            </td>
                                        ),
                                    )}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
            {f.periods.length > 0 && (
                <p className="research-dim">
                    Each figure as first filed with the SEC (hover for the filing date and form);
                    later restatements are not shown. CIK {f.cik}.
                </p>
            )}
            <Notes notes={f.notes} />
        </div>
    );
}

export function TimingTag({ timing }: { timing: string }) {
    if (!timing) return null;
    const tag = timingTag(timing);
    return <span className={`rt-tag ${tag.cls}`}>{tag.label}</span>;
}

// A price move: signed and coloured, with a bar scaled to the largest move
// in the same table.
export function MoveCell({ move, has, largest }: { move: number; has: boolean; largest: number }) {
    if (!has) return <span className="research-dim">—</span>;
    return (
        <span className={`rt-move ${move >= 0 ? "pos" : "neg"}`}>
            <span className="rt-bar" aria-hidden="true">
                <span style={{ width: `${barWidth(move, largest)}%` }} />
            </span>
            {formatMove(move)}
        </span>
    );
}

export function FormChip({ form, kind }: { form: string; kind: string }) {
    return <span className={`filing-form kind-${kind}`}>{form}</span>;
}

const ernColumns: Column<main.EarningsRow>[] = [
    {
        key: "date", label: "Reported", sortValue: (r) => r.reported, firstDir: "desc",
        render: (r) => fmtDate(r.reported),
    },
    { key: "time", label: "Time (ET)", render: (r) => fmtTime(r.reported) },
    { key: "timing", label: "Timing", sortValue: (r) => r.timing, render: (r) => <TimingTag timing={r.timing} /> },
    {
        key: "move", label: "Price move", numeric: true, firstDir: "desc",
        sortValue: (r) => (r.hasMove ? r.move : null),
        render: () => null, // replaced per table, to know the largest move
    },
    {
        key: "measured", label: "Measured",
        render: (r) => (r.hasMove ? `${fmtShortDate(r.from)} close → ${fmtShortDate(r.to)} close` : ""),
    },
];

export function ErnView({ e, today = new Date() }: { e: main.CompanyEarnings; today?: Date }) {
    const past = e.reports.filter((r) => !r.upcoming);
    const next = e.reports.filter((r) => r.upcoming).sort((a, b) => (a.reported < b.reported ? -1 : 1));
    const moves = past.filter((r) => r.hasMove).map((r) => r.move);
    const largest = Math.max(0, ...moves.map(Math.abs));
    const summary = moveSummary(moves);
    const columns = ernColumns.map((c) =>
        c.key === "move"
            ? { ...c, render: (r: main.EarningsRow) => <MoveCell move={r.move} has={r.hasMove} largest={largest} /> }
            : c,
    );
    return (
        <div className="research-view">
            <h2 className="research-title">
                <span className="research-ticker">{e.ticker} ERN</span>
            </h2>
            {next.length > 0 && (
                <div className="rt-next">
                    <span className="research-dim">Next report</span>
                    <strong>{fmtDate(next[0].reported)}</strong>
                    {fmtTime(next[0].reported) && <span>{fmtTime(next[0].reported)} ET</span>}
                    <TimingTag timing={next[0].timing} />
                    <span className="research-dim">{countdown(next[0].reported, today)}</span>
                </div>
            )}
            {summary && (
                <div className="rt-stats">
                    <div>
                        <span className="research-dim">Reports measured</span>
                        <strong>{moves.length}</strong>
                    </div>
                    <div>
                        <span className="research-dim">Average move (either way)</span>
                        <strong>{(summary.avgAbs * 100).toFixed(2)}%</strong>
                    </div>
                    <div>
                        <span className="research-dim">Up / down</span>
                        <strong>
                            <span className="pos">{summary.up}</span> / <span className="neg">{summary.down}</span>
                        </strong>
                    </div>
                </div>
            )}
            <ResearchTable
                label={`${e.ticker} earnings reports`}
                rows={past}
                columns={columns}
                rowKey={(r) => r.reported}
                initialSort={{ key: "date", dir: "desc" }}
            />
            {past.length > 0 && (
                <p className="research-dim">
                    The move runs from the last close before the report to the first close after
                    it, from this database's bars. A report during the session mixes in that
                    morning's trading. Click a column to sort.
                </p>
            )}
            <Notes notes={e.notes} />
        </div>
    );
}

export function FilingsView({
    c,
    filter,
    onFilter,
    selected,
    onSelect,
}: {
    c: main.CompanyFilings;
    filter: FilingFilter;
    onFilter: (f: FilingFilter) => void;
    selected: string;
    onSelect: (row: main.FilingRow) => void;
}) {
    const shown = filterFilings(c.filings, filter);
    return (
        <div className="research-view">
            <h2 className="research-title">
                {c.company || c.ticker} <span className="research-ticker">{c.ticker} CN</span>
            </h2>
            {c.filings.length > 0 && (
                <div className="research-toggle" role="group" aria-label="Show">
                    {FILING_FILTERS.map((f) => (
                        <button
                            key={f.id}
                            type="button"
                            className={filter === f.id ? "active" : ""}
                            onClick={() => onFilter(f.id)}
                        >
                            {f.label}
                        </button>
                    ))}
                </div>
            )}
            {c.filings.length > 0 && shown.length === 0 && (
                <p className="research-dim">None of the recent filings are of this kind.</p>
            )}
            {shown.length > 0 && (
                <ul className="filing-list">
                    {shown.map((f) => (
                        <li key={f.accession}>
                            <button
                                type="button"
                                className={`filing-row${selected === f.accession ? " active" : ""}`}
                                onClick={() => onSelect(f)}
                            >
                                <span className="filing-when">{f.accepted || f.filed}</span>
                                <span className={`filing-form kind-${f.kind}`}>{f.form}</span>
                                <span className="filing-title">{f.title}</span>
                            </button>
                        </li>
                    ))}
                </ul>
            )}
            {c.filings.length > 0 && (
                <p className="research-dim">
                    Straight from SEC EDGAR: the company's own filings, newest first, timed when EDGAR
                    accepted them (US Eastern) — the moment each became public.
                </p>
            )}
            <Notes notes={c.notes} />
        </div>
    );
}

export function ReaderView({
    filing,
    docs,
    doc,
    loading,
    onOpen,
}: {
    filing: main.FilingRow;
    docs: main.FilingDocument[];
    doc: main.ReaderDoc | null;
    loading: boolean;
    onOpen: (url: string) => void;
}) {
    return (
        <div className="reader">
            <div className="reader-head">
                <strong>{filing.title}</strong>
                <span className="research-dim">
                    {filing.form} · accepted {filing.accepted || filing.filed}
                </span>
                <button
                    type="button"
                    className="link-button"
                    onClick={() => openExternal(filing.indexUrl)}
                    title="Open this filing on sec.gov in your browser"
                >
                    On sec.gov ↗
                </button>
            </div>
            {docs.length > 1 && (
                <div className="research-toggle" role="group" aria-label="Document">
                    {docs.map((d) => (
                        <button
                            key={d.url}
                            type="button"
                            className={doc?.url === d.url ? "active" : ""}
                            onClick={() => onOpen(d.url)}
                            title={d.name}
                        >
                            {d.type}
                            {d.description && d.description !== d.type ? ` — ${d.description}` : ""}
                        </button>
                    ))}
                </div>
            )}
            {loading && <p className="research-dim">Reading from SEC EDGAR…</p>}
            {!loading && doc?.error && <div className="coverage-msg err">{doc.error}</div>}
            {!loading && doc && !doc.error && (
                <>
                    <pre className="reader-text">{doc.text}</pre>
                    {doc.truncated && (
                        <p className="research-dim">
                            Long document: the rest is on sec.gov.
                        </p>
                    )}
                </>
            )}
        </div>
    );
}

export function EcoView({ e }: { e: main.EconomicCalendar }) {
    const past = e.meetings.filter((m) => !m.upcoming);
    const next = e.meetings.filter((m) => m.upcoming);
    return (
        <div className="research-view">
            <h2 className="research-title">
                The economy <span className="research-ticker">ECO</span>
            </h2>
            {e.readings.length > 0 && (
                <div className="research-table-wrap">
                    <table className="research-table eco-table">
                        <thead>
                            <tr>
                                <th>Indicator</th>
                                <th>Latest</th>
                                <th>Change</th>
                                <th>Over a year</th>
                                <th>Published</th>
                                <th>Next release</th>
                            </tr>
                        </thead>
                        <tbody>
                            {e.readings.map((r) => (
                                <tr key={r.id}>
                                    <th scope="row">
                                        <button
                                            type="button"
                                            className="link-button"
                                            onClick={() => openExternal(r.url)}
                                            title={`${r.id} on FRED, from the ${r.releaseName} release`}
                                        >
                                            {r.name}
                                        </button>
                                        <span className={`eco-badge${r.source === "database" ? " stale" : ""}`}>
                                            {r.source === "database" ? "Database copy" : "Official statistic"}
                                        </span>
                                    </th>
                                    <td>
                                        <strong>{formatEcoValue(r.kind, r.units, r.value)}</strong>{" "}
                                        <span className="research-dim">{r.period}</span>
                                    </td>
                                    <td title={r.hasPrior ? `From ${formatEcoValue(r.kind, r.units, r.prior)} in ${r.priorPeriod}` : ""}>
                                        {r.hasPrior ? formatEcoChange(r.kind, r.units, r.change) : "—"}
                                        {r.hasPrior && <span className="research-dim"> vs {r.priorPeriod}</span>}
                                    </td>
                                    <td>{r.hasYearChange ? formatEcoChange(r.kind, r.units, r.yearChange) : "—"}</td>
                                    <td
                                        title={r.publishedExact
                                            ? "From FRED's release calendar"
                                            : "Estimated from the release's usual timing"}
                                    >
                                        {r.publishedExact ? r.published : `≈ ${r.published}`}
                                    </td>
                                    <td className={r.nextRelease ? "" : "research-dim"}>{r.nextRelease || "—"}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
            {e.readings.length > 0 && (
                <p className="research-dim">
                    Official statistics from the agencies that produce them, as republished unchanged
                    by FRED (St. Louis Fed). Each reading names the period it describes and when it
                    came out — a number is only knowable from the day it is published.
                </p>
            )}

            {e.meetings.length > 0 && (
                <section className="research-section">
                    <h3>FOMC meetings</h3>
                    <ul className="research-list eco-meetings">
                        {[...past, ...next].map((m) => (
                            <li key={m.start} className={m.upcoming ? "research-upcoming" : ""}>
                                <strong>{m.dates}</strong>
                                {m.projections && <span className="research-dim"> · with economic projections</span>}
                                {m.note && <span className="research-dim"> · {m.note}</span>}
                                {m.upcoming && <span className="research-dim"> · upcoming</span>}
                                {m.statementUrl && (
                                    <>
                                        {" "}
                                        <button
                                            type="button"
                                            className="link-button"
                                            onClick={() => openExternal(m.statementUrl)}
                                            title="The FOMC's statement on federalreserve.gov"
                                        >
                                            Statement ↗
                                        </button>
                                    </>
                                )}
                                {m.minutesUrl && (
                                    <>
                                        {" "}
                                        <button
                                            type="button"
                                            className="link-button"
                                            onClick={() => openExternal(m.minutesUrl)}
                                            title={m.minutesReleased ? `Minutes, released ${m.minutesReleased}` : "Minutes"}
                                        >
                                            Minutes ↗
                                        </button>
                                    </>
                                )}
                            </li>
                        ))}
                    </ul>
                    <p className="research-dim">
                        From the Federal Reserve's own calendar; a date ahead is tentative until the
                        meeting before it confirms it.{" "}
                        <button
                            type="button"
                            className="link-button"
                            onClick={() => openExternal(e.calendarUrl)}
                        >
                            Calendar on federalreserve.gov ↗
                        </button>
                    </p>
                </section>
            )}
            <Notes notes={e.notes} />
        </div>
    );
}

export function MyView({
    feed,
    holdings,
    onChange,
    selected,
    onSelect,
}: {
    feed: main.WatchFeed;
    // The Backtest form's tickers, offered for the list.
    holdings: string[];
    onChange: (tickers: string[]) => void;
    selected: string;
    onSelect: (row: main.FilingRow, cik: string) => void;
}) {
    const [adding, setAdding] = useState("");
    const offer = holdingsToAdd(holdings, feed.tickers);
    const add = () => {
        const more = adding.split(/[\s,]+/).map((s) => s.trim().toUpperCase()).filter(Boolean);
        if (more.length > 0) onChange([...feed.tickers, ...more]);
        setAdding("");
    };
    return (
        <div className="research-view">
            <h2 className="research-title">
                My watchlist <span className="research-ticker">MY</span>
            </h2>
            <div className="watch-chips">
                {feed.tickers.map((t) => (
                    <span key={t} className="watch-chip">
                        {t}
                        <button
                            type="button"
                            className="link-button"
                            aria-label={`Remove ${t}`}
                            title={`Remove ${t} from the watchlist`}
                            onClick={() => onChange(feed.tickers.filter((x) => x !== t))}
                        >
                            ×
                        </button>
                    </span>
                ))}
                <input
                    className="watch-add"
                    placeholder="Add tickers"
                    value={adding}
                    spellCheck={false}
                    onChange={(e) => setAdding(e.target.value)}
                    onKeyDown={(e) => {
                        if (e.key === "Enter") add();
                    }}
                />
                <button type="button" onClick={add} disabled={!adding.trim()}>
                    Add
                </button>
                {offer.length > 0 && (
                    <button
                        type="button"
                        onClick={() => onChange([...feed.tickers, ...offer])}
                        title={offer.join(", ")}
                    >
                        Add the form's holdings ({offer.length})
                    </button>
                )}
            </div>

            {feed.tickers.length === 0 && (
                <p className="research-dim">
                    The watchlist is empty. Add the companies you follow, and MY gathers what they
                    have filed and reported in one list.
                </p>
            )}

            {feed.upcoming.length > 0 && (
                <section className="research-section">
                    <h3>Coming up</h3>
                    <ul className="research-list">
                        {feed.upcoming.map((u) => (
                            <li key={`${u.ticker} ${u.when}`} className="research-upcoming">
                                <strong>{u.ticker}</strong> earnings {u.when} ET
                                {u.timing && <span className="research-dim"> · {u.timing}</span>}
                            </li>
                        ))}
                    </ul>
                </section>
            )}

            {feed.recent.length > 0 && (
                <ul className="filing-list watch-list">
                    {feed.recent.map((it) =>
                        it.filing ? (
                            <li key={`${it.ticker} ${it.filing.accession}`}>
                                <button
                                    type="button"
                                    className={`filing-row${selected === it.filing.accession ? " active" : ""}`}
                                    onClick={() => onSelect(it.filing!, it.cik)}
                                >
                                    <span className="filing-when">{it.when} ET</span>
                                    <span className="watch-ticker">{it.ticker}</span>
                                    <span className={`filing-form kind-${it.filing.kind}`}>{it.filing.form}</span>
                                    <span className="filing-title">{it.title}</span>
                                </button>
                            </li>
                        ) : (
                            <li key={`${it.ticker} ${it.when} earnings`}>
                                <div className="filing-row watch-earnings">
                                    <span className="filing-when">{it.when} ET</span>
                                    <span className="watch-ticker">{it.ticker}</span>
                                    <span className="filing-form">ERN</span>
                                    <span className="filing-title">
                                        {it.title}
                                        {it.timing && <span className="research-dim"> · {it.timing}</span>}
                                        {it.hasMove && (
                                            <span className={it.move >= 0 ? "pos" : "neg"}> {formatMove(it.move)}</span>
                                        )}
                                    </span>
                                </div>
                            </li>
                        ),
                    )}
                </ul>
            )}
            {feed.tickers.length > 0 && (
                <p className="research-dim">
                    Since {feed.since}: each company's SEC events, reports and proxy statements, and
                    its earnings reports with the price's answer, newest first, US Eastern. Click a
                    filing to read it.
                </p>
            )}
            <Notes notes={feed.notes} />
        </div>
    );
}

export function SearchView({
    s,
    range,
    onRange,
    order,
    onOrder,
    onForm,
    onPage,
    selected,
    onSelect,
}: {
    s: main.FilingSearch;
    range: SearchRange;
    onRange: (r: SearchRange) => void;
    order: "newest" | "relevance";
    onOrder: (o: "newest" | "relevance") => void;
    // One form to narrow to, or "" for all.
    onForm: (form: string) => void;
    onPage: (from: number) => void;
    selected: string;
    onSelect: (hit: main.SearchHitRow) => void;
}) {
    const hits = orderHits(s.hits, order);
    const narrowed = s.forms.length === 1 && s.hits.every((h) => h.form.replace("/A", "") === s.forms[0].form);
    return (
        <div className="research-view">
            <h2 className="research-title">
                {s.query}{" "}
                <span className="research-dim">in {s.company || s.ticker || "every filer's text"}</span>{" "}
                <span className="research-ticker">FTS</span>
            </h2>
            <div className="research-toggle" role="group" aria-label="When">
                {SEARCH_RANGES.map((r) => (
                    <button
                        key={r.id}
                        type="button"
                        className={range === r.id ? "active" : ""}
                        onClick={() => onRange(r.id)}
                    >
                        {r.label}
                    </button>
                ))}
            </div>
            {s.hits.length > 1 && (
                <div className="research-toggle" role="group" aria-label="Order">
                    <button type="button" className={order === "newest" ? "active" : ""} onClick={() => onOrder("newest")}>
                        Newest first
                    </button>
                    <button
                        type="button"
                        className={order === "relevance" ? "active" : ""}
                        onClick={() => onOrder("relevance")}
                    >
                        Most relevant
                    </button>
                </div>
            )}
            {s.forms.length > 1 && (
                <div className="research-toggle" role="group" aria-label="Form">
                    {s.forms.slice(0, 8).map((f) => (
                        <button key={f.form} type="button" onClick={() => onForm(f.form)} title={`Only ${f.form}`}>
                            {f.form} <span className="research-dim">{f.count.toLocaleString("en-US")}</span>
                        </button>
                    ))}
                </div>
            )}
            {narrowed && (
                <button type="button" className="link-button" onClick={() => onForm("")}>
                    All forms
                </button>
            )}
            {s.total > 0 && (
                <p className="research-dim">
                    {s.total.toLocaleString("en-US")}
                    {s.atLeast ? "+" : ""} matching documents
                    {s.total > s.hits.length
                        ? `; ${s.from + 1}–${s.from + s.hits.length} shown`
                        : ""}
                    .
                </p>
            )}
            {hits.length > 0 && (
                <ul className="filing-list search-list">
                    {hits.map((h) => (
                        <li key={`${h.accession}:${h.fileName}`}>
                            <button
                                type="button"
                                className={`filing-row${selected === `${h.accession}:${h.fileName}` ? " active" : ""}`}
                                onClick={() => onSelect(h)}
                            >
                                <span className="filing-when">{h.filed}</span>
                                <span className={`filing-form kind-${h.kind}`}>{h.form}</span>
                                <span className="filing-title">
                                    {!s.ticker && <strong>{h.company} · </strong>}
                                    {h.title}
                                    {h.docType && h.docType !== h.form && (
                                        <span className="research-dim"> · in {h.docType}</span>
                                    )}
                                    {h.others.length > 0 && (
                                        <span className="research-dim"> · with {h.others.join(", ")}</span>
                                    )}
                                </span>
                            </button>
                        </li>
                    ))}
                </ul>
            )}
            {(s.from > 0 || s.total > s.from + s.hits.length) && (
                <div className="research-toggle" role="group" aria-label="Page">
                    <button type="button" disabled={s.from === 0} onClick={() => onPage(Math.max(0, s.from - 100))}>
                        Previous 100
                    </button>
                    <button
                        type="button"
                        disabled={s.total <= s.from + s.hits.length}
                        onClick={() => onPage(s.from + 100)}
                    >
                        Next 100
                    </button>
                </div>
            )}
            {s.hits.length > 0 && (
                <p className="research-dim">
                    EDGAR's own full-text search over every filing since 2001. Each row is a document
                    whose text matches; click it to read that document. Quotes search for an exact phrase.
                </p>
            )}
            <Notes notes={s.notes} />
        </div>
    );
}

type Result =
    | { fn: "DES"; data: main.CompanyOverview }
    | { fn: "FA"; data: main.CompanyFinancials }
    | { fn: "ERN"; data: main.CompanyEarnings }
    | { fn: "CN"; data: main.CompanyFilings }
    | { fn: "ECO"; data: main.EconomicCalendar }
    | { fn: "MY"; data: main.WatchFeed }
    | { fn: "FTS"; data: main.FilingSearch };

export default function Research({
    dbPath,
    focusSignal = 0,
    holdings = [],
}: {
    dbPath: string;
    // Bumped by the command palette to put the cursor in the command line.
    focusSignal?: number;
    // The Backtest form's tickers: what a first watchlist is seeded from.
    holdings?: string[];
}) {
    const [input, setInput] = useState("");
    const [ticker, setTicker] = useState("");
    const [fn, setFn] = useState<ResearchFn>("DES");
    const [freq, setFreq] = useState<"annual" | "quarterly">("annual");
    const [result, setResult] = useState<Result | null>(null);
    const [loading, setLoading] = useState(false);
    const [pool, setPool] = useState<string[]>([]);
    const [names, setNames] = useState<Record<string, string>>({});
    const inputRef = useRef<HTMLInputElement>(null);
    // CN: the filter, the filing open in the reader, its documents and text.
    const [filter, setFilter] = useState<FilingFilter>("news");
    const [filing, setFiling] = useState<main.FilingRow | null>(null);
    const [docs, setDocs] = useState<main.FilingDocument[]>([]);
    const [doc, setDoc] = useState<main.ReaderDoc | null>(null);
    const [reading, setReading] = useState(false);
    // Only the newest request may land, so a slow document cannot replace
    // the one clicked after it.
    const readSeq = useRef(0);
    // MY: bumped after the watchlist is saved, to read the feed again.
    const [watchVersion, setWatchVersion] = useState(0);
    // FTS: the search as typed, and how it is narrowed.
    const [searchText, setSearchText] = useState("");
    const [searchRange, setSearchRange] = useState<SearchRange>("year");
    const [searchOrder, setSearchOrder] = useState<"newest" | "relevance">("newest");
    const [searchForm, setSearchForm] = useState("");
    const [searchFrom, setSearchFrom] = useState(0);
    // Which search hit the reader shows: accession:file.
    const [hitKey, setHitKey] = useState("");
    // The holdings as of the first MY, for seeding; a ref so editing the form
    // does not refetch the feed.
    const holdingsRef = useRef(holdings);
    holdingsRef.current = holdings;

    useEffect(() => {
        if (focusSignal > 0) inputRef.current?.focus();
    }, [focusSignal]);

    // The symbols and company names, for resolving "apple" to AAPL.
    useEffect(() => {
        if (!dbPath) return;
        let cancelled = false;
        ListTickers(dbPath)
            .then((ts) => !cancelled && setPool(ts ?? []))
            .catch(() => !cancelled && setPool([]));
        TickerNames(dbPath)
            .then((n) => !cancelled && setNames(n ?? {}))
            .catch(() => !cancelled && setNames({}));
        return () => {
            cancelled = true;
        };
    }, [dbPath]);

    useEffect(() => {
        if (!dbPath || (!ticker && !isMarketFunction(fn))) return;
        let cancelled = false;
        setLoading(true);
        const done = (r: Result) => {
            if (!cancelled) {
                setResult(r);
                setLoading(false);
            }
        };
        const fail = () => !cancelled && setLoading(false);
        if (fn === "FTS") {
            SearchFilings(dbPath, searchText, ticker, searchForm ? [searchForm] : [],
                searchSince(searchRange, new Date()), searchFrom)
                .then((data) => done({ fn: "FTS", data }))
                .catch(fail);
        } else if (fn === "MY") {
            // A user with no saved watchlist starts from the form's holdings.
            LoadWatchlist()
                .then((w) =>
                    !w.saved && holdingsToAdd(holdingsRef.current, []).length > 0
                        ? SaveWatchlist(holdingsToAdd(holdingsRef.current, []))
                        : w,
                )
                .then((w) => WatchlistFeed(dbPath, w.tickers ?? []))
                .then((data) => done({ fn: "MY", data }))
                .catch(fail);
        } else if (fn === "ECO") {
            EconomicReleases(dbPath).then((data) => done({ fn, data })).catch(fail);
        } else if (fn === "DES") {
            CompanyOverview(dbPath, ticker).then((data) => done({ fn, data })).catch(fail);
        } else if (fn === "FA") {
            CompanyFinancials(dbPath, ticker, freq).then((data) => done({ fn, data })).catch(fail);
        } else if (fn === "CN") {
            CompanyFilings(dbPath, ticker).then((data) => done({ fn, data })).catch(fail);
        } else {
            CompanyEarnings(dbPath, ticker).then((data) => done({ fn, data })).catch(fail);
        }
        return () => {
            cancelled = true;
        };
    }, [ticker, fn, freq, dbPath, watchVersion, searchText, searchRange, searchForm, searchFrom]);

    // A new company or view closes the reader.
    useEffect(() => {
        setFiling(null);
        setDocs([]);
        setDoc(null);
    }, [ticker, fn, searchText]);

    function saveWatchlist(tickers: string[]) {
        SaveWatchlist(tickers)
            .then(() => setWatchVersion((n) => n + 1))
            .catch(() => setWatchVersion((n) => n + 1));
    }

    function openDocument(url: string) {
        const seq = ++readSeq.current;
        setReading(true);
        ReadFiling(url)
            .then((d) => {
                if (seq === readSeq.current) {
                    setDoc(d);
                    setReading(false);
                }
            })
            .catch((e) => {
                if (seq === readSeq.current) {
                    setDoc({ url, text: "", truncated: false, error: String(e) });
                    setReading(false);
                }
            });
    }

    // open, when given, is the document to show instead of the filing's
    // first: a search opens the document that matched.
    function selectFiling(row: main.FilingRow, cik: string, open = "") {
        const seq = ++readSeq.current;
        setFiling(row);
        setDocs([]);
        setDoc(null);
        setReading(true);
        FilingDocuments(cik, row.accession)
            .then((list) => {
                if (seq !== readSeq.current) return;
                const ds = list.documents ?? [];
                setDocs(ds);
                const first = firstDocument(ds, row.form);
                if (open) {
                    openDocument(open);
                } else if (first) {
                    openDocument(first.url);
                } else if (row.primaryUrl) {
                    openDocument(row.primaryUrl);
                } else {
                    setDoc({ url: row.indexUrl, text: "", truncated: false,
                        error: list.error || "This filing has no readable document." });
                    setReading(false);
                }
            })
            .catch((e) => {
                if (seq !== readSeq.current) return;
                setDoc({ url: row.indexUrl, text: "", truncated: false, error: String(e) });
                setReading(false);
            });
    }

    const named = useMemo(() => (ticker && names[ticker] ? names[ticker] : ""), [ticker, names]);

    function submit() {
        const cmd = parseResearchCommand(input);
        if (!cmd) return;
        if (cmd.fn === "FTS") {
            const sym = cmd.query ? resolveQuery(cmd.query, pool, (q) => searchTickers(pool, names, q, 1)) : "";
            setTicker(sym);
            setFn("FTS");
            setSearchText(cmd.text ?? "");
            setSearchForm("");
            setSearchFrom(0);
            setHitKey("");
            setInput(sym ? `${cmd.text} ${sym}` : cmd.text ?? "");
            return;
        }
        if (isMarketFunction(cmd.fn)) {
            setFn(cmd.fn);
            setInput(cmd.fn);
            return;
        }
        const sym = resolveQuery(cmd.query, pool, (q) => searchTickers(pool, names, q, 1));
        setTicker(sym);
        setFn(cmd.fn);
        setInput(`${sym} ${cmd.fn}`);
    }

    function choose(next: ResearchFn) {
        setFn(next);
        if (isMarketFunction(next)) setInput(next);
        else if (ticker) setInput(`${ticker} ${next}`);
    }

    const error = result?.data.error ?? "";

    return (
        <div className="research">
            <div className="research-bar">
                <input
                    ref={inputRef}
                    className="research-input"
                    data-shortcut="research"
                    placeholder='A ticker or name, then DES, FA, ERN or CN (e.g. AAPL CN) · ECO · MY · "a phrase" to search every filing'
                    value={input}
                    spellCheck={false}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={(e) => {
                        if (e.key === "Enter") submit();
                    }}
                />
                <button className="primary" onClick={submit} disabled={!input.trim() || !dbPath}>
                    Look up
                </button>
            </div>
            <div className="research-fns" role="group" aria-label="Function">
                {RESEARCH_FUNCTIONS.map((f) => (
                    <button
                        key={f.code}
                        type="button"
                        className={fn === f.code ? "active" : ""}
                        disabled={!ticker && !f.market}
                        onClick={() => choose(f.code)}
                        title={f.hint}
                    >
                        <strong>{f.code}</strong> {f.label}
                    </button>
                ))}
                {named && !isMarketFunction(fn) && <span className="research-dim research-resolved">{ticker} — {named}</span>}
            </div>

            {!dbPath && <p className="research-empty">Choose a database first (the database chip in the top bar).</p>}
            {dbPath && !ticker && !isMarketFunction(fn) && (
                <div className="research-empty">
                    <p>Look a company up by ticker or name, type ECO for the economy, MY for your watchlist, or put words in quotes to search the text of every SEC filing since 2001 (add a ticker to search one company's). Everything shown comes from primary data — SEC filings, official statistics and the database's own prices — and says where it came from.</p>
                    <ul>
                        {RESEARCH_FUNCTIONS.map((f) => (
                            <li key={f.code}>
                                <strong>{f.code}</strong> — {f.hint}
                            </li>
                        ))}
                    </ul>
                </div>
            )}
            {loading && <p className="research-dim">Reading…</p>}
            {error && <div className="coverage-msg err">{error}</div>}
            {!loading && !error && result?.fn === "DES" && <DesView d={result.data} />}
            {!loading && !error && result?.fn === "FA" && <FaView f={result.data} onFrequency={setFreq} />}
            {!loading && !error && result?.fn === "ERN" && <ErnView e={result.data} />}
            {!loading && !error && result?.fn === "ECO" && <EcoView e={result.data} />}
            {!loading && !error && result?.fn === "FTS" && (
                <div className={filing ? "cn-split" : ""}>
                    <SearchView
                        s={result.data}
                        range={searchRange}
                        onRange={(r) => {
                            setSearchRange(r);
                            setSearchFrom(0);
                        }}
                        order={searchOrder}
                        onOrder={setSearchOrder}
                        onForm={(f) => {
                            setSearchForm(f);
                            setSearchFrom(0);
                        }}
                        onPage={setSearchFrom}
                        selected={filing ? hitKey : ""}
                        onSelect={(h) => {
                            setHitKey(`${h.accession}:${h.fileName}`);
                            selectFiling(h.filing, h.cik, h.url);
                        }}
                    />
                    {filing && (
                        <ReaderView
                            filing={filing}
                            docs={docs}
                            doc={doc}
                            loading={reading}
                            onOpen={openDocument}
                        />
                    )}
                </div>
            )}
            {!loading && !error && result?.fn === "MY" && (
                <div className={filing ? "cn-split" : ""}>
                    <MyView
                        feed={result.data}
                        holdings={holdings}
                        onChange={saveWatchlist}
                        selected={filing?.accession ?? ""}
                        onSelect={selectFiling}
                    />
                    {filing && (
                        <ReaderView
                            filing={filing}
                            docs={docs}
                            doc={doc}
                            loading={reading}
                            onOpen={openDocument}
                        />
                    )}
                </div>
            )}
            {!loading && !error && result?.fn === "CN" && (
                <div className={filing ? "cn-split" : ""}>
                    <FilingsView
                        c={result.data}
                        filter={filter}
                        onFilter={setFilter}
                        selected={filing?.accession ?? ""}
                        onSelect={(row) => selectFiling(row, result.data.cik)}
                    />
                    {filing && (
                        <ReaderView
                            filing={filing}
                            docs={docs}
                            doc={doc}
                            loading={reading}
                            onOpen={openDocument}
                        />
                    )}
                </div>
            )}
        </div>
    );
}
