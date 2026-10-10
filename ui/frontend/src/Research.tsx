// The Research tab: a company's SEC filings, read the way sec.gov shows them.
//
//   CN   the company's own SEC filings, in a reader (the default for a ticker)
//   MY   the watchlist: its companies' filings, newest first
//   NEWS the economic releases (BLS, BEA, Census) and the watchlist's
//        material 8-K events, one stream
//   "…"  full-text search of every filing since 2001, optionally one company's
//
// Nothing here is a live quote or a headline: everything is a primary source
// with its filing date. The views are exported and take plain data, so the
// smoke render exercises them without a backend; the default export owns the
// command line and the fetching.

import { useEffect, useMemo, useRef, useState, type CSSProperties, type MouseEvent, type PointerEvent, type ReactNode } from "react";
import {
    CompanyFilings,
    EconomicReleases,
    FilingDocuments,
    ListTickers,
    LoadWatchlist,
    ReadFiling,
    ReadRelease,
    SaveWatchlist,
    SearchFilings,
    TickerNames,
    WatchlistFeed,
} from "../wailsjs/go/main/App";
import { BrowserOpenURL } from "../wailsjs/runtime/runtime";
import type { main } from "../wailsjs/go/models";
import { searchTickers } from "./tickerSearch";
import { readStoredNumber, writeStored } from "./storage";
import { trackDrag } from "./frame";
import { ResearchTable, type Column } from "./ResearchTable";
import { fmtCountdown, fmtDate, todayISO } from "./researchFormat";
import {
    FILING_FILTERS,
    RESEARCH_FUNCTIONS,
    SEARCH_RANGES,
    filterFilings,
    firstDocument,
    isMarketFunction,
    holdingsToAdd,
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

function openExternal(url: string) {
    try {
        BrowserOpenURL(url);
    } catch {
        // No Wails runtime (the smoke harness): nothing to open.
    }
}

export function FormChip({ form, kind }: { form: string; kind: string }) {
    return <span className={`filing-form kind-${kind}`}>{form}</span>;
}

// Filings carry their filing date, not EDGAR's acceptance time, which is
// four hours off for some filers (T143).
const cnColumns: Column<main.FilingRow>[] = [
    {
        key: "filed", label: "Filed", firstDir: "desc",
        sortValue: (f) => `${f.filed} ${f.accepted}`, render: (f) => fmtDate(f.filed),
    },
    { key: "form", label: "Form", sortValue: (f) => f.form, render: (f) => <FormChip form={f.form} kind={f.kind} /> },
    { key: "what", label: "What", sortValue: (f) => f.title, render: (f) => f.title },
    {
        key: "period", label: "Period", firstDir: "desc",
        sortValue: (f) => f.reportDate, render: (f) => (f.reportDate ? fmtDate(f.reportDate) : ""),
    },
];

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
            <ResearchTable
                label={`${c.ticker} filings`}
                rows={shown}
                columns={cnColumns}
                rowKey={(f) => f.accession}
                initialSort={{ key: "filed", dir: "desc" }}
                onRowClick={onSelect}
                activeKey={selected}
            />
            {c.filings.length > 0 && (
                <p className="research-dim">
                    Straight from SEC EDGAR: the company&rsquo;s own filings, by filing date. Click one to
                    read it, a column to sort.
                </p>
            )}
            <Notes notes={c.notes} />
        </div>
    );
}

// A pane width the user drags, in em so it follows the app's text size,
// remembered across launches. Double-clicking the handle restores the default.
function usePaneWidth(key: string, initial: number, min: number, max: number) {
    const [width, setWidth] = useState(() =>
        readStoredNumber(key, initial, (n) => n >= min && n <= max),
    );
    useEffect(() => {
        writeStored(key, String(Math.round(width * 10) / 10));
    }, [key, width]);
    // sign is +1 when dragging right widens the pane, -1 when it narrows it.
    function startDrag(e: PointerEvent<HTMLElement>, sign: 1 | -1) {
        if (e.button !== 0) return;
        e.preventDefault();
        const em = parseFloat(getComputedStyle(e.currentTarget).fontSize) || 16;
        const start = width;
        trackDrag(e, "ew-resize", (dx) => {
            setWidth(Math.min(max, Math.max(min, start + (sign * dx) / em)));
        });
    }
    return { width, startDrag, reset: () => setWidth(initial) };
}

// The bar between two panes: drag to move it, double-click to reset.
function Splitter({
    label,
    onDrag,
    onReset,
}: {
    label: string;
    onDrag: (e: PointerEvent<HTMLElement>) => void;
    onReset: () => void;
}) {
    return (
        <div
            className="vresize-handle research-splitter"
            role="separator"
            aria-orientation="vertical"
            aria-label={label}
            onPointerDown={onDrag}
            onDoubleClick={onReset}
            title={`Drag to resize the ${label.toLowerCase()} · double-click to reset`}
        />
    );
}

const LIST_WIDTH_KEY = "backtester-research-list-width";
const CONTENTS_WIDTH_KEY = "backtester-reader-contents-width";

// A list of filings and, once one is open, the reader beside it: each in its
// own pane that scrolls by itself, with a bar between them to drag.
export function ResearchPanes({ list, reader }: { list: ReactNode; reader: ReactNode }) {
    const pane = usePaneWidth(LIST_WIDTH_KEY, 30, 14, 70);
    // One shape whether or not a filing is open, so opening one does not
    // remount the list and lose its sort and scroll.
    return (
        <div
            className={reader ? "research-stage split" : "research-stage"}
            style={{ "--list-w": `${pane.width}em` } as CSSProperties}
        >
            <div className="research-list-pane">{list}</div>
            {reader && (
                <Splitter label="Filings list" onDrag={(e) => pane.startDrag(e, 1)} onReset={pane.reset} />
            )}
            {reader}
        </div>
    );
}

// The reader's text size: a CSS zoom of the filing's own pt sizes.
const READER_ZOOM_KEY = "backtester-reader-zoom";
const READER_ZOOM_DEFAULT = 1.3;
const READER_ZOOM_MIN = 0.7;
const READER_ZOOM_MAX = 2.5;

// A document the reader could not open: the error, and nothing to show.
function failedRead(url: string, error: string): main.ReaderDoc {
    return { url, html: "", text: "", contents: [], truncated: false, error } as unknown as main.ReaderDoc;
}

// The publisher a NEWS release is read from, for the reader's link out.
const AGENCY_SITES: Record<string, string> = { BLS: "bls.gov", BEA: "bea.gov", Census: "census.gov" };

export function ReaderView({
    filing,
    release,
    docs,
    doc,
    loading,
    onOpen,
}: {
    // What is open: a filing, or (NEWS) an agency's release.
    filing?: main.FilingRow | null;
    release?: main.ReleaseRow | null;
    docs: main.FilingDocument[];
    doc: main.ReaderDoc | null;
    loading: boolean;
    onOpen: (url: string) => void;
}) {
    const paper = useRef<HTMLDivElement>(null);
    const hasContents = (doc?.contents?.length ?? 0) > 0;
    // Maximised, the reader fills the window the way a maximised results
    // window does: double-click its bar, or the button; Esc restores.
    const [max, setMax] = useState(false);
    // Filings print at 8-10pt; the reader enlarges them, remembered.
    const [zoom, setZoom] = useState(() =>
        readStoredNumber(READER_ZOOM_KEY, READER_ZOOM_DEFAULT, (n) => n >= READER_ZOOM_MIN && n <= READER_ZOOM_MAX),
    );
    // The contents column: as wide as the user drags it, or hidden.
    const contentsPane = usePaneWidth(CONTENTS_WIDTH_KEY, 15, 8, 40);
    const [showContents, setShowContents] = useState(true);

    useEffect(() => {
        if (!max) return;
        const onKey = (e: KeyboardEvent) => {
            if (e.key === "Escape") setMax(false);
        };
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [max]);

    function zoomBy(step: number) {
        const next = Math.round(Math.min(READER_ZOOM_MAX, Math.max(READER_ZOOM_MIN, zoom + step)) * 10) / 10;
        setZoom(next);
        writeStored(READER_ZOOM_KEY, String(next));
    }

    // Scroll the page, not the app, to a section: target is a prefixed id
    // (or name, in older filings); "" is the top.
    function jump(target: string) {
        const box = paper.current;
        if (!box) return;
        if (!target) {
            box.scrollTo({ top: 0 });
            return;
        }
        const esc = typeof CSS !== "undefined" && CSS.escape ? CSS.escape(target) : target;
        const el = box.querySelector(`[id="${esc}"], [name="${esc}"]`);
        if (!el) return;
        const top = el.getBoundingClientRect().top - box.getBoundingClientRect().top + box.scrollTop;
        box.scrollTo({ top: Math.max(0, top - 8) });
    }

    // The filing's own links: within the document they jump, outside it they
    // open in the browser. Never a navigation of the app itself.
    function onPaperClick(e: MouseEvent<HTMLDivElement>) {
        const a = (e.target as HTMLElement).closest("a");
        if (!a) return;
        const href = a.getAttribute("href");
        if (!href) return;
        e.preventDefault();
        if (href.startsWith("#")) jump(href.slice(1));
        else if (a.dataset.external) openExternal(href);
    }

    return (
        <div className={max ? "reader max" : "reader"}>
            <div
                className="reader-head"
                // The bar, not its controls: a double-click on a button means
                // that button.
                onDoubleClick={(e) => {
                    if ((e.target as HTMLElement).closest("button")) return;
                    setMax((m) => !m);
                }}
                title={max ? "Double-click to restore (Esc)" : "Double-click to maximise"}
            >
                {filing && (
                    <>
                        <strong>{filing.title}</strong>
                        <span className="research-dim">
                            {filing.form} · filed {fmtDate(filing.filed)}
                        </span>
                        <button
                            type="button"
                            className="link-button"
                            onClick={() => openExternal(filing.indexUrl)}
                            title="Open this filing on sec.gov in your browser"
                        >
                            On sec.gov ↗
                        </button>
                    </>
                )}
                {release && (
                    <>
                        <strong>{release.title}</strong>
                        <span className="research-dim">
                            {release.agency} · {release.series} · published {fmtDate(release.date)}
                        </span>
                        <button
                            type="button"
                            className="link-button"
                            onClick={() => openExternal(release.url)}
                            title={`Open this release on ${AGENCY_SITES[release.agency] ?? release.agency} in your browser`}
                        >
                            On {AGENCY_SITES[release.agency] ?? release.agency} ↗
                        </button>
                    </>
                )}
                <span className="reader-head-spacer" />
                {doc?.html && hasContents && (
                    <button
                        type="button"
                        className={showContents ? "active" : ""}
                        aria-pressed={showContents}
                        onClick={() => setShowContents((v) => !v)}
                        title={showContents ? "Hide the table of contents" : "Show the table of contents"}
                    >
                        Contents
                    </button>
                )}
                {doc?.html && (
                    <span className="reader-zoom" role="group" aria-label="Text size">
                        <button
                            type="button"
                            onClick={() => zoomBy(-0.1)}
                            disabled={zoom <= READER_ZOOM_MIN}
                            title="Smaller text"
                            aria-label="Smaller text"
                        >
                            A−
                        </button>
                        <span className="research-dim">{Math.round(zoom * 100)}%</span>
                        <button
                            type="button"
                            onClick={() => zoomBy(0.1)}
                            disabled={zoom >= READER_ZOOM_MAX}
                            title="Larger text"
                            aria-label="Larger text"
                        >
                            A+
                        </button>
                    </span>
                )}
                <button
                    type="button"
                    className="panel-icon"
                    onClick={() => setMax((m) => !m)}
                    title={max ? "Restore (Esc)" : "Maximise"}
                    aria-label={max ? "Restore reader" : "Maximise reader"}
                >
                    {max ? "❐" : "□"}
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
            {loading && <p className="research-dim">Reading from {release ? release.agency : "SEC EDGAR"}…</p>}
            {!loading && doc?.error && <div className="coverage-msg err">{doc.error}</div>}
            {!loading && doc && !doc.error && (
                <>
                    {doc.html ? (
                        <div
                            className="reader-body"
                            style={{ "--contents-w": `${contentsPane.width}em` } as CSSProperties}
                        >
                            {hasContents && showContents && (
                                <nav className="reader-contents" aria-label="Table of contents">
                                    <div className="reader-contents-head">Contents</div>
                                    <button type="button" onClick={() => jump("")}>
                                        Top of document
                                    </button>
                                    {doc.contents.map((s) => (
                                        <button
                                            key={s.target}
                                            type="button"
                                            className={/^part\b/i.test(s.label) ? "part" : ""}
                                            onClick={() => jump(s.target)}
                                        >
                                            {s.label}
                                        </button>
                                    ))}
                                </nav>
                            )}
                            {hasContents && showContents && (
                                <Splitter
                                    label="Contents"
                                    onDrag={(e) => contentsPane.startDrag(e, 1)}
                                    onReset={contentsPane.reset}
                                />
                            )}
                            <div ref={paper} className="reader-paper" onClick={onPaperClick}>
                                <div
                                    className={release ? "reader-page release" : "reader-page"}
                                    style={{ zoom }}
                                    dangerouslySetInnerHTML={{ __html: doc.html }}
                                />
                            </div>
                        </div>
                    ) : (
                        <pre className="reader-text">{doc.text}</pre>
                    )}
                    {release ? (
                        <p className="research-dim reader-foot">
                            {doc.truncated ? `Long release: the rest is on ${AGENCY_SITES[release.agency]}. ` : ""}
                            The release as {release.agency} publishes it, its own tables and charts, without the
                            site around it.
                        </p>
                    ) : (
                        <p className="research-dim reader-foot">
                            {doc.truncated ? "Long document: the rest is on sec.gov. " : ""}
                            The filing as SEC EDGAR publishes it{doc.html ? ", its own layout and tables" : ""}
                            {hasContents ? "; the contents link to each part" : ""}.
                        </p>
                    )}
                </>
            )}
        </div>
    );
}

const tickerColumn = <T extends { ticker: string }>(): Column<T> => ({
    key: "ticker", label: "Ticker", sortValue: (r) => r.ticker,
    render: (r) => <span className="watch-ticker">{r.ticker}</span>,
});

// Filings show their filing date only: EDGAR's acceptance times are wrong
// by four hours for some filers (T143).
const watchFilingColumns: Column<main.WatchItem>[] = [
    {
        key: "filed", label: "Filed", firstDir: "desc",
        sortValue: (it) => `${it.filing!.filed} ${it.when}`, render: (it) => fmtDate(it.filing!.filed),
    },
    tickerColumn<main.WatchItem>(),
    {
        key: "form", label: "Form", sortValue: (it) => it.filing!.form,
        render: (it) => <FormChip form={it.filing!.form} kind={it.filing!.kind} />,
    },
    { key: "what", label: "What", sortValue: (it) => it.title, render: (it) => it.title },
];

// MY is fetched one company at a time (each is its own EDGAR request, paced
// by the client), so the watchlist shows at once and fills in as answers
// arrive rather than waiting on the slowest company. mergeWatchFeeds joins
// the per-company feeds the way WatchlistFeed joins them in one call.
export const MY_MAX_FEED = 300; // ui/watchlist.go maxFeed
const NO_FILER = "No SEC filer under ";

export function mergeWatchFeeds(
    tickers: string[],
    parts: Record<string, main.WatchFeed>,
    since: string,
): main.WatchFeed {
    const recent: main.WatchItem[] = [];
    const notes: string[] = [];
    const noFiler: string[] = [];
    let noFilerTail = "";
    let error = "";
    for (const t of tickers) {
        const p = parts[t];
        if (!p) continue;
        if (p.error && !error) error = p.error;
        recent.push(...(p.recent ?? []));
        if (p.since) since = p.since;
        for (const n of p.notes ?? []) {
            // Each part names its own filer-less ticker; say it once for all.
            if (n.startsWith(NO_FILER)) {
                const dash = n.indexOf(" — ");
                if (dash > 0) {
                    noFiler.push(...n.slice(NO_FILER.length, dash).split(", "));
                    noFilerTail = n.slice(dash);
                    continue;
                }
            }
            if (!notes.includes(n)) notes.push(n);
        }
    }
    if (noFiler.length > 0) notes.push(NO_FILER + noFiler.join(", ") + noFilerTail);
    // Newest first, stable, as the backend sorts.
    recent.sort((a, b) => (a.when < b.when ? 1 : a.when > b.when ? -1 : 0));
    if (recent.length > MY_MAX_FEED) {
        recent.length = MY_MAX_FEED;
        notes.push(`Showing the newest ${MY_MAX_FEED} items; open a company's CN for the rest.`);
    }
    return { tickers, recent, since, notes, error } as unknown as main.WatchFeed;
}

export function MyView({
    feed,
    holdings,
    onChange,
    selected,
    onSelect,
    pending = [],
}: {
    feed: main.WatchFeed;
    // The Backtest form's tickers, offered for the list.
    holdings: string[];
    // Companies whose filings are still on their way from EDGAR.
    pending?: string[];
    onChange: (tickers: string[]) => void;
    selected: string;
    onSelect: (row: main.FilingRow, cik: string) => void;
}) {
    const [adding, setAdding] = useState("");
    const offer = holdingsToAdd(holdings, feed.tickers);
    const filings = feed.recent.filter((it) => !!it.filing);
    const activeFiling = filings.find((it) => it.filing!.accession === selected);
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
                        Add the form&rsquo;s holdings ({offer.length})
                    </button>
                )}
            </div>

            {feed.tickers.length === 0 && (
                <p className="research-dim">
                    The watchlist is empty. Add the companies you follow, and MY gathers what they
                    have filed in one list.
                </p>
            )}

            {pending.length > 0 && (
                <p className="research-dim research-pending">
                    Reading from SEC EDGAR… {feed.tickers.length - pending.length} of{" "}
                    {feed.tickers.length} companies in
                </p>
            )}

            {filings.length > 0 && (
                <section className="research-section">
                    <h3>Filings</h3>
                    <ResearchTable
                        label="Filings"
                        rows={filings}
                        columns={watchFilingColumns}
                        rowKey={(it) => `${it.ticker} ${it.filing!.accession}`}
                        initialSort={{ key: "filed", dir: "desc" }}
                        onRowClick={(it) => onSelect(it.filing!, it.cik)}
                        activeKey={activeFiling ? `${activeFiling.ticker} ${selected}` : ""}
                    />
                </section>
            )}
            {feed.tickers.length > 0 && (
                <p className="research-dim">
                    Since {fmtDate(feed.since)}: each company&rsquo;s SEC events, reports and proxy
                    statements, by filing date. Click a filing to read it, a column to sort.
                </p>
            )}
            <Notes notes={feed.notes} />
        </div>
    );
}

// NEWS: what the agencies published and what the watchlist's companies
// reported, in one stream. Releases are the BLS, BEA and Census notices
// (src/news keeps only the market-moving series); events are the material
// 8-Ks (edgar.Material: results, deals, bankruptcy, leadership, restatements,
// write-downs) from the same per-company feeds MY reads.

export type NewsFilter = "all" | "economy" | "watchlist";

const NEWS_FILTERS: { id: NewsFilter; label: string }[] = [
    { id: "all", label: "All" },
    { id: "economy", label: "Economy" },
    { id: "watchlist", label: "Watchlist" },
];

export type NewsRow = {
    key: string;
    // YYYY-MM-DD, and with the time for ordering within a day.
    date: string;
    when: string;
    // An agency (BLS, BEA, Census) or a ticker.
    source: string;
    series: string;
    title: string;
    summary: string;
    // A release's next date in its series, YYYY-MM-DD, or "".
    next: string;
    // A release opens at the agency; an event opens in the reader.
    url: string;
    item: main.WatchItem | null;
    release: main.ReleaseRow | null;
};

export function newsRows(econ: main.EconomicNews | null, watch: main.WatchFeed | null, filter: NewsFilter): NewsRow[] {
    const rows: NewsRow[] = [];
    if (filter !== "watchlist") {
        for (const r of econ?.releases ?? []) {
            rows.push({
                key: `${r.agency} ${r.when} ${r.url}`, date: r.date, when: r.when, source: r.agency,
                series: r.series, title: r.title, summary: r.summary, next: r.next, url: r.url, item: null,
                release: r,
            });
        }
    }
    if (filter !== "economy") {
        for (const it of watch?.recent ?? []) {
            if (!it.filing?.material) continue;
            rows.push({
                key: `${it.ticker} ${it.filing.accession}`, date: it.filing.filed, when: it.when,
                source: it.ticker, series: "", title: it.title, summary: "", next: "", url: "", item: it,
                release: null,
            });
        }
    }
    return rows;
}

// A release feed the backend could not be asked for at all.
function failedReleases(error: string): main.EconomicNews {
    return { releases: [], since: "", notes: [], error } as unknown as main.EconomicNews;
}

function newsColumns(today: string): Column<NewsRow>[] {
    return [
        { key: "date", label: "Date", firstDir: "desc", sortValue: (r) => r.when, render: (r) => fmtDate(r.date) },
        {
            key: "source", label: "Source", sortValue: (r) => r.source,
            title: (r) => (r.item ? `${r.source}'s SEC filing` : r.series),
            render: (r) => <span className={r.item ? "news-source event" : "news-source release"}>{r.source}</span>,
        },
        {
            key: "what", label: "What", sortValue: (r) => r.title,
            render: (r) => {
                const countdown = r.next ? fmtCountdown(r.next, today) : "";
                return (
                    <div className="news-what">
                        <div>
                            {r.item ? <FormChip form={r.item.filing!.form} kind={r.item.filing!.kind} /> : null}
                            {r.item ? " " : null}
                            {r.title}
                        </div>
                        {r.summary && <div className="news-summary">{r.summary}</div>}
                        {countdown && (
                            <div className="research-dim">
                                Next {r.series} release {fmtDate(r.next)} ({countdown})
                            </div>
                        )}
                    </div>
                );
            },
        },
    ];
}

export function NewsView({
    econ,
    watch,
    pending,
    selected,
    onSelect,
    selectedRelease = "",
    onOpenRelease = () => {},
    today = todayISO(),
}: {
    econ: main.EconomicNews | null;
    watch: main.WatchFeed | null;
    pending: string[];
    // The open filing's accession, or the open release's URL.
    selected: string;
    onSelect: (row: main.FilingRow, cik: string) => void;
    selectedRelease?: string;
    onOpenRelease?: (r: main.ReleaseRow) => void;
    today?: string;
}) {
    const [filter, setFilter] = useState<NewsFilter>("all");
    const rows = newsRows(econ, watch, filter);
    const tickers = watch?.tickers ?? [];
    const reading = econ === null || watch === null || pending.length > 0;
    const notes = [...(econ?.notes ?? []), ...(watch?.notes ?? [])];
    if (watch && tickers.length === 0) {
        notes.push("Your watchlist is empty, so no company events are shown. Add the companies you follow in MY.");
    }
    return (
        <div className="research-view">
            <h2 className="research-title">
                News <span className="research-ticker">NEWS</span>
            </h2>
            <div className="research-toggle" role="group" aria-label="Show">
                {NEWS_FILTERS.map((f) => (
                    <button
                        key={f.id}
                        type="button"
                        className={filter === f.id ? "active" : ""}
                        onClick={() => setFilter(f.id)}
                    >
                        {f.label}
                    </button>
                ))}
            </div>
            {econ === null && <p className="research-dim">Reading the BLS, BEA and Census release feeds…</p>}
            {econ?.error && (
                <div className="coverage-msg err">
                    {econ.error} Your watchlist&rsquo;s events still show below.
                </div>
            )}
            {pending.length > 0 && (
                <p className="research-dim research-pending">
                    Reading from SEC EDGAR… {tickers.length - pending.length} of {tickers.length} companies in
                </p>
            )}
            <ResearchTable
                label="News"
                rows={rows}
                columns={newsColumns(today)}
                rowKey={(r) => r.key}
                initialSort={{ key: "date", dir: "desc" }}
                onRowClick={(r) => (r.item ? onSelect(r.item.filing!, r.item.cik) : onOpenRelease(r.release!))}
                activeKey={
                    rows.find((r) =>
                        r.item ? r.item.filing?.accession === selected : !!selectedRelease && r.url === selectedRelease,
                    )?.key ?? ""
                }
                empty={reading ? undefined : "Nothing published in the last 90 days."}
            />
            <p className="research-dim">
                Since {fmtDate(econ?.since || watch?.since || "")}: the official release notices from BLS, BEA and
                Census (prices, jobs, GDP and spending, trade, housing, factory orders), each in the agency&rsquo;s own
                words, and the material events your watchlist&rsquo;s companies filed with the SEC (results, deals,
                bankruptcy, leadership changes, restatements, write-downs). No media, no commentary. Click either to
                read it here: a release as the agency publishes it, an event as the filing.
            </p>
            <Notes notes={notes} />
        </div>
    );
}

type SearchRow = main.SearchHitRow & { rank: number };

function searchColumns(everyFiler: boolean): Column<SearchRow>[] {
    const cols: Column<SearchRow>[] = [
        { key: "rank", label: "#", numeric: true, sortValue: (h) => h.rank, render: (h) => h.rank },
        { key: "filed", label: "Filed", firstDir: "desc", sortValue: (h) => h.filed, render: (h) => fmtDate(h.filed) },
    ];
    if (everyFiler) {
        cols.push({ key: "company", label: "Company", sortValue: (h) => h.company, render: (h) => h.company });
    }
    cols.push(
        { key: "form", label: "Form", sortValue: (h) => h.form, render: (h) => <FormChip form={h.form} kind={h.kind} /> },
        {
            key: "what", label: "What", sortValue: (h) => h.title,
            render: (h) => (
                <>
                    {h.title}
                    {h.others.length > 0 && <span className="research-dim"> · with {h.others.join(", ")}</span>}
                </>
            ),
        },
        {
            key: "doc", label: "Matched in", sortValue: (h) => h.docType,
            render: (h) => (h.docType && h.docType !== h.form ? h.docType : <span className="research-dim">the filing</span>),
        },
    );
    return cols;
}

export function SearchView({
    s,
    range,
    onRange,
    onForm,
    onPage,
    selected,
    onSelect,
}: {
    s: main.FilingSearch;
    range: SearchRange;
    onRange: (r: SearchRange) => void;
    // One form to narrow to, or "" for all.
    onForm: (form: string) => void;
    onPage: (from: number) => void;
    selected: string;
    onSelect: (hit: main.SearchHitRow) => void;
}) {
    // # is EDGAR's relevance rank, so sorting by it restores EDGAR's order.
    const rows: SearchRow[] = s.hits.map((h, i) => ({ ...h, rank: s.from + i + 1 }) as SearchRow);
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
            <ResearchTable
                label={`Search results for ${s.query}`}
                rows={rows}
                columns={searchColumns(!s.ticker)}
                rowKey={(h) => `${h.accession}:${h.fileName}`}
                initialSort={{ key: "filed", dir: "desc" }}
                onRowClick={onSelect}
                activeKey={selected}
            />
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
                    EDGAR&rsquo;s own full-text search over every filing since 2001. Each row is a document
                    whose text matches; click it to read that document. Sort by # for the order EDGAR
                    ranks by relevance. Quotes search for an exact phrase.
                </p>
            )}
            <Notes notes={s.notes} />
        </div>
    );
}

// A company's MY feed is reused this long -- the EDGAR client caches the
// same five minutes -- and this many are asked for at once (the client
// paces them all to SEC's limit regardless).
const MY_PART_TTL_MS = 5 * 60 * 1000;
const MY_CONCURRENCY = 4;

type Result =
    | { fn: "CN"; data: main.CompanyFilings }
    | { fn: "MY"; data: main.WatchFeed; pending: string[] }
    | { fn: "FTS"; data: main.FilingSearch }
    // Either half null until it first answers.
    | { fn: "NEWS"; econ: main.EconomicNews | null; watch: main.WatchFeed | null; pending: string[] };

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
    const [fn, setFn] = useState<ResearchFn>("CN");
    const [result, setResult] = useState<Result | null>(null);
    const [loading, setLoading] = useState(false);
    const [pool, setPool] = useState<string[]>([]);
    const [names, setNames] = useState<Record<string, string>>({});
    const inputRef = useRef<HTMLInputElement>(null);
    // CN: the filter, the filing open in the reader, its documents and text.
    const [filter, setFilter] = useState<FilingFilter>("news");
    const [filing, setFiling] = useState<main.FilingRow | null>(null);
    // NEWS: the release open in the reader, when it is a release.
    const [release, setRelease] = useState<main.ReleaseRow | null>(null);
    const [docs, setDocs] = useState<main.FilingDocument[]>([]);
    const [doc, setDoc] = useState<main.ReaderDoc | null>(null);
    const [reading, setReading] = useState(false);
    // Only the newest request may land, so a slow document cannot replace
    // the one clicked after it.
    const readSeq = useRef(0);
    // MY: bumped after the watchlist is saved, to read the feed again.
    const [watchVersion, setWatchVersion] = useState(0);
    // MY: each company's feed as it arrived, so editing the list fetches
    // only the companies it adds.
    const myParts = useRef<Record<string, { feed: main.WatchFeed; at: number }>>({});
    // FTS: the search as typed, and how it is narrowed.
    const [searchText, setSearchText] = useState("");
    const [searchRange, setSearchRange] = useState<SearchRange>("year");
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
        // An edited watchlist keeps its list on screen while it refetches.
        if (!((fn === "MY" || fn === "NEWS") && result?.fn === fn)) setLoading(true);
        const done = (r: Result) => {
            if (!cancelled) {
                setResult(r);
                setLoading(false);
            }
        };
        const fail = () => !cancelled && setLoading(false);
        // The watchlist's filings a company at a time (MY, and NEWS's
        // events): update is called with the list at once, then again as
        // each company's answer arrives.
        const loadWatch = (update: (feed: main.WatchFeed, pending: string[]) => void) => {
            // A user with no saved watchlist starts from the form's holdings.
            LoadWatchlist()
                .then((w) =>
                    !w.saved && holdingsToAdd(holdingsRef.current, []).length > 0
                        ? SaveWatchlist(holdingsToAdd(holdingsRef.current, []))
                        : w,
                )
                .then((w) => {
                    if (cancelled) return;
                    const tickers = w.tickers ?? [];
                    const parts = myParts.current;
                    const fresh = (t: string) =>
                        parts[t] && Date.now() - parts[t].at < MY_PART_TTL_MS;
                    const since = todayISO(new Date(Date.now() - 90 * 86400000));
                    const show = () => {
                        if (cancelled) return;
                        const have: Record<string, main.WatchFeed> = {};
                        for (const t of tickers) if (parts[t]) have[t] = parts[t].feed;
                        update(mergeWatchFeeds(tickers, have, since), tickers.filter((t) => !fresh(t)));
                    };
                    // The list at once, with whatever is already in hand.
                    show();
                    const queue = tickers.filter((t) => !fresh(t));
                    const worker = async () => {
                        for (let t = queue.shift(); t !== undefined && !cancelled; t = queue.shift()) {
                            const feed = await WatchlistFeed(dbPath, [t]).catch(
                                (e) => ({ tickers: [t], recent: [], since: "", notes: [`${t}: ${e}`], error: "" }) as unknown as main.WatchFeed,
                            );
                            parts[t] = { feed, at: Date.now() };
                            show();
                        }
                    };
                    for (let i = 0; i < MY_CONCURRENCY; i++) void worker();
                })
                .catch(fail);
        };
        if (fn === "FTS") {
            SearchFilings(dbPath, searchText, ticker, searchForm ? [searchForm] : [],
                searchSince(searchRange, new Date()), searchFrom)
                .then((data) => done({ fn: "FTS", data }))
                .catch(fail);
        } else if (fn === "MY") {
            loadWatch((data, pending) => done({ fn: "MY", data, pending }));
        } else if (fn === "NEWS") {
            // Two halves, each shown as it lands: the agencies' releases in
            // one call, the watchlist's events a company at a time.
            let econ: main.EconomicNews | null = null;
            let watch: main.WatchFeed | null = null;
            let pending: string[] = [];
            const show = () => done({ fn: "NEWS", econ, watch, pending });
            show();
            EconomicReleases()
                .then((e) => {
                    econ = e;
                    show();
                })
                .catch((e) => {
                    econ = failedReleases(String(e));
                    show();
                });
            loadWatch((data, p) => {
                watch = data;
                pending = p;
                show();
            });
        } else {
            CompanyFilings(dbPath, ticker).then((data) => done({ fn: "CN", data })).catch(fail);
        }
        return () => {
            cancelled = true;
        };
    }, [ticker, fn, dbPath, watchVersion, searchText, searchRange, searchForm, searchFrom]);

    // A new company or view closes the reader.
    useEffect(() => {
        setFiling(null);
        setRelease(null);
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
                    setDoc(failedRead(url, String(e)));
                    setReading(false);
                }
            });
    }

    // open, when given, is the document to show instead of the filing's
    // first: a search opens the document that matched.
    function selectFiling(row: main.FilingRow, cik: string, open = "") {
        const seq = ++readSeq.current;
        setFiling(row);
        setRelease(null);
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
                    setDoc(failedRead(row.indexUrl,
                        list.error || "This filing has no readable document."));
                    setReading(false);
                }
            })
            .catch((e) => {
                if (seq !== readSeq.current) return;
                setDoc(failedRead(row.indexUrl, String(e)));
                setReading(false);
            });
    }

    // A NEWS release, read in the app: the release alone, from the agency.
    function openRelease(r: main.ReleaseRow) {
        const seq = ++readSeq.current;
        setFiling(null);
        setRelease(r);
        setDocs([]);
        setDoc(null);
        setReading(true);
        ReadRelease(r.url)
            .then((d) => {
                if (seq === readSeq.current) {
                    setDoc(d);
                    setReading(false);
                }
            })
            .catch((e) => {
                if (seq === readSeq.current) {
                    setDoc(failedRead(r.url, String(e)));
                    setReading(false);
                }
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

    // NEWS has no single error: each half says what failed beside the other.
    const error = result && "data" in result ? result.data.error ?? "" : "";
    const readerPane = filing || release ? (
        <ReaderView filing={filing} release={release} docs={docs} doc={doc} loading={reading} onOpen={openDocument} />
    ) : null;

    return (
        <div className={filing || release ? "research reading" : "research"}>
            <div className="research-bar">
                <input
                    ref={inputRef}
                    className="research-input"
                    data-shortcut="research"
                    placeholder='A ticker or name for its filings (e.g. AAPL) · MY for your watchlist · NEWS · "a phrase" to search every filing'
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
                    <p>Look a company up by ticker or name to read its SEC filings, type MY for your watchlist&rsquo;s filings or NEWS for the economic releases and your watchlist&rsquo;s material events, or put words in quotes to search the text of every SEC filing since 2001 (add a ticker to search one company&rsquo;s). Everything shown comes straight from SEC EDGAR or the government agency that published it.</p>
                    <ul>
                        {RESEARCH_FUNCTIONS.map((f) => (
                            <li key={f.code}>
                                <strong>{f.code}</strong> — {f.hint}
                            </li>
                        ))}
                    </ul>
                </div>
            )}
            {loading && (
                <p className="research-dim">
                    {fn === "CN" && ticker ? `Reading ${named || ticker}’s filings from SEC EDGAR…` : "Reading…"}
                </p>
            )}
            {error && <div className="coverage-msg err">{error}</div>}
            {!loading && !error && result?.fn === "FTS" && (
                <ResearchPanes
                    list={
                        <SearchView
                            s={result.data}
                            range={searchRange}
                            onRange={(r) => {
                                setSearchRange(r);
                                setSearchFrom(0);
                            }}
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
                    }
                    reader={readerPane}
                />
            )}
            {!loading && !error && result?.fn === "MY" && (
                <ResearchPanes
                    list={
                        <MyView
                            feed={result.data}
                            pending={result.pending}
                            holdings={holdings}
                            onChange={saveWatchlist}
                            selected={filing?.accession ?? ""}
                            onSelect={selectFiling}
                        />
                    }
                    reader={readerPane}
                />
            )}
            {!loading && result?.fn === "NEWS" && (
                <ResearchPanes
                    list={
                        <NewsView
                            econ={result.econ}
                            watch={result.watch}
                            pending={result.pending}
                            selected={filing?.accession ?? ""}
                            onSelect={selectFiling}
                            selectedRelease={release?.url ?? ""}
                            onOpenRelease={openRelease}
                        />
                    }
                    reader={readerPane}
                />
            )}
            {!loading && !error && result?.fn === "CN" && (
                <ResearchPanes
                    list={
                        <FilingsView
                            c={result.data}
                            filter={filter}
                            onFilter={setFilter}
                            selected={filing?.accession ?? ""}
                            onSelect={(row) => selectFiling(row, result.data.cik)}
                        />
                    }
                    reader={readerPane}
                />
            )}
        </div>
    );
}
