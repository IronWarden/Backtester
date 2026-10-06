// The Research tab (T142a): look a company up the way a terminal does --
// a ticker, then a function -- and read facts from primary data only.
//
//   DES  what the company is (SEC EDGAR profile, index membership, the
//        database's own price history and its quality audit)
//   FA   reported financials, each figure as first FILED, with the date
//   ERN  report dates and how the price met each one
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
    FilingDocuments,
    ListTickers,
    ReadFiling,
    TickerNames,
} from "../wailsjs/go/main/App";
import { BrowserOpenURL } from "../wailsjs/runtime/runtime";
import type { main } from "../wailsjs/go/models";
import { searchTickers } from "./tickerSearch";
import {
    FILING_FILTERS,
    RESEARCH_FUNCTIONS,
    filterFilings,
    firstDocument,
    formatFigure,
    formatMove,
    formatPrice,
    parseResearchCommand,
    resolveQuery,
    type FilingFilter,
    type ResearchFn,
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

export function ErnView({ e }: { e: main.CompanyEarnings }) {
    return (
        <div className="research-view">
            <h2 className="research-title">
                <span className="research-ticker">{e.ticker} ERN</span>
            </h2>
            {e.reports.length > 0 && (
                <div className="research-table-wrap">
                    <table className="research-table">
                        <thead>
                            <tr>
                                <th>Reported (US Eastern)</th>
                                <th>When</th>
                                <th>Price move</th>
                                <th>Measured</th>
                            </tr>
                        </thead>
                        <tbody>
                            {e.reports.map((r) => (
                                <tr key={r.reported} className={r.upcoming ? "research-upcoming" : ""}>
                                    <td>{r.reported}</td>
                                    <td>{r.upcoming ? `upcoming, ${r.timing}` : r.timing}</td>
                                    <td className={r.hasMove ? (r.move >= 0 ? "pos" : "neg") : "research-dim"}>
                                        {r.hasMove ? formatMove(r.move) : "—"}
                                    </td>
                                    <td className="research-dim">
                                        {r.hasMove ? `close ${r.from} → close ${r.to}` : ""}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
            {e.reports.length > 0 && (
                <p className="research-dim">
                    The move runs from the last close before the report to the first close after
                    it, from this database's bars. A report during the session mixes in that
                    morning's trading.
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

type Result =
    | { fn: "DES"; data: main.CompanyOverview }
    | { fn: "FA"; data: main.CompanyFinancials }
    | { fn: "ERN"; data: main.CompanyEarnings }
    | { fn: "CN"; data: main.CompanyFilings };

export default function Research({
    dbPath,
    focusSignal = 0,
}: {
    dbPath: string;
    // Bumped by the command palette to put the cursor in the command line.
    focusSignal?: number;
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
        if (!ticker || !dbPath) return;
        let cancelled = false;
        setLoading(true);
        const done = (r: Result) => {
            if (!cancelled) {
                setResult(r);
                setLoading(false);
            }
        };
        const fail = () => !cancelled && setLoading(false);
        if (fn === "DES") {
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
    }, [ticker, fn, freq, dbPath]);

    // A new company closes the reader.
    useEffect(() => {
        setFiling(null);
        setDocs([]);
        setDoc(null);
    }, [ticker]);

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

    function selectFiling(row: main.FilingRow) {
        const cik = result?.fn === "CN" ? result.data.cik : "";
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
                if (first) {
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
        const sym = resolveQuery(cmd.query, pool, (q) => searchTickers(pool, names, q, 1));
        setTicker(sym);
        setFn(cmd.fn);
        setInput(`${sym} ${cmd.fn}`);
    }

    function choose(next: ResearchFn) {
        setFn(next);
        if (ticker) setInput(`${ticker} ${next}`);
    }

    const error = result?.data.error ?? "";

    return (
        <div className="research">
            <div className="research-bar">
                <input
                    ref={inputRef}
                    className="research-input"
                    data-shortcut="research"
                    placeholder="A ticker or company name, then DES, FA, ERN or CN — e.g. AAPL CN"
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
                        disabled={!ticker}
                        onClick={() => choose(f.code)}
                        title={f.hint}
                    >
                        <strong>{f.code}</strong> {f.label}
                    </button>
                ))}
                {named && <span className="research-dim research-resolved">{ticker} — {named}</span>}
            </div>

            {!dbPath && <p className="research-empty">Choose a database first (the database chip in the top bar).</p>}
            {dbPath && !ticker && (
                <div className="research-empty">
                    <p>Look a company up by ticker or name. Everything shown comes from primary data — SEC filings and the database's own prices — and says where it came from.</p>
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
            {!loading && !error && result?.fn === "CN" && (
                <div className={filing ? "cn-split" : ""}>
                    <FilingsView
                        c={result.data}
                        filter={filter}
                        onFilter={setFilter}
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
        </div>
    );
}
