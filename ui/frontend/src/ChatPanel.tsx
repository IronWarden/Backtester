import React, { useEffect, useRef, useState } from "react";
import {
    Chat,
    HasClaudeKey,
    ListChatModels,
    SaveClaudeKey,
} from "../wailsjs/go/main/App";
import { main } from "../wailsjs/go/models";
import { EventsOn } from "../wailsjs/runtime/runtime";

// Shown until the backend reports the real list (Claude models plus
// whatever the local Ollama server has pulled). Sonnet is the default:
// strong at code/SQL and cheap enough for iterative chat.
const FALLBACK_MODELS: main.ModelOption[] = [
    { id: "claude-sonnet-5", label: "Claude Sonnet 5" },
    { id: "claude-opus-4-8", label: "Claude Opus 4.8" },
    { id: "claude-haiku-4-5-20251001", label: "Claude Haiku 4.5" },
] as main.ModelOption[];

// Local (Ollama) models are prefixed "ollama:" by the backend and need no
// API key.
const isLocalModel = (id: string) => id.startsWith("ollama:");

// Remember the chosen model across app restarts.
const MODEL_STORAGE_KEY = "backtester-chat-model";

type Msg = {
    role: "user" | "assistant";
    content: string;
    // DB queries the assistant ran while producing this message.
    toolNotes?: string[];
    streaming?: boolean;
    error?: string;
};

type Props = {
    // Rendered width in px, already zoom-scaled; App owns it so the drag
    // handle beside the panel can adjust it.
    width: number;
    dbPath: string;
    cfgText: string;
    luaText: string;
    // Push a Claude-authored code block into the corresponding editor.
    onApplyToml: (toml: string) => void;
    onApplyLua: (lua: string) => void;
    onClose: () => void;
};

const STARTERS = [
    "Explain my current config and suggest improvements.",
    "Write a Lua strategy: RSI mean reversion with a 200-day SMA filter.",
    "Which tickers in my DB have data back to 2000?",
];

// --- Markdown-lite rendering -----------------------------------------------
// No markdown dependency: handles fenced code blocks (with insert/copy
// actions), headers, lists, bold and inline code — enough for chat replies.

function inlineParts(text: string, key: number): React.ReactNode {
    const nodes: React.ReactNode[] = [];
    // Split on `code` spans first, then **bold** inside the plain parts.
    const codeSplit = text.split(/(`[^`]+`)/g);
    codeSplit.forEach((part, i) => {
        if (part.startsWith("`") && part.endsWith("`") && part.length > 2) {
            nodes.push(<code key={`${key}-c${i}`}>{part.slice(1, -1)}</code>);
            return;
        }
        const boldSplit = part.split(/(\*\*[^*]+\*\*)/g);
        boldSplit.forEach((seg, j) => {
            if (seg.startsWith("**") && seg.endsWith("**") && seg.length > 4) {
                nodes.push(<strong key={`${key}-b${i}-${j}`}>{seg.slice(2, -2)}</strong>);
            } else if (seg) {
                nodes.push(seg);
            }
        });
    });
    return nodes;
}

function renderTextSegment(seg: string, keyBase: number): React.ReactNode[] {
    const out: React.ReactNode[] = [];
    const lines = seg.split("\n");
    let list: { ordered: boolean; items: React.ReactNode[] } | null = null;
    let para: string[] = [];
    let k = 0;

    const flushPara = () => {
        const text = para.join("\n").trim();
        para = [];
        if (text) out.push(<p key={`${keyBase}-p${k++}`}>{inlineParts(text, k)}</p>);
    };
    const flushList = () => {
        if (!list) return;
        const items = list.items;
        out.push(
            list.ordered ? (
                <ol key={`${keyBase}-l${k++}`}>{items}</ol>
            ) : (
                <ul key={`${keyBase}-l${k++}`}>{items}</ul>
            ),
        );
        list = null;
    };

    for (const line of lines) {
        const h = line.match(/^(#{1,4})\s+(.*)$/);
        const bullet = line.match(/^\s*[-*]\s+(.*)$/);
        const num = line.match(/^\s*\d+[.)]\s+(.*)$/);
        if (h) {
            flushPara();
            flushList();
            out.push(
                <div className={`chat-h chat-h${h[1].length}`} key={`${keyBase}-h${k++}`}>
                    {inlineParts(h[2], k)}
                </div>,
            );
        } else if (bullet || num) {
            flushPara();
            const ordered = !!num;
            if (!list || list.ordered !== ordered) {
                flushList();
                list = { ordered, items: [] };
            }
            list.items.push(
                <li key={`${keyBase}-i${k++}`}>
                    {inlineParts((bullet ?? num)![1], k)}
                </li>,
            );
        } else if (line.trim() === "") {
            flushPara();
            flushList();
        } else {
            flushList();
            para.push(line);
        }
    }
    flushPara();
    flushList();
    return out;
}

function CodeBlock({
    lang,
    code,
    busy,
    onApplyToml,
    onApplyLua,
}: {
    lang: string;
    code: string;
    busy: boolean;
    onApplyToml: (t: string) => void;
    onApplyLua: (t: string) => void;
}) {
    const [copied, setCopied] = useState(false);
    const applyTarget =
        lang === "toml" ? "TOML" : lang === "lua" ? "Lua" : null;
    return (
        <div className="chat-codeblock">
            <div className="chat-codebar">
                <span className="chat-lang">{lang || "code"}</span>
                <span className="chat-codebar-spacer" />
                {applyTarget && !busy && (
                    <button
                        onClick={() =>
                            applyTarget === "TOML" ? onApplyToml(code) : onApplyLua(code)
                        }
                    >
                        Insert into {applyTarget} editor
                    </button>
                )}
                <button
                    onClick={() => {
                        navigator.clipboard?.writeText(code);
                        setCopied(true);
                        setTimeout(() => setCopied(false), 1200);
                    }}
                >
                    {copied ? "Copied" : "Copy"}
                </button>
            </div>
            <pre>{code}</pre>
        </div>
    );
}

function Markdown({
    text,
    busy,
    onApplyToml,
    onApplyLua,
}: {
    text: string;
    busy: boolean;
    onApplyToml: (t: string) => void;
    onApplyLua: (t: string) => void;
}) {
    // Split into alternating [text, lang, code, text, lang, code, ...].
    const parts = text.split(/```(\w*)\n?([\s\S]*?)```/g);
    const nodes: React.ReactNode[] = [];
    for (let i = 0; i < parts.length; i += 3) {
        if (parts[i]) nodes.push(...renderTextSegment(parts[i], i));
        if (i + 2 < parts.length) {
            nodes.push(
                <CodeBlock
                    key={`cb${i}`}
                    lang={(parts[i + 1] || "").toLowerCase()}
                    code={parts[i + 2].replace(/\n$/, "")}
                    busy={busy}
                    onApplyToml={onApplyToml}
                    onApplyLua={onApplyLua}
                />,
            );
        }
    }
    return <>{nodes}</>;
}

// --- Panel -----------------------------------------------------------------

export default function ChatPanel({
    width,
    dbPath,
    cfgText,
    luaText,
    onApplyToml,
    onApplyLua,
    onClose,
}: Props) {
    const [msgs, setMsgs] = useState<Msg[]>([]);
    const [input, setInput] = useState("");
    const [busy, setBusy] = useState(false);
    const [models, setModels] = useState<main.ModelOption[]>(FALLBACK_MODELS);
    const [model, setModel] = useState<string>(FALLBACK_MODELS[0].id);
    // null = still checking; false shows the API-key setup form (only for
    // Claude models — local Ollama models need no key).
    const [hasKey, setHasKey] = useState<boolean | null>(null);
    const [keyInput, setKeyInput] = useState("");
    const [keyError, setKeyError] = useState("");
    const scrollRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        HasClaudeKey().then(setHasKey).catch(() => setHasKey(false));
        ListChatModels()
            .then((opts) => {
                if (!opts || opts.length === 0) return;
                setModels(opts);
                const stored = localStorage.getItem(MODEL_STORAGE_KEY);
                if (stored && opts.some((o) => o.id === stored)) {
                    setModel(stored);
                }
            })
            .catch(() => {});
    }, []);

    function pickModel(id: string) {
        setModel(id);
        try {
            localStorage.setItem(MODEL_STORAGE_KEY, id);
        } catch {}
    }

    // Streamed deltas and tool notices append to the trailing assistant
    // message. Registered once; functional setState keeps them race-free.
    useEffect(() => {
        const offDelta = EventsOn("chat:delta", (chunk: string) => {
            setMsgs((ms) => {
                const last = ms[ms.length - 1];
                if (!last || last.role !== "assistant" || !last.streaming) return ms;
                return [
                    ...ms.slice(0, -1),
                    { ...last, content: last.content + chunk },
                ];
            });
        });
        const offTool = EventsOn("chat:tool", (query: string) => {
            setMsgs((ms) => {
                const last = ms[ms.length - 1];
                if (!last || last.role !== "assistant" || !last.streaming) return ms;
                return [
                    ...ms.slice(0, -1),
                    { ...last, toolNotes: [...(last.toolNotes ?? []), query] },
                ];
            });
        });
        return () => {
            offDelta();
            offTool();
        };
    }, []);

    // Keep the newest message in view as it streams.
    useEffect(() => {
        const el = scrollRef.current;
        if (el) el.scrollTop = el.scrollHeight;
    }, [msgs]);

    async function saveKey() {
        setKeyError("");
        try {
            await SaveClaudeKey(keyInput.trim());
            setKeyInput("");
            setHasKey(true);
        } catch (e: any) {
            setKeyError(String(e?.message ?? e));
        }
    }

    function send(text: string) {
        const trimmed = text.trim();
        if (!trimmed || busy) return;
        const history: Msg[] = [...msgs, { role: "user", content: trimmed }];
        setMsgs([...history, { role: "assistant", content: "", streaming: true }]);
        setInput("");
        setBusy(true);
        Chat(
            history.map((m) => ({ role: m.role, content: m.content })),
            model,
            cfgText,
            luaText,
            dbPath,
        )
            .then((final) => {
                setMsgs((ms) => {
                    const last = ms[ms.length - 1];
                    if (!last?.streaming) return ms;
                    return [
                        ...ms.slice(0, -1),
                        { ...last, content: final || last.content, streaming: false },
                    ];
                });
            })
            .catch((e: any) => {
                setMsgs((ms) => {
                    const last = ms[ms.length - 1];
                    if (!last?.streaming) return ms;
                    return [
                        ...ms.slice(0, -1),
                        { ...last, streaming: false, error: String(e?.message ?? e) },
                    ];
                });
            })
            .finally(() => setBusy(false));
    }

    // The API key only gates Claude models; a local model chats keyless.
    const needsKeySetup = hasKey === false && !isLocalModel(model);

    return (
        <div className="chat-panel" style={{ width }}>
            <div className="chat-head">
                <span className="chat-title">✦ Assistant</span>
                <select
                    value={model}
                    onChange={(e) => pickModel(e.target.value)}
                    disabled={busy}
                    title="Model"
                >
                    {models.map((m) => (
                        <option key={m.id} value={m.id}>
                            {m.label}
                        </option>
                    ))}
                </select>
                <span className="chat-codebar-spacer" />
                <button
                    onClick={() => setMsgs([])}
                    disabled={busy || msgs.length === 0}
                    title="Clear conversation"
                >
                    Clear
                </button>
                <button onClick={onClose} title="Close panel">
                    ×
                </button>
            </div>

            {needsKeySetup ? (
                <div className="chat-setup">
                    <p>
                        Claude models need an Anthropic API key. It will be stored in
                        the project's <code>.env</code> as{" "}
                        <code>ANTHROPIC_API_KEY</code>.
                    </p>
                    <p>
                        Create one at <code>console.anthropic.com</code> → API keys —
                        or pick a local Ollama model from the dropdown above, which
                        needs no key.
                    </p>
                    <input
                        type="password"
                        placeholder="sk-ant-…"
                        value={keyInput}
                        onChange={(e) => setKeyInput(e.target.value)}
                        onKeyDown={(e) => e.key === "Enter" && saveKey()}
                    />
                    <button disabled={!keyInput.trim()} onClick={saveKey}>
                        Save key
                    </button>
                    {keyError && <div className="chat-error">{keyError}</div>}
                </div>
            ) : (
                <>
                    <div className="chat-scroll" ref={scrollRef}>
                        {msgs.length === 0 && (
                            <div className="chat-empty">
                                <p>
                                    Ask about strategies, your configs, or the market data —
                                    I can query the database and write TOML/Lua straight
                                    into the editors.
                                </p>
                                {STARTERS.map((s) => (
                                    <button
                                        key={s}
                                        className="chat-starter"
                                        onClick={() => send(s)}
                                    >
                                        {s}
                                    </button>
                                ))}
                            </div>
                        )}
                        {msgs.map((m, i) => (
                            <div key={i} className={`chat-msg ${m.role}`}>
                                {m.toolNotes?.map((q, j) => (
                                    <div key={j} className="chat-toolnote" title={q}>
                                        ⛁ {q}
                                    </div>
                                ))}
                                {m.role === "assistant" ? (
                                    <Markdown
                                        text={m.content}
                                        busy={busy}
                                        onApplyToml={onApplyToml}
                                        onApplyLua={onApplyLua}
                                    />
                                ) : (
                                    <p>{m.content}</p>
                                )}
                                {m.streaming && !m.content && (
                                    <div className="chat-thinking">thinking…</div>
                                )}
                                {m.error && <div className="chat-error">{m.error}</div>}
                            </div>
                        ))}
                    </div>
                    <div className="chat-inputrow">
                        <textarea
                            placeholder="Ask about strategies, data, configs… (Enter to send)"
                            value={input}
                            rows={2}
                            onChange={(e) => setInput(e.target.value)}
                            onKeyDown={(e) => {
                                if (e.key === "Enter" && !e.shiftKey) {
                                    e.preventDefault();
                                    send(input);
                                }
                            }}
                        />
                        <button
                            className="run"
                            disabled={busy || !input.trim()}
                            onClick={() => send(input)}
                        >
                            {busy ? "…" : "Send"}
                        </button>
                    </div>
                </>
            )}
        </div>
    );
}
