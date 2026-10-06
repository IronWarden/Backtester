import { useEffect, useState } from "react";

// "7s", "59s", "1m 05s", "12m 00s". Whole seconds: a tenth ticking past is
// noise, and a run short enough to need one is over before it can be read.
export function formatElapsed(ms: number): string {
    const s = Math.max(0, Math.floor(ms / 1000));
    if (s < 60) return `${s}s`;
    return `${Math.floor(s / 60)}m ${String(s % 60).padStart(2, "0")}s`;
}

// How long the current run has been going, ticking once a second. Its own
// component so the tick re-renders this span and nothing else -- App
// re-rendering every second would redraw the form, the editors and the
// results under a run that is meant to be the only thing working.
export default function RunTimer({ since }: { since: number }) {
    const [now, setNow] = useState(() => Date.now());
    useEffect(() => {
        const id = setInterval(() => setNow(Date.now()), 1000);
        return () => clearInterval(id);
    }, []);
    return <span className="run-timer">{formatElapsed(now - since)}</span>;
}
