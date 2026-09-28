// Coalesce a drag's mousemoves into at most one call per animation frame. A
// gaming mouse reports up to 1000 moves a second and each one re-rendered the
// whole app; the screen shows ~60 of them, so the rest were pure lag. The
// last position always wins, and `flush` applies a pending one at mouseup so
// the handle ends exactly where it was released.
export function perFrame<T>(fn: (v: T) => void) {
    let pending: T | undefined;
    let id = 0;
    const run = () => {
        id = 0;
        if (pending !== undefined) fn(pending);
        pending = undefined;
    };
    return {
        push(v: T) {
            pending = v;
            if (!id) id = requestAnimationFrame(run);
        },
        flush() {
            if (id) cancelAnimationFrame(id);
            run();
        },
    };
}
