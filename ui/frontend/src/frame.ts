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

// Follow one pointer drag from its pointerdown to its release, reporting the
// distance travelled. The pointer is CAPTURED, so the drag cannot come
// unstuck: the old mouse listeners lost their mouseup whenever the button was
// released outside the window (or over an editor that swallowed it), leaving
// the panel resizing on every later hover. Capture delivers the release to
// this element wherever it happens, and a cancelled or lost capture ends the
// drag the same way.
export function trackDrag(
    e: { currentTarget: EventTarget; pointerId: number; clientX: number; clientY: number },
    cursor: string,
    move: (dx: number, dy: number) => void,
    end?: () => void,
) {
    const el = e.currentTarget as HTMLElement;
    const x0 = e.clientX;
    const y0 = e.clientY;
    try {
        el.setPointerCapture(e.pointerId);
    } catch {
        // A pointer that is already gone cannot be captured; the listeners
        // below still see its release if it arrives.
    }
    const frame = perFrame((p: { x: number; y: number }) => move(p.x - x0, p.y - y0));
    const onMove = (ev: PointerEvent) => frame.push({ x: ev.clientX, y: ev.clientY });
    const done = () => {
        frame.flush();
        el.removeEventListener("pointermove", onMove);
        el.removeEventListener("pointerup", done);
        el.removeEventListener("pointercancel", done);
        el.removeEventListener("lostpointercapture", done);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
        end?.();
    };
    el.addEventListener("pointermove", onMove);
    el.addEventListener("pointerup", done);
    el.addEventListener("pointercancel", done);
    el.addEventListener("lostpointercapture", done);
    document.body.style.cursor = cursor;
    document.body.style.userSelect = "none";
}
