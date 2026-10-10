import { memo } from "react";
import type { ReactNode } from "react";

type Props = {
    active: boolean;
    className: string;
    children: ReactNode;
};

// A top-level tab that stays mounted while another one is showing.
//
// Switching tabs used to unmount the one being left, so every return to Config
// rebuilt a Monaco editor from nothing and every return to Data threw away the
// last query's results. Hidden instead, a tab comes back instantly and exactly
// as it was left.
//
// Hiding is display:none, not the `hidden` attribute: the panes' own classes
// set display:flex, which beats the attribute's user-agent rule.
function Pane({ active, className, children }: Props) {
    return (
        <div className={className} style={active ? undefined : { display: "none" }}>
            {children}
        </div>
    );
}

// While hidden and staying hidden, skip the render outright. App re-renders
// on every keystroke in any editor, and a pane nobody can see has nothing to
// show for it; the render it skipped happens the moment it becomes active, so
// what appears is never stale.
export function skipRender(prev: Props, next: Props): boolean {
    return !prev.active && !next.active;
}

export default memo(Pane, skipRender);
