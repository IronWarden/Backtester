// One table for every Research view (T144): sortable headers, text left and
// numbers right, a header that stays put while the rows scroll, and rows
// that can open something. The sorting is pure (sortRows) so the smoke
// render can pin it without a click.

import { useEffect, useRef, useState, type ReactNode } from "react";

export type Column<T> = {
  key: string;
  label: string;
  // Numbers sit right so their digits line up.
  numeric?: boolean;
  render: (row: T) => ReactNode;
  // What the column sorts by; a column without one does not sort.
  sortValue?: (row: T) => string | number | null;
  title?: (row: T) => string;
  // The direction a first click sorts: dates and moves read best
  // descending.
  firstDir?: SortDir;
};

export type SortDir = "asc" | "desc";
export type Sort = { key: string; dir: SortDir } | null;

// sortRows returns a sorted copy; rows with no value sort last either way,
// and ties keep their order.
export function sortRows<T>(rows: T[], columns: Column<T>[], sort: Sort): T[] {
  if (!sort) return rows;
  const col = columns.find((c) => c.key === sort.key);
  if (!col?.sortValue) return rows;
  const value = col.sortValue;
  const sign = sort.dir === "asc" ? 1 : -1;
  return rows
    .map((row, i) => ({ row, i, v: value(row) }))
    .sort((a, b) => {
      const an = a.v === null || a.v === "";
      const bn = b.v === null || b.v === "";
      if (an || bn) return an === bn ? a.i - b.i : an ? 1 : -1;
      if (a.v! < b.v!) return -sign;
      if (a.v! > b.v!) return sign;
      return a.i - b.i;
    })
    .map((x) => x.row);
}

// nextSort is what a click on a header does: a new column starts in its
// first direction, the same column flips.
export function nextSort<T>(current: Sort, col: Column<T>): Sort {
  if (current?.key === col.key) return { key: col.key, dir: current.dir === "asc" ? "desc" : "asc" };
  return { key: col.key, dir: col.firstDir ?? "asc" };
}

// A long list (a big filer's thousand CN filings) draws this many rows, then
// the next batch as the bottom scrolls into view, so it appears at once.
export const TABLE_PAGE = 100;

// shownCount is how many sorted rows to draw: the page limit, stretched to
// keep the active row on screen.
export function shownCount(total: number, limit: number, activeIndex: number): number {
  return Math.min(total, Math.max(limit, activeIndex + 1));
}

export function ResearchTable<T>({
  rows,
  columns,
  rowKey,
  initialSort = null,
  onRowClick,
  activeKey,
  rowClass,
  label,
  empty,
}: {
  rows: T[];
  columns: Column<T>[];
  rowKey: (row: T) => string;
  initialSort?: Sort;
  onRowClick?: (row: T) => void;
  activeKey?: string;
  rowClass?: (row: T) => string;
  // Names the table for a screen reader.
  label: string;
  // Said instead of an empty table.
  empty?: ReactNode;
}) {
  const [sort, setSort] = useState<Sort>(initialSort);
  const [limit, setLimit] = useState(TABLE_PAGE);
  const more = useRef<HTMLDivElement>(null);
  const sorted = sortRows(rows, columns, sort);
  const count = shownCount(
    sorted.length,
    limit,
    activeKey ? sorted.findIndex((r) => rowKey(r) === activeKey) : -1,
  );
  const left = sorted.length - count;
  useEffect(() => {
    const el = more.current;
    if (!el || left <= 0 || typeof IntersectionObserver === "undefined") return;
    const io = new IntersectionObserver(
      (entries) => {
        if (entries.some((e) => e.isIntersecting)) setLimit((n) => n + TABLE_PAGE);
      },
      { rootMargin: "400px" },
    );
    io.observe(el);
    return () => io.disconnect();
  }, [left]);
  if (rows.length === 0) {
    return empty ? <p className="research-dim">{empty}</p> : null;
  }
  const shown = sorted.slice(0, count);
  return (
    <div className="research-table-wrap rt-wrap">
      <table className="research-table rt" aria-label={label}>
        <thead>
          <tr>
            {columns.map((c) => {
              const active = sort?.key === c.key;
              const cls = c.numeric ? "num" : "";
              if (!c.sortValue) {
                return (
                  <th key={c.key} className={cls} scope="col">
                    {c.label}
                  </th>
                );
              }
              return (
                <th
                  key={c.key}
                  className={cls}
                  scope="col"
                  aria-sort={active ? (sort!.dir === "asc" ? "ascending" : "descending") : "none"}
                >
                  <button type="button" className="rt-sort" onClick={() => setSort(nextSort(sort, c))}>
                    {c.label}
                    <span className="rt-arrow">{active ? (sort!.dir === "asc" ? "▲" : "▼") : "↕"}</span>
                  </button>
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {shown.map((row) => {
            const key = rowKey(row);
            const classes = [
              onRowClick ? "rt-click" : "",
              activeKey !== undefined && activeKey === key ? "active" : "",
              rowClass ? rowClass(row) : "",
            ]
              .filter(Boolean)
              .join(" ");
            return (
              <tr
                key={key}
                className={classes || undefined}
                onClick={onRowClick ? () => onRowClick(row) : undefined}
                tabIndex={onRowClick ? 0 : undefined}
                onKeyDown={
                  onRowClick
                    ? (e) => {
                        if (e.key === "Enter") onRowClick(row);
                      }
                    : undefined
                }
              >
                {columns.map((c) => (
                  <td key={c.key} className={c.numeric ? "num" : ""} title={c.title ? c.title(row) : undefined}>
                    {c.render(row)}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
      {left > 0 && (
        <div ref={more} className="rt-more">
          <button type="button" className="link-button" onClick={() => setLimit(count + TABLE_PAGE)}>
            Show {Math.min(left, TABLE_PAGE)} more of {left}
          </button>
        </div>
      )}
    </div>
  );
}
