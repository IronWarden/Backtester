// Turning an imported holdings file into a portfolio card.
//
// The Go side (ui/holdings.go) does the reading and the symbol matching; this
// is the part that decides what the CARD becomes, kept pure so the smoke
// render can check it without a file picker.
import type { SimpleForm } from "./buildToml";

// The shape App.ImportHoldings returns, structurally -- the generated class is
// not imported so this file stays usable from node.
export type HoldingsImport = {
  file: string;
  tickers: string[] | null;
  allocations: Record<string, number> | null;
  total: number;
  percent: boolean;
  mapped: { from: string; to: string; reason: string }[] | null;
  unmatched: { symbol: string; pct: number }[] | null;
  unmatchedPct: number;
  skipped: string[] | null;
};

// The imported book REPLACES the card's holdings: importing a portfolio means
// "this is the portfolio", and merging into whatever was there would produce
// percentages that describe neither. Custom weights, because the file's weights
// are the point. Dollar amounts also set the starting capital; a file of
// percentages says nothing about capital, so that is left alone.
export function applyImport(form: SimpleForm, imp: HoldingsImport): SimpleForm {
  const tickers = imp.tickers ?? [];
  const allocations: Record<string, number> = {};
  for (const t of tickers) allocations[t] = imp.allocations?.[t] ?? 0;
  const next: SimpleForm = { ...form, tickers, allocations, evenSplit: false };
  if (!imp.percent && imp.total > 0) next.buyingPower = Math.round(imp.total);
  if (!form.name.trim()) next.name = imp.file.replace(/\.[^.]+$/, "");
  return next;
}

// What the card says after an import. Anything the importer changed or left
// out is stated, because a holding that silently vanished -- or a Cash line
// that silently became a bank stock -- is a wrong backtest that looks right.
export function importSummary(imp: HoldingsImport): string[] {
  const n = imp.tickers?.length ?? 0;
  const lines: string[] = [];
  const capital = imp.percent
    ? "weights from the file"
    : `$${Math.round(imp.total).toLocaleString("en-US")} starting capital`;
  lines.push(`Imported ${n} holding${n === 1 ? "" : "s"} from ${imp.file} — ${capital}.`);
  for (const m of imp.mapped ?? []) {
    lines.push(`${m.from} → ${m.to}: ${m.reason}.`);
  }
  const miss = imp.unmatched ?? [];
  if (miss.length > 0) {
    const list = miss.map((u) => `${u.symbol} (${u.pct}%)`).join(", ");
    lines.push(
      `Not in the database, left out: ${list}. That is ${imp.unmatchedPct}% of the file; the rest was scaled to 100%.`,
    );
  }
  if ((imp.skipped ?? []).length > 0) {
    lines.push(`Ignored: ${(imp.skipped ?? []).join(", ")}.`);
  }
  return lines;
}
