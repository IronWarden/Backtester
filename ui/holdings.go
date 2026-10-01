package main

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"my-backtester/src/data"

	wruntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

// Importing a portfolio from a file.
//
// A user's portfolio already exists somewhere -- a brokerage export, a
// spreadsheet they keep by hand -- and retyping twenty symbols and working out
// each one's percentage is the step this removes. The file is a list of
// (asset, amount) rows; the amounts become the allocation percentages of a
// buy-and-hold card, and when they are dollar amounts their sum becomes the
// starting capital, so the backtest starts with the book the user actually
// holds.
//
// Parsed here rather than in the frontend because .xlsx is a zip of XML: Go's
// standard library reads it, and the frontend would need a new dependency.
// Legacy binary .xls is not read; it is refused with a message saying to save
// as .xlsx or .csv.

// HoldingMapping records a symbol the importer changed on the way in, so the
// card can say so instead of the user discovering it in the results.
type HoldingMapping struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Reason string `json:"reason"`
}

// UnmatchedHolding is a row whose symbol the database cannot price. Its Pct is
// of the WHOLE file, so the user sees how much of their book was left out.
type UnmatchedHolding struct {
	Symbol string  `json:"symbol"`
	Pct    float64 `json:"pct"`
}

// HoldingsImport is what one file becomes.
type HoldingsImport struct {
	// File is the base name of what was read; empty means the picker was
	// cancelled and nothing should change.
	File string `json:"file"`
	// Tickers are in file order, after symbol resolution and with duplicate
	// rows (one symbol in two accounts) merged.
	Tickers []string `json:"tickers"`
	// Allocations are percentages of the MATCHED holdings, to two decimals,
	// summing to exactly 100.
	Allocations map[string]float64 `json:"allocations"`
	// Total is the dollar value of the matched holdings, or 0 when the file
	// gave percentages and so says nothing about capital.
	Total float64 `json:"total"`
	// Percent reports that the value column was a weight, not an amount.
	Percent   bool               `json:"percent"`
	Mapped    []HoldingMapping   `json:"mapped"`
	Unmatched []UnmatchedHolding `json:"unmatched"`
	// UnmatchedPct is the share of the file the unmatched rows carried.
	UnmatchedPct float64 `json:"unmatchedPct"`
	// Skipped names rows that were read and deliberately ignored (a total
	// line, a zero or negative amount), with why.
	Skipped []string `json:"skipped"`
}

// ImportHoldings asks for a CSV or spreadsheet and turns it into allocations,
// resolving every symbol against the database at dbPath. A cancelled picker
// returns a zero HoldingsImport and no error.
func (a *App) ImportHoldings(dbPath string) (HoldingsImport, error) {
	path, err := wruntime.OpenFileDialog(a.ctx, wruntime.OpenDialogOptions{
		Title: "Import a portfolio",
		Filters: []wruntime.FileFilter{
			{DisplayName: "Holdings (CSV, Excel, text)", Pattern: "*.csv;*.tsv;*.txt;*.xlsx"},
			{DisplayName: "Any file", Pattern: "*"},
		},
	})
	if err != nil || path == "" {
		return HoldingsImport{}, err
	}
	if dbPath == "" {
		return HoldingsImport{}, fmt.Errorf("choose a database first")
	}
	if _, err := data.InitDB(dbPath); err != nil {
		return HoldingsImport{}, fmt.Errorf("open db %q: %w", dbPath, err)
	}
	universe, err := data.ListTickers()
	if err != nil {
		return HoldingsImport{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return HoldingsImport{}, err
	}
	rows, err := parseHoldingsFile(filepath.Base(path), raw)
	if err != nil {
		return HoldingsImport{}, err
	}
	out, err := resolveHoldings(rows, universe)
	if err != nil {
		return HoldingsImport{}, err
	}
	out.File = filepath.Base(path)
	return out, nil
}

// holdingRow is one parsed line: the asset as written and its amount.
type holdingRow struct {
	Symbol  string
	Value   float64
	Percent bool
}

// parseHoldingsFile reads rows out of a spreadsheet or delimited text file.
func parseHoldingsFile(name string, raw []byte) ([]holdingRow, error) {
	var table [][]string
	switch {
	case bytes.HasPrefix(raw, []byte("PK\x03\x04")):
		t, err := readXLSX(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		table = t
	case bytes.HasPrefix(raw, []byte("\xD0\xCF\x11\xE0")):
		return nil, fmt.Errorf("%s is a legacy .xls workbook; save it as .xlsx or .csv and import that", name)
	default:
		t, err := readDelimited(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		table = t
	}
	rows, err := holdingsFromTable(table)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return rows, nil
}

// readDelimited splits text on whichever of tab, comma or semicolon its
// opening lines use most, and on runs of whitespace when they use none -- a
// table pasted out of a chat or a web page arrives that way.
func readDelimited(raw []byte) ([][]string, error) {
	raw = bytes.TrimPrefix(raw, []byte("\xEF\xBB\xBF"))
	text := strings.ReplaceAll(string(raw), "\r\n", "\n")
	lines := strings.Split(text, "\n")

	sample := lines
	if len(sample) > 20 {
		sample = sample[:20]
	}
	best, bestN := rune(0), 0
	for _, d := range []rune{'\t', ',', ';'} {
		n := 0
		for _, l := range sample {
			n += strings.Count(l, string(d))
		}
		if n > bestN {
			best, bestN = d, n
		}
	}

	if best == 0 {
		var table [][]string
		for _, l := range lines {
			if f := strings.Fields(l); len(f) > 0 {
				table = append(table, f)
			}
		}
		return table, nil
	}

	r := csv.NewReader(strings.NewReader(text))
	r.Comma = best
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	var table [][]string
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		table = append(table, rec)
	}
	return table, nil
}

// readXLSX returns the first worksheet as a table of strings. Only what a
// holdings list needs is read: shared strings, inline strings and numbers.
func readXLSX(raw []byte) ([][]string, error) {
	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return nil, fmt.Errorf("not a readable .xlsx: %w", err)
	}
	files := map[string]*zip.File{}
	for _, f := range zr.File {
		files[f.Name] = f
	}
	readXML := func(name string, v any) error {
		f, ok := files[name]
		if !ok {
			return fmt.Errorf("missing %s", name)
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer rc.Close()
		return xml.NewDecoder(rc).Decode(v)
	}

	// Shared strings: each <si> is one string, possibly split into rich-text
	// runs, all of which concatenate.
	var shared []string
	if _, ok := files["xl/sharedStrings.xml"]; ok {
		var sst struct {
			SI []struct {
				T string `xml:"t"`
				R []struct {
					T string `xml:"t"`
				} `xml:"r"`
			} `xml:"si"`
		}
		if err := readXML("xl/sharedStrings.xml", &sst); err != nil {
			return nil, err
		}
		for _, si := range sst.SI {
			s := si.T
			for _, r := range si.R {
				s += r.T
			}
			shared = append(shared, s)
		}
	}

	sheet, err := firstSheetPath(readXML)
	if err != nil {
		return nil, err
	}
	var ws struct {
		Rows []struct {
			Cells []struct {
				Ref    string `xml:"r,attr"`
				Type   string `xml:"t,attr"`
				Value  string `xml:"v"`
				Inline struct {
					T string `xml:"t"`
				} `xml:"is"`
			} `xml:"c"`
		} `xml:"sheetData>row"`
	}
	if err := readXML(sheet, &ws); err != nil {
		return nil, err
	}

	var table [][]string
	for _, row := range ws.Rows {
		var rec []string
		for i, c := range row.Cells {
			col := i
			if c.Ref != "" {
				col = xlsxColumn(c.Ref)
			}
			var v string
			switch c.Type {
			case "s":
				idx, err := strconv.Atoi(strings.TrimSpace(c.Value))
				if err == nil && idx >= 0 && idx < len(shared) {
					v = shared[idx]
				}
			case "inlineStr":
				v = c.Inline.T
			default:
				v = c.Value
			}
			for len(rec) <= col {
				rec = append(rec, "")
			}
			rec[col] = v
		}
		table = append(table, rec)
	}
	return table, nil
}

// firstSheetPath finds the first worksheet the workbook lists, falling back
// to the conventional sheet1.xml when the relationships cannot be followed.
func firstSheetPath(readXML func(string, any) error) (string, error) {
	const fallback = "xl/worksheets/sheet1.xml"
	var wb struct {
		Sheets []struct {
			RID string `xml:"http://schemas.openxmlformats.org/officeDocument/2006/relationships id,attr"`
		} `xml:"sheets>sheet"`
	}
	var rels struct {
		Rel []struct {
			ID     string `xml:"Id,attr"`
			Target string `xml:"Target,attr"`
		} `xml:"Relationship"`
	}
	if readXML("xl/workbook.xml", &wb) != nil || len(wb.Sheets) == 0 ||
		readXML("xl/_rels/workbook.xml.rels", &rels) != nil {
		return fallback, nil
	}
	for _, r := range rels.Rel {
		if r.ID == wb.Sheets[0].RID {
			t := strings.TrimPrefix(r.Target, "/")
			if !strings.HasPrefix(t, "xl/") {
				t = "xl/" + t
			}
			return t, nil
		}
	}
	return fallback, nil
}

// xlsxColumn turns a cell reference ("C12") into a zero-based column index.
func xlsxColumn(ref string) int {
	n := 0
	for _, r := range ref {
		if r < 'A' || r > 'Z' {
			break
		}
		n = n*26 + int(r-'A'+1)
	}
	return n - 1
}

var (
	symbolHeader  = regexp.MustCompile(`(?i)^\s*(symbol|ticker)`)
	nameHeader    = regexp.MustCompile(`(?i)(asset|holding|security|investment|instrument|name|description|position)`)
	amountHeader  = regexp.MustCompile(`(?i)(market\s*value|current\s*value|value|amount|balance|usd|\$)`)
	percentHeader = regexp.MustCompile(`(?i)(weight|alloc|percent|pct|%)`)
	totalRow      = regexp.MustCompile(`(?i)^\s*(grand\s+|account\s+)?totals?\b`)
)

// parseAmount reads "$8,889", "(1,200.50)", "4.5%" or "1e3". The bool reports
// a trailing percent sign.
func parseAmount(s string) (float64, bool, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false, false
	}
	neg := strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")")
	s = strings.Trim(s, "()")
	pct := strings.HasSuffix(s, "%")
	s = strings.TrimSuffix(s, "%")
	s = strings.NewReplacer("$", "", ",", "", " ", "", "USD", "", "usd", "").Replace(s)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false, false
	}
	if neg {
		v = -v
	}
	return v, pct, true
}

// holdingsFromTable finds the header, the symbol column and the amount column,
// then reads every row under them.
func holdingsFromTable(table [][]string) ([]holdingRow, error) {
	for i := range table {
		for j := range table[i] {
			table[i][j] = strings.TrimSpace(table[i][j])
		}
	}

	// The header is the early row naming the most columns we recognise.
	// Brokerage exports put account details above it, so it is searched for
	// rather than assumed to be line one.
	header, bestScore := -1, 0
	for i := 0; i < len(table) && i < 20; i++ {
		score := 0
		for _, c := range table[i] {
			if _, _, ok := parseAmount(c); ok {
				continue
			}
			if symbolHeader.MatchString(c) || nameHeader.MatchString(c) ||
				amountHeader.MatchString(c) || percentHeader.MatchString(c) {
				score++
			}
		}
		if score > bestScore {
			header, bestScore = i, score
		}
	}

	symCol, valCol, percentCol := -1, -1, false
	if header >= 0 {
		h := table[header]
		for j, c := range h {
			if symbolHeader.MatchString(c) {
				symCol = j
				break
			}
		}
		if symCol < 0 {
			for j, c := range h {
				if nameHeader.MatchString(c) && !amountHeader.MatchString(c) {
					symCol = j
					break
				}
			}
		}
		for j, c := range h {
			if j != symCol && amountHeader.MatchString(c) && !percentHeader.MatchString(c) {
				valCol = j
				break
			}
		}
		if valCol < 0 {
			for j, c := range h {
				if j != symCol && percentHeader.MatchString(c) {
					valCol, percentCol = j, true
					break
				}
			}
		}
	}
	data := table[header+1:]
	if symCol < 0 {
		symCol = 0
	}
	if valCol < 0 {
		valCol = lastNumericColumn(data, symCol)
	}
	if valCol < 0 {
		return nil, fmt.Errorf("no column of amounts found; expected an asset column and a value or weight column")
	}

	var rows []holdingRow
	for _, rec := range data {
		if symCol >= len(rec) || valCol >= len(rec) || rec[symCol] == "" {
			continue
		}
		v, pct, ok := parseAmount(rec[valCol])
		if !ok {
			continue
		}
		rows = append(rows, holdingRow{Symbol: rec[symCol], Value: v, Percent: percentCol || pct})
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no holdings found")
	}
	return rows, nil
}

// lastNumericColumn is the rightmost column, other than skip, in which most
// rows hold a number -- the amount column of a header-less file.
func lastNumericColumn(table [][]string, skip int) int {
	width := 0
	for _, r := range table {
		width = max(width, len(r))
	}
	for j := width - 1; j >= 0; j-- {
		if j == skip {
			continue
		}
		n, filled := 0, 0
		for _, r := range table {
			if j < len(r) && r[j] != "" {
				filled++
				if _, _, ok := parseAmount(r[j]); ok {
					n++
				}
			}
		}
		if filled > 0 && n*2 > filled {
			return j
		}
	}
	return -1
}

// cashLabels are how a holdings list names uninvested money. They map to
// $CASH, the T-bill total-return series, NOT to the stock CASH -- that symbol
// is Pathward Financial, and a cash line silently becoming a bank stock is the
// failure this guards against.
var cashLabels = map[string]bool{
	"cash": true, "usd": true, "cash & equivalents": true,
	"cash and equivalents": true, "cash & cash investments": true,
	"money market": true, "core position": true, "pending activity": true,
	"cash (usd)": true, "us dollar": true, "sweep": true,
}

// resolveHoldings maps each row's symbol onto the database and turns amounts
// into percentages of what matched.
func resolveHoldings(rows []holdingRow, universe []string) (HoldingsImport, error) {
	known := make(map[string]bool, len(universe))
	for _, t := range universe {
		known[t] = true
	}
	out := HoldingsImport{Allocations: map[string]float64{}}

	values := map[string]float64{}
	unmatched := map[string]float64{}
	var unmatchedOrder []string
	mapped := map[string]bool{}
	grand := 0.0
	for _, r := range rows {
		if totalRow.MatchString(r.Symbol) {
			out.Skipped = append(out.Skipped, fmt.Sprintf("%s (a total line)", r.Symbol))
			continue
		}
		if r.Value <= 0 {
			out.Skipped = append(out.Skipped, fmt.Sprintf("%s (amount %v is not positive)", r.Symbol, r.Value))
			continue
		}
		if r.Percent {
			out.Percent = true
		}
		grand += r.Value

		sym, reason := resolveSymbol(r.Symbol, known)
		if sym == "" {
			if _, seen := unmatched[r.Symbol]; !seen {
				unmatchedOrder = append(unmatchedOrder, r.Symbol)
			}
			unmatched[r.Symbol] += r.Value
			continue
		}
		if sym != r.Symbol && !mapped[r.Symbol] {
			mapped[r.Symbol] = true
			out.Mapped = append(out.Mapped, HoldingMapping{From: r.Symbol, To: sym, Reason: reason})
		}
		if _, seen := values[sym]; !seen {
			out.Tickers = append(out.Tickers, sym)
		}
		values[sym] += r.Value
	}
	if grand <= 0 {
		return out, fmt.Errorf("no holdings with a positive amount")
	}
	for _, s := range unmatchedOrder {
		pct := round2(100 * unmatched[s] / grand)
		out.Unmatched = append(out.Unmatched, UnmatchedHolding{Symbol: s, Pct: pct})
		out.UnmatchedPct += unmatched[s]
	}
	out.UnmatchedPct = round2(100 * out.UnmatchedPct / grand)
	if len(out.Tickers) == 0 {
		return out, fmt.Errorf("none of the %d symbols is in the database", len(unmatchedOrder))
	}

	matched := 0.0
	for _, t := range out.Tickers {
		matched += values[t]
	}
	out.Allocations = percentages(out.Tickers, values, matched)
	if !out.Percent {
		out.Total = math.Round(matched*100) / 100
	}
	return out, nil
}

// resolveSymbol finds the database's name for what a holdings file wrote.
// Returns "" when nothing matches.
func resolveSymbol(s string, known map[string]bool) (string, string) {
	if cashLabels[strings.ToLower(s)] && known["$CASH"] {
		return "$CASH", "uninvested cash, modelled as T-bills (the stock CASH is Pathward Financial)"
	}
	if known[s] {
		return s, ""
	}
	up := strings.ToUpper(s)
	if known[up] {
		return up, "upper-cased"
	}
	// Share classes are written BRK.B, BRK/B, BRK B or BRK-B depending on
	// the broker; crypto is quoted against the dollar as BTC-USD.
	for _, v := range []string{
		strings.NewReplacer(".", "-", "/", "-", " ", "-").Replace(up),
		strings.NewReplacer("-", ".", "/", ".", " ", ".").Replace(up),
		up + "-USD",
	} {
		if v != up && known[v] {
			return v, "the database's spelling of " + s
		}
	}
	return "", ""
}

// percentages splits 100 across tickers in proportion to their values, to two
// decimals, by largest remainder so the parts sum to exactly 100.00.
func percentages(tickers []string, values map[string]float64, total float64) map[string]float64 {
	type part struct {
		t    string
		cent int
		rem  float64
	}
	parts := make([]part, len(tickers))
	left := 10000
	for i, t := range tickers {
		exact := 10000 * values[t] / total
		parts[i] = part{t, int(math.Floor(exact)), exact - math.Floor(exact)}
		left -= parts[i].cent
	}
	order := make([]int, len(parts))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return parts[order[a]].rem > parts[order[b]].rem })
	for i := 0; left > 0 && len(order) > 0; i, left = i+1, left-1 {
		parts[order[i%len(order)]].cent++
	}
	out := make(map[string]float64, len(parts))
	for _, p := range parts {
		out[p.t] = float64(p.cent) / 100
	}
	return out
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
