package main

import (
	"archive/zip"
	"bytes"
	"math"
	"strings"
	"testing"
)

// The table a user pasted when asking for this feature, verbatim: spaces, a
// "Value(USD)" header, a Cash line, a share class and three crypto symbols.
const pastedHoldings = `Asset    Value(USD)
VYM    8889
IAUM    1389
VONG    3756
AMZN    1563
ARM    967
BRK-B    3530
FSLR    1447
IBM    1409
NFLX    822
NVDA    2127
NXT    2127
OKLO    266
TSM    2529
VOO    11397
Cash    60569
VT    6566
BTC    1167
SOL    581
ETH    773
RKLB    493
IONQ    637
GEV    700
`

var holdingsUniverse = []string{
	"VYM", "IAUM", "VONG", "AMZN", "ARM", "BRK.B", "FSLR", "IBM", "NFLX",
	"NVDA", "NXT", "OKLO", "TSM", "VOO", "VT", "RKLB", "IONQ", "GEV",
	"CASH", "$CASH", "BTC-USD",
}

func sumPct(m map[string]float64) float64 {
	s := 0.0
	for _, v := range m {
		s += v
	}
	return s
}

func TestImportPastedHoldings(t *testing.T) {
	rows, err := parseHoldingsFile("p.txt", []byte(pastedHoldings))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 22 {
		t.Fatalf("read %d rows, want 22", len(rows))
	}
	got, err := resolveHoldings(rows, holdingsUniverse)
	if err != nil {
		t.Fatal(err)
	}

	// Cash is the T-bill series, never Pathward Financial.
	if _, ok := got.Allocations["CASH"]; ok {
		t.Error("a Cash line became the stock CASH (Pathward Financial)")
	}
	// The file totals $113,704; SOL (581) and ETH (773) do not match.
	matched := 113704.0 - 581 - 773
	if got.Total != matched {
		t.Errorf("Total = %v, want %v", got.Total, matched)
	}
	if want := math.Round(10000*60569/matched) / 100; math.Abs(got.Allocations["$CASH"]-want) > 0.011 {
		t.Errorf("$CASH = %v%%, want %v%%", got.Allocations["$CASH"], want)
	}
	if got.Allocations["BRK.B"] == 0 || got.Allocations["BTC-USD"] == 0 {
		t.Errorf("share class / crypto spelling not resolved: %v", got.Allocations)
	}
	if math.Abs(sumPct(got.Allocations)-100) > 1e-9 {
		t.Errorf("allocations sum to %v, want exactly 100", sumPct(got.Allocations))
	}
	if len(got.Tickers) != 20 || got.Tickers[0] != "VYM" || got.Tickers[len(got.Tickers)-1] != "GEV" {
		t.Errorf("tickers not in file order: %v", got.Tickers)
	}
	var miss []string
	for _, u := range got.Unmatched {
		miss = append(miss, u.Symbol)
	}
	if strings.Join(miss, ",") != "SOL,ETH" {
		t.Errorf("unmatched = %v, want SOL,ETH", miss)
	}
	if want := math.Round(10000*(581.0+773)/113704) / 100; got.UnmatchedPct != want {
		t.Errorf("UnmatchedPct = %v, want %v", got.UnmatchedPct, want)
	}
	if len(got.Mapped) != 3 {
		t.Errorf("mapped = %+v, want Cash, BRK-B, BTC", got.Mapped)
	}
}

func TestImportBrokerageCSV(t *testing.T) {
	// Preamble above the header, a symbol AND a description column, dollar
	// formatting, a duplicate symbol across accounts, and a total line.
	csv := "Account Summary,Brokerage\r\n\r\n" +
		"Account,Symbol,Description,Quantity,Current Value,Percent Of Account\r\n" +
		"X1,VOO,Vanguard S&P 500,10,\"$5,000.00\",50%\r\n" +
		"X2,VOO,Vanguard S&P 500,5,\"$2,500.00\",25%\r\n" +
		"X1,AMZN,Amazon,1,\"$2,500.00\",25%\r\n" +
		"Total,,,,\"$10,000.00\",100%\r\n"
	rows, err := parseHoldingsFile("b.csv", []byte(csv))
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolveHoldings(rows, holdingsUniverse)
	if err != nil {
		t.Fatal(err)
	}
	if got.Allocations["VOO"] != 75 || got.Allocations["AMZN"] != 25 || got.Total != 10000 {
		t.Errorf("got %+v", got)
	}
	if got.Percent {
		t.Error("an amount column was read as weights")
	}
}

func TestImportWeights(t *testing.T) {
	rows, err := parseHoldingsFile("w.csv", []byte("Ticker;Weight\nVOO;60%\nVT;40%\n"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolveHoldings(rows, holdingsUniverse)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Percent || got.Total != 0 || got.Allocations["VOO"] != 60 {
		t.Errorf("weights mis-read: %+v", got)
	}
}

func TestPercentagesSumExactly(t *testing.T) {
	tickers := []string{"A", "B", "C"}
	got := percentages(tickers, map[string]float64{"A": 1, "B": 1, "C": 1}, 3)
	if got["A"]+got["B"]+got["C"] != 100 || got["A"] != 33.34 {
		t.Errorf("thirds = %v", got)
	}
}

func TestImportXLSX(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	write := func(name, body string) {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		w.Write([]byte(body))
	}
	write("xl/workbook.xml", `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Holdings" sheetId="1" r:id="rId7"/></sheets></workbook>`)
	write("xl/_rels/workbook.xml.rels", `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId7" Type="worksheet" Target="worksheets/holdings.xml"/></Relationships>`)
	write("xl/sharedStrings.xml", `<sst><si><t>Asset</t></si><si><t>Value(USD)</t></si><si><r><t>V</t></r><r><t>OO</t></r></si></sst>`)
	write("xl/worksheets/holdings.xml", `<worksheet><sheetData>`+
		`<row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>`+
		`<row r="2"><c r="A2" t="s"><v>2</v></c><c r="B2"><v>3000</v></c></row>`+
		`<row r="3"><c r="A3" t="inlineStr"><is><t>Cash</t></is></c><c r="B3"><v>1000</v></c></row>`+
		`</sheetData></worksheet>`)
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	rows, err := parseHoldingsFile("h.xlsx", buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolveHoldings(rows, holdingsUniverse)
	if err != nil {
		t.Fatal(err)
	}
	if got.Allocations["VOO"] != 75 || got.Allocations["$CASH"] != 25 || got.Total != 4000 {
		t.Errorf("got %+v", got)
	}
}

func TestImportRefusesLegacyXLS(t *testing.T) {
	_, err := parseHoldingsFile("old.xls", []byte("\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1rest"))
	if err == nil || !strings.Contains(err.Error(), ".xlsx") {
		t.Errorf("err = %v, want a save-as-xlsx message", err)
	}
}
