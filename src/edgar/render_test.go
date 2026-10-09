package edgar

import (
	"strings"
	"testing"
)

const tenQURL = "https://www.sec.gov/Archives/edgar/data/320193/000032019326000020/aapl-20260627.htm"

// doc-10q.htm is Apple's 10-Q for the quarter to June 27, 2026, cut to its
// cover, its table of contents and its first statement, with the hidden XBRL
// header shortened to one fact.
func TestRenderKeepsA10QAsSECShowsIt(t *testing.T) {
	r := RenderDocument(string(fixture(t, "doc-10q.htm")), tenQURL)
	if r.HTML == "" || r.Text != "" || r.Truncated {
		t.Fatalf("a 10-Q is a web page: html %d bytes, text %d, truncated %v", len(r.HTML), len(r.Text), r.Truncated)
	}
	for _, want := range []string{
		"<table", "<td", "font-family:", // its own layout
		"Net sales", // a statement line, unwrapped from inline XBRL
		`href="#sec-ib65f44d2415e4f40854b44fa987f6c2a_13"`, // the contents still link
		`id="sec-ib65f44d2415e4f40854b44fa987f6c2a_13"`,    // to their prefixed target
	} {
		if !strings.Contains(r.HTML, want) {
			t.Errorf("rendered 10-Q lacks %q", want)
		}
	}
	for _, never := range []string{"<ix:", "AmendmentFlag", "<html", "<head", "<title", "<script"} {
		if strings.Contains(r.HTML, never) {
			t.Errorf("rendered 10-Q carries %q", never)
		}
	}

	want := []string{
		"Part I",
		"Item 1. Financial Statements",
		"Item 2. Management’s Discussion and Analysis of Financial Condition and Results of Operations",
		"Item 3. Quantitative and Qualitative Disclosures About Market Risk",
		"Item 4. Controls and Procedures",
		"Part II",
		"Item 1. Legal Proceedings",
		"Item 1A. Risk Factors",
		"Item 2. Unregistered Sales of Equity Securities and Use of Proceeds",
		"Item 3. Defaults Upon Senior Securities",
		"Item 4. Mine Safety Disclosures",
		"Item 5. Other Information",
		"Item 6. Exhibits",
	}
	if len(r.Contents) != len(want) {
		t.Fatalf("contents: %+v", r.Contents)
	}
	for i, s := range r.Contents {
		if s.Label != want[i] || !strings.HasPrefix(s.Target, AnchorPrefix) {
			t.Errorf("contents[%d] = %+v, want %q", i, s, want[i])
		}
	}
}

func TestRenderRemovesEverythingThatCouldAct(t *testing.T) {
	doc := `<html><head><style>body{background:url(x)}</style><script>evil()</script></head><body>
<p onclick="evil()" style="color:red;background:url(http://x/y.png);position:fixed;font-weight:bold">Hi</p>
<a href="javascript:evil()">js</a><a href="data:text/html,x">data</a>
<a href="https://www.sec.gov/cgi-bin/x">out</a><a href="exhibit.htm">rel</a>
<a name="toc"></a><a href="#toc">1</a>
<iframe src="https://evil.example"></iframe><form action="x"><input name="p"></form>
<img src="javascript:evil()"><img src="g1.jpg" onerror="evil()">
<object data="x"></object><svg><script>evil()</script></svg>
</body></html>`
	got, contents, truncated := RenderHTML(doc, "https://www.sec.gov/Archives/edgar/data/1/2/doc.htm")
	for _, never := range []string{"evil", "<script", "<style", "<iframe", "<form", "<input",
		"<object", "<svg", "url(", "position", "onclick", "onerror", "javascript", "data:"} {
		if strings.Contains(got, never) {
			t.Errorf("sanitized output carries %q:\n%s", never, got)
		}
	}
	for _, want := range []string{
		`<p style="color:red;font-weight:bold">Hi</p>`,
		`href="https://www.sec.gov/cgi-bin/x" data-external="1"`,
		`href="https://www.sec.gov/Archives/edgar/data/1/2/exhibit.htm" data-external="1"`,
		`<a name="sec-toc"></a>`,
		`<a href="#sec-toc">1</a>`,
		`<img src="https://www.sec.gov/Archives/edgar/data/1/2/g1.jpg">`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("sanitized output lacks %q:\n%s", want, got)
		}
	}
	if truncated || len(contents) != 0 {
		t.Errorf("a bare page number is not a contents entry: %+v %v", contents, truncated)
	}
}

func TestRenderTruncatesWithEveryElementClosed(t *testing.T) {
	doc := "<html><body><div><table>" + strings.Repeat("<tr><td>row</td></tr>", 50) + "</table></div></body></html>"
	if _, _, truncated := renderLimited(doc, "", 1<<20); truncated {
		t.Fatal("a short document was cut")
	}
	out, _, truncated := renderLimited(doc, "", 200)
	if !truncated || len(out) > 400 {
		t.Fatalf("not cut: %v %d", truncated, len(out))
	}
	if !strings.HasSuffix(out, "</td></tr></tbody></table></div>") {
		t.Errorf("a cut document must still close what it opened: …%s", out[len(out)-60:])
	}
}

func TestRenderLeavesPlainTextAsText(t *testing.T) {
	r := RenderDocument("<SEC-DOCUMENT>\n<TYPE>10-Q\nQUARTERLY REPORT\n  Net sales   $ 100\n", "")
	if r.HTML != "" || !strings.Contains(r.Text, "QUARTERLY REPORT") {
		t.Errorf("a .txt filing reads as text: %+v", r)
	}
	if IsHTML(`<?xml version="1.0"?><ownershipDocument><issuer/></ownershipDocument>`) {
		t.Error("raw Form 4 XML is not a web page")
	}
}

func TestSafeStyle(t *testing.T) {
	for in, want := range map[string]string{
		"font-size:9pt; color:#000000":               "font-size:9pt;color:#000000",
		"background-image: URL( x )":                 "",
		"width:expression(alert(1))":                 "",
		"position:absolute;top:0;z-index:9;margin:0": "top:0;margin:0",
		`font-family:'Helvetica',sans-serif;;bogus`:  "font-family:'Helvetica',sans-serif",
		`content:"\75 rl(x)"`:                        "",
	} {
		if got := SafeStyle(in); got != want {
			t.Errorf("SafeStyle(%q) = %q, want %q", in, got, want)
		}
	}
}
