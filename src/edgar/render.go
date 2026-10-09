package edgar

// Rendering a filing as SEC shows it. HTMLToText flattens a document, which
// is right for a press release and wrong for a 10-Q: its table of contents,
// its statements and its notes are tables, and their columns are the content.
// RenderHTML keeps the filing's own layout, tables and inline styles, and
// removes everything that could act: scripts, event handlers, forms, frames,
// external stylesheets, CSS that loads anything. What survives is the markup
// sec.gov itself would display, safe to put in the app's page.

import (
	"bytes"
	"html"
	"strings"
	"time"

	xhtml "golang.org/x/net/html"
)

// Rendered is one document ready to show: HTML when the document is a web
// page, Text when it is plain text or XML (old .txt filings, raw Form 4 XML).
type Rendered struct {
	HTML      string
	Text      string
	Contents  []Section
	Truncated bool
}

// Section is one entry of a document's own table of contents: its label and
// the (prefixed) id the label links to.
type Section struct {
	Label  string
	Target string
}

// AnchorPrefix namespaces every id and name in a rendered document, so a
// filing's anchors can never collide with the app's own ids.
const AnchorPrefix = "sec-"

// MaxHTMLBytes caps the rendered markup; the rest is on sec.gov. Large 10-Ks
// run to a few MB once their XBRL is gone, so this cuts only the outliers.
const MaxHTMLBytes = 8 << 20

// Render fetches a filing document and prepares it to show. Documents are
// immutable, so they are cached for a day.
func (c *Client) Render(rawURL string) (Rendered, error) {
	u, err := ArchiveURL(rawURL)
	if err != nil {
		return Rendered{}, err
	}
	raw, err := c.get(u, 24*time.Hour, MaxDocBytes)
	if err != nil {
		return Rendered{}, err
	}
	return RenderDocument(string(raw), u), nil
}

// RenderDocument is Render's pure half: HTML for a web page, text otherwise.
func RenderDocument(doc, docURL string) Rendered {
	if !IsHTML(doc) {
		text := HTMLToText(doc)
		if len(text) > MaxTextChars {
			cut := strings.LastIndex(text[:MaxTextChars], "\n")
			if cut < MaxTextChars/2 {
				cut = MaxTextChars
			}
			return Rendered{Text: text[:cut], Truncated: true}
		}
		return Rendered{Text: text}
	}
	out, contents, truncated := RenderHTML(doc, docURL)
	return Rendered{HTML: out, Contents: contents, Truncated: truncated}
}

// IsHTML says whether a document is a web page rather than plain text or XML
// that happens to contain tags.
func IsHTML(doc string) bool {
	head := strings.ToLower(doc)
	if len(head) > 64<<10 {
		head = head[:64<<10]
	}
	return strings.Contains(head, "<html") || strings.Contains(head, "<body") ||
		strings.Contains(head, "<table")
}

// Elements kept as they are. Anything else that is not dropped is unwrapped:
// its tag goes and its content stays, which is what inline XBRL needs —
// <ix:nonFraction>383,285</ix:nonFraction> is a number in a table cell.
var keepTags = map[string]bool{
	"a": true, "b": true, "big": true, "blockquote": true, "br": true, "caption": true,
	"center": true, "col": true, "colgroup": true, "dd": true, "div": true, "dl": true,
	"dt": true, "em": true, "font": true, "h1": true, "h2": true, "h3": true, "h4": true,
	"h5": true, "h6": true, "hr": true, "i": true, "img": true, "li": true, "ol": true,
	"p": true, "pre": true, "s": true, "small": true, "span": true, "strike": true,
	"strong": true, "sub": true, "sup": true, "table": true, "tbody": true, "td": true,
	"tfoot": true, "th": true, "thead": true, "tr": true, "tt": true, "u": true, "ul": true,
}

// Elements removed with everything inside them.
var dropTags = map[string]bool{
	"script": true, "style": true, "head": true, "title": true, "meta": true, "link": true,
	"iframe": true, "frame": true, "frameset": true, "object": true, "embed": true,
	"applet": true, "form": true, "input": true, "button": true, "select": true,
	"textarea": true, "noscript": true, "template": true, "svg": true, "math": true,
	"base": true, "audio": true, "video": true, "source": true, "canvas": true,
	"ix:header": true,
}

var keepAttrs = map[string]bool{
	"align": true, "alt": true, "bgcolor": true, "border": true, "cellpadding": true,
	"cellspacing": true, "color": true, "colspan": true, "face": true, "height": true,
	"rowspan": true, "size": true, "span": true, "style": true, "title": true,
	"valign": true, "width": true, "nowrap": true, "start": true, "type": true,
}

// RenderHTML sanitizes a filing's HTML for display and lists its table of
// contents. docURL resolves relative image sources to sec.gov. Internal links
// become "#sec-<id>"; external links keep their absolute URL and are opened in
// the browser by the UI, never inside the app.
func RenderHTML(doc, docURL string) (string, []Section, bool) {
	return renderLimited(doc, docURL, MaxHTMLBytes)
}

func renderLimited(doc, docURL string, limit int) (string, []Section, bool) {
	root, err := xhtml.Parse(strings.NewReader(doc))
	if err != nil {
		return "<pre>" + html.EscapeString(HTMLToText(doc)) + "</pre>", nil, false
	}
	body := find(root, "body")
	if body == nil {
		body = root
	}
	r := &renderer{folder: folderOf(docURL), limit: limit, seen: map[string]bool{}}
	for n := body.FirstChild; n != nil; n = n.NextSibling {
		r.node(n)
	}
	return r.buf.String(), r.contents, r.truncated
}

type renderer struct {
	buf       bytes.Buffer
	folder    string
	limit     int
	truncated bool
	contents  []Section
	seen      map[string]bool
}

func (r *renderer) node(n *xhtml.Node) {
	if r.truncated {
		return
	}
	switch n.Type {
	case xhtml.TextNode:
		r.buf.WriteString(html.EscapeString(n.Data))
		if r.buf.Len() > r.limit {
			r.truncated = true
		}
		return
	case xhtml.ElementNode:
	default:
		return
	}
	// SVG and MathML are foreign content: a filing never needs them, and
	// their own <script> would otherwise slip past a check on the name.
	if n.Namespace != "" {
		return
	}
	tag := strings.ToLower(n.Data)
	if dropTags[tag] || strings.HasPrefix(tag, "xbrli:") || strings.HasPrefix(tag, "xbrldi:") ||
		strings.HasPrefix(tag, "link:") {
		return
	}
	if !keepTags[tag] {
		r.children(n)
		return
	}
	r.buf.WriteByte('<')
	r.buf.WriteString(tag)
	for _, a := range n.Attr {
		r.attr(tag, a)
	}
	if tag == "a" {
		r.link(n)
	}
	r.buf.WriteByte('>')
	if tag == "br" || tag == "hr" || tag == "img" || tag == "col" {
		return
	}
	r.children(n)
	// A truncated document still closes every element it opened.
	r.buf.WriteString("</" + tag + ">")
}

func (r *renderer) children(n *xhtml.Node) {
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		r.node(c)
	}
}

func (r *renderer) attr(tag string, a xhtml.Attribute) {
	key := strings.ToLower(a.Key)
	if a.Namespace != "" {
		return
	}
	val := a.Val
	switch {
	case key == "id" || (key == "name" && tag == "a"):
		val = AnchorPrefix + val
	case key == "src" && tag == "img":
		abs := r.absolute(val)
		if abs == "" {
			return
		}
		val = abs
	case key == "style":
		val = SafeStyle(val)
		if val == "" {
			return
		}
	case key == "href":
		return // written by link
	case !keepAttrs[key]:
		return
	}
	r.buf.WriteString(" " + key + `="` + html.EscapeString(val) + `"`)
}

// link writes an anchor's href: "#sec-<id>" within the document, an absolute
// http(s) URL outside it, nothing for anything else (javascript:, data:, …).
// An internal link whose text is a section title joins the contents.
func (r *renderer) link(n *xhtml.Node) {
	href := strings.TrimSpace(attr(n, "href"))
	switch {
	case href == "":
		return
	case strings.HasPrefix(href, "#"):
		target := AnchorPrefix + href[1:]
		r.buf.WriteString(` href="#` + html.EscapeString(target) + `"`)
		if label := sectionLabel(n); label != "" && !r.seen[target] {
			r.seen[target] = true
			r.contents = append(r.contents, Section{Label: label, Target: target})
		}
	default:
		if abs := r.absolute(href); abs != "" {
			r.buf.WriteString(` href="` + html.EscapeString(abs) + `" data-external="1"`)
		}
	}
}

// absolute resolves a link or image source against the document's folder on
// sec.gov; only http(s) results are kept.
func (r *renderer) absolute(ref string) string {
	ref = strings.TrimSpace(ref)
	low := strings.ToLower(ref)
	switch {
	case strings.HasPrefix(low, "https://") || strings.HasPrefix(low, "http://"):
		return ref
	case strings.HasPrefix(ref, "//"):
		return "https:" + ref
	case strings.HasPrefix(ref, "/"):
		return WWWHost + ref
	case strings.Contains(ref, ":"):
		return "" // javascript:, data:, mailto: …
	case r.folder == "":
		return ""
	}
	return r.folder + ref
}

func folderOf(docURL string) string {
	i := strings.LastIndex(docURL, "/")
	if i < 0 {
		return ""
	}
	return docURL[:i+1]
}

// sectionLabel names a table-of-contents entry from an internal link. A 10-Q's
// contents are a table — "Item 1. | Financial Statements | 1" with every cell
// linked — so the label is the row's text without its page number; a link
// whose row is only a page number is not an entry.
func sectionLabel(a *xhtml.Node) string {
	scope := a
	for p := a.Parent; p != nil; p = p.Parent {
		if p.Type == xhtml.ElementNode && strings.EqualFold(p.Data, "tr") {
			scope = p
			break
		}
	}
	var cells []string
	if scope != a {
		for c := scope.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == xhtml.ElementNode {
				if t := collapse(textOf(c)); t != "" {
					cells = append(cells, t)
				}
			}
		}
	} else {
		cells = []string{collapse(textOf(a))}
	}
	// Drop a trailing page number ("1", "iii", "F-3").
	if len(cells) > 1 && isPageNumber(cells[len(cells)-1]) {
		cells = cells[:len(cells)-1]
	}
	label := strings.Join(cells, " ")
	if label == "" || isPageNumber(label) || len(label) > 160 {
		return ""
	}
	return label
}

func isPageNumber(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" || len(s) > 6 {
		return false
	}
	return strings.Trim(s, "0123456789-ivxlcf") == ""
}

func textOf(n *xhtml.Node) string {
	var b strings.Builder
	var walk func(*xhtml.Node)
	walk = func(n *xhtml.Node) {
		if n.Type == xhtml.TextNode {
			b.WriteString(n.Data)
			b.WriteByte(' ')
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return b.String()
}

func collapse(s string) string {
	return strings.TrimSpace(spaceRe.ReplaceAllString(strings.Join(strings.Fields(s), " "), " "))
}

func attr(n *xhtml.Node, key string) string {
	for _, a := range n.Attr {
		if strings.EqualFold(a.Key, key) {
			return a.Val
		}
	}
	return ""
}

func find(n *xhtml.Node, tag string) *xhtml.Node {
	if n.Type == xhtml.ElementNode && n.Data == tag {
		return n
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if f := find(c, tag); f != nil {
			return f
		}
	}
	return nil
}

// SafeStyle keeps an inline style's declarations except the ones that could
// load something or escape the reader: url(), expression(), imports,
// behaviours and fixed or absolute positioning.
func SafeStyle(style string) string {
	var keep []string
	for _, decl := range strings.Split(style, ";") {
		decl = strings.TrimSpace(decl)
		low := strings.ToLower(strings.Join(strings.Fields(decl), ""))
		prop, _, ok := strings.Cut(low, ":")
		if !ok || prop == "" {
			continue
		}
		if strings.Contains(low, "url(") || strings.Contains(low, "expression(") ||
			strings.Contains(low, "javascript:") || strings.Contains(low, "@import") ||
			strings.Contains(low, "\\") || prop == "behavior" || prop == "-moz-binding" ||
			prop == "position" || prop == "z-index" {
			continue
		}
		keep = append(keep, decl)
	}
	return strings.Join(keep, ";")
}
