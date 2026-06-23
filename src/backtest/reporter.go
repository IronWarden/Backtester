package backtest

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Reporter writes backtest Results to a file according to OutputConfig.
// Use NewReporter to construct one; Write is safe to call from a single
// goroutine. Close flushes buffered writers (CSV) and closes the file.
type Reporter struct {
	format  string
	file    *os.File
	out     *bufio.Writer // used for txt/json; nil for csv (csv has its own buffering)
	csv     *csv.Writer
	filter  ast.Expr
	fields  []string
	wrote   int
	headerW bool
	sortBy  string
	sortAsc bool
	limit   int
	buf     []Result
}

// resultFields lists every value addressable from a filter expression or
// fields list. The same names are used in `filter` and `fields`.
var resultFields = []string{
	"PortfolioName",
	"Strategy",
	"SharpeRatio",
	"SortinoRatio",
	"MaxDrawdown",
	"AnnualReturn",
	"StandardDev",
	"AvgCorrelation",
	"CointegratedPairs",
}

func resultValue(r Result, name string) (any, bool) {
	switch name {
	case "PortfolioName":
		return r.PortfolioName, true
	case "Strategy":
		return r.Strategy, true
	case "SharpeRatio":
		return r.Metrics.SharpeRatio, true
	case "SortinoRatio":
		return r.Metrics.SortinoRatio, true
	case "MaxDrawdown":
		return r.Metrics.MaxDrawdown, true
	case "AnnualReturn":
		return r.Metrics.AnnualReturn, true
	case "StandardDev":
		return r.Metrics.StandardDev, true
	case "AvgCorrelation":
		return r.Metrics.AvgCorrelation, true
	case "CointegratedPairs":
		return float64(r.Metrics.CointegratedPairs), true
	}
	return nil, false
}

func NewReporter(cfg *OutputConfig) (*Reporter, error) {
	if cfg == nil || cfg.Path == "" {
		return nil, nil
	}

	format := strings.ToLower(cfg.Format)
	if format == "" {
		format = "txt"
	}
	switch format {
	case "txt", "csv", "json":
	default:
		return nil, fmt.Errorf("output format %q: must be txt, csv, or json", cfg.Format)
	}

	fields := cfg.Fields
	if len(fields) == 0 {
		fields = []string{
			"PortfolioName", "Strategy", "SharpeRatio",
			"SortinoRatio", "MaxDrawdown", "AnnualReturn",
		}
	}
	for _, f := range fields {
		if _, ok := resultValue(Result{}, f); !ok {
			return nil, fmt.Errorf("unknown output field %q", f)
		}
	}

	var filter ast.Expr
	if strings.TrimSpace(cfg.Filter) != "" {
		expr, err := parser.ParseExpr(cfg.Filter)
		if err != nil {
			return nil, fmt.Errorf("output filter %q: %w", cfg.Filter, err)
		}
		if err := validateFilter(expr); err != nil {
			return nil, fmt.Errorf("output filter %q: %w", cfg.Filter, err)
		}
		filter = expr
	}

	sortBy := cfg.SortBy
	if sortBy != "" {
		if _, ok := resultValue(Result{}, sortBy); !ok {
			return nil, fmt.Errorf("unknown sort_by field %q", sortBy)
		}
	}

	sortAsc := false
	switch strings.ToLower(cfg.Order) {
	case "", "desc":
		sortAsc = false
	case "asc":
		sortAsc = true
	default:
		return nil, fmt.Errorf("output order %q: must be asc or desc", cfg.Order)
	}

	if cfg.Limit < 0 {
		return nil, fmt.Errorf("output limit %d: must be >= 0", cfg.Limit)
	}

	file, err := os.OpenFile(
		cfg.Path,
		os.O_TRUNC|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("open output %q: %w", cfg.Path, err)
	}

	r := &Reporter{
		format:  format,
		file:    file,
		fields:  fields,
		filter:  filter,
		sortBy:  sortBy,
		sortAsc: sortAsc,
		limit:   cfg.Limit,
	}
	if format == "csv" {
		r.csv = csv.NewWriter(file)
	} else {
		r.out = bufio.NewWriter(file)
	}
	return r, nil
}

// Write emits one Result if it passes the filter (or if no filter is set).
// When sort_by is configured, results are buffered and emitted in Close.
func (r *Reporter) Write(res Result) error {
	if r == nil {
		return nil
	}
	if r.filter != nil {
		match, err := evalFilter(r.filter, res)
		if err != nil {
			return fmt.Errorf("filter eval: %w", err)
		}
		if !match {
			return nil
		}
	}

	if r.sortBy != "" {
		r.buf = append(r.buf, res)
		return nil
	}

	if r.limit > 0 && r.wrote >= r.limit {
		return nil
	}
	return r.emit(res)
}

func (r *Reporter) emit(res Result) error {
	r.wrote++
	switch r.format {
	case "txt":
		return r.writeTxt(res)
	case "csv":
		return r.writeCSV(res)
	case "json":
		return r.writeJSON(res)
	}
	return nil
}

// lessByField reports whether a sorts before b under the configured sort_by
// and order. Unknown types preserve buffered order (returns false).
func (r *Reporter) lessByField(a, b Result) bool {
	av, _ := resultValue(a, r.sortBy)
	bv, _ := resultValue(b, r.sortBy)
	switch x := av.(type) {
	case float64:
		y, ok := bv.(float64)
		if !ok {
			return false
		}
		if r.sortAsc {
			return x < y
		}
		return x > y
	case string:
		y, ok := bv.(string)
		if !ok {
			return false
		}
		if r.sortAsc {
			return x < y
		}
		return x > y
	}
	return false
}

func (r *Reporter) writeTxt(res Result) error {
	parts := make([]string, 0, len(r.fields))
	for _, f := range r.fields {
		v, _ := resultValue(res, f)
		parts = append(parts, fmt.Sprintf("%s: %s", f, formatValue(v)))
	}
	_, err := io.WriteString(r.out, strings.Join(parts, ", ")+"\n")
	return err
}

func (r *Reporter) writeCSV(res Result) error {
	if !r.headerW {
		if err := r.csv.Write(r.fields); err != nil {
			return err
		}
		r.headerW = true
	}
	row := make([]string, 0, len(r.fields))
	for _, f := range r.fields {
		v, _ := resultValue(res, f)
		row = append(row, formatValue(v))
	}
	return r.csv.Write(row)
}

func (r *Reporter) writeJSON(res Result) error {
	row := make(map[string]any, len(r.fields))
	for _, f := range r.fields {
		v, _ := resultValue(res, f)
		row[f] = v
	}
	b, err := json.Marshal(row)
	if err != nil {
		return err
	}
	_, err = r.out.Write(append(b, '\n'))
	return err
}

func (r *Reporter) Close() error {
	if r == nil {
		return nil
	}
	var emitErr error
	if r.sortBy != "" {
		sort.SliceStable(r.buf, func(i, j int) bool {
			return r.lessByField(r.buf[i], r.buf[j])
		})
		n := len(r.buf)
		if r.limit > 0 && r.limit < n {
			n = r.limit
		}
		for i := 0; i < n; i++ {
			if err := r.emit(r.buf[i]); err != nil && emitErr == nil {
				emitErr = err
			}
		}
	}
	if r.csv != nil {
		r.csv.Flush()
		if err := r.csv.Error(); err != nil {
			r.file.Close()
			return err
		}
	}
	if r.out != nil {
		if err := r.out.Flush(); err != nil {
			r.file.Close()
			return err
		}
	}
	if err := r.file.Close(); err != nil {
		return err
	}
	return emitErr
}

func formatValue(v any) string {
	switch x := v.(type) {
	case float64:
		return strconv.FormatFloat(x, 'f', 2, 64)
	case string:
		return x
	default:
		return fmt.Sprintf("%v", x)
	}
}

// --- filter expression evaluation -------------------------------------------

func validateFilter(e ast.Expr) error {
	var walkErr error
	ast.Inspect(e, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.Ident:
			if v.Name == "true" || v.Name == "false" {
				return true
			}
			if _, ok := resultValue(Result{}, v.Name); !ok {
				walkErr = fmt.Errorf("unknown identifier %q", v.Name)
				return false
			}
		case *ast.CallExpr, *ast.SelectorExpr, *ast.IndexExpr:
			walkErr = fmt.Errorf("unsupported expression: %T", v)
			return false
		}
		return true
	})
	return walkErr
}

// evalFilter returns whether the expression evaluates truthy against res.
func evalFilter(e ast.Expr, res Result) (bool, error) {
	v, err := evalExpr(e, res)
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("filter expression must evaluate to bool, got %T", v)
	}
	return b, nil
}

func evalExpr(e ast.Expr, res Result) (any, error) {
	switch n := e.(type) {
	case *ast.ParenExpr:
		return evalExpr(n.X, res)
	case *ast.BasicLit:
		return evalLit(n)
	case *ast.Ident:
		switch n.Name {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		v, ok := resultValue(res, n.Name)
		if !ok {
			return nil, fmt.Errorf("unknown identifier %q", n.Name)
		}
		return v, nil
	case *ast.UnaryExpr:
		x, err := evalExpr(n.X, res)
		if err != nil {
			return nil, err
		}
		return evalUnary(n.Op, x)
	case *ast.BinaryExpr:
		// Short-circuit boolean ops.
		if n.Op == token.LAND || n.Op == token.LOR {
			lv, err := evalExpr(n.X, res)
			if err != nil {
				return nil, err
			}
			lb, ok := lv.(bool)
			if !ok {
				return nil, fmt.Errorf("%s requires bool operands", n.Op)
			}
			if n.Op == token.LAND && !lb {
				return false, nil
			}
			if n.Op == token.LOR && lb {
				return true, nil
			}
			rv, err := evalExpr(n.Y, res)
			if err != nil {
				return nil, err
			}
			rb, ok := rv.(bool)
			if !ok {
				return nil, fmt.Errorf("%s requires bool operands", n.Op)
			}
			return rb, nil
		}
		lv, err := evalExpr(n.X, res)
		if err != nil {
			return nil, err
		}
		rv, err := evalExpr(n.Y, res)
		if err != nil {
			return nil, err
		}
		return evalBinary(n.Op, lv, rv)
	}
	return nil, fmt.Errorf("unsupported expression: %T", e)
}

func evalLit(n *ast.BasicLit) (any, error) {
	switch n.Kind {
	case token.INT, token.FLOAT:
		f, err := strconv.ParseFloat(n.Value, 64)
		if err != nil {
			return nil, err
		}
		return f, nil
	case token.STRING:
		s, err := strconv.Unquote(n.Value)
		if err != nil {
			return nil, err
		}
		return s, nil
	}
	return nil, fmt.Errorf("unsupported literal %s", n.Kind)
}

func evalUnary(op token.Token, x any) (any, error) {
	switch op {
	case token.NOT:
		b, ok := x.(bool)
		if !ok {
			return nil, fmt.Errorf("! requires bool")
		}
		return !b, nil
	case token.SUB:
		f, ok := x.(float64)
		if !ok {
			return nil, fmt.Errorf("unary - requires number")
		}
		return -f, nil
	}
	return nil, fmt.Errorf("unsupported unary op %s", op)
}

func evalBinary(op token.Token, l, r any) (any, error) {
	// Equality works for strings or numbers.
	if op == token.EQL || op == token.NEQ {
		eq := l == r
		if op == token.NEQ {
			return !eq, nil
		}
		return eq, nil
	}

	lf, lok := l.(float64)
	rf, rok := r.(float64)
	if !lok || !rok {
		return nil, fmt.Errorf("%s requires numeric operands (got %T, %T)", op, l, r)
	}
	switch op {
	case token.LSS:
		return lf < rf, nil
	case token.GTR:
		return lf > rf, nil
	case token.LEQ:
		return lf <= rf, nil
	case token.GEQ:
		return lf >= rf, nil
	case token.ADD:
		return lf + rf, nil
	case token.SUB:
		return lf - rf, nil
	case token.MUL:
		return lf * rf, nil
	case token.QUO:
		if rf == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return lf / rf, nil
	}
	return nil, fmt.Errorf("unsupported binary op %s", op)
}
