package main

import (
	"fmt"
	"strings"
	"time"

	"my-backtester/src/data"
)

// Result cap for the Data view's SQL console — generous enough for
// exploration, small enough to keep the webview table snappy.
const maxConsoleRows = 1000

// QueryResult is the wire shape of one console query's result set.
type QueryResult struct {
	Columns   []string   `json:"columns"`
	Rows      [][]string `json:"rows"`
	Truncated bool       `json:"truncated"`
	ElapsedMs int64      `json:"elapsedMs"`
}

// RunUserQuery executes one SQL statement against the DuckDB at dbPath for
// the Data view's console. Same read-only gate as the assistant's query_db
// tool: the DB is the market-data store maintained by external pipelines, so
// mutating statements are rejected rather than risked.
func (a *App) RunUserQuery(dbPath, sql string) (QueryResult, error) {
	var res QueryResult
	if dbPath == "" {
		return res, fmt.Errorf("no database chosen — pick a DB first")
	}
	if strings.TrimSpace(sql) == "" {
		return res, fmt.Errorf("empty query")
	}
	kw := firstSQLKeyword(sql)
	allowed := false
	for _, p := range readOnlySQLPrefixes {
		if kw == p {
			allowed = true
			break
		}
	}
	if !allowed {
		return res, fmt.Errorf(
			"%q is not allowed — only read-only statements run here "+
				"(SELECT, WITH, DESCRIBE, SHOW, SUMMARIZE, EXPLAIN)", kw)
	}
	if _, err := data.InitDB(dbPath); err != nil {
		return res, fmt.Errorf("open db %q: %w", dbPath, err)
	}
	start := time.Now()
	cols, rows, truncated, err := data.RunQuery(sql, maxConsoleRows)
	if err != nil {
		return res, err
	}
	res.Columns = cols
	res.Rows = rows
	res.Truncated = truncated
	res.ElapsedMs = time.Since(start).Milliseconds()
	return res, nil
}
