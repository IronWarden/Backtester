package data

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

var (
	db     *sql.DB
	dbDSN  string
	initMu sync.Mutex
)

// InitDB opens the DuckDB at dataSourceName and installs it as the package's
// shared handle. Re-initializing with the same path is a no-op returning the
// existing handle; a new path closes the old handle first so the process
// doesn't accumulate file locks.
func InitDB(dataSourceName string) (*sql.DB, error) {
	initMu.Lock()
	defer initMu.Unlock()
	if db != nil && dataSourceName == dbDSN {
		return db, nil
	}
	next, err := sql.Open("duckdb", dataSourceName)
	if err != nil {
		return nil, err
	}
	if db != nil {
		db.Close()
	}
	db = next
	dbDSN = dataSourceName
	return db, nil
}

// ListTickers returns the distinct ticker symbols available in the price
// table, sorted alphabetically. Used to populate the UI's ticker picker.
// Requires InitDB to have been called.
func ListTickers() ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	rows, err := db.Query(
		`SELECT DISTINCT Ticker FROM stock_data_optimized ORDER BY Ticker`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tickers []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		tickers = append(tickers, t)
	}
	return tickers, rows.Err()
}

// DateRange is the earliest and latest Date available for a ticker.
type DateRange struct {
	Min time.Time
	Max time.Time
}

// TickerDateRanges returns the available [Min, Max] Date for each of the given
// tickers. Tickers with no rows are omitted. A nil/empty `tickers` returns
// ranges for every ticker in the table. Used to validate that a backtest's
// window is actually covered by its tickers' data, and to constrain the UI's
// date pickers. Requires InitDB to have been called.
func TickerDateRanges(tickers []string) (map[string]DateRange, error) {
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}

	query := `SELECT Ticker, MIN(Date), MAX(Date)
		FROM stock_data_optimized`
	args := make([]any, 0, len(tickers))
	if len(tickers) > 0 {
		placeholders := strings.Repeat("?,", len(tickers))
		placeholders = placeholders[:len(placeholders)-1]
		query += fmt.Sprintf(" WHERE Ticker IN (%s)", placeholders)
		for _, t := range tickers {
			args = append(args, t)
		}
	}
	query += " GROUP BY Ticker"

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]DateRange)
	for rows.Next() {
		var ticker string
		var min, max time.Time
		if err := rows.Scan(&ticker, &min, &max); err != nil {
			return nil, err
		}
		out[ticker] = DateRange{Min: min, Max: max}
	}
	return out, rows.Err()
}

// RunQuery executes an arbitrary SQL statement on the open database and
// returns the column names plus up to maxRows rows, every value rendered as
// a string ("NULL" for SQL NULLs, YYYY-MM-DD for midnight timestamps). The
// boolean reports whether the result set was cut off at maxRows. Backs the
// in-app assistant's read-only query tool. Requires InitDB.
func RunQuery(query string, maxRows int) ([]string, [][]string, bool, error) {
	if db == nil {
		return nil, nil, false, fmt.Errorf("database not initialized")
	}
	rows, err := db.Query(query)
	if err != nil {
		return nil, nil, false, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, false, err
	}

	out := make([][]string, 0)
	truncated := false
	for rows.Next() {
		if len(out) >= maxRows {
			truncated = true
			break
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, false, err
		}
		rec := make([]string, len(cols))
		for i, v := range vals {
			rec[i] = renderSQLValue(v)
		}
		out = append(out, rec)
	}
	return cols, out, truncated, rows.Err()
}

func renderSQLValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case time.Time:
		if x.Hour() == 0 && x.Minute() == 0 && x.Second() == 0 {
			return x.Format("2006-01-02")
		}
		return x.Format("2006-01-02 15:04:05")
	case []byte:
		return string(x)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	default:
		return fmt.Sprint(x)
	}
}

type AssetData struct {
	Date   time.Time
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume float64
}

func ReadStocks(rows *sql.Rows) map[string][]AssetData {
	allAssetData := make(map[string][]AssetData)
	var currentTicker string
	var dailyAssets []AssetData

	for rows.Next() {
		var assetData AssetData
		var ticker string
		err := rows.Scan(&assetData.Date, &ticker, &assetData.Open,
			&assetData.High, &assetData.Low, &assetData.Close, &assetData.Volume)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		if currentTicker != "" && ticker != currentTicker {
			allAssetData[currentTicker] = dailyAssets
			dailyAssets = nil
		}
		currentTicker = ticker
		dailyAssets = append(dailyAssets, assetData)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error during rows iteration: %v", err)
	}

	// Add the last ticker
	if currentTicker != "" {
		allAssetData[currentTicker] = dailyAssets
	}

	return allAssetData
}

func QueryAllAssets(
	startTime time.Time,
	endTime time.Time,
) map[string][]AssetData {
	timeQuery := time.Now()
	var rows *sql.Rows
	var err error
	query := `
	SELECT Date, Ticker, Open, High, Low, Close, Volume FROM stock_data_optimized
	WHERE Date BETWEEN CAST(? AS TIMESTAMP_NS) AND CAST(? AS TIMESTAMP_NS);
	`

	startTimeStr := startTime.Format("2006-01-02 15:04:05.000000000")
	endTimeStr := endTime.Format("2006-01-02 15:04:05.000000000")

	rows, err = db.Query(query, startTimeStr, endTimeStr)
	if err != nil {
		log.Printf("Error querying data: %v", err)
	}
	defer rows.Close()
	stocks := ReadStocks(rows)
	log.Printf("Query time: %s\n", time.Since(timeQuery))
	return stocks
}

// QueryAssetsForTickers fetches OHLCV data for a known set of tickers
// in a single round-trip, bucketing rows by ticker via ReadStocks.
func QueryAssetsForTickers(
	tickers []string,
	startTime time.Time,
	endTime time.Time,
) map[string][]AssetData {
	if len(tickers) == 0 {
		return map[string][]AssetData{}
	}

	placeholders := strings.Repeat("?,", len(tickers))
	placeholders = placeholders[:len(placeholders)-1]
	query := fmt.Sprintf(`
		SELECT Date, Ticker, Open, High, Low, Close, Volume
		FROM stock_data_optimized
		WHERE Ticker IN (%s)
		  AND Date BETWEEN CAST(? AS TIMESTAMP_NS) AND CAST(? AS TIMESTAMP_NS)
		ORDER BY Ticker, Date;
	`, placeholders)

	args := make([]any, 0, len(tickers)+2)
	for _, t := range tickers {
		args = append(args, t)
	}
	args = append(args,
		startTime.Format("2006-01-02 15:04:05.000000000"),
		endTime.Format("2006-01-02 15:04:05.000000000"),
	)

	queryTime := time.Now()
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("Error querying assets for %d tickers: %v", len(tickers), err)
		return map[string][]AssetData{}
	}
	defer rows.Close()

	result := ReadStocks(rows)
	log.Printf("Query time for %d tickers: %s\n", len(tickers), time.Since(queryTime))
	return result
}

func QueryAssetData(
	ticker string,
	startTime time.Time,
	endTime time.Time,
) []AssetData {
	queryTime := time.Now()
	query := `
	SELECT Date, Ticker, Open, High, Low, Close, Volume FROM stock_data_optimized
	WHERE Ticker = ? AND 
		Date BETWEEN CAST(? AS TIMESTAMP_NS) AND CAST(? AS TIMESTAMP_NS)
	ORDER BY Date;
	`

	startTimeStr := startTime.Format("2006-01-02 15:04:05.000000000")
	endTimeStr := endTime.Format("2006-01-02 15:04:05.000000000")

	rows, err := db.Query(query, ticker, startTimeStr, endTimeStr)
	if err != nil {
		log.Printf("Error querying data for ticker %s: %v", ticker, err)
		return nil
	}
	defer rows.Close()

	var dailyAssets []AssetData
	for rows.Next() {
		var assetData AssetData
		var ticker string
		err := rows.Scan(&assetData.Date, &ticker, &assetData.Open,
			&assetData.High, &assetData.Low, &assetData.Close, &assetData.Volume)
		if err != nil {
			log.Printf("Failed to scan row for ticker %s: %v", ticker, err)
			continue
		}
		dailyAssets = append(dailyAssets, assetData)
	}
	if err := rows.Err(); err != nil {
		log.Printf("Error during rows iteration for ticker %s: %v", ticker, err)
	}
	log.Printf("Query time for %s: %s\n", ticker, time.Since(queryTime))
	return dailyAssets
}

func GetRiskFreeRates(
	startTime time.Time,
	endTime time.Time,
) map[int64]float64 {
	query := "SELECT daily_risk_free_rate_decimal, Date FROM " +
		"\"3MTreasuryYields\" WHERE Date BETWEEN CAST(? AS " +
		"TIMESTAMP_NS) AND CAST(? AS TIMESTAMP_NS) ORDER BY Date;"
	startTimeStr := startTime.Format("2006-01-02 15:04:05.000000000")
	endTimeStr := endTime.Format("2006-01-02 15:04:05.000000000")

	rows, err := db.Query(query, startTimeStr, endTimeStr)
	if err != nil {
		log.Printf("Error querying risk free rates: %v, returning empty map", err)
		return make(map[int64]float64)
	}
	defer rows.Close()
	riskFreeRates := make(map[int64]float64)
	for rows.Next() {
		var rate sql.NullFloat64
		var date time.Time
		if err := rows.Scan(&rate, &date); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}
		if rate.Valid {
			riskFreeRates[date.Unix()] = rate.Float64
		}
	}
	return riskFreeRates
}

func GetTickersWithSufficientData(
	startTime time.Time,
	endTime time.Time,
) []string {
	var rows *sql.Rows
	var err error

	// Calculate the minimum number of data points required
	years := endTime.Sub(startTime).Hours() / 24 / 365
	requiredDataPoints := int(years * 250)

	query := `
        SELECT Ticker
        FROM stock_data_optimized
        WHERE Date BETWEEN CAST(? AS TIMESTAMP_NS) AND CAST(? AS TIMESTAMP_NS)
        GROUP BY Ticker
        HAVING COUNT(*) >= ?
    `
	startTimeStr := startTime.Format("2006-01-02 15:04:05.000000000")
	endTimeStr := endTime.Format("2006-01-02 15:04:05.000000000")

	rows, err = db.Query(query, startTimeStr, endTimeStr, requiredDataPoints)
	if err != nil {
		log.Printf("Error querying data: %v", err)
	}
	defer rows.Close()

	tickers := make([]string, 0)
	for rows.Next() {
		var ticker string
		err := rows.Scan(&ticker)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
		}
		tickers = append(tickers, ticker)
	}
	return tickers
}

// Bar is one daily OHLCV row destined for stock_data_optimized. Date is a
// YYYY-MM-DD string; prices are split/dividend-adjusted per the table's
// convention, Volume is float to match the DOUBLE column.
type Bar struct {
	Date   string
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume float64
}

// UpsertBars replaces ticker's rows within the bars' date range, in one
// transaction — the same idempotent upsert add_ticker.py performs, but
// through this process's handle so it works while the UI holds the DuckDB
// write lock. Creates the table when writing into a fresh database.
// Requires InitDB.
func UpsertBars(ticker string, bars []Bar) error {
	if db == nil {
		return fmt.Errorf("database not initialized")
	}
	if len(bars) == 0 {
		return fmt.Errorf("no bars to store")
	}
	lo, hi := bars[0].Date, bars[0].Date
	for _, b := range bars {
		if b.Date < lo {
			lo = b.Date
		}
		if b.Date > hi {
			hi = b.Date
		}
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS stock_data_optimized (
		Date TIMESTAMP_NS, Ticker VARCHAR,
		Open DOUBLE, High DOUBLE, Low DOUBLE, Close DOUBLE,
		Volume DOUBLE, "Adj Close" DOUBLE)`); err != nil {
		return fmt.Errorf("ensuring table: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`DELETE FROM stock_data_optimized WHERE Ticker = ?
		 AND Date BETWEEN CAST(? AS TIMESTAMP_NS) AND CAST(? AS TIMESTAMP_NS)`,
		ticker, lo, hi,
	); err != nil {
		return fmt.Errorf("deleting old rows: %w", err)
	}

	// Multi-row inserts in chunks: one statement per row is painfully slow
	// over database/sql, and a single statement for decades of history gets
	// unwieldy. 500 rows x 7 params stays well inside DuckDB's limits.
	const chunk = 500
	for at := 0; at < len(bars); at += chunk {
		end := at + chunk
		if end > len(bars) {
			end = len(bars)
		}
		part := bars[at:end]
		placeholders := make([]string, 0, len(part))
		args := make([]any, 0, len(part)*7)
		for _, b := range part {
			placeholders = append(placeholders,
				"(CAST(? AS TIMESTAMP_NS), ?, ?, ?, ?, ?, ?)")
			args = append(args, b.Date, ticker, b.Open, b.High, b.Low, b.Close, b.Volume)
		}
		q := `INSERT INTO stock_data_optimized
			(Date, Ticker, Open, High, Low, Close, Volume) VALUES ` +
			strings.Join(placeholders, ",")
		if _, err := tx.Exec(q, args...); err != nil {
			return fmt.Errorf("inserting rows: %w", err)
		}
	}
	return tx.Commit()
}
