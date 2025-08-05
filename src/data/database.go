package data

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

var db *sql.DB

func InitDB(dataSourceName string) (*sql.DB, error) {
	var err error
	db, err = sql.Open("duckdb", dataSourceName)
	if err != nil {
		return nil, err
	}
	return db, nil
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
		if err := rows.Scan(&assetData.Date, &ticker, &assetData.Open, &assetData.High, &assetData.Low, &assetData.Close, &assetData.Volume); err != nil {
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

func QueryAllAssets(startTime time.Time, endTime time.Time) map[string][]AssetData {
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

func QueryAssetData(ticker string, startTime time.Time, endTime time.Time) []AssetData {
	queryTime := time.Now()
	query := `
	SELECT Date, Ticker, Open, High, Low, Close, Volume FROM stock_data_optimized
	WHERE Ticker = ? AND Date BETWEEN CAST(? AS TIMESTAMP_NS) AND CAST(? AS TIMESTAMP_NS)
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
		if err := rows.Scan(&assetData.Date, &ticker, &assetData.Open, &assetData.High, &assetData.Low, &assetData.Close, &assetData.Volume); err != nil {
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

func GetRiskFreeRates(startTime time.Time, endTime time.Time) map[int64]float64 {
	query := "SELECT daily_risk_free_rate_decimal, Date FROM \"3MTreasuryYields\" WHERE Date  BETWEEN CAST(? AS TIMESTAMP_NS) AND CAST(? AS TIMESTAMP_NS) ORDER BY Date;"
	startTimeStr := startTime.Format("2006-01-02 15:04:05.000000000")
	endTimeStr := endTime.Format("2006-01-02 15:04:05.000000000")

	rows, err := db.Query(query, startTimeStr, endTimeStr)
	if err != nil {
		log.Printf("Error querying data: %v", err)
	}
	defer rows.Close()
	riskFreeRates := make(map[int64]float64, 0)
	for rows.Next() {
		var rate sql.NullFloat64
		var date time.Time
		err := rows.Scan(&rate, &date)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
		}
		if rate.Valid {
			riskFreeRates[date.Unix()] = rate.Float64
		}
	}
	log.Printf("Risk Free Rates: %v\n", riskFreeRates)
	return riskFreeRates
}

func GetTickersWithSufficientData(startTime time.Time, endTime time.Time) []string {
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
