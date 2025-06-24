package main

import (
	"database/sql"
	"fmt"
	_ "github.com/marcboeker/go-duckdb"
	"log"
	"time"
)

var db *sql.DB

type StockData struct {
	Date     time.Time
	Ticker   string
	Open     float64
	High     float64
	Low      float64
	Close    float64
	Volume   float64
	AdjClose sql.NullFloat64
}

type Portfolio struct {
	BuyingPower float64
	Positions   []Position
}

// Simple long position is tickers and amount of stocks owned with price
type Position struct {
	Ticker       string
	Amount       float64
	InitialPrice float64
}

// Long buy signal
func (p *Portfolio) Buy(ticker string, amount float64, initialPrice float64) {
	position := Position{
		Ticker:       ticker,
		Amount:       amount,
		InitialPrice: initialPrice,
	}
	if p.BuyingPower < amount*initialPrice {
		fmt.Println("Not enough buying power to buy", amount, "shares of", ticker)
		return
	}
	p.Positions = append(p.Positions, position)
	p.BuyingPower -= amount * initialPrice
}

func (p *Portfolio) Deposit(cash float64) {
	p.BuyingPower += cash
}

func (p *Portfolio) Withdraw(cash float64) {
	p.BuyingPower -= cash
}

// sell position method should modify average price and
func (pos *Position) Sell(amount float64, currentPrice float64) {
	if pos.Amount < amount {
		fmt.Println("Not enough shares to sell")
		return
	}
	pos.Amount -= amount
	pos.InitialPrice = (pos.InitialPrice*pos.Amount + currentPrice*amount) / (pos.Amount + amount)
}

// readDB will read the rows returned from a query and return a []StockData
func readDB(rows *sql.Rows) []StockData {
	stocks := make([]StockData, 0)
	for rows.Next() {
		stockData := StockData{}
		err := rows.Scan(&stockData.Date, &stockData.Ticker, &stockData.Open, &stockData.High, &stockData.Low, &stockData.Close, &stockData.Volume, &stockData.AdjClose)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		stocks = append(stocks, stockData)
	}

	return stocks
}

// SMA given (ticker, startTime, endTime)
func SMA(stocks []StockData) float64 {
	var mean float64
	for _, stock := range stocks {
		mean += stock.Close
	}
	mean /= float64(len(stocks))
	return mean
}

func queryStocks(ticker string, startTime time.Time, endTime time.Time) []StockData {
	query := "SELECT * FROM stock_data WHERE Ticker = ? AND Date  BETWEEN CAST(? AS TIMESTAMP_NS) AND CAST(? AS TIMESTAMP_NS);"
	startTimeStr := startTime.Format("2006-01-02 15:04:05.000000000")
	endTimeStr := endTime.Format("2006-01-02 15:04:05.000000000")
	rows, err := db.Query(query, ticker, startTimeStr, endTimeStr)
	if err != nil {
		fmt.Printf("Error querying data: %v", err)
	}
	defer rows.Close()
	stocks := readDB(rows)
	return stocks
}

// TODO: Make a SMACross
func SMACross(ticker string, shortPeriod int, longPeriod int, startTime time.Time, endTime time.Time, portfolio *Portfolio) {
	startTime = startTime.AddDate(0, 0, -longPeriod)
	stocks := queryStocks(ticker, startTime, endTime)

	for i := 0; i < len(stocks); i++ {
		currentDayData := stocks[i]
		currentDate := currentDayData.Date

		if currentDate.Before(startTime) || currentDate.After(endTime) {
			continue
		}
		if i < longPeriod-1 {
			continue
		}

		smaShort := SMA(stocks[i-shortPeriod+1 : i+1])
		smaLong := SMA(stocks[i-longPeriod+1 : i+1])

		if smaShort > smaLong {
			portfolio.Buy(ticker, 100, currentDayData.Close)
		}
	}
}

func fetchPrice(ticker string, currentTime string) float64 {
	var price float64
	query := "SELECT Close FROM stock_data WHERE Ticker = ? AND Date = CAST(? AS TIMESTAMP_NS);"
	rows, err := db.Query(query, ticker, currentTime)
	if err != nil {
		log.Fatalf("Error querying data: %v", err)
	}
	defer rows.Close()
	rows.Scan(&price)

	return price
}

func queryAllStocks() {
	fmt.Println("\nQuerying first 5 rows from 'stock_data' table:")
	rows, err := db.Query("SELECT Date, Ticker, Open, High, Low, Close, Volume, \"Adj Close\" FROM stock_data LIMIT 5;")
	if err != nil {
		log.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()
	// Get column types BEFORE iterating through rows
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Fatalf("Failed to get column types: %v", err)
	}

	fmt.Println("--- Column Data Types ---")
	for _, ct := range columnTypes {
		fmt.Printf("  Column: %-15s | DB Type: %-10s | Go Scan Type: %-15v | Nullable: %t\n",
			ct.Name(),             // Column name
			ct.DatabaseTypeName(), // Database-specific type name (e.g., "DATE", "VARCHAR", "DOUBLE")
			ct.ScanType(),         // Go's reflect.Type that Scan would typically use (e.g., time.Time, string, float64)
		)
	}
	fmt.Println("-------------------------")
	for rows.Next() {
		var (
			date     time.Time
			ticker   string
			open     float64
			high     float64
			low      float64
			close    float64
			volume   float64
			adjClose sql.NullFloat64
		)
		err := rows.Scan(&date, &ticker, &open, &high, &low, &close, &volume, &adjClose)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("Date: %s, Ticker: %s, Open: %.2f, High: %.2f, Low: %.2f, Close: %.2f, Volume: %.0f, AdjClose: %.2f\n",
			date, ticker, open, high, low, close, volume, adjClose)
	}
	if err = rows.Err(); err != nil {
		log.Fatalf("Error iterating rows: %v", err)
	}
}

func main() {
	duckDBPath := "stock_data.db" // Name of your DuckDB file
	var err error
	db, err = sql.Open("duckdb", duckDBPath)
	if err != nil {
		log.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer db.Close()
	fmt.Printf("Connected to DuckDB: %s\n", duckDBPath)
	// queryAllStocks()
	startTime := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC)
	portfolio := Portfolio{
		BuyingPower: 1000000,
		Positions:   []Position{},
	}
	SMACross("AAPL", 10, 30, startTime, endTime, &portfolio)
	fmt.Printf("Buying Power: %.2f\n", portfolio.BuyingPower)
	for _, pos := range portfolio.Positions {
		fmt.Printf("Ticker: %s, Amount: %.2f, Initial Price: %.2f\n", pos.Ticker, pos.Amount, pos.InitialPrice)
	}
}
