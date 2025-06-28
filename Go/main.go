package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"gonum.org/v1/gonum/stat"
)

var db *sql.DB

type StockData struct {
	date     time.Time
	ticker   string
	open     float64
	high     float64
	low      float64
	close    float64
	volume   float64
	adjClose sql.NullFloat64
}

// TODO: Add ability to hold historical positions in portfolio?
type Portfolio struct {
	buyingPower float64
	positions   map[string]position
	openValue   float64
	closeValue  float64
	dailyAvg    map[string]float64 // Store dailyAvg which maps a date to the average change of the day
	closeValues []float64
	metrics     Metrics
}

type position struct {
	time         string
	ticker       string
	amount       float64
	averagePrice float64
	currentPrice float64
}

type Metrics struct {
	sharpeRatio  float64
	maxDrawdown  float64
	annualReturn float64
	standardDev  float64
}

// getRiskFreeRates() will return a map of daily risk free rates with the key being a date string i.e 'YYYY-MM-DD'
func getRiskFreeRates(startTime time.Time, endTime time.Time) map[string]float64 {
	query := "SELECT daily_risk_free_rate_decimal, Date FROM \"3MTreasuryYields\" WHERE Date  BETWEEN CAST(? AS TIMESTAMP_NS) AND CAST(? AS TIMESTAMP_NS);"
	startTimeStr := startTime.Format("2006-01-02 15:04:05.000000000")
	endTimeStr := endTime.Format("2006-01-02 15:04:05.000000000")
	rows, err := db.Query(query, startTimeStr, endTimeStr)
	if err != nil {
		fmt.Printf("Error querying data: %v", err)
	}
	defer rows.Close()
	riskFreeRates := make(map[string]float64, 0)
	for rows.Next() {
		var rate sql.NullFloat64
		var date time.Time
		err := rows.Scan(&rate, &date)
		if err != nil {
			fmt.Printf("Error scanning row: %v", err)
		}
		dateStr := date.Format("2006-01-02")
		if rate.Valid {
			riskFreeRates[dateStr] = rate.Float64
		} else {
			riskFreeRates[dateStr] = 0.0
		}
	}
	return riskFreeRates
}

// getAnnualReturn will return the annual return over the period as a percentage
func getAnnualReturn(dailyAvg []float64) float64 {
	startValue := 1.0

	for _, value := range dailyAvg {
		startValue *= (1 + value)
	}
	numYears := float64(len(dailyAvg)) / 365.25
	// Compound Annual Growth Rate - (end/start) ^ 1/n - 1
	CAGR := math.Pow(startValue, 1/float64(numYears)-1)
	return CAGR * 100
}

// getMaxDrawdown will return the maximum drawdown
func getMaxDrawdown(closeValues []float64) float64 {
	peak := closeValues[0]
	peakIdx := 0
	minValue := closeValues[0]
	for i, value := range closeValues {
		if value > peak {
			peakIdx = i
			peak = value
		}
	}
	for i := peakIdx + 1; i < len(closeValues); i++ {
		minValue = math.Min(minValue, closeValues[i])
	}

	return (peak - minValue) / peak
}

// getSharpeRatio will return the sharpeRatio
func getSharpeRatio(riskFreeRates map[string]float64, dailyAvg map[string]float64) float64 {
	excessReturns := make([]float64, 0)
	// Iterate through map for common dates calculate excess return
	for key := range riskFreeRates {
		excessReturn := dailyAvg[key] - riskFreeRates[key]
		fmt.Println("excess return:", excessReturn)
		if _, ok := dailyAvg[key]; ok {
			excessReturns = append(excessReturns, excessReturn)
		}
	}
	excessStdev := stat.StdDev(excessReturns, nil)
	sharpeRatio := stat.Mean(excessReturns, nil) / excessStdev
	return sharpeRatio
}

//TODO: Implement getting Sortino Ratio

func (p *Portfolio) PrintMetrics() {
	fmt.Println("=============================================")
	fmt.Printf("Sharpe Ratio: %.2f\n", p.metrics.sharpeRatio)
	fmt.Printf("MaxDrawdown: %.2f\n", p.metrics.maxDrawdown)
	fmt.Printf("Annual Return: %.2f\n", p.metrics.annualReturn)
	fmt.Printf("Standard Deviation: %.2f\n", p.metrics.standardDev)
	fmt.Println("=============================================")
}

func (p *Portfolio) GetBacktestingData(startTime time.Time, endTime time.Time) {
	// Create []float64 from map[string]float64
	dailyAvgSlice := make([]float64, 0)
	for _, value := range p.dailyAvg {
		dailyAvgSlice = append(dailyAvgSlice, value)
	}
	standardDev := stat.StdDev(dailyAvgSlice, nil)
	riskFreeRates := getRiskFreeRates(startTime, endTime)
	sharpeRatio := getSharpeRatio(riskFreeRates, p.dailyAvg)
	annualReturn := getAnnualReturn(dailyAvgSlice)
	maxDrawdown := getMaxDrawdown(p.closeValues)
	metrics := Metrics{
		standardDev:  standardDev,
		sharpeRatio:  sharpeRatio,
		maxDrawdown:  maxDrawdown,
		annualReturn: annualReturn,
	}
	p.metrics = metrics
	p.PrintMetrics()
}

func (p *Portfolio) Buy(ticker string, amount float64, initialPrice float64, time string) {
	if p.buyingPower < amount*initialPrice {
		fmt.Println("Not enough buying power to buy", amount, "shares of", ticker)
		return
	}
	pos, ok := p.positions[ticker]
	if !ok {
		position := position{
			time:         time,
			ticker:       ticker,
			amount:       amount,
			averagePrice: initialPrice,
		}
		p.positions[ticker] = position
	} else {
		pos.averagePrice = (pos.averagePrice*pos.amount + initialPrice*amount) / (pos.amount + amount)
		pos.amount += amount
		pos.time = time
		p.positions[ticker] = pos
	}
	fmt.Printf("Buying %.2f amount of %s at $%.2f date: %s\n", amount, ticker, initialPrice, time)
	p.buyingPower -= amount * initialPrice
}

func (p *Portfolio) Deposit(cash float64) {
	p.buyingPower += cash
}

func (p *Portfolio) Withdraw(cash float64) {
	p.buyingPower -= cash
}

func (p *Portfolio) Sell(ticker string, stockAmount float64, currentPrice float64) {
	pos, ok := p.positions[ticker]
	if !ok {
		fmt.Println("You don't have a position here!")
	} else if pos.amount >= stockAmount {
		fmt.Printf("Selling %.2f amount of %s at $%.2f\n", stockAmount, ticker, currentPrice)
		pos.amount -= stockAmount
		if pos.amount == 0 {
			delete(p.positions, ticker)
		} else {
			p.positions[ticker] = pos
		}
		p.Deposit(stockAmount * currentPrice)
	} else {
		fmt.Println("You don't have enough shares to sell")
	}
}

func readStocks(rows *sql.Rows) []StockData {
	stocks := make([]StockData, 0)
	for rows.Next() {
		stockData := StockData{}
		err := rows.Scan(&stockData.date, &stockData.ticker, &stockData.open, &stockData.high, &stockData.low, &stockData.close, &stockData.volume, &stockData.adjClose)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		stocks = append(stocks, stockData)
	}

	return stocks
}

func SMA(stocks []StockData) float64 {
	var mean float64
	for _, stock := range stocks {
		mean += stock.close
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
	stocks := readStocks(rows)
	return stocks
}

func SMACross(ticker string, shortPeriod int, longPeriod int, startTime time.Time, endTime time.Time, portfolio *Portfolio) {
	adjustedStartTime := startTime.AddDate(0, 0, -longPeriod)
	stocks := queryStocks(ticker, adjustedStartTime, endTime)

	for i := 0; i < len(stocks); i++ {
		currentDayData := stocks[i]
		date := currentDayData.date.Format("2006-01-02")

		if i < longPeriod-1 {
			continue
		}
		startingValue := portfolio.buyingPower + portfolio.positions[ticker].amount*currentDayData.open

		smaShort := SMA(stocks[i-shortPeriod+1 : i+1])
		smaLong := SMA(stocks[i-longPeriod+1 : i+1])

		if smaShort > smaLong {
			portfolio.Buy(ticker, 100, currentDayData.close, date)
		}
		if smaShort < smaLong {
			amount := portfolio.positions[ticker].amount
			portfolio.Sell(ticker, amount, currentDayData.close)
		}

		endingValue := portfolio.buyingPower + portfolio.positions[ticker].amount*currentDayData.close
		// adjust the dailyAvg and closedValues lists
		portfolio.closeValues = append(portfolio.closeValues, endingValue)
		portfolio.dailyAvg[date] = (endingValue - startingValue) / startingValue
		// adjust current price of a stock in a positions
		pos := portfolio.positions[ticker]
		pos.currentPrice = currentDayData.close
		portfolio.positions[ticker] = pos
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
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Fatalf("Failed to get column types: %v", err)
	}

	fmt.Println("--- Column Data Types ---")
	for _, ct := range columnTypes {
		fmt.Printf("  Column: %-15s | DB Type: %-10s | Go Scan Type: %-15v | Nullable: %t\n",
			ct.Name(),
			ct.DatabaseTypeName(),
			ct.ScanType(),
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

func test() {
	startTime := time.Date(2011, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	portfolio := Portfolio{
		buyingPower: 20000,
		positions:   make(map[string]position),
		dailyAvg:    make(map[string]float64),
		closeValues: make([]float64, 0),
	}
	SMACross("AAPL", 10, 30, startTime, endTime, &portfolio)
	fmt.Printf("Buying Power: %.2f\n", portfolio.buyingPower)
	for _, pos := range portfolio.positions {
		fmt.Printf("Ticker: %s, Amount: %.2f, Average Price: %.2f, CurrentPrice: %.2f\n", pos.ticker, pos.amount, pos.averagePrice, pos.currentPrice)
		fmt.Println("Amount now is", pos.amount*pos.currentPrice+portfolio.buyingPower)
	}
	portfolio.GetBacktestingData(startTime, endTime)
}

func main() {
	duckDBPath := "stock_data.db"
	var err error
	db, err = sql.Open("duckdb", duckDBPath)
	if err != nil {
		log.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer db.Close()
	fmt.Printf("Connected to DuckDB: %s\n", duckDBPath)
	test()
}
