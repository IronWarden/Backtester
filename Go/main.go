package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"sort"
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
	buyingPower          float64
	positions            map[string]*position
	openValue            float64
	closeValue           float64
	dailyAvg             map[string]float64 // Store dailyAvg which maps a date to the average change of the day
	portfolioCloseValues []float64
	metrics              Metrics
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
	sortinoRatio float64
	maxDrawdown  float64
	annualReturn float64
	standardDev  float64
}

// getSortinoRatio returns the Sortino Ratio
func getSortinoRatio(riskFreeRates map[string]float64, dailyAvg map[string]float64) float64 {
	excessReturns := make([]float64, 0)
	downsideReturns := make([]float64, 0)

	// Iterate through map for common dates to calculate excess returns
	for key := range dailyAvg {
		if riskFreeRate, ok := riskFreeRates[key]; ok {
			excessReturn := dailyAvg[key] - riskFreeRate
			excessReturns = append(excessReturns, excessReturn)
			if excessReturn < 0 {
				downsideReturns = append(downsideReturns, excessReturn)
			}
		}
	}

	if len(downsideReturns) == 0 {
		return 0.0 // Avoid division by zero if there are no negative returns
	}

	averageExcessReturn := stat.Mean(excessReturns, nil)
	downsideDeviation := stat.StdDev(downsideReturns, nil)

	if downsideDeviation == 0 {
		return 0.0 // Avoid division by zero
	}

	sortinoRatio := averageExcessReturn / downsideDeviation
	// Annualize
	annualizedSortino := sortinoRatio * math.Sqrt(252.0)
	return annualizedSortino
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
	fmt.Println("Length of risk free rates: ", len(riskFreeRates))

	return riskFreeRates
}

// getAnnualReturn will return the annual return over the period as a percentage
func getAnnualReturn(dailyAvg []float64) float64 {
	startValue := 1.0

	for i := range dailyAvg {
		startValue *= (1 + dailyAvg[i])
	}
	fmt.Println("EndValue: ", startValue)
	numYears := float64(len(dailyAvg)) / 252.0
	// Compound Annual Growth Rate - (end/start) ^ 1/n - 1
	CAGR := math.Pow(startValue, 1/numYears) - 1
	return CAGR * 100
}

// getMaxDrawdown will return the maximum drawdown
func getMaxDrawdown(portfolioCloseValues []float64) float64 {
	peak := portfolioCloseValues[0]
	peakIdx := 0
	for i, value := range portfolioCloseValues {
		// Find peak but allow atleast 1 day remaining in the period
		if value > peak && i < len(portfolioCloseValues)-2 {
			peakIdx = i
			peak = value
		}
	}
	// Set minValue to peak for initial value
	minValue := peak
	for i := peakIdx + 1; i < len(portfolioCloseValues); i++ {
		minValue = math.Min(minValue, portfolioCloseValues[i])
	}
	fmt.Printf("Peak: %.2f, Min: %.2f \n", peak, minValue)
	change := (minValue - peak) / peak

	return change * 100 // Return percentage
}

// getSharpeRatio will return the sharpeRatio
func getSharpeRatio(riskFreeRates map[string]float64, dailyAvg map[string]float64) float64 {
	excessReturns := make([]float64, 0)
	// Iterate through map for common dates calculate excess return
	for key := range dailyAvg {
		if _, ok := riskFreeRates[key]; ok {
			excessReturn := dailyAvg[key] - riskFreeRates[key]
			excessReturns = append(excessReturns, excessReturn)
		}
	}
	excessStdev := stat.StdDev(excessReturns, nil)
	// Calculate daily Sharpe ratio
	sharpeRatio := stat.Mean(excessReturns, nil) / excessStdev
	// Annualize
	annualizedSharpe := sharpeRatio * math.Sqrt(252.0)
	return annualizedSharpe
}

//TODO: Implement getting Sortino Ratio

func (p *Portfolio) PrintMetrics() {
	fmt.Println("=============================================")
	fmt.Printf("Sharpe Ratio: %.2f\n", p.metrics.sharpeRatio)
	fmt.Printf("Sortino Ratio: %.2f\n", p.metrics.sortinoRatio)
	fmt.Printf("MaxDrawdown: %.2f\n", p.metrics.maxDrawdown)
	fmt.Printf("Annual Return: %.2f\n", p.metrics.annualReturn)
	fmt.Printf("Standard Deviation: %.4f\n", p.metrics.standardDev)
	fmt.Println("=============================================")
}

func (p *Portfolio) GetBacktestingData(startTime time.Time, endTime time.Time) {
	// Create []float64 from map[string]float64
	dailyAvgSlice := make([]float64, 0)
	dailyAvgStrSlice := make([]string, 0)
	// NOTE: Daily avg has to retain order for calculations to be correct
	for date := range p.dailyAvg {
		dailyAvgStrSlice = append(dailyAvgStrSlice, date)
	}
	sort.Strings(dailyAvgStrSlice)
	for _, value := range dailyAvgStrSlice {
		// fmt.Println(value)
		dailyAvgSlice = append(dailyAvgSlice, p.dailyAvg[value])
	}

	fmt.Println("length of dailyAvgSlice: ", len(dailyAvgSlice))
	standardDev := stat.StdDev(dailyAvgSlice, nil) * math.Sqrt(252.0)
	riskFreeRates := getRiskFreeRates(startTime, endTime)
	sharpeRatio := getSharpeRatio(riskFreeRates, p.dailyAvg)
	sortinoRatio := getSortinoRatio(riskFreeRates, p.dailyAvg)
	annualReturn := getAnnualReturn(dailyAvgSlice)
	maxDrawdown := getMaxDrawdown(p.portfolioCloseValues)
	metrics := Metrics{
		standardDev:  standardDev,
		sharpeRatio:  sharpeRatio,
		sortinoRatio: sortinoRatio,
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
	if amount == 0 {
		return
	}
	pos, ok := p.positions[ticker]
	if !ok {
		position := &position{
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
	} else if pos.amount >= stockAmount && pos.amount > 0 {
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

// TODO: Implement mean reversion strategies

// RSI will calculate the Relative Strength Index and return it
func RSI(closeValues []float64, rsPeriod float64) float64 {
	upDays := make([]float64, 0)
	downDays := make([]float64, 0)
	upDayRs, downDayRs := 0.0, 0.0

	for i := range closeValues {
		if closeValues[i] >= 0 {
			upDays = append(upDays, closeValues[i])
		} else {
			downDays = append(downDays, closeValues[i])
		}
	}
	for _, value := range upDays {
		upDayRs += value
	}
	upDayRs /= rsPeriod
	for _, value := range downDays {
		downDayRs += value
	}
	downDayRs /= rsPeriod
	rs := upDayRs / downDayRs

	// RSI - 100 - (100  / (1 +RS))
	return 100 - (100 / (1 + rs))
}

func RSICross(ticker string, startTime time.Time, endTime time.Time, portfolio *Portfolio, rsPeriod int, lowerThreshold float64, upperThreshold float64) {
	adjustedEndTime := startTime.AddDate(0, 0, -rsPeriod)
	stocks := queryStocks(ticker, startTime, adjustedEndTime)
	closeValues := make([]float64, 0)
	pastRsi := 0.0

	for i := 1; i < len(stocks); i++ {
		currentDayData := stocks[i]
		previousDayData := stocks[i-1]

		price := currentDayData.close
		if currentDayData.adjClose.Valid {
			price = currentDayData.adjClose.Float64
		}

		if i < rsPeriod {
			change := (price - previousDayData.close) / previousDayData.close
			closeValues = append(closeValues, change)
			continue
		}
		startingValue := portfolio.getPortfolioValue(ticker, previousDayData)

		currentRsi := RSI(closeValues, float64(rsPeriod))

		if currentRsi > upperThreshold && pastRsi <= upperThreshold {
			amount := greedyBuy(portfolio.buyingPower, price)
			portfolio.Buy(ticker, float64(amount), price, currentDayData.date.Format("2006-01-02"))
		} else if currentRsi < lowerThreshold && pastRsi >= lowerThreshold {
			amount := portfolio.positions[ticker].amount * 0.5
			portfolio.Sell(ticker, amount, price)
		}

		pastRsi = currentRsi
		endingValue := portfolio.getPortfolioValue(ticker, currentDayData)
		portfolio.adjustPortfolioParameters(currentDayData, startingValue, endingValue)
	}
}

// Just buy and stock at the initialPrice at startTime and hold till endTime
func (p *Portfolio) BuyAndHold(ticker string, startTime time.Time, endTime time.Time, strategyType string) {
	stocks := queryStocks(ticker, startTime, endTime)
	fmt.Println(stocks[0].date, stocks[0].close, stocks[0].adjClose)
	initialPrice := stocks[0].close
	if stocks[0].adjClose.Valid {
		fmt.Println("Using adjusted close")
		initialPrice = stocks[0].adjClose.Float64
	}
	amount := generalBuy(p.buyingPower, initialPrice, strategyType)
	p.Buy(ticker, float64(amount), initialPrice, stocks[0].date.Format("2006-01-02"))
	// NOTE: Close to close values should be used for correct metrics

	for i := range stocks {
		if i == 0 {
			continue
		}
		currentDayData := stocks[i]
		previousDayData := stocks[i-1]

		startingValue := p.getPortfolioValue(ticker, previousDayData)
		endingValue := p.getPortfolioValue(ticker, currentDayData)
		p.adjustPortfolioParameters(currentDayData, startingValue, endingValue)
	}
}

func (p *Portfolio) adjustPortfolioParameters(currentDayData StockData, startingValue float64, endingValue float64) {
	date := currentDayData.date.Format("2006-01-02")
	ticker := currentDayData.ticker
	dailyChange := (endingValue - startingValue) / startingValue
	// adjust the dailyAvg and closedValues lists
	p.portfolioCloseValues = append(p.portfolioCloseValues, endingValue)
	// fmt.Println("Starting Value: ", startingValue)
	// fmt.Println("Ending Value: ", endingValue)
	// fmt.Printf("Daily Change: %.5f \n", dailyChange*100)
	p.dailyAvg[date] = dailyChange
	// adjust current price of a stock in a positions
	if pos, ok := p.positions[ticker]; ok {
		pos.currentPrice = currentDayData.close
	}
}

func generalBuy(buyingPower float64, stockValue float64, strategyType string) float64 {
	amount := 0.0
	switch strategyType {
	// buy as much as you can
	case "greedy":
		amount = float64(greedyBuy(buyingPower, stockValue))
	// buy fixed percent
	case "percentBuy":
		amount = float64(greedyBuy(buyingPower*0.25, stockValue))
	}
	return amount
}

// Buy has much stock as you can without fractional shares
func greedyBuy(buyingPower float64, stockValue float64) int {
	return int(buyingPower / stockValue)
}

// getPortfolioValue() will return the portfolio underlying value of assets plus cash
// we will use the close value for close to close comparisions
func (p *Portfolio) getPortfolioValue() float64 {
	value := p.buyingPower
	for _, pos := range p.positions {
		value += pos.amount * pos.currentPrice
	}
	return value
}

// TODO: Extend functionality of strategies to work with multiple tickers
// SMACross strategy
func (p *Portfolio) SMACross(tickers []string, shortPeriod int, longPeriod int, startTime time.Time, endTime time.Time) {
	adjustedStartTime := startTime.AddDate(0, 0, -longPeriod)
	var shortestRange int
	var shortestRangeTicker string
	historicalData := make(map[string][]StockData, 0)
	for _, ticker := range tickers {
		currentData := queryStocks(ticker, adjustedStartTime, endTime)
		currentShortestRange := len(currentData)
		// Update the shortest range and shortest range ticker
		if shortestRange == 0 || currentShortestRange < shortestRange {
			shortestRange = currentShortestRange
			shortestRangeTicker = ticker
		}
		historicalData[ticker] = currentData
	}
	commonPeriod := historicalData[shortestRangeTicker]
	fmt.Println("Length of period is: ", len(commonPeriod[longPeriod:]))
	prevShort := make(map[string]float64)
	prevLong := make(map[string]float64)

	// Iterate each day
	for i := range commonPeriod {
		// Iterate for each ticker given in order
		for _, ticker := range tickers {
			value := historicalData[ticker]
			if i < longPeriod {
				continue
			}
			currentDayData := value[i]
			previousDayData := value[i-1]
			ticker := currentDayData.ticker
			date := currentDayData.date.Format("2006-01-02")
			fmt.Printf("Date: %s, Ticker: %s \n", date, ticker)
			startingValue := p.getPortfolioValue()

			smaShort := SMA(value[i-shortPeriod : i])
			smaLong := SMA(value[i-longPeriod : i])

			changeAmount := 0.0 // Change in stock amount throughout the day
			if smaShort > smaLong && prevShort[ticker] <= prevLong[ticker] {
				changeAmount = float64(greedyBuy(p.buyingPower*0.50, currentDayData.close))
				p.Buy(ticker, changeAmount, currentDayData.close, date)
			} else if smaShort < smaLong && prevShort[ticker] >= prevLong[ticker] {
				// Sell all stocks
				if _, ok := p.positions[ticker]; ok {
					changeAmount = p.positions[ticker].amount * 0.50
				}
				p.Sell(ticker, changeAmount, currentDayData.close)
			}
			endingValue := p.getPortfolioValue()
			p.adjustPortfolioParameters(currentDayData, startingValue, endingValue)
			prevShort[ticker] = smaShort
			prevLong[ticker] = smaLong
		}
	}
}
func test() {
	startTime := time.Date(2014, 6, 29, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 6, 29, 0, 0, 0, 0, time.UTC)
	// shortPeriod, longPeriod := 10, 20
	portfolio := &Portfolio{
		buyingPower:          20000,
		positions:            make(map[string]*position),
		dailyAvg:             make(map[string]float64),
		portfolioCloseValues: make([]float64, 0),
	}
	tickers := []string{"TSLA", "AAPL", "NVDA", "MSFT"}
	portfolio.SMACross(tickers, 10, 20, startTime, endTime)

	// SMACross("VOO", 10, 20, startTime, endTime, portfolio)
	// portfolio.BuyAndHold("VOO", startTime, endTime, "percentBuy")
	// portfolio.BuyAndHold("NVDA", startTime, endTime, "greedy")
	// portfolio.BuyAndHold("TSLA", startTime, endTime, "greedy")
	fmt.Printf("Buying Power: %.2f\n", portfolio.buyingPower)
	if len(portfolio.positions) == 0 {
		fmt.Println("No positions")
	}
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
