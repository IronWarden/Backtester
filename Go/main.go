package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"gonum.org/v1/gonum/stat"
)

var db *sql.DB
var verbose bool

// logVerbosef prints formatted output if the verbose flag is set.
func logVerbosef(format string, a ...interface{}) {
	if verbose {
		log.Printf(format, a...)
	}
}

type AssetData struct {
	date   time.Time
	open   float64
	high   float64
	low    float64
	close  float64
	volume float64
}

// TODO: Add ability to hold historical positions in portfolio?
type Portfolio struct {
	buyingPower          float64
	positions            map[string]*position
	dailyAvg             map[time.Time]float64
	portfolioCloseValues []float64
	metrics              Metrics
}

// Constructor
func InitializePortfolio(buyingPower float64) *Portfolio {
	return &Portfolio{
		buyingPower:          buyingPower,
		positions:            make(map[string]*position),
		dailyAvg:             make(map[time.Time]float64),
		portfolioCloseValues: make([]float64, 0),
	}
}

func (p *Portfolio) Reset(buyingPower float64) {
	p.buyingPower = buyingPower
	for k := range p.positions {
		delete(p.positions, k)
	}
	for k := range p.dailyAvg {
		delete(p.dailyAvg, k)
	}
	p.portfolioCloseValues = p.portfolioCloseValues[:0]
}

type position struct {
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
func getSortinoRatio(riskFreeRates map[time.Time]float64, dailyAvg map[time.Time]float64) float64 {
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

func getRiskFreeRates(startTime time.Time, endTime time.Time) map[time.Time]float64 {
	query := "SELECT daily_risk_free_rate_decimal, Date FROM \"3MTreasuryYields\" WHERE Date  BETWEEN CAST(? AS TIMESTAMP_NS) AND CAST(? AS TIMESTAMP_NS) ORDER BY Date;"
	startTimeStr := startTime.Format("2006-01-02 15:04:05.000000000")
	endTimeStr := endTime.Format("2006-01-02 15:04:05.000000000")
	rows, err := db.Query(query, startTimeStr, endTimeStr)
	if err != nil {
		log.Printf("Error querying data: %v", err)
	}
	defer rows.Close()
	riskFreeRates := make(map[time.Time]float64, 0)
	for rows.Next() {
		var rate sql.NullFloat64
		var date time.Time
		err := rows.Scan(&rate, &date)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
		}
		if rate.Valid {
			riskFreeRates[date] = rate.Float64
		} else {
			riskFreeRates[date] = 0.0
		}
	}
	logVerbosef("Length of risk free rates: %d", len(riskFreeRates))

	return riskFreeRates
}

func getAnnualReturn(dailyAvg []float64) float64 {
	startValue := 1.0

	for i := range dailyAvg {
		startValue *= (1 + dailyAvg[i])
	}
	logVerbosef("EndValue: %f", startValue)
	numYears := float64(len(dailyAvg)) / 252.0
	// Compound Annual Growth Rate - (end/start) ^ 1/n - 1
	CAGR := math.Pow(startValue, 1/numYears) - 1
	return CAGR * 100
}

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
	logVerbosef("Peak: %.2f, Min: %.2f \n", peak, minValue)
	change := (minValue - peak) / peak

	return change * 100 // Return percentage
}

// getSharpeRatio will return the sharpeRatio
func getSharpeRatio(riskFreeRates map[time.Time]float64, dailyAvg map[time.Time]float64) float64 {
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

//TODO: Add ability to hold short positions

func (p *Portfolio) PrintMetrics() {
	log.Printf("Buying Power: %.2f\n", p.buyingPower)
	if len(p.positions) == 0 {
		log.Println("No positions")
	}
	for key, pos := range p.positions {
		log.Printf("Ticker: %s, Amount: %.2f, Average Price: %.2f, CurrentPrice: %.2f\n", key, pos.amount, pos.averagePrice, pos.currentPrice)
		log.Println("Amount now is", pos.amount*pos.currentPrice+p.buyingPower)
	}
	log.Println("=============================================")
	log.Printf("Annual Metrics: \n")
	log.Printf("Sharpe Ratio: %.2f\n", p.metrics.sharpeRatio)
	log.Printf("Sortino Ratio: %.2f\n", p.metrics.sortinoRatio)
	log.Printf("MaxDrawdown: %.2f\n", p.metrics.maxDrawdown)
	log.Printf("Annual Return: %.2f\n", p.metrics.annualReturn)
	log.Printf("Standard Deviation: %.4f\n", p.metrics.standardDev)
	log.Println("=============================================")
}

func (p *Portfolio) GetBacktestingData(startTime time.Time, endTime time.Time, riskFreeRates map[time.Time]float64) {
	// Create []float64 from map[string]float64
	dailyAvgSlice := make([]float64, 0)
	for date := startTime; date.Before(endTime); date = date.AddDate(0, 0, 1) {
		if val, ok := p.dailyAvg[date.Truncate(24*time.Hour)]; ok {
			dailyAvgSlice = append(dailyAvgSlice, val)
		}
	}

	logVerbosef("length of dailyAvgSlice: %d", len(dailyAvgSlice))
	// annualize standard deviation
	standardDev := stat.StdDev(dailyAvgSlice, nil) * math.Sqrt(252.0)
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

func (p *Portfolio) evaluateAndSaveTicker() {
	if p.metrics.sharpeRatio > 1.0 {
		// Get ticker from positions
		var ticker string
		for t := range p.positions {
			ticker = t
			break
		}
		if ticker == "" {
			return
		}

		// Open file in append mode
		file, err := os.OpenFile("worthy_tickers.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Failed to open file: %v", err)
			return
		}
		defer file.Close()

		// Write ticker to file
		string := fmt.Sprintf("%s, Sharpe Ratio: %.2f, Sortino Ratio: %.2f, Max Drawdown: %.2f, Annual Return: %.2f", ticker, p.metrics.sharpeRatio, p.metrics.sortinoRatio, p.metrics.maxDrawdown, p.metrics.annualReturn)
		if _, err := file.WriteString(string + "\n"); err != nil {
			log.Printf("Failed to write to file: %v", err)
		}
	}
}

func (p *Portfolio) Buy(ticker string, amount float64, initialPrice float64, time time.Time) {
	if p.buyingPower < amount*initialPrice {
		logVerbosef("Not enough buying power to buy %f shares of %s", amount, ticker)
		return
	}
	if amount == 0.0 {
		return
	}
	pos, ok := p.positions[ticker]
	if !ok {
		position := &position{
			amount:       amount,
			averagePrice: initialPrice,
		}
		p.positions[ticker] = position
	} else {
		pos.averagePrice = (pos.averagePrice*pos.amount + initialPrice*amount) / (pos.amount + amount)
		pos.amount += amount
	}
	logVerbosef("Buying %.2f amount of %s at $%.2f date: %s\n", amount, ticker, initialPrice, time)
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
		logVerbosef("You don't have a position here!")
	} else if pos.amount >= stockAmount && pos.amount > 0 {
		logVerbosef("Selling %.2f amount of %s at $%.2f\n", stockAmount, ticker, currentPrice)
		pos.amount -= stockAmount
		if pos.amount == 0 {
			delete(p.positions, ticker)
		} else {
			p.positions[ticker] = pos
		}
		p.Deposit(stockAmount * currentPrice)
	} else {
		logVerbosef("You don't have enough shares to sell")
	}
}

func readStocks(rows *sql.Rows) map[string][]AssetData {
	allAssetData := make(map[string][]AssetData)
	var currentTicker string
	var dailyAssets []AssetData

	for rows.Next() {
		var assetData AssetData
		var ticker string
		if err := rows.Scan(&assetData.date, &ticker, &assetData.open, &assetData.high, &assetData.low, &assetData.close, &assetData.volume); err != nil {
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

func SMA(stocks []AssetData) float64 {
	var mean float64
	for _, stock := range stocks {
		mean += stock.close
	}
	mean /= float64(len(stocks))
	return mean
}

func queryAllAssets(startTime time.Time, endTime time.Time) map[string][]AssetData {
	var rows *sql.Rows
	var err error
	query := `
	SELECT Date, Ticker, Open, High, Low, Close, Volume FROM stock_data_optimized 
	WHERE Date BETWEEN CAST(? AS TIMESTAMP_NS) AND CAST(? AS TIMESTAMP_NS) 
	ORDER BY Ticker, Date;`

	startTimeStr := startTime.Format("2006-01-02 15:04:05.000000000")
	endTimeStr := endTime.Format("2006-01-02 15:04:05.000000000")
	rows, err = db.Query(query, startTimeStr, endTimeStr)
	if err != nil {
		log.Printf("Error querying data: %v", err)
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

func (p *Portfolio) RSICross(ticker string, historicalData []AssetData, startTime time.Time, endTime time.Time, rsPeriod int, lowerThreshold float64, upperThreshold float64) {
	closeValues := make([]float64, 0)
	pastRsi := 0.0

	for i := 1; i < len(historicalData); i++ {
		currentDayData := historicalData[i]
		previousDayData := historicalData[i-1]
		price := currentDayData.close
		change := (price - previousDayData.close) / previousDayData.close

		if i < rsPeriod {
			closeValues = append(closeValues, change)
			continue
		}
		startingValue := p.getPortfolioValue(ticker, previousDayData)
		currentRsi := RSI(closeValues, float64(rsPeriod))
		logVerbosef("currentRsi: %f", currentRsi)

		if currentRsi > upperThreshold && pastRsi <= upperThreshold {
			amount := greedyBuy(p.buyingPower, price)
			p.Buy(ticker, float64(amount), price, currentDayData.date)
		} else if currentRsi < lowerThreshold && pastRsi >= lowerThreshold {
			amount := p.positions[ticker].amount
			p.Sell(ticker, amount, price)
		}

		pastRsi = currentRsi
		endingValue := p.getPortfolioValue(ticker, currentDayData)
		p.adjustPortfolioParameters(ticker, currentDayData, startingValue, endingValue)
		closeValues = append(closeValues[1:], change)
	}
}

func (p *Portfolio) RandomBuySell(ticker string, historicalData []AssetData, localRand *rand.Rand) {

	for i := range historicalData {
		if i == 0 {
			continue
		}

		previousDayData := historicalData[i-1]
		currentDayData := historicalData[i]
		startingValue := p.getPortfolioValue(ticker, previousDayData)
		randomAmount, randomNum := localRand.Float64(), localRand.Float64() // Buy or sell random amounts
		logVerbosef("randomAmount: %f", randomAmount)

		if randomNum >= 0.5 {
			amount := greedyBuy(p.buyingPower*randomAmount, currentDayData.close)
			p.Buy(ticker, float64(amount), currentDayData.close, currentDayData.date)
		} else {
			if pos, ok := p.positions[ticker]; ok && pos.amount > 0 {
				amount := int(pos.amount * randomAmount)
				p.Sell(ticker, float64(amount), currentDayData.close)
			}
		}
		endingValue := p.getPortfolioValue(ticker, currentDayData)
		p.adjustPortfolioParameters(ticker, currentDayData, startingValue, endingValue)
	}
}

// Just buy and stock at the initialPrice at startTime and hold till endTime
func (p *Portfolio) BuyAndHold(ticker string, historicalData []AssetData, startTime time.Time, endTime time.Time, strategyType string) {
	logVerbosef("%s %f", historicalData[0].date, historicalData[0].close)
	initialPrice := historicalData[0].close
	amount := generalBuy(p.buyingPower, initialPrice, strategyType)
	p.Buy(ticker, float64(amount), initialPrice, historicalData[0].date)
	// NOTE: Close to close values should be used for correct metrics

	for i := range historicalData {
		if i == 0 {
			continue
		}
		currentDayData := historicalData[i]
		previousDayData := historicalData[i-1]

		startingValue := p.getPortfolioValue(ticker, previousDayData)
		endingValue := p.getPortfolioValue(ticker, currentDayData)
		p.adjustPortfolioParameters(ticker, currentDayData, startingValue, endingValue)
	}
}

func (p *Portfolio) adjustPortfolioParameters(ticker string, currentDayData AssetData, startingValue float64, endingValue float64) {
	dailyChange := 0.0
	if startingValue > 0.0 {
		dailyChange = (endingValue - startingValue) / startingValue
	}
	// adjust the dailyAvg and closedValues lists
	p.portfolioCloseValues = append(p.portfolioCloseValues, endingValue)
	// fmt.Println("Starting Value: ", startingValue)
	// fmt.Println("Ending Value: ", endingValue)
	// fmt.Printf("Daily Change: %.5f \n", dailyChange*100)
	p.dailyAvg[currentDayData.date] = dailyChange
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
func (p *Portfolio) getPortfolioValue(ticker string, stockData AssetData) float64 {
	var amountAtOpen float64
	if position, ok := p.positions[ticker]; ok {
		amountAtOpen = position.amount
	}
	price := stockData.close
	return p.buyingPower + amountAtOpen*price
}

// TODO: Extend functionality of strategies to work with multiple tickers

// SMACross strategy
func (p *Portfolio) SMACross(ticker string, historicalData []AssetData, shortPeriod int, longPeriod int, startTime time.Time, endTime time.Time) {
	logVerbosef("Length of period is: %d", len(historicalData[longPeriod:]))
	prevShort, prevLong := 0.0, 0.0

	for i := range historicalData {
		if i < longPeriod {
			continue
		}
		currentDayData := historicalData[i]
		previousDayData := historicalData[i-1]
		startingValue := p.getPortfolioValue(ticker, previousDayData)

		smaShort := SMA(historicalData[i-shortPeriod : i])
		smaLong := SMA(historicalData[i-longPeriod : i])

		// Skip first iteration where prevShort is 0
		if prevShort != 0.0 && prevLong != 0.0 {

			changeAmount := 0.0 // Change in stock amount throughout the day
			averagePrice := (currentDayData.low + currentDayData.high + currentDayData.close) / 3.0
			if smaShort > smaLong && prevShort <= prevLong {
				// Use the assets average price in the day
				changeAmount = float64(greedyBuy(p.buyingPower, averagePrice))
				p.Buy(ticker, changeAmount, averagePrice, currentDayData.date)
			} else if smaShort < smaLong && prevShort >= prevLong {
				// Sell all stocks
				if _, ok := p.positions[ticker]; ok {
					changeAmount = p.positions[ticker].amount
				}
				p.Sell(ticker, changeAmount, averagePrice)
			}
		}
		endingValue := p.getPortfolioValue(ticker, currentDayData)
		p.adjustPortfolioParameters(ticker, currentDayData, startingValue, endingValue)
		prevShort, prevLong = smaShort, smaLong
	}
}
func getTickersWithSufficientData(startTime time.Time, endTime time.Time) []string {
	var rows *sql.Rows
	var err error

	// Calculate the minimum number of data points required
	years := endTime.Sub(startTime).Hours() / 24 / 365
	requiredDataPoints := int(years * 250)

	query := `
        SELECT Ticker
        FROM stock_data
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

func test() {
	startTime := time.Date(2015, 6, 12, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2025, 6, 12, 0, 0, 0, 0, time.UTC)
	// shortPeriod, longPeriod := 10, 20
	buyingPower := 20000.0
	tickers := getTickersWithSufficientData(startTime, endTime)
	// tickers := []string{"NVDA"}
	riskFreeRates := getRiskFreeRates(startTime, endTime)
	timeNow := time.Now()
	assets := queryAllAssets(startTime, endTime)
	timeToQuery := time.Since(timeNow)
	simulationTimes := 150
	numWorkers := runtime.NumCPU()

	jobs := make(chan string, len(tickers)*simulationTimes)

	var wg sync.WaitGroup
	var mutex = &sync.Mutex{}

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			localRand := rand.New(rand.NewSource(time.Now().UnixNano()))
			defer wg.Done()
			portfolio := InitializePortfolio(buyingPower)
			for ticker := range jobs {
				portfolio.Reset(buyingPower)
				portfolio.RandomBuySell(ticker, assets[ticker], localRand)
				portfolio.GetBacktestingData(startTime, endTime, riskFreeRates)
				mutex.Lock()
				portfolio.evaluateAndSaveTicker()
				mutex.Unlock()
			}
		}()
	}

	// Distribute jobs
	for i := 0; i < simulationTimes; i++ {
		for _, ticker := range tickers {
			jobs <- ticker
		}
	}
	close(jobs)
	wg.Wait()
	logVerbosef("Time to query is: %v\n", timeToQuery)
}

func main() {
	flag.BoolVar(&verbose, "verbose", false, "Enable verbose output")
	flag.Parse()

	if verbose {
		file, err := os.OpenFile("backtester.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			log.Fatalf("Failed to open log file: %v", err)
		}
		log.SetOutput(file)
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	duckDBPath := "./stock_data.db"
	var err error
	db, err = sql.Open("duckdb", duckDBPath)
	if err != nil {
		log.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer db.Close()
	logVerbosef("Connected to DuckDB: %s\n", duckDBPath)
	test()
}
