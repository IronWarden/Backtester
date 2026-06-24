package backtest

import (
	"io"
	"log"
	"my-backtester/src/data"
	"time"
)

// TransactionLogger receives BUY/SELL records. Defaults to a discard sink
// so callers that don't enable transaction logging (e.g. the UI) don't
// nil-panic; the CLI's --debug flag replaces it with a file-backed logger.
var TransactionLogger = log.New(io.Discard, "", 0)

type DailyReturn struct {
	Date   time.Time
	Return float64
}

type Portfolio struct {
	Pname                string // Portfolio name for tracking purposes
	BuyingPower          float64
	InitialBuyingPower   float64
	Positions            map[string]*Position
	DailyReturns         []DailyReturn
	PortfolioCloseValues []float64
	Metrics              Metrics
	Tickers              []string
	StrategySpec         string
	StrategyParams       map[string]any
	Strategy             Strategy
	StartTime            time.Time
	EndTime              time.Time
}

func InitializePortfolio(
	buyingPower float64,
	startTime, endTime time.Time,
	pname string,
	tickers []string,
	strategySpec string,
	strategyParams map[string]any,
) (*Portfolio, error) {
	strat, err := NewStrategy(strategySpec, strategyParams)
	if err != nil {
		return nil, err
	}
	days := int(endTime.Sub(startTime).Hours() / 24)

	return &Portfolio{
		Pname:                pname,
		BuyingPower:          buyingPower,
		InitialBuyingPower:   buyingPower,
		Positions:            make(map[string]*Position),
		DailyReturns:         make([]DailyReturn, 0, days),
		PortfolioCloseValues: make([]float64, 0, days),
		StartTime:            startTime,
		EndTime:              endTime,
		Tickers:              tickers,
		StrategySpec:         strategySpec,
		StrategyParams:       strategyParams,
		Strategy:             strat,
	}, nil
}

// Clone returns a fresh portfolio with reset state and a new Strategy
// instance built from StrategySpec. Used by the runner so each simulation
// pass gets independent state and workers don't race on shared portfolios.
func (p *Portfolio) Clone() (*Portfolio, error) {
	strat, err := NewStrategy(p.StrategySpec, p.StrategyParams)
	if err != nil {
		return nil, err
	}
	days := cap(p.DailyReturns)
	return &Portfolio{
		Pname:                p.Pname,
		BuyingPower:          p.InitialBuyingPower,
		InitialBuyingPower:   p.InitialBuyingPower,
		Positions:            make(map[string]*Position),
		DailyReturns:         make([]DailyReturn, 0, days),
		PortfolioCloseValues: make([]float64, 0, days),
		StartTime:            p.StartTime,
		EndTime:              p.EndTime,
		Tickers:              p.Tickers,
		StrategySpec:         p.StrategySpec,
		StrategyParams:       p.StrategyParams,
		Strategy:             strat,
	}, nil
}

type Position struct {
	Amount       float64
	AveragePrice float64
	CurrentPrice float64
}

func (p *Portfolio) FindPosition(ticker string) (*Position, bool) {
	pos, ok := p.Positions[ticker]
	return pos, ok
}

func (p *Portfolio) PrintMetrics() {
	log.Printf("Buying Power: %.2f\n", p.BuyingPower)
	if len(p.Positions) == 0 {
		log.Println("No positions")
	}
	for key, pos := range p.Positions {
		log.Printf(
			"Ticker: %s, Amount: %.2f, Average Price: %.2f, CurrentPrice: %.2f\n",
			key, pos.Amount, pos.AveragePrice, pos.CurrentPrice,
		)
		currentValue := pos.Amount*pos.CurrentPrice + p.BuyingPower
		log.Println("Amount now is", currentValue)
	}
	log.Println("=============================================")
	log.Printf("Annual Metrics: \n")
	log.Printf("Sharpe Ratio: %.2f\n", p.Metrics.SharpeRatio)
	log.Printf("Sortino Ratio: %.2f\n", p.Metrics.SortinoRatio)
	log.Printf("MaxDrawdown: %.2f\n", p.Metrics.MaxDrawdown)
	log.Printf("Annual Return: %.2f\n", p.Metrics.AnnualReturn)
	log.Printf("Standard Deviation: %.4f\n", p.Metrics.StandardDev)
	log.Println("=============================================")
}

func (p *Portfolio) Buy(
	ticker string,
	amount float64,
	initialPrice float64,
	time time.Time,
) {
	if p.BuyingPower < amount*initialPrice {
		return
	}
	if amount == 0.0 {
		return
	}
	pos, ok := p.FindPosition(ticker)
	if !ok {
		// Position does not exist, create a new one
		p.Positions[ticker] = &Position{
			Amount:       amount,
			AveragePrice: initialPrice,
		}
	} else {
		// Position exists, update it
		pos.AveragePrice = (pos.AveragePrice*pos.Amount +
			initialPrice*amount) / (pos.Amount + amount)
		pos.Amount += amount
	}
	TransactionLogger.Printf(
		"BUY: %s, Amount: %.2f, Price: %.2f, Date: %s\n",
		ticker, amount, initialPrice, time,
	)
	p.BuyingPower -= amount * initialPrice
}

func (p *Portfolio) Deposit(cash float64) {
	p.BuyingPower += cash
}

func (p *Portfolio) Withdraw(cash float64) {
	p.BuyingPower -= cash
}

func (p *Portfolio) Sell(
	ticker string,
	stockAmount float64,
	currentPrice float64,
	time time.Time,
) {
	pos, ok := p.FindPosition(ticker)
	if !ok || pos.Amount < stockAmount || pos.Amount <= 0 {
		return
	}
	TransactionLogger.Printf(
		"SELL: %s, Amount: %.2f, Price: %.2f, Date: %s\n",
		ticker, stockAmount, currentPrice, time,
	)
	pos.Amount -= stockAmount
	if pos.Amount == 0 {
		delete(p.Positions, ticker)
	}
	p.Deposit(stockAmount * currentPrice)
}

func (p *Portfolio) GetPortfolioValue(
	tickers []string,
	historicalData map[string][]data.AssetData,
	day int,
) float64 {
	value := p.BuyingPower
	for _, ticker := range tickers {
		tickerData := historicalData[ticker]
		if day >= len(tickerData) {
			continue
		}
		if position, ok := p.Positions[ticker]; ok && position.Amount > 0 {
			value += position.Amount * tickerData[day].Close
		}
	}
	return value
}

// AdjustPortfolioParameters records the day's return and refreshes
// current prices on open positions.
func (p *Portfolio) AdjustPortfolioParameters(
	tickers []string,
	currentDayData map[string][]data.AssetData,
	day int,
	startingValue float64,
	endingValue float64,
) {
	dailyChange := 0.0
	if startingValue > 0.0 {
		dailyChange = (endingValue - startingValue) / startingValue
	}
	TransactionLogger.Printf("dailyChange: %.4f\n", dailyChange*100)
	date := currentDayData[tickers[0]][day].Date
	p.DailyReturns = append(p.DailyReturns,
		DailyReturn{Date: date, Return: dailyChange})
	p.PortfolioCloseValues = append(p.PortfolioCloseValues, endingValue)

	for _, ticker := range tickers {
		if pos, ok := p.Positions[ticker]; ok && pos.Amount > 0 {
			tickerData := currentDayData[ticker]
			if day < len(tickerData) {
				pos.CurrentPrice = tickerData[day].Close
			}
		}
	}
}
