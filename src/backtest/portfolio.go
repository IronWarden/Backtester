package backtest

import (
	"log"
	"time"

	"my-backtester/src/data"
)

var TransactionLogger *log.Logger

type DailyReturn struct {
	Date   time.Time
	Return float64
}

type Portfolio struct {
	BuyingPower          float64
	Positions            map[string]*Position
	DailyReturns         []DailyReturn
	PortfolioCloseValues []float64
	Metrics              Metrics
}

func InitializePortfolio(buyingPower float64, days int) *Portfolio {
	return &Portfolio{
		BuyingPower:          buyingPower,
		Positions:            make(map[string]*Position),
		DailyReturns:         make([]DailyReturn, 0, days),
		PortfolioCloseValues: make([]float64, 0, days),
	}
}

func (p *Portfolio) Reset(buyingPower float64) {
	p.BuyingPower = buyingPower
	// Clear the map for the next simulation
	for k := range p.Positions {
		delete(p.Positions, k)
	}
	p.DailyReturns = p.DailyReturns[:0]
	p.PortfolioCloseValues = p.PortfolioCloseValues[:0]
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
		log.Printf("Ticker: %s, Amount: %.2f, Average Price: %.2f, CurrentPrice: %.2f\n", key, pos.Amount, pos.AveragePrice, pos.CurrentPrice)
		log.Println("Amount now is", pos.Amount*pos.CurrentPrice+p.BuyingPower)
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

func (p *Portfolio) Buy(ticker string, amount float64, initialPrice float64, time time.Time) {
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
		pos.AveragePrice = (pos.AveragePrice*pos.Amount + initialPrice*amount) / (pos.Amount + amount)
		pos.Amount += amount
	}
	TransactionLogger.Printf("BUY: %s, Amount: %.2f, Price: %.2f, Date: %s\n", ticker, amount, initialPrice, time)
	p.BuyingPower -= amount * initialPrice
}

func (p *Portfolio) Deposit(cash float64) {
	p.BuyingPower += cash
}

func (p *Portfolio) Withdraw(cash float64) {
	p.BuyingPower -= cash
}

func (p *Portfolio) Sell(ticker string, stockAmount float64, currentPrice float64, time time.Time) {
	pos, ok := p.FindPosition(ticker)
	if !ok {
	} else if pos.Amount >= stockAmount && pos.Amount > 0 {
		TransactionLogger.Printf("SELL: %s, Amount: %.2f, Price: %.2f, Date: %s\n", ticker, stockAmount, currentPrice, time)
		pos.Amount -= stockAmount
		if pos.Amount == 0 {
			delete(p.Positions, ticker) // Remove from map
		}
		p.Deposit(stockAmount * currentPrice)
	} else {
	}
}

func (p *Portfolio) GetPortfolioValue(ticker string, price float64) float64 {
	var amount float64
	if position, ok := p.FindPosition(ticker); ok {
		amount = position.Amount
	}
	return p.BuyingPower + amount*price
}

func (p *Portfolio) AdjustPortfolioParameters(ticker string, currentDayData data.AssetData, startingValue float64, endingValue float64) {
	dailyChange := 0.0
	if startingValue > 0.0 {
		dailyChange = (endingValue - startingValue) / startingValue
	}
	TransactionLogger.Printf("dailyChange: %.4f\n", dailyChange*100)
	p.DailyReturns = append(p.DailyReturns, DailyReturn{Date: currentDayData.Date, Return: dailyChange})
	p.PortfolioCloseValues = append(p.PortfolioCloseValues, endingValue)

	if pos, _ := p.FindPosition(ticker); pos != nil {
		pos.CurrentPrice = currentDayData.Close
	}
}
