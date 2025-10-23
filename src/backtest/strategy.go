package backtest

import (
	"math/rand"

	"my-backtester/src/data"
)

// NOTE:
// A strategy must be defined as follows
// For any give day it may be given context it needs to execute the logic
// It must return a signal defined as Buy, Hold, Sell

type Signal int

const (
	Buy Signal = iota
	Sell
	Hold
)

func SMA(stocks []data.AssetData) float64 {
	var mean float64
	for _, stock := range stocks {
		mean += stock.Close
	}
	mean /= float64(len(stocks))
	return mean
}

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

func (p *Portfolio) RSICross(historicalData map[string]float64, rsPeriod int, lowerThreshold float64, upperThreshold float64) {
	closeValues := make([]float64, 0)
	pastRsi := 0.0

	for i := 1; i < len(historicalData); i++ {
		currentDayData := historicalData[i]
		previousDayData := historicalData[i-1]
		price := currentDayData.Close
		change := (price - previousDayData.Close) / previousDayData.Close

		if i < rsPeriod {
			closeValues = append(closeValues, change)
			continue
		}
		startingValue := p.GetPortfolioValue(ticker, previousDayData.Close)
		currentRsi := RSI(closeValues, float64(rsPeriod))

		if currentRsi > upperThreshold && pastRsi <= upperThreshold {
			amount := greedyBuy(p.BuyingPower, price)
			p.Buy(ticker, float64(amount), price, currentDayData.Date)
		} else if currentRsi < lowerThreshold && pastRsi >= lowerThreshold {
			pos, _ := p.FindPosition(ticker)
			if pos != nil {
				amount := pos.Amount
				p.Sell(ticker, amount, price, currentDayData.Date)
			}
		}

		pastRsi = currentRsi
		endingValue := p.GetPortfolioValue(ticker, currentDayData.Close)
		p.AdjustPortfolioParameters(ticker, currentDayData, startingValue, endingValue)
		closeValues = append(closeValues[1:], change)
	}
}

func (p *Portfolio) RandomBuySell(ticker string, historicalData []data.AssetData, localRand *rand.Rand) {

	for i := range historicalData {
		if i == 0 {
			continue
		}

		previousDayData := historicalData[i-1]
		currentDayData := historicalData[i]
		startingValue := p.GetPortfolioValue(ticker, previousDayData.Close)
		randomAmount, randomNum := localRand.Float64(), localRand.Float64() // Buy or sell random amounts

		if randomNum >= 0.5 {
			amount := greedyBuy(p.BuyingPower*randomAmount, currentDayData.Close)
			p.Buy(ticker, float64(amount), currentDayData.Close, currentDayData.Date)
		} else {
			if pos, _ := p.FindPosition(ticker); pos != nil && pos.Amount > 0 {
				amount := int(pos.Amount * randomAmount)
				p.Sell(ticker, float64(amount), currentDayData.Close, currentDayData.Date)
			}
		}
		endingValue := p.GetPortfolioValue(ticker, currentDayData.Close)
		p.AdjustPortfolioParameters(ticker, currentDayData, startingValue, endingValue)
	}
}

func (p *Portfolio) BuyAndHold(historicalData map[string][]data.AssetData, strategyType string) Signal {
	tickers := p.Tickers
	startTime, endTime := p.StartTime, p.EndTime
	// Initialize the positions at the start
	for _, ticker := range tickers {
		initialPrice := historicalData[ticker][0].Close
		amount := generalBuy(p.BuyingPower, initialPrice, strategyType)
		p.Buy(ticker, float64(amount), initialPrice, historicalData[ticker][0].Date)
	}

	// Day loop from startTime to endTime
	i := 1
	arbitraryTicker := tickers[0]
	for time := startTime; time.Before(endTime); time = time.AddDate(0, 0, 1) {
		for _, ticker := range tickers {
			if historicalData[ticker][i-1].Date != time {
				continue
			}
		}
		startingValue := p.GetPortfolioValue(tickers, historicalData, i-1)
		endingValue := p.GetPortfolioValue(tickers, historicalData, i)
		p.AdjustPortfolioParameters(tickers, historicalData, i, startingValue, endingValue)
	}
}

func (p *Portfolio) SMACross(historicalData map[string][]data.AssetData, shortPeriod int, longPeriod int, buyingType string) {
	prevShort, prevLong := make(map[string]float64), make(map[string]float64)
	arbitraryTicker := p.Tickers[0]

	for i := longPeriod; i < len(historicalData[arbitraryTicker]); i++ {
		previousDay, currentDay := (i - 1), i
		startingValue := p.GetPortfolioValue(p.Tickers, historicalData, previousDay)
		for _, ticker := range p.Tickers {
			currentDayData := historicalData[ticker][currentDay]

			smaShort := SMA(historicalData[ticker][i-shortPeriod : i])
			smaLong := SMA(historicalData[ticker][i-longPeriod : i])

			if prevShort[ticker] != 0.0 && prevLong[ticker] != 0.0 {

				changeAmount := 0.0 // Change in stock amount throughout the day
				averagePrice := (currentDayData.Low + currentDayData.High + currentDayData.Close) / 3.0
				if smaShort > smaLong && prevShort[ticker] <= prevLong[ticker] {
					changeAmount = float64(generalBuy(p.BuyingPower, averagePrice, "equalWeights", p.Tickers))
					p.Buy(ticker, changeAmount, averagePrice, currentDayData.Date)
				} else if smaShort < smaLong && prevShort[ticker] >= prevLong[ticker] {
					if pos, _ := p.FindPosition(ticker); pos != nil {
						changeAmount = pos.Amount
					}
					p.Sell(ticker, changeAmount, averagePrice, currentDayData.Date)
				}
			}
			prevShort[ticker], prevLong[ticker] = smaShort, smaLong
		}
		endingValue := p.GetPortfolioValue(p.Tickers, historicalData, currentDay)
		p.AdjustPortfolioParameters(p.Tickers, historicalData, currentDay, startingValue, endingValue)
	}
}

func generalBuy(buyingPower float64, stockValue float64, strategyType string, tickers []string) float64 {
	amount := 0.0
	switch strategyType {
	case "greedy":
		amount = float64(greedyBuy(buyingPower, stockValue))
	case "equalWeights":
		amount = float64(greedyBuy(buyingPower*float64(1/len(tickers)), stockValue))
	}
	return amount
}

func greedyBuy(buyingPower float64, stockValue float64) int {
	return int(buyingPower / stockValue)
}
