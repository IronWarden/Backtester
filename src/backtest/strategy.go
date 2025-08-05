package backtest

import (
	"math/rand"
	"time"

	"my-backtester/src/data"
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

func (p *Portfolio) RSICross(ticker string, historicalData []data.AssetData, startTime time.Time, endTime time.Time, rsPeriod int, lowerThreshold float64, upperThreshold float64) {
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

func (p *Portfolio) BuyAndHold(ticker string, historicalData []data.AssetData, startTime time.Time, endTime time.Time, strategyType string) {
	initialPrice := historicalData[0].Close
	amount := generalBuy(p.BuyingPower, initialPrice, strategyType)
	p.Buy(ticker, float64(amount), initialPrice, historicalData[0].Date)

	for i := range historicalData {
		if i == 0 {
			continue
		}
		currentDayData := historicalData[i]
		previousDayData := historicalData[i-1]

		startingValue := p.GetPortfolioValue(ticker, previousDayData.Close)
		endingValue := p.GetPortfolioValue(ticker, currentDayData.Close)
		p.AdjustPortfolioParameters(ticker, currentDayData, startingValue, endingValue)
	}
}

func (p *Portfolio) SMACross(ticker string, historicalData []data.AssetData, shortPeriod int, longPeriod int, startTime time.Time, endTime time.Time) {
	prevShort, prevLong := 0.0, 0.0

	for i := range historicalData {
		if i < longPeriod {
			continue
		}
		currentDayData := historicalData[i]
		previousDayData := historicalData[i-1]
		startingValue := p.GetPortfolioValue(ticker, previousDayData.Close)

		smaShort := SMA(historicalData[i-shortPeriod : i])
		smaLong := SMA(historicalData[i-longPeriod : i])

		if prevShort != 0.0 && prevLong != 0.0 {

			changeAmount := 0.0 // Change in stock amount throughout the day
			averagePrice := (currentDayData.Low + currentDayData.High + currentDayData.Close) / 3.0
			if smaShort > smaLong && prevShort <= prevLong {
				changeAmount = float64(greedyBuy(p.BuyingPower, averagePrice))
				p.Buy(ticker, changeAmount, averagePrice, currentDayData.Date)
			} else if smaShort < smaLong && prevShort >= prevLong {
				if pos, _ := p.FindPosition(ticker); pos != nil {
					changeAmount = pos.Amount
				}
				p.Sell(ticker, changeAmount, averagePrice, currentDayData.Date)
			}
		}
		endingValue := p.GetPortfolioValue(ticker, currentDayData.Close)
		p.AdjustPortfolioParameters(ticker, currentDayData, startingValue, endingValue)
		prevShort, prevLong = smaShort, smaLong
	}
}

func generalBuy(buyingPower float64, stockValue float64, strategyType string) float64 {
	amount := 0.0
	switch strategyType {
	case "greedy":
		amount = float64(greedyBuy(buyingPower, stockValue))
	case "percentBuy":
		amount = float64(greedyBuy(buyingPower*0.25, stockValue))
	}
	return amount
}

func greedyBuy(buyingPower float64, stockValue float64) int {
	return int(buyingPower / stockValue)
}