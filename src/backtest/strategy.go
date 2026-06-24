package backtest

import (
	"fmt"
	"my-backtester/src/data"
	"strconv"
	"strings"
)

// Strategy operates on a portfolio one day at a time. Step is called for
// every day in the backtest window; the strategy decides whether to act.
// Per-strategy state (e.g. previous SMA values) lives on the implementing
// struct, so a Strategy instance is single-use — clone the portfolio (which
// rebuilds the strategy from its spec) for repeat runs.
type Strategy interface {
	Name() string
	Step(p *Portfolio, hist map[string][]data.AssetData, day int)
}

// NewStrategy builds a Strategy from a spec string and optional typed
// params. Formats:
//   - "greedy" / "equalWeights"          -> BuyAndHold with that buy type
//   - "buyAndHold:<buyType>"             -> BuyAndHold
//   - "smaCross:<short>:<long>:<buyType>" -> SMACross
//   - "lua:<path>"                       -> LuaStrategy (params from arg)
func NewStrategy(spec string, params map[string]any) (Strategy, error) {
	parts := strings.SplitN(spec, ":", 2)
	switch parts[0] {
	case "greedy", "equalWeights":
		return &BuyAndHold{BuyType: parts[0]}, nil
	case "buyAndHold":
		if len(parts) < 2 {
			return nil, fmt.Errorf("buyAndHold spec needs a buy type: %q", spec)
		}
		return &BuyAndHold{BuyType: parts[1]}, nil
	case "smaCross":
		sub := strings.Split(parts[1], ":")
		if len(sub) < 3 {
			return nil, fmt.Errorf(
				"smaCross spec needs short:long:buyType: %q", spec,
			)
		}
		short, err := strconv.Atoi(sub[0])
		if err != nil {
			return nil, fmt.Errorf("smaCross short period: %w", err)
		}
		long, err := strconv.Atoi(sub[1])
		if err != nil {
			return nil, fmt.Errorf("smaCross long period: %w", err)
		}
		return &SMACross{Short: short, Long: long, BuyType: sub[2]}, nil
	case "lua":
		if len(parts) < 2 || parts[1] == "" {
			return nil, fmt.Errorf("lua spec needs a script path: %q", spec)
		}
		return NewLuaStrategy(parts[1], params)
	}
	return nil, fmt.Errorf("unknown strategy spec: %q", spec)
}

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

	return 100 - (100 / (1 + rs))
}

type BuyAndHold struct {
	BuyType string
}

func (s *BuyAndHold) Name() string { return "buyAndHold:" + s.BuyType }

func (s *BuyAndHold) Step(
	p *Portfolio, hist map[string][]data.AssetData, day int,
) {
	if day != 0 {
		return
	}
	for _, ticker := range p.Tickers {
		td := hist[ticker]
		if len(td) == 0 {
			continue
		}
		price := td[0].Close
		amount := generalBuy(p.BuyingPower, price, s.BuyType, p.Tickers)
		p.Buy(ticker, amount, price, td[0].Date)
	}
}

type SMACross struct {
	Short, Long int
	BuyType     string
	prevShort   map[string]float64
	prevLong    map[string]float64
	sumShort    map[string]float64
	sumLong     map[string]float64
	havePrev    map[string]bool
}

func (s *SMACross) Name() string {
	return fmt.Sprintf("smaCross:%d:%d:%s", s.Short, s.Long, s.BuyType)
}

func (s *SMACross) Step(
	p *Portfolio, hist map[string][]data.AssetData, day int,
) {
	if day < s.Long {
		return
	}
	if s.prevShort == nil {
		s.prevShort = make(map[string]float64, len(p.Tickers))
		s.prevLong = make(map[string]float64, len(p.Tickers))
		s.sumShort = make(map[string]float64, len(p.Tickers))
		s.sumLong = make(map[string]float64, len(p.Tickers))
		s.havePrev = make(map[string]bool, len(p.Tickers))
	}
	for _, ticker := range p.Tickers {
		td := hist[ticker]
		if day >= len(td) {
			continue
		}
		var sShort, sLong float64
		if _, seeded := s.sumShort[ticker]; seeded {
			sShort = s.sumShort[ticker] - td[day-s.Short-1].Close + td[day-1].Close
			sLong = s.sumLong[ticker] - td[day-s.Long-1].Close + td[day-1].Close
		} else {
			for i := day - s.Short; i < day; i++ {
				sShort += td[i].Close
			}
			for i := day - s.Long; i < day; i++ {
				sLong += td[i].Close
			}
		}
		s.sumShort[ticker] = sShort
		s.sumLong[ticker] = sLong
		smaShort := sShort / float64(s.Short)
		smaLong := sLong / float64(s.Long)

		if s.havePrev[ticker] {
			currentDayData := td[day]
			avg := (currentDayData.Low +
				currentDayData.High + currentDayData.Close) / 3.0
			if smaShort > smaLong && s.prevShort[ticker] <= s.prevLong[ticker] {
				amount := generalBuy(p.BuyingPower, avg, s.BuyType, p.Tickers)
				p.Buy(ticker, amount, avg, currentDayData.Date)
			} else if smaShort < smaLong && s.prevShort[ticker] >= s.prevLong[ticker] {
				if pos, _ := p.FindPosition(ticker); pos != nil {
					p.Sell(ticker, pos.Amount, avg, currentDayData.Date)
				}
			}
		}
		s.prevShort[ticker] = smaShort
		s.prevLong[ticker] = smaLong
		s.havePrev[ticker] = true
	}
}

func generalBuy(
	buyingPower float64,
	stockValue float64,
	strategyType string,
	tickers []string,
) float64 {
	amount := 0.0
	switch strategyType {
	case "greedy":
		amount = float64(greedyBuy(buyingPower, stockValue))
	case "equalWeights":
		amount = float64(
			greedyBuy(buyingPower/float64(len(tickers)), stockValue),
		)
	}
	return amount
}

func greedyBuy(buyingPower float64, stockValue float64) int {
	return int(buyingPower / stockValue)
}
