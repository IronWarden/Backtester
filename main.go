package main

import (
	"fmt"
	"github.com/piquette/finance-go/chart"
	"github.com/piquette/finance-go/datetime"
)

func main() {
	params := &chart.Params{
		Symbol:   "AAPL",
		Interval: datetime.OneDay, // daily data
		Start: &datetime.Datetime{
			Year:  2023,
			Month: 1,
			Day:   1,
		},
		End: &datetime.Datetime{
			Year:  2024,
			Month: 1,
			Day:   1,
		},
	}

	iter := chart.Get(params)

	for iter.Next() {
		bar := iter.Bar()
		fmt.Printf("Date: %s, Open: %.2f, High: %.2f, Low: %.2f, Close: %.2f, Volume: %d\n",
			bar.Timestamp.Format("2006-01-02"), bar.Open, bar.High, bar.Low, bar.Close, bar.Volume)
	}

	if err := iter.Err(); err != nil {
		fmt.Println("Error fetching data:", err)
	}
}

