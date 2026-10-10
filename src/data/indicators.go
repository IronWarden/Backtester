package data

import "time"

// IndicatorPoint is one observation of an economic_indicators series. Date is
// the start of the period it describes, not when it was published.
type IndicatorPoint struct {
	Date  time.Time
	Value float64
}

// LoadIndicator reads one economic_indicators series, sorted by date: the
// Research tab's fallback when FRED cannot be reached. Nil, nil when the
// table is absent; an empty slice when the series is. Like GetCPISeries it
// reads the date through asDate, since loaders here have stored it as DATE,
// TIMESTAMP and text, and it keeps zero values, which are real for a rate.
func LoadIndicator(seriesID string) ([]IndicatorPoint, error) {
	if db == nil || !tableExists("economic_indicators") {
		return nil, nil
	}
	rows, err := db.Query(
		`SELECT Date, value FROM economic_indicators WHERE series_id = ? AND value IS NOT NULL ORDER BY Date`,
		seriesID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []IndicatorPoint{}
	for rows.Next() {
		var raw any
		var v float64
		if err := rows.Scan(&raw, &v); err != nil {
			return nil, err
		}
		if d, ok := asDate(raw); ok {
			out = append(out, IndicatorPoint{Date: d, Value: v})
		}
	}
	return out, rows.Err()
}
