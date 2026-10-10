package backtest

import (
	"strings"
	"testing"
	"time"

	"my-backtester/src/data"
)

// hostHist runs 2024-01-02 .. 2024-01-06, one calendar day per index.
func hostDay(i int) time.Time { return time.Date(2024, 1, 2+i, 0, 0, 0, 0, time.UTC) }

func freqPoint(
	metric, freq string, periodEnd string, known time.Time, value float64,
) data.FundamentalPoint {
	pe, _ := time.Parse("2006-01-02", periodEnd)
	return data.FundamentalPoint{
		RawFundamental: data.RawFundamental{
			Ticker: "AAA", Metric: metric, PeriodEnd: pe, Value: value,
			Frequency: freq,
		},
		KnownFrom: known,
	}
}

func secPoint(
	metric, freq string, periodEnd string, known time.Time, value float64, instant bool,
) data.FundamentalPoint {
	p := freqPoint(metric, freq, periodEnd, known, value)
	p.Source = data.SourceSEC
	p.Instant = instant
	return p
}

const logDay4 = `
	function step(day)
	  if day ~= 4 then return end
	  %s
	end
`

func script(body string) string { return strings.Replace(logDay4, "%s", body, 1) }

// T108: a ticker publishing both frequencies. The four-argument form ignores
// the other frequency; the three-argument form is what it always was — the
// newest publication, here the quarter.
func TestFundamentalFrequencyKeepsQuarterAndYearApart(t *testing.T) {
	points := []data.FundamentalPoint{
		freqPoint("Net Income", "annual", "2023-09-30", hostDay(0), 400),
		freqPoint("Net Income", "quarterly", "2023-12-31", hostDay(2), 110),
	}
	out := runFundamentalScript(t, script(`
		log("bare " .. tostring(fundamental("AAA","Net Income",day)))
		log("annual " .. tostring(fundamental("AAA","Net Income",day,"annual")))
		log("quarterly " .. tostring(fundamental("AAA","Net Income",day,"quarterly")))
	`), []string{"Net Income"}, points)
	for _, want := range []string{"bare 110", "annual 400", "quarterly 110"} {
		if !contains(out, want) {
			t.Errorf("want %q; got:\n%s", want, out)
		}
	}
}

func TestFundamentalPeriodsBack(t *testing.T) {
	points := []data.FundamentalPoint{
		freqPoint("Total Assets", "annual", "2021-12-31", hostDay(0), 100),
		freqPoint("Total Assets", "annual", "2022-12-31", hostDay(0), 120),
		// Published on day 4 — not yet known on day 3.
		freqPoint("Total Assets", "annual", "2023-12-31", hostDay(4), 150),
	}
	out := runFundamentalScript(t, `
		function step(day)
		  if day ~= 3 and day ~= 4 then return end
		  local now  = fundamental("AAA","Total Assets",day,"annual")
		  local prev = fundamental("AAA","Total Assets",day,"annual",1)
		  local far  = fundamental("AAA","Total Assets",day,"annual",5)
		  log("day " .. day .. ": " .. now .. " " .. prev .. " " .. tostring(far))
		end
	`, []string{"Total Assets"}, points)
	for _, want := range []string{"day 3: 120 100 nil", "day 4: 150 120 nil"} {
		if !contains(out, want) {
			t.Errorf("want %q; got:\n%s", want, out)
		}
	}
}

// A typo must be an error, never a silent fall-back to "any frequency".
func TestFundamentalRejectsBadArguments(t *testing.T) {
	points := []data.FundamentalPoint{
		freqPoint("Net Income", "annual", "2023-09-30", hostDay(0), 400),
	}
	for name, call := range map[string]string{
		"typo":            `fundamental("AAA","Net Income",day,"anual")`,
		"negative back":   `fundamental("AAA","Net Income",day,"annual",-1)`,
		"fractional back": `fundamental("AAA","Net Income",day,"annual",1.5)`,
		"back, no freq":   `fundamental("AAA","Net Income",day,nil,1)`,
	} {
		t.Run(name, func(t *testing.T) {
			out := runFundamentalScript(t, script(`log("got " .. tostring(`+call+`))`),
				[]string{"Net Income"}, points)
			if contains(out, "got ") {
				t.Errorf("%s returned instead of raising:\n%s", call, out)
			}
			if !contains(out, "bad argument") {
				t.Errorf("%s: no argument error logged:\n%s", call, out)
			}
		})
	}
}

// Under SEC the bare form never mixes: a flow reads the newest full year even
// when a newer quarter exists, and a balance-sheet level reads the newest
// balance sheet.
func TestSECFundamentalBareFormDefaults(t *testing.T) {
	points := []data.FundamentalPoint{
		secPoint("Net Income", "annual", "2023-09-30", hostDay(0), 400, false),
		secPoint("Net Income", "quarterly", "2023-12-31", hostDay(1), 110, false),
		secPoint("Stockholders Equity", "annual", "2023-09-30", hostDay(0), 900, true),
		secPoint("Stockholders Equity", "quarterly", "2023-09-30", hostDay(0), 900, true),
		secPoint("Stockholders Equity", "quarterly", "2023-12-31", hostDay(1), 950, true),
	}
	out := runFundamentalScriptFrom(t, script(`
		log("ni " .. tostring(fundamental("AAA","Net Income",day)))
		log("eq " .. tostring(fundamental("AAA","Stockholders Equity",day)))
		log("eq annual " .. tostring(fundamental("AAA","Stockholders Equity",day,"annual")))
		log("ni prev " .. tostring(fundamental("AAA","Net Income",day,nil,1)))
	`), []string{"Net Income", "Stockholders Equity"}, points, data.SourceSEC)
	for _, want := range []string{"ni 400", "eq 950", "eq annual 900", "ni prev nil"} {
		if !contains(out, want) {
			t.Errorf("want %q; got:\n%s", want, out)
		}
	}
}

// A filing can publish an OLDER period for the first time after a newer one
// was known (a metric a company starts tagging, with comparatives). The newer
// period must still win.
func TestSECFundamentalLatestPeriodNotLatestPublication(t *testing.T) {
	points := []data.FundamentalPoint{
		secPoint("Goodwill", "quarterly", "2023-12-31", hostDay(0), 50, true),
		secPoint("Goodwill", "quarterly", "2022-12-31", hostDay(2), 10, true),
	}
	out := runFundamentalScriptFrom(t, script(
		`log("gw " .. tostring(fundamental("AAA","Goodwill",day)))`),
		[]string{"Goodwill"}, points, data.SourceSEC)
	if !contains(out, "gw 50") {
		t.Errorf("want the 2023 figure; got:\n%s", out)
	}
}

func TestSECMarketCapIsNil(t *testing.T) {
	points := []data.FundamentalPoint{
		secPoint("Ordinary Shares Number", "quarterly", "2023-12-31", hostDay(0), 1e9, true),
	}
	out := runFundamentalScriptFrom(t, script(
		`log("cap " .. tostring(market_cap("AAA", day)))`),
		[]string{"Ordinary Shares Number"}, points, data.SourceSEC)
	if !contains(out, "cap nil") {
		t.Errorf("market_cap under SEC must be nil; got:\n%s", out)
	}
}

// Coverage under SEC counts what the bare form returns — a level with only a
// quarterly copy is covered, a flow with only a quarter is not.
func TestSECFundamentalCoverageUsesTheDefaults(t *testing.T) {
	points := []data.FundamentalPoint{
		secPoint("Total Assets", "quarterly", "2023-12-31", hostDay(0), 1, true),
		secPoint("Net Income", "quarterly", "2023-12-31", hostDay(0), 1, false),
	}
	got := FundamentalCoverage(points, []string{"AAA"},
		[]string{"Total Assets", "Net Income"}, []time.Time{hostDay(1)})
	if got != 50 {
		t.Errorf("coverage = %v, want 50", got)
	}
}
