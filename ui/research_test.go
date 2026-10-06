package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"my-backtester/src/data"
)

// researchDB builds a scratch database with prices, a profile, S&P 500
// membership and earnings dates for ACME -- and no sec_financials or
// data_quality table, so the views' missing-table paths run too.
func researchDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "research.db")
	db, err := data.InitDB(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`CREATE TABLE stock_data_optimized (Date TIMESTAMP, Ticker VARCHAR, Open DOUBLE,
			High DOUBLE, Low DOUBLE, Close DOUBLE, Volume BIGINT)`,
		`INSERT INTO stock_data_optimized VALUES
			('2025-01-27', 'ACME', 0, 0, 0, 100, 1),
			('2025-01-28', 'ACME', 0, 0, 0, 110, 1),
			('2025-01-29', 'ACME', 0, 0, 0, 121, 1)`,
		`CREATE TABLE company_profile (ticker VARCHAR, cik VARCHAR, name VARCHAR, sic VARCHAR,
			sic_description VARCHAR, sector VARCHAR, exchange VARCHAR, former_names VARCHAR,
			state_of_incorporation VARCHAR, fetched_at TIMESTAMP)`,
		`INSERT INTO company_profile VALUES ('ACME', '0000123', 'Acme Corp', '3571',
			'Electronic Computers', 'Manufacturing', 'Nasdaq', '', 'DE', '2026-01-01')`,
		`CREATE TABLE index_membership (index_name VARCHAR, ticker VARCHAR, security VARCHAR,
			start_date DATE, end_date DATE, start_is_horizon BOOLEAN, confidence VARCHAR,
			source VARCHAR, fetched_at TIMESTAMP)`,
		`INSERT INTO index_membership VALUES ('SP500', 'ACME', 'Acme', '2011-01-03',
			'9999-12-31', true, 'high', 'wikipedia', '2026-01-01')`,
		`CREATE TABLE earnings_calendar (Ticker VARCHAR, Date TIMESTAMP, Value VARCHAR)`,
		`INSERT INTO earnings_calendar VALUES ('ACME', '2025-01-28 16:00:00', 'Earnings')`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
	}
	return path
}

func TestCompanyOverview(t *testing.T) {
	path := researchDB(t)
	a := &App{}
	got := a.CompanyOverview(path, " acme ")
	if got.Error != "" {
		t.Fatal(got.Error)
	}
	if got.Ticker != "ACME" || got.Name != "Acme Corp" || got.CIK != "0000123" || got.Exchange != "Nasdaq" {
		t.Errorf("profile: %+v", got)
	}
	if got.Price == nil || got.Price.LastClose != 121 || got.Price.LastDate != "2025-01-29" ||
		got.Price.High52 != 121 || got.Price.Low52 != 100 {
		t.Errorf("price: %+v", got.Price)
	}
	if len(got.Index) != 1 || got.Index[0].End != "" || !got.Index[0].AtLeastSince {
		t.Errorf("a current member with a horizon start: %+v", got.Index)
	}
	if !strings.Contains(strings.Join(got.Notes, " "), "add_data_quality.py") {
		t.Errorf("a missing audit table must say which loader fills it: %v", got.Notes)
	}

	unknown := a.CompanyOverview(path, "ZZZZ")
	if unknown.Error != "" || unknown.Price != nil {
		t.Errorf("an unknown ticker is notes, not an error: %+v", unknown)
	}
	if !strings.Contains(strings.Join(unknown.Notes, " "), "no price history") {
		t.Errorf("unknown ticker notes: %v", unknown.Notes)
	}
	if a.CompanyOverview("", "ACME").Error == "" {
		t.Error("no database chosen must say so")
	}
}

func TestCompanyFinancialsWithoutTheTable(t *testing.T) {
	got := (&App{}).CompanyFinancials(researchDB(t), "ACME", "weird")
	if got.Error != "" || got.Frequency != "annual" {
		t.Fatalf("%+v", got)
	}
	if len(got.Notes) == 0 || !strings.Contains(got.Notes[0], "add_sec_financials.py") {
		t.Errorf("missing table note: %v", got.Notes)
	}
}

func TestCompanyEarnings(t *testing.T) {
	got := (&App{}).CompanyEarnings(researchDB(t), "ACME")
	if got.Error != "" || len(got.Reports) != 1 {
		t.Fatalf("%+v", got)
	}
	r := got.Reports[0]
	if r.Reported != "2025-01-28 16:00" || r.Timing != "after close" || !r.HasMove ||
		r.From != "2025-01-28" || r.To != "2025-01-29" || r.Move < 0.0999 || r.Move > 0.1001 {
		t.Errorf("%+v", r)
	}
}

// The Research views are smoke-rendered against what these bindings really
// return -- written from Go here, on the scratch database plus a small
// sec_financials table, so the frontend can never be testing a shape the app
// does not send. Same -update flag as the other fixtures.
func TestResearchFixturesMatchTheBindings(t *testing.T) {
	path := researchDB(t)
	db, err := data.InitDB(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`CREATE TABLE sec_financials (cik VARCHAR, ticker VARCHAR, company_name VARCHAR,
			sic VARCHAR, metric VARCHAR, tag VARCHAR, period_end DATE, filed DATE,
			filing_period DATE, value DOUBLE, form VARCHAR, fy VARCHAR, fp VARCHAR,
			frequency VARCHAR, accession VARCHAR, tag_rank SMALLINT, qtrs SMALLINT)`,
		`INSERT INTO sec_financials VALUES
			('123', 'ACME', 'ACME CORP', '3571', 'Total Revenue', 'Revenues', '2024-12-31',
			 '2025-02-14', '2024-12-31', 4.2e11, '10-K', '2024', 'FY', 'annual', 'a1', 0, 4),
			('123', 'ACME', 'ACME CORP', '3571', 'Net Income', 'NetIncomeLoss', '2024-12-31',
			 '2025-02-14', '2024-12-31', -1.5e9, '10-K', '2024', 'FY', 'annual', 'a1', 0, 4),
			('123', 'ACME', 'ACME CORP', '3571', 'Diluted EPS', 'EPS', '2024-12-31',
			 '2025-02-14', '2024-12-31', 6.42, '10-K', '2024', 'FY', 'annual', 'a1', 0, 4),
			('123', 'ACME', 'ACME CORP', '3571', 'Total Revenue', 'Revenues', '2023-12-31',
			 '2024-02-14', '2023-12-31', 3.9e11, '10-K', '2023', 'FY', 'annual', 'a0', 0, 4)`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
	}
	a := &App{}
	fa := a.CompanyFinancials(path, "ACME", "annual")
	if len(fa.Periods) != 2 || fa.Rows[0].Cells[1] == nil {
		t.Fatalf("FA fixture should have two periods and a gap: %+v", fa)
	}
	for name, v := range map[string]any{
		"research-des": a.CompanyOverview(path, "ACME"),
		"research-fa":  fa,
		"research-ern": a.CompanyEarnings(path, "ACME"),
	} {
		want, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, '\n')
		file := filepath.Join(fixtureDir, name+".json")
		if *updateFixtures {
			if err := os.WriteFile(file, want, 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		got, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("%s: %v -- regenerate with `go test ./ui -run TestResearchFixtures -update`", file, err)
		}
		if string(got) != string(want) {
			t.Errorf("%s is stale: a research binding's output changed but the fixture the "+
				"views render against did not. Regenerate with "+
				"`go test ./ui -run TestResearchFixtures -update`.", file)
		}
	}
}
