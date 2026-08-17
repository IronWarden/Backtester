package backtest

// The experiment registry: remember every run.
//
// Nothing persisted. `output.txt` is truncated on each run, so the app had no
// memory: you could not compare today's idea with last week's, could not tell
// whether you had already tested a hypothesis, and — the one that matters most —
// could not count how many things you had tried. That last number is the
// denominator every overfitting correction needs. A search that reports its best
// result without saying how many it looked at is not reporting a result.
//
// Three properties, each load-bearing:
//
//   - **A separate database file.** `stock_data.db` is 2.9 GB and single-writer,
//     and the UI holds it open; a research log that cannot be written while the
//     market data is open is a research log nobody writes to.
//   - **Append-only.** Never update, never delete. The point is that you cannot
//     quietly retry until something works: a re-run is a new row with the same
//     config hash, and that repetition is itself the signal.
//   - **A failed write never fails a backtest.** Recording is bookkeeping;
//     losing a run because the bookkeeping failed would be the worst possible
//     trade.
//
// Off by default, so existing CLI usage is untouched: -record turns it on.

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"gonum.org/v1/gonum/stat"
)

// DefaultRegistryPath is where the research log lives, beside the market data
// but deliberately not inside it.
const DefaultRegistryPath = "../research.db"

// Registry is an open handle on the research log.
type Registry struct {
	db *sql.DB
}

// ConfigHash identifies a fully-expanded portfolio configuration: the same
// hash means the same experiment.
//
// Pure and stable across processes — it hashes the config's own text, not a Go
// pointer or a map iteration order, so two runs a week apart agree.
//
// Ticker ORDER is part of the hash, deliberately. It looks like a set but is
// not: `buyAndHold:greedy` spends everything on the first ticker, so reordering
// the list is a different experiment.
func ConfigHash(p *Portfolio) string {
	var b strings.Builder
	fmt.Fprintf(&b, "strategy=%s\n", p.StrategySpec)
	fmt.Fprintf(&b, "tickers=%s\n", strings.Join(p.Tickers, ","))
	fmt.Fprintf(&b, "start=%s\nend=%s\n",
		p.StartTime.Format("2006-01-02"), p.EndTime.Format("2006-01-02"))
	fmt.Fprintf(&b, "capital=%.6f\n", p.InitialBuyingPower)
	fmt.Fprintf(&b, "benchmark=%s\n", p.Benchmark)
	fmt.Fprintf(&b, "costs=%+v\n", p.Costs)

	// Params is a map, so its keys are sorted: an unsorted walk would give the
	// same config two different hashes on different runs.
	keys := make([]string, 0, len(p.StrategyParams))
	for k := range p.StrategyParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "param.%s=%v\n", k, p.StrategyParams[k])
	}

	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:12])
}

// OpenRegistry opens (or creates) the research log and ensures its schema.
func OpenRegistry(path string) (*Registry, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("opening research log %s: %w", path, err)
	}
	if _, err := db.Exec(registrySchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating research log schema: %w", err)
	}
	return &Registry{db: db}, nil
}

const registrySchema = `
CREATE TABLE IF NOT EXISTS runs (
    run_id          VARCHAR NOT NULL,
    recorded_at     TIMESTAMP NOT NULL,
    campaign        VARCHAR,
    config_hash     VARCHAR NOT NULL,
    portfolio_name  VARCHAR,
    strategy        VARCHAR,
    tickers         VARCHAR,
    start_date      DATE,
    end_date        DATE,
    initial_capital DOUBLE,
    final_value     DOUBLE,
    annual_return   DOUBLE,
    sharpe          DOUBLE,
    sortino         DOUBLE,
    max_drawdown    DOUBLE,
    stdev           DOUBLE,
    turnover        DOUBLE,
    trials          INTEGER,
    trial_group     VARCHAR,
    deflated_sharpe DOUBLE,
    baseline_excess DOUBLE,
    beat_baseline   BOOLEAN,
    p_value         DOUBLE,
    trades          INTEGER,
    win_rate        DOUBLE,
    params          VARCHAR,
    costs           VARCHAR,
    benchmark       VARCHAR,
    splits          VARCHAR,
    walk_forward    VARCHAR,
    -- The campaign's cumulative trial count at the moment this row was
    -- written, and the Sharpe deflated against it. Stored rather than derived
    -- because the log is append-only: a row records what was known then.
    campaign_trials INTEGER,
    campaign_deflated_sharpe DOUBLE
)`

// Record appends one row per result. Never updates and never deletes: a re-run
// of the same config is a new row sharing its hash, and that repetition is the
// signal the whole registry exists to preserve.
//
// The caller supplies the portfolios so the config hash and parameters can be
// recorded alongside the metrics; results and portfolios are matched by name.
func (r *Registry) Record(
	campaign string, portfolios []*Portfolio, results []Result,
) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("registry is not open")
	}

	byName := make(map[string]*Portfolio, len(portfolios))
	for _, p := range portfolios {
		byName[p.Pname] = p
	}

	// The campaign's history BEFORE this batch. Deflating against the whole
	// campaign rather than one sweep is the difference between a research tool
	// and a slot machine: an agent that tries 40 hypotheses of 50 parameter
	// sets each has run 2,000 trials, and correcting the winner for 50 of them
	// overstates it enormously.
	priorSharpes, _ := r.CampaignSharpes(campaign)
	batch := make([]float64, 0, len(results))
	for _, res := range results {
		batch = append(batch, res.Metrics.SharpeRatio)
	}
	campaignTrials := len(priorSharpes) + len(results)
	campaignBar := ExpectedMaxSharpe(campaignTrials,
		stat.StdDev(append(append([]float64{}, priorSharpes...), batch...), nil))

	now := time.Now().UTC()
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i, res := range results {
		p := byName[res.PortfolioName]
		var hash, tickers, params, costs, benchmark string
		var start, end any
		if p != nil {
			hash = ConfigHash(p)
			tickers = strings.Join(p.Tickers, ",")
			params = asJSON(p.StrategyParams)
			costs = asJSON(p.Costs)
			benchmark = p.Benchmark
			start = p.StartTime.Format("2006-01-02")
			end = p.EndTime.Format("2006-01-02")
		}

		runID := fmt.Sprintf("%s-%d-%d", now.Format("20060102T150405"), now.UnixNano()%1e6, i)
		_, err := tx.Exec(`INSERT INTO runs VALUES (
		    ?, ?, ?, ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?,
		    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			runID, now, campaign, hash, res.PortfolioName, res.Strategy,
			tickers, start, end, res.InitialCapital, res.FinalValue,
			res.Metrics.AnnualReturn, res.Metrics.SharpeRatio,
			res.Metrics.SortinoRatio, res.Metrics.MaxDrawdown,
			res.Metrics.StandardDev, res.Metrics.Turnover,
			res.Trials, res.TrialGroup, res.DeflatedSharpe,
			res.Baseline.ExcessFinalValue, res.Baseline.Beat,
			res.Significance.PValue, res.TradeStats.Trades,
			res.TradeStats.WinRate, params, costs, benchmark,
			asJSON(res.Splits), asJSON(res.WalkForward),
			campaignTrials, deflatedFromResult(&results[i], campaignBar),
		)
		if err != nil {
			return fmt.Errorf("recording %q: %w", res.PortfolioName, err)
		}
	}
	return tx.Commit()
}

// PriorRuns counts how many times a config hash has been recorded before, and
// when it was last seen. This is the "have I tested this already?" question, and
// a non-zero count on a fresh idea is worth knowing before spending an evening
// on it.
func (r *Registry) PriorRuns(hash string) (count int, last time.Time, err error) {
	if r == nil || r.db == nil {
		return 0, time.Time{}, fmt.Errorf("registry is not open")
	}
	var lastSeen sql.NullTime
	err = r.db.QueryRow(
		`SELECT COUNT(*), MAX(recorded_at) FROM runs WHERE config_hash = ?`,
		hash).Scan(&count, &lastSeen)
	if err != nil {
		return 0, time.Time{}, err
	}
	return count, lastSeen.Time, nil
}

// CampaignTrials is the total number of runs recorded under a campaign — the
// denominator T23 needs. A campaign that has spent two thousand trials cannot
// have its best result judged as though it were one.
func (r *Registry) CampaignTrials(campaign string) (int, error) {
	if r == nil || r.db == nil {
		return 0, fmt.Errorf("registry is not open")
	}
	var n int
	err := r.db.QueryRow(
		`SELECT COUNT(*) FROM runs WHERE campaign = ?`, campaign).Scan(&n)
	return n, err
}

// CampaignSharpes returns every Sharpe already recorded under a campaign — the
// spread a campaign-level correction needs. An unknown campaign yields nothing,
// which is the right answer for the first run of a new one.
func (r *Registry) CampaignSharpes(campaign string) ([]float64, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("registry is not open")
	}
	rows, err := r.db.Query(
		`SELECT sharpe FROM runs WHERE campaign = ? AND sharpe IS NOT NULL`,
		campaign)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []float64
	for rows.Next() {
		var s sql.NullFloat64
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		if s.Valid {
			out = append(out, s.Float64)
		}
	}
	return out, rows.Err()
}

// CampaignDeflation reports the campaign's cumulative trial count and the
// resulting bar a result must clear — the number to quote beside a headline
// Sharpe once a campaign has been running for a while.
func (r *Registry) CampaignDeflation(campaign string) (trials int, bar float64, err error) {
	sharpes, err := r.CampaignSharpes(campaign)
	if err != nil {
		return 0, 0, err
	}
	return len(sharpes), ExpectedMaxSharpe(len(sharpes), stat.StdDev(sharpes, nil)), nil
}

// RecentRuns returns the last n runs as printable lines, newest first — the
// cheapest possible "what have I tried lately".
func (r *Registry) RecentRuns(n int) ([]string, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("registry is not open")
	}
	rows, err := r.db.Query(`
	    SELECT recorded_at, campaign, portfolio_name, strategy, sharpe,
	           annual_return, beat_baseline, config_hash
	    FROM runs ORDER BY recorded_at DESC LIMIT ?`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var at time.Time
		var campaign, name, strategy, hash sql.NullString
		var sharpe, annual sql.NullFloat64
		var beat sql.NullBool
		if err := rows.Scan(&at, &campaign, &name, &strategy, &sharpe,
			&annual, &beat, &hash); err != nil {
			return nil, err
		}
		verdict := "lost to baseline"
		if beat.Bool {
			verdict = "beat baseline"
		}
		// annual_return is stored exactly as Metrics.AnnualReturn holds it,
		// which GetAnnualReturn already expresses in PERCENT. Multiplying
		// again here printed a 50% year as 5041%.
		out = append(out, fmt.Sprintf(
			"%s  %-18s %-28s Sharpe %6.2f  CAGR %6.2f%%  %s  [%s]",
			at.Format("2006-01-02 15:04"), campaign.String, name.String,
			sharpe.Float64, annual.Float64, verdict, hash.String))
	}
	return out, rows.Err()
}

// Close releases the handle.
func (r *Registry) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

// asJSON marshals a value for storage, returning "" rather than failing: a
// field that cannot be serialised must not cost the whole row.
func asJSON(v any) string {
	if v == nil {
		return ""
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(raw)
}
