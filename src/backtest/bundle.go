package backtest

// A run you can hand to someone else — including future you.
//
// A result lives in a table and cannot be checked by anyone who was not sitting
// at this machine on the day it ran. Sharing a Sharpe is an assertion; sharing a
// bundle is evidence, because a bundle carries everything needed to produce the
// number again: the expanded config, the strategy's SOURCE, the engine's commit,
// what the data looked like, and the metrics to compare against.
//
// The re-run is the part that earns its place. Verifying a six-month-old bundle
// and getting different numbers means either the engine drifted or the data
// changed, and both are things you want to hear about — from a different
// direction than the correctness suite, which can only catch what it was written
// to expect.
//
// The strategy source is embedded rather than referenced on purpose. A path is
// not a strategy: `lua:strategies/rsi.lua` names a file that anyone can edit,
// and a bundle whose numbers no longer reproduce because the script changed
// underneath it should say exactly that rather than reporting a mysterious
// drift.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"time"
)

// BundleVersion is the format version. Bumped when a field changes meaning, so
// an old bundle is refused rather than silently misread.
const BundleVersion = 1

// BundleRun is one portfolio's config and its result, together.
type BundleRun struct {
	Name       string         `json:"name"`
	Strategy   string         `json:"strategy"`
	ConfigHash string         `json:"configHash"`
	Params     map[string]any `json:"params,omitempty"`
	Tickers    []string       `json:"tickers"`
	Start      string         `json:"start"`
	End        string         `json:"end"`
	Capital    float64        `json:"capital"`
	Costs      CostConfig     `json:"costs"`
	Benchmark  string         `json:"benchmark,omitempty"`
	// StrategySource is the Lua script's full text for a lua: spec, and empty
	// for a built-in. SourceSHA is its digest, which is what a verification
	// compares — a changed script is a different experiment, not a drift.
	StrategySource string `json:"strategySource,omitempty"`
	SourceSHA      string `json:"sourceSha,omitempty"`

	// The figures a verification checks.
	Metrics    Metrics `json:"metrics"`
	FinalValue float64 `json:"finalValue"`
	Trades     int     `json:"trades"`
	Days       int     `json:"days"`
}

// Bundle is a whole run, portable.
type Bundle struct {
	Version   int       `json:"version"`
	CreatedAt time.Time `json:"createdAt"`
	// EngineSHA is the commit the engine was built from, with "-dirty" when the
	// tree had uncommitted changes: a bundle produced from a modified tree
	// cannot be reproduced from its sha alone, and should say so.
	EngineSHA string `json:"engineSha"`
	// DataNote records what the market data looked like — the price table's
	// last bar and the tickers' own ranges — so a verification that fails can
	// distinguish "the engine changed" from "the data was refreshed".
	DataNote string      `json:"dataNote,omitempty"`
	Runs     []BundleRun `json:"runs"`
}

// Discrepancy is one figure that no longer reproduces.
type Discrepancy struct {
	Run      string  `json:"run"`
	Field    string  `json:"field"`
	Recorded float64 `json:"recorded"`
	Fresh    float64 `json:"fresh"`
}

func (d Discrepancy) String() string {
	return fmt.Sprintf("%s: %s was %.10g, now %.10g (%+.4g%%)",
		d.Run, d.Field, d.Recorded, d.Fresh,
		relativeChange(d.Recorded, d.Fresh)*100)
}

func relativeChange(was, now float64) float64 {
	if was == 0 {
		if now == 0 {
			return 0
		}
		return math.Inf(1)
	}
	return (now - was) / math.Abs(was)
}

// verifyTolerance is how far a re-run may differ and still count as reproduced.
// The engine is deterministic over identical data, so this is a float-noise
// allowance rather than a modelling one — anything larger is a real change and
// should be reported as one.
const verifyTolerance = 1e-9

// NewBundle captures a finished run. Results are matched to portfolios by name.
func NewBundle(portfolios []*Portfolio, results []Result) Bundle {
	b := Bundle{
		Version:   BundleVersion,
		CreatedAt: time.Now().UTC(),
		EngineSHA: engineSHA(),
	}

	byName := make(map[string]*Portfolio, len(portfolios))
	for _, p := range portfolios {
		byName[p.Pname] = p
	}

	for _, res := range results {
		run := BundleRun{
			Name:       res.PortfolioName,
			Strategy:   res.Strategy,
			Metrics:    res.Metrics,
			FinalValue: res.FinalValue,
			Trades:     res.TradeStats.Trades,
			Days:       len(res.EquityCurve),
		}
		if p := byName[res.PortfolioName]; p != nil {
			run.ConfigHash = ConfigHash(p)
			run.Params = p.StrategyParams
			run.Tickers = p.Tickers
			run.Start = p.StartTime.Format("2006-01-02")
			run.End = p.EndTime.Format("2006-01-02")
			run.Capital = p.InitialBuyingPower
			run.Costs = p.Costs
			run.Benchmark = p.Benchmark
			run.StrategySource, run.SourceSHA = strategySource(p.StrategySpec)
		}
		b.Runs = append(b.Runs, run)
	}
	return b
}

// strategySource reads a lua: spec's script so the bundle is self-contained. A
// built-in strategy has no source to embed and reports empty, which is not a
// failure.
func strategySource(spec string) (source, sha string) {
	const prefix = "lua:"
	if !strings.HasPrefix(spec, prefix) {
		return "", ""
	}
	path, err := resolveLuaPath(strings.TrimPrefix(spec, prefix))
	if err != nil {
		return "", ""
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", ""
	}
	sum := sha256.Sum256(raw)
	return string(raw), hex.EncodeToString(sum[:12])
}

// engineSHA asks git what the engine was built from. "unknown" when git is not
// available, which is honest: a bundle from an unidentifiable build is still
// worth having, it just cannot claim reproducibility from a commit.
func engineSHA() string {
	run := func(args ...string) (string, error) {
		cmd := exec.Command("git", args...)
		out, err := cmd.Output()
		return strings.TrimSpace(string(out)), err
	}
	sha, err := run("rev-parse", "--short", "HEAD")
	if err != nil || sha == "" {
		return "unknown"
	}
	if dirty, err := run("status", "--porcelain"); err == nil && dirty != "" {
		return sha + "-dirty"
	}
	return sha
}

// Save writes the bundle as indented JSON — readable on purpose, since the point
// is that someone else can inspect it.
func (b Bundle) Save(path string) error {
	raw, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

// LoadBundle reads a bundle, refusing a format version it does not understand
// rather than misreading it.
func LoadBundle(path string) (Bundle, error) {
	var b Bundle
	raw, err := os.ReadFile(path)
	if err != nil {
		return b, err
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		return b, fmt.Errorf("parsing %s: %w", path, err)
	}
	if b.Version != BundleVersion {
		return b, fmt.Errorf(
			"%s is a version %d bundle and this engine writes version %d",
			path, b.Version, BundleVersion)
	}
	return b, nil
}

// CompareRun checks a fresh result against what the bundle recorded. Pure, so
// the comparison is testable without re-running anything.
//
// The source check comes first and is reported as its own discrepancy: if the
// script changed, every number below it is expected to differ and reporting them
// individually would bury the reason.
func CompareRun(recorded BundleRun, fresh Result, freshSourceSHA string) []Discrepancy {
	var out []Discrepancy

	if recorded.SourceSHA != "" && freshSourceSHA != "" &&
		recorded.SourceSHA != freshSourceSHA {
		out = append(out, Discrepancy{
			Run: recorded.Name, Field: "strategy source changed",
		})
	}

	for _, f := range []struct {
		name string
		was  float64
		now  float64
	}{
		{"final value", recorded.FinalValue, fresh.FinalValue},
		{"annual return", recorded.Metrics.AnnualReturn, fresh.Metrics.AnnualReturn},
		{"sharpe", recorded.Metrics.SharpeRatio, fresh.Metrics.SharpeRatio},
		{"sortino", recorded.Metrics.SortinoRatio, fresh.Metrics.SortinoRatio},
		{"max drawdown", recorded.Metrics.MaxDrawdown, fresh.Metrics.MaxDrawdown},
		{"stdev", recorded.Metrics.StandardDev, fresh.Metrics.StandardDev},
		{"turnover", recorded.Metrics.Turnover, fresh.Metrics.Turnover},
		{"trades", float64(recorded.Trades), float64(fresh.TradeStats.Trades)},
		{"days", float64(recorded.Days), float64(len(fresh.EquityCurve))},
	} {
		if !closeEnough(f.was, f.now) {
			out = append(out, Discrepancy{
				Run: recorded.Name, Field: f.name,
				Recorded: f.was, Fresh: f.now,
			})
		}
	}
	return out
}

// closeEnough is equality within float noise. Deliberately tight: the engine is
// deterministic over identical data, so a difference beyond rounding is a real
// change and hiding it behind a generous tolerance would defeat the whole check.
func closeEnough(was, now float64) bool {
	if math.IsNaN(was) || math.IsNaN(now) {
		return math.IsNaN(was) && math.IsNaN(now)
	}
	return math.Abs(was-now) <= verifyTolerance*math.Max(1, math.Abs(was))
}

// VerifyReport is the outcome of re-running a bundle.
type VerifyReport struct {
	Path          string        `json:"path"`
	EngineSHA     string        `json:"engineSha"`
	CurrentSHA    string        `json:"currentSha"`
	Discrepancies []Discrepancy `json:"discrepancies"`
	Missing       []string      `json:"missing"`
	Reproduced    bool          `json:"reproduced"`
}

func (v VerifyReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "verifying %s\n", v.Path)
	fmt.Fprintf(&b, "  recorded on engine %s, re-run on %s\n",
		v.EngineSHA, v.CurrentSHA)
	for _, m := range v.Missing {
		fmt.Fprintf(&b, "  MISSING  %s could not be re-run\n", m)
	}
	for _, d := range v.Discrepancies {
		fmt.Fprintf(&b, "  DRIFT    %s\n", d)
	}
	if v.Reproduced {
		b.WriteString("  reproduced: every figure matches\n")
	} else {
		b.WriteString("  NOT reproduced — either the engine changed, the " +
			"strategy changed, or the data did\n")
	}
	return b.String()
}

// BundleToPortfolios rebuilds the portfolios a bundle describes, ready to run.
func (b Bundle) BundleToPortfolios() ([]*Portfolio, error) {
	const layout = "2006-01-02"
	out := make([]*Portfolio, 0, len(b.Runs))
	for _, run := range b.Runs {
		start, err := time.Parse(layout, run.Start)
		if err != nil {
			return nil, fmt.Errorf("%s: bad start date %q: %w",
				run.Name, run.Start, err)
		}
		end, err := time.Parse(layout, run.End)
		if err != nil {
			return nil, fmt.Errorf("%s: bad end date %q: %w",
				run.Name, run.End, err)
		}
		p, err := InitializePortfolio(run.Capital, start, end, run.Name,
			run.Tickers, run.Strategy, run.Params)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", run.Name, err)
		}
		p.Costs = run.Costs
		p.Benchmark = run.Benchmark
		out = append(out, p)
	}
	return out, nil
}

// Verify compares fresh results against the bundle. The caller supplies the
// results, so this stays free of the database and the runner.
func (b Bundle) Verify(path string, fresh []Result) VerifyReport {
	report := VerifyReport{
		Path: path, EngineSHA: b.EngineSHA, CurrentSHA: engineSHA(),
	}
	byName := make(map[string]Result, len(fresh))
	for _, r := range fresh {
		byName[r.PortfolioName] = r
	}

	for _, run := range b.Runs {
		got, ok := byName[run.Name]
		if !ok {
			report.Missing = append(report.Missing, run.Name)
			continue
		}
		_, sha := strategySource(run.Strategy)
		report.Discrepancies = append(report.Discrepancies,
			CompareRun(run, got, sha)...)
	}
	report.Reproduced = len(report.Discrepancies) == 0 && len(report.Missing) == 0
	return report
}
