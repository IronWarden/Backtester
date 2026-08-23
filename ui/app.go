package main

import (
	"context"
	"fmt"
	"my-backtester/src/backtest"
	"my-backtester/src/data"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"

	wruntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

type App struct {
	ctx context.Context
}

func NewApp() *App {
	return &App{}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
}

// RunResult is the wire shape of a single portfolio's metrics, exposed to
// the frontend. Float fields are sent as JSON numbers.
type RunResult struct {
	PortfolioName     string  `json:"portfolioName"`
	Strategy          string  `json:"strategy"`
	SharpeRatio       float64 `json:"sharpeRatio"`
	SortinoRatio      float64 `json:"sortinoRatio"`
	MaxDrawdown       float64 `json:"maxDrawdown"`
	AnnualReturn      float64 `json:"annualReturn"`
	StandardDev       float64 `json:"standardDev"`
	AvgCorrelation    float64 `json:"avgCorrelation"`
	CointegratedPairs int     `json:"cointegratedPairs"`
	Turnover          float64 `json:"turnover"`
	AvgExposure       float64 `json:"avgExposure"`
	// FundamentalCoverage is the share of (day x ticker x declared metric)
	// cells that had a published figure. 0 when the config declares no
	// Fundamentals, so the column is blank for every run that does not use
	// them rather than showing a misleading zero.
	FundamentalCoverage   float64 `json:"fundamentalCoverage"`
	CalmarRatio           float64 `json:"calmarRatio"`
	LongestUnderwaterDays int     `json:"longestUnderwaterDays"`
	TimeInMarket          float64 `json:"timeInMarket"`
	Alpha                 float64 `json:"alpha"`
	Beta                  float64 `json:"beta"`
	TrackingError         float64 `json:"trackingError"`
	InformationRatio      float64 `json:"informationRatio"`
	UpCapture             float64 `json:"upCapture"`
	DownCapture           float64 `json:"downCapture"`
	// InitialCapital is the starting cash and FinalValue the ending worth, so
	// the frontend can show the profit in dollars alongside the percentages.
	InitialCapital float64 `json:"initialCapital"`
	FinalValue     float64 `json:"finalValue"`
	// EquityCurve is the portfolio's daily total value; Dates are the
	// matching trading days (YYYY-MM-DD), 1:1 with EquityCurve. The frontend
	// plots these as the equity-curve chart.
	EquityCurve []float64 `json:"equityCurve"`
	Dates       []string  `json:"dates"`
	// Time-sliced views of the same run: the deepest drawdowns with their
	// recovery, a rolling one-year Sharpe series, and compounded calendar
	// returns by year and by month.
	Drawdowns      []backtest.Drawdown     `json:"drawdowns"`
	RollingSharpe  []backtest.RollingPoint `json:"rollingSharpe"`
	YearlyReturns  []backtest.PeriodReturn `json:"yearlyReturns"`
	MonthlyReturns []backtest.PeriodReturn `json:"monthlyReturns"`
	// BenchmarkCurve is the configured Benchmark's value over the same days,
	// compounded from InitialCapital so it shares an axis and an origin with
	// EquityCurve, and 1:1 with Dates. BenchmarkStats is what the benchmark
	// did on its own terms; the alpha/beta/capture fields above describe the
	// portfolio against it. Both are empty when no Benchmark is configured.
	BenchmarkCurve []float64               `json:"benchmarkCurve"`
	BenchmarkStats backtest.BenchmarkStats `json:"benchmarkStats"`
	// Splits holds the in-sample and out-of-sample segments of this run when
	// [portfolio.Validation] is set, in-sample first, and is empty otherwise.
	Splits []backtest.SegmentStats `json:"splits"`
	// Trials is how many parameter sets the config block that produced this
	// result expanded to. ExpectedMaxSharpe is the Sharpe the best of them
	// would be expected to show with no edge at all, and DeflatedSharpe is
	// the probability in [0,1] that this result beats that bar.
	// WalkForward describes the rolling schedule that produced this result,
	// and is nil for every ordinary run.
	WalkForward       *backtest.WalkForwardResult `json:"walkForward"`
	Trials            int                         `json:"trials"`
	ExpectedMaxSharpe float64                     `json:"expectedMaxSharpe"`
	DeflatedSharpe    float64                     `json:"deflatedSharpe"`
	// Baseline is what an equal-weight buy-and-hold of the SAME tickers would
	// have returned over the same days with the same costs, and Significance
	// is the result against a random-timing null. Both are computed on every
	// run and were previously dropped on the way to the frontend, which left
	// the two questions that kill an idea — "was this better than doing
	// nothing" and "was this better than luck" — visible only on the CLI.
	Baseline     backtest.BaselineStats `json:"baseline"`
	Significance backtest.Significance  `json:"significance"`
	// TradeStats is the blotter's summary — win rate, profit factor, holding
	// periods — plus the per-ticker P&L attribution, which answers whether an
	// edge was broad or was one lucky position. Trades == 0 is itself the most
	// important signal it can carry.
	TradeStats backtest.TradeStats `json:"tradeStats"`
}

// RunOutput is what one run hands back: the per-portfolio results, plus
// whatever the engine had to tell the user while producing them. Diagnostics
// travel with the results rather than through a separate call so the console
// can never show the leftovers of a previous run.
type RunOutput struct {
	Results     []RunResult           `json:"results"`
	Diagnostics []backtest.Diagnostic `json:"diagnostics"`
	// Verdicts is one mechanical judgement per result, present only when the
	// run was given a pre-registered hypothesis. Empty otherwise — a run with
	// no registered claim has nothing to be judged against, and inventing a
	// default criterion would be the exact failure the feature prevents.
	Verdicts []backtest.VerdictReport `json:"verdicts"`
	// HypothesisError is a hypothesis that could not be registered, reported
	// separately so a bad criterion is a visible refusal rather than a run
	// that quietly went unjudged.
	HypothesisError string `json:"hypothesisError"`
}

// maxDiagnostics bounds what one run may report. A Lua script that raises on
// every one of five thousand trading days would otherwise send five thousand
// identical records across the bridge, which helps nobody — the first few say
// everything the user needs.
const maxDiagnostics = 200

// diagMu serialises runs. backtest.Diagnostics is a process-global sink, so
// two concurrent RunBacktest calls would collect each other's messages; the
// lock is held for the whole run rather than just the swap.
var diagMu sync.Mutex

// diagCollector gathers a run's diagnostics. The engine calls record from the
// worker pool, concurrently, and does not lock on its own behalf — so this
// does.
type diagCollector struct {
	mu        sync.Mutex
	collected []backtest.Diagnostic
	dropped   int
}

func (c *diagCollector) record(d backtest.Diagnostic) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.collected) >= maxDiagnostics {
		c.dropped++
		return
	}
	c.collected = append(c.collected, d)
}

// records returns what was collected, with a final note when the cap bit. The
// note matters more than the messages it replaces: a console that silently
// stops at two hundred lines is lying about what happened.
func (c *diagCollector) records() []backtest.Diagnostic {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]backtest.Diagnostic(nil), c.collected...)
	if c.dropped > 0 {
		out = append(out, backtest.Diagnostic{
			Level: "warn",
			Message: fmt.Sprintf(
				"%d further diagnostics were suppressed; this run reported "+
					"more than the %d the console shows",
				c.dropped, maxDiagnostics),
		})
	}
	return out
}

// CheckConfig validates the in-editor TOML without running it. It opens no
// database and simulates nothing, so it answers immediately even for a config
// that would take minutes to run.
//
// It returns a RunOutput with no results rather than a type of its own: the
// check produces exactly one kind of thing, diagnostics, and the console panel
// already knows how to render those. A second wire type would mean a second
// renderer for identical content.
func (a *App) CheckConfig(cfgText, defaultLuaPath string) (out RunOutput, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("config check panic: %v\n%s", r, debug.Stack())
		}
	}()
	cfg, err := backtest.LoadConfigText(cfgText)
	if err != nil {
		return RunOutput{}, fmt.Errorf("parse toml: %w", err)
	}

	diagMu.Lock()
	defer diagMu.Unlock()
	var collector diagCollector
	backtest.Diagnostics = collector.record
	defer func() { backtest.Diagnostics = nil }()

	backtest.CheckConfig(cfg, defaultLuaPath)
	return RunOutput{Diagnostics: collector.records()}, nil
}

// RunBacktest executes the in-editor TOML config against the chosen DB.
// defaultLuaPath is used as the strategy for any portfolio that omits its
// Strategy field, so the script open in the Lua tab becomes the default.
// Returns the per-portfolio results in run order, with the run's diagnostics.
// RunBacktest runs a config and, when hypothesisText is non-empty, judges every
// result against it. The hypothesis is parsed BEFORE the run, so a criterion
// that cannot be judged is refused while there are still no numbers to see.
func (a *App) RunBacktest(
	cfgText, dbPath, defaultLuaPath, hypothesisText string,
) (out RunOutput, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("backtest panic: %v\n%s", r, debug.Stack())
		}
	}()
	if dbPath == "" {
		return RunOutput{}, fmt.Errorf("db path is empty")
	}

	diagMu.Lock()
	defer diagMu.Unlock()
	var collector diagCollector
	backtest.Diagnostics = collector.record
	defer func() { backtest.Diagnostics = nil }()

	// Registered first: parsing after the run would mean a criterion accepted
	// or rejected with the results already in hand.
	var hyp backtest.Hypothesis
	var hypErr string
	if strings.TrimSpace(hypothesisText) != "" {
		h, perr := backtest.ParseHypothesis([]byte(hypothesisText))
		if perr != nil {
			hypErr = perr.Error()
		} else {
			hyp = h
		}
	}

	raw, err := backtest.RunFromConfigText(cfgText, dbPath, defaultLuaPath)
	if err != nil {
		return RunOutput{}, err
	}
	results := make([]RunResult, 0, len(raw))
	for _, r := range raw {
		results = append(results, RunResult{
			PortfolioName:         r.PortfolioName,
			Strategy:              r.Strategy,
			SharpeRatio:           r.Metrics.SharpeRatio,
			SortinoRatio:          r.Metrics.SortinoRatio,
			MaxDrawdown:           r.Metrics.MaxDrawdown,
			AnnualReturn:          r.Metrics.AnnualReturn,
			StandardDev:           r.Metrics.StandardDev,
			AvgCorrelation:        r.Metrics.AvgCorrelation,
			CointegratedPairs:     r.Metrics.CointegratedPairs,
			Turnover:              r.Metrics.Turnover,
			AvgExposure:           r.Metrics.AvgExposure,
			FundamentalCoverage:   r.Metrics.FundamentalCoverage,
			CalmarRatio:           r.Metrics.CalmarRatio,
			LongestUnderwaterDays: r.Metrics.LongestUnderwaterDays,
			TimeInMarket:          r.Metrics.TimeInMarket,
			Alpha:                 r.Metrics.Alpha,
			Beta:                  r.Metrics.Beta,
			TrackingError:         r.Metrics.TrackingError,
			InformationRatio:      r.Metrics.InformationRatio,
			UpCapture:             r.Metrics.UpCapture,
			DownCapture:           r.Metrics.DownCapture,
			InitialCapital:        r.InitialCapital,
			FinalValue:            r.FinalValue,
			EquityCurve:           r.EquityCurve,
			Dates:                 r.Dates,
			Drawdowns:             r.Drawdowns,
			RollingSharpe:         r.RollingSharpe,
			YearlyReturns:         r.YearlyReturns,
			MonthlyReturns:        r.MonthlyReturns,
			BenchmarkCurve:        r.BenchmarkCurve,
			BenchmarkStats:        r.BenchmarkStats,
			Splits:                r.Splits,
			WalkForward:           r.WalkForward,
			Trials:                r.Trials,
			ExpectedMaxSharpe:     r.ExpectedMaxSharpe,
			DeflatedSharpe:        r.DeflatedSharpe,
			Baseline:              r.Baseline,
			Significance:          r.Significance,
			TradeStats:            r.TradeStats,
		})
	}
	out = RunOutput{
		Results:         results,
		Diagnostics:     collector.records(),
		HypothesisError: hypErr,
	}
	if hyp.ID != "" {
		for _, r := range raw {
			// The campaign trial count lives in the research log, which the
			// desktop app does not open; this run's own trial group is the
			// conservative fallback (fewer trials is a lower bar, so it can
			// never make a verdict easier to fake).
			out.Verdicts = append(out.Verdicts, backtest.Judge(hyp, r, r.Trials))
		}
	}
	return out, nil
}

// loadStrategyGallery parses the shipped Lua library's header cards from
// dir. A missing directory is an EMPTY gallery, not an error: a packaged
// install may ship without the library, and a UI that fails to start
// because it cannot list strategies is worse than one that lists none.
// Split from the bound method so the resolution and the parsing can be
// tested without a repo layout around them.
func loadStrategyGallery(dir string) []backtest.StrategyCard {
	if _, err := os.Stat(dir); err != nil {
		return []backtest.StrategyCard{}
	}
	cards, err := backtest.LoadGallery(dir)
	if err != nil || cards == nil {
		return []backtest.StrategyCard{}
	}
	return cards
}

// ListStrategies returns the shipped Lua library as gallery cards — what
// each script is for, and the conditions it works and fails under. The CLI
// has had this as -list-strategies since the gallery landed; binding it
// here is what lets the UI name a strategy in the user's language instead
// of making them read Lua.
//
// NOTE: this reads the library at repoRoot(), which is NOT the directory
// the engine opens a "lua:strategies/..." spec from — that resolution is
// relative to the process working directory. See T86 in TASKS.md; a picker
// built on these cards would emit specs the engine cannot load.
func (a *App) ListStrategies() []backtest.StrategyCard {
	return loadStrategyGallery(filepath.Join(repoRoot(), "strategies"))
}

// CriterionMetrics returns the metric names a pre-registered hypothesis may
// test, straight from the engine's own table. Bound rather than mirrored in
// the frontend on purpose: a hand-copied list is how the UI ends up offering
// a criterion ParseCriterion rejects, or hiding one it accepts.
func (a *App) CriterionMetrics() []string {
	return backtest.CriterionMetricNames()
}

// PickConfigFile opens a native file picker for TOML configs.
func (a *App) PickConfigFile() (string, error) {
	return wruntime.OpenFileDialog(a.ctx, wruntime.OpenDialogOptions{
		Title: "Choose a portfolio config",
		Filters: []wruntime.FileFilter{
			{DisplayName: "TOML config", Pattern: "*.toml"},
		},
	})
}

// PickDBFile opens a native file picker for the DuckDB database.
func (a *App) PickDBFile() (string, error) {
	return wruntime.OpenFileDialog(a.ctx, wruntime.OpenDialogOptions{
		Title: "Choose the DuckDB database",
		Filters: []wruntime.FileFilter{
			{DisplayName: "DuckDB", Pattern: "*.db"},
			{DisplayName: "Any file", Pattern: "*"},
		},
	})
}

// ListTickers opens the DuckDB at dbPath and returns the distinct ticker
// symbols available for backtesting, sorted alphabetically. Powers the
// simple-mode ticker picker so users choose from real symbols.
func (a *App) ListTickers(dbPath string) ([]string, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("db path is empty")
	}
	if _, err := data.InitDB(dbPath); err != nil {
		return nil, fmt.Errorf("open db %q: %w", dbPath, err)
	}
	return data.ListTickers()
}

// benchmarkNames labels the $-prefixed synthetic series. They are built by
// add_collections.py rather than filed with the SEC, so no company_profile row
// will ever exist for them -- without this the picker would show the ten most
// useful symbols in the database as bare tickers. Names match COLLECTIONS in
// the loader.
var benchmarkNames = map[string]string{
	"$SP500": "S&P 500",
	"$USTOT": "Total U.S. Market",
	"$SP100": "S&P 100",
	"$SP400": "S&P 400 MidCap",
	"$SP600": "S&P 600 SmallCap",
	"$RUS2K": "Russell 2000",
	"$EXUS":  "Total ex-US Market",
	"$EM":    "Emerging Markets",
	"$WORLD": "Total World Market",
	"$CASH":  "Cash (T-bill)",
}

// TickerNames returns {"SYMBOL": "Company Name"} for every symbol we can name,
// so the picker can show what a ticker actually is instead of asking the user
// to already know.
//
// Two sources, neither of them required: company_profile (SEC EDGAR, loaded by
// add_company_info.py) and the benchmark labels above. company_profile is
// optional -- LoadCompanyProfiles returns nil when the table is absent -- so a
// database without it yields an empty map and the picker falls back to bare
// symbols rather than failing.
//
// Coverage is partial by nature: company_tickers.json lists only CURRENT
// filers, so delisted symbols have no row, and neither do the generation
// suffixes split_recycled.py assigns (COR.1). That last one is deliberate --
// a generation suffix is the symbol's PREVIOUS occupant, a different company,
// and labelling it with the current filer's name would be a lie rather than a
// gap.
func (a *App) TickerNames(dbPath string) (map[string]string, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("db path is empty")
	}
	if _, err := data.InitDB(dbPath); err != nil {
		return nil, fmt.Errorf("open db %q: %w", dbPath, err)
	}
	profiles, err := data.LoadCompanyProfiles(nil)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(profiles)+len(benchmarkNames))
	for sym, p := range profiles {
		if p.Name != "" {
			out[sym] = p.Name
		}
	}
	for sym, name := range benchmarkNames {
		out[sym] = name
	}
	return out, nil
}

// TickerDateRanges returns, for every ticker in the DB, the earliest and
// latest date it has data for, as {"SYMBOL": ["YYYY-MM-DD", "YYYY-MM-DD"]}.
// The simple-mode form uses these to constrain the date pickers to the span
// the selected tickers actually cover, so a user can't request a window with
// no data.
func (a *App) TickerDateRanges(dbPath string) (map[string][]string, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("db path is empty")
	}
	if _, err := data.InitDB(dbPath); err != nil {
		return nil, fmt.Errorf("open db %q: %w", dbPath, err)
	}
	ranges, err := data.TickerDateRanges(nil)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]string, len(ranges))
	for sym, r := range ranges {
		out[sym] = []string{
			r.Min.Format("2006-01-02"),
			r.Max.Format("2006-01-02"),
		}
	}
	return out, nil
}

// PickLuaFile opens a native file picker for Lua strategy scripts.
func (a *App) PickLuaFile() (string, error) {
	return wruntime.OpenFileDialog(a.ctx, wruntime.OpenDialogOptions{
		Title: "Choose a Lua strategy script",
		Filters: []wruntime.FileFilter{
			{DisplayName: "Lua script", Pattern: "*.lua"},
		},
	})
}

// ReadFile returns the file's contents as a string. Used by the frontend
// to load the TOML/Lua source into Monaco.
func (a *App) ReadFile(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("path is empty")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// WriteFile overwrites path with content, creating both the file and any
// missing parent directories. Lets users save a Lua script for a path the
// TOML references but doesn't yet exist on disk.
func (a *App) WriteFile(path, content string) error {
	if path == "" {
		return fmt.Errorf("path is empty")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("mkdir %q: %w", dir, err)
		}
	}
	return os.WriteFile(path, []byte(content), 0644)
}

// FileExists reports whether path resolves to a regular file. Used by the
// UI to distinguish "missing file we'll create" from real read errors.
func (a *App) FileExists(path string) bool {
	if path == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

var luaStrategyRe = regexp.MustCompile(`Strategy\s*=\s*"lua:([^"]+)"`)

// FindLuaPathInConfig parses cfgText for a Strategy="lua:<path>" line and
// returns that path resolved relative to cfgPath's directory (or absolute
// if it already is). Returns "" if none is found.
func (a *App) FindLuaPathInConfig(cfgText, cfgPath string) string {
	m := luaStrategyRe.FindStringSubmatch(cfgText)
	if len(m) < 2 {
		return ""
	}
	p := m[1]
	if filepath.IsAbs(p) || cfgPath == "" {
		return p
	}
	return filepath.Join(filepath.Dir(cfgPath), p)
}
