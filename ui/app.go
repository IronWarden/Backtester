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
	// EquityCurve is the portfolio's daily total value; Dates are the
	// matching trading days (YYYY-MM-DD), 1:1 with EquityCurve. The frontend
	// plots these as the equity-curve chart.
	EquityCurve []float64 `json:"equityCurve"`
	Dates       []string  `json:"dates"`
}

// RunBacktest executes the in-editor TOML config against the chosen DB.
// defaultLuaPath is used as the strategy for any portfolio that omits its
// Strategy field, so the script open in the Lua tab becomes the default.
// Returns the per-portfolio results in run order.
func (a *App) RunBacktest(cfgText, dbPath, defaultLuaPath string) (results []RunResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("backtest panic: %v\n%s", r, debug.Stack())
		}
	}()
	if dbPath == "" {
		return nil, fmt.Errorf("db path is empty")
	}
	raw, err := backtest.RunFromConfigText(cfgText, dbPath, defaultLuaPath)
	if err != nil {
		return nil, err
	}
	results = make([]RunResult, 0, len(raw))
	for _, r := range raw {
		results = append(results, RunResult{
			PortfolioName:     r.PortfolioName,
			Strategy:          r.Strategy,
			SharpeRatio:       r.Metrics.SharpeRatio,
			SortinoRatio:      r.Metrics.SortinoRatio,
			MaxDrawdown:       r.Metrics.MaxDrawdown,
			AnnualReturn:      r.Metrics.AnnualReturn,
			StandardDev:       r.Metrics.StandardDev,
			AvgCorrelation:    r.Metrics.AvgCorrelation,
			CointegratedPairs: r.Metrics.CointegratedPairs,
			EquityCurve:       r.EquityCurve,
			Dates:             r.Dates,
		})
	}
	return results, nil
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
