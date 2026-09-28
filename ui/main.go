package main

import (
	"embed"
	"os"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
	"github.com/wailsapp/wails/v2/pkg/options/linux"
)

//go:embed all:frontend/dist
var assets embed.FS

func main() {
	// `ui --sync <db>` refreshes the price data and exits without opening a
	// window, for machines with no display and for the first sync of a newly
	// downloaded database — which is when it is furthest behind.
	if handled, code := runSyncCommand(os.Args[1:]); handled {
		os.Exit(code)
	}

	// Create an instance of the app structure
	app := NewApp()

	// Create application with options
	err := wails.Run(&options.App{
		Title:  "Backtester",
		Width:  1400,
		Height: 900,
		AssetServer: &assetserver.Options{
			Assets: assets,
		},
		BackgroundColour: &options.RGBA{R: 27, G: 38, B: 54, A: 1},
		// A non-nil Linux block replaces Wails' GPU-off fallback with our
		// own, which is also off unless BACKTESTER_GPU asks otherwise.
		Linux:     &linux.Options{WebviewGpuPolicy: gpuPolicy(os.Getenv)},
		OnStartup: app.startup,
		Bind: []interface{}{
			app,
		},
	})

	if err != nil {
		println("Error:", err.Error())
	}
}
