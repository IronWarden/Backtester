package main

import (
	"strings"

	"github.com/wailsapp/wails/v2/pkg/options/linux"
)

// gpuEnvVar selects the webview's hardware-acceleration policy on Linux.
//
// Wails turns acceleration OFF whenever options.Linux is nil (wails#2977:
// blank or crashing windows on some NVIDIA/Wayland stacks), so by default
// every scroll, hover and chart redraw is painted on the CPU. That default is
// kept, because the failure it avoids is a window that does not draw at all;
// this lets a machine where acceleration works opt in without a rebuild.
const gpuEnvVar = "BACKTESTER_GPU"

// gpuPolicy maps BACKTESTER_GPU to a policy: "on"/"always", "auto"/"ondemand",
// anything else (including unset) keeps the safe default of off.
func gpuPolicy(getenv func(string) string) linux.WebviewGpuPolicy {
	switch strings.ToLower(strings.TrimSpace(getenv(gpuEnvVar))) {
	case "on", "always", "1", "true":
		return linux.WebviewGpuPolicyAlways
	case "auto", "ondemand", "on-demand":
		return linux.WebviewGpuPolicyOnDemand
	default:
		return linux.WebviewGpuPolicyNever
	}
}
