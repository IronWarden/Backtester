package main

import (
	"testing"

	"github.com/wailsapp/wails/v2/pkg/options/linux"
)

func TestGPUPolicyDefaultsOff(t *testing.T) {
	cases := map[string]linux.WebviewGpuPolicy{
		"":         linux.WebviewGpuPolicyNever,
		"off":      linux.WebviewGpuPolicyNever,
		"nonsense": linux.WebviewGpuPolicyNever,
		"on":       linux.WebviewGpuPolicyAlways,
		" ALWAYS ": linux.WebviewGpuPolicyAlways,
		"1":        linux.WebviewGpuPolicyAlways,
		"auto":     linux.WebviewGpuPolicyOnDemand,
		"ondemand": linux.WebviewGpuPolicyOnDemand,
	}
	for in, want := range cases {
		got := gpuPolicy(func(string) string { return in })
		if got != want {
			t.Errorf("BACKTESTER_GPU=%q: got %v, want %v", in, got, want)
		}
	}
}
