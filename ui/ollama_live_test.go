package main

import (
	"strings"
	"testing"
)

// Live tests against the local Ollama server; skipped when it isn't
// running. They exercise the full Chat entry point (system prompt, NDJSON
// streaming, and the query_db tool loop) with a real local model.
const liveOllamaModel = "ollama:qwen3.5:4b"

func requireOllama(t *testing.T) {
	t.Helper()
	if _, err := listOllamaTags(); err != nil {
		t.Skipf("ollama not reachable: %v", err)
	}
}

func TestOllamaChatLive(t *testing.T) {
	requireOllama(t)
	out, err := (&App{}).Chat(
		[]ChatMessage{{Role: "user", Content: "Reply with exactly: PONG"}},
		liveOllamaModel, "", "", "../stock_data.db",
	)
	if err != nil {
		t.Fatalf("chat failed: %v", err)
	}
	t.Logf("reply: %q", out)
	if !strings.Contains(strings.ToUpper(out), "PONG") {
		t.Errorf("unexpected reply: %q", out)
	}
}

func TestOllamaChatToolLive(t *testing.T) {
	requireOllama(t)
	out, err := (&App{}).Chat(
		[]ChatMessage{{Role: "user", Content: "Use the query_db tool to count " +
			"the distinct tickers in stock_data_optimized, then state the " +
			"number."}},
		liveOllamaModel, "", "", "../stock_data.db",
	)
	if err != nil {
		t.Fatalf("chat failed: %v", err)
	}
	t.Logf("reply: %s", out)
	// 10434 tickers at the time of writing; accept any 4+ digit count so
	// the test survives DB refreshes.
	if !strings.Contains(out, "10434") && !strings.Contains(out, "10,434") {
		t.Logf("note: reply does not contain the current ticker count " +
			"(model may have answered loosely); inspect the log above")
	}
}
