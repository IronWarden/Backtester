package main

// Regression net for the chat tool surface. Future tools must be wired into
// BOTH providers with identical schemas, must fit the lean Ollama prompt,
// and must be routed by the system prompt — these tests fail loudly when a
// new tool (or an edit to an existing one) misses one of those spots.

import (
	"my-backtester/src/backtest"
	"reflect"
	"strings"
	"testing"
)

// anthropicByName flattens anthropicTools() to name -> (description, schema).
func anthropicByName(t *testing.T) map[string][2]any {
	t.Helper()
	out := map[string][2]any{}
	for _, tool := range anthropicTools() {
		name, _ := tool["name"].(string)
		if name == "" {
			t.Fatalf("anthropic tool with empty name: %v", tool)
		}
		out[name] = [2]any{tool["description"], tool["input_schema"]}
	}
	return out
}

// ollamaByName flattens ollamaTools()' OpenAI-style wrappers the same way.
func ollamaByName(t *testing.T) map[string][2]any {
	t.Helper()
	out := map[string][2]any{}
	for _, tool := range ollamaTools() {
		if tool["type"] != "function" {
			t.Fatalf("ollama tool missing type=function: %v", tool)
		}
		fn, ok := tool["function"].(map[string]any)
		if !ok {
			t.Fatalf("ollama tool missing function object: %v", tool)
		}
		name, _ := fn["name"].(string)
		if name == "" {
			t.Fatalf("ollama tool with empty name: %v", tool)
		}
		out[name] = [2]any{fn["description"], fn["parameters"]}
	}
	return out
}

// Both providers must expose the same tools with byte-identical
// descriptions and schemas — a tool added to one but not the other would
// silently vanish for half the models.
func TestProviderToolParity(t *testing.T) {
	a, o := anthropicByName(t), ollamaByName(t)
	if len(a) == 0 {
		t.Fatal("no tools registered")
	}
	if len(a) != len(o) {
		t.Fatalf("tool count differs: anthropic=%d ollama=%d", len(a), len(o))
	}
	for name, av := range a {
		ov, ok := o[name]
		if !ok {
			t.Errorf("tool %q registered for Claude but not Ollama", name)
			continue
		}
		if av[0] != ov[0] {
			t.Errorf("tool %q: descriptions differ between providers", name)
		}
		if !reflect.DeepEqual(av[1], ov[1]) {
			t.Errorf("tool %q: schemas differ between providers", name)
		}
	}
}

// Every schema must be a well-formed object schema whose required fields
// actually exist in properties.
func TestToolSchemasWellFormed(t *testing.T) {
	for name, v := range anthropicByName(t) {
		schema, ok := v[1].(map[string]any)
		if !ok {
			t.Errorf("tool %q: schema is not an object", name)
			continue
		}
		if schema["type"] != "object" {
			t.Errorf("tool %q: schema type = %v, want object", name, schema["type"])
		}
		props, ok := schema["properties"].(map[string]any)
		if !ok || len(props) == 0 {
			t.Errorf("tool %q: schema has no properties", name)
			continue
		}
		if req, ok := schema["required"].([]string); ok {
			for _, r := range req {
				if _, exists := props[r]; !exists {
					t.Errorf("tool %q: required field %q not in properties",
						name, r)
				}
			}
		}
	}
}

// Descriptions ride in every request; the Ollama lean prompt runs in a 16k
// context, so a description ballooning past ~1000 chars is a regression.
func TestToolDescriptionBounds(t *testing.T) {
	for name, v := range anthropicByName(t) {
		desc, _ := v[0].(string)
		if len(desc) < 50 {
			t.Errorf("tool %q: description too short to be useful (%d chars)",
				name, len(desc))
		}
		if len(desc) > 1000 {
			t.Errorf("tool %q: description too long for the lean prompt (%d chars)",
				name, len(desc))
		}
	}
}

// The system prompt must name every registered tool (routing guidance) and
// keep the load-bearing data facts that past failures traced back to.
func TestSystemPromptRoutesAllTools(t *testing.T) {
	for name := range anthropicByName(t) {
		if !strings.Contains(appReference, name) {
			t.Errorf("appReference never mentions tool %q — the model gets "+
				"no routing guidance for it", name)
		}
	}
	for _, want := range []string{
		// Fundamentals live in Yahoo tools, never in the DB (the "P/E < 20
		// from the DB" failure).
		"NO fundamentals",
		// CASH-the-ticker is a bank stock, not a cash proxy.
		"Pathward",
		"$CASH",
	} {
		if !strings.Contains(appReference, want) {
			t.Errorf("appReference lost the %q guidance", want)
		}
	}
	if !strings.Contains(queryDBToolDescription, "no fundamentals") {
		t.Error("query_db description no longer warns it has no fundamentals")
	}
}

// The assistant's knowledge of the engine is a hand-written const, so it
// drifts silently every time the engine grows a feature and nobody edits it —
// which is exactly how it ended up telling users the engine modelled no
// transaction costs for as long as it did. Reflecting over the metric struct
// the engine actually fills in turns that drift into a build failure: add a
// field to backtest.Metrics and this test fails until appReference explains
// it. Documenting the metric to the user is part of shipping it, not a
// follow-up.
func TestSystemPromptDocumentsEveryMetric(t *testing.T) {
	mt := reflect.TypeOf(backtest.Metrics{})
	for i := range mt.NumField() {
		name := mt.Field(i).Name
		if !strings.Contains(appReference, name) {
			t.Errorf("appReference never mentions the %q metric — the "+
				"assistant cannot explain a number the results panel shows",
				name)
		}
	}
}

// Config knobs have the same drift problem as metrics, and a worse failure
// mode: the assistant writes the TOML, so a field it does not know about is a
// field the user never gets offered. Keyed by field name rather than reflected
// so the check covers the [Output] and [portfolio.Costs] sub-tables too.
func TestSystemPromptDocumentsConfigSchema(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeOf(backtest.PortfolioConfig{}),
		reflect.TypeOf(backtest.CostConfig{}),
	} {
		for i := range typ.NumField() {
			tag := typ.Field(i).Tag.Get("toml")
			if tag == "" {
				continue
			}
			if !strings.Contains(appReference, tag) {
				t.Errorf("appReference never mentions the %s config key %q — "+
					"the assistant will never write it into a config",
					typ.Name(), tag)
			}
		}
	}
}
