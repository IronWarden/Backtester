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
		// Quarterly fundamentals DO exist. This assertion previously pinned
		// the opposite — the prompt claimed the database had none, which was
		// false for years and had the assistant denying a real capability to
		// every user. Pinning the correction stops it regressing.
		"financials(metric, date, value, ticker, frequency)",
		// ...but its dates are fiscal period ends, so a naive join is
		// look-ahead bias. An assistant that knows the table exists and not
		// this is more dangerous than one that knows neither, which is why
		// the two are asserted together.
		"FISCAL PERIOD END",
		"look-ahead",
		// Sector data genuinely is absent: company_info is empty.
		"company_info is EMPTY",
		// CASH-the-ticker is a bank stock, not a cash proxy.
		"Pathward",
		"$CASH",
	} {
		if !strings.Contains(appReference, want) {
			t.Errorf("appReference lost the %q guidance", want)
		}
	}
	// Shorting shipped, but the Lua section went on flatly denying it while
	// the same prompt documented AllowShort further down — the transaction-
	// cost failure repeating itself. Pin the retraction, not just the
	// feature: the config keys alone are already covered by reflection, and
	// they were present the whole time this claim was wrong.
	for _, gone := range []string{"There is no shorting", "no shorting and no"} {
		if strings.Contains(appReference, gone) {
			t.Errorf("appReference still claims %q — shorting is opt-in, and "+
				"the same prompt documents AllowShort", gone)
		}
	}
	// The tool description is what a model sees when deciding whether to
	// reach for query_db at all, so the same caveat has to survive there.
	for _, want := range []string{"FISCAL PERIOD END", "look-ahead"} {
		if !strings.Contains(queryDBToolDescription, want) {
			t.Errorf("query_db description lost the %q warning", want)
		}
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

// The Lua globals now have one authority — backtest.LuaAPI, pinned by a test
// there to the engine's own SetGlobal calls — so the prompt's hand-written
// listing can be checked against it. The failure this prevents is specific and
// has happened in the metric case already: the assistant writes strategies, so
// a global it has never heard of is one it will never use, and worse, one it
// may replace with something plausible that does not exist.
func TestSystemPromptDocumentsEveryLuaGlobal(t *testing.T) {
	for _, g := range backtest.LuaAPI() {
		if !strings.Contains(appReference, g.Name) {
			t.Errorf("appReference never mentions the Lua global %q — the "+
				"assistant cannot write a strategy that uses it", g.Name)
		}
		if !strings.Contains(leanReference, g.Name) {
			t.Errorf("leanReference never mentions the Lua global %q — a local "+
				"model will not know it exists", g.Name)
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
		reflect.TypeOf(backtest.MarginConfig{}),
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

// The lean reference is what a local model actually receives, so it drifts
// exactly like appReference does — and it is the one a small model has no
// spare context to recover from. Same reflection checks, same reason.
func TestLeanReferenceDocumentsEngine(t *testing.T) {
	for name := range anthropicByName(t) {
		if !strings.Contains(leanReference, name) {
			t.Errorf("leanReference never mentions tool %q — a local model "+
				"gets no routing guidance for it", name)
		}
	}
	mt := reflect.TypeOf(backtest.Metrics{})
	for i := range mt.NumField() {
		if name := mt.Field(i).Name; !strings.Contains(leanReference, name) {
			t.Errorf("leanReference never mentions the %q metric", name)
		}
	}
	for _, typ := range []reflect.Type{
		reflect.TypeOf(backtest.PortfolioConfig{}),
		reflect.TypeOf(backtest.CostConfig{}),
		reflect.TypeOf(backtest.MarginConfig{}),
	} {
		for i := range typ.NumField() {
			tag := typ.Field(i).Tag.Get("toml")
			if tag == "" {
				continue
			}
			if !strings.Contains(leanReference, tag) {
				t.Errorf("leanReference never mentions the %s config key %q",
					typ.Name(), tag)
			}
		}
	}
	// The traps that make a wrong answer rather than a vague one survive
	// the condensing, or the condensing is what broke the assistant.
	for _, want := range []string{
		"FISCAL PERIOD END", "look-ahead", "Pathward", "$CASH",
		"company_info is EMPTY",
	} {
		if !strings.Contains(leanReference, want) {
			t.Errorf("leanReference lost the %q guidance", want)
		}
	}
}

// The whole point of the lean reference is fitting ollamaNumCtx with room
// left for the live context, the tool schemas and an actual conversation.
// Budget it at a third of the window (~4 chars per token) so that headroom
// is enforced rather than hoped for: appReference failed this by 3x, which
// is how a local model lost the tail of its own prompt.
func TestLeanPromptFitsOllamaContext(t *testing.T) {
	const budget = ollamaNumCtx / 3 * 4
	if n := len(buildSystemPrompt("", "", "", true)); n > budget {
		t.Errorf("lean system prompt is %d chars, over the %d-char budget "+
			"for num_ctx %d — a local model will silently lose the tail",
			n, budget, ollamaNumCtx)
	}
}

// The Lua tab's ✓ is the shortest path from a typo to knowing about it, and it
// has to work on the two things a user's editor actually holds: a script with
// a config beside it, and a script with a config that does not parse yet.
func TestCheckLuaScriptFindsParamsAndSurvivesABrokenConfig(t *testing.T) {
	const script = "function step(day)\n  local _ = params.period\nend\n"
	const cfg = `
[[portfolio]]
Name = "P"
BuyingPower = 1000.0
StartDate = "2020-01-01"
EndDate = "2021-01-01"
Tickers = ["AAA"]
Strategy = "lua:strategies/mine.lua"
[portfolio.Params]
period = 20
lookbck = 60
`
	a := &App{}
	out, err := a.CheckLuaScript(script, "strategies/mine.lua", cfg)
	if err != nil {
		t.Fatalf("CheckLuaScript: %v", err)
	}
	var joined string
	for _, d := range out.Diagnostics {
		joined += d.Level + ": " + d.Message + "\n"
	}
	if !strings.Contains(joined, "params.lookbck") {
		t.Errorf("the params the config sets were not checked against the script:\n%s", joined)
	}
	if strings.Contains(joined, "error") {
		t.Errorf("a working script reported an error:\n%s", joined)
	}

	// A config mid-edit must not stop the script being checked: the TOML tab's
	// own ✓ is what reports a broken config.
	out, err = a.CheckLuaScript(script, "strategies/mine.lua", "[[portfolio]\nName =")
	if err != nil {
		t.Fatalf("CheckLuaScript with an unparseable config: %v", err)
	}
	for _, d := range out.Diagnostics {
		if d.Level == "error" {
			t.Errorf("an unparseable config broke the script check: %s", d.Message)
		}
	}

	// And the script's own failure still reaches the user.
	out, _ = a.CheckLuaScript("step = 3\n", "strategies/mine.lua", "")
	found := false
	for _, d := range out.Diagnostics {
		if d.Level == "error" && strings.Contains(d.Message, "not a function") {
			found = true
		}
	}
	if !found {
		t.Errorf("a script whose step is not a function passed the check: %+v", out.Diagnostics)
	}
}
