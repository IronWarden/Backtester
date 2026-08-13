package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Anthropic Messages API client for the chat assistant: streaming SSE,
// plus the agentic loop that services query_db tool calls. Shared prompt
// building, tool execution, and event plumbing live in chat.go.

const (
	anthropicMessagesURL = "https://api.anthropic.com/v1/messages"
	anthropicVersion     = "2023-06-01"
	defaultClaudeModel   = "claude-sonnet-5"
	maxChatTokens        = 8192
)

// parseEnvFile reads KEY=VALUE lines, ignoring blanks and # comments.
func parseEnvFile(path string) map[string]string {
	out := map[string]string{}
	b, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
	}
	return out
}

// loadClaudeKey finds the API key: environment first, then the project's
// .env (cwd and repo root), matching where FRED_API_KEY already lives.
func loadClaudeKey() string {
	if k := os.Getenv("ANTHROPIC_API_KEY"); k != "" {
		return k
	}
	for _, p := range []string{".env", filepath.Join(repoRoot(), ".env")} {
		if k := parseEnvFile(p)["ANTHROPIC_API_KEY"]; k != "" {
			return k
		}
	}
	return ""
}

// HasClaudeKey reports whether an Anthropic API key is configured, so the
// frontend can show the one-time setup form instead of a chat box.
func (a *App) HasClaudeKey() bool {
	return loadClaudeKey() != ""
}

// SaveClaudeKey persists the key into the project's .env (updating an
// existing ANTHROPIC_API_KEY line or appending one), alongside the other
// keys the project keeps there.
func (a *App) SaveClaudeKey(key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("key is empty")
	}
	envPath := filepath.Join(repoRoot(), ".env")
	var lines []string
	if b, err := os.ReadFile(envPath); err == nil {
		lines = strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	}
	replaced := false
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "ANTHROPIC_API_KEY=") {
			lines[i] = "ANTHROPIC_API_KEY=" + key
			replaced = true
		}
	}
	if !replaced {
		lines = append(lines, "ANTHROPIC_API_KEY="+key)
	}
	return os.WriteFile(envPath, []byte(strings.Join(lines, "\n")+"\n"), 0600)
}

// anthropicTools wraps the shared tool schemas in the Messages API
// tool format.
func anthropicTools() []map[string]any {
	return []map[string]any{
		{
			"name":         queryDBToolName,
			"description":  queryDBToolDescription,
			"input_schema": queryDBToolSchema(),
		},
		{
			"name":         lookupQuoteToolName,
			"description":  lookupQuoteToolDescription,
			"input_schema": lookupQuoteToolSchema(),
		},
		{
			"name":         fetchHistoryToolName,
			"description":  fetchHistoryToolDescription,
			"input_schema": fetchHistoryToolSchema(),
		},
		{
			"name":         screenStocksToolName,
			"description":  screenStocksToolDescription,
			"input_schema": screenStocksToolSchema(),
		},
	}
}

// sseEvent is the union of the streaming event payloads we care about.
type sseEvent struct {
	Type  string `json:"type"`
	Index int    `json:"index"`

	ContentBlock struct {
		Type string `json:"type"`
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"content_block"`

	Delta struct {
		Type        string `json:"type"`
		Text        string `json:"text"`
		PartialJSON string `json:"partial_json"`
		StopReason  string `json:"stop_reason"`
	} `json:"delta"`

	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// streamedBlock accumulates one content block across its deltas.
type streamedBlock struct {
	typ       string // "text" | "tool_use"
	id, name  string
	text      strings.Builder
	inputJSON strings.Builder
}

// toolUse is a fully-received tool invocation from the model.
type toolUse struct {
	ID    string
	Name  string
	Input map[string]any
}

// streamClaudeOnce performs one streaming Messages API call. Text deltas
// are forwarded through em as they arrive. It returns the assistant
// message's content blocks (ready to append to the conversation), any tool
// calls, and the stop reason.
func streamClaudeOnce(
	ctx context.Context,
	key, model, system string,
	msgs []map[string]any,
	em *chatEmitter,
) (content []map[string]any, tools []toolUse, stopReason string, err error) {
	body, err := json.Marshal(map[string]any{
		"model":      model,
		"max_tokens": maxChatTokens,
		"system":     system,
		"messages":   msgs,
		"tools":      anthropicTools(),
		"stream":     true,
	})
	if err != nil {
		return nil, nil, "", err
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, anthropicMessagesURL, bytes.NewReader(body))
	if err != nil {
		return nil, nil, "", err
	}
	req.Header.Set("x-api-key", key)
	req.Header.Set("anthropic-version", anthropicVersion)
	req.Header.Set("content-type", "application/json")
	req.Header.Set("accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, "", fmt.Errorf("calling Anthropic API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		var apiErr struct {
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(raw, &apiErr) == nil && apiErr.Error.Message != "" {
			return nil, nil, "", fmt.Errorf(
				"API error (%s): %s", apiErr.Error.Type, apiErr.Error.Message)
		}
		return nil, nil, "", fmt.Errorf(
			"API returned HTTP %d: %s", resp.StatusCode, string(raw))
	}

	blocks := map[int]*streamedBlock{}
	var order []int

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			continue
		}
		var ev sseEvent
		if err := json.Unmarshal([]byte(payload), &ev); err != nil {
			continue
		}
		switch ev.Type {
		case "content_block_start":
			b := &streamedBlock{
				typ:  ev.ContentBlock.Type,
				id:   ev.ContentBlock.ID,
				name: ev.ContentBlock.Name,
			}
			blocks[ev.Index] = b
			order = append(order, ev.Index)
		case "content_block_delta":
			b := blocks[ev.Index]
			if b == nil {
				continue
			}
			switch ev.Delta.Type {
			case "text_delta":
				b.text.WriteString(ev.Delta.Text)
				em.text(ev.Delta.Text)
			case "input_json_delta":
				b.inputJSON.WriteString(ev.Delta.PartialJSON)
			}
		case "message_delta":
			if ev.Delta.StopReason != "" {
				stopReason = ev.Delta.StopReason
			}
		case "error":
			return nil, nil, "", fmt.Errorf(
				"stream error (%s): %s", ev.Error.Type, ev.Error.Message)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, "", fmt.Errorf("reading stream: %w", err)
	}

	sort.Ints(order)
	for _, idx := range order {
		b := blocks[idx]
		switch b.typ {
		case "text":
			if b.text.Len() == 0 {
				continue
			}
			content = append(content, map[string]any{
				"type": "text", "text": b.text.String(),
			})
		case "tool_use":
			input := map[string]any{}
			if raw := b.inputJSON.String(); raw != "" {
				// A malformed input still needs to reach the model as a
				// tool_result error, so decode failures leave input empty.
				_ = json.Unmarshal([]byte(raw), &input)
			}
			content = append(content, map[string]any{
				"type": "tool_use", "id": b.id, "name": b.name, "input": input,
			})
			tools = append(tools, toolUse{ID: b.id, Name: b.name, Input: input})
		}
	}
	return content, tools, stopReason, nil
}

// anthropicChat runs the agentic loop against the Anthropic API: stream a
// turn, service any query_db calls, repeat until the model stops.
func anthropicChat(
	ctx context.Context,
	model, system string,
	history []ChatMessage,
	dbPath string,
	em *chatEmitter,
) error {
	key := loadClaudeKey()
	if key == "" {
		return fmt.Errorf(
			"no API key: set ANTHROPIC_API_KEY in the project's .env " +
				"(the chat panel has a setup form for this), or pick a " +
				"local Ollama model instead")
	}
	if model == "" {
		model = defaultClaudeModel
	}

	msgs := make([]map[string]any, 0, len(history))
	for _, m := range history {
		msgs = append(msgs, map[string]any{"role": m.Role, "content": m.Content})
	}

	for iter := 0; iter < maxToolIterations; iter++ {
		em.startTurn()
		content, tools, stopReason, err := streamClaudeOnce(
			ctx, key, model, system, msgs, em)
		if err != nil {
			return err
		}
		if len(content) == 0 {
			return nil
		}
		msgs = append(msgs, map[string]any{
			"role": "assistant", "content": content,
		})
		if stopReason != "tool_use" || len(tools) == 0 {
			if stopReason == "max_tokens" {
				em.text("\n\n*(response cut off: output limit reached)*")
			}
			return nil
		}

		results := make([]map[string]any, 0, len(tools))
		for _, tu := range tools {
			var out string
			switch tu.Name {
			case queryDBToolName:
				sql, _ := tu.Input["sql"].(string)
				em.tool(sql)
				out = runQueryTool(dbPath, sql)
			case lookupQuoteToolName:
				syms, _ := tu.Input["symbols"].([]any)
				em.tool("lookup_quote: " + symbolsPreview(syms))
				out = runQuoteTool(ctx, syms)
			case fetchHistoryToolName:
				em.tool("fetch_history: " + fetchPreview(tu.Input))
				out = runFetchTool(ctx, dbPath, tu.Input, em)
			case screenStocksToolName:
				em.tool("screen_stocks: " + screenPreview(tu.Input))
				out = runScreenTool(ctx, tu.Input)
			default:
				out = fmt.Sprintf("ERROR: unknown tool %q", tu.Name)
			}
			results = append(results, map[string]any{
				"type":        "tool_result",
				"tool_use_id": tu.ID,
				"content":     out,
			})
		}
		msgs = append(msgs, map[string]any{"role": "user", "content": results})
	}
	return nil
}
