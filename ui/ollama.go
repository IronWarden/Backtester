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
	"strings"
	"time"
)

// Ollama client for the chat assistant, so locally-pulled models work
// alongside the Claude ones. Uses the native /api/chat NDJSON streaming
// endpoint. Models that advertise the "tools" capability get the same
// query_db tool as Claude; others just chat over the (leaner) system
// prompt. Shared prompt building, tool execution, and event plumbing live
// in chat.go.

// ollamaNumCtx overrides Ollama's small default context window so the
// system prompt plus a real conversation fits. Kept moderate because the
// KV cache for a large num_ctx costs RAM on the user's machine.
const ollamaNumCtx = 16384

// ollamaHost resolves the server base URL from OLLAMA_HOST (the variable
// the ollama CLI itself honors), defaulting to the standard local port.
func ollamaHost() string {
	h := strings.TrimSpace(os.Getenv("OLLAMA_HOST"))
	if h == "" {
		return "http://localhost:11434"
	}
	if !strings.Contains(h, "://") {
		h = "http://" + h
	}
	return strings.TrimRight(h, "/")
}

// ollamaModel is one locally-available model from /api/tags.
type ollamaModel struct {
	Name         string   `json:"name"`
	Capabilities []string `json:"capabilities"`
}

// listOllamaTags asks the local server what models it has. A short timeout
// keeps the model dropdown snappy when no server is running.
func listOllamaTags() ([]ollamaModel, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, ollamaHost()+"/api/tags", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama /api/tags: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Models []ollamaModel `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Models, nil
}

// modelCapabilities returns the capability list for name, or nil if the
// server or model is unavailable (callers treat nil as "assume basic").
func modelCapabilities(name string) []string {
	tags, err := listOllamaTags()
	if err != nil {
		return nil
	}
	for _, m := range tags {
		if m.Name == name || m.Name == name+":latest" {
			return m.Capabilities
		}
	}
	return nil
}

func hasCapability(caps []string, want string) bool {
	for _, c := range caps {
		if c == want {
			return true
		}
	}
	return false
}

// ollamaToolCall mirrors the wire shape of a tool call; it is echoed back
// verbatim in the assistant message when building the follow-up request.
type ollamaToolCall struct {
	Function struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	} `json:"function"`
}

// ollamaStreamChunk is one NDJSON line of a streaming /api/chat response.
type ollamaStreamChunk struct {
	Message struct {
		Role      string           `json:"role"`
		Content   string           `json:"content"`
		Thinking  string           `json:"thinking"`
		ToolCalls []ollamaToolCall `json:"tool_calls"`
	} `json:"message"`
	Done       bool   `json:"done"`
	DoneReason string `json:"done_reason"`
	Error      string `json:"error"`
}

// ollamaTools wraps the shared tool schemas in Ollama's (OpenAI-style)
// tool format.
func ollamaTools() []map[string]any {
	return []map[string]any{
		{
			"type": "function",
			"function": map[string]any{
				"name":        queryDBToolName,
				"description": queryDBToolDescription,
				"parameters":  queryDBToolSchema(),
			},
		},
		{
			"type": "function",
			"function": map[string]any{
				"name":        lookupQuoteToolName,
				"description": lookupQuoteToolDescription,
				"parameters":  lookupQuoteToolSchema(),
			},
		},
		{
			"type": "function",
			"function": map[string]any{
				"name":        fetchHistoryToolName,
				"description": fetchHistoryToolDescription,
				"parameters":  fetchHistoryToolSchema(),
			},
		},
		{
			"type": "function",
			"function": map[string]any{
				"name":        screenStocksToolName,
				"description": screenStocksToolDescription,
				"parameters":  screenStocksToolSchema(),
			},
		},
	}
}

// streamOllamaOnce performs one streaming /api/chat call, forwarding text
// deltas through em. Thinking deltas are dropped: the panel shows its own
// placeholder until visible text arrives. Returns the assistant's text and
// any tool calls.
func streamOllamaOnce(
	ctx context.Context,
	model string,
	msgs []map[string]any,
	withTools bool,
	think *bool,
	em *chatEmitter,
) (text string, calls []ollamaToolCall, err error) {
	payload := map[string]any{
		"model":    model,
		"messages": msgs,
		"stream":   true,
		"options":  map[string]any{"num_ctx": ollamaNumCtx},
	}
	if withTools {
		payload["tools"] = ollamaTools()
	}
	if think != nil {
		payload["think"] = *think
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", nil, err
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, ollamaHost()+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("content-type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf(
			"calling Ollama at %s: %w (is `ollama serve` running?)",
			ollamaHost(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		var apiErr struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(raw, &apiErr) == nil && apiErr.Error != "" {
			return "", nil, fmt.Errorf("ollama: %s", apiErr.Error)
		}
		return "", nil, fmt.Errorf(
			"ollama returned HTTP %d: %s", resp.StatusCode, string(raw))
	}

	var sb strings.Builder
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var chunk ollamaStreamChunk
		if err := json.Unmarshal([]byte(line), &chunk); err != nil {
			continue
		}
		if chunk.Error != "" {
			return sb.String(), calls, fmt.Errorf("ollama: %s", chunk.Error)
		}
		if chunk.Message.Content != "" {
			sb.WriteString(chunk.Message.Content)
			em.text(chunk.Message.Content)
		}
		calls = append(calls, chunk.Message.ToolCalls...)
		if chunk.Done {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return sb.String(), calls, fmt.Errorf("reading ollama stream: %w", err)
	}
	return sb.String(), calls, nil
}

// ollamaChat runs the agentic loop against a local Ollama model: stream a
// turn, service any query_db calls, repeat until the model stops calling
// tools. Mirrors anthropicChat but speaks Ollama's message format.
func ollamaChat(
	ctx context.Context,
	model, system string,
	history []ChatMessage,
	dbPath string,
	em *chatEmitter,
) error {
	caps := modelCapabilities(model)
	// Unknown capabilities (older server, model missing from tags) fall
	// back to attempting tools; the server rejects them with a clear error
	// the user can act on by picking another model.
	withTools := caps == nil || hasCapability(caps, "tools")
	// Thinking-capable models (like qwen3.5) are told not to think so the
	// panel isn't silent for minutes on a small local model; only send the
	// flag when the capability is advertised or older servers would error.
	var think *bool
	if hasCapability(caps, "thinking") {
		f := false
		think = &f
	}

	if !withTools {
		system += "\n\n(Note: the query_db tool is NOT available in this " +
			"session — answer from the context above and say so when the " +
			"user asks for data you cannot verify.)"
	}

	msgs := make([]map[string]any, 0, len(history)+1)
	msgs = append(msgs, map[string]any{"role": "system", "content": system})
	for _, m := range history {
		msgs = append(msgs, map[string]any{"role": m.Role, "content": m.Content})
	}

	for iter := 0; iter < maxToolIterations; iter++ {
		em.startTurn()
		text, calls, err := streamOllamaOnce(
			ctx, model, msgs, withTools, think, em)
		if err != nil {
			return err
		}
		if len(calls) == 0 {
			return nil
		}

		assistant := map[string]any{"role": "assistant", "content": text}
		assistant["tool_calls"] = calls
		msgs = append(msgs, assistant)

		for _, call := range calls {
			var out string
			switch call.Function.Name {
			case queryDBToolName:
				sql, _ := call.Function.Arguments["sql"].(string)
				em.tool(sql)
				out = runQueryTool(dbPath, sql)
			case lookupQuoteToolName:
				syms, _ := call.Function.Arguments["symbols"].([]any)
				em.tool("lookup_quote: " + symbolsPreview(syms))
				out = runQuoteTool(ctx, syms)
			case fetchHistoryToolName:
				em.tool("fetch_history: " + fetchPreview(call.Function.Arguments))
				out = runFetchTool(ctx, dbPath, call.Function.Arguments, em)
			case screenStocksToolName:
				em.tool("screen_stocks: " + screenPreview(call.Function.Arguments))
				out = runScreenTool(ctx, call.Function.Arguments)
			default:
				out = fmt.Sprintf("ERROR: unknown tool %q", call.Function.Name)
			}
			msgs = append(msgs, map[string]any{
				"role":      "tool",
				"tool_name": call.Function.Name,
				"content":   out,
			})
		}
	}
	return nil
}
