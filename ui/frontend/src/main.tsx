import React from "react";
import { createRoot } from "react-dom/client";
import "./style.css";
import App from "./App";
import ErrorBoundary from "./ErrorBoundary";

// Bundle Monaco locally so the app works offline. Importing from
// editor.api skips Monaco's "all basic languages" barrel — we add only
// the syntaxes we use (Lua, INI for TOML). Vite turns ?worker into a
// worker entry point, which MonacoEnvironment uses to spawn the editor's
// background services without touching the network.
import * as monaco from "monaco-editor/esm/vs/editor/editor.api";
import "monaco-editor/esm/vs/basic-languages/lua/lua.contribution";
import "monaco-editor/esm/vs/basic-languages/ini/ini.contribution";
import "monaco-editor/esm/vs/basic-languages/sql/sql.contribution";
import editorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker";
import { loader } from "@monaco-editor/react";

self.MonacoEnvironment = {
  getWorker() {
    return new editorWorker();
  },
};
loader.config({ monaco });

// vs-dark's #1e1e1e clashes with the app's Kanagawa surfaces; every editor
// (TOML, Lua, SQL console) uses this re-grounded variant instead. The token
// rules are Kanagawa's own syntax colours, so code reads like it does in the
// editor the palette comes from.
monaco.editor.defineTheme("backtester-dark", {
  base: "vs-dark",
  inherit: true,
  rules: [
    { token: "", foreground: "dcd7ba" },
    { token: "comment", foreground: "727169", fontStyle: "italic" },
    { token: "keyword", foreground: "957fb8" },
    { token: "string", foreground: "98bb6c" },
    { token: "number", foreground: "d27e99" },
    { token: "type", foreground: "7aa89f" },
    { token: "identifier", foreground: "dcd7ba" },
    { token: "delimiter", foreground: "9cabca" },
    { token: "operator", foreground: "c0a36e" },
    { token: "predefined", foreground: "7e9cd8" },
    { token: "constant", foreground: "ffa066" },
  ],
  colors: {
    "editor.background": "#181820",
    "editor.foreground": "#dcd7ba",
    "editorGutter.background": "#181820",
    "editor.lineHighlightBackground": "#1f1f28",
    "editor.selectionBackground": "#2d4f67",
    "editorCursor.foreground": "#c8c093",
    "editorLineNumber.foreground": "#54546d",
    "editorLineNumber.activeForeground": "#ffa066",
    "editorWidget.background": "#1f1f28",
    "editorSuggestWidget.background": "#1f1f28",
    "editorSuggestWidget.selectedBackground": "#2d4f67",
    "editorIndentGuide.background": "#2a2a37",
  },
});

const container = document.getElementById("root");
const root = createRoot(container!);

root.render(
  <React.StrictMode>
    <ErrorBoundary label="The app">
      <App />
    </ErrorBoundary>
  </React.StrictMode>,
);
