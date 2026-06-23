import React from "react";
import { createRoot } from "react-dom/client";
import "./style.css";
import App from "./App";

// Bundle Monaco locally so the app works offline. Importing from
// editor.api skips Monaco's "all basic languages" barrel — we add only
// the syntaxes we use (Lua, INI for TOML). Vite turns ?worker into a
// worker entry point, which MonacoEnvironment uses to spawn the editor's
// background services without touching the network.
import * as monaco from "monaco-editor/esm/vs/editor/editor.api";
import "monaco-editor/esm/vs/basic-languages/lua/lua.contribution";
import "monaco-editor/esm/vs/basic-languages/ini/ini.contribution";
import editorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker";
import { loader } from "@monaco-editor/react";

self.MonacoEnvironment = {
  getWorker() {
    return new editorWorker();
  },
};
loader.config({ monaco });

const container = document.getElementById("root");
const root = createRoot(container!);

root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
