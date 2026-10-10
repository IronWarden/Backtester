import React from "react";
import { createRoot } from "react-dom/client";
import "./style.css";
import App from "./App";
import ErrorBoundary from "./ErrorBoundary";

// Monaco is NOT imported here: see monacoSetup.ts. Preloaded after the first
// paint, so it costs the first screen nothing and the Config tab nothing
// either. setTimeout rather than requestIdleCallback, which WebKitGTK lacks.
setTimeout(() => void import("./monacoSetup"), 1200);

const container = document.getElementById("root");
const root = createRoot(container!);

root.render(
  <React.StrictMode>
    <ErrorBoundary label="The app">
      <App />
    </ErrorBoundary>
  </React.StrictMode>,
);
