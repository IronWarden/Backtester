// Bundles and runs src/smoke.tsx, which renders the app's components against
// fixtures generated from the Go structs.
//
// esbuild is used directly rather than a test runner because it is already
// vendored with vite -- this adds no dependency. See T87/T91 for the real
// runner, which needs one.
import { build } from "esbuild";
import { mkdirSync, rmSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
// The bundle has to sit inside the project: react and react-dom are left
// external so node loads them itself, and node resolves a bare specifier by
// walking up from the importing file to node_modules.
const out = join(root, "node_modules", ".cache", "bt-smoke");
const bundle = join(out, `smoke.${Date.now()}.mjs`);
mkdirSync(out, { recursive: true });

try {
  await build({
    entryPoints: [join(root, "src", "smoke.tsx")],
    bundle: true,
    platform: "node",
    format: "esm",
    outfile: bundle,
    loader: { ".css": "text" },
    logLevel: "warning",
    // react and friends stay external so node resolves them itself; bundling
    // react-dom's CJS build turns its `require("stream")` into a dynamic
    // require that ESM cannot serve.
    external: ["react", "react/jsx-runtime", "react-dom", "react-dom/server"],
  });
  process.env.SMOKE_TESTDATA = join(root, "testdata");
  await import(pathToFileURL(bundle).href);
} finally {
  rmSync(out, { recursive: true, force: true });
}
