import { Suspense, lazy } from "react";
import type { EditorProps } from "@monaco-editor/react";

// The Monaco editor, loaded on first use (see monacoSetup.ts). A drop-in for
// @monaco-editor/react's default export: same props, plus a quiet placeholder
// for the moment the chunk is still arriving. In practice main.tsx has
// preloaded it long before anyone opens the Config or Data tab.
const MonacoEditor = lazy(async () => {
    await import("./monacoSetup");
    return import("@monaco-editor/react");
});

export default function Editor(props: EditorProps) {
    return (
        <Suspense fallback={<div className="editor-loading">Loading editor…</div>}>
            <MonacoEditor {...props} />
        </Suspense>
    );
}
