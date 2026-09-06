// Monaco glue for the Lua editor: completion and hover, from the engine's own
// API description.
//
// Everything here is the mapping onto Monaco's enums and callback shapes. What
// is offered, in what order, with what text, lives in luaApi.ts as pure
// functions -- so the decisions are exercised by the smoke run and only the
// wiring is untested.
import type { Monaco } from "@monaco-editor/react";
import { documentation, findGlobal, luaSuggestions } from "./luaApi";
import type { LuaGlobal } from "./luaApi";

// Providers are registered against the Monaco instance, not against an editor,
// so mounting the editor a second time (switching tabs, remounting the pane)
// must not register them again -- Monaco would then offer every completion
// twice, which reads as the app being broken.
let registered = false;

export function registerLuaLanguageSupport(
    monaco: Monaco,
    getApi: () => LuaGlobal[],
): void {
    if (registered) return;
    registered = true;

    monaco.languages.registerCompletionItemProvider("lua", {
        provideCompletionItems(model, position) {
            const word = model.getWordUntilPosition(position);
            // The range the accepted text replaces: the partial word being
            // typed, so completing `sm` gives `sma(...)` and not `smsma(...)`.
            const range = {
                startLineNumber: position.lineNumber,
                endLineNumber: position.lineNumber,
                startColumn: word.startColumn,
                endColumn: word.endColumn,
            };
            const kinds = monaco.languages.CompletionItemKind;
            return {
                suggestions: luaSuggestions(getApi()).map((s) => ({
                    label: s.label,
                    kind:
                        s.kind === "value"
                            ? kinds.Variable
                            : s.kind === "snippet"
                              ? kinds.Snippet
                              : kinds.Function,
                    detail: s.detail,
                    documentation: { value: s.documentation },
                    insertText: s.snippet,
                    insertTextRules:
                        monaco.languages.CompletionItemInsertTextRule
                            .InsertAsSnippet,
                    sortText: s.sortText,
                    range,
                })),
            };
        },
    });

    monaco.languages.registerHoverProvider("lua", {
        provideHover(model, position) {
            const word = model.getWordAtPosition(position);
            if (!word) return null;
            const g = findGlobal(getApi(), word.word);
            if (!g) return null;
            return { contents: [{ value: documentation(g) }] };
        },
    });
}
