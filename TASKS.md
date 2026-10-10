# Task queue

The unattended agent works this file top-to-bottom. See the protocol section of
`CLAUDE.md` for the rules it follows.

**Status values:** `todo` · `in-progress` · `done` · `blocked` · `needs-user` ·
`parked`

The agent takes only `todo`, and only from the live queue. `needs-user` needs
you; `parked` is deliberately out of scope until the beta ships, and stays out
even when the queue above is empty — see the rule below.

Finished work lives in **`TASKS-ARCHIVE.md`** (122 blocks, verbatim; last move 2026-10-07). The
per-session narrative lives in `AGENT_LOG.md`.

## The rule for this file, until the beta ships

**Only work that gets a stranger to a correct first result belongs in the live
queue.** The engine is finished for beta purposes: sweeps, walk-forward,
significance, robustness, regimes, shorting, margin, costs, cashflows,
fundamentals, hypotheses, bundles and the research log all shipped. What is not
finished is everything around it — the app only runs inside its own checkout,
against its author's database.

So a new capability is not a beta task, however good it is. Anything that adds
surface area goes to **After beta** below, and nothing there gets started until
the beta is out. Adding features during a beta is how a beta slips; that is the
whole reason this file was cut down on 2026-09-05.

Thirteen tasks were parked that day, none deleted — they are listed with the reason
each was parked, and every one of them can be pulled back up in a minute.

**One deliberate exception, decided by the user on 2026-09-27: strategy rules
(T133–T136) are a beta task.** The user's goal is software "used by as many
people as possible", and the finding behind it is that the assistant writing
*Lua* for a beginner is the step that fails them: they cannot check code they
did not write. A strategy the assistant fills in as rules, and the app reads
back as a sentence, is what gets a stranger from an idea to a correct first
result — which is the test above, so this passes it. It is new surface, and
that is accepted knowingly. It does **not** reopen the parked list: T108–T112,
T48 and T110 stay parked, and T133–T136 are scoped so they need none of them.

---

## Regression gate — run this after EVERY task, before marking it `done`

This is not a task and is never consumed; it is the exit condition for all of
them. A feature that works is not finished — a feature that works *and breaks
nothing else* is. Passing your own new test proves only that you wrote a test
that passes.

Run all of it, in order, from the repo root:

```bash
go build ./...
go vet ./...
go test ./... -count=1        # full suite (~50s). NOT -short.
```

- **`-count=1` is mandatory.** Without it Go serves cached results, and a
  cached `ok` from before your change is exactly the regression you are
  looking for. A green run you did not actually execute is worse than no run.
- **Never `-short`** in the gate. `-short` skips the slow UI and end-to-end
  tests, which are the ones that cross package boundaries and so the ones most
  likely to catch a regression. Use `-short` while iterating, never to finish.
- **UI-facing change** (anything under `ui/`, including `appReference`) also
  needs `cd ui/frontend && npm run build`.
- **A component that renders must be EXERCISED, not just compiled.**
  `npm run build` is `tsc` plus a bundle; neither has ever mounted a component,
  and that is how T90 shipped a black screen through a green gate. Two checks
  now cover it, both inside `go test ./...`, so the commands above are still
  the whole gate:
  - `TestNoConditionalHooks` (`ui/frontend_hooks_test.go`) reads every `.tsx`
    and rejects a hook that is nested in a block or sits below an early
    return. That is the T90 bug, refused by construction.
  - `TestFrontendSmokeRender` (`ui/frontend_smoke_test.go`) shells out to
    `npm run smoke`, which renders `ResultsView` (and the Backtest form) through
    `react-dom/server` against four fixtures — full, plain, no-curve, empty —
    at both zoom extremes, and diffs `buildToml`/`buildHypothesis` against
    checked-in goldens. It skips (not fails) when `ui/frontend/node_modules`
    is absent.
  - The fixtures are generated from the Go structs by
    `TestResultFixturesMatchTheWireShape`; if it says they are stale, the wire
    shape moved. Regenerate with `go test ./ui -run TestResultFixtures
    -update`, refresh the goldens with `SMOKE_UPDATE=1 npm run smoke`, and
    **read the diff** — a changed golden means every config the form generates
    changed.
  - What this still does not cover: anything needing a real DOM — click
    handlers, `localStorage`, the empty→populated re-render. That is T91, and
    it needs a test runner.
- **UI-facing change:** also walk the **UI standard** below, item by item,
  and say in the log that you did.
- **Touched `runner.go`, goroutines, or shared state:** also
  `go test ./src/backtest/ -race -count=1`. The worker pool is the one place a
  regression can be invisible on a green single-threaded run.

### Reading the result

Compare against the baseline, not against zero. Before starting a task you
know the suite is green — that is the protocol's one rule. So:

- **A test you wrote fails** → your code or your test is wrong. Normal work.
- **A test you did NOT write, which passed before, now fails** → that is a
  regression, and it is the finding. Do not "fix" that test to make it pass.
  Do not weaken its assertions, loosen a tolerance, or add a skip. Either your
  change is wrong, or the old expectation was genuinely wrong — and deciding
  it was the latter is a `needs-user` call, not yours. See "When a new test
  fails because the production code is wrong" in `CLAUDE.md`.
- **Numbers moved but nothing failed** → still a regression if the change was
  not supposed to move them. Metric values, equity curves, and fill prices are
  the app's whole output; a config field that defaults to something other than
  the previous behavior silently changes every existing config's results.
  New config fields must default to a zero value that reproduces the old
  numbers exactly.

Record the outcome in `AGENT_LOG.md` — say which commands you ran and that
they passed. "Verified" with no commands named is not a record of anything.
If the gate is red after a genuine attempt, revert per protocol step 6; a
reverted task honestly logged is worth more than a green-looking broken tree.
---

## UI standard — every UI-facing task meets it before `done`

Added 2026-10-07 at the user's request: *"I want the UI to be standardized
… for all new features to ensure the UI stays consistent with established
patterns."* Like the regression gate, this is not a task and is never
consumed. A view that works but invents its own look is not done. When a
pattern below does not fit, extend the shared piece (one place, every view
benefits) rather than writing a one-off, and say so in the log.

**Use the shared pieces, never a local copy**

| Need | Use | Lives in |
|---|---|---|
| Any list of records | `ResearchTable` with `Column`s: every column with a `sortValue` sorts; dates and amounts `firstDir: "desc"`; numbers `numeric: true` | `ResearchTable.tsx` |
| Dates | `fmtDate` ("Jul 30, 2026"); a time, if one is ever shown, names its zone ("Time (ET)") | `researchFormat.ts` |
| Filing forms | `FormChip` | `Research.tsx` |
| A choice of 2–5 options | a `.research-toggle` group with `aria-label`, the chosen one `active` | — |
| Why something is missing | `Notes` (one sentence per missing source, naming the loader or key that fills it) | `Research.tsx` |
| An error | `.coverage-msg.err`, saying what failed and what still works | — |
| Remembered UI state | `readStored` / `writeStored` (they survive a blocked storage), never raw `localStorage` | `storage.ts` |
| Backtest results | `ResultPanel` (collapsible, pops out) — the results strip has its own established pattern; keep new results panels in it | `ResultPanel.tsx` |

**Rules**

1. **Tables, not prose or bullet lists, for anything with more than one
   record.** Sortable by every column that has a meaningful order. Text left,
   numbers right. A table with no rows renders its `empty` sentence, never
   an empty grid or nothing at all.
2. **Dates read as words, never ISO**, and a time always names its zone.
   Parse date strings by their parts (as `researchFormat` does), never with
   `Date.parse`, which shifts them into the viewer's zone. Anything in the
   future gets a countdown.
3. **Colour has a meaning or it is not used.** Theme tokens only (`--text-1..3`,
   `--bg-*`, `--accent*`, `--pos`/`--neg`/`--warn`, `--fs-*`, `--radius-*`), so
   both themes keep working. Green/red only where up is plainly good (a
   return, a price move); a statistic that can be good or bad either way
   (unemployment, CPI) gets a neutral ▲/▼.
4. **Every view says where its data came from and as of when**, in a
   `.research-dim` caption under it. Primary sources get a badge. A number
   from the database is never presented as live.
5. **Never a blank view.** A missing table, key or network answer is a note
   next to everything that still works.
6. **Interaction:** anything clickable is a `<button>` or a `ResearchTable` row
   (which takes Enter); a link that leaves the app goes through
   `BrowserOpenURL`/`openExternal` and ends in "↗"; a new tab or view gets a
   palette entry and, if it is a tab, a shortcut in `shortcuts.tsx`.
7. **Copy:** sentence case, plain words, no jargon the README does not define.
   Write any apostrophe in JSX text as `&rsquo;` — `TestNoConditionalHooks`
   reads a bare one as a string quote and loses its place (T144).
8. **Proof:** every new or changed view gets a `smoke.tsx` case rendering it
   from a fixture the Go side wrote, asserting the formatted text (not raw
   values), plus an empty or failed case. Bindings come from
   `wails generate module`, never by hand, and the task ends with a real
   `wails build -tags webkit2_41`.

**Known exceptions, not to be copied** — tracked as T145: the results
strip's tables in `ResultsView.tsx` are hand-built `<table>`s with their own
`fmtDate`/`fmtPct`, and do not sort.

---

## Beta release — the live queue

Ordered by what blocks what. T116 and T117 decide whether the thing a user
downloads works at all. T120–T123 decide whether someone who has never used a
backtester gets a result out of it — the engine is not the hard part for them,
the empty form is. Everything after that is honesty and release mechanics.

| id | task | status |
|---|---|---|
| **T88/T103** | Refresh the stale series *before* snapshotting the DB | needs-user (prices done) |
| **T133** | Strategy rules: `[Portfolio.Rules]`, checked and run by the engine | done 2026-10-06 |
| **T134** | The assistant writes rules, and checks them before proposing | done 2026-10-06 |
| **T135** | Describe your idea → read it back → run it, in the Backtest tab | done 2026-10-06 |
| **T136** | The starter examples and library, restated as rules | done 2026-10-06 |
| **T119** | Install the package on a clean machine and run it | needs-user |
| **T139** | Strategies read SEC values (2009+), not just SEC dates | done 2026-10-03 |
| **T141** | UI cleanup: nice and fast (shortcuts, panes, palette, speed) | done 2026-10-06 |
| **T142a** | Command bar + DES / FA / ERN | done 2026-10-06 |
| **T142b** | EDGAR source + CN (company news) + reader | done 2026-10-06 |
| **T142c** | ECO: economic releases and calendar | done 2026-10-07 |
| **T142d** | Watchlist + MY | done 2026-10-07 |
| **T144** | Research tab: every view a sortable table; filing times date-only | done 2026-10-07 |
| **T145** | Bring the results strip onto the UI standard (shared formatting; sortable tables) | todo |
| **T143** | EDGAR acceptance times are 4h early for half of all filers (CN, MY) | needs-user |
| **T142e** | Full-text search of filings | done 2026-10-07 |
| **T142f** | Assistant tools over the same sources | needs-user (context budget) |

Finished tasks dated before 2026-09-30 (T116, T117, T120–T132, T137, T50, T99,
T118) were moved to `TASKS-ARCHIVE.md` on 2026-10-07. T138 and T140 (done
2026-09-29) never had blocks here, so only their table rows were dropped; their
record is in `AGENT_LOG.md`.

---

## T88/T103 — Refresh the stale series *before* snapshotting the DB

**Category:** data · **Status:** needs-user · **merged 2026-09-05; full history in the archive**

Both were backlog items while the database was yours alone. Shipping it makes
them a release step with an ordering constraint: whatever you snapshot is what
every beta user has, permanently.

```
$SP500, $USTOT, $CASH …   last bar 2026-07-02      prices  2026-08-07
economic_indicators CPI   last row 2026-02-01
```

- Benchmark-relative metrics — alpha, beta, tracking error, information ratio,
  up/down capture — are dropped entirely for any window the benchmark does not
  fully cover. The Backtest form sets `Benchmark = "$SP500"` by default, so
  **every beta user's default run silently loses the comparison.**
- Real (inflation-adjusted) return is blank past the last CPI reading, which is
  every window ending after February.

**The price half is now automated** (commit `0347358`, 2026-09-07): the Data
tab has a **Sync now** button and the assistant has a `sync_data` tool, which
bring every tradable ticker to the latest session in one action — batched, tail
only, resumable. Prices no longer need a loader run at all.

**Still yours**, because these are write-mode loaders against the single-writer
database and the app must be closed:

```
python3 update_treasury_yields.py     # 3M/10Yr stop 2026-05-21
python3 add_collections.py            # $SP500, $CASH, … stop 2026-07-02
```

Worth checking `FEDFUNDS`, `GDP`, `INDPRO`, `M2SL` and `UNRATE` while you are
there — `economic_indicators` stops 2026-02-01.

**Also yours, and cheaper to decide now than after the download link exists:**
whether the beta ships the whole 3.26 GB file or a pruned universe.

---

## Strategy rules — T133–T136

Added 2026-09-27 from the request: *"My goal for the backtester is to be easy
to use for the common user … Ideally the AI assistant should derive the
strategy based on the user's ideas."* Included in the beta by the user's
decision; see the exception under *The rule for this file*.

**The problem.** Today an idea becomes a strategy in one of two ways: pick one
of eleven library scripts, or have the assistant write Lua. The second is the
one a beginner needs, and it fails them at the one step that matters —
*checking that the AI understood them*. A user cannot tell whether
`rank > 0.8` should have been `< 0.2`, or whether a lookback reads tomorrow's
close. Swapping Lua for another language fixes none of that (weighed
2026-09-27: Starlark is the best fit if the language is ever changed, and it
would be a new Go dependency; it is not part of this group).

**The idea.** A strategy is a small set of **rules** chosen from a fixed menu
the engine implements once, in Go, on point-in-time data. The assistant fills
the rules in; the app **reads them back as a sentence**; the user confirms the
sentence, not the code, and only then runs it:

```toml
[Portfolio.Rules]
Rebalance = "monthly"                 # daily | weekly | monthly | quarterly | annually
Rank      = "return_126"              # a signal from the menu
Pick      = 3                         # hold the top N by Rank
Keep      = 5                         # …and keep a holding while it stays in the top 5
Filter    = "above_sma_200"           # optional: a name must pass this to be held
Weight    = "equal"                   # equal | inverse_vol_63 | allocations
RiskOff   = "benchmark_below_sma_200" # optional: whole book to cash while true
```

> *"Each month, hold the 3 of these stocks that rose most over the last 126
> trading days, equally weighted, but only those trading above their 200-day
> average; keep a holding while it stays in the top 5. Go to cash whenever the
> S&P 500 is below its 200-day average."*

What that buys, and why each matters to a beginner:

- **Look-ahead is impossible by construction.** Every signal is written once in
  Go against data up to the decision day. The assistant cannot write a bug that
  peeks, because it writes no code.
- **Small local models can do it.** Filling a schema is far easier than writing
  correct Lua; qwen3.5:4b-class models (the user runs these) must be able to
  produce valid rules from the lean prompt.
- **Nothing the user reads is ambiguous.** One Go function renders every rule
  set as a sentence, and the UI, the assistant and the results page all use it,
  so the three cannot describe one strategy differently.
- **Lua stays** as the escape hatch for anything the menu cannot say.

**Scope limits — deliberate, so this needs nothing parked:**

- **The universe is the portfolio's own `Tickers`** (typed, a starter basket,
  or the assistant's pick). "S&P 500 members on each date" is T110/T48 and
  stays after beta. The shared-window invariant is untouched.
- **Price-based signals only.** Fundamental signals (`earnings_yield`,
  `book_to_price`, …) wait for T108, and cannot ship before T112 is fixed:
  today's `market_cap` is built from a dividend-adjusted close, so a
  fundamental rule would compute a wrong number that looks right.
- **No sweeping over rule fields** in this group. A `Sweep` key that targets
  `Rules` is refused by `CheckConfig`, never silently ignored.

**The menu, v1** (each implemented once, each with a plain-English phrase for
the sentence; `N` is a number of trading days, validated to a sane range):

| kind | entries |
|---|---|
| rank signals | `return_N`, `momentum_12_1`, `low_vol_N` (lower is better), `rsi_N`, `drawdown_N` (distance below the N-day high) |
| filters | `above_sma_N`, `below_sma_N`, `sma_cross_S_L` (S-day above L-day), `rsi_below_X`, `rsi_above_X` |
| risk-off | `benchmark_below_sma_N`, `benchmark_drawdown_over_X` |
| weights | `equal`, `inverse_vol_N`, `allocations` (the form's percentages) |

The menu is served to the UI and the assistant by the engine (the way
`CriterionMetrics` already serves the hypothesis vocabulary), so neither can
offer an entry the engine would refuse.

---

## T133 — Strategy rules: `[Portfolio.Rules]`, checked and run by the engine

**Category:** feature · **Status:** done 2026-10-06 · **touches the TOML schema (authorised by this task)**

The engine half. No UI and no prompt work here beyond what the definition of
done requires.

1. **Types and parsing** in `src/backtest/config.go`: an optional
   `Rules *RulesConfig` on the portfolio. **Absent means bit-identical to
   today** — the zero value is inert. `Rules` together with `Strategy` is a
   config error that says to pick one.
2. **Validation in `CheckConfig`**: an unknown signal is refused with the
   valid forms listed and the nearest match suggested (`retrun_126` → "did you
   mean return_126?"); `Keep < Pick`, `Pick` larger than the ticker count,
   `RiskOff` with no `Benchmark`, and a warm-up (largest `N`) longer than the
   window are all refused in plain words. `✓ Check` and `-check` report them
   with no code change, because they already route through `CheckConfig`.
3. **Execution**: a Go-native rules strategy on the **same order path as
   Lua's `target_weights`** — costs, cash, shorting refusal, delisting and the
   blotter all behave identically, because it is the same path. A warm-up
   period holds cash, and says so once as an `info` diagnostic.
4. **`DescribeRules(RulesConfig) string`**: the one sentence renderer. Pure,
   deterministic, and exhaustive over the menu — a table test asserts every
   menu entry has a phrase, so adding a signal without one breaks the build.
5. **`RulesVocabulary()`**: the menu, with each entry's parameter ranges and
   phrase, for the UI and the assistant.

**The test that matters — equivalence.** For each library script the menu can
express (at least `momentum_rotation`, `trend_following`, `rebalance`), a
rules config and the Lua script must produce the **same equity curve to the
cent** on the fixture data. That pins signal definitions, fill timing and
rebalance dates against code that has already been reviewed; a rules engine
that disagrees with its Lua twin is wrong until proven otherwise, and the
disagreement is the finding.

**Done means:** the five parts; the equivalence tests; a look-ahead test
(a signal computed on day *t* must be unchanged when bars after *t* are
altered); `PortfolioConfig`'s new field in `appReference` (enforced by
`TestSystemPromptDocumentsConfigSchema`); a `README.md` section with the menu
table and the sentence for the example above.

**Effort:** large — the biggest task in the group; do it first, everything
else reads from it.

---

## T134 — The assistant writes rules, and checks them before proposing

**Category:** feature · **Status:** done 2026-10-06 · **needs T133**

1. **The prompt** (`appReference` full and lean) teaches rules as the
   **default** way to express an idea, with Lua only when the menu cannot say
   it — and says which: *"a rule set cannot express X, so here is a Lua
   script"*. The menu table goes in verbatim from `RulesVocabulary()` so the
   prompt cannot drift from the engine.
2. **A `check_rules` tool** (both providers; `TestProviderToolParity` must
   stay green): takes a rules block plus tickers and dates, runs
   `CheckConfig`'s rules validation and the coverage check, and returns either
   the errors or **the sentence from `DescribeRules`** and the window the data
   supports. The assistant must call it before showing a rule set, and shows
   the user the returned sentence — so what the user reads is the engine's
   rendering, never the model's paraphrase of what it thinks it wrote.
3. **Ask, don't guess, on the two things ideas usually leave out**: how often
   to rebalance and what to hold instead. The prompt says to propose a default
   *in the sentence* ("…checked monthly — say if you meant weekly") rather
   than silently choosing.

**Done means:** prompt and tool; a test that the lean prompt still fits the
Ollama context budget (`TestLeanPromptFitsOllamaContext`); a live test gated
like the others; `README.md`'s assistant section says it now writes rules
first.

**Effort:** medium.

---

## T135 — Describe your idea → read it back → run it, in the Backtest tab

**Category:** feature · **Status:** done 2026-10-06 · **needs T133, T134**

The beginner path this whole group exists for:

1. The strategy card's picker gains **"Describe my own idea…"** above the
   library entries. Choosing it opens the assistant with the portfolio's
   holdings and window already in context.
2. When the assistant proposes rules, the card shows **the sentence** large,
   with each rule beneath it as an editable control — dropdowns and number
   boxes filled from `RulesVocabulary()`, so the guards from 2026-09-27 apply
   (no free text reaches the engine; numbers are clamped to each entry's range;
   `Keep` cannot go below `Pick`). Editing a control re-renders the sentence
   immediately.
3. **Run is labelled with the sentence's promise** — "Run: hold the top 3
   by 6-month return" — so the confirmation *is* the button, not an extra
   dialog.
4. **"Open as Lua"** writes an equivalent script into the Lua tab for the user
   who outgrows the menu. Optional within this task; if cut, say so in the
   log.

**Done means:** the path end to end; `smoke.tsx` cases for the rules card in
its empty, proposed, edited and invalid states; `TestNoConditionalHooks`
green; the ticker-filter timing check still under budget (the card must not
re-filter the universe); a `README.md` walkthrough.

**Effort:** medium.

---

## T136 — The starter examples and library, restated as rules

**Category:** feature · **Status:** done 2026-10-06 · **needs T133**

The starter examples (T120) and the library scripts the menu can express are
restated as rules, so a beginner's first contact is the readable form. Each
keeps its `@works` / `@fails` text and gains its sentence. The Lua originals
stay in `strategies/` — the equivalence tests from T133 are what keep the two
in step. Examples that need something the menu lacks stay as Lua and say so.

**Done means:** examples and gallery show the sentence; `buildExample` output
still `portfolioRunnable` against a realistic ranges table (existing smoke
case); goldens refreshed only where the change is intended, with the diff read.

**Effort:** small.

---

## T119 — Install the package on a clean machine and run it

**Category:** release · **Status:** needs-user · **the actual acceptance test**

Everything above is a prediction until someone runs the packaged build as a
user rather than as its author. On a machine (or a fresh user account) with no
checkout, no `.env`, no Python, and no `stock_data.db` outside the package:
install it, open it, pick five tickers, run the default ten-year buy-and-hold,
and check that the equity curve, the `$SP500` comparison and the Real % column
all populate.

**Yours because** it needs a second machine or account, and because the only
useful version of this test is one where the person running it cannot fix the
environment from memory.

Whatever it turns up is the real beta list. Everything before it is what I can
see from inside the repository.

---

## T141 — UI cleanup: nice and fast

**Category:** ui · **Status:** done 2026-10-06 (user: "UI looking much better — go ahead and work on the news work")

The user's goal (2026-10-04): the app is to be *the* place they go for their
investing — "elegant, powerful, and user friendly". Before anything new is
added, the existing UI has to feel finished and fast. Small, incremental
passes, each shipped whole (smoke case, README, `appReference`).

Done so far (uncommitted in the working tree):
- keyboard shortcuts in one table (`ui/frontend/src/shortcuts.tsx`), F1 help
  overlay, ⌨ chip; Cmd on a Mac, labels say so;
- Ctrl/Cmd+Shift+P command palette (`commandPalette.tsx`), Ctrl+K alias;
- results start minimised until the first run; double-click the results bar
  cycles min → normal → max; double-click the assistant header maximises it;
- Ctrl/Cmd + = / - / 0 zoom; "/" focuses the ticker search.
- **speed, first measured win**: Monaco moved out of the startup bundle
  (`monacoSetup.ts` + `LazyEditor.tsx`, preloaded 1.2 s after first paint).
  Main JS 4.0 MB -> 263 KB, main CSS 181 KB -> 38 KB. Verified in the real app
  (Hyprland `send_shortcut` Ctrl+2 -> editor loads with its theme).
- **open: 4K full-screen click offset** on Hyprland at fractional scale 1.67
  (native Wayland, WebKitGTK 2.52). Not reproducible without input injection
  (`uinput` module not loaded); waiting on the user's A/B of
  `BACKTESTER_GPU=on`, `WEBKIT_DISABLE_DMABUF_RENDERER=1`, `GDK_BACKEND=x11`.

Still open — the bar for "done" is that nothing feels slow or unfinished:
- **speed**: measure first (startup to interactive, tab switch, a keystroke in
  the form with results showing, opening the results with a large run) and fix
  what the numbers say, not what looks slow in the code;
- visual consistency (spacing, empty states, focus rings, button hierarchy);
- anything a pass finds that a first-time user would trip on.

**Done when** the user says the UI is where they want it. T142 does not start
before then.

---

## T142 — Research: primary sources, searched like a terminal (replaces the old quotes + headlines T142)

Decided with the user 2026-10-06. **The goal is a noise-free way to get the
information a user wants** — "too many adverts in modern news sources and
misleading information … something akin to Bloomberg search functionality but
in limited scope". So: **no live quotes** (dropped by the user), **no media
headlines**, and **wire-service press releases left out for now** (agent's
default, since the user did not choose; trivially added later as an allow-list).

Principles every sub-task keeps:
- **Primary sources only**: the company's own filing, the government statistic,
  the Fed statement. Every item carries a source badge (Filing / Official
  statistic / …) so the user always knows what they are reading.
- **Read in-app as clean text**: primary documents are fetched, stripped to
  text (no scripts, trackers or ads) and shown in a reader, with a link out.
- **Numbers come from data, not prose** (FRED values, XBRL figures).
- **The assistant summarises only what it can cite**, each claim linked.
- One `Source` interface (fetch, normalise, cache); one source failing never
  blanks a view. Tests use recorded fixtures, never the network. Nothing is
  written to `stock_data.db`. SEC requests carry `SEC_CONTACT` and stay under
  10 req/s.

Each sub-task lands whole (engine + README + appReference + smoke + gate), and
any new App binding is regenerated with `wails generate module`, then verified
with `wails build -tags webkit2_41` (see AGENT_LOG, 0c1a410).

## T142a — Command bar + DES / FA / ERN over tables we already have

**Category:** feature · **Status:** done 2026-10-06

A Bloomberg-style lookup reachable from the command palette: type a ticker
then a function. `AAPL DES` — what the company is (`company_profile`: name,
sector/SIC, exchange, former names; `index_membership`: in the S&P 500 since;
first/last bar). `AAPL FA` — reported financials by period from
`sec_financials`, each figure with the date it was FILED. `AAPL ERN` — report
dates from `earnings_calendar`. A bare ticker opens DES. No new data, no
network: read-only views over existing tables. Every table is optional — a
missing one says so in the view instead of erroring.

## T142b — EDGAR source + CN (company news) + reader

**Category:** feature · **Status:** done 2026-10-06 · **needs T142a**

`AAPL CN`: the company's recent filings from EDGAR `submissions/CIK*.json`
(keyless), 8-Ks labelled by item (2.02 earnings, 5.02 officers, 1.01/2.01
deals, 1.03 bankruptcy), Form 4 insider trades, 13D/G stakes. Open one in a
reader pane: the primary document fetched and stripped to text. In-memory TTL
cache. Ticker→CIK via `company_profile`.

## T142c — ECO: economic releases and calendar

**Category:** feature · **Status:** done 2026-10-07 (34c284f) · **needs T142a**

Recent readings from `economic_indicators` and FRED's keyless CSV (actual vs
prior, with the period each describes and roughly when it was published),
upcoming release dates (FRED releases API needs `FRED_API_KEY`, already used by
`update_treasury_yields.py`; without it, say so), FOMC meeting dates.

## T142d — Watchlist + MY

**Category:** feature · **Status:** done 2026-10-07 (1f0943a) · **needs T142b**

A saved watchlist (app config dir, not the DB) seeded from the form's holdings;
`MY` runs CN/ERN across it, newest first.

## T144 — Research tab: every view a sortable table

**Category:** ui · **Status:** done 2026-10-07 (2f78287, 98b39d4, adf562d, 8dd9684) · **user request 2026-10-07: "tabular format for earnings and such"; chose sortable everywhere, date-only filing times**

A shared `ResearchTable` (sortable headers, numbers right-aligned, sticky
header, zebra rows, clickable rows, empty state) and shared formatting (dates
as "Jul 30, 2026", times "4:30 pm ET", countdowns, timing tags, move bars,
form chips by kind). Four commits: (1) table + formatting + ERN with a
next-report panel and summary; (2) MY as three tables — upcoming earnings,
recent earnings, filings; (3) CN and search as tables, search's order toggle
replaced by sortable columns with a relevance rank; (4) ECO meetings table and
sortable readings, FA sortable, DES key-facts tiles. **Filing times are shown
as dates only until T143 is decided** — earnings times come from the
database and stay.

## T145 — Bring the results strip onto the UI standard

**Category:** ui · **Status:** todo · **from the UI standard, 2026-10-07**

`ResultsView.tsx` predates the standard: eight hand-built `<table>`s, and its
own `fmtDate`, `fmtPct`, `fmtMoney`, `fmtDollars`. Move its date and percent
formatting onto `researchFormat.ts` / `researchCommand.ts` (extending them
where a results format genuinely differs) and make its record tables sortable
through `ResearchTable`, keeping `ResultPanel` as the frame. The metrics table's
column chooser and the benchmark/baseline rows must survive unchanged.
**Numbers on screen must not move**: same values, same precision — diff the
smoke output before and after. Also: teach `stripNoise` in
`ui/frontend_hooks_test.go` that JSX text is not JavaScript, so the
`&rsquo;` rule in the UI standard can be retired.

## T143 — EDGAR acceptance times are 4 hours early for half of all filers

**Category:** bug · **Status:** needs-user · **found 2026-10-07 during T142d's live check; touches T142b's code**

`edgar.acceptedInstant` assumes the submissions API's `acceptanceDateTime` is
always the Eastern time shifted by twice the UTC offset, and undoes one
conversion. That holds for some companies and not others. Checked against
EDGAR's own `-index.htm` "Accepted" line on 2026-10-07:

| filing | API `acceptanceDateTime` | index page (ET) | API is |
|---|---|---|---|
| AAPL 0000320193-26-000018 | 2026-07-31T00:30:28Z | 2026-07-30 16:30:28 | shifted +8h |
| JPM 0000019617-26-000288 | 2026-07-24T00:15:21Z | 2026-07-23 16:15:21 | shifted +8h |
| MSFT 0001193125-26-323632 | 2026-07-29T20:04:53Z | 2026-07-29 16:04:53 | **true UTC** |
| NVDA 0001045810-26-000073 | 2026-08-26T20:21:19Z | 2026-08-26 16:21:19 | **true UTC** |

So CN and MY show Microsoft's and NVIDIA's after-close earnings at ~12:00 ET.
It is not the filing agent (JPM's agent-filed 8-Ks are shifted, MSFT's
agent-filed ones are not) and not the time of day. `filingDate` can expose a
shift only when the wrong time crosses 17:30 ET, so it is not a general test.

Options, for you to choose: (1) read "Accepted" from each filing's index page
— exact, but one extra request per filing shown (CN shows ~20, MY up to 300;
cacheable forever, filings never change); (2) show the date only, no time,
until a reliable source is found; (3) find per-company structure in the API
that predicts the shift (not found in one pass). `TestAcceptedTimesMatchEDGARsFilingPages`
pins only shifted filers, which is why the gate stayed green.

## T142e — Full-text search of filings

**Category:** feature · **Status:** done 2026-10-07 (f3fa77c) · **needs T142b**

`"going concern"` or `"buyback" AAPL` → EDGAR full-text search (keyless, 2001+),
results opening in the T142b reader.

## T142f — Assistant tools over the same sources

**Category:** feature · **Status:** needs-user · **needs T142b, T142c — all built; waiting on one decision**

`get_company`, `get_filings`, `get_eco` (both providers), summaries that cite
each item's link and say nothing the documents do not.

**The decision (raised under T142a and T142b, still open):** the local-model
context. Measured 2026-10-07: the lean prompt is 21,837 of its 21,844-char
budget (a third of `num_ctx` 16384), and the tool schemas sent alongside it
are another 5,789 chars that no test counts. `TestProviderToolParity` puts
every tool on Ollama too, so three more tools add roughly 3,000 chars (~750
tokens) to every local request, out of the conversation's share. Choose one:

1. **Add the tools; routing lives only in the full prompt** (the lean one
   needs no new words — the tool descriptions carry it). Accept ~750 fewer
   tokens of conversation on local models. *Agent's recommendation.*
2. Same, and also cut the lean prompt by ~3,000 chars so the total stays put
   — say which sections matter least to you.
3. Raise the budget and make the test count tool schemas too (e.g. prompt +
   tools ≤ 45% of `num_ctx`), so the true cost is measured from now on.
4. Claude only — breaks the parity rule and the "local models are
   first-class" principle; not recommended.

Also open from T142d: whether CN's default view should drop 13D/13G (see the
T142d log; MY already does), and T143 (EDGAR times).

---

## After beta — parked, do not start

Nothing here is wrong or abandoned. Each one was in the live queue on
2026-09-05 and was parked for the reason given, because none of them get a
stranger to a correct first result. Their full task blocks are kept below this
table, so pulling one back up is a matter of moving its block into the queue
above.

| id | task | why parked |
|---|---|---|
| **T108** | A fundamental figure that means one thing | **Done inside T139 (2026-10-03)**: the optional frequency argument, plus `periods_back`. TTM is still not built. |
| **T109** | Score a factor before backtesting it | New analysis surface, built on T108. |
| **T111** | `factor_screen.lua`: hold the top names by a reported ratio | Largely covered by T139's `quality_roe.lua` / `gross_profitability.lua` / `strong_balance_sheet.lua` (price-free ratios only, per T112). A generic `metric =` screen is still not built. |
| **T110** | A universe, not a ticker list | A config-schema change during a beta, and it needs T108. |
| **T112** | A market cap built from a dividend-adjusted close is not a market cap | Real correctness bug, but only reachable through the fundamentals path a beta user will not take. Now disclosed in `DATA-LICENSE.md`. |
| **T48** | Point-in-time universes in the config | Same schema-churn reason as T110. |
| **T20** | Survivorship: the universe has no delisted companies | **Cannot be fixed** without paid data (CRSP/Norgate/Sharadar). Handled the only honest way available: disclosed in `DATA-LICENSE.md`. Keep the block for when the budget question comes up. |
| **T47** | Probe the free tiers that need a key | Sourcing research; only matters if T20 is ever funded. |
| **T35** | From verdict to positions: the paper-trading bridge | An entire second product. The clearest example of the drift this cleanup was for. |
| **T96** | `time.Time` reaches the frontend as `any` | Cosmetic. A runtime check already covers the hole. |
| **T8** | Standing complexity-reduction pass | Process task with no endpoint; it competes with shipping. |
| **T91/T87** | Frontend test runner | Largely solved by `smoke.tsx` running inside `go test ./...`. |

### Three small decisions, also parked

Carried from the old "Left explicitly for you" section. None blocks the beta;
all three are one-liners once decided.

- **Should `smaCross` be sweepable?** Its periods live in the spec string, so
  today it needs one `[[portfolio]]` block per setting. An ergonomics gap.
- **Should a bare `PValue` filter be refused?** An uncomputed `Significance`
  reports `PValue` 0, which passes `PValue < 0.05`. Documented and pinned by a
  test; refusing it changes config-validation semantics.
- **Zero `BuyingPower`** is accepted (negative is rejected), because `Deposit`
  makes starting empty legitimate. One line to change if you disagree.

---

## T108 — A fundamental figure that means one thing

**Category:** bug · **Status:** parked

`queryRawFundamentals` (`src/data/fundamentals.go:268`) selects `frequency` and
never filters on it, and `fundamentalLookup.at`
(`src/backtest/strategy_lua_fundamentals.go:82`) returns the newest point by
`KnownFrom` regardless of which frequency produced it. So
`fundamental(t, "Net Income", day)` can return a quarter's figure one day and a
year's the next, and every ratio built on it — P/E, ROE, margin — steps by
roughly 4x at each company's own fiscal year end. Silently, and on a different
date per company, which is the worst possible shape for a cross-sectional rank.

Balance-sheet metrics are snapshots and are unaffected. The hazard is flows:
Net Income, Total Revenue, Operating Income, EBIT, Free Cash Flow.

The `frequency` column is independently unreliable at fiscal year ends. AAPL's
2025-09-30 row labelled `quarterly` holds 112.01bn — the annual figure, not the
~27bn quarter — and 3,659 of the 8,405 (ticker, period) pairs that carry both
frequencies hold identical values. `sec_financials` derives frequency from the
`qtrs` field and does not have this defect, which is one more reason Stage 0
comes first.

The proposed shape, an optional fourth argument:

```lua
fundamental(ticker, metric, day)              -- unchanged: newest, any frequency
fundamental(ticker, metric, day, "annual")    -- newest annual figure only
fundamental(ticker, metric, day, "quarterly")
```

**The invariant:** the three-argument form must return exactly what it returns
today, bit for bit, so every existing config and every shipped `strategies/*.lua`
produces identical numbers. The new behaviour is opt-in, like every other
config field added since costs.

An unknown frequency string must be an error naming the valid ones, not a
fall-through to "any" — a typo that silently widens the query is precisely the
bug this task exists to remove.

A TTM form (four quarters summed) is the right denominator for a flow ratio and
is deliberately **not** in this task: with five rolling quarters on the current
database it would return nil for nearly every (ticker, day). Revisit it as its
own task after Stage 0.

**Done means:** the engine change; a test with a ticker publishing both
frequencies for the same metric, asserting the four-argument form ignores the
other and the three-argument form is unchanged; `strategies/README.md`; and
`appReference` in `ui/chat.go`, which documents the three-argument form as the
whole API.

**Effort:** small.

---

---

## T109 — Score a factor before backtesting it

**Category:** feature · **Status:** parked · **says nothing useful before Stage 0**

`signalscan.go` already makes the right measurement — cross-sectional Spearman
IC against forward returns, non-overlapping sampling, a minimum cross-section,
a quintile spread withheld below a wider universe. It just cannot see a
fundamental: `Signal` is `func([]data.AssetData, int) (float64, bool)`, bars
and an index and nothing else, so `BuiltinSignals()` is entirely price-based
and the whole value/quality space is unscannable.

Widen the signal signature so a signal can also read the point-in-time
fundamentals for that ticker on that date. `fundamentalLookup` and `Signal` are
both in package `backtest`, so this is internal plumbing and adds no dependency.
Keep the existing price signals working unchanged — either by giving them a
nil-safe wrapper or by carrying both constructors.

Then add the factors actually asked for, with the sign chosen so **positive
means expected outperformance**, which is the file's existing convention:

| signal | expression |
|---|---|
| `earnings_yield` | Net Income / market cap |
| `book_to_price` | Stockholders Equity / market cap |
| `roe` | Net Income / Stockholders Equity |
| `book_to_sales` | Stockholders Equity / Total Revenue |
| `equity_to_assets` | Stockholders Equity / Total Assets |

E/P and B/P rather than P/E and P/B deliberately: a price multiple is undefined
at zero earnings and its ordering is discontinuous through it, while the yield
is monotone and finite across the whole range.

**Both market-cap signals are compromised until T112 is resolved** — the
close is dividend-adjusted, so the denominator is understated for dividend
payers and biased in the same direction as the value factor. `roe`,
`book_to_sales` and `equity_to_assets` need no price and are unaffected;
start with those.

Every one of them must return `false`, not zero, when either side is unpublished
**and** when the denominator is non-positive. A company with negative equity has
no meaningful book-to-price, and ranking it as the cheapest name in the universe
is the textbook way a value screen fills up with broken balance sheets.

**Done means:** engine plus tests (including one asserting a signal cannot read
a figure published after the scored day — the Go layer guarantees it, the test
proves the new binding does not undo it), the signal-scan section of
`README.md`, and `appReference`.

**Effort:** medium.

---

---

## T110 — A universe, not a ticker list

**Category:** feature · **Status:** parked · **supersedes T48**

Two deliberate design decisions make a cross-sectional factor test impossible to
express, and a factor test is the case that needs them revisited:

1. **`Tickers` is a hand-typed list.** A factor test wants "every name that
   passed a liquidity filter on this date", not fifteen symbols someone chose —
   and choosing them is itself the selection bias the test is meant to avoid.
2. **The shared-window invariant.** `validateCoverage` (`runner.go:390`)
   refuses any portfolio whose window is not fully covered by every ticker, and
   `alignToWindow` (`runner.go:133`) keeps only the dates every ticker has a bar
   for. Together, a name that dies mid-window is refused outright and
   survivorship is built into the design rather than being a bias the run can
   measure. That invariant is right for a hand-built portfolio and wrong for a
   thousand-name cross-section.

Sizing, so the shape is chosen against real numbers: 6,047 tickers have full
price coverage 2021-01 → 2026-06, so an intersection at that size does exist.
Memory does not come free — ~6,000 tickers x ~1,380 bars x 56 bytes is ~0.5 GB
of bars *before* `alignToWindow` builds a per-ticker `map[int64]AssetData` and a
windowed copy per portfolio. Plan on capping the universe around 1,000–1,500
names, or on making the alignment cheaper.

The decisions, all of which are yours because they change what `Tickers` means:

- **The shape.** A `[portfolio.Universe]` block, a magic
  `Tickers = "$INDEX:SP500"` string (T48's suggestion), or a `Universe` key
  beside `Tickers`.
- **The filters.** `min_price`, `min_dollar_volume`, `top_n_by_dollar_volume`,
  and `index = "SP500"` via `index_membership` — already loaded, trustworthy
  back to ~2011, approximate to 2007.
- **Resolved once at config time, or per rebalance date?** Per-date is the
  honest answer and the expensive one.
- **What replaces the shared-window invariant.** A per-ticker presence mask — a
  ticker is tradable on the days it has bars, and the strategy sees which — is
  the real fix, and it is a change to the core simulation loop rather than a
  config feature.

T48 is the index-membership half of this same question. Whichever shape wins
should cover both, and T48 should be closed into it.

---

---

## T111 — `factor_screen.lua`: hold the top names by a reported ratio

**Category:** feature · **Status:** parked · **needs T108**

The strategy itself. It runs against a hand-typed `Tickers` list without T110,
and that is the useful first version — the universe version arrives later
without the script changing.

```toml
[[Portfolio]]
Strategy = "lua:strategies/factor_screen.lua"
Fundamentals = ["Net Income", "Stockholders Equity", "Total Revenue",
                "Total Assets", "Ordinary Shares Number"]

[Portfolio.Params]
metric         = "roe"    # roe | earnings_yield | book_to_price |
                          # book_to_sales | equity_to_assets
top_n          = 20
rebalance_days = 252
min_market_cap = 3e8
weighting      = "equal"  # equal | cap
```

Every global it needs already exists. The work is not the plumbing, it is the
guards — these are what decide whether the result means anything, and each one
is a way a factor screen is known to lie:

- **nil is exclusion, not zero.** A company that has not reported is not a
  company earning nothing. This is why `fundamental()` returns nil (T33).
- **Exclude non-positive denominators** rather than ranking them. Negative
  equity is not deep value.
- **Refuse to rebalance below a minimum cross-section.** `MinCrossSection = 5`
  in `signalscan.go` is the existing precedent; ranking four names is not a
  screen.
- **Winsorize the tail.** One 4,000x ratio off a near-zero denominator
  otherwise owns the top decile by itself.
- **Make the rebalance cadence a parameter and default it long.** With annual
  fundamentals a 21-day rebalance churns the same ranking through transaction
  costs and calls the loss a result.
- **Default `metric` to a price-free ratio.** `earnings_yield` and
  `book_to_price` divide by a market cap that T112 shows is understated for
  dividend payers; `roe` does not.

Note for the tests: `strategies_library_test.go` globs the directory and runs
every script against synthetic price series with **no** database and no
fundamentals, so this one must place no trades and log nothing when every
`fundamental()` call returns nil. That is a real requirement, not a test
artefact — it is the same thing that happens on any window before 2021.

**Done means:** the script, carrying the four `@works` / `@fails` / `@sweep` /
`@baseline` header tags the gallery test requires; `strategies/README.md`; a
worked example in `README.md`; and `appReference`. Then run it through the loop
that already exists — pre-register the hypothesis, set `Benchmark = "$SP500"`,
sweep `top_n` and `rebalance_days`, walk forward. Expect `inconclusive` on
today's data; `MinFundamentalCoverage = 50` exists to say exactly that.

**Effort:** medium.

---

---

## T112 — A market cap built from a dividend-adjusted close is not a market cap

**Category:** bug · **Status:** parked · **every fix is a loader change**

`market_cap()` (`src/backtest/strategy_lua_fundamentals.go:150`) multiplies
`Ordinary Shares Number` by the close. The prices are loaded with
`auto_adjust=True` (`add_ticker.py:71`, `fetch_history.py:41`), so the close is
split **and dividend** adjusted.

The split half is sound, and it was checked: Yahoo restates the reported share
count onto the current basis to match. NVDA's FY2022 balance sheet carries
25.06bn shares — already post the 10:1 split — against a 2024-06-06 close of
$120.79 in `stock_data_optimized` rather than the ~$1,209 it actually traded at.
Both sides are adjusted, so the product is right.

The dividend half is wrong, and it is wrong in a direction that matters. A
back-adjusted close sits below the price the stock actually traded at by the
cumulative dividend factor since that date, so a market cap computed at a past
date is understated in proportion to what the company has paid out since, and
the understatement grows the further back the date is. KO's 2021-01-04 bar reads
$44.79. Feed that into E/P or B/P and every dividend payer looks systematically
cheaper than it was — **a bias pointing the same way as the value factor a
screen would be trying to measure**, which is the worst available way to be
wrong. T109 and T111 both define signals in terms of market cap and both inherit
this.

The adjusted series is *correct* for measuring returns; total return is what a
backtest should compound. It is wrong as a price *level*. Those are different
uses of the same column and only the second one is broken.

The database cannot repair it locally. `Adj Close` is populated for 46,032 of
34,988,956 rows and merely duplicates `Close` where it exists; both loaders pass
`actions=False`, so no dividend stream was ever stored; and there is no
dividends table.

Three ways out, cheapest first:

1. **Rank on the ratios that need no price.** ROE, ROA, book-to-assets and
   book-to-sales are two figures from the same filing and are untouched by this.
   It costs P/E and P/B and nothing else, and it needs no loader run at all —
   so it is the right default for the first pass of T109 and T111.
2. **Store the dividend stream** (`actions=True`) in its own table and
   reconstruct the unadjusted close from it. Additive: no existing number moves,
   and the price table is not rewritten.
3. **Re-fetch unadjusted prices into a second column.** Correct and expensive —
   ~35M rows against a 2.9 GB single-writer database, so the app must be closed
   and the agent must not run it.

Until one of 2 or 3 lands, any price multiple this app computes should say in
the same breath that it is biased cheap for dividend payers.

---

---

## T96 — `time.Time` reaches the frontend as `any`

**Category:** cleanup · **Status:** parked · **priority: low**.
The `any` hole is now covered by a runtime check instead — see below.

`wails generate` prints seven `Not found: time.Time` lines on every binding
regeneration (so on every Go change under `wails dev`). It has no TypeScript
mapping for `time.Time`, dumps its registry, and falls back to `any`:

```ts
export class Drawdown {
    // Go type: time
    start: any;
```

One line per unresolved field, and the count matches exactly — seven fields in
the bound surface:

| Struct | Fields |
|---|---|
| `backtest.Drawdown` | `Start`, `Trough`, `Recovered` |
| `backtest.PeriodReturn` | `Start`, `End` |
| `backtest.RollingPoint` | `Date` |
| `backtest.VerdictReport` | `JudgedAt` |

**Nothing is broken.** Go marshals `time.Time` to RFC3339 and the frontend
already treats these as strings: `Drawdown`'s three dates go through `fmtDate`
(`ResultsView.tsx:86`), which parses and rejects anything unparseable;
`PeriodReturn` renders from its `period` string, not `start`/`end`; `judgedAt`
is not rendered at all. Verified 2026-08-23 — no raw `time.Time` value reaches
the DOM.

### Why it is still worth a task

`any` switches off type-checking on those seven fields, and there is already a
trap in reach of it: **`SegmentStats.start` is a plain `string` in Go while
`PeriodReturn.start` is a `time.Time`.** Two fields, same name, different wire
types, and `tsc` can only check one of them. Nothing is wrong with either
today; that is the shape of bug this hides.

### The fix, and why it was not done on sight

Give the UI its own wire structs with `string` date fields and convert in
`ui/app.go`, the way `TickerDateRanges` already formats to `"2006-01-02"`
before it crosses. That is a **wire-shape change**: `ui/frontend/testdata/*.json`
will go stale and `TestResultFixturesMatchTheWireShape` will say so, which is
exactly what it is for. Regenerate the fixtures, re-run `npm run smoke`, and
check `fmtDate` still gets what it expects.

Weigh it honestly first: the conversion layer is real code that has to be kept
in step with four structs, bought against seven fields that are currently
correct and covered by a defensive `fmtDate`. Closing the hole may not be worth
opening the seam. Suppressing the log output alone is **not** a reason to do it.

### Done instead, at no wire risk (2026-08-23)

`no raw timestamp reaches the DOM` in `smoke.tsx` renders the full fixture and
fails if `T00:00:00Z` appears anywhere in the output. The fixture dates are all
midnight UTC, which makes them a reliable canary. That covers what `any` cannot
be trusted to catch — a `time.Time` rendered without `fmtDate` — without
touching the wire shape at all. Verified by rendering `d.start` raw and
watching it fail.

**Left undone deliberately**: the string-typed wire structs. It is a real
change to `RunOutput` for no runtime benefit, and the user asked for anything
doubtful to be flagged rather than done.

---

---

## T8 — Standing complexity-reduction pass

**Category:** cleanup · **Status:** parked · **recurring — keep this task last**
· iteration 16 surveyed on 2026-08-20 after the T27 series and applied one finding; **streak 0 of 3**

A survey-then-apply loop that runs one small cleanup per iteration. Keep this
block at the bottom of the file: it re-arms itself, so anywhere above T1–T6
would starve them.

### Per iteration

1. **Survey.** Run `gofmt -s -l ./src ./ui` and `go vet ./...`, then read for
   the signals below. Append every finding — applied or not — to
   `analysis/complexity_findings.md` with file, line, and one line on why it is
   worth changing. That file is git-ignored; it is a notebook, not a
   deliverable.
2. **Apply exactly one** finding from the allowlist. One per iteration, no
   batching: a cleanup commit that touches six files cannot be reviewed. One
   *mechanical* change applied repo-wide counts as a single finding — running
   `gofmt -s -w` across every file it flags is one commit, not one per file.
3. **Verify and commit** per the normal protocol. Message form
   `agent(T8): <what changed>`.
4. **Re-arm.** Set this task back to `todo` so it runs again next iteration.
5. **Terminate.** If three consecutive iterations survey and find nothing on
   the allowlist, set status `done`, note it in `AGENT_LOG.md`, and stop
   re-arming. Track the streak in the log so it survives a crash.

### May change (allowlist)

- Anything `gofmt -s` flags, and anything `go vet` flags.
- **Dead struct fields** — declared and written but never read.
- Redundant statements: a value assigned then immediately overwritten,
  `if c { return true } else { return false }`, `err != nil` checked twice on
  the same call.
- A literal block duplicated 3+ times where a helper for it *already exists* —
  call the helper. Do not invent a new abstraction to deduplicate two sites.
- Nested conditionals that inverting into guard clauses flattens, with no
  change in behavior.
- Comments that contradict the code they sit on.

### Must not touch (denylist)

- **Complete-but-unused methods.** These are deliberate — the user prototypes
  with them. Unused *fields* are fair game; unused *methods* are not. Never
  delete one to reduce a line count.
- Metric formulas in `metrics.go`, the TOML config schema, the worker pool and
  channel choreography in `runner.go`, and the Lua API surface in
  `strategy_lua.go` — that last one is a public contract for `strategies/*.lua`.
- Moving code between packages, splitting or merging files, renaming exported
  identifiers.
- Anything needing a new dependency, and anything changing observable behavior.
- **Test logic** — assertions, cases, fixtures. Weakening or deleting a test is
  never simplification. Mechanical reformatting of a test file is fine; if a
  test looks redundant, file it as `needs-user` rather than removing it.

### Judgment

"Fewer lines" is not the goal and is a bad proxy for it. Change something only
when the result is easier for a human to be *confident* about. If a cleanup
requires a comment explaining why it is still correct, it was not a cleanup —
leave it and note it.

Anything genuinely worth doing but outside the allowlist — a function that
really is too long, a duplicated concept that wants a new abstraction — is not
yours to decide. Append it to this file as a new `needs-user` task describing
the change and its risk, and move on.


---

---

## T91 — The gate compiles the frontend; it has never once run it

**Category:** process · **Status:** parked
zero-dependency half is in and wired to the gate; **the DOM half is
`needs-user`** and still needs T87.

The regression gate's UI clause was `cd ui/frontend && npm run build`. That is
a type-check and a bundle. Every frontend regression this project has shipped —
T90 among them — passed it, because a component that is never mounted cannot
throw.

### Done, no dependency

`npm run smoke` (`ui/frontend/src/smoke.tsx`, run from
`TestFrontendSmokeRender` so `go test ./...` is still the whole gate) bundles
with the esbuild vite already vendors and renders `ResultsView` and
`ConsolePanel` through `react-dom/server` against four fixtures at two zoom
levels, plus golden-file diffs for `buildToml` and `buildHypothesis`. Proven in
both directions: green on the current tree, red when a component is broken.

Fixtures come from `TestResultFixturesMatchTheWireShape`, which reflect-fills
the real `RunOutput` and diffs it against what is checked in — so a renamed
`json` tag fails the suite instead of silently leaving the frontend tested
against a shape the app never sends.

### The DOM half, which still needs you

`react-dom/server` renders once, so three things remain untestable without
jsdom or happy-dom:

1. **Re-render transitions** — mount `ResultsView` with `results={[]}`, then
   re-render the *same* root with a populated result. This is literally the
   T90 crash. The static check now forbids the cause, but nothing tests the
   symptom.
2. **Event handlers** — clicking Run, collapsing a panel, dismissing the
   first-run card. All untested; all one typo from broken.
3. **`localStorage`** — the zoom level, the chat width, the open-panel set, the
   onboarding flag. T92 is a latent crash in exactly this code, and SSR cannot
   reach it.

Once T87 lands a runner, `npm run smoke` folds into it rather than being
replaced — the fixtures and goldens carry over unchanged.

---

---

## T47 — Probe the free tiers that need a key (the only paid-adjacent question)

**Category:** data · **Status:** parked

Everything above uses sources needing no signup. Two free tiers *might* serve
delisted price history — the one thing no free source has yielded so far — and
both need a key the agent cannot create. Both are free to obtain.

| source | the specific question | why it matters |
|---|---|---|
| **Tiingo** | does the free tier serve EOD bars for a delisted symbol, and does `supported_tickers.csv` carry a usable `endDate`? | if yes, this is the whole survivorship fix, free |
| **Polygon.io** | is `/v3/reference/tickers?date=YYYY-MM-DD` (point-in-time listings) in the free tier, and how deep is free aggregate history? | dated reference data would beat the Wikipedia reconstruction in T43 outright |

**What is needed from you:** a key for either or both in `.env`
(`TIINGO_KEY`, `POLYGON_KEY`). The probe itself is then half an hour of
read-only HTTP against five known-dead symbols (`LEHMQ`, `ENRNQ`, `SIVBQ`,
`SUNEQ`, `BBBYQ`) and the answer gets written into
`analysis/free_data_sources.md`, which already has a section holding the
unverified claims.

Also unresolved and yours: **Stooq**. Its bulk CSV endpoint now answers
non-browser clients with a JavaScript proof-of-work challenge. Solving it is
about fifteen lines of Python, but it is plainly an anti-automation measure, so
it is a terms-of-service decision rather than a technical one and the agent has
not attempted it.

If both free tiers come back negative, T20's paid tiers are the only remaining
path and that decision stays where it is — with you.

---

---

## T48 — Point-in-time universes in the config

**Category:** feature · **Status:** parked · **needs T43, T45**

The payoff of T43, and a TOML schema change, which the scope rules reserve for
you. Once membership is known, the natural way to spend it is to let a config
name a *universe as of a date* rather than a hand-typed ticker list:

```toml
Tickers = "$INDEX:SP500"      # membership resolved per rebalance date
```

so a 2005 backtest holds the S&P 500 of 2005 — including the members that later
died — instead of the survivors of 2026. That is the single largest honesty
improvement available to this app, and it is also a change in what `Tickers`
*means*, which is why it is yours: the shape (a magic string? a new
`[portfolio.Universe]` block? a `Universe` key beside `Tickers`?) and the
rebalance-frequency semantics both need deciding before any of it is written.

Everything else in this programme is useful without it.

---

---

## T20 — Survivorship: the universe has no delisted companies

**Category:** data · **Status:** parked

Found by T18; the full audit with every query is in `analysis/survivorship.md`.

`stock_data_optimized` contains **no company that stopped trading before
2025**. Of the 1,792 tickers trading in 2000, **1,792 are still present in
2025** — a 100% twenty-five-year survival rate, against a real figure nearer
40–50%. Enron, Lehman, WorldCom, Bear Stearns, Washington Mutual, SVB and
Bed Bath & Beyond are all simply absent. Tickers like `GM` and `HTZ` are
present but are the post-bankruptcy re-listings, with the collapse that wiped
out the prior shareholders excised from the price history.

So every multi-asset backtest in this repo is choosing among known survivors,
and the sweep, out-of-sample and deflated-Sharpe machinery (T15–T17) are all
rigorous measurements of a universe that never existed. `$`-prefixed
benchmarks are unaffected — they are reconstructed index series and already
contain the companies that failed out of the index.

### Why this is `needs-user`

It cannot be fixed with code. It needs two datasets this repo does not have:

1. **Delisted price history** — bars through each company's final trading day.
2. **Point-in-time index membership** — who was in an index on a given date.

Both are paid products (CRSP, Norgate, Sharadar). Yahoo Finance does not serve
delisted history, so `fetch_history.py` cannot close the gap however it is
pointed. Buying data, or deciding not to, is your call.

### What can be done without new data

- **Prefer `$`-benchmark portfolios** for anything long-horizon. They are the
  only currently-trustworthy way to backtest decades here.
- Consider a warning in the UI when a run's window starts more than a year or
  two back and its tickers are real companies rather than `$` series. That is
  a small, self-contained task if you want it — say so and it can be written
  up properly.

### Resolution plan — three tiers, pick one

This is the only blocked item where the decision costs money. Tiers, cheapest
first:

**Tier 0 — say "T20: warn" (free, ~half a day).** Do not buy anything; make
the bias impossible to forget.
1. A `SurvivorshipRisk` flag on each `Result`, set when a portfolio holds real
   tickers (not `$` series) over a window starting more than ~2 years back.
   Severity scales with window length.
2. Surface it as a banner in the results panel and a field in `[Output]`, with
   the honest text: *"this universe contains no delisted companies; returns
   are overstated by roughly 1–4 points a year."*
3. Teach `appReference` to say it unprompted whenever it proposes a long
   backtest on real tickers, and to prefer `$`-benchmarks for long horizons.
4. Re-run the T18 audit as a test, so if you ever *do* load delisted history
   the warning switches itself off rather than lying in the other direction.

This does not fix anything. It stops you being fooled, which is most of the
value and all of the cost-effectiveness.

**Tier 1 — say "T20: free data" (free, ~2–3 days, partial).** Some delisting
coverage can be reconstructed without a paid feed:
- SEC EDGAR company tickers files are published historically; diffing
  snapshots yields an approximate delisting date per ticker.
- Wikipedia maintains dated S&P 500 constituent-change tables, which give
  point-in-time membership for one index back to the 1990s.
Both are scrapes with real gaps and no price history for the dead names — you
get "who left and when", not "what it did on the way out". Enough to build a
*point-in-time S&P 500 universe*, which is a genuinely useful thing to
backtest against, and not enough to fix the broad universe.

**Tier 2 — say "T20: buy" (money, ~1 week).** A real fix needs delisted prices
plus point-in-time membership. Rough shape of the market: Sharadar SF1/SEP via
Nasdaq Data Link is the usual first stop for an individual, Norgate is
strongest for point-in-time index membership, CRSP is the academic standard
and priced accordingly. **I have not checked current pricing or terms — verify
before committing.** Once bought, the work is a loader plus a
`delisted_returns` convention (a bankruptcy is a −100% return, not a missing
row) and a rerun of the T18 audit to confirm the fix.

**My recommendation: Tier 0 now, regardless of what else you choose.** It is
free, it is half a day, and every result the app has ever produced needs that
caveat attached. Then decide on Tier 1 vs 2 separately — Tier 1 is a good deal
if you mainly care about index-constituent strategies, and pointless if you
want the broad market.

---

---

## T35 — From verdict to positions: the paper-trading bridge

**Category:** feature · **Status:** parked · **needs T22, T24, T32**

The natural end of the funnel. A strategy that survives should produce
**today's target positions**, be tracked against what it actually would have
done, and show live-versus-backtest divergence — which is the only honest
early warning that an edge has decayed or was never there.

### What to build

1. **Signal generation.** Run a saved strategy against data through yesterday
   and emit target weights for today. Same engine, same code path, window
   ending now — no separate "live" implementation, because two implementations
   is how live and backtest silently diverge.
2. **A paper book.** Persist those targets, mark them to market daily, and
   keep a real equity curve for the live period.
3. **Live-versus-backtest tracking.** The comparison that matters: the paper
   curve against the backtest's own out-of-sample expectation, with a
   divergence measure. An edge that decays shows up here months before it
   shows up in a re-run backtest.
4. **Staleness and failure handling.** A signal computed from stale data is
   worse than no signal. Refuse to emit if the latest bar is older than a
   configurable tolerance, and say why.

### Why `needs-user`, and read this part

Everything upstream of here produces *estimates*. This produces *instructions*,
and the gap between the two is where people lose money:

- **The backtest is optimistic and you now know by how much.** Survivorship
  (T20) alone is worth 1–4 points a year on this data. A strategy whose live
  expectation is not comfortably above that margin has no measured edge at all.
- **Fills are modelled at the close.** Real execution is not, and the
  difference is largest exactly when a strategy most wants to trade.
- **This app has no risk layer.** No position limits, no exposure caps, no
  kill switch. Those are not features to add later if the output is going to
  inform real orders; they are prerequisites.

So: **paper only, and no broker integration.** Emitting a target weight is a
different act from placing an order, and the second one should stay outside
this application. If you want it to go further than paper, that is a decision
worth taking deliberately and separately, with the risk layer built first.

I would also gate it: no strategy reaches the paper book without a walk-forward
result (T19) and a surviving verdict (T26). A bridge that will forward anything
is a bridge from a spreadsheet to your brokerage account.

---

---

## T87 — No frontend test runner, and the onboarding work is all frontend

**Category:** infrastructure · **Status:** parked

`ui/frontend` has no test framework. `npm run build` runs `tsc`, so types are
enforced by the gate, but nothing executes a line of frontend logic. That was
tolerable when the frontend was thin; T81–T85 put real logic there —
`buildToml` decides what config the engine runs, and it now branches.

For T81 I verified the byte-identical constraint by compiling the pre-change
`buildToml.ts` from git next to the new one and diffing their output over three
forms. That works, it is honest, and it is not repeatable by anyone else.

**This needs you because it is a dependency change**, which the protocol says
the agent must not make. `vitest` is the obvious fit — it reuses the existing
vite config, adds one devDependency, and `buildToml.ts` is a pure function with
no DOM, so the first tests need no jsdom.

Once it exists, the tests that should have shipped with T81:

- an untouched form generates exactly today's buy-and-hold TOML (the
  byte-identical guarantee, currently proven only in a commit message);
- a library pick emits `Strategy = "lua:strategies/<file>"` and no `Params`;
- `usesAllocations` gates the weights block;
- symbols needing quoting (`9988.HK`) survive `tomlKey`.

---