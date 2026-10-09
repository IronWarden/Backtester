# Backtester — project & agent guide

Concurrent Go backtesting engine over historical equity data in DuckDB, plus a
Wails desktop UI with a built-in AI assistant. Strategies are scriptable in Lua.

## Layout

```
src/main.go                    entry point; resolves ../stock_data.db and ../config.toml
src/backtest/
  config.go                    TOML loading -> Portfolio construction
  portfolio.go                 Portfolio/Position state, Buy/Sell/Deposit/Withdraw
  runner.go                    worker pool (NumCPU), data prefetch, result collection
  strategy.go                  BuyAndHold; SMACross/RSI helpers
  strategy_lua.go              Lua strategy host + API surface
  metrics.go                   Sharpe, Sortino, max drawdown, CAGR, stdev (252d annualized)
  reporter.go                  result formatting/output
  correlation.go               cross-asset correlation
src/data/database.go           DuckDB access: stock_data_optimized, "3MTreasuryYields", RunQuery
ui/                            Wails app: app.go, chat.go (prompt/tools), claude.go,
                               ollama.go, query.go, quote.go, fetch.go, screen.go
ui/frontend/src/               React: App.tsx, ChatPanel.tsx, QueryConsole.tsx,
                               ResultsView.tsx, SimpleForm.tsx
strategies/*.lua               Lua strategy library (see strategies/README.md)
*.py                           data loaders: add_collections, fetch_history, add_ticker,
                               update_treasury_yields, extend_risk_free, screen_stocks
build_db.py                    builds a whole DB from scratch (runs the loaders in order;
                               no API keys). WRITES -- only ever point it at a new
                               scratch path, never stock_data.db, in unattended runs
fred_public.py                 keyless FRED CSV reader (build_db, add_collections fallback)
```

`README.md` is thorough and current — read it before changing config schema,
metrics, the Lua API, or the assistant. Keep it in sync with your changes.

## Build / test

```bash
go build ./...
go vet ./...
go test ./...            # full suite ~50s (ui package dominates)
go test ./... -short
```

Wails UI builds need the webkit tag, and the binary is copied into place:

```bash
wails build -tags webkit2_41 && cp build/bin/ui ui/ui
```

`ui/ui` is a 54 MB build artifact and is **git-ignored** — never commit it.

**After any change to the app, build it headless and never launch the GUI.**
The user works in the app (and often has `wails dev` running) while agents
work, and a window an agent opens interferes with that. So: `wails build` (or
`wails generate module`) plus the copy above, and that is all. Never run
`wails dev`, `./ui/ui`, `build/bin/ui`, or anything that opens a window, and
never stop the user's running app or dev session to free the DB or a port.
Verify through the gate (smoke render, tests), not by opening the app.

## Domain notes

- `$`-prefixed tickers (`$SP500`, `$USTOT`, `$CASH`, …) are index/asset-class
  total-return benchmarks loaded by `add_collections.py`, on the NYSE calendar
  so they mix with real stocks. See the README table for coverage windows.
- `CASH` (no `$`) is **Pathward Financial**, a real stock — not a cash proxy.
  Cash-the-asset is `$CASH` / the `3MTreasuryYields` table.
- DuckDB is **single-writer**. The UI holds the connection; close it before
  running any Python loader that writes.
- **Every DuckDB that touches the file must be on one version line — 1.5.x.**
  The app bundles DuckDB 1.5 via `github.com/duckdb/duckdb-go/v2` (was
  `marcboeker/go-duckdb` 1.1.3 until 2026-09-27). A newer writer can leave
  storage an older reader cannot scan: Python DuckDB 1.5.0 did exactly that,
  and the old app failed every cross-ticker query while single-ticker reads
  worked. **Verify a DB after a write with a cross-ticker scan** (`GROUP BY
  Ticker` over the whole table, in the app's engine) — a single-ticker or
  `LIKE '$%'` query skips the affected blocks and proves nothing.
- Metrics annualize over a 252-trading-day year.
- The DB holds **more than prices**, despite what the docs said until
  2026-08-16: `financials` (quarterly fundamentals, ~6,900 tickers, 2020-07+),
  `earnings_calendar` (real report timestamps, ~1,000 tickers),
  `economic_indicators` (CPI, FEDFUNDS, GDP, INDPRO, M2, UNRATE, 1927+) and
  `"10YrTreasuryYields"`. `company_info` is empty and its `(Ticker, Date,
  Value)` shape was never company metadata — ignore it.
- **Sector data is free and does exist**, contrary to what the docs said until
  2026-08-17: SEC EDGAR publishes every filer's SIC code with no key.
  `add_company_info.py` loads it into the optional `company_profile` table
  (`ticker, cik, name, sic, sic_description, sector, exchange, former_names,
  …`); read it via `src/data.LoadCompanyProfiles` / `SectorsForTickers`, which
  return nil when the table is absent. SIC is coarser than GICS and its
  divisions have gaps, and coverage is ~5,600 of the priced tickers — delisted
  symbols are absent because `company_tickers.json` lists only current tickers.
- **SEC bulk fundamentals, 2009+, dead companies included, with real `filed`
  dates**: `add_sec_financials.py` → `sec_financials`, read via
  `src/data.LoadSECFundamentals` + the pure `FirstKnown` / `KnownOn` (earliest
  filing of a figure = when it became knowable). Measured lag for a filing's own
  figures: **median 48 days**, max 90 — so T29's 90-day fallback is the worst
  case, not a typical one. ~60% of rows are prior-year comparatives
  (`period_end < filing_period`); never join a restatement to the original
  filing's date. **Wired into `PointInTimeFundamentals` (T53)**: the precedence
  is now SEC `filed` +1 day → `earnings_calendar` +1 day → period end + 90 days,
  and the 90-day constant stays at the worst case on purpose (a median would put
  half of all figures earlier than publication).
- **Every loader records provenance** into `data_sources` via
  `loader_provenance.py` (one row per table+loader; `stock_data_optimized` has
  two contributors). Read with `src/data.LoadDataSources` / `SummarizeSources`.
  A `-dirty` sha means the load came from an uncommitted tree. New loaders must
  call `loader_provenance.record()` — it cannot be filled in retroactively.
- **Point-in-time S&P 500 membership** is reconstructable free from Wikipedia:
  `add_index_membership.py` → `index_membership`, read via
  `src/data.IndexMembersOn` / pure `MembersOn`. Trustworthy back to ~2011,
  approximate to 2007, fiction before (the source records 16–30 changes/year
  from 2011 and almost none pre-2007). Current members have `end_date`
  9999-12-31. No config plumbing selects a universe by index yet (T48).
- **Recycled tickers are repaired by splitting, not deleting.** `delistings`
  is loaded (2026-08-20). `split_recycled.py` separates a spliced series: the
  company trading today keeps the bare symbol, the previous occupant takes a
  generation suffix (`COR.1`), numbering from the oldest. Dry run unless
  `--apply`; **the user runs the write**. Every candidate is corroborated
  against `company_profile` first — a delisted name matching the current
  filer's name or a former name is a RENAME and is left alone. **120 operating
  symbols are genuinely spliced, not 269**: a series that BEGINS after its
  delisting date holds only the later company, and that rule alone accounts for
  149 of them. `DD` is a rename, not a recycling. Go side:
  `src/data.ParseGeneration` / `GenerationSymbol` / `GenerationSeparator`, and
  `ClassifyCoverage` now takes a `DateRange` so it can apply the first-bar rule.
  **`--apply` was run 2026-08-20**: 48 symbols split, 91,993 bars moved.
- **Shorting is opt-in and partly built** (T27a). `AllowShort` on
  `[[portfolio]]`, default false = the long-only engine, bit-identical. On, a
  `sell` past the holding closes the long then shorts the rest; `short()` (a
  TARGET, not a delta) and `cover()` say it directly, and a negative
  `target_weights` leg is market-neutral with no new API; `Amount` goes negative, `AveragePrice` is the short entry,
  covering realises `AveragePrice - fill`, and exposure becomes gross rather
  than not-cash. Borrow cost is T27b: `borrow_bps_annual` under
  `[portfolio.Costs]`, accrued daily on the marked short over a 252-day year,
  charged to cash before the strategy acts, never in the blotter, surfaced as
  `Metrics.BorrowPaid`. One flat rate is a simplification — real borrow is
  per-security. Margin is T27c: `[portfolio.Margin]` `initial_pct` /
  `maintenance_pct`, fractions of the short's MARKED value, **zero means
  default (Reg T 50% / 30%), not "no requirement"** — the one knob here whose
  zero is not inert. A breach covers the minimum that restores it
  (`x >= S - E/m`, since covering is equity-neutral), falling back to full
  liquidation if costs leave it still in breach; a forced cover may overdraw,
  because a margin call is not a discretionary order. `Metrics.MarginCalls`.
- **A bankrupt holding is not worth its last close** (`src/backtest/delisting.go`,
  T38). `DelistingReturn` on `[[portfolio]]`, default 0.0 (identical numbers),
  must be in [-1, 0]. **Reason-aware**: only bankruptcy and compliance removals
  are haircut — acquisitions keep their last close, because of the delistings
  whose reason is known the majority ARE acquisitions. An unidentified reason is
  counted in `UnclassifiedDelistings`, never guessed at. Liquidation runs
  through `Sell` so the blotter and attribution see it. Fires only on the final
  simulated day: `validateCoverage` refuses a window past a ticker's last bar,
  so a holding cannot die mid-run.
- **A rename is not a recycling** (`src/data/renames.go`, T79). The registry
  records a name change the same way it records a death, so `ClassifyTickers`
  corroborates any condemning verdict against `company_profile` and returns the
  new `renamed` verdict — tradable — when the delisted name is one the current
  filer used to have. 74 symbols on the loaded DB, `DD` among them. Inert
  without `company_profile`. Known false positive: an acquirer taking its
  target's name (`COHR`).
- **Delisting reasons are free from EDGAR.** `add_delisting_reasons.py` harvests
  the quarterly filing indexes for Forms 25/25-NSE/15 and reads 8-K items 1.03
  (bankruptcy) and 2.01 (completed acquisition) into `delisting_reasons`; read it
  via `src/data.LoadDelistingReasons` / the pure `ClassifyReason`. Keyed by
  **CIK, not ticker** — the symbol join is unsolved. Bankruptcy outranks
  acquisition (SVB filed both).
- **The bars have real defects** (audited 2026-08-17, `analysis/data_quality.md`,
  `add_data_quality.py`): `VHI`/`SVA`/`SAFE`/`DEC` carry **negative** closes
  (11,931 bars, `VHI` to −639.30) from Yahoo back-adjustment; `HUBB` has 1,862
  consecutive identical closes and synthetic 1972 bars; 281 tickers have a flat
  run ≥ 60; 263 have a single-day move over 500%. 273 tickers are `unusable`.
  No duplicate rows, though — unlike `financials`. Read verdicts via
  `src/data.LoadDataQuality`; nothing refuses to trade a bad series yet.
- **`financials.date` is the fiscal period end, not the publication date.**
  Joining it to prices on that date is look-ahead bias. Read it through
  `src/data.PointInTimeFundamentals`, which lags to the real report date where
  `earnings_calendar` has one and to period end + 90 days otherwise. The raw
  table also contains exact duplicate rows; that helper dedupes. The same
  period-vs-release trap applies to `economic_indicators`.

---

# Unattended agent protocol

This section governs sessions running via `/loop` while the user is away.
Follow it exactly.

## The one rule

**Never break the green baseline.** `go build ./...`, `go vet ./...`, and
`go test ./...` all pass on `main` as of the baseline commit. Every commit you
make must leave all three passing.

## Working set

- Work only on the branch `agent/unattended` (or the branch you were started
  on, if it differs). **Never** commit to, rebase, or merge into `main`.
- `CLAUDE.md`, `TASKS.md`, `AGENT_LOG.md`, and `.claude/` are **git-ignored on
  purpose** — they are local tooling, not part of the application, and must
  stay out of PR diffs. Keep updating them on disk; never `git add -f` them,
  and never remove them from `.gitignore`. Every commit you make must contain
  application changes only, so `git status` staying clean of these files is
  expected, not a problem to fix.
- Never `git push`. Never `git rebase`, `git reset --hard`, or force-anything.
- Never modify: `.env`, `src/.env`, `stock_data.db`, `go.mod`, `go.sum`.
  If a task appears to require a new Go dependency, **stop that task** and
  record it in `AGENT_LOG.md` as blocked — dependency changes need the user.
- Never run the Python loaders in write mode (`add_collections.py` without
  `--dry-run`, `fetch_history.py`, `add_ticker.py`, `update_treasury_yields.py`).
  They write to a 2.9 GB single-writer database the user may have open.
  Read-only inspection via `RunQuery` / `duckdb` SELECTs is fine.

## Each iteration

1. Read `TASKS.md`. Take the **topmost** task whose status is `todo` and whose
   category you are able to complete. Skip anything marked `needs-user`.
2. Set its status to `in-progress` and save the file, so a crashed iteration
   leaves a visible trail on disk.
3. Do the work. Prefer small, complete changes over large partial ones.
4. Verify by running the **Regression gate** at the top of `TASKS.md` in full —
   `go build ./...`, `go vet ./...`, `go test ./... -count=1`, plus the
   frontend build for UI-facing changes and `-race` for concurrency ones. That
   block is the authority on what "verified" means, including how to tell a
   regression from a test of your own that you got wrong; do not substitute a
   quicker check for it.
5. If green: set status `done`, append an entry to `AGENT_LOG.md`, and commit
   the work with a message of the form `agent(T<id>): <what changed>`.
6. If red after a genuine attempt: `git checkout` the files you touched to
   revert cleanly, set the task to `blocked` with a one-line reason in
   `TASKS.md`, log it in `AGENT_LOG.md`, and move to the next task.
   **Do not leave a broken tree.**

## When a new test fails because the production code is wrong

Step 6 is for tests you got wrong. This is the other case: the test is right
and the code has a real bug. Never delete a correct test to get back to green —
that discards the finding, which is the opposite of the point.

First confirm the bug is real: re-read the production code and decide whether
it actually violates its documented or obviously-intended behavior. A test
asserting behavior you merely assumed is a wrong test — that is step 6.

Once confirmed:

- **Small, localized, obviously correct fix** (a wrong comparison operator, a
  missing bounds check, an off-by-one): make it in the same commit as the test.
  Say plainly in the commit message and `AGENT_LOG.md` that you changed
  production code and why. The test must fail before the fix and pass after.
- **Anything larger** — or any fix touching metric formulas, the TOML schema,
  or concurrency in `runner.go`: do not attempt it. Revert to green, then
  append a new task to `TASKS.md` with status `needs-user` describing the bug,
  the input that triggers it, and the test you wrote to expose it. Record it in
  `AGENT_LOG.md` under **Notes** so it is the first thing the user sees.

Either way the tree stays green and the finding survives.
7. If `TASKS.md` has no actionable `todo` items, do not invent scope. Append a
   note to `AGENT_LOG.md` saying the queue is drained and stop the loop.

## Definition of done for a feature

A feature is not done when the code works. It is done when nothing downstream
still describes the old world. Before marking any feature task `done`, walk
all five — a missed one is a regression even though the build is green.

1. **The engine.** Code plus tests that fail without it. Tests go in the
   package that owns the behavior.
2. **`README.md`.** It is the reference the user actually reads; a config key,
   metric, or Lua global that is not in it does not exist as far as they are
   concerned. `strategies/README.md` too if the Lua API surface changed.
3. **The in-app assistant.** `appReference` in `ui/chat.go` is the assistant's
   *entire* knowledge of this engine — it cannot read the source at runtime.
   Anything user-visible you add (a config key, a metric, a Lua global, a
   result field, a tool) must be described there, or the assistant will keep
   confidently telling users the feature does not exist. This has already
   happened: the prompt claimed the engine modelled no transaction costs for
   several releases after `[portfolio.Costs]` shipped.
   `TestSystemPromptDocumentsEveryMetric` and
   `TestSystemPromptDocumentsConfigSchema` in `ui/tools_test.go` enforce this
   for `backtest.Metrics`, `PortfolioConfig`, and `CostConfig` by reflection —
   if you add a field there and skip the prompt, the build breaks. Nothing
   enforces it for Lua globals, `[Output]` result fields, or new tools, so
   those are on you.
4. **A component that renders must be EXERCISED, not compiled.** `npm run
   build` is `tsc` plus a bundle, and neither has ever mounted a component —
   which is how T90 shipped a black screen through a green gate for a week. If
   you touch anything under `ui/frontend/src/`, add or extend a case in
   `ui/frontend/src/smoke.tsx`, and keep `TestNoConditionalHooks` green (it
   rejects a hook nested in a block or below an early return — the T90 bug,
   refused by construction). Both run inside `go test ./...`.
   It must also **look like the rest of the app**: walk the *UI standard*
   in `TASKS.md` (shared `ResearchTable` and formatters, dates as words,
   theme tokens, a source caption, never a blank view). A one-off component
   or formatter where a shared one exists is a regression even when it
   renders.

5. **No regressions.** `go build ./...`, `go vet ./...`, `go test ./...` all
   green, and — this is the part that gets skipped — the *existing* tests must
   still assert something meaningful. If a change made an old test fail,
   fixing the test is only correct when the old expectation was genuinely
   wrong; see the section above on when the production code is the bug.
   Default new config fields to a zero value that reproduces the previous
   behavior exactly, so an untouched config keeps producing identical numbers.

When a feature spans engine and UI, prefer one commit that lands all five
together over a commit that lands code and a follow-up that lands docs — the
follow-up is what never gets written.

## Scope discipline

- One task per commit. Do not opportunistically refactor adjacent code.
- Keep complete-but-unused methods — the user prototypes with them. Dead
  *fields* and redundant statements are fair game.
- Match the surrounding code's style, naming, and comment density.
- Do not add dependencies, change the TOML config schema, alter metric
  formulas, or restructure packages without an explicit task saying so.

## Logging

Append to `AGENT_LOG.md` after every task — done, blocked, or skipped. Format:

```
## T<id> — <title>
- **Status:** done | blocked | skipped
- **Commit:** <sha or ->
- **What:** one or two sentences
- **Notes:** anything the user should look at on return
```

Be honest in the log. A blocked task recorded accurately is more useful than a
task marked done that does not work.

---

# Parallel mode

Optional. Use it only when the loop prompt says "parallel mode". Without that,
the single-agent protocol above runs unchanged. Every rule above still applies.
This section changes **who** does each step, not what counts as done.

## Roles

- **Coordinator**: the `/loop` session, working in the main checkout on
  `agent/unattended`. Only the coordinator edits `TASKS.md` or `AGENT_LOG.md`,
  and only the coordinator commits to `agent/unattended`.
- **Workers**: subagents the coordinator launches with
  `Agent(..., isolation: "worktree")`, one task each. A worker commits only on
  its own worktree branch, and never touches `TASKS.md` or `AGENT_LOG.md`.

## Each coordinator iteration

1. Read `TASKS.md`. From the top, pick **at most 2** `todo` tasks (3 only if
   none of them is UI-facing) that are **independent**: no shared source
   files, and at most one of them may touch the shared hot spots
   `README.md`, `strategies/README.md`, `ui/chat.go`,
   `ui/frontend/src/smoke.tsx` or `ui/frontend/src/App.tsx`. Never pick
   `needs-user`. If only one task qualifies, run one worker, or do the task
   yourself under the single-agent protocol.
2. Mark the chosen tasks `in-progress` and save the file **before** launching
   any worker.
3. Launch every worker in the same message so they run in parallel. Each worker
   starts with no context, and the git-ignored files below are **not in its
   worktree**, so its prompt must contain:
   - the task's full text, copied from `TASKS.md`;
   - an instruction to read `/home/sky/Projects/Backtester/CLAUDE.md` (absolute
     path) before doing anything, and to follow its "Unattended agent protocol"
     section, which this section modifies;
   - the Regression gate block from `TASKS.md`, copied verbatim;
   - this setup, run first from the worktree root (these links are git-ignored,
     so the tree stays clean):
     ```bash
     ln -s /home/sky/Projects/Backtester/stock_data.db stock_data.db
     ln -s /home/sky/Projects/Backtester/ui/frontend/node_modules ui/frontend/node_modules
     ```
   - the rule: commit once, `agent(T<id>): …`, only when the full gate is green
     in the worktree. If it is red, commit nothing. Either way, return the
     commit sha (or none), the gate output, and a one-paragraph log entry.
4. When each worker returns, on `agent/unattended`, one task at a time:
   - Red, or no commit: mark the task `blocked` with the worker's reason, and
     log it.
   - Green: `git cherry-pick <sha>`, then run the **full** Regression gate
     again here. Two changes that each pass on their own can still fail
     together. If it is green, mark the task `done` and log it, using the
     cherry-picked sha. If the cherry-pick conflicts, or the gate goes red,
     run `git cherry-pick --abort`, or `git revert --no-edit HEAD` if the
     cherry-pick already committed. Then set the task back to `todo` with the
     note "re-run serially: conflicted with T<other>", and log it.
5. Repeat until no actionable `todo` remains, then drain the queue as in
   step 7 of the single-agent protocol.

## Constraints specific to parallel mode

- **The DB is shared.** Every worker's `stock_data.db` is a link to the one
  real file. Workers only read it. If the UI holds a write lock, every worker's
  DB tests fail together. Treat that as an environment problem, not a task
  failure: stop the loop and log it.
- **CPU.** `go test ./...` already uses every core. Running more workers makes
  each gate slower, not the queue faster. That is why the cap is 2.
- **Never merge.** `git merge` is denied in settings and `git rebase` is
  forbidden. `cherry-pick` is the only way a worker's commit reaches
  `agent/unattended`.
- **Worktree base.** `.claude/settings.local.json` sets `worktree.baseRef` to
  `head`, so workers branch from the coordinator's current `agent/unattended`
  commit, not `origin/main`. Cherry-pick each finished task before launching
  the next batch, so new workers start from it.
- Worktrees are left under `.claude/worktrees/`, which is git-ignored. Don't
  delete them. The user cleans them up.

## Starting it

```
/loop Run the unattended protocol in CLAUDE.md in parallel mode, as coordinator.
```
