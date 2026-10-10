# The desktop app

The Backtest and Research tabs, and the built-in AI assistant.

[← README](../README.md) · [Installation and setup](installation.md) · [Data](data.md) · [Configuration](configuration.md) · [Command line](cli.md) · **The desktop app** · [Architecture](architecture.md)

## The Backtest tab (UI)

The topbar has four tabs, named for what they operate on rather than for how
advanced they are: **Backtest** (the form below), **Config** (the raw TOML and
Lua editors), **Data** (a SQL console over the database) and **Research**
(read SEC filings — see [The Research tab](#the-research-tab-ui)).

The desktop app opens on the **Backtest** tab: a form for the things every
backtest needs — name, starting capital, a date window with `Max`/`YTD`/`1Y`/`5Y`
presets, and a list of tickers with allocation percentages. It generates the
TOML for you, and **Edit as TOML →** hands that config to the **Config** tab's editor
when you want the full schema.

### The Lua editor knows the strategy API

The app's Lua tab completes and explains all 34 globals a strategy can call:
type `sm` and get `sma(ticker, day, period)` with its documentation, accept it
and the cursor lands on the first argument. Hovering any name shows the same
card. On an empty file the first completion is `step`, the one function every
strategy defines.

The descriptions are served by the engine (`backtest.LuaAPI`), not copied into
the frontend, and `TestLuaAPICoversEveryGlobal` parses the package for its own
`SetGlobal` calls and fails if the two sets differ in either direction. A
global added to the engine cannot ship without its documentation, and the
editor can never offer one that does not exist — which would be worse than
offering nothing, since you would only find out on day 0 of a run.

### Reading a library strategy, and editing your own copy

Every strategy picked on the Backtest tab opens in the Lua tab in a buffer of
its own, and its card there has a **Read the script →** link that goes
straight to it — so what a strategy actually does is one click from where it
was chosen. Each open script is a chip above the editor; 🔒 marks a library
script and • unsaved work.

A library script is **read-only**. It is what the gallery describes and what
every config naming it runs, so an edit would quietly change other runs. To
change one, **Edit a copy**: it opens `strategies/my_<name>.lua` — your copy,
first line relabelled, created on Save — or your existing copy if there
already is one, so a second click never overwrites work. Run it with **▶ Run
this script**, or name it in a config as `Strategy =
"lua:strategies/my_<name>.lua"`. The Assistant's "Insert into Lua editor"
lands in the scratch file when a library script is showing.

A portfolio that omits `Strategy` runs the script the config names (or the
scratch file), not whichever chip is open — reading a library script never
changes what a run does.

### Starting a script, and running it

The Lua tab opens on `strategies/my_strategy.lua`, your scratch file — a blank
`step(day)` until you first save it, so Save never changes a library strategy.
A blank file says nothing about how a strategy is shaped, though. Above the editor, while the buffer holds no
unsaved work, are three **starting shapes**:

| shape | for |
|---|---|
| A signal, per ticker | decide about each holding on its own — a moving average, an RSI level, a breakout |
| Rebalance on a schedule | hold everything and put the weights back every so often; no signal, no timing |
| Rank and hold the top few | score every ticker against the others and hold the best — momentum, value, low vol |

Each runs as written and has one `TODO` where the idea goes, and each is put
through the app's own ✓ Check by the engine's tests — a skeleton that does not
run teaches a beginner that they broke it. Picking one moves the path to
`strategies/my_strategy.lua`, so it can never overwrite the script that was
open, and they are only offered when the buffer is unmodified: an unmodified
file is still on disk, so replacing it loses nothing and there is no "are you
sure" to get wrong.

**▶ Run this script** runs the open script against the holdings already on the
Backtest tab — every portfolio there, pointed at this file instead of whatever
it was going to run. Without it, trying an idea means going to the Config tab,
writing a portfolio around it in TOML, and coming back, which is a lot to learn
before finding out whether the idea does anything at all.

### Seeing what the strategy saw: `plot()`

A strategy can record a number per day with `plot(name, value)`, and each name
becomes a small chart under **What the strategy plotted**, drawn against the
same days as the equity curve. It is how a person finds out *why* a run did
what it did — the moving average being crossed, the z-score being waited on —
without reading it out of a thousand `log()` lines.

Separate charts rather than extra lines on the equity curve, deliberately:
that axis is in dollars, and an RSI between 0 and 100 drawn on a $184,000 axis
is a flat line along the bottom.

One value per name per day (the last call wins), at most 8 names, series start
where you start plotting them, and NaN/infinity are dropped. Both bounds report
themselves once as a diagnostic rather than silently losing data. Nothing here
reaches a metric: it is an instrument, not a measurement.

### An error marked on its line

Every diagnostic that carries a Lua position — a raised error, a syntax error,
anything the script check found — arrives with the script and line parsed out
of it (`backtest.ScriptPosition`, done once in `diagf` so no consumer parses
messages itself). Those positions are drawn as markers in the editor's gutter
for the script that is open.

Both formats are recognised, which matters more than it sounds: a raised error
reads `mine.lua:41:` and a *syntax* error reads `mine.lua line:3(column:3)`,
sharing no punctuation — and the syntax error is the one whose line is
certain.

### Checking a script before running it

The Lua tab's **✓ Check script** compiles the script, requires its `step(day)`,
and then *runs that function* for three days against a two-ticker synthetic
history built in memory. It opens no database and simulates no market, so it
answers on a keystroke.

Running it is the part that matters. Anything that only compiles the file will
never execute the body, and the body is where the mistakes are — a misspelled
global, a `nil` compared with a number, arguments in the wrong order. Those are
reported as **warnings**, not errors, and the message says what they ran
against: a script that needs fundamentals gets `nil` from this history and may
raise here while being perfectly correct in a real run. Only the unambiguous
failures are errors — a syntax error with its line, no `step` at all, a `step`
that is not a function, an empty file.

**A name nothing defines is an error, not `nil`.** While the check runs, the
globals table carries a metatable that raises on reading a name the engine
never registered and the script never assigned, with the closest real name
offered: *unknown name "smaa" — did you mean "sma"?* This is Lua's worst trap
for a beginner. `smaa(t, day, 20)` is a call on nil and at least raises; `local
n = my_thresold` is simply nil, forever, and a strategy that quietly never
trades looks exactly like one that decided not to.

It is installed **in the check only**. A real run is untouched: a config that
ran yesterday produces identical numbers today, and a script that relies on
reading an undefined global as nil still does. Assigning a global declares it,
so a script keeping its own state (`held = false`, then reading `held`) is
unaffected — as is the whole standard library, since the metatable only ever
sees names the table does not have.

It also reports a `[portfolio.Params]` key the script never reads, which in a
sweep means every trial ran identical settings. The reverse is deliberately not
reported: `params.period or 20` is ordinary Lua.

An endless loop at the top of a script is bounded rather than fatal — the check
runs under a two-second deadline, because hanging the app is the one failure a
checker must not have. Every one of the twelve shipped strategies passes it,
which is what `TestCheckPassesEveryShippedStrategy` exists to keep true.

### Basics, or everything

The metrics table has two views, chosen in its header:

- **Basics** — final value, profit, annual %, max drawdown, Sharpe, and the
  strategy and portfolio names. Plus, whenever the run produced them,
  *Contributed*, *Missed W/D*, *Confidence* and *Alpha %*.
- **Everything** — the full table, exactly as it has always been.

The four extras in Basics are not a judgement call about importance. They are
there because hiding them makes a visible column lie: Profit without
*Contributed* reads as investment return when part of it is money paid in; a
swept Sharpe without *Confidence* reads as edge when it may be selection; and
a withdrawal plan that ran out of money says so in *Missed W/D* and nowhere
else.

The choice is remembered, and Basics is what a first run shows. Nothing is
removed by it — every hidden column is one click away, with its definition
still on hover.

### The form is remembered between launches

Everything on the Backtest tab — the portfolio cards, holdings, allocations,
window, strategy, benchmark, contribution schedule and the pre-registered
claim — is saved as you edit it and restored next launch. A first session is
exploratory, and closing the app to think about a result should not cost the
portfolio you built to get it.

Three things are deliberately *not* remembered:

- **the results.** A stale result beside a fresh form reads as today's numbers.
- **the database and config paths.** They may have moved, and the app resolves
  them properly on launch (see *Where the app looks for its files*).
- **anything that no longer matches the current form.** A stored session is
  input like any other: it survives upgrades, so it is validated field by
  field and dropped *whole* if any part of it fails. Half a restored form —
  holdings with no allocations, a criterion with no metric — is worse than an
  empty one, because you cannot see what is missing.

Stored under `backtester-session-v1` in the app's local storage; the version
is in the key, so a shape change re-defaults rather than restoring a form the
app no longer understands.

### Starting from a question

An empty form asks for holdings, two dates, a strategy, an allocation and a
success criterion before anything has happened — five decisions a first-time
user has no basis to make. So while no portfolio has holdings, the tab offers
**starter examples**, each labelled with the question it asks:

| example | the question it asks |
|---|---|
| Hold the big five | What would simply holding the five biggest tech names have done? |
| Chase the winners | Does rotating into whatever has been rising beat holding all of them? |
| Step out of falls | Does selling when the market drops below its average beat sitting through it? |
| Drip money in | Is paying in a bit each month better than investing it all at once? |

One click fills the holdings, the window, the strategy, the benchmark and a
matching pre-registered hypothesis — a complete run you are then meant to
**edit**, which is a much easier first act than assembling one from nothing.

*Chase the winners* is written as [rules](configuration.md#portfoliorules), so its button and
its card read as a sentence — "Every 21 trading days (about monthly), hold the 2
stocks that rose most over the last 126 trading days, counting only those up
over the last 126 trading days…" — rather than as a script you would have to
open. It is the exact twin of `momentum_rotation.lua` (see below). *Step out of
falls* and *Drip money in* stay scripts and say why on their buttons: one buys
and sells on the crossing itself, which a target-weight rule set does not
reproduce, and the other is driven by deposits, which no rule describes.

**Library scripts with a rules twin.** Two shipped scripts can be said exactly
by the menu, at their default parameters:

| script | the same strategy as rules |
|---|---|
| `momentum_rotation.lua` | `Rebalance = "monthly"`, `Rank = "return_126"`, `Pick = 2`, `Filter = "rising_126"` (the script only funds winners with positive momentum) |
| `rebalance.lua` | `Rebalance = "quarterly"`; its optional `weights` are `Weight = "allocations"` |

Pick either on the Backtest tab and its card shows the sentence under the
works/fails text, with **Use as rules →** to switch the card over.
`TestLibraryTwinsMatchTheirScripts` (`src/backtest/rules_test.go`) runs each twin
against its script and requires the same equity curve to the cent, so the two
cannot drift apart; the Lua originals stay in `strategies/` unchanged. Nothing
else in the library has an exact twin — `trend_following.lua` was tried as a
daily `above_sma_200` filter and differs from the first trade, because the
script sizes with `buy_max` rather than to a target weight.

Every example is built against the database you have loaded: symbols it does
not carry are dropped, the window is clamped to what the remaining holdings
**and** the benchmark both cover, and an example that cannot be made runnable
is not offered at all. So clicking one always produces a run rather than a red
date error. They vanish once a portfolio has holdings, and return if you clear
it.

### When you do not know what to hold

The ticker box searches company names as well as symbols, which helps if you
know you want Apple. If you do not know what you want at all, an empty holdings
box offers **starter baskets** — big tech, the whole market as one fund, or a
spread across five sectors — and one click adds the lot at equal weight.

Like the examples, a basket is filtered to symbols your database actually has
and is withdrawn entirely if too few survive, so "spread across sectors" never
resolves to two names. They appear only once something else in the form has
holdings: on a wholly empty form the starter examples above are the better
answer, and offering both would be two answers to one question.

### Copying a famous investor

Every portfolio card has a **Copy a famous investor** dropdown in its
*Holdings & allocation* box, so one can run next to your own; an empty form
also lists them as cards under the starter examples. Picking one **replaces**
the card's holdings, weights, dates and strategy (capital and any cashflow
schedule are kept), and the card then shows that portfolio's caveat. One your
database cannot build stays in the list, disabled and marked *not in this
database*. The portfolios:

| portfolio | holdings | held how |
|---|---|---|
| Buffett 90/10 | `$SP500` 90, `$CASH` 10 — his instructions for his own estate (2013 letter) | `rebalance.lua`, 30 years |
| Berkshire 13F, Q4 2015 | the filing's 25 largest holdings at its dollar weights (WFC, KHC, KO, IBM, AXP, …), less any your database cannot price | bought 2016-02-17 and held |
| Dalio All Weather | VTI 30, TLT 40, IEF 15, GLD 7.5, DBC 7.5 | `rebalance.lua`, 20 years |
| Browne Permanent Portfolio | VTI, TLT, GLD, `$CASH` at 25 each | `rebalance.lua`, 20 years |
| Swensen portfolio | VTI 30, VEA 15, VWO 5, VNQ 20, TIP 15, IEF 15 | `rebalance.lua`, 15 years |

**There is deliberately no "Buffett's current holdings".** Today's portfolio
run back over ten years is a list of what worked, chosen with the ending
already known — it says nothing about what copying him would have earned. So
every entry is something a person could have bought on the window's first day:
a published recipe, or a **13F bought the day after it was filed** (Berkshire's
Q4 2015 report went public on 16 Feb 2016, SEC accession
0000950123-16-015025). A database whose data does not cover that day withdraws
it rather than starting it elsewhere.

Each shows its caveat: the recipes rebalance quarterly where their
authors gave a band or nothing; years before a recipe was published (All
Weather, 2014) test the recipe, not anyone following it; and the 13F leaves out
Precision Castparts (bought outright before the filing) and anything your
database cannot price, scaling the rest to 100%. Weights are filled as exact
percentages, and picking one also registers a hypothesis, as the examples do.
On the bundled database the 13F copy made 11.2%/yr against the S&P 500's 15.7%
over the same days.

`rebalance.lua` now takes a custom split from the form as its `weights`
Param, the same table buy-and-hold reads; every other library strategy still
sizes its own positions. A `[[Portfolio]]` block for each lives under
*Famous investors* in `strategy_library.toml`.

### Importing the portfolio you already hold

**Import…**, beside *Add* in the holdings box, fills the card from a file of
what you own: a **CSV** (comma, tab or semicolon), an **Excel `.xlsx`** (first
sheet), or plain text with the columns separated by spaces — a table pasted out
of a spreadsheet or a web page works as-is:

```
Asset    Value(USD)
VOO      11397
BRK-B    3530
Cash     60569
```

- **Columns are found, not assumed.** The header is the early row naming the
  most recognisable columns (`Symbol`/`Ticker`, else `Asset`/`Holding`/`Name`…,
  and `Value`/`Market Value`/`Amount`/`Balance`, else `Weight`/`%`), so a
  brokerage export with account details above its header still reads. Without
  a header, the first column is the asset and the rightmost numeric column the
  amount. `$8,889`, `(1,200)` and `4.5%` all parse.
- **Amounts become exact percentages** of what matched, to two decimals and
  summing to exactly 100 — and the allocation boxes now accept decimals. Dollar
  amounts also set the **starting capital** to their sum, so the backtest starts
  from your real book; a file of weights leaves capital alone. One symbol on two
  rows (two accounts) is merged. Total lines and zero or negative amounts are
  skipped and listed.
- **A `Cash` line becomes `$CASH`**, the T-bill series — never the stock `CASH`,
  which is Pathward Financial. `USD`, `Money Market`, `Cash & Equivalents` and
  a few broker spellings are treated the same.
- **Spellings are reconciled**: `BRK.B` / `BRK/B` / `BRK-B` resolve to whichever
  the database has, and `BTC` to `BTC-USD` when only that exists.
- **Nothing disappears silently.** A note under the box says what was mapped,
  and which symbols the database cannot price were left out and what share of
  the file they were; the rest is scaled to 100%.

The import **replaces** the card's holdings and switches it to custom weights.
Legacy binary `.xls` is refused with a message to save as `.xlsx` or `.csv`.
Parsed in Go (`ui/holdings.go`, `App.ImportHoldings`) with the standard library
only. Holdings that started trading recently (an IPO in 2024) pull the window
forward like any other ticker — the dates re-snap to what every holding covers.

### Several portfolios in one run

**+ Add portfolio** appends another card, and the run carries all of them: one
`[[Portfolio]]` block each, one line per portfolio on the equity chart, one row
each in the metrics table. That is what makes a result a comparison rather than
a number — and the runner and results view have always supported it. Until now
the form emitted exactly one block, so the only way to compare two ideas was to
hand-write TOML, which is a strange thing to ask of the tab the app opens on.

A new portfolio inherits the previous one's window and starting capital (two
portfolios over different date ranges are not a comparison) with no holdings
and a fresh name. Each card carries its own tickers, dates, strategy, benchmark
and coverage verdict. **Run is enabled only when every portfolio is runnable** — and when it is
not, a line beside it says what to fix first (*To run, B: add at least one
holding.*), from the same check that disables the button (`runBlocker` in
`portfolioCheck.ts`), so the two cannot disagree —
a run carries all of them, so one broken card stops the run rather than quietly
contributing a bad block — and **two portfolios may not share a name**, because
the name is how every result, trade and verdict is labelled.

The pre-registered hypothesis stays **one per run**, not one per portfolio: the
engine registers a single claim and judges every result against it, so a box per
card would invent a concept the engine does not have.

A single portfolio generates byte-for-byte the config it always did.

### Recurring contributions, on the form

The **Portfolio** card carries a *Deposit / Withdraw* dropdown, an amount, and a
frequency (Never / Monthly / Quarterly / Annually). **Never** is the default and
emits no `[portfolio.Cashflow]` block at all, so a form you have not touched
generates exactly the config it always did — which is also why there is no
separate enable checkbox.

Direction is a dropdown rather than a minus sign. The TOML field is a signed
`Amount`, but a currency box that means the opposite thing when you type `-` is
a trap, and a mistyped sign turns a contribution plan into a decumulation one.

A line under the row says what it means in words — *"$500 added on the first
trading day of each month, beginning one month after 2015-06-01"* — because
"monthly" hides both the first-trading-day-on-or-after rule and the one-period
delay, and those are exactly what someone reconciling against a spreadsheet
will trip on.

It is **per portfolio**, so lump-sum against monthly-contributions is a
two-card comparison on one chart.

### The ticker picker knows what a symbol is

Typing in the holdings box searches **symbols and company names** and shows
both, so `apple` finds `AAPL` and `AAPL` reads as "Apple Inc." rather than as
four letters you have to already recognise. Matches rank symbol before name and
prefix before substring, so `AA` still gives you `AA` and `AAPL` first. Arrow
keys move, Enter takes the highlighted row (or whatever you typed, if none is),
Escape closes. The names also sit beside each selected holding in the
allocation list.

Names come from `App.TickerNames`: the optional `company_profile` table
(SEC EDGAR, loaded by `add_company_info.py`) plus built-in labels for the ten
`$`-prefixed benchmark series, which are synthetic and have no filer behind
them. **Both sources are optional and coverage is partial** — about 5,600 of
the priced symbols have a name, because `company_tickers.json` lists only
current filers. A database with no `company_profile` table shows bare symbols
and works exactly as it did before. Symbols carrying a generation suffix
(`COR.1`, from `split_recycled.py`) are deliberately left unnamed: that series
is the symbol's *previous* occupant, a different company, and the current
filer's name would be wrong rather than missing.

**Strategy** is a picker fed by the shipped library's gallery headers
(`App.ListStrategies` over `backtest.LoadGallery`). Choosing one shows what it
does, what it needs to work, and — the useful part — how it fails, before the
run rather than after a bad result:

> **Works when** trends run far beyond the point they look extended.
> **Fails when** range-bound markets: every breakout fails back into the
> range, and the win rate is low by design even when it works.

The default is buy-and-hold, which is the mode's original behaviour: it runs
`strategies/buy_and_hold.lua`, splitting equally, or passing your custom
percentages as `[portfolio.Params] weights`. Deposits from a cashflow schedule
are invested the day they arrive, split the same way. Picking a library strategy emits
`Strategy = "lua:strategies/<file>"` and runs it on its own defaults —
the strategy sizes its own positions, so the allocation percentages are not
consulted and the form says so — except `rebalance.lua`, which holds them as
its target weights. To tune its parameters, switch to Advanced and
add a `[portfolio.Params]` block.

**Compare it against** sets `Benchmark`, defaulting to `$SP500`. This is the
one simple-mode default that is *not* a zero value, and it is deliberate: with
`Benchmark` unset, Alpha, Beta, TrackingError, InformationRatio and
Up/DownCapture are all zero and "did this beat just buying the market?" — the
question a user actually has — is unanswerable. The portfolio's own numbers do
not change; the benchmark is scored against, never traded, and stays out of
`Tickers` so it receives no capital and does not constrain the trading
calendar. Choose *Nothing — skip the comparison* to get the previous output
back exactly.

The engine skips every benchmark-relative metric unless the benchmark has a bar
for **every trading day in the window**, and says so only in a warning diagnostic
(the form keeps the window inside the benchmark's for this reason).
The Backtest tab checks it up front instead: the `$`-series currently end weeks
earlier than the priced tickers (`$SP500` to 2026-07-02 against 2026-08-07 for
real symbols), so the date presets snap to the earlier of the two ends, and a
hand-typed end date past it gets a warning with a one-click fix.

**Before you run** is pre-registration, and it is the reason the mode exists in
this order: you say what you think is true, and what would count as being
right, *before* seeing a number. The engine has judged hypotheses mechanically
for a while, but it wanted hand-written TOML, so nobody on the Backtest tab ever saw
a verdict. The card builds the same document from a claim and a
metric/operator/number triple — which is the whole grammar `ParseCriterion`
accepts:

```toml
ID          = "H-11eyu7q"
Claim       = "12-1 momentum beats buy-and-hold"
Criterion   = "sharpe >= 1.2"
```

The metric list is served by the engine (`App.CriterionMetrics` over
`backtest.CriterionMetricNames`), so the picker cannot offer a criterion the
parser would reject. The `ID` is derived from the question itself, which makes
*"a changed question is a new ID"* structural rather than a rule to remember:
edit the claim or the threshold and the next run registers a new id, while
re-running the same question keeps it. Leaving the claim empty runs the
backtest unjudged — different from judging it against a default nobody chose.
**Edit as TOML →** carries the question across to the Config tab's raw box.

`my_strategy.lua` is deliberately absent from the picker: it is the Lua tab's
scratch file, not a strategy. `buy_and_hold_weighted.lua` and `example.lua`
were folded into `buy_and_hold.lua`; a saved config that still names either
runs `buy_and_hold.lua`.

### Reading the results

The results view leads with what decides whether to keep looking: the
hypothesis verdict, the "does this survive the obvious objections?" strip, the
equity curve, and the metrics table. Everything below those — walk-forward
windows, in-sample vs out-of-sample, the ten deepest drawdowns, trading,
attribution, calendar-year and monthly returns — is **collapsed behind its own
title** and opens on click, with the choice remembered across runs.

A panel that has nothing to report is hidden rather than collapsed, so an
absent title means absent data (no `[portfolio.Sweep]`, no
`[portfolio.Validation]`, no trades) rather than a closed section.

**Any panel can leave the strip for a window of its own.** Double-click its
header, or press **⤢** at its right edge. The window drags by its header,
resizes from its bottom-right corner, maximises on another double-click (or
**□**), and goes back with **×** or **Esc**; clicking a window brings it to
the front. The equity curve grows to the window's height, so popping it out is
how to read it at a real size. The strip keeps a one-line stub — *in its own
window* — with **Show** and **Dock** buttons, and which panels are out, and
where, is remembered across runs and launches. A window is always pulled back
on screen, whatever the display did since.

The bar between the tab and the results drags to resize and remembers its
height; so does the assistant panel's left edge (double-click that edge to
reset its width). Its **▁** button minimises the results down to the bar,
giving the tab the whole window, and **□** maximises them over the tab;
pressing the same button again (**▴** / **❐**) restores, and dragging the bar
restores from either. **Double-clicking the bar** steps through the same
states like a window's title bar: minimised → normal → maximised → normal.

**The results start minimised** at every launch — results are never restored,
so an open panel would only be an empty box — and open by themselves the
first time a run produces results. Maximised never survives a restart, so
the app never opens with its tabs hidden.

The **assistant** maximises the same way: double-click its header bar (not
its model picker or buttons), or its **□** button, and it takes the whole
row while the tabs step aside; again (or **❐**, or Esc) restores.

### Keyboard shortcuts

**F1** (or the **⌨** chip in the top bar) lists them. **On a Mac every one is
pressed with Cmd instead of Ctrl** — Cmd+Shift+P, Cmd+Enter, Cmd+1 — and the
list, the palette and the button tooltips print Cmd there. Zoom follows the
browser convention, Ctrl/Cmd with **=**, **-** and **0**; Ctrl+↑/↓ and
Ctrl+scroll still work too, but not with Cmd, since Cmd+arrow moves the caret
on a Mac.

| keys | does |
|---|---|
| Ctrl+Shift+P | **Command palette** (Ctrl+K also opens it): every action by name — type any words of it, ↑/↓, Enter. Includes the actions with no key (Check config/script, Open config file…, Choose database…, zoom), and shows each one's shortcut, so it also teaches them. An action that does not apply here (Save on the Backtest tab) is listed greyed out with the reason. |
| / | Jump to the ticker search on the Backtest tab (only when you are not typing in a field — there it types a `/`) |
| Ctrl+Enter | Run the backtest — the visible Run button, so it is refused exactly when the button is disabled, and then the status line says why (*Can't run yet: add at least one holding.*). On the Data tab it runs the query, as before. |
| Ctrl+1 / 2 / 3 / 4 | Backtest / Config / Data / Research tab (Ctrl+4 also puts the cursor in the Research command line) |
| Ctrl+J | Show or hide the assistant |
| Ctrl+Shift+J | Maximise or restore the assistant |
| Ctrl+M | Minimise or restore the results |
| Ctrl+Shift+M | Maximise or restore the results |
| Ctrl+S | Save the config and scripts (Config tab); never the webview's "save page" |
| Esc | Close the palette or the shortcut list, else restore a maximised pane; otherwise it reaches whatever has focus |
| Ctrl+= / Ctrl+- / Ctrl+0 | Zoom in / out / reset (also Ctrl+↑ / ↓ and Ctrl+scroll) |

**While a run is going** the status line counts up beside *Running…*
(`7s`, `1m 05s`), so a long run reads as working rather than frozen. The
timer is its own tiny component: its once-a-second tick redraws that span
and nothing else.

**Startup.** The code editor (Monaco) is most of the app's JavaScript and the
Backtest tab never shows it, so it is not in the startup bundle: it loads in
the background just after the first screen draws (`monacoSetup.ts`,
`LazyEditor.tsx`), and is ready long before the Config or Data tab is opened.
The startup bundle went from 4.0 MB to 263 KB.

The table lives in `ui/frontend/src/shortcuts.tsx`; the overlay, the palette
(`commandPalette.tsx`) and the button tooltips read it, and a key and a palette
pick run the same code, so they cannot drift from what the keys do.

## The Research tab (UI)

A reader for **primary sources**, and nothing else: a company's SEC filings,
your watchlist's, a full-text search of every filing since 2001, and a news
stream of the official economic releases and your watchlist's material
events. It shows no quotes, financials, earnings dates, economic data series
or media headlines — the views that did (DES, FA, ERN and ECO) were removed
to keep the tab on primary documents. Everything it shows comes straight
from SEC EDGAR or the government agency that published it, with its date.

Type a ticker or a company name and press Enter (`Ctrl+4` jumps here with the
cursor in the box):

| command | shows | from |
|---|---|---|
| `AAPL` or `AAPL CN` | the company's **own news**: its recent SEC filings, newest first, each in plain words — an 8-K by what it reports (*Earnings results*, *Directors or officers changed*, *Material agreement*, *Bankruptcy*, …), 10-K/10-Q, 13D/13G stakes, proxies, insider forms — as a sortable table by filing date (form, what, period covered). Click one to read it in the app laid out as sec.gov shows it, with the document's own table of contents beside it; an earnings 8-K opens on its press release (EX-99.1) | SEC EDGAR, live (keyless) |
| `MY` | **your watchlist's filings**, no ticker needed: one sortable table over the last 90 days of every company on it (filing date, ticker, form, what — events, reports and proxies as CN labels them). Click a filing to read it. Add and remove tickers in the view | `watchlist.json` in the config directory, SEC EDGAR |
| `NEWS` | **the news, without the noise**, no ticker needed: one sortable stream over the last 90 days of the official economic release notices (BLS, BEA, Census — each in the agency's own words) and the *material* events your watchlist's companies filed (8-K results, deals, bankruptcy, leadership changes, restatements, write-downs). *All / Economy / Watchlist* filters it. Both open in the reader: a release as the agency publishes it, an event as the filing | bls.gov, bea.gov, census.gov feeds (keyless), SEC EDGAR |
| `"going concern"` or `"share repurchase" AAPL` | **full-text search** of every SEC filing since 2001: words in quotes are an exact phrase, a ticker narrows to that company. Each row is a document whose text matches — an 8-K's press release says *in EX-99.1* — and opens in the reader. Past year by default (or past 30 days, or since 2001), narrowed by form; a sortable table, newest first, whose `#` column is EDGAR's relevance rank | EDGAR full-text search, live (keyless) |

Case does not matter, and a name resolves to the best-matching symbol
(`apple` → `AAPL`) the same way the Backtest tab's ticker search does.
**Every table sorts**: click a column header, click again to reverse (dates
start newest first). A lone `MY` is the watchlist and a lone `NEWS` the news
stream; the tickers themselves are `MY CN` and `NEWS CN`. Anything in quotes is a search, wherever the ticker goes
(`AAPL "buyback"` works too).

**CN reads SEC EDGAR live**, because filings are where company news starts: an
earnings headline is a rewrite of the 8-K's exhibit 99.1, an executive
departure is an 8-K item 5.02, an insider sale is a Form 4. Reading the filing
skips the rewrite, the adverts and the spin.

- **Filters.** *Events & reports* is the default and leaves out insider forms
  (3/4/5/144), which for a large company are most of what it files and bury
  the events; *Insiders* and *All* bring them back.
- **Dates, not times, for now.** The list and the reader show each filing's
  filing date. EDGAR's acceptance times are four hours off for some filers
  (below, and T143); the time comes back once that is settled.
- **The reader** shows the document as sec.gov does: its own HTML, tables,
  fonts and page breaks, on white paper in either theme. A 10-Q or 10-K
  gets its own table of contents (Part I, Item 1. Financial Statements, Item
  1A. Risk Factors, …) as a sidebar, read from the document's internal links;
  clicking an entry, or a link in the document's contents page, jumps within
  the reader, and a link out of the filing opens in the browser. Opening a
  filing makes it the focus: the view widens to the full window, with the
  list, the contents and the document each in a pane of its own that fills
  the window's height and scrolls by itself. The bars between them drag to
  resize (double-click one to reset; the widths are remembered), *Contents*
  hides or shows the contents column, and a narrow window stacks the list
  over the reader. *A− / A+* sets the text size (130% of print by default,
  remembered), and double-clicking the reader's bar — or its □ button —
  maximises it to fill the window; Esc or another double-click restores. The HTML is
  sanitized first (`src/edgar/render.go`): no scripts, event handlers, forms,
  frames, stylesheets or CSS that loads anything, and the hidden inline-XBRL
  header is dropped while the tagged figures stay as plain text. Plain-text
  and XML documents (pre-2001 `.txt` filings, raw Form 4 XML) are shown as
  text. *On sec.gov ↗* opens the original. It fetches only documents under
  `www.sec.gov/Archives/`, so a link cannot point it anywhere else. Very long
  documents are cut at 8 MB of markup (400,000 characters of text) and say so.
- **Polite by construction.** Every request carries `SEC_CONTACT` in its
  User-Agent (from the environment or `.env`, else the same default as the
  Python loaders), the whole app stays under ~8 requests a second, 429s and
  5xx back off and retry, and a filing list is cached for five minutes and a
  document for a day — in memory only; nothing is written to the database.
- **EDGAR's API mis-states acceptance times.** `acceptanceDateTime` in the
  submissions JSON is not the UTC instant its `Z` claims: it is the Eastern
  time shifted by *twice* the UTC offset (+8 hours in summer, +10 in winter).
  Apple's July 2026 earnings 8-K reads `2026-07-31T00:30:28Z` there and
  *Accepted 2026-07-30 16:30:28* on EDGAR's own filing page. The app corrects
  it; `TestAcceptedTimesMatchEDGARsFilingPages` pins four filings, summer and
  winter, against those pages.
- CN finds a company by its SEC CIK from `company_profile`, so it needs that
  table, and works for current filers only (delisted symbols have no profile).
- Wire-service press releases and media headlines are deliberately absent.

**MY is CN across a watchlist.**

- **The list is yours, not the database's**: `watchlist.json` in the per-user
  config directory (`~/.config/backtester` on Linux), beside the API key — a
  shipped database may be read-only, and a watchlist is a preference, not
  data. The first time you open MY with no list saved, it is seeded from the
  Backtest form's holdings; after that, *Add the form's holdings* offers any
  you have added since. Emptying the list is kept as a choice — it does not
  re-seed. Up to 50 tickers, upper-cased, repeats dropped.
- **One quarter, newest first**: 90 days, so every company's latest quarterly
  report is in it, at most 300 filings. Only events, reports and
  proxies are shown. Insider forms and securities offerings would bury the
  rest (JPMorgan alone files thousands of structured-note prospectuses a
  quarter). Ownership stakes (13D/13G) are left out because a company's EDGAR
  list mixes stakes in it with stakes it holds elsewhere — most of
  JPMorgan's are the latter — and the record does not say which.
- **Filings show their filing date, not a time**, until T143 is settled:
  EDGAR's API is four hours off for some filers (Microsoft and NVIDIA) and
  not others (Apple).
- A ticker with no SEC filer (a fund, a delisted symbol) or that EDGAR does
  not answer for is named in a note; the rest of the feed still shows. Each
  company is one EDGAR request, paced like CN's.

**NEWS is what was published, not what was said about it.** Two halves, each
shown as soon as it answers — the agencies' feeds in one call, the
watchlist's events a company at a time, like MY.

- **Economic releases, allow-listed.** Each agency's feed carries plenty that
  moves nothing, so only the headline series are kept (`src/news`):
  - *BLS*: Consumer Price Index, Employment Situation, Producer Price Index,
    JOLTS, Employment Cost Index, productivity and costs, import and export
    prices — one Atom feed per release.
  - *BEA*: GDP, personal income and outlays (PCE), international trade,
    international transactions. Its regional, territorial and satellite
    accounts (outdoor recreation, arts, marine economy, …) are dropped.
  - *Census*: retail sales, durable goods orders, housing starts, new home
    sales, construction spending, factory orders, business inventories,
    advance goods trade. Its copy of the trade report is dropped (the
    release is joint with BEA, which carries it), as are the quarterly
    surveys, business formation and vacancy rates.

  Each release shows the agency's own headline and lede, and BEA's next
  release date with a countdown. Feeds are cached for 15 minutes, in memory;
  one agency failing is a note and the rest still show.
- **Releases read in the app.** Clicking one opens the release itself in the
  reader, beside the list: the agency's own page with its banner, menus and
  footer cut away (BLS's `#bodytext`, BEA's release body, Census's release
  text), sanitized exactly as a filing is — no scripts, forms, frames or
  tracking — with its tables and charts kept. Census's feed links each
  indicator's landing page, which describes the survey, so NEWS links the
  indicator's *current release* page instead. *On bls.gov ↗* (or bea.gov,
  census.gov) opens the original. The reader fetches only https pages on
  www.bls.gov, www.bea.gov and www.census.gov; a page whose layout an agency
  has changed says so instead of showing the whole site.
- **Material events only.** From the same per-company EDGAR feeds MY reads,
  NEWS keeps an 8-K only when it reports an item an investor acts on:
  material agreements and their termination (1.01/1.02), bankruptcy (1.03),
  cybersecurity incidents (1.05), completed acquisitions (2.01), earnings
  (2.02), restructuring (2.05), impairments (2.06), delisting notices (3.01),
  auditor changes (4.01), restatements (4.02), change in control (5.01) and
  officer or director changes (5.02). The routine ones a large company files
  weekly — new debt (2.03), Regulation FD slides (7.01), vote results (5.07),
  bylaws (5.03), "other events" (8.01) — are left out (`edgar.Material`).
- **Deliberately absent:** media and wire stories, the Fed and the
  regulators. A source joins only if it is the primary publisher and its
  items move prices.

**Search is EDGAR's own full-text search**, the service behind
efts.sec.gov, over the text of every filing since 2001:

- **Relevance is EDGAR's, dates are yours.** EDGAR returns matches ranked by
  relevance, 100 at a time; the `#` column keeps that rank. The table opens
  newest first, which re-orders the page shown, not all of EDGAR, which is
  why the default range is the past year —
  inside a year, the 100 most relevant are usually all of them. *Since 2001*
  and *Next 100* reach the rest; the form chips (with EDGAR's counts) narrow it.
- **A row is a document, not a filing.** A press release matching inside an
  8-K is listed as the 8-K, labelled as CN labels it, *in EX-99.1*; clicking
  opens that document, not the cover page. A holder's filing about a company
  names both (*with ICAHN CARL C*).
- A ticker needs `company_profile` for the CIK, which EDGAR's index matches
  only as ten digits. Without a ticker no database is needed at all.
- No snippets: EDGAR's search returns which documents match, never the
  sentence, so the reader is where you see it.

Code: `ui/frontend/src/Research.tsx`; CN is `src/edgar` (the client, parsing
and the reader's sanitizer, tested against SEC responses recorded in
`src/edgar/testdata`) and `ui/filings.go`; MY is `ui/watchlist.go`; search is
`src/edgar/search.go` and `ui/search.go`; NEWS is `src/news` (the feeds,
their allow-lists and the release reader, tested against feeds and release
pages recorded in `src/news/testdata`),
`ui/news.go` and `edgar.Material`.

## AI assistant (UI) — Claude or local Ollama models

The Wails UI has a built-in AI chat, toggled with the **✦ Assistant**
button in the topbar. It is specialized for this app: its system prompt
carries the TOML config schema, the strategy spec strings, the full Lua
strategy API, the metric definitions, the live schema of the chosen DuckDB
(every table, bar counts, date span, available `$`-benchmarks), the saved
`*.toml` configs and `strategies/*.lua` library, and whatever TOML/Lua is
open (even unsaved) in the editors.

- **Models**: the header dropdown lists the Claude models (Sonnet 5 /
  Opus 4.8 / Haiku 4.5) plus every model your local Ollama server has
  pulled (labelled "(local)", discovered live from `/api/tags`;
  `OLLAMA_HOST` overrides the default `localhost:11434`). The choice is
  remembered across restarts.
- It cannot refresh the whole database — that is `ui --sync <db>`, run from
  a terminal with the app closed, and the assistant will tell you so. See
  [Keeping the price data current](data.md#keeping-the-price-data-current).
- It can query the database itself through a read-only SQL tool
  (SELECT-family statements only, capped at 100 rows) — e.g. to check
  ticker coverage before proposing a date range. Queries it runs are shown
  inline in the chat. Local models get the tool too if they advertise the
  `tools` capability (qwen3.5 does); models that don't simply chat over
  the embedded context.
- **It writes strategies as rules first.** Asked to turn an idea into a
  strategy, it proposes a [`[portfolio.Rules]`](configuration.md#portfoliorules) block and
  writes Lua only when the menu cannot say what you mean — and then says
  what the menu was missing. Before showing a rule set it calls the
  `check_rules` tool, which runs the rules through the same construction a
  backtest does and returns either the errors or the engine's own sentence
  for them (`DescribeRules`) plus the window the database covers for those
  tickers. The assistant shows you that sentence verbatim, so what you
  confirm is the engine's reading of the strategy, not the model's
  paraphrase. When your idea leaves out how often to re-decide or what to
  hold when the market turns, it proposes a default and says so ("checked
  monthly — say if you meant weekly") rather than choosing silently.
  Small local models follow this less reliably than Claude: in testing,
  qwen3.5:4b called `check_rules` on about half of attempts and otherwise
  wrote rules from memory, once with an entry that is not on the menu — the
  app's ✓ Check catches those before a run.
- Fenced ` ```toml `/` ```lua ` blocks in its replies get an
  **Insert into … editor** button, so a generated config or strategy lands
  directly in the corresponding editor tab.
- Setup: Claude models need an Anthropic API key — set
  `ANTHROPIC_API_KEY` in the environment or a `.env`, or paste it into the
  panel's first-run form, which writes it for you. The form writes the
  repo-root `.env` when there is one (a checkout, alongside the other project
  keys) and `~/.config/backtester/.env` when there is not — an installed
  binary may sit somewhere unwritable, and silently failing to save a key
  someone just typed is the worst available behaviour. Reads go the other way
  round: environment, then `~/.config/backtester/.env`, then the project's,
  so the last key you saved is the one in use. Local Ollama models need no
  key at all.
- Local-model notes: the request pins `num_ctx` to 16384 (Ollama's default
  is too small for the embedded context) and sends a leaner system prompt;
  "thinking" models have thinking disabled for snappier replies.
  The lean prompt is a *different, condensed reference* (`leanReference` in
  `ui/chat.go`), not the full one with the saved-config dump removed: the
  full reference is ~19k tokens on its own and does not fit `num_ctx` with
  a conversation and tool results on top, so a local model was silently
  losing the tail of its own prompt — which is where the tool routing
  lived. The lean one runs ~4.5k tokens with the live database context
  included, and keeps the config schema, the Lua surface, every metric
  name, the tool routing and the data traps; it drops narrative the model
  can recover with `query_db`. `TestLeanPromptFitsOllamaContext` budgets it
  at a third of the window so it cannot drift back over.

Backend: `ui/chat.go` (shared prompt/tool/dispatch), `ui/claude.go`
(streaming Anthropic client), `ui/ollama.go` (streaming Ollama client),
`data.RunQuery` in `src/data/database.go`; frontend:
`ui/frontend/src/ChatPanel.tsx`. Live tests: `go test ./ui` (the Ollama
ones skip when no server is running).
