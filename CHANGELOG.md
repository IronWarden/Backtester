# Changelog

What changed, per release. Dates are the release date, not the last commit.

The version this file describes is in `src/version/version.go`, and the running
app shows it beside its name — quote that in a bug report.

## 0.1.0-beta.1 — unreleased

The first release anyone but its author can run. The engine has been usable for
months; what this release adds is everything around it.

**It runs outside its own checkout.** Paths resolve from the executable as well
as the working directory, the Lua strategy library is embedded in the binary
(an on-disk copy still wins), the API key goes to `~/.config/backtester`, and
the app asks Go where the database is instead of assuming `../stock_data.db`.

**It opens a database it did not create.** A read-only file is copied once to
`~/.local/share/backtester` and opened there; if that is impossible it opens
read-only and says so. A database already open elsewhere is reported as
"already open in <program> (PID n)" rather than as a DuckDB lock error.

**A first run you did not have to invent.** Starter examples fill a whole
portfolio from one click, starter baskets fill an empty holdings box, and the
success criterion can be picked in English. The form is remembered between
launches.

**Results you can read.** Every result opens with a sentence saying what
happened — what the money did, the worst fall, how it compares to the
benchmark. The metrics table shows six columns by default and all twenty on
request, and a collapsible guide explains which panels decide whether an idea
was any good.

**Writing a strategy in Lua, without holding the API in your head.** The editor
completes and explains all 35 globals. "✓ Check script" compiles a script and
dry-runs it against synthetic prices without a backtest, with strict globals so
a misspelled name is an error rather than a silent `nil`. Errors name a line
you can click. `plot(name, value)` draws what the strategy saw. Three starting
shapes replace the blank file, and "Run this script" runs it against the
holdings already on the Backtest tab.

**Honesty about the data.** A holding the data audit calls unusable or suspect
produces a warning naming the defect (nothing is excluded). Portfolios covering
different windows say so under the chart.

### Known limitations

- **Linux only.** The build needs `-tags webkit2_41`. The `build/darwin` and
  `build/windows` directories are Wails scaffolding and have never been built
  or tested.
- **The universe has no delisted companies**, so backtests are
  survivorship-biased. See `DATA-LICENSE.md`; it cannot be fixed without paid
  data.
- **273 price series are unusable** and 1,053 more carry defects large enough
  to move a metric. The app warns; it does not refuse.
- Prices are adjusted closes, not what traded on the day.
- The beta's licence terms end at 1.0, and its numbers may be wrong. Nothing it
  produces is investment advice.
