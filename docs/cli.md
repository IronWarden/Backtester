# Command line

Running backtests, sweeps, walk-forward and robustness checks from `src/`.

[← README](../README.md) · [Installation and setup](installation.md) · [Data](data.md) · [Configuration](configuration.md) · **Command line** · [The desktop app](app.md) · [Architecture](architecture.md)

## Running

The binary expects to be launched from `src/` because it resolves `../stock_data.db` and `../config.toml` relative to the working directory.

```bash
cd src
go run main.go                          # quiet run; logs are discarded
go run main.go -debug                   # writes backtester.log + transactions.log, and serves pprof on :6060
go run main.go -config ../strategy_library.toml
go run main.go -scan-signals             # screen signals instead of backtesting
go run main.go -event-study              # what happened after drawdowns, breakouts
go run main.go -list-strategies          # the gallery: what each script needs and how it fails
go run main.go -check                    # validate the config and exit; opens no database
```

To build a binary:

```bash
cd src
go build -o backtester
./backtester -debug
```

### Finding a strategy: screen signals first

A backtest is an expensive way to learn that a signal contains no information.
`-scan-signals` asks the cheap question first — does this signal rank tomorrow's
winners above tomorrow's losers, across the universe? — and only the ones that do
are worth writing a strategy around.

```bash
cd src && go run main.go -scan-signals
```

It reads the tickers and window from your config, screens a built-in library of
twelve signals at 5-, 21- and 63-day horizons, and prints them strongest first:

```
Signal scan: 30 tickers, 2637 trading days

mom_252               5d  IC +0.0338  t  +2.34  hit  55.3%  Q5-Q1   0.253%  n=476
mom_126               5d  IC +0.0276  t  +2.02  hit  53.3%  Q5-Q1   0.173%  n=501
sma_50_200           21d  IC +0.0502  t  +1.77  hit  57.4%  Q5-Q1   1.065%  n=115
lowvol_21            21d  IC -0.0374  t  -1.45  hit  42.7%  Q5-Q1  -1.062%  n=124
```

- **IC** is the cross-sectional Spearman rank correlation between the signal and
  the forward return, averaged over sampled days. **Real ICs are small** — 0.02
  to 0.05 with |t| > 2 is a genuinely usable signal, and anyone expecting a
  correlation will misread that as nothing.
- **hit** is the share of days with a positive IC. A good mean IC with a hit rate
  near 50% is a few lucky days, not an edge, and the two columns together catch
  that where either alone would not.
- **Q5-Q1** is the top-fifth minus bottom-fifth forward return: the IC in money.
  Withheld below 10 tickers, because a "quintile" of eight names is one stock
  wearing a portfolio's clothes.
- **Coverage** is reported when it matters. A day is skipped when fewer than 5
  tickers have a value (a warm-up or a short history) or when every ticker scores
  the same (no ordering to correlate). Past a quarter of candidate days, the row
  says so and names the reason — the two are different problems, one fixed by
  adding tickers and one a property of the signal:

  ```
  mom_252  5d  IC +0.0338  t +2.34  n=476  (312 of 500 days skipped: fewer than 5 tickers had a value)
  ```

  It is context, not a refusal: a scan with too few samples is already refused.
- **n** is the number of **non-overlapping** samples. This is the honesty knob:
  scoring every day against a 21-day forward return reuses each return 21 times
  and inflates the t-statistic by roughly √21, so the scan steps by the horizon
  instead. It is why `n` is 115 rather than 2,600 at the 21-day horizon.

Signs are chosen so a **positive IC always means "high signal predicted high
return"** — hence `lowvol_21` is *negated* volatility and `rev_5` is *negated*
one-week return. The example above reads as: 12-month momentum and the 50/200
trend ratio had modest predictive power on those 30 large caps, and low
volatility did not — high-volatility names outperformed over that window.

Three refusals, all deliberate: fewer than 5 usable tickers on a day is not a
cross-section and the day is skipped; fewer than 12 non-overlapping samples is an
anecdote and no summary is printed at all; and a signal that is constant or
undefined produces no row rather than a `NaN`.

The signal library covers trailing return over 21/63/126/252 days, 12-1 momentum,
one-week reversal, realised volatility at 21 and 63 days, distance from the
52-week high, the 50/200 SMA ratio, a volume trend, and volatility-scaled
momentum. Each is a pure function of one ticker's bars **up to and including the
scored day** — a signal is handed an index and can never see the forward window
it is being judged against, which is asserted by a test rather than by
convention.

**A screen is not a backtest.** It ignores costs, position sizing and when you
could actually trade. A signal that survives here still has to survive a real run
with `[portfolio.Costs]`, an out-of-sample split and walk-forward.

### What happens after X? Event studies

"Buy after a 10% drawdown", "buy the breakout", "fade a three-sigma day" — a
large class of strategy ideas, answerable in seconds without writing one:

```bash
cd src && go run main.go -event-study
```

It finds every occurrence of each built-in event across your universe and reports
what followed, at 5, 21 and 63 days. Real output, 30 large caps over 2016–2026:

```
drawdown_20  63d  n= 214  abnormal  +0.97% (median +0.39%)  raw  +8.12%  t +0.87  hit 51.4%
drop_3sigma  21d  n= 760  abnormal  -0.24% (median -0.59%)  raw  +1.41%  t -0.96  hit 45.9%
high_52w      5d  n=2016  abnormal  +0.09% (median +0.06%)  raw  +0.29%  t +1.16  hit 51.2%
```

Read the first row carefully, because it is the whole point of the feature.
"Buy the 20% dip" returned **+8.1% over the next quarter** — and **+0.97% net of
what the rest of the universe did over the same days**, with a t of 0.87 and a
hit rate barely over half. The strategy is almost entirely the market recovering,
not the dip. A raw-return study would have called that an edge.

- **abnormal** is the return after the event net of the equal-weight return of
  the universe over the same window. It is the number that matters; `raw` is
  shown beside it so the gap is visible.
- **n** counts **non-overlapping** occurrences. A drawdown that persists fires
  on twenty consecutive days, and counting each as independent evidence inflates
  everything — one occurrence per ticker per horizon.
- **median** is reported because one 400% recovery can carry a mean on its own.
- Below 10 occurrences nothing is reported at all: a confident mean over a dozen
  events is how a backtest lies.

Built-in events: `drawdown_10`, `drawdown_20` (crossing 10%/20% below the 1-year
high — the *crossing*, not the state), `high_52w`, `drop_3sigma`, `jump_3sigma`
(a day beyond three times its 1-month volatility) and `volume_spike`.

Earnings and macro-release events are deliberately absent for now. They need a
database read inside the run path and, more importantly, the publication-lag
rules — an earnings event dated at the fiscal period end rather than the report
date would make every study of it look wonderful and mean nothing.

### Desktop UI

```bash
wails dev -tags webkit2_41              # live-reloading development build
wails build -tags webkit2_41 && cp build/bin/ui ui/ui
```

#### Checking a config before running it

The **✓ Check** button next to Run validates the config in the editor without
running it — the UI half of `-check`. It opens no database and simulates
nothing, so it answers immediately even for a config whose run would take
minutes. The first error it finds is shown in the status bar (see *Where a
run's problems show up* below). Errors
mean the config will not run as written; warnings mean it will run but may not
do what was meant.

#### Saying what you are hoping for

Pre-registering a claim is the most useful habit the app can teach and the
hardest one to start: the criterion is a metric, an operator and a number, and
`sharpe >= 1.2` means very little on a first run. So under the criterion row
there are presets:

| preset | the criterion it fills |
|---|---|
| Better than a coin flip | `p_value <= 0.05` |
| Steady enough to hold | `sharpe >= 1` |
| Never fell more than 30% | `max_drawdown <= 30` |
| Made at least 8% a year | `annual_return >= 8` |

They **fill** the existing controls rather than replacing them, so one click
also shows you what the phrase meant and leaves you free to change the number.
A preset only appears if the engine actually serves its metric, so the form
cannot offer a criterion the run would reject. If the claim box is empty a
preset fills that too; if you have written your own words, they are left alone.

#### Keeping bad input out of the form

The Backtest tab refuses input that would make a run fail, rather than
reporting it afterwards. Every rule is a pure function in
`ui/frontend/src/portfolioCheck.ts`, pinned by the smoke run:

- **Holdings** come only from the database's own list. Enter and **Add** take
  the highlighted suggestion, else an exact symbol, else the best match — never
  free text — and a symbol whose history does not overlap what is already held
  is not offered at all.
- **Dates** accept only digits (dashes are inserted as you type) and are
  disabled until there is a holding to take a window from. On leaving the box a
  typo falls back to the last good date, anything outside the window every
  holding covers is pulled inside it, and a start after the end moves the
  *other* date so what you typed is what you keep. The window also stops at the
  benchmark's last bar when the two overlap, because a window past it silently
  drops every comparison metric. The **Compare it against** list disables a
  benchmark this database does not hold.
- **Names** are made unique when you leave the box (`Value` → `Value (2)`), and
  a blank one becomes `Portfolio N`.
- **Money** boxes take digits only, up to $999 billion; a starting capital left
  at zero goes back to $10,000.
- **The criterion value** accepts only a signed decimal.

#### Where a run's problems show up

There is no console panel. What the form cannot prevent — a Lua script that
raises, a hand-written TOML config — surfaces in two places: the **first
error** of a run or ✓ Check appears as one line in the status bar (`+N more`
when there are several), and a Lua error that names a line is drawn as a
marker in the editor's gutter. Warnings and `log()` notes are not shown in the
app; a CLI run prints every diagnostic to its log (stderr, or
`backtester.log` under `-debug`), and `-check` prints the config-level ones.
The diagnostics the engine produces:

| level | what produces it |
|---|---|
| `info` | a strategy calling `log()` — the script talking, not a defect |
| `warn` | a `[portfolio.Params]` key the strategy never reads — usually a typo, and in a sweep it means every trial ran identical settings |
| `warn` | `Params` or `Sweep` keys on a **built-in** strategy, which cannot read either — including on a `[portfolio.WalkForward]` block, where identical candidates make the selection meaningless |
| `error` | a Lua strategy that failed to load, or raised on a given day |
| `error` | a portfolio that simulated nothing — no tickers, or no trading day covered by all of them, with each ticker's real range named |
| `warn` | fewer days simulated than the window covers, and which ticker is short |
| `warn` | a benchmark with no data, or one not covering every trading day |
| `warn` | a holding the data audit calls `unusable` or `suspect`, naming the defect — see *What is wrong with the bars themselves*. Nothing is excluded; the run includes it |
| `info` | this database has no `data_quality` table, so no holding was checked (said once per session) |
| `warn` | an in-sample/out-of-sample split with too few days either side to score |

Each carries the level, the portfolio it belongs to, and the message. The
engine's own logging — query timings, row-scan warnings, the `PrintMetrics`
dump — is not a diagnostic; the split is decided in Go at each call site, by
`backtest.diagf`, not filtered afterwards.

At most 200 diagnostics are kept per run; beyond that a final one says how many
were suppressed. A strategy that raises on every trading day is the case this
bounds.
