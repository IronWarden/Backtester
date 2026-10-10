# Installation and setup

Requirements, building from a fresh clone, where the app looks for its files, and what a release contains.

[← README](../README.md) · **Installation and setup** · [Data](data.md) · [Configuration](configuration.md) · [Command line](cli.md) · [The desktop app](app.md) · [Architecture](architecture.md)

## Prerequisites

- Go 1.25+ (module pinned to `go 1.25.0`).
- A DuckDB file named `stock_data.db` in the repository root (or any file you
  open with the app's database picker) containing:
  - `stock_data_optimized(Date, Ticker, Open, High, Low, Close, Volume)`
  - `"3MTreasuryYields"(Date, daily_risk_free_rate_decimal)`

  Don't have one? [Build your own](data.md#build-your-own-database) with one command.
- A `config.toml` in the repository root (see [Configuration](configuration.md)).

Dependencies (`github.com/duckdb/duckdb-go/v2`, `gonum.org/v1/gonum`, `github.com/BurntSushi/toml`, `github.com/yuin/gopher-lua`, and the Wails runtime) are pulled via `go mod`.

**Keep every DuckDB that writes the file on the same version line.** The app
bundles DuckDB **1.5.x** (`duckdb-go` v2.10505). The Python loaders use
whatever `pip` installed, and a newer DuckDB can write storage an older one
cannot read — on 2026-09-27 a loader run under Python DuckDB 1.5.0 left a
database that the app's previous engine (1.1.3) and a 1.3.2 CLI could read one
ticker at a time but failed on any scan across tickers, with an `INTERNAL
Error: Attempted to access index 9223372036854775807`. The data was intact;
only the older readers were stuck. So: use `pip install "duckdb>=1.5,<1.6"`
for the loaders, a 1.5 CLI for inspection, and upgrade all three together.

### Building from a fresh clone

`go build ./...` fails on a clean checkout with:

```
pattern all:frontend/dist: no matching files found
```

This is expected. `ui/main.go` embeds `frontend/dist`, which is a build output ignored by `ui/.gitignore`, so the frontend has to be built before the Go build can embed it — normal for a Wails project. Either build the whole app:

```bash
wails build -tags webkit2_41   # the webkit tag is required on this setup
```

or just the frontend, if you only want `go build ./...` to succeed:

```bash
cd ui/frontend && npm install && npm run build
```

Do not "fix" this by committing `dist`. The engine itself needs no such step: `go build ./src/...` works on a bare clone.

### Where the app looks for its files

Nothing has to be launched from a particular directory. Both binaries resolve
the strategy library, the database and `config.toml` through `src/apppath`,
which searches in this order and takes the first hit:

1. the working directory, then its parent, then its parent — the walk this
   project has always done, so a checkout behaves exactly as it did (the CLI
   runs from `src/`, the desktop app from `ui/`, and `../strategies` is right
   for both);
2. `~/.config/backtester` — the per-user directory, where a packaged app saves
   a script and the API key;
3. the executable's own directory and its parent, with symlinks resolved, so a
   binary installed under `/opt` or reached through `$PATH` finds what was
   installed beside it.

The **strategy library is embedded in the binary** (`strategies/embed.go`, the
way `ui` embeds `frontend/dist`), so `Strategy = "lua:strategies/rsi.lua"`
resolves even when no `strategies/` directory exists anywhere. The embedded
copy is always the last answer: a script on disk with the same name wins.
Saving a script from the Lua tab writes to the on-disk library when there is
one and to `~/.config/backtester/strategies/` when there is not — where the
engine then finds it. **The shipped scripts themselves are never written**:
the Lua tab opens them read-only, and `WriteFile` refuses one outright
(`strategies.IsLibrary`). Your own scripts are the ones named `my_…`.

The database is the one thing that can genuinely be missing. The UI asks Go
where it is (`DefaultDBPath`) instead of assuming `../stock_data.db`; when
nothing is found the header reads **Choose DB…** rather than naming a file that
is not there.

### How the database is opened

DuckDB is single-writer *and* single-opener, and a shipped database is often
not writable at all, so `data.InitDB` tries three things in order and
`data.Status()` reports which one happened:

| what it finds | what it does | `Status().Mode` |
|---|---|---|
| a file it can write (every checkout) | opens it in place — unchanged | `read-write`, no notice |
| a file it cannot write (`/usr/share`, `/opt`, an AppImage mount, `chmod 444`) | copies it once to `~/.local/share/backtester/` and opens the copy | `read-write`, with a notice naming the copy |
| that, but the copy is impossible too | opens the original with `access_mode=read_only` | `read-only`, with a notice saying why |

Nothing is ever copied out from under a database you own: the copy happens
only on the branch where the file refused a read-write open. An existing copy
is reused whenever the source it was made from is unchanged — tracked by a
`<name>.db.source` stamp beside it, so bars you fetch *into* the copy are not
wiped by the next launch.

Read-only is a real mode, not a failure: everything reads normally and only
writing stops. `UpsertBars` refuses up front with the reason rather than
letting DuckDB complain about the attach mode halfway through.

If another program already holds the database — a second window, a CLI run, a
Python loader — DuckDB refuses the attach in both modes. The app turns its
message into a sentence naming the program and PID to close, instead of
showing `IO Error: Could not set lock on file …: Conflicting lock is held in
…` at the user. In the desktop app the notice arrives through
`DatabaseNotice`, which the header calls whenever the database changes.

## Version, platform, and what a release contains

The build names itself: `Backtester 0.1.0-beta.1 · linux/amd64` beside the app's
name in the header, and from the CLI:

```bash
cd src && go run . -version
```

Quote it in a bug report — a report that cannot name a build is very hard to
act on. The constant lives in `src/version/version.go` and nowhere else;
`scripts/package.sh` reads it, and the release script stamps the commit in with
`-ldflags "-X my-backtester/src/version.Commit=$(git rev-parse --short HEAD)"`.
`CHANGELOG.md` says what each release changed.

**The beta is Linux only.** The desktop build needs `-tags webkit2_41`; the
`ui/build/darwin` and `ui/build/windows` directories are Wails scaffolding that
has never been built or tested, and a report from either is answered
differently from one that says `linux/amd64`.

**If the window feels sluggish, try GPU rendering.** Wails paints the Linux
webview on the CPU by default, because acceleration leaves a blank or crashing
window on some NVIDIA/Wayland stacks. `BACKTESTER_GPU=on ./backtester` (or
`auto` for WebKit's on-demand policy) turns it on for one launch; unset or
`off` keeps the safe default. If the window comes up blank with it on, also try
`WEBKIT_DISABLE_DMABUF_RENDERER=1`.

`scripts/package.sh` assembles a release directory. What ships and why:

| file | why it has to be there |
|---|---|
| `backtester` | the binary, with the frontend and the Lua library embedded |
| `LICENSE` | the software's terms — MIT |
| `DATA-LICENSE.md` | the data's terms, deliberately stricter than the software's, plus the defect inventory |
| `THIRD-PARTY-NOTICES.txt` | **required**: shipping the binary without it breaks the MIT and BSD terms of everything it links |
| `CHANGELOG.md`, `README.md` | what changed, and how to use it |
| `strategies/` | readable, editable copies — the binary carries its own, and an on-disk script of the same name wins |
| `stock_data.db` | only with `--with-db`, since it is ~2.9 GB and a package is usually assembled several times before it is right |
