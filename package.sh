#!/usr/bin/env bash
# Assemble a release directory.
#
# What ships, and why each piece has to: the binary; the licence for the
# software; the separate and stricter licence for the data; and the third-party
# notices, without which shipping the binary breaks the MIT and BSD terms of
# everything it links. The strategy library is embedded in the binary (T116),
# so it is included only as something to read and edit, not as something the
# app needs to find.
#
# The database is NOT copied by default: it is ~2.9 GB, and a release directory
# is usually assembled several times before it is right. Pass --with-db when
# building the real thing.
#
# Usage:
#   ./package.sh [--with-db] [--dry-run]
#
# The binary must already be built, because building it needs a webkit
# toolchain this script has no business guessing at:
#   wails build -tags webkit2_41   (from ui/)
set -euo pipefail

cd "$(dirname "$0")"

VERSION=$(grep -oP 'Version = "\K[^"]+' src/version/version.go)
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
OUT="dist/backtester-${VERSION}-linux-amd64"

WITH_DB=0
DRY=0
for arg in "$@"; do
  case "$arg" in
    --with-db) WITH_DB=1 ;;
    --dry-run) DRY=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

say() { printf '%s\n' "$*"; }
run() { if [ "$DRY" = 1 ]; then say "would: $*"; else "$@"; fi; }

BINARY=""
for candidate in ui/build/bin/ui ui/ui; do
  if [ -f "$candidate" ]; then BINARY="$candidate"; break; fi
done
if [ -z "$BINARY" ]; then
  say "no binary found at ui/build/bin/ui or ui/ui." >&2
  say "build it first:  cd ui && wails build -tags webkit2_41" >&2
  exit 1
fi

say "packaging ${VERSION} (${COMMIT}) from ${BINARY}"
run rm -rf "$OUT"
run mkdir -p "$OUT/strategies"

run cp "$BINARY" "$OUT/backtester"
run cp LICENSE DATA-LICENSE.md THIRD-PARTY-NOTICES.txt CHANGELOG.md "$OUT/"
run cp README.md "$OUT/"
# Editable copies of the shipped scripts. The binary carries its own, so these
# are for reading and for editing in the Lua tab -- an on-disk script of the
# same name wins over the embedded one.
run bash -c "cp strategies/*.lua strategies/README.md '$OUT/strategies/'"

if [ "$WITH_DB" = 1 ]; then
  if [ ! -f stock_data.db ]; then
    say "stock_data.db not found; --with-db cannot be honoured" >&2
    exit 1
  fi
  say "copying the database (this is ~2.9 GB and takes a while)"
  run cp stock_data.db "$OUT/"
else
  say "database NOT included (pass --with-db). The app will ask for one."
fi

say "done: $OUT"
say "the app looks for stock_data.db beside the binary; anywhere else, pick it"
say "with the header's Choose DB button."
