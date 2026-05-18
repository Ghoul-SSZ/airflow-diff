#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
TMPDIR="$(mktemp -d)"
SMOKE_LOG="$TMPDIR/log"
PATH_SHIM="$TMPDIR/bin"
mkdir -p "$PATH_SHIM"
cp tests/smoke/fake_gh.sh "$PATH_SHIM/gh"
cp tests/smoke/fake_airflow_diff.sh "$PATH_SHIM/airflow-diff"
chmod +x "$PATH_SHIM/gh" "$PATH_SHIM/airflow-diff"
export PATH="$PATH_SHIM:$PATH"
export GITHUB_EVENT_PATH="$(pwd)/tests/smoke/fake_event.json"
export GITHUB_WORKSPACE="$(pwd)"
export SMOKE_LOG

echo "=== happy path ==="
bash action/entrypoint.sh
grep -q "pr comment 42" "$SMOKE_LOG" || { echo "FAIL: gh not invoked correctly"; cat "$SMOKE_LOG"; exit 1; }
echo "ok"

echo "=== fork PR rejected ==="
export GITHUB_EVENT_PATH="$(pwd)/tests/smoke/fake_event_fork.json"
: > "$SMOKE_LOG"
bash action/entrypoint.sh
if grep -q "pr comment" "$SMOKE_LOG"; then
  echo "FAIL: should not have invoked gh on fork PR"
  cat "$SMOKE_LOG"
  exit 1
fi
echo "ok"

echo "ALL SMOKE TESTS PASSED"
