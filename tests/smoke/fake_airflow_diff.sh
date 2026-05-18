#!/usr/bin/env bash
# Mimic `airflow-diff diff ... --out PATH --json-out PATH`
# and    `airflow-diff report PATH --format html --out PATH`

SUBCOMMAND="${1:-}"
shift

case "$SUBCOMMAND" in
  diff)
    OUT=""
    JSON_OUT=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --out) shift; OUT="$1" ;;
        --json-out) shift; JSON_OUT="$1" ;;
      esac
      shift
    done
    echo "## airflow-diff (stub)" > "$OUT"
    [ -n "$JSON_OUT" ] && echo '{"dags":[]}' > "$JSON_OUT"
    exit 0
    ;;
  report)
    OUT=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --out) shift; OUT="$1" ;;
      esac
      shift
    done
    [ -n "$OUT" ] && echo "<html><body>stub</body></html>" > "$OUT"
    exit 0
    ;;
  *)
    echo "[fake airflow-diff] unknown subcommand: $SUBCOMMAND" >&2
    exit 1
    ;;
esac
