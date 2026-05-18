#!/usr/bin/env bash
set -euo pipefail

if [ -z "${GITHUB_EVENT_PATH:-}" ]; then
  echo "::error::GITHUB_EVENT_PATH not set; this action must run in a GitHub Actions context."
  exit 2
fi

EVENT="$GITHUB_EVENT_PATH"

BASE_SHA="${INPUT_BASE_SHA:-}"
HEAD_SHA="${INPUT_HEAD_SHA:-}"
PR_NUMBER="$(jq -r '.pull_request.number // empty' "$EVENT")"
BASE_REPO="$(jq -r '.pull_request.base.repo.full_name // empty' "$EVENT")"
HEAD_REPO="$(jq -r '.pull_request.head.repo.full_name // empty' "$EVENT")"

if [ -z "$PR_NUMBER" ]; then
  echo "::error::This action must run on a pull_request event."
  exit 2
fi

if [ "$BASE_REPO" != "$HEAD_REPO" ]; then
  echo "::warning::Refusing to run on a fork PR (base=$BASE_REPO head=$HEAD_REPO). airflow-diff imports user code and is not safe to run on untrusted forks."
  exit 0
fi

[ -z "$BASE_SHA" ] && BASE_SHA="$(jq -r '.pull_request.base.sha' "$EVENT")"
[ -z "$HEAD_SHA" ] && HEAD_SHA="$(jq -r '.pull_request.head.sha' "$EVENT")"

COMMENT_PATH="$(mktemp -t airflow-diff-comment-XXXXXX.md)"
JSON_PATH="${COMMENT_PATH%.md}.json"
HTML_PATH="/tmp/airflow-diff-report.html"

set +e
airflow-diff diff "$BASE_SHA" "$HEAD_SHA" \
  --repo "$GITHUB_WORKSPACE" \
  --format markdown \
  --out "$COMMENT_PATH" \
  --json-out "$JSON_PATH"
DIFF_EXIT=$?
set -e

if [ ! -s "$COMMENT_PATH" ]; then
  echo "::error::airflow-diff produced no output (exit=$DIFF_EXIT)"
  exit 1
fi

# Always produce the HTML report so the action.yml upload step can include it.
airflow-diff report "$JSON_PATH" --format html --out "$HTML_PATH" || true

# Post (or update) the PR comment
gh pr comment "$PR_NUMBER" --edit-last --body-file "$COMMENT_PATH" \
  || gh pr comment "$PR_NUMBER" --body-file "$COMMENT_PATH"

exit "$DIFF_EXIT"
