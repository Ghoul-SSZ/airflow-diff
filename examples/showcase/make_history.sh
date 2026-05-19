#!/usr/bin/env bash
# Materialize a temporary inner git repo for the showcase, apply one of the
# case patches as a "head" commit, then print (or run) the airflow-diff
# command that diffs base..head.
#
# Usage:
#   ./make_history.sh case-1            # prepare repo, print the diff command
#   ./make_history.sh case-1 --run      # also execute airflow-diff
#   ./make_history.sh all               # prepare all three (no --run)
#   ./make_history.sh all --run         # prepare all three and run each
set -euo pipefail

SHOWCASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_ROOT="/tmp/airflow-diff-showcase"

prepare_case() {
    local case_name="$1"
    local run_after="$2"

    local patch="${SHOWCASE_DIR}/scenarios/${case_name}.patch"
    if [[ ! -f "${patch}" ]]; then
        echo "error: no patch for ${case_name} at ${patch}" >&2
        exit 1
    fi

    local work_dir="${WORK_ROOT}/${case_name}"
    rm -rf "${work_dir}"
    mkdir -p "${work_dir}"

    cp -r "${SHOWCASE_DIR}/dags" "${work_dir}/"
    cp "${SHOWCASE_DIR}/requirements.txt" "${work_dir}/"
    cp "${SHOWCASE_DIR}/constraints.txt" "${work_dir}/"
    cp "${SHOWCASE_DIR}/.airflow-diff.toml" "${work_dir}/"

    (
        cd "${work_dir}"
        git init -q -b main
        git -c user.email=demo@example.com -c user.name=demo \
            add . && git -c user.email=demo@example.com -c user.name=demo \
            commit -q -m "base"
        local base_sha
        base_sha=$(git rev-parse HEAD)

        git apply --check "${patch}"
        git apply "${patch}"
        git -c user.email=demo@example.com -c user.name=demo \
            add . && git -c user.email=demo@example.com -c user.name=demo \
            commit -q -m "head: ${case_name}"
        local head_sha
        head_sha=$(git rev-parse HEAD)

        echo
        echo "Prepared ${case_name} at ${work_dir}"
        echo "  base: ${base_sha}"
        echo "  head: ${head_sha}"
        echo
        local cmd="airflow-diff diff ${base_sha} ${head_sha} --repo ${work_dir} --format markdown"
        echo "Run:"
        echo "  ${cmd}"

        if [[ "${run_after}" == "--run" ]]; then
            if ! command -v airflow-diff >/dev/null 2>&1; then
                echo "error: airflow-diff not on PATH" >&2
                exit 1
            fi
            echo
            echo "--- airflow-diff output ---"
            eval "${cmd}" || true
        fi
    )
}

main() {
    local target="${1:-}"
    local flag="${2:-}"

    if [[ -z "${target}" ]]; then
        echo "usage: $0 {case-1|case-2|case-3|all} [--run]" >&2
        exit 2
    fi

    if [[ "${target}" == "all" ]]; then
        for c in case-1-regression case-2-sensor case-3-ripple; do
            prepare_case "${c}" "${flag}"
        done
    else
        # Allow short names
        case "${target}" in
            case-1|case-1-regression) target="case-1-regression" ;;
            case-2|case-2-sensor)     target="case-2-sensor" ;;
            case-3|case-3-ripple)     target="case-3-ripple" ;;
        esac
        prepare_case "${target}" "${flag}"
    fi
}

main "$@"
