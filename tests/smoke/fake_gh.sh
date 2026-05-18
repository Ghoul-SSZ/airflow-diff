#!/usr/bin/env bash
echo "[fake gh] $@" >> "${SMOKE_LOG:-/tmp/smoke.log}"
exit 0
