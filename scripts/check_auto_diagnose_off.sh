#!/usr/bin/env bash
# Refuse a paid benchmark run while automatic diagnosis could spend in the background:
# SENTINEL_AUTO_DIAGNOSE must be 0 in the environment, and in sentinel-detector if it runs.
set -euo pipefail
if [[ "${SENTINEL_AUTO_DIAGNOSE:-1}" != "0" ]]; then
  echo "Set SENTINEL_AUTO_DIAGNOSE=0 (in .env) for benchmark runs." >&2
  exit 2
fi
cid=$(docker compose ps -q sentinel-detector 2>/dev/null || true)
if [[ -n "$cid" ]]; then
  v=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$cid" | sed -n 's/^SENTINEL_AUTO_DIAGNOSE=//p')
  if [[ "$v" != "0" ]]; then
    echo "sentinel-detector is running with SENTINEL_AUTO_DIAGNOSE=${v:-1}." >&2
    echo "Run: docker compose up -d sentinel-detector   (after setting it to 0 in .env)" >&2
    exit 2
  fi
fi
echo "auto-diagnosis is off"
