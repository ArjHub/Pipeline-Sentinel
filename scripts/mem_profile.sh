#!/usr/bin/env bash
# Run a command while sampling `docker stats` every 2s; print idle, peak and total memory.
set -euo pipefail
cd "$(dirname "$0")/.."

samples=$(mktemp)
ids=$(docker compose ps -q)

sample() {
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' $ids | awk '{print $1, $2}' >>"$samples"
  echo "---" >>"$samples"
}

sample
idle_end=$(wc -l <"$samples")
( while true; do sample; sleep 2; done ) &
sampler=$!
trap 'kill $sampler 2>/dev/null || true' EXIT

status=0
"$@" || status=$?
kill $sampler 2>/dev/null || true
wait $sampler 2>/dev/null || true
sample

python3 - "$samples" "$idle_end" <<'EOF'
import re, sys
from collections import defaultdict

def mib(v):
    n, unit = re.match(r"([\d.]+)([KMG]i?B)", v).groups()
    return float(n) * {"KiB": 1/1024, "MiB": 1, "GiB": 1024, "kB": 1/1000, "MB": 1, "GB": 1000}[unit]

lines = open(sys.argv[1]).read().splitlines()
idle_end = int(sys.argv[2])
idle, peak = {}, defaultdict(float)
totals, current, seen = [], 0.0, set()
for i, line in enumerate(lines):
    if line == "---":
        totals.append(current); current, seen = 0.0, set(); continue
    name, usage = line.split()
    if name in seen:  # a sample cut short by `kill` has no "---"; don't sum two samples
        totals.append(current); current, seen = 0.0, set()
    seen.add(name)
    m = mib(usage)
    current += m
    if i < idle_end:
        idle[name] = m
    peak[name] = max(peak[name], m)
print(f"\n{'container':<45}{'idle MiB':>10}{'peak MiB':>10}")
for name in sorted(peak):
    print(f"{name:<45}{idle.get(name, 0):>10.0f}{peak[name]:>10.0f}")
print(f"{'TOTAL (sum at idle / max simultaneous)':<45}{totals[0]:>10.0f}{max(totals):>10.0f}")
EOF
exit $status
