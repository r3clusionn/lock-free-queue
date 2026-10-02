#!/usr/bin/env bash
# Runs every check: unit and stress tests, the same tests with delay injection, and the loom models.
# Usage: scripts/verify.sh
set -euo pipefail
cd "$(dirname "$0")/.."

summary() { grep -E "^test result" | awk '{p+=$4; f+=$6} END {printf "%d passed, %d failed\n", p, f}'; }

printf '%-58s' "unit, stress, allocator tests"
cargo test --release 2>&1 | summary
printf '%-58s' "same, with delay injection (--features chaos)"
cargo test --release --features chaos 2>&1 | summary
printf '%-58s' "loom models (every interleaving, preemption bound 3)"
RUSTFLAGS="--cfg loom" cargo test --release --test loom --target-dir target/loom 2>&1 | summary
