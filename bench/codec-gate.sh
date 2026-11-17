#!/usr/bin/env bash
# Compare external codec workloads; see codec/CONTRACT.md.
set -euo pipefail
HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
exec python3 -I "$HERE/codec/gate.py" "$@"
