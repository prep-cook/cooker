#!/usr/bin/env bash
# Usage: bench/container-start.sh IMAGE OUT
# OUT must be a new directory outside the checkout; no image pulls or builds occur.
set -euo pipefail
HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
if [[ $# != 2 ]]; then
  echo 'usage: bench/container-start.sh IMAGE OUT' >&2
  exit 2
fi
exec python3 -I "$HERE/container-start.py" "$1" "$2"
