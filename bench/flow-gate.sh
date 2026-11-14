#!/usr/bin/env bash
# Run from the repository root; OUT must be a new external output directory.
# Acceptance by default: exits nonzero unless every scenario, flow metric, frame
# position and FLOW_BUILD_PROVENANCE is verified. --diagnostic never passes.
# Each scenario reaches one provider entry of the generated config (flow/driver.py):
# signatures-tools uses the claude API-key entry (P1); choices-logprobs-tools,
# slow-downstream, stream-limit and the SSE resumes use openai-compatibility (P2);
# terminal-snapshots and realtime use codex (P3); the media rows use xai (P6). A converted
# provider needs no driver change: rebuild NEW and its rows exercise the new executor.
set -euo pipefail
HERE=$(cd -- "$(dirname -- "$0")" && pwd)
exec python3 -I "$HERE/flow/driver.py" "$@"
