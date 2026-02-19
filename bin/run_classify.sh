#!/bin/bash
# =============================================================================
# Run Phase 1: Identify
# =============================================================================
# Reads: data/staging/snapshot_latest.json  (output of 0_observe.py)
# Writes: data/marts/classification_latest.json
#
# No venv needed — this script uses only Python stdlib (json, argparse, etc).
# No cluster needed — it just processes the JSON file from Phase 0.
#
# Usage:
#   ./run_classify.sh             # classify latest snapshot
#   ./run_classify.sh --stdout    # print to terminal instead of file
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "=== Phase 1: Identify ==="
python scripts/1_classify.py "$@"
