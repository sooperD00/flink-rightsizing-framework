# setup/run_observe.sh
#!/bin/bash
# =============================================================================
# Run Phase 0: Observe
# =============================================================================
# Wrapper script for different environments. Edit ENDPOINT for your setup.
#
# Usage:
#   ./run_observe.sh              # One-shot collection
#   ./run_observe.sh --loop 300   # Continuous collection every 300 seconds
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# === CONFIGURATION ===
ENDPOINT="${FLINK_ENDPOINT:-http://localhost:8081}"
OUTPUT_DIR="${PROJECT_ROOT}/data/staging"

# === RUN ===
cd "$PROJECT_ROOT"

if [[ "$1" == "--loop" ]]; then
  INTERVAL="${2:-300}"
  echo "=== Continuous observation mode: every ${INTERVAL}s ==="
  echo "Endpoint: $ENDPOINT"
  echo "Output:   $OUTPUT_DIR"
  echo "Press Ctrl+C to stop"
  echo ""
  
  while true; do
    python scripts/0_observe.py --endpoint "$ENDPOINT" --output "$OUTPUT_DIR"
    sleep "$INTERVAL"
  done
else
  echo "=== Single observation ==="
  echo "Endpoint: $ENDPOINT"
  echo "Output:   $OUTPUT_DIR"
  echo ""
  
  python scripts/0_observe.py --endpoint "$ENDPOINT" --output "$OUTPUT_DIR"
fi