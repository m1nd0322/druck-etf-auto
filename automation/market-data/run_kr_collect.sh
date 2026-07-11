#!/bin/sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
PYTHON=${DRUCK_PYTHON:-"$PROJECT_ROOT/.venv/bin/python"}
LOG_DIR=${DRUCK_LOG_DIR:-"$SCRIPT_DIR"}

if [ ! -x "$PYTHON" ]; then
  echo "Python interpreter is not executable: $PYTHON" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"
cd "$PROJECT_ROOT"
"$PYTHON" run_collect_market_data.py --lookback-years 3 --full --kr-chunk-size 1 --us-chunk-size 50 --index-chunk-size 10 --prices-limit 0 > "$LOG_DIR/kr_collect.log" 2>&1
