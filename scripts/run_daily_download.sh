#!/usr/bin/env bash
# Wrapper to run the stock history downloader daily inside the project's virtualenv.
# Run this script from cron. It activates the venv, cd's to project root and runs the downloader.

set -euo pipefail
LOGDIR="$(pwd)/logs"
mkdir -p "$LOGDIR"
PROJECT_ROOT="/home/PyFin/Documents/StockAnalysis/StockAnalysis"
VENV_BIN="$PROJECT_ROOT/venv_ai/bin/python"
SCRIPT="$PROJECT_ROOT/StockDataDownload.py"

# Environment: load .env if present
if [ -f "$PROJECT_ROOT/.env" ]; then
  # shellcheck disable=SC1090
  export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs -d '\n') || true
fi

cd "$PROJECT_ROOT"
# Run daily update (StockDataDownload default is 'daily')
"$VENV_BIN" "$SCRIPT" daily >> "$LOGDIR/stock_download_$(date +%F).log" 2>&1
# After successful download, run scoring and option selection
"$VENV_BIN" "$PROJECT_ROOT/score_engine.py" --write --lookback 90 --threshold 0.0 >> "$LOGDIR/score_engine_$(date +%F).log" 2>&1 || true
"$VENV_BIN" "$PROJECT_ROOT/option_selector.py" --top 20 --threshold 90.0 >> "$LOGDIR/option_selector_$(date +%F).log" 2>&1 || true
# Send pipeline email summary (dry-run by default; set SMTP_* env vars and use --send to email)
"$VENV_BIN" "$PROJECT_ROOT/email_pipeline_report.py" --limit 20 >> "$LOGDIR/email_pipeline_$(date +%F).log" 2>&1 || true
