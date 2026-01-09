#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a
. .env
set +a
venv_ai/bin/python email_top5.py
