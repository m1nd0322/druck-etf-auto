#!/bin/sh
set -eu
cd /home/node/.openclaw/workspace/tmp/druck-etf-auto
. .venv/bin/activate
python run_collect_market_data.py --lookback-years 3 --full --kr-chunk-size 50 --us-chunk-size 100 --index-chunk-size 20 --prices-limit 0 > automation/market-data/kr_collect.log 2>&1
