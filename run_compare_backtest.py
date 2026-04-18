from __future__ import annotations

import argparse
from pprint import pprint

from druck.compare_backtest import run_scoring_comparison
from druck.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="Run baseline-vs-current scoring comparative backtest")
    parser.add_argument("--config", default="config.yaml", help="Path to main config file")
    parser.add_argument("--local-config", default="config.local.yaml", help="Path to local override config file")
    parser.add_argument("--output-dir", default="output/comparative", help="Directory for comparative outputs")
    parser.add_argument("--include-kr", action="store_true", help="Include KR universe instead of forcing US-only comparison")
    args = parser.parse_args()

    cfg = load_config(args.config, args.local_config)
    result = run_scoring_comparison(cfg, outdir=args.output_dir, us_only=not args.include_kr)
    payload = result["payload"]

    print("[Comparative Backtest Output]")
    print(result["output_path"])
    print("\n[Summary Delta]")
    pprint(payload.get("summary_delta", {}))
    print("\n[Selection Delta]")
    pprint(payload.get("selection_delta", {}))
    print("\n[Latest Picks]")
    print(f"current: {payload.get('new_last_alpha_picks', [])}")
    print(f"baseline: {payload.get('base_last_alpha_picks', [])}")


if __name__ == "__main__":
    main()
