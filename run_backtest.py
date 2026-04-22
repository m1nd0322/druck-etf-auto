import argparse
from pprint import pprint

from druck.backtest import run_backtest
from druck.config import load_config
from druck.notifier import send_telegram
from druck.runtime import RuntimeEvent, db_runtime_reporter, run_guarded


def _print_optional_report(title: str, df):
    if df is None or df.empty:
        return
    print(f"\n[{title}]")
    print(df.to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run druck backtest")
    parser.add_argument("--config", default="config.yaml", help="config path")
    args = parser.parse_args()

    cfg = load_config(args.config)
    print(f"[run_backtest] config={args.config}")

    db_reporter = db_runtime_reporter()

    def reporter(event: RuntimeEvent):
        msg = f"[Druck Backtest] {event.category}: {event.message}"
        if event.detail:
            msg += f" | {event.detail}"
        print(msg)
        db_reporter(event)
        send_telegram(cfg, msg)

    runtime = run_guarded(lambda: {"backtest": run_backtest(cfg)}, reporter=reporter)
    if runtime.ok:
        result = runtime.payload["backtest"]
        pprint(result.summary)
        if result.analytics and result.analytics.get("selection_score_comparison"):
            print("\n[Selection Score Comparison]")
            pprint(result.analytics["selection_score_comparison"])
        _print_optional_report("Scenario Summary", result.scenario_summary)
        _print_optional_report("Walkforward Summary", result.walkforward_summary)
        print("\n[Rebalance Log]")
        print(result.rebalance_log.to_string(index=False))
