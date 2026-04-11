from pprint import pprint

from druck.backtest import run_backtest
from druck.config import load_config
from druck.notifier import send_telegram
from druck.runtime import RuntimeEvent, run_guarded


if __name__ == "__main__":
    cfg = load_config("config.yaml")

    def reporter(event: RuntimeEvent):
        msg = f"[Druck Backtest] {event.category}: {event.message}"
        if event.detail:
            msg += f" | {event.detail}"
        print(msg)
        send_telegram(cfg, msg)

    runtime = run_guarded(lambda: {"backtest": run_backtest(cfg)}, reporter=reporter)
    if runtime.ok:
        result = runtime.payload["backtest"]
        pprint(result.summary)
        print(result.rebalance_log.to_string(index=False))
