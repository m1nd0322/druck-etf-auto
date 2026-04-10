from pprint import pprint

from druck.backtest import run_backtest
from druck.config import load_config


if __name__ == "__main__":
    cfg = load_config("config.yaml")
    result = run_backtest(cfg)
    pprint(result.summary)
    print(result.rebalance_log.to_string(index=False))
