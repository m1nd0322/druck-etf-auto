import yaml
from druck.engine import run_once

if __name__ == "__main__":
    cfg = yaml.safe_load(open("config.yaml", "r", encoding="utf-8"))
    run_once(cfg, do_trade=False)
