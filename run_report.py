from druck.config import load_config
from druck.engine import run_once

if __name__ == "__main__":
    cfg = load_config("config.yaml")
    run_once(cfg, do_trade=False)
