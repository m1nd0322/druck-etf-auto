from pathlib import Path
import runpy

import pytest


@pytest.mark.parametrize(
    "script_name",
    [
        "test_kiwoom_rest_account.py",
        "test_kiwoom_rest_balance_candidates.py",
        "test_kiwoom_rest_balance_probe.py",
        "test_kiwoom_rest_order_probe.py",
    ],
)
def test_kiwoom_probe_uses_checkout_local_config(script_name):
    root = Path(__file__).resolve().parents[1]
    namespace = runpy.run_path(str(root / "scripts" / script_name))

    assert namespace["CONFIG"] == root / "config.local.yaml"
