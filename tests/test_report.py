from pathlib import Path

import pandas as pd

from druck.report import save_report


def test_save_report_includes_score_diagnostics_section(tmp_path):
    selection = pd.DataFrame(
        {
            "weight_after_cuts": [0.6, 0.4],
            "score": [1.2, 1.1],
            "relative_strength_6m": [0.05, 0.01],
            "capacity_score": [0.9, 0.8],
            "diversification_score": [0.4, 0.3],
            "diversification_penalty": [0.01, 0.02],
            "residual_strength": [0.02, -0.01],
        },
        index=["MTUM", "SPY"],
    )
    report_path = save_report(str(tmp_path), selection, {"risk_score": 0.7}, None)
    text = Path(report_path).read_text(encoding="utf-8")
    assert "## Score Diagnostics" in text
    assert "- capacity_score:" in text
    assert "- residual_strength:" in text
