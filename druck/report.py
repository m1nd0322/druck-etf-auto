from __future__ import annotations
import os
from datetime import datetime
import pandas as pd

def save_report(out_dir: str, selection: pd.DataFrame, regime_details: dict, cuts: pd.DataFrame | None=None) -> str:
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = os.path.join(out_dir, f"selection_{ts}.csv")
    md_path = os.path.join(out_dir, f"report_{ts}.md")
    selection.to_csv(csv_path, encoding='utf-8-sig')

    lines=[]
    lines.append(f"# Druckenmiller ETF Report — {ts}")
    lines.append("")
    lines.append("## Macro Regime")
    for k,v in regime_details.items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    if cuts is not None and len(cuts)>0:
        lines.append("## Risk Cuts")
        lines.append(cuts.to_markdown(index=False))
        lines.append("")
    score_cols = [c for c in ["score", "relative_strength_6m", "capacity_score", "diversification_score", "diversification_penalty", "residual_strength"] if c in selection.columns]
    if score_cols:
        lines.append("## Score Diagnostics")
        diagnostics = selection[score_cols].mean(numeric_only=True).to_dict()
        for k, v in diagnostics.items():
            lines.append(f"- {k}: {v:.6f}")
        lines.append("")
    lines.append("## Selected ETFs")
    lines.append(selection.to_markdown())
    lines.append("")
    with open(md_path,'w',encoding='utf-8') as f:
        f.write("\n".join(lines))
    return md_path
