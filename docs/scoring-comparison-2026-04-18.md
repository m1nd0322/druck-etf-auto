# Scoring comparison, 2026-04-18

## Scope
Validated the newly added scoring wave against a baseline configuration.

New scoring stack:
- `capacity_awareness`
- `correlation_diversification`
- `residual_strength_anchors`
- `score_weights.residual_strength`

Baseline configuration for comparison:
- `capacity_awareness = 0.0`
- `residual_strength = 0.0`
- `correlation_diversification.enabled = false`
- `residual_strength_anchors.enabled = false`

To keep the comparison focused on the modified production scoring path, the comparative run used the US strategy universe only, with KR auto-generation disabled for the comparison job.

## Verification
- Full test suite: `72 passed`
- Comparative backtest artifact directory: `output/comparative/`
- Comparative run completed successfully, though one Yahoo Finance rate-limit warning appeared for `XLV` during the run.

## Summary delta, new minus baseline
- total_return: `+0.0090894891`
- cagr: `+0.0043090639`
- sharpe: `+0.0717354659`
- sortino: `+0.0758146127`
- calmar: `+0.0611956377`
- max_drawdown: `+0.0009803701` (slightly less severe drawdown)
- avg_turnover: `-0.0678235025`
- total_cost: `-0.0005389624`
- total_slippage_cost: `-0.0001616620`
- total_impact_cost: `-0.0002719145`
- total_liquidity_penalty: `-0.0000245549`
- avg_capacity_estimate: `+0.0086280128`
- active_return: `+0.0090894891`

## Selection-score comparison deltas
- avg_capacity_score: `+0.0049431423`
- avg_diversification_score: `+0.5171012681`
- avg_diversification_penalty: `+0.0129478478`
- avg_residual_strength: `-2.997510161769192e-13`
- avg_benchmark_relative_fail_count: `-0.0625`
- avg_preferred_factor_gate_fail_count: `-0.0625`
- avg_factor_selected_ratio: `-0.0078125`

## Interpretation
- The new scoring stack improved return and risk-adjusted metrics modestly in this comparison window.
- The stack also reduced turnover and estimated execution drag, which is encouraging for live tradability.
- Latest end-of-test alpha picks were unchanged between runs:
  - new: `XLE`, `EWZ`, `UUP`, `SHY`
  - baseline: `XLE`, `EWZ`, `UUP`, `SHY`
- This suggests the main observed benefit in this snapshot came more from path-level ranking and rebalance behavior than from the final terminal basket changing.
- Residual strength averaged near zero, which is consistent with a centered residual-style signal. It may still matter at rank-order level even when the unconditional mean is near zero.

## Notes
- Raw machine outputs were written under `output/comparative/` but are not tracked because `output/` is gitignored.
- If this comparison workflow becomes routine, a future improvement would be adding a dedicated tracked `artifacts/` or `docs/research/` export path plus a small helper script for reproducible baseline-vs-current scoring comparisons.
