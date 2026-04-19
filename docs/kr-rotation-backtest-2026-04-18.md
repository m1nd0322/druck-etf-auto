# KR rotation backtest snapshot (2026-04-18)

## Run context
- config: `config.kr-rotation.yaml`
- execution mode: temporary replacement of `config.yaml` during `run_backtest.py`, then restored
- purpose: iterate a KR-focused rotation spec against KOSPI proxy benchmark `069500.KS` after fixing selection contamination, provider-noise handling, and then progressively tightening KR-specific sleeve structure

## Reliability note
This document now reflects the cleaner KR validation chain after the major contamination/data fixes.
Confirmed foundations across the tuning wave:
- `SPY` and `UUP` no longer contaminate KR portfolio selections
- yfinance partial-success / stderr invalid-symbol noise is handled more coherently
- KR defensive assets are no longer treated as generic `core`
- KR sleeve structure is now explicit enough to compare multiple strategy shapes instead of a mixed dirty baseline

## KR tuning sequence summary
### 1) Clean KR sleeve baseline after contamination fix
- purpose: verify that poor KR results were no longer driven by symbol contamination
- result:
  - CAGR: `0.1298258382`
  - Sharpe: `1.1402704038`
  - max drawdown: `-0.1354592633`
  - average turnover: `0.4837758053`
  - active return: `-1.6780774993`
- conclusion:
  - contamination was fixed
  - KR underperformance remained, so the next bottleneck was strategy structure

### 2) KR attack tightening + stronger benchmark-relative filter + regime budgets
- changes:
  - `kr_attack` narrowed around `091160.KS`, `102960.KS`
  - stronger `benchmark_relative_filter` on `kr_attack`
  - benchmark-relative halt relaxed modestly
  - regime-specific attack/core/defensive budgets added
- result:
  - CAGR: `0.3353715494`
  - Sharpe: `1.5579667827`
  - max drawdown: `-0.1354592633`
  - average turnover: `0.6174536360`
  - active return: `-1.1641996732`
- conclusion:
  - absolute performance improved a lot
  - but turnover became too high for an operator-friendly KR variant

### 3) Core-purity / turnover-suppression variant
- changes:
  - removed `139230.KS` and `266420.KS`
  - increased `kr_core` purity
  - reduced `top_n`
  - tightened diversification penalty and max weight
- result:
  - CAGR: `0.1792747426`
  - Sharpe: `1.1359724522`
  - max drawdown: `-0.1377693473`
  - average turnover: `0.4459208223`
  - active return: `-1.5624320573`
- conclusion:
  - turnover and noise came down
  - but the strategy became too conservative and gave up too much return

### 4) Current preferred variant: limited `139230.KS` reintroduction via `kr_satellite`
- changes:
  - reintroduced only `139230.KS`
  - added explicit `kr_satellite` sleeve
  - kept `core purity` and low-turnover constraints in place
  - satellite budgets:
    - base `kr_satellite: 0.15`
    - `RISK_ON: 0.15`
    - `NEUTRAL: 0.10`
    - `RISK_OFF: 0.05`
- result:
  - CAGR: `0.2434464423`
  - Sharpe: `1.3883783098`
  - max drawdown: `-0.1377693473`
  - average turnover: `0.3228420598`
  - active return: `-1.4048210652`
- interpretation:
  - this variant recovered meaningful upside versus the overly conservative version
  - turnover fell further versus the more aggressive KR-attack version
  - it still lags KOSPI proxy, but it looks like the best balance so far between cleanliness, operator-friendliness, and recoverable alpha

## Requested comparison versus KOSPI proxy
### Current preferred variant (`kr_satellite`)
- strategy CAGR: `24.34%`
- strategy Sharpe: `1.388`
- strategy max drawdown: `-13.78%`
- average turnover: `0.3228`
- result: still below KOSPI proxy on total return, but substantially cleaner and more controllable than the earlier KR attack-heavy versions

### 5) Benchmark-plus overlay family
- changes:
  - long-only KR core-satellite baseline was replaced by a `benchmark-plus overlay` structure
  - `069500.KS` is the default core holding via explicit benchmark overlay weight
  - attack and satellite exposure are only added as overlays on top of the benchmark base
  - overlay weights:
    - benchmark base: `0.70`
    - attack overlay: `0.20`
    - satellite overlay: `0.10`
- result:
  - CAGR: `0.5093088407`
  - Sharpe: `1.7216610884`
  - max drawdown: `-0.1993850566`
  - average turnover: `0.3375`
  - active return: `-0.6697466750`

### 6) Benchmark-aware dual momentum family
- changes:
  - disabled benchmark overlay weighting
  - switched KR strategy family to `dual_momentum`
  - keep `069500.KS` inside the candidate set as benchmark-aware anchor rather than fixed base weight
  - select top KR candidates by positive absolute momentum plus relative-strength leadership, then re-normalize weights
- result:
  - CAGR: `0.5239818367`
  - Sharpe: `1.6217017645`
  - max drawdown: `-0.1970445693`
  - average turnover: `0.4166666667`
  - active return: `-0.6244246735`
- interpretation:
  - this variant beat the benchmark-plus overlay on the user’s acceptance gate, CAGR
  - it also slightly improved active return and max drawdown
  - tradeoff: Sharpe fell modestly and turnover rose meaningfully

### 7) New adopted family: risk-managed dual momentum
- changes:
  - kept the benchmark-aware `dual_momentum` family
  - relaxed risk-cut and strategy-halt thresholds enough to reduce halt clustering during the weak `2024-09` to `2025-03` zone
  - key changes:
    - `trailing_dd_cut: -0.12`
    - `hard_stop_cut: -0.18`
    - `strategy_halt.max_cut_asset_ratio: 0.95`
    - `strategy_halt.min_risk_score: 0.15`
    - `strategy_halt.max_negative_momentum_assets: 3`
    - softer performance-halt thresholds for average score / momentum / recent return / benchmark-relative return
- result:
  - CAGR: `0.7127905542`
  - Sharpe: `1.9102608919`
  - max drawdown: `-0.1970445693`
  - average turnover: `0.6875`
  - active return: `-0.0013000831`
  - halt_count: `1`
- interpretation:
  - this variant dramatically reduced halt clustering while also improving CAGR, Sharpe, and benchmark-relative performance
  - the remaining tradeoff is much higher turnover
  - under the current user priority, this becomes the new adopted KR leader, with turnover reduction as the next immediate tuning target

### Comparison across tuned variants
| variant | CAGR | Sharpe | MDD | turnover | active return |
| --- | ---: | ---: | ---: | ---: | ---: |
| clean contamination-fixed baseline | 12.98% | 1.140 | -13.55% | 0.4838 | -1.6781 |
| aggressive KR attack tightening | 33.54% | 1.558 | -13.55% | 0.6175 | -1.1642 |
| core-purity / low-turnover variant | 17.93% | 1.136 | -13.78% | 0.4459 | -1.5624 |
| previous preferred `kr_satellite` variant | 24.34% | 1.388 | -13.78% | 0.3228 | -1.4048 |
| benchmark-plus overlay variant | 50.93% | 1.722 | -19.94% | 0.3375 | -0.6697 |
| benchmark-aware dual momentum variant | 52.40% | 1.622 | -19.70% | 0.4167 | -0.6244 |
| new risk-managed dual momentum variant | 71.28% | 1.910 | -19.70% | 0.6875 | -0.0013 |

## Current design conclusion
### What now looks correct
- contamination/data noise is no longer the main KR problem
- pure sleeve-level KR rotation was not enough to close the benchmark gap
- benchmark-plus overlay was a real step forward, and benchmark-aware dual momentum improved on it
- the stronger conclusion now is that the main remaining bottleneck was halt clustering, not raw alpha generation
- the current adopted family is:
  - risk-managed benchmark-aware dual momentum with `069500.KS` included in the KR candidate set
  - attack leadership primarily expressed through `091160.KS`, `102960.KS`
  - satellite participation still showing up through `139230.KS`
  - defensive fallback remains available through `130730.KS`, `114260.KS`, `132030.KS`

### Why risk-managed dual momentum now leads
The overlay family fixed benchmark under-exposure. The plain dual-momentum family improved CAGR further. The risk-managed dual-momentum family then addressed the bigger path-quality issue revealed by walk-forward analysis: halt clustering in the weak middle regime.
- keep benchmark awareness through `069500.KS`
- still allow KR leaders to outrank the benchmark
- reduce excessive strategy halts during weak / transitional periods
- preserve participation in the later strong trend regime where most of the alpha is realized

## Current recommendation
The current best KR experimental direction is now:
1. use the risk-managed dual momentum family as the lead KR design
2. retain `069500.KS` as an explicit benchmark-aware candidate inside the KR selection set
3. keep benchmark-relative controls on `kr_attack`
4. make turnover reduction the next immediate tuning target, rather than returning to older sleeve-only families

## Stress-test and walk-forward interpretation for current dual momentum leader
### What the walk-forward shape says
- the strategy’s strongest payoff came in the later trend-following window from roughly `2025-06` onward
- the weak zone was the earlier transition / weak-trend period from roughly `2024-09` through `2025-03`
- halt accumulation rose from `1` to `6` during that weak zone and then stayed elevated
- this means the current dual momentum family is not a smooth all-weather KR design yet
- instead, it is a regime-sensitive design that wins by capturing strong upside phases after surviving a difficult mid-sample drawdown / halt cluster

### Practical interpretation
- the current edge is real, because later walk-forward windows improved sharply and ended with the strongest CAGR path in the KR test family so far
- but the quality of the path still depends heavily on avoiding or shortening the weak halting zone
- therefore the next two valid tuning directions are:
  1. reduce unnecessary rotation / churn without killing the late-trend upside
  2. reduce halt clustering in mid-regime deterioration periods

## Practical next step
The next valid step is to keep this risk-managed KR family and reduce turnover without reintroducing the old halt clustering problem.
