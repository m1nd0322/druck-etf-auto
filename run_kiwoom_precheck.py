from __future__ import annotations

import json
from pathlib import Path

from druck.config import load_config
from druck.kiwoom_rest import get_mock_deposit, get_mock_positions

ETF_KEYWORDS = [
    'ETF', 'KODEX', 'TIGER', 'KOSEF', 'ACE', 'ARIRANG', 'KBSTAR', 'HANARO', 'TIMEFOLIO', 'PLUS', 'RISE', 'KINDEX', 'KIWOOM'
]


def is_etf_name(name: str) -> bool:
    s = (name or '').upper()
    return any(k.upper() in s for k in ETF_KEYWORDS)


def main() -> None:
    cfg = load_config('config.yaml')
    local_cfg = Path('config.local.yaml')
    if local_cfg.exists():
        # lightweight local override for mode/kiwoom section only
        import yaml
        local = yaml.safe_load(local_cfg.read_text(encoding='utf-8')) or {}
        cfg.update({k: v for k, v in local.items() if k in ['mode', 'kiwoom']})

    deposit = get_mock_deposit('config.local.yaml')
    positions = get_mock_positions('config.local.yaml')

    payload = deposit.get('response', {}).get('payload', {}) if deposit.get('ok') else {}
    etf_positions = [p for p in positions.get('positions', []) if is_etf_name(p.get('name', ''))]

    checks = {
        'kiwoom_enabled': bool(cfg.get('mode', {}).get('enable_kiwoom', False)),
        'dry_run_true': bool(cfg.get('mode', {}).get('dry_run', True)),
        'account_configured': bool(str(cfg.get('kiwoom', {}).get('account_no', '')).strip()),
        'market_order_configured': isinstance(cfg.get('kiwoom', {}).get('market_order', None), bool),
        'split_n': cfg.get('kiwoom', {}).get('split_n'),
        'slippage_limit_bps': cfg.get('kiwoom', {}).get('slippage_limit_bps'),
        'deposit_lookup_ok': bool(deposit.get('ok')),
        'positions_lookup_ok': bool(positions.get('ok')),
    }

    out = {
        'checks': checks,
        'deposit_summary': {
            'dnca_tot_amt': payload.get('dnca_tot_amt'),
            'ord_psbl_cash': payload.get('ord_psbl_cash'),
            'bncr_buy_alowa': payload.get('bncr_buy_alowa'),
            'return_code': payload.get('return_code'),
            'return_msg': payload.get('return_msg'),
        },
        'position_summary': {
            'position_count': len(positions.get('positions', [])),
            'etf_position_count': len(etf_positions),
            'sample_etf_positions': etf_positions[:10],
        },
        'test_ready': bool(
            checks['kiwoom_enabled']
            and checks['account_configured']
            and checks['deposit_lookup_ok']
            and checks['positions_lookup_ok']
        ),
        'notes': [
            'dry_run_true=True means the environment is ready for safe simulation/precheck, not live order submission.',
            'Set mode.dry_run to false only right before an actual paper order test.',
        ],
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
