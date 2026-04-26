from __future__ import annotations

from pathlib import Path
from typing import Any
import time

import requests
import yaml

MOCK_BASE = 'https://mockapi.kiwoom.com'


def load_local_kiwoom_config(path: str = 'config.local.yaml') -> dict[str, Any]:
    cfg = yaml.safe_load(Path(path).read_text(encoding='utf-8'))
    return cfg.get('kiwoom', {})


def issue_mock_token(app_key: str, app_secret: str) -> dict[str, Any]:
    resp = requests.post(
        f'{MOCK_BASE}/oauth2/token',
        json={'grant_type': 'client_credentials', 'appkey': app_key, 'secretkey': app_secret},
        timeout=20,
    )
    data = resp.json()
    return {'status_code': resp.status_code, 'data': data}


def call_account_tr(token: str, api_id: str, body: dict[str, Any], cont_yn: str = 'N', next_key: str = '') -> dict[str, Any]:
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json;charset=UTF-8',
        'api-id': api_id,
        'cont-yn': cont_yn,
        'next-key': next_key,
    }
    resp = requests.post(f'{MOCK_BASE}/api/dostk/acnt', headers=headers, json=body, timeout=20)
    try:
        payload = resp.json()
    except Exception:
        payload = {'text': resp.text[:1000]}
    return {
        'status_code': resp.status_code,
        'headers': {k.lower(): v for k, v in resp.headers.items()},
        'payload': payload,
    }


def get_mock_deposit(local_config_path: str = 'config.local.yaml') -> dict[str, Any]:
    kcfg = load_local_kiwoom_config(local_config_path)
    paper = kcfg.get('paper', {})
    tok = issue_mock_token(paper['app_key'], paper['app_secret'])
    token = tok['data'].get('token', '')
    if not token:
        return {'ok': False, 'stage': 'token', 'token': tok}
    time.sleep(1.2)
    res = call_account_tr(token, 'kt00001', {'qry_tp': '0', 'stex_tp': '0'})
    return {'ok': res.get('payload', {}).get('return_code') == 0, 'stage': 'deposit', 'token': tok, 'response': res}


def get_mock_positions(local_config_path: str = 'config.local.yaml') -> dict[str, Any]:
    kcfg = load_local_kiwoom_config(local_config_path)
    paper = kcfg.get('paper', {})
    tok = issue_mock_token(paper['app_key'], paper['app_secret'])
    token = tok['data'].get('token', '')
    if not token:
        return {'ok': False, 'stage': 'token', 'token': tok}
    time.sleep(1.2)
    res = call_account_tr(token, 'kt00018', {'qry_tp': '0', 'dmst_stex_tp': 'KRX'})
    payload = res.get('payload', {}) if isinstance(res.get('payload'), dict) else {}
    items = payload.get('acnt_evlt_remn_indv_tot') or payload.get('acnt_evlt_remn') or []
    positions = []
    for row in items:
        if not isinstance(row, dict):
            continue
        positions.append({
            'ticker': str(row.get('stk_cd', '')).replace('A', ''),
            'name': row.get('stk_nm', ''),
            'qty': int(str(row.get('rmnd_qty', '0')).lstrip('0') or '0'),
            'tradable_qty': int(str(row.get('trde_able_qty', '0')).lstrip('0') or '0'),
            'current_price': int(str(row.get('cur_prc', '0')).lstrip('0') or '0'),
            'profit_rate': float(str(row.get('prft_rt', '0')).replace(',', '')) if str(row.get('prft_rt', '0')).strip() else 0.0,
        })
    return {
        'ok': payload.get('return_code') == 0,
        'stage': 'positions',
        'token': tok,
        'response': res,
        'positions': positions,
    }
