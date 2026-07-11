from __future__ import annotations

import json
from pathlib import Path

import requests
import yaml

CONFIG = Path(__file__).resolve().parents[1] / 'config.local.yaml'
MOCK_BASE = 'https://mockapi.kiwoom.com'


def load_local() -> dict:
    return yaml.safe_load(CONFIG.read_text(encoding='utf-8'))


def issue_token(appkey: str, secretkey: str) -> str:
    resp = requests.post(
        f'{MOCK_BASE}/oauth2/token',
        json={'grant_type': 'client_credentials', 'appkey': appkey, 'secretkey': secretkey},
        timeout=20,
    )
    data = resp.json()
    return data.get('token', '')


def place_buy_order(token: str) -> dict:
    url = f'{MOCK_BASE}/api/dostk/ordr'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json;charset=UTF-8',
        'api-id': 'kt10000',
    }
    body = {
        'dmst_stex_tp': 'KRX',
        'stk_cd': '069500',
        'ord_qty': '1',
        'trde_tp': '3',
        'ord_uv': '0',
        'cond_uv': '',
    }
    resp = requests.post(url, headers=headers, json=body, timeout=20)
    try:
        payload = resp.json()
    except Exception:
        payload = {'text': resp.text[:2000]}
    return {'status_code': resp.status_code, 'payload': payload, 'body': body}


def main():
    cfg = load_local()
    paper = cfg['kiwoom']['paper']
    token = issue_token(paper['app_key'], paper['app_secret'])
    result = place_buy_order(token)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
