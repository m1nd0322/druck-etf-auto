from __future__ import annotations

import json
from pathlib import Path

import requests
import yaml

CONFIG = Path('/home/node/.openclaw/workspace/tmp/druck-etf-auto/config.local.yaml')
MOCK_BASE = 'https://mockapi.kiwoom.com'
TR_IDS = [
    'ka10000',
    'ka10001',
    'ka10002',
    'ka10003',
    'ka10004',
    'ka10005',
    'ka10006',
    'ka10007',
    'ka10008',
    'ka10009',
    'ka10010',
]


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


def call_tr(token: str, api_id: str, body: dict) -> dict:
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json;charset=UTF-8',
        'api-id': api_id,
        'cont-yn': 'N',
        'next-key': '',
    }
    resp = requests.post(f'{MOCK_BASE}/api/dostk/acnt', headers=headers, json=body, timeout=20)
    try:
        payload = resp.json()
    except Exception:
        payload = {'text': resp.text[:1000]}
    return {'status_code': resp.status_code, 'payload': payload}


def main():
    cfg = load_local()
    paper = cfg['kiwoom']['paper']
    token = issue_token(paper['app_key'], paper['app_secret'])
    acct = cfg['kiwoom']['account_no']
    tried = []
    for api_id in TR_IDS:
        body = {'acct_no': acct, 'account_no': acct, 'acctNo': acct}
        res = call_tr(token, api_id, body)
        payload = res['payload'] if isinstance(res['payload'], dict) else {}
        tried.append({
            'api_id': api_id,
            'status_code': res['status_code'],
            'return_code': payload.get('return_code'),
            'return_msg': payload.get('return_msg'),
            'keys': sorted(list(payload.keys()))[:15],
        })
    print(json.dumps(tried, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
