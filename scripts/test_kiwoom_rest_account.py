from __future__ import annotations

import json
from pathlib import Path

import requests
import yaml

CONFIG = Path(__file__).resolve().parents[1] / 'config.local.yaml'
MOCK_BASE = 'https://mockapi.kiwoom.com'


def load_local() -> dict:
    return yaml.safe_load(CONFIG.read_text(encoding='utf-8'))


def issue_token(appkey: str, secretkey: str) -> dict:
    url = f'{MOCK_BASE}/oauth2/token'
    payload = {
        'grant_type': 'client_credentials',
        'appkey': appkey,
        'secretkey': secretkey,
    }
    resp = requests.post(url, json=payload, timeout=20)
    return {'status_code': resp.status_code, 'json': resp.json()}


def lookup_accounts(token: str) -> dict:
    url = f'{MOCK_BASE}/api/dostk/acnt'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json;charset=UTF-8',
        'api-id': 'ka00001',
        'cont-yn': 'N',
        'next-key': '',
    }
    resp = requests.post(url, headers=headers, json={}, timeout=20)
    out = {'status_code': resp.status_code}
    try:
        out['json'] = resp.json()
    except Exception:
        out['text'] = resp.text[:1000]
    return out


def main():
    cfg = load_local()
    paper = cfg['kiwoom']['paper']
    token_res = issue_token(paper['app_key'], paper['app_secret'])
    print(json.dumps({'token': {'status_code': token_res['status_code'], 'return_code': token_res['json'].get('return_code'), 'return_msg': token_res['json'].get('return_msg'), 'token_type': token_res['json'].get('token_type'), 'expires_dt': token_res['json'].get('expires_dt')}}, ensure_ascii=False, indent=2))
    token = token_res['json'].get('token')
    if not token:
        return
    acct_res = lookup_accounts(token)
    payload = acct_res.get('json', {}) if isinstance(acct_res.get('json'), dict) else {}
    print(json.dumps({'account_lookup': {'status_code': acct_res['status_code'], 'keys': sorted(list(payload.keys()))[:20], 'return_code': payload.get('return_code'), 'return_msg': payload.get('return_msg'), 'raw': payload}}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
