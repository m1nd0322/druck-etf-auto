from __future__ import annotations

import json
from pathlib import Path

import requests
import yaml

import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))
from druck.db import init_db, log_order_operation

CONFIG = Path('config.local.yaml')
MOCK_BASE = 'https://mockapi.kiwoom.com'
TICKERS = ['069500', '091160', '114260', '132030', '122630']


def load_local() -> dict:
    return yaml.safe_load(CONFIG.read_text(encoding='utf-8')) or {}


def issue_token(appkey: str, secretkey: str) -> str:
    resp = requests.post(
        f'{MOCK_BASE}/oauth2/token',
        json={'grant_type': 'client_credentials', 'appkey': appkey, 'secretkey': secretkey},
        timeout=20,
    )
    data = resp.json()
    return data.get('token', '')


def place_buy_order(token: str, ticker: str) -> dict:
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json;charset=UTF-8',
        'api-id': 'kt10000',
    }
    body = {
        'dmst_stex_tp': 'KRX',
        'stk_cd': ticker,
        'ord_qty': '1',
        'trde_tp': '3',
        'ord_uv': '0',
        'cond_uv': '',
    }
    resp = requests.post(f'{MOCK_BASE}/api/dostk/ordr', headers=headers, json=body, timeout=20)
    try:
        payload = resp.json()
    except Exception:
        payload = {'text': resp.text[:1000]}
    return {'ticker': ticker, 'status_code': resp.status_code, 'payload': payload, 'body': body}


def send_telegram(token: str, chat_id: str, text: str) -> dict:
    resp = requests.post(
        f'https://api.telegram.org/bot{token}/sendMessage',
        json={'chat_id': chat_id, 'text': text},
        timeout=20,
    )
    try:
        payload = resp.json()
    except Exception:
        payload = {'text': resp.text[:1000]}
    return {'status_code': resp.status_code, 'payload': payload}


def main() -> None:
    cfg = load_local()
    paper = cfg['kiwoom']['paper']
    tg = cfg.get('telegram_local', {}) or {}
    bot_token = str(tg.get('bot_token', '')).strip()
    chat_id = str(tg.get('chat_id', '')).strip()

    token = issue_token(paper['app_key'], paper['app_secret'])
    conn = init_db('trade_log.db')
    results = []
    for ticker in TICKERS:
        row = place_buy_order(token, ticker)
        payload = row.get('payload', {}) if isinstance(row.get('payload'), dict) else {}
        log_order_operation(
            conn,
            action_type='buy_submit',
            ticker=f'{ticker}.KS',
            side='buy',
            api_id='kt10000',
            status_code=row.get('status_code'),
            return_code=str(payload.get('return_code', '')),
            return_msg=str(payload.get('return_msg', '')),
            request_summary=json.dumps(row.get('body', {}), ensure_ascii=False),
            response_summary=json.dumps(payload, ensure_ascii=False)[:1000],
            order_ref=str(payload.get('ord_no', '') or payload.get('order_no', '')),
            success=(str(payload.get('return_code', '')) == '0'),
        )
        results.append(row)
    conn.close()

    lines = ['[Kiwoom Mock] KR 대표 5종 시장가 1주 매수 결과', '']
    for row in results:
        payload = row.get('payload', {}) if isinstance(row.get('payload'), dict) else {}
        lines.append(
            f"- {row['ticker']}: status={row['status_code']}, return_code={payload.get('return_code')}, return_msg={payload.get('return_msg')}"
        )
    text = '\n'.join(lines)

    telegram = None
    if bot_token and chat_id:
        telegram = send_telegram(bot_token, chat_id, text)

    print(json.dumps({'orders': results, 'telegram': telegram}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
