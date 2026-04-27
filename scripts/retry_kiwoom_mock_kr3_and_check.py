from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import requests
import yaml

sys.path.append(str(Path(__file__).resolve().parents[1]))
from druck.db import init_db, log_order_operation
from druck.kiwoom_rest import get_mock_deposit, get_mock_positions

CONFIG = Path('config.local.yaml')
MOCK_BASE = 'https://mockapi.kiwoom.com'
RETRY_TICKERS = ['114260', '132030', '122630']
ORDER_SPACING_SECONDS = 1.6


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
    return {'ticker': ticker, 'status_code': resp.status_code, 'payload': payload}


def call_order_tr(token: str, api_id: str, body: dict) -> dict:
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
    bot_token = str((cfg.get('telegram_local', {}) or {}).get('bot_token', '')).strip()
    chat_id = str((cfg.get('telegram_local', {}) or {}).get('chat_id', '')).strip()

    token = issue_token(paper['app_key'], paper['app_secret'])
    conn = init_db('trade_log.db')
    order_results = []
    for i, ticker in enumerate(RETRY_TICKERS):
        if i > 0:
            time.sleep(ORDER_SPACING_SECONDS)
        row = place_buy_order(token, ticker)
        payload = row.get('payload', {}) if isinstance(row.get('payload'), dict) else {}
        log_order_operation(
            conn,
            action_type='buy_submit_retry',
            ticker=f'{ticker}.KS',
            side='buy',
            api_id='kt10000',
            status_code=row.get('status_code'),
            return_code=str(payload.get('return_code', '')),
            return_msg=str(payload.get('return_msg', '')),
            request_summary=json.dumps({'ticker': ticker, 'qty': 1, 'market': 'KRX', 'trade_type': 'market'}, ensure_ascii=False),
            response_summary=json.dumps(payload, ensure_ascii=False)[:1000],
            order_ref=str(payload.get('ord_no', '') or payload.get('order_no', '')),
            success=(str(payload.get('return_code', '')) == '0'),
        )
        order_results.append(row)

    time.sleep(ORDER_SPACING_SECONDS)
    unfilled = call_order_tr(token, 'ka10075', {})
    log_order_operation(
        conn,
        action_type='unfilled_check',
        api_id='ka10075',
        status_code=unfilled.get('status_code'),
        return_code=str((unfilled.get('payload') or {}).get('return_code', '')),
        return_msg=str((unfilled.get('payload') or {}).get('return_msg', '')),
        request_summary='{}',
        response_summary=json.dumps(unfilled.get('payload', {}), ensure_ascii=False)[:1000],
        success=(str((unfilled.get('payload') or {}).get('return_code', '')) == '0'),
    )
    time.sleep(ORDER_SPACING_SECONDS)
    filled = call_order_tr(token, 'ka10076', {})
    log_order_operation(
        conn,
        action_type='fills_check',
        api_id='ka10076',
        status_code=filled.get('status_code'),
        return_code=str((filled.get('payload') or {}).get('return_code', '')),
        return_msg=str((filled.get('payload') or {}).get('return_msg', '')),
        request_summary='{}',
        response_summary=json.dumps(filled.get('payload', {}), ensure_ascii=False)[:1000],
        success=(str((filled.get('payload') or {}).get('return_code', '')) == '0'),
    )
    time.sleep(ORDER_SPACING_SECONDS)
    deposit = get_mock_deposit('config.local.yaml')
    log_order_operation(
        conn,
        action_type='deposit_check',
        api_id='kt00001',
        return_msg='ok' if deposit.get('ok') else 'failed',
        response_summary=json.dumps(deposit.get('response', {}), ensure_ascii=False)[:1000],
        success=bool(deposit.get('ok')),
    )
    time.sleep(ORDER_SPACING_SECONDS)
    positions = get_mock_positions('config.local.yaml')
    log_order_operation(
        conn,
        action_type='positions_check',
        api_id='kt00018',
        return_msg='ok' if positions.get('ok') else 'failed',
        response_summary=json.dumps({'count': len(positions.get('positions', []))}, ensure_ascii=False),
        success=bool(positions.get('ok')),
    )
    conn.close()

    lines = ['[Kiwoom Mock] KR 남은 3종 재주문 및 조회 결과', '']
    lines.append('재주문 결과')
    for row in order_results:
        payload = row.get('payload', {}) if isinstance(row.get('payload'), dict) else {}
        lines.append(f"- {row['ticker']}: status={row['status_code']}, return_code={payload.get('return_code')}, return_msg={payload.get('return_msg')}")
    lines.append('')
    lines.append(f"미체결조회(ka10075): status={unfilled.get('status_code')}, return_code={(unfilled.get('payload') or {}).get('return_code')}, return_msg={(unfilled.get('payload') or {}).get('return_msg')}")
    lines.append(f"체결조회(ka10076): status={filled.get('status_code')}, return_code={(filled.get('payload') or {}).get('return_code')}, return_msg={(filled.get('payload') or {}).get('return_msg')}")
    dep = deposit.get('response', {}).get('payload', {}) if deposit.get('ok') else {}
    lines.append(f"예수금조회: ok={deposit.get('ok')}, return_msg={dep.get('return_msg')}")
    lines.append(f"잔고조회: ok={positions.get('ok')}, 보유종목수={len(positions.get('positions', []))}")
    text = '\n'.join(lines)

    telegram = None
    if bot_token and chat_id:
        telegram = send_telegram(bot_token, chat_id, text)

    print(json.dumps({
        'orders': order_results,
        'unfilled': unfilled,
        'filled': filled,
        'deposit': deposit,
        'positions': {'ok': positions.get('ok'), 'count': len(positions.get('positions', [])), 'sample': positions.get('positions', [])[:10]},
        'telegram': telegram,
    }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
