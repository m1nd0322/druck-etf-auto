from __future__ import annotations

import json
import sys
from pathlib import Path

import requests
import yaml

sys.path.append(str(Path(__file__).resolve().parents[1]))
from druck.db import init_db, log_order_operation
from druck.kiwoom_rest import get_mock_deposit, get_mock_positions

CONFIG = Path('config.local.yaml')


def load_local() -> dict:
    return yaml.safe_load(CONFIG.read_text(encoding='utf-8')) or {}


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


def to_int(value: str | int | float | None) -> int:
    s = str(value or '0').replace(',', '').strip()
    neg = s.startswith('-')
    s = s.lstrip('-').lstrip('0') or '0'
    v = int(s)
    return -v if neg else v


def main() -> None:
    cfg = load_local()
    tg = cfg.get('telegram_local', {}) or {}
    bot_token = str(tg.get('bot_token', '')).strip()
    chat_id = str(tg.get('chat_id', '')).strip()
    if not bot_token or not chat_id:
        raise SystemExit('missing telegram_local.bot_token or telegram_local.chat_id')

    conn = init_db('trade_log.db')
    deposit = get_mock_deposit('config.local.yaml')
    log_order_operation(
        conn,
        action_type='scheduled_deposit_check',
        api_id='kt00001',
        return_msg='ok' if deposit.get('ok') else 'failed',
        response_summary=json.dumps(deposit.get('response', {}), ensure_ascii=False)[:1000],
        success=bool(deposit.get('ok')),
    )
    positions = get_mock_positions('config.local.yaml')
    log_order_operation(
        conn,
        action_type='scheduled_positions_check',
        api_id='kt00018',
        return_msg='ok' if positions.get('ok') else 'failed',
        response_summary=json.dumps({'count': len(positions.get('positions', []))}, ensure_ascii=False),
        success=bool(positions.get('ok')),
    )
    conn.close()

    payload = deposit.get('response', {}).get('payload', {}) if deposit.get('ok') else {}
    pos = positions.get('positions', []) if positions.get('ok') else []
    pos_sorted = sorted(pos, key=lambda x: x.get('profit_rate', 0), reverse=True)

    def won(v: int) -> str:
        return f'{v:,}원'

    lines = ['[Kiwoom Mock] 일일 계좌 조회', '']
    lines.append(f"주문가능금액: {won(to_int(payload.get('ord_psbl_cash') or payload.get('ord_alow_amt')))}")
    lines.append(f"예수금: {won(to_int(payload.get('dnca_tot_amt') or payload.get('entr')))}")
    lines.append(f"보유종목수: {len(pos)}")
    lines.append('')
    lines.append('상위 수익률 5종목')
    for row in pos_sorted[:5]:
        lines.append(f"- {row.get('name')}({row.get('ticker')}): {row.get('profit_rate', 0):.2f}% / {row.get('qty')}주")

    text = '\n'.join(lines)
    result = send_telegram(bot_token, chat_id, text)
    print(json.dumps({'deposit_ok': deposit.get('ok'), 'positions_ok': positions.get('ok'), 'telegram': result}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
