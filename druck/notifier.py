from __future__ import annotations
import os
import requests


def _telegram_target(cfg: dict) -> tuple[str, str]:
    local = cfg.get('telegram_local', {}) or {}
    token = str(local.get('bot_token', '') or '').strip()
    chat_id = str(local.get('chat_id', '') or '').strip()
    if token and chat_id:
        return token, chat_id

    tcfg = cfg.get('notifier', {}).get('telegram', {}) or {}
    if not tcfg.get('enabled', False):
        return '', ''
    token = os.getenv(tcfg.get('bot_token_env', 'TELEGRAM_BOT_TOKEN'), '')
    chat_id = os.getenv(tcfg.get('chat_id_env', 'TELEGRAM_CHAT_ID'), '')
    return token, chat_id


def send_telegram(cfg: dict, message: str):
    token, chat_id = _telegram_target(cfg)
    if not token or not chat_id:
        print('[notifier] telegram skipped: missing token/chat_id')
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(url, json={'chat_id': chat_id, 'text': message}, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[notifier] telegram send failed: {exc}")
