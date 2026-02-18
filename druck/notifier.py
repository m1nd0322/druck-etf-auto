from __future__ import annotations
import os
import requests

def send_telegram(cfg: dict, message: str):
    tcfg = cfg.get('notifier', {}).get('telegram', {})
    if not tcfg.get('enabled', False):
        return
    token = os.getenv(tcfg.get('bot_token_env','TELEGRAM_BOT_TOKEN'), '')
    chat_id = os.getenv(tcfg.get('chat_id_env','TELEGRAM_CHAT_ID'), '')
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, json={'chat_id': chat_id, 'text': message}, timeout=10)
