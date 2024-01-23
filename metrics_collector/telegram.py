import requests
import json
from urllib.parse import urlencode

# Чтение данных для авторизации
PATH_TO_CREDENTIAL = r"/home/user/auth-telegram.json"

with open(PATH_TO_CREDENTIAL) as f:
    data = json.load(f)

TOKEN = data["token"]

ANALYTICS_CHAT_ID = -1002115902949
ERRORS_CHAT_ID = -1002103199357


def send_telegram_message(chat_id: int, message: str):
    params = {"chat_id": chat_id, "text": message, "parse_mode": "MarkdownV2"}
    base_url = f"https://api.telegram.org/bot{TOKEN}/"
    url = base_url + "sendMessage?" + urlencode(params)
    resp = requests.get(url)
    return resp
