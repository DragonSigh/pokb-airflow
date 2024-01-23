import requests
import json
import re
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


def escape_markdown(
    text: str, version=2, entity_type=None
) -> str:
    """Helper function to escape telegram markup symbols.

    .. versionchanged:: 20.3
        Custom emoji entity escaping is now supported.

    Args:
        text (:obj:`str`): The text.
        version (:obj:`int` | :obj:`str`): Use to specify the version of telegrams Markdown.
            Either ``1`` or ``2``. Defaults to ``1``.
        entity_type (:obj:`str`, optional): For the entity types
            :tg-const:`telegram.MessageEntity.PRE`, :tg-const:`telegram.MessageEntity.CODE` and
            the link part of :tg-const:`telegram.MessageEntity.TEXT_LINK` and
            :tg-const:`telegram.MessageEntity.CUSTOM_EMOJI`, only certain characters need to be
            escaped in :tg-const:`telegram.constants.ParseMode.MARKDOWN_V2`. See the `official API
            documentation <https://core.telegram.org/bots/api#formatting-options>`_ for details.
            Only valid in combination with ``version=2``, will be ignored else.
    """
    if int(version) == 1:
        escape_chars = r"_*`["
    elif int(version) == 2:
        if entity_type in ["pre", "code"]:
            escape_chars = r"\`"
        elif entity_type in ["text_link", "custom_emoji"]:
            escape_chars = r"\)"
        else:
            escape_chars = r"\_*[]()~`>#+-=|{}.!"
    else:
        raise ValueError("Markdown version must be either 1 or 2!")

    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", text)
