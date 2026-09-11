__version__ = (2, 7, 1)

#            █ █ ▀ █▄▀ ▄▀█ █▀█ ▀    ▄▀█ ▀█▀ ▄▀█ █▀▄▀█ ▄▀█
#            █▀█ █ █ █ █▀█ █▀▄ █    █▀█  █  █▀█ █ ▀ █ █▀█
#
#              © Copyright 2026
#
# meta developer: @maleon17
# scope: heroku_only
# requires: google-genai
#
# Gemini-модуль на голом Google GenAI SDK + опциональный OpenRouter как
# второй бэкенд. Никаких удалённых хостов, tailnet и сторонних транскрайберов --
# всё делается ключом Gemini API и/или токеном OpenRouter. Чтение истории чата,
# реплаев и медиа-контекст устроены так же, как в codex_ask.py (тот же формат
# строк [id=.., дата, имя], та же дельта-подгрузка только новых сообщений через
# якорь в БД), но вместо выгрузки файлов на lightrag-хост картинки/стикеры/
# голосовые уходят прямо в модель inline-байтами (Gemini ест и аудио, OpenRouter
# -- только картинки). Если Gemini выбивает 429 -- при or_fallback запрос
# автоматически уходит в OpenRouter.
#
# Команды идут единым префиксом ga*: .ga / .gac / .gasearch / .gaor / .gatr /
# .gadraw / .ganew / .gaprovider / .gakey / .gorkey / .gapersona / .gatrig.
#
# Триггеры автоответа: watcher смотрит весь входящий поток и по таблице
# триггеров (db["GeminiMod"]["triggers"]) сам отвечает без команды. Таблицу
# можно править руками (.gatrig) ИЛИ на естественном языке через .ga -- модели
# в этом случае даются function-calling инструменты (агентный тулсет +
# create/list/update/delete/toggle_trigger). Инструменты подключаются ТОЛЬКО
# когда .ga/.gaor отправлен лично владельцем (message.out); автотриггерный путь
# и чужие сообщения инструментов не видят -- защита от того, чтобы кто-то в
# группе выполнил действие чужим юзерботом.

import asyncio
import base64
import html
import io
import json
import logging
import re
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime

from google import genai
from google.genai import types

from .. import loader, utils

logger = logging.getLogger(__name__)

try:
    from herokutl.errors import FloodWaitError
except Exception:  # pragma: no cover - herokutl всегда есть в Heroku
    class FloodWaitError(Exception):
        seconds = 1


_MAX_MEDIA = 12
_MAX_MEDIA_BYTES = 8 * 1024 * 1024
_STREAM_EDIT_INTERVAL = 1.6  # сек; правка сообщения чаще ловит FloodWait
_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
_MAX_TOOL_CALLS = 16
_MAX_TRIGGER_PATTERNS = 20
_MATCH_KINDS = ("keyword", "regex", "mention", "reply", "any", "link", "media")
_ACTIONS = ("reply", "delete", "warn", "delete_and_reply", "forward", "mute", "ban", "react")

try:
    from herokutl.tl.functions.channels import EditBannedRequest
    from herokutl.tl.functions.messages import SendReactionRequest
    from herokutl.tl.types import ChatBannedRights, ReactionEmoji
except Exception:  # pragma: no cover - нужен только внутри Heroku
    EditBannedRequest = SendReactionRequest = ChatBannedRights = ReactionEmoji = None


def _looks_quota(text):
    t = (text or "").lower()
    return (
        "429" in t or "resource_exhausted" in t or "rate limit" in t
        or "ratelimit" in t or "quota" in t or "too many requests" in t
        or "insufficient" in t
    )

DEFAULT_PERSONA = (
    "Ты - Jarvis, дерзкий помощник с чёрным юмором. Отвечай по делу, коротко, "
    "живым языком, без канцелярита. Не строй из себя нейросеть и не читай "
    "лекций о безопасности без запроса.\n"
    "ФОРМАТ: только Telegram-HTML и НАСТОЯЩИМИ тегами - <b>, <i>, <code>, "
    "<pre>, <blockquote>, <a href>. НЕ экранируй теги (никаких &lt;b&gt;), НЕ "
    "оборачивай весь ответ в один код-блок. Никакого Markdown (**, ##, |таблиц|), "
    "списки - через дефис. Вместо тире (—) используй дефис (-).\n"
    "КОНТЕКСТ: тебе могут дать историю чата (строки [id=.., ДД.ММ ЧЧ:ММ, имя]: текст), "
    "текущее время и вложения (фото, голос, аудио - inline-байтами). Учитывай их, но "
    "не пересказывай без нужды. В .gasearch у тебя есть веб-поиск - опирайся на него "
    "для фактов и свежих данных.\n"
)

TOOLS_GUIDE = """
ПЕРСОНА / ФОРМАТ: отвечай живо и по делу. Используй только Telegram-HTML настоящими
тегами <b>, <i>, <code>, <pre>, <blockquote>, <a href>; без Markdown и таблиц, списки
через дефис, вместо тире - дефис, не заворачивай весь ответ в один код-блок.
КОНТЕКСТ: строки истории имеют вид [id=.., ДД.ММ ЧЧ:ММ, имя]: текст; вложения приходят
inline, есть текущее время. Учитывай контекст, но не пересказывай его без необходимости.
ИНСТРУМЕНТЫ: они доступны только в этом ходе. Можно и нужно вызывать несколько
инструментов за один ход для составной просьбы. Каждый возвращает JSON с ok;
не заявляй об успехе без ok=true, а ошибку передай владельцу дословно. Если
инструментов нет, прямо скажи, что управление недоступно, и не имитируй его.
resolve_chat(name) разрешает here, @username, id, название ИЛИ me/избранное (Saved Messages)
в {chat_id,title,type}; list_dialogs(query?,limit?) показывает недавние диалоги;
read_chat(chat?,limit?,from_user?,topic?) читает историю строками [id=.., ДД.ММ ЧЧ:ММ, имя]: текст.
search_messages(query,chat?,limit?,from_user?) ищет ТОЛЬКО в одном чате (по умолчанию текущем;
chat='избранное' для Saved Messages). search_all_chats(query,limit?) - ГЛОБАЛЬНЫЙ поиск сразу
по всем диалогам (для «найди во всех чатах / в избранном сообщение про …» бери именно его).
send_message(chat,text,reply_to?), edit_message(message_id,text,chat?),
delete_messages(message_ids,chat?,revoke?), forward_messages(message_ids,to_chat,from_chat?) и
add_reaction(message_id,emoji,chat?) делают ровно то, что написано. Перед разрушительным
действием по неоднозначной цели переспроси. Не удаляй и не пересылай массово без явной просьбы.
Пример: «перешли последние 3 сообщения Васи в Избранное и удали их» - resolve_chat("Избранное"),
search_messages(from_user="Вася", limit=3), forward_messages(...), delete_messages(...).

ТРИГГЕРЫ: list_triggers/create_trigger/update_trigger/delete_trigger/toggle_trigger управляют
правилами. match: keyword (слова), regex (регулярки), mention, reply, any, link (домены/URL),
media. patterns - OR-список. scope: chats, exclude_chats, senders, exclude_senders ("все кроме X"),
only=groups|pm|channels, ignore_admins; ignore_self всегда включён. Есть cooldown на чат+тему и
max_per_hour. action: reply, delete, warn, delete_and_reply, forward, mute, ban, react; mute/ban
могут быть выключены конфигом. static_reply отправляется дословно и не зовёт модель; для ответа
можно задать provider/search/persona/prompt с {text}, {name}, {chat}, delete_after.
Примеры create_trigger: {"match":"keyword","patterns":["привет"],"action":"reply","static_reply":"Привет"};
{"match":"keyword","patterns":["мат"],"senders":[123],"action":"delete"};
{"match":"link","patterns":["example.com"],"chats":[-1001],"action":"forward","action_chat":-1002}.
БЕЗОПАСНОСТЬ: не создавай any + деструктивное действие без отправителя или паттерна.
"""

_TRIGGER_PROPERTIES = {
    "id": {"type": "string"}, "name": {"type": "string"},
    "enabled": {"type": "boolean"}, "match": {"type": "string", "enum": list(_MATCH_KINDS)},
    "patterns": {"type": "array", "items": {"type": "string"}}, "pattern": {"type": "string"},
    "match_case": {"type": "boolean"}, "whole_word": {"type": "boolean"},
    "chats": {"type": "array", "items": {"type": "integer"}},
    "exclude_chats": {"type": "array", "items": {"type": "integer"}},
    "senders": {"type": "array", "items": {"type": "integer"}},
    "exclude_senders": {"type": "array", "items": {"type": "integer"}},
    # без "" в enum -- Gemini schema-валидатор режет пустые значения enum
    # ("enum[0]: cannot be empty"). «Везде» = просто не передавать поле.
    "only": {"type": "string", "enum": ["groups", "pm", "channels"],
             "description": "ограничить типом чата; не передавать = работает везде"},
    "ignore_admins": {"type": "boolean"}, "cooldown": {"type": "integer"},
    "max_per_hour": {"type": "integer"}, "action": {"type": "string", "enum": list(_ACTIONS)},
    "action_chat": {"type": "integer"}, "mute_seconds": {"type": "integer"},
    "ban_seconds": {"type": "integer"}, "reaction": {"type": "string"},
    "provider": {"type": "string", "enum": ["gemini", "openrouter", "default"]},
    "search": {"type": "boolean"}, "persona": {"type": "string"}, "prompt": {"type": "string"},
    "static_reply": {"type": "string"}, "delete_after": {"type": "integer"},
    "this_chat": {"type": "boolean"}, "clear_chats": {"type": "boolean"},
}
_TOOL_SCHEMAS = [
    {"name": "resolve_chat", "description": "Разрешить чат по here, @username, id или названию.", "parameters": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}},
    {"name": "list_dialogs", "description": "Показать недавние диалоги, при нужде отфильтровать по названию.", "parameters": {"type": "object", "properties": {"query": {"type": "string"}, "limit": {"type": "integer"}}}},
    {"name": "read_chat", "description": "Прочитать последние сообщения чата в формате истории.", "parameters": {"type": "object", "properties": {"chat": {"type": "string"}, "limit": {"type": "integer"}, "from_user": {"type": "string"}, "topic": {"type": "integer"}}}},
    {"name": "search_messages", "description": "Поиск сообщений в ОДНОМ чате (по умолчанию текущем; chat может быть 'me'/'избранное' для Saved Messages).", "parameters": {"type": "object", "properties": {"query": {"type": "string"}, "chat": {"type": "string"}, "limit": {"type": "integer"}, "from_user": {"type": "string"}}, "required": ["query"]}},
    {"name": "search_all_chats", "description": "ГЛОБАЛЬНЫЙ поиск сообщений сразу по всем чатам и диалогам, включая Избранное. Каждое совпадение помечено чатом (chat_id) и msg_id.", "parameters": {"type": "object", "properties": {"query": {"type": "string"}, "limit": {"type": "integer"}}, "required": ["query"]}},
    {"name": "send_message", "description": "Отправить текстовое сообщение.", "parameters": {"type": "object", "properties": {"chat": {"type": "string"}, "text": {"type": "string"}, "reply_to": {"type": "integer"}}, "required": ["chat", "text"]}},
    {"name": "edit_message", "description": "Изменить только своё сообщение.", "parameters": {"type": "object", "properties": {"message_id": {"type": "integer"}, "text": {"type": "string"}, "chat": {"type": "string"}}, "required": ["message_id", "text"]}},
    {"name": "delete_messages", "description": "Удалить до 50 сообщений.", "parameters": {"type": "object", "properties": {"message_ids": {"type": "array", "items": {"type": "integer"}}, "chat": {"type": "string"}, "revoke": {"type": "boolean"}}, "required": ["message_ids"]}},
    {"name": "forward_messages", "description": "Переслать до 50 сообщений.", "parameters": {"type": "object", "properties": {"message_ids": {"type": "array", "items": {"type": "integer"}}, "to_chat": {"type": "string"}, "from_chat": {"type": "string"}}, "required": ["message_ids", "to_chat"]}},
    {"name": "add_reaction", "description": "Поставить реакцию на сообщение.", "parameters": {"type": "object", "properties": {"message_id": {"type": "integer"}, "emoji": {"type": "string"}, "chat": {"type": "string"}}, "required": ["message_id", "emoji"]}},
    {"name": "list_triggers", "description": "Показать все триггеры.", "parameters": {"type": "object", "properties": {}}},
    {"name": "create_trigger", "description": "Создать триггер полной модели.", "parameters": {"type": "object", "properties": _TRIGGER_PROPERTIES, "required": ["match"]}},
    {"name": "update_trigger", "description": "Изменить поля триггера по id.", "parameters": {"type": "object", "properties": _TRIGGER_PROPERTIES, "required": ["id"]}},
    {"name": "delete_trigger", "description": "Удалить триггер по id.", "parameters": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}},
    {"name": "toggle_trigger", "description": "Включить или выключить триггер.", "parameters": {"type": "object", "properties": {"id": {"type": "string"}, "enabled": {"type": "boolean"}}, "required": ["id", "enabled"]}},
]


def _h(text):
    return html.escape(text or "", quote=False)


_MD_PRE_RE = re.compile(r"```[a-zA-Z0-9_+#.-]*\n?(.*?)```", re.DOTALL)
_MD_CODE_RE = re.compile(r"(?<![`\w])`([^`\n]+)`(?!`)")
_MD_BOLD_RE = re.compile(r"\*\*(?=\S)(.+?)(?<=\S)\*\*", re.DOTALL)
_MD_BOLD2_RE = re.compile(r"(?<!_)__(.+?)__(?!_)", re.DOTALL)
_MD_ITALIC_RE = re.compile(r"(?<![*\w])\*(?=\S)([^*\n]+?)(?<=\S)\*(?![*\w])")
_MD_LINK_RE = re.compile(r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)")
_MD_HEAD_RE = re.compile(r"(?m)^\s{0,3}#{1,6}\s+(.+?)\s*#*$")
_MD_BULLET_RE = re.compile(r"(?m)^([ \t]*)[-*+][ \t]+")


def _to_html(text):
    """Ответ модели считаем УЖЕ HTML (персона обязана отдавать Telegram-HTML) и
    НЕ экранируем -- иначе её же <b>..</b> превратятся в видимый «сырой текст»
    (та же логика, что в codex_ask: то, что сгенерила модель, доверяется как
    есть). Экранируем только содержимое code/pre. Плюс дораскрываем Markdown,
    если модель всё-таки скатилась в ** и ` `. Кривой HTML подхватит фолбэк
    _safe_edit -- он повторит правку без parse_mode."""
    if not text:
        return ""
    t = _MD_PRE_RE.sub(
        lambda m: "<pre>" + _h(m.group(1).rstrip()) + "</pre>", text
    )
    t = _MD_CODE_RE.sub(lambda m: "<code>" + _h(m.group(1)) + "</code>", t)
    t = _MD_LINK_RE.sub(r'<a href="\2">\1</a>', t)
    t = _MD_HEAD_RE.sub(r"<b>\1</b>", t)
    t = _MD_BOLD_RE.sub(r"<b>\1</b>", t)
    t = _MD_BOLD2_RE.sub(r"<b>\1</b>", t)
    t = _MD_ITALIC_RE.sub(r"<i>\1</i>", t)
    t = _MD_BULLET_RE.sub(r"\1• ", t)
    return t


@loader.tds
class GeminiMod(loader.Module):
    """Gemini на чистом API: контекст чата, реплаи, медиа, память диалога, поиск, картинки, триггеры."""

    strings = {
        "name": "Gemini",
        "no_key": (
            "🔑 <b>API-ключ не задан.</b>\n"
            "Установи: <code>{prefix}gakey ТВОЙ_КЛЮЧ</code>\n"
            "Ключ: <a href='https://aistudio.google.com/apikey'>aistudio.google.com/apikey</a>"
        ),
        "key_set": "✅ <b>Ключ сохранён.</b> Сообщение с ключом затёрто.",
        "key_cleared": "🧹 <b>Ключ удалён из конфига.</b>",
        "no_or_key": (
            "🔑 <b>Токен OpenRouter не задан.</b>\n"
            "Установи: <code>{prefix}gorkey ТВОЙ_ТОКЕН</code>\n"
            "Токен: <a href='https://openrouter.ai/keys'>openrouter.ai/keys</a>"
        ),
        "usage_ga": "⚠️ <b>{prefix}ga</b> &lt;запрос&gt; — можно реплаем на текст, фото или голосовое",
        "usage_gc": "⚠️ <b>{prefix}gac</b> &lt;запрос&gt; — чат с историей и памятью, без агентных инструментов",
        "usage_search": "⚠️ <b>{prefix}gasearch</b> &lt;запрос&gt;",
        "usage_tr": "⚠️ <b>{prefix}gatr</b> &lt;текст&gt; — или реплаем на сообщение",
        "usage_draw": "⚠️ <b>{prefix}gadraw</b> &lt;описание картинки&gt;",
        "thinking": "🤔 Думаю…",
        "drawing": "🎨 Рисую…",
        "empty": "🤷 <b>Пустой ответ от API.</b>",
        "error": "❌ <b>Ошибка API:</b> <code>{}</code>",
        "no_image": "🤷 <b>Модель не вернула картинку.</b> {}",
        "ctx_cleared": "🧹 <b>Контекст Gemini для этого чата очищен.</b>",
        "persona_head": "🎭 <b>Текущая персона Gemini:</b>\n<pre>{}</pre>",
        "persona_set": "✅ <b>Персона обновлена.</b>",
        "persona_reset": "♻️ <b>Персона сброшена к дефолту.</b>",
        "persona_empty": "❌ <b>Пустая персона.</b>",
        "no_triggers": (
            "🎯 <b>Триггеров нет.</b>\n"
            "Заведи руками: <code>{prefix}gatrig add keyword слово</code>\n"
            "Или просто попроси в <code>{prefix}ga</code>: «заведи триггер на …»"
        ),
        "trig_head": "🎯 <b>Триггеры Gemini:</b>",
        "trig_bad_args": "⚠️ <b>Мало аргументов.</b> <code>{prefix}gatrig</code> без аргументов — список и подсказка.",
        "trig_help": (
            "🎯 <b>{prefix}gatrig</b>\n"
            "<code>{prefix}gatrig</code> — список\n"
            "<code>{prefix}gatrig add keyword|regex|mention|reply|any|link|media [шаблон]</code>\n"
            "<code>{prefix}gatrig del &lt;id&gt;</code>\n"
            "<code>{prefix}gatrig on|off &lt;id&gt;</code>\n"
            "<code>{prefix}gatrig here &lt;id&gt;</code> — привязать к текущему чату\n"
            "<code>{prefix}gatrig global &lt;id&gt;</code> — снять привязку к чатам\n"
            "<code>{prefix}gatrig set &lt;id&gt; &lt;поле&gt; &lt;значение&gt;</code> — "
            "любое поле модели; списки через запятую\n"
            "<code>{prefix}gatrig scope|exclude &lt;id&gt; chats|senders &lt;id,...&gt;</code>\n"
            "<code>{prefix}gatrig action &lt;id&gt; reply|delete|warn|delete_and_reply|forward|mute|ban|react [значение]</code>"
        ),
    }

    strings_ru = {
        "_cls_doc": "Gemini на чистом API: контекст чата, реплаи, медиа, память диалога, поиск, картинки, триггеры.",
        "no_key": (
            "🔑 <b>API-ключ не задан.</b>\n"
            "Установи: <code>{prefix}gakey ТВОЙ_КЛЮЧ</code>\n"
            "Ключ: <a href='https://aistudio.google.com/apikey'>aistudio.google.com/apikey</a>"
        ),
        "key_set": "✅ <b>Ключ сохранён.</b> Сообщение с ключом затёрто.",
        "key_cleared": "🧹 <b>Ключ удалён из конфига.</b>",
        "no_or_key": (
            "🔑 <b>Токен OpenRouter не задан.</b>\n"
            "Установи: <code>{prefix}gorkey ТВОЙ_ТОКЕН</code>\n"
            "Токен: <a href='https://openrouter.ai/keys'>openrouter.ai/keys</a>"
        ),
        "usage_ga": "⚠️ <b>{prefix}ga</b> &lt;запрос&gt; — можно реплаем на текст, фото или голосовое",
        "usage_gc": "⚠️ <b>{prefix}gac</b> &lt;запрос&gt; — чат с историей и памятью, без агентных инструментов",
        "usage_search": "⚠️ <b>{prefix}gasearch</b> &lt;запрос&gt;",
        "usage_tr": "⚠️ <b>{prefix}gatr</b> &lt;текст&gt; — или реплаем на сообщение",
        "usage_draw": "⚠️ <b>{prefix}gadraw</b> &lt;описание картинки&gt;",
        "thinking": "🤔 Думаю…",
        "drawing": "🎨 Рисую…",
        "empty": "🤷 <b>Пустой ответ от API.</b>",
        "error": "❌ <b>Ошибка API:</b> <code>{}</code>",
        "no_image": "🤷 <b>Модель не вернула картинку.</b> {}",
        "ctx_cleared": "🧹 <b>Контекст Gemini для этого чата очищен.</b>",
        "persona_head": "🎭 <b>Текущая персона Gemini:</b>\n<pre>{}</pre>",
        "persona_set": "✅ <b>Персона обновлена.</b>",
        "persona_reset": "♻️ <b>Персона сброшена к дефолту.</b>",
        "persona_empty": "❌ <b>Пустая персона.</b>",
        "no_triggers": (
            "🎯 <b>Триггеров нет.</b>\n"
            "Заведи руками: <code>{prefix}gatrig add keyword слово</code>\n"
            "Или просто попроси в <code>{prefix}ga</code>: «заведи триггер на …»"
        ),
        "trig_head": "🎯 <b>Триггеры Gemini:</b>",
        "trig_bad_args": "⚠️ <b>Мало аргументов.</b> <code>{prefix}gatrig</code> без аргументов — список и подсказка.",
        "trig_help": (
            "🎯 <b>{prefix}gatrig</b>\n"
            "<code>{prefix}gatrig</code> — список\n"
            "<code>{prefix}gatrig add keyword|regex|mention|reply|any|link|media [шаблон]</code>\n"
            "<code>{prefix}gatrig del &lt;id&gt;</code>\n"
            "<code>{prefix}gatrig on|off &lt;id&gt;</code>\n"
            "<code>{prefix}gatrig here &lt;id&gt;</code> — привязать к текущему чату\n"
            "<code>{prefix}gatrig global &lt;id&gt;</code> — снять привязку к чатам\n"
            "<code>{prefix}gatrig set &lt;id&gt; &lt;поле&gt; &lt;значение&gt;</code> — "
            "любое поле модели; списки через запятую\n"
            "<code>{prefix}gatrig scope|exclude &lt;id&gt; chats|senders &lt;id,...&gt;</code>\n"
            "<code>{prefix}gatrig action &lt;id&gt; reply|delete|warn|delete_and_reply|forward|mute|ban|react [значение]</code>"
        ),
    }

    def __init__(self):
        self._genai = None
        self._genai_key = None
        self._convos = {}
        self._locks = {}
        self._trg_last = {}
        self._re_cache = {}
        self._trg_hour = {}
        self._trg_actions = []
        self._trg_rate_warned = 0.0
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "api_key",
                None,
                "API-ключ Google AI Studio",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "model",
                "gemini-3.6-flash",
                "Модель для текстовых ответов",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "image_model",
                "gemini-2.5-flash-image",
                "Модель для генерации картинок (.gadraw)",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "persona",
                DEFAULT_PERSONA,
                "Системный промпт (персона)",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "history_limit",
                15,
                "Сколько последних сообщений подгружать при первом запросе в чате",
                validator=loader.validators.Integer(minimum=0, maximum=60),
            ),
            loader.ConfigValue(
                "context_turns",
                8,
                "Глубина памяти диалога (пар вопрос-ответ) на чат",
                validator=loader.validators.Integer(minimum=0, maximum=30),
            ),
            loader.ConfigValue(
                "max_chars",
                4000,
                "Максимальная длина ответа в символах",
                validator=loader.validators.Integer(minimum=500, maximum=4096),
            ),
            loader.ConfigValue(
                "stream",
                True,
                "Живой стриминг ответа (только в личных чатах -- в группах правка-анимация ловит бан)",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "attach_media",
                True,
                "Прикладывать фото/стикеры/голосовые из чата к запросу",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "force_search",
                False,
                "Всегда включать поиск (Google grounding / OpenRouter web) в .g",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "provider",
                "gemini",
                "Провайдер по умолчанию для .ga / .gasearch / .gatr",
                validator=loader.validators.Choice(["gemini", "openrouter"]),
            ),
            loader.ConfigValue(
                "openrouter_key",
                None,
                "Токен OpenRouter (openrouter.ai/keys)",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "openrouter_model",
                "deepseek/deepseek-chat-v3-0324:free",
                "Модель OpenRouter для текста (список: openrouter.ai/models)",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "openrouter_image_model",
                "google/gemini-3.1-flash-lite-image",
                "Модель OpenRouter для .gadraw (платная; бесплатных image-моделей нет)",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "or_fallback",
                True,
                "Автоматически падать на OpenRouter, когда Gemini упёрся в лимит",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "triggers_enabled",
                True,
                "Мастер-выключатель watcher'а триггеров автоответа",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "tools_enabled",
                True,
                "Разрешить агентные инструменты в .ga/.gaor от владельца",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "allow_punitive_actions",
                False,
                "Разрешить триггерам mute и ban (опасное действие)",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "agent_max_rounds",
                8,
                "Максимум раундов agent function-calling за запрос",
                validator=loader.validators.Integer(minimum=1, maximum=16),
            ),
            loader.ConfigValue(
                "trigger_action_rate",
                20,
                "Общий лимит исполненных действий триггеров в минуту (0 - запрет)",
                validator=loader.validators.Integer(minimum=0, maximum=120),
            ),
        )

    async def client_ready(self):
        self._convos = {}
        self._locks = {}
        self._trg_last = {}
        self._re_cache = {}
        self._trg_hour = {}
        self._trg_actions = []
        self._trg_rate_warned = 0.0
        self._seed_default_triggers()

    def _seed_default_triggers(self):
        """Один раз при первой загрузке засеять триггер-обращение по имени
        (гемини / геминай, регистр не важен -- keyword по умолчанию IGNORECASE).
        Флаг в БД: если владелец потом удалит триггер, обратно он не вернётся."""
        if self.db.get("GeminiMod", "triggers_seeded", False):
            return
        self.db.set("GeminiMod", "triggers_seeded", True)
        if self._triggers():
            return
        seed = self._normalize_trigger({
            "id": "t1", "name": "Обращение по имени", "match": "keyword",
            "patterns": ["гемини", "геминай"], "action": "reply", "cooldown": 20,
        })
        self._save_triggers([seed])

    # ------------------------------------------------------------------ gemini
    def _client_or_none(self):
        key = self.config["api_key"]
        if not key:
            return None
        if self._genai is None or self._genai_key != key:
            self._genai = genai.Client(api_key=key)
            self._genai_key = key
        return self._genai

    def _search_tools(self):
        try:
            return [types.Tool(google_search=types.GoogleSearch())]
        except Exception:
            try:
                return [types.Tool(google_search_retrieval=types.GoogleSearchRetrieval())]
            except Exception:
                return []

    def _lock(self, chat_id):
        """Один asyncio.Lock на чат: сериализует конкурентные .ga / автоответы в
        одном чате, чтобы якорь дельты и память диалога не гонялись."""
        lk = self._locks.get(chat_id)
        if lk is None:
            lk = self._locks[chat_id] = asyncio.Lock()
        return lk

    @staticmethod
    def _as_int(v, default):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _as_bool(v):
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("1", "true", "yes", "on", "да", "y", "+")

    # --------------------------------------------------------------- tg helpers
    async def _work_message(self, message):
        """Редактировать можно только своё сообщение. Свой .ga -- редактируем на
        месте (выглядит будто юзер сам написал). Чужой триггер -- шлём новое от
        себя и правим его."""
        if message.out:
            return message
        return await message.respond("⏳")

    async def _safe_edit(self, message, text, parse_mode=None):
        cur = getattr(message, "text", "") or getattr(message, "raw_text", "")
        if cur == text:
            return message
        modes = [parse_mode, None] if parse_mode else [None]
        for mode in modes:
            for attempt in range(2):
                try:
                    if mode:
                        await message.edit(text, parse_mode=mode)
                    else:
                        await message.edit(text)
                    return message
                except asyncio.CancelledError:
                    raise
                except FloodWaitError as exc:
                    if attempt:
                        break
                    await asyncio.sleep(max(1, int(getattr(exc, "seconds", 1) or 1)))
                except Exception:
                    break
        return message

    def _topic_of(self, message):
        rt = getattr(message, "reply_to", None)
        if rt and getattr(rt, "forum_topic", False):
            return rt.reply_to_top_id or rt.reply_to_msg_id
        return None

    async def _get_reply_text(self, message):
        rid = getattr(message, "reply_to_msg_id", None)
        if not rid:
            return None
        try:
            msgs = await self._client.get_messages(message.chat_id, ids=[rid])
            return msgs[0].raw_text.strip() if msgs and msgs[0] and msgs[0].raw_text else None
        except Exception:
            return None

    async def _media_blob(self, m):
        """Одно сообщение -> кортеж (mime, bytes) или None. Нейтральное
        представление: Gemini получает его как inline Part, OpenRouter - как
        data:-URL (только картинки; аудио большинство OR-моделей не ест)."""
        if not self.config["attach_media"]:
            return None
        try:
            mime = None
            if getattr(m, "photo", None):
                mime = "image/jpeg"
                data = await self._client.download_media(m, bytes)
            elif getattr(m, "sticker", None):
                smime = getattr(m.sticker, "mime_type", "") or ""
                if smime == "image/webp":
                    mime = "image/webp"
                    data = await self._client.download_media(m, bytes)
                else:
                    mime = "image/jpeg"
                    data = await self._client.download_media(m, bytes, thumb=-1)
            elif getattr(m, "voice", None):
                mime = "audio/ogg"
                data = await self._client.download_media(m, bytes)
            elif getattr(m, "audio", None):
                mime = getattr(m.audio, "mime_type", "") or "audio/mpeg"
                data = await self._client.download_media(m, bytes)
            elif getattr(m, "document", None):
                dmime = getattr(m.document, "mime_type", "") or ""
                if dmime.startswith(("image/", "audio/")):
                    mime = dmime
                    data = await self._client.download_media(m, bytes)
                else:
                    return None
            else:
                return None
            if not data or len(data) > _MAX_MEDIA_BYTES:
                return None
            return (mime, bytes(data))
        except Exception:
            return None

    async def _format_messages(self, msgs, media_parts, name_cache=None, char_limit=300, seen_media_ids=None):
        """msgs -- в хронологическом порядке. Один в один формат codex_ask:
        строка [id=.., ДД.ММ ЧЧ:ММ, имя]: контент. Медиа отмечается и, если
        лимит не выбран, докладывается в media_parts как вложение #N.
        seen_media_ids -- id сообщений, чьё медиа уже приложено (реплай), чтобы
        не приложить тот же файл дважды."""
        if name_cache is None:
            name_cache = {}
        if seen_media_ids is None:
            seen_media_ids = set()
        lines = []
        for m in msgs:
            try:
                sid = getattr(m, "sender_id", None)
                name = str(sid) if sid else "???"
                if sid in name_cache:
                    name = name_cache[sid]
                elif sid:
                    try:
                        ent = await self._client.get_entity(sid)
                        name = (
                            getattr(ent, "first_name", "")
                            or getattr(ent, "title", "")
                            or getattr(ent, "username", "")
                            or str(sid)
                        )
                        name_cache[sid] = name
                    except Exception:
                        pass
                txt = m.raw_text or ""
                ts = ""
                if getattr(m, "date", None):
                    try:
                        ts = m.date.astimezone().strftime("%d.%m %H:%M") + " "
                    except Exception:
                        ts = ""
                pfx = f"[id={m.id}, {ts}{name}]: "
                is_media = bool(
                    m.document or m.photo or getattr(m, "sticker", None) or getattr(m, "gif", None)
                    or m.video or m.voice or getattr(m, "video_note", None)
                )
                caption = f" (подпись: {txt[:300]})" if txt.strip() and is_media else ""

                tag = ""
                if is_media and len(media_parts) < _MAX_MEDIA and m.id not in seen_media_ids:
                    blob = await self._media_blob(m)
                    if blob is not None:
                        media_parts.append(blob)
                        seen_media_ids.add(m.id)
                        tag = f" [вложение #{len(media_parts)}]"

                if m.document:
                    fn = ""
                    for a in getattr(m.document, "attributes", []):
                        if hasattr(a, "file_name"):
                            fn = getattr(a, "file_name", "")
                    dmime = getattr(m.document, "mime_type", "") or ""
                    if dmime.startswith("audio/"):
                        line = pfx + f"🎤 Аудиофайл{tag}{caption}"
                    else:
                        line = pfx + f"📄 {fn or 'файл'}{tag}{caption}"
                elif m.photo:
                    line = pfx + f"📷 Фото{tag}{caption}"
                elif getattr(m, "sticker", None):
                    line = pfx + f"🎭 Стикер{tag}{caption}"
                elif getattr(m, "gif", None):
                    line = pfx + f"🎬 GIF{caption}"
                elif m.video:
                    line = pfx + f"🎥 Видео{caption}"
                elif m.voice:
                    line = pfx + f"🎤 Голосовое{tag}{caption}"
                elif getattr(m, "video_note", None):
                    line = pfx + f"🎥 Кружок{caption}"
                elif getattr(m, "poll", None):
                    line = pfx + "📊 Опрос"
                elif getattr(m, "action", None):
                    line = pfx + f"⚡ {m.action.__class__.__name__}"
                elif txt.strip():
                    line = pfx + txt[:char_limit] + ("..." if len(txt) > char_limit else "")
                else:
                    line = pfx + "..."
                lines.append(line)
            except Exception:
                lines.append("[не удалось разобрать сообщение]")
        return "\n".join(lines)

    async def _get_chat_history(self, message, limit, media_parts, char_limit=300, exclude_id=None, seen_media_ids=None):
        topic_id = self._topic_of(message)
        try:
            kwargs = {"reply_to": topic_id} if topic_id else {}
            msgs = await self._client.get_messages(message.chat_id, limit=limit, **kwargs)
        except Exception as e:
            return f"[Не удалось получить историю: {e}]"
        if exclude_id is not None:
            msgs = [m for m in msgs if m.id != exclude_id]
        return await self._format_messages(
            list(reversed(msgs)), media_parts, char_limit=char_limit, seen_media_ids=seen_media_ids
        )

    _HISTORY_DELTA_CAP = 50

    async def _get_chat_history_delta(self, message, media_parts, seen_media_ids=None):
        """Как в codex_ask: первый .ga в чате -> фикс-окно, дальше -> только
        сообщения новее прошлого якоря (по возрастающим id Telegram), плюс сам
        якорь для сшивки. Экономит токены -- память диалога и так хранит
        предыдущее окно. Если новых сообщений больше _HISTORY_DELTA_CAP, самые
        старые из них (сразу за якорем) в окно не попадут -- об этом ставится
        явная пометка, якорь всё равно двигается на newest. Весь блок под
        _lock(chat_id): при конкурентных .ga / автоответах якорь не гонится."""
        chat_id = message.chat_id
        topic_id = self._topic_of(message)
        key = f"last_seen_id_{chat_id}_{topic_id}" if topic_id else f"last_seen_id_{chat_id}"

        async with self._lock(chat_id):
            anchor = self.db.get("GeminiMod", key, None)

            if anchor is None:
                text = await self._get_chat_history(
                    message, self.config["history_limit"], media_parts,
                    exclude_id=message.id, seen_media_ids=seen_media_ids,
                )
                self.db.set("GeminiMod", key, message.id)
                return text, False

            try:
                kwargs = {"reply_to": topic_id} if topic_id else {}
                new_msgs = await self._client.get_messages(
                    chat_id, min_id=anchor, limit=self._HISTORY_DELTA_CAP, **kwargs
                )
            except Exception as e:
                return f"[Не удалось получить историю: {e}]", True

            truncated = len(new_msgs) >= self._HISTORY_DELTA_CAP
            new_msgs = [m for m in new_msgs if m.id != message.id]
            newest_id = max([message.id] + [m.id for m in new_msgs])
            self.db.set("GeminiMod", key, newest_id)
            if not new_msgs:
                return "", True

            try:
                anchor_msgs = await self._client.get_messages(chat_id, ids=[anchor])
                anchor_msg = anchor_msgs[0] if anchor_msgs and anchor_msgs[0] else None
            except Exception:
                anchor_msg = None

            ordered = ([anchor_msg] if anchor_msg else []) + list(reversed(new_msgs))
            body = await self._format_messages(ordered, media_parts, seen_media_ids=seen_media_ids)
            if truncated:
                body = "[…часть сообщений между прошлым запросом и этим окном пропущена…]\n" + body
            return body, True

    # ------------------------------------------------------------------- render
    @staticmethod
    def _render(orig, body):
        return f"<blockquote>💬 {_h(orig)}</blockquote>\n🤖 {body}"

    def _load_convo(self, chat_id):
        if chat_id not in self._convos:
            stored = self.db.get("GeminiMod", f"convo_{chat_id}", []) or []
            self._convos[chat_id] = [tuple(x) for x in stored if isinstance(x, (list, tuple)) and len(x) == 2]
        return self._convos[chat_id]

    def _save_convo(self, chat_id, convo):
        convo = convo[-40:]
        self._convos[chat_id] = convo
        self.db.set("GeminiMod", f"convo_{chat_id}", [list(x) for x in convo])

    async def _pump(self, make_iter, work, orig, animate):
        """make_iter() -> синхронный генератор кусков текста. Крутим его в
        отдельном потоке (оба SDK синхронные), при animate раз в ~1.6с
        подрисовываем накопленное (сырым _h -- недостроенный HTML в parse_mode
        не суём). Возвращает полный текст, пробрасывает исключение бэкенда."""
        chunks = []
        done = threading.Event()
        box = {}

        def worker():
            try:
                for piece in make_iter():
                    if piece:
                        chunks.append(piece)
            except Exception as e:  # noqa: BLE001
                box["err"] = e
            finally:
                done.set()

        fut = asyncio.get_running_loop().run_in_executor(None, worker)
        last = ""
        while not done.is_set():
            await asyncio.sleep(_STREAM_EDIT_INTERVAL)
            if not animate:
                continue
            cur = "".join(chunks)
            if cur and cur != last:
                last = cur
                await self._safe_edit(
                    work, self._render(orig, _h(cur[: self.config["max_chars"]]) + " ▍"),
                    parse_mode="html",
                )
        await fut
        if box.get("err"):
            raise box["err"]
        return "".join(chunks)

    def _history_contents(self, convo):
        turns = self.config["context_turns"]
        return convo[-2 * turns:] if turns else []

    # ---------------------------------------------------------- backend payloads
    def _gemini_contents(self, prompt, media, convo):
        contents = []
        for role, txt in self._history_contents(convo):
            contents.append(types.Content(role=role, parts=[types.Part(text=txt)]))
        parts = [types.Part(text=prompt)]
        for mime, data in media:
            try:
                parts.append(types.Part.from_bytes(data=data, mime_type=mime))
            except Exception:
                pass
        contents.append(types.Content(role="user", parts=parts))
        return contents

    def _openrouter_messages(self, prompt, media, convo, system):
        msgs = [{"role": "system", "content": system}]
        for role, txt in self._history_contents(convo):
            msgs.append({"role": "assistant" if role == "model" else "user", "content": txt})
        content = [{"type": "text", "text": prompt}]
        for mime, data in media:
            if mime.startswith("image/"):
                b64 = base64.b64encode(data).decode()
                content.append(
                    {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}}
                )
        msgs.append({"role": "user", "content": content})
        return msgs

    def _make_gemini_iter(self, prompt, media, convo, use_search, system):
        client = self._client_or_none()
        contents = self._gemini_contents(prompt, media, convo)
        cfg_kwargs = {"system_instruction": system, "max_output_tokens": 4096}
        if use_search:
            tools = self._search_tools()
            if tools:
                cfg_kwargs["tools"] = tools
        try:
            config = types.GenerateContentConfig(**cfg_kwargs)
        except Exception:
            cfg_kwargs.pop("max_output_tokens", None)
            config = types.GenerateContentConfig(**cfg_kwargs)
        model = self.config["model"]

        def it():
            for ch in client.models.generate_content_stream(
                model=model, contents=contents, config=config
            ):
                piece = getattr(ch, "text", None)
                if piece:
                    yield piece

        return it

    def _make_openrouter_iter(self, prompt, media, convo, use_search, system):
        key = self._or_key()
        msgs = self._openrouter_messages(prompt, media, convo, system)
        body = {
            "model": self.config["openrouter_model"],
            "messages": msgs,
            "stream": True,
            "max_tokens": 4096,
        }
        if use_search:
            body["plugins"] = [{"id": "web"}]
        data = json.dumps(body).encode()

        def it():
            req = urllib.request.Request(
                _OPENROUTER_URL, data=data, method="POST",
                headers={
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://github.com/hikariatama/Heroku",
                    "X-Title": "Heroku GeminiMod",
                },
            )
            try:
                resp = urllib.request.urlopen(req, timeout=180)
            except urllib.error.HTTPError as e:
                detail = ""
                try:
                    detail = json.loads(e.read().decode("utf-8", "ignore")).get("error", {}).get("message", "")
                except Exception:
                    pass
                raise RuntimeError(f"OpenRouter {e.code}: {detail or e.reason}")
            with resp:
                for raw in resp:
                    line = raw.decode("utf-8", "ignore").strip()
                    if not line or line.startswith(":") or not line.startswith("data:"):
                        continue
                    payload = line[5:].strip()
                    if payload == "[DONE]":
                        break
                    try:
                        obj = json.loads(payload)
                    except Exception:
                        continue
                    if obj.get("error"):
                        raise RuntimeError(str(obj["error"].get("message") or obj["error"]))
                    choice = (obj.get("choices") or [{}])[0]
                    piece = (choice.get("delta") or {}).get("content")
                    if piece:
                        yield piece

        return it

    def _or_key(self):
        """Токен OpenRouter, очищенный от пробелов/переносов и случайного
        префикса 'Bearer ' (частая причина 401 Missing Authentication header
        при вводе ключа через .cfg)."""
        k = (self.config["openrouter_key"] or "").strip()
        if k.lower().startswith("bearer "):
            k = k[7:].strip()
        return k

    def _openrouter_headers(self):
        return {
            "Authorization": f"Bearer {self._or_key()}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/hikariatama/Heroku",
            "X-Title": "Heroku GeminiMod",
        }

    def _openrouter_post(self, body):
        """Синхронный не-стримовый POST в OpenRouter -> распарсенный JSON."""
        req = urllib.request.Request(
            _OPENROUTER_URL, data=json.dumps(body).encode(), method="POST",
            headers=self._openrouter_headers(),
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                obj = json.loads(r.read().decode("utf-8", "ignore"))
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = json.loads(e.read().decode("utf-8", "ignore")).get("error", {}).get("message", "")
            except Exception:
                pass
            raise RuntimeError(f"OpenRouter {e.code}: {detail or e.reason}")
        if obj.get("error"):
            raise RuntimeError(str(obj["error"].get("message") or obj["error"]))
        return obj

    # ------------------------------------------------------------ function calls
    def _gemini_tools(self):
        try:
            decls = [
                types.FunctionDeclaration(
                    name=s["name"], description=s["description"], parameters=s["parameters"]
                )
                for s in _TOOL_SCHEMAS
            ]
            return [types.Tool(function_declarations=decls)]
        except Exception:
            return []

    async def _gemini_tool_loop(self, message, prompt, media, convo, system, use_search=False):
        """Мультитёрн Gemini; SDK уходит в поток, Telegram-инструменты - в loop.
        use_search: добавить google-grounding РЯДОМ с function-инструментами; если
        конкретная версия SDK/модели не проглотит комбо -- откат на первом же
        вызове к чистым function-инструментам (агентность важнее web-поиска)."""
        client = self._client_or_none()
        if client is None:
            raise RuntimeError("нет ключа Gemini")
        contents = self._gemini_contents(prompt, media, convo)
        fn_tools = list(self._gemini_tools())
        tools = fn_tools + self._search_tools() if use_search else fn_tools

        def _build(tool_list):
            ck = {"system_instruction": system, "max_output_tokens": 4096}
            if tool_list:
                ck["tools"] = tool_list
                try:
                    ck["automatic_function_calling"] = types.AutomaticFunctionCallingConfig(
                        disable=True
                    )
                except Exception:
                    pass
            try:
                return types.GenerateContentConfig(**ck)
            except Exception:
                ck.pop("max_output_tokens", None)
                return types.GenerateContentConfig(**ck)

        config = _build(tools)
        combo_ok = bool(use_search)
        notools = False

        out, calls_total = [], 0
        for _ in range(self.config["agent_max_rounds"]):
            try:
                resp = await asyncio.to_thread(
                    client.models.generate_content,
                    model=self.config["model"], contents=contents, config=config,
                )
            except Exception:
                # Ступенчатая деградация: сначала снимаем grounding, потом -- все
                # инструменты (с честной припиской), только потом падаем.
                if combo_ok:
                    combo_ok = False
                    config = _build(fn_tools)
                elif not notools and fn_tools:
                    notools = True
                    system += (
                        "\n\nВНИМАНИЕ: инструменты недоступны в этом запросе. НЕ "
                        "заявляй о выполненных действиях (триггеры, удаление, "
                        "пересылка) - честно скажи, что выполнить не смог."
                    )
                    config = _build([])
                else:
                    raise
                resp = await asyncio.to_thread(
                    client.models.generate_content,
                    model=self.config["model"], contents=contents, config=config,
                )
            cand = (getattr(resp, "candidates", None) or [None])[0]
            content = getattr(cand, "content", None) if cand else None
            parts = getattr(content, "parts", None) or []
            calls = []
            for p in parts:
                if getattr(p, "text", None):
                    out.append(p.text)
                fc = getattr(p, "function_call", None)
                if fc:
                    calls.append(fc)
            if not calls:
                return "".join(out)
            contents.append(content)
            fr_parts = []
            for fc in calls:
                try:
                    a = dict(fc.args) if fc.args else {}
                except Exception:
                    a = {}
                if calls_total >= _MAX_TOOL_CALLS:
                    result = {"ok": False, "error": "достигнут лимит 16 вызовов инструментов"}
                else:
                    calls_total += 1
                    result = await self._exec_tool(fc.name, a, message)
                fr_parts.append(
                    types.Part.from_function_response(name=fc.name, response={"result": result})
                )
            contents.append(types.Content(role="user", parts=fr_parts))
            if calls_total >= _MAX_TOOL_CALLS:
                final = await asyncio.to_thread(
                    client.models.generate_content,
                    model=self.config["model"], contents=contents,
                    config=types.GenerateContentConfig(system_instruction=system, max_output_tokens=4096),
                )
                return "".join(out) + (getattr(final, "text", None) or "")
        final = await asyncio.to_thread(
            client.models.generate_content, model=self.config["model"], contents=contents,
            config=types.GenerateContentConfig(system_instruction=system, max_output_tokens=4096),
        )
        return "".join(out) + (getattr(final, "text", None) or "")

    async def _openrouter_tool_loop(self, message, prompt, media, convo, system, use_search=False):
        """OpenAI-style tools/tool_calls; сеть OpenRouter не блокирует event loop.
        use_search: web-плагин OpenRouter идёт вместе с function-инструментами."""
        if not self._or_key():
            raise RuntimeError("нет токена OpenRouter")
        msgs = self._openrouter_messages(prompt, media, convo, system)
        tools = [{"type": "function", "function": s} for s in _TOOL_SCHEMAS]
        extra = {"plugins": [{"id": "web"}]} if use_search else {}
        calls_total, stripped = 0, False
        for _ in range(self.config["agent_max_rounds"]):
            body = {"model": self.config["openrouter_model"], "messages": msgs, "max_tokens": 4096}
            if not stripped:
                body.update({"tools": tools, **extra})
            try:
                obj = await asyncio.to_thread(self._openrouter_post, body)
            except Exception:
                if stripped:
                    raise
                # Многие :free-модели OpenRouter не переваривают tools/plugins и
                # отдают 400 "Provider returned error". Отвечаем без инструментов,
                # но честно предупреждаем модель, чтобы не врала про действия.
                stripped = True
                for m in msgs:
                    if m.get("role") == "system":
                        m["content"] += (
                            "\n\nВНИМАНИЕ: текущая модель OpenRouter не поддерживает "
                            "инструменты. НЕ заявляй о выполненных действиях (триггеры, "
                            "удаление, пересылка) - честно скажи, что для этого нужна "
                            "tool-capable модель OpenRouter или провайдер gemini."
                        )
                        break
                obj = await asyncio.to_thread(
                    self._openrouter_post,
                    {"model": self.config["openrouter_model"], "messages": msgs, "max_tokens": 4096},
                )
            msg = (obj.get("choices") or [{}])[0].get("message") or {}
            tcs = msg.get("tool_calls") or []
            content = msg.get("content")
            if isinstance(content, list):
                content = " ".join(p.get("text", "") for p in content if isinstance(p, dict))
            if not tcs:
                return content or ""
            msgs.append({"role": "assistant", "content": content or "", "tool_calls": tcs})
            for tc in tcs:
                fn = tc.get("function") or {}
                try:
                    a = json.loads(fn.get("arguments") or "{}")
                except Exception:
                    a = {}
                if calls_total >= _MAX_TOOL_CALLS:
                    result = {"ok": False, "error": "достигнут лимит 16 вызовов инструментов"}
                else:
                    calls_total += 1
                    result = await self._exec_tool(fn.get("name") or "", a, message)
                msgs.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.get("id"),
                        "content": json.dumps(result, ensure_ascii=False),
                    }
                )
            if calls_total >= _MAX_TOOL_CALLS:
                break
        final = await asyncio.to_thread(
            self._openrouter_post,
            {"model": self.config["openrouter_model"], "messages": msgs, "max_tokens": 4096},
        )
        content = ((final.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        return " ".join(p.get("text", "") for p in content if isinstance(p, dict)) if isinstance(content, list) else content

    # ------------------------------------------------------------ trigger store
    def _triggers(self):
        raw = self.db.get("GeminiMod", "triggers", []) or []
        norm = [self._normalize_trigger(t) for t in raw if isinstance(t, dict)]
        if norm != raw:
            self.db.set("GeminiMod", "triggers", norm[:50])
        return norm

    def _save_triggers(self, lst):
        self.db.set("GeminiMod", "triggers", lst[:50])

    def _new_trigger_id(self):
        nums = [
            int(t["id"][1:]) for t in (self.db.get("GeminiMod", "triggers", []) or [])
            if isinstance(t.get("id"), str) and re.fullmatch(r"t\d+", t["id"])
        ]
        return f"t{(max(nums) + 1) if nums else 1}"

    def _normalize_trigger(self, source):
        """Старые записи с pattern читаем как patterns[0], новые поля - с дефолтами."""
        t = dict(source)
        raw_patterns = t.get("patterns")
        if not isinstance(raw_patterns, list):
            raw_patterns = [t.get("pattern", "")] if t.get("pattern") else []
        patterns = [str(x)[:200].strip() for x in raw_patterns if str(x).strip()][:_MAX_TRIGGER_PATTERNS]
        t.update({
            "id": str(t.get("id") or self._new_trigger_id()), "name": str(t.get("name") or "")[:80],
            "enabled": self._as_bool(t.get("enabled", True)), "match": str(t.get("match") or "keyword").lower(),
            "patterns": patterns, "match_case": self._as_bool(t.get("match_case", False)),
            "whole_word": self._as_bool(t.get("whole_word", True)), "chats": self._id_list(t.get("chats")),
            "exclude_chats": self._id_list(t.get("exclude_chats")), "senders": self._id_list(t.get("senders")),
            "exclude_senders": self._id_list(t.get("exclude_senders")),
            "only": str(t.get("only") or "").lower(), "ignore_admins": self._as_bool(t.get("ignore_admins", False)),
            "ignore_self": True, "cooldown": max(0, self._as_int(t.get("cooldown", 30), 30)),
            "max_per_hour": max(0, self._as_int(t.get("max_per_hour", 0), 0)),
            "action": str(t.get("action") or "reply").lower(), "action_chat": self._as_int(t.get("action_chat"), 0),
            "mute_seconds": max(0, self._as_int(t.get("mute_seconds", 0), 0)),
            "ban_seconds": max(0, self._as_int(t.get("ban_seconds", 0), 0)), "reaction": str(t.get("reaction") or "")[:80],
            "provider": t.get("provider") if t.get("provider") in ("gemini", "openrouter") else None,
            "search": self._as_bool(t.get("search", False)), "persona": self._text_or_none(t.get("persona")),
            "prompt": self._text_or_none(t.get("prompt")), "static_reply": self._text_or_none(t.get("static_reply")),
            "delete_after": max(0, self._as_int(t.get("delete_after", 0), 0)),
        })
        if t["match"] not in _MATCH_KINDS:
            t["match"] = "keyword"
        if t["only"] not in ("", "groups", "pm", "channels"):
            t["only"] = ""
        if t["action"] not in _ACTIONS:
            t["action"] = "reply"
        return t

    @staticmethod
    def _id_list(value):
        if value in (None, "", "clear", "-", []):
            return []
        items = value if isinstance(value, list) else str(value).replace(",", " ").split()
        return [int(x) for x in items if str(x).lstrip("-").isdigit()]

    @staticmethod
    def _text_or_none(value):
        text = str(value or "").strip()[:2000]
        return text or None

    def _compiled(self, kind, pattern, match_case=False, whole_word=True):
        ck = (kind, pattern, match_case, whole_word)
        rx = self._re_cache.get(ck)
        if rx is None:
            try:
                src = pattern if kind == "regex" else re.escape(pattern)
                if kind == "keyword" and whole_word:
                    src = r"(?<!\w)" + src + r"(?!\w)"
                rx = self._re_cache[ck] = re.compile(src, 0 if match_case else re.IGNORECASE)
            except re.error:
                rx = self._re_cache[ck] = False
        return rx

    def _apply_trigger_fields(self, t, args, message):
        if "name" in args:
            t["name"] = str(args["name"] or "")[:80]
        if "match" in args and str(args["match"]).lower() in _MATCH_KINDS:
            t["match"] = str(args["match"]).lower()
        if "patterns" in args or "pattern" in args:
            raw = args.get("patterns", args.get("pattern"))
            t["patterns"] = [str(x)[:200].strip() for x in (raw if isinstance(raw, list) else str(raw).split(",")) if str(x).strip()][:_MAX_TRIGGER_PATTERNS]
        for field in ("chats", "exclude_chats", "senders", "exclude_senders"):
            if field in args:
                t[field] = self._id_list(args[field])
        for field in ("match_case", "whole_word", "ignore_admins", "enabled"):
            if field in args:
                t[field] = self._as_bool(args[field])
        if "only" in args:
            only = str(args["only"] or "").lower()
            t["only"] = only if only in ("", "groups", "pm", "channels") else ""
        for field, cap in (("cooldown", 86400), ("max_per_hour", 10000), ("mute_seconds", 31536000), ("ban_seconds", 31536000), ("delete_after", 86400)):
            if field in args:
                t[field] = max(0, min(self._as_int(args[field], 0), cap))
        if "action" in args and str(args["action"]).lower() in _ACTIONS:
            t["action"] = str(args["action"]).lower()
        if "action_chat" in args:
            t["action_chat"] = self._as_int(args["action_chat"], 0)
        if "reaction" in args:
            t["reaction"] = str(args["reaction"] or "")[:80]
        if "provider" in args:
            p = str(args["provider"]).lower()
            t["provider"] = p if p in ("gemini", "openrouter") else None
        if "search" in args:
            t["search"] = self._as_bool(args["search"])
        for field in ("persona", "prompt", "static_reply"):
            if field in args:
                t[field] = self._text_or_none(args[field])
        if self._as_bool(args.get("this_chat")):
            ch = list(t.get("chats") or [])
            if message.chat_id not in ch:
                ch.append(message.chat_id)
            t["chats"] = ch
        if self._as_bool(args.get("clear_chats")):
            t["chats"] = []
        return self._normalize_trigger(t)

    def _validate_trigger(self, t):
        if t["match"] in ("keyword", "regex") and not t["patterns"]:
            return "для match=%s нужен patterns" % t["match"]
        if t["match"] == "regex":
            for pattern in t["patterns"]:
                try:
                    re.compile(pattern)
                except re.error as e:
                    return f"кривая регулярка: {e}"
        if t["action"] in ("mute", "ban") and not self.config["allow_punitive_actions"]:
            return "mute и ban выключены в allow_punitive_actions"
        if t["action"] == "forward" and not t["action_chat"]:
            return "для action=forward нужен action_chat"
        if t["action"] == "mute" and t["mute_seconds"] <= 0:
            return "для action=mute нужен mute_seconds > 0"
        if t["action"] == "react" and not t["reaction"]:
            return "для action=react нужен reaction"
        if t["action"] != "reply" and t["match"] == "any" and not t["senders"] and not t["patterns"]:
            return "деструктивный триггер без сужения по отправителю или паттерну запрещён"
        return None

    def _tool_create(self, args, message):
        if len(self._triggers()) >= 50:
            return {"ok": False, "error": "достигнут лимит 50 триггеров"}
        match = str(args.get("match") or "keyword").lower()
        if match not in _MATCH_KINDS:
            return {"ok": False, "error": "match: keyword|regex|mention|reply|any"}
        t = self._normalize_trigger({"id": self._new_trigger_id(), "match": match, "enabled": True, "cooldown": 30})
        t = self._apply_trigger_fields(t, {**args, "match": match}, message)
        error = self._validate_trigger(t)
        if error:
            return {"ok": False, "error": error}
        if not t["name"]:
            t["name"] = (t["patterns"] or [match])[0]
        lst = self._triggers()
        lst.append(t)
        self._save_triggers(lst)
        return {"ok": True, "trigger": t}

    def _tool_update(self, args, message):
        tid = str(args.get("id") or "")
        lst = self._triggers()
        for t in lst:
            if t.get("id") == tid:
                candidate = self._apply_trigger_fields(self._normalize_trigger(t), args, message)
                error = self._validate_trigger(candidate)
                if error:
                    return {"ok": False, "error": error}
                t.clear()
                t.update(candidate)
                self._save_triggers(lst)
                return {"ok": True, "trigger": t}
        return {"ok": False, "error": f"нет триггера {tid}"}

    _SAVED_ALIASES = {
        "me", "saved", "saved messages", "saved_messages", "savedmessages",
        "избранное", "избранные", "заметки", "self",
    }

    async def _resolve_chat(self, value, message):
        if value in (None, "", "here"):
            return message.chat_id, await self._client.get_entity(message.chat_id)
        if isinstance(value, str) and value.casefold().strip() in self._SAVED_ALIASES:
            entity = await self._client.get_entity("me")
            return getattr(entity, "id", None), entity
        if isinstance(value, str) and value.lstrip("-").isdigit():
            value = int(value)
        if not isinstance(value, str) or value.startswith("@") or isinstance(value, int):
            entity = await self._client.get_entity(value)
            return getattr(entity, "id", value), entity
        needle = value.casefold()
        async for dialog in self._client.iter_dialogs(limit=100):
            title = (getattr(dialog, "name", "") or "").casefold()
            if title == needle:
                return dialog.id, dialog.entity
        raise ValueError(f"чат «{value}» не найден")

    @staticmethod
    def _chat_info(entity, chat_id):
        return {"chat_id": chat_id, "title": getattr(entity, "title", None) or getattr(entity, "first_name", None) or getattr(entity, "username", None) or str(chat_id), "type": "channel" if getattr(entity, "broadcast", False) else ("group" if getattr(entity, "megagroup", False) or getattr(entity, "participants_count", None) else "pm")}

    async def _resolve_user(self, value):
        if value in (None, ""):
            return None
        if isinstance(value, str) and value.lstrip("-").isdigit():
            value = int(value)
        return await self._client.get_entity(value)

    async def _exec_tool(self, name, args, message):
        """Единственный async-исполнитель агентных инструментов Telegram и БД."""
        try:
            if not isinstance(args, dict):
                args = {}
            if name == "resolve_chat":
                chat_id, entity = await self._resolve_chat(args.get("name"), message)
                return {"ok": True, **self._chat_info(entity, chat_id)}
            if name == "list_dialogs":
                query = str(args.get("query") or "").casefold()
                limit = max(1, min(self._as_int(args.get("limit"), 20), 100))
                rows = []
                async for dialog in self._client.iter_dialogs(limit=100):
                    title = getattr(dialog, "name", "") or ""
                    if not query or query in title.casefold():
                        rows.append(self._chat_info(dialog.entity, dialog.id))
                    if len(rows) >= limit:
                        break
                return {"ok": True, "dialogs": rows}
            if name in ("read_chat", "search_messages"):
                chat_id, _ = await self._resolve_chat(args.get("chat"), message)
                limit = max(1, min(self._as_int(args.get("limit"), 30), 100))
                user = await self._resolve_user(args.get("from_user"))
                kw = {"limit": limit}
                if user is not None:
                    kw["from_user"] = user
                if name == "read_chat" and args.get("topic"):
                    kw["reply_to"] = self._as_int(args.get("topic"), 0)
                if name == "search_messages":
                    kw["search"] = str(args.get("query") or "")
                msgs = await self._client.get_messages(chat_id, **kw)
                return {"ok": True, "chat_id": chat_id, "messages": await self._format_messages(list(reversed(msgs)), [], char_limit=1000)}
            if name == "search_all_chats":
                q = str(args.get("query") or "").strip()
                if not q:
                    return {"ok": False, "error": "нужен query"}
                limit = max(1, min(self._as_int(args.get("limit"), 30), 100))
                try:
                    msgs = await self._client.get_messages(None, search=q, limit=limit)
                except Exception as e:  # noqa: BLE001
                    return {"ok": False, "error": f"глобальный поиск не удался: {str(e)[:160]}"}
                lines = []
                for m in reversed(list(msgs)):
                    ch = getattr(m, "chat", None)
                    cname = (
                        getattr(ch, "title", None) or getattr(ch, "first_name", None)
                        or getattr(ch, "username", None) or str(getattr(m, "chat_id", "?"))
                    )
                    ts = ""
                    if getattr(m, "date", None):
                        try:
                            ts = m.date.astimezone().strftime("%d.%m.%Y %H:%M")
                        except Exception:
                            ts = ""
                    body = (m.raw_text or "").strip().replace("\n", " ")[:500] or "[без текста]"
                    lines.append(f"[{cname} | chat_id={getattr(m, 'chat_id', '?')} | msg_id={m.id} | {ts}]: {body}")
                return {"ok": True, "scope": "all_chats", "count": len(lines),
                        "messages": "\n".join(lines) or "ничего не найдено"}
            if name == "send_message":
                chat_id, _ = await self._resolve_chat(args.get("chat"), message)
                sent = await self._client.send_message(chat_id, str(args.get("text") or ""), reply_to=args.get("reply_to"))
                return {"ok": True, "chat_id": chat_id, "message_id": sent.id}
            if name == "edit_message":
                chat_id, _ = await self._resolve_chat(args.get("chat"), message)
                mid = self._as_int(args.get("message_id"), 0)
                found = await self._client.get_messages(chat_id, ids=[mid])
                msg = found[0] if isinstance(found, list) and found else found
                if not msg or not getattr(msg, "out", False):
                    return {"ok": False, "error": "можно редактировать только своё сообщение"}
                await self._client.edit_message(chat_id, mid, str(args.get("text") or ""))
                return {"ok": True, "chat_id": chat_id, "message_id": mid}
            if name == "delete_messages":
                ids = self._id_list(args.get("message_ids"))
                if not ids or len(ids) > 50:
                    return {"ok": False, "error": "message_ids: от 1 до 50 id"}
                chat_id, _ = await self._resolve_chat(args.get("chat"), message)
                await self._client.delete_messages(chat_id, ids, revoke=self._as_bool(args.get("revoke", True)))
                # Telethon не кидает ошибку, когда прав на чужие сообщения нет --
                # просто молчаливо не удаляет. Перечитываем и докладываем правду.
                still = await self._client.get_messages(chat_id, ids=ids)
                gone = [i for i, m in zip(ids, still) if m is None]
                remained = [i for i in ids if i not in gone]
                if not gone:
                    return {"ok": False, "chat_id": chat_id, "remained": remained,
                            "error": "ничего не удалено (нет прав на чужие сообщения?)"}
                res = {"ok": True, "chat_id": chat_id, "deleted": gone}
                if remained:
                    res["remained"] = remained
                    res["warning"] = "часть сообщений не удалена -- вероятно нет прав"
                return res
            if name == "forward_messages":
                ids = self._id_list(args.get("message_ids"))
                if not ids or len(ids) > 50:
                    return {"ok": False, "error": "message_ids: от 1 до 50 id"}
                to_chat, _ = await self._resolve_chat(args.get("to_chat"), message)
                from_chat, _ = await self._resolve_chat(args.get("from_chat"), message)
                sent = await self._client.forward_messages(to_chat, ids, from_peer=from_chat)
                return {"ok": True, "from_chat": from_chat, "to_chat": to_chat, "message_ids": [getattr(x, "id", x) for x in (sent if isinstance(sent, list) else [sent])]}
            if name == "add_reaction":
                chat_id, _ = await self._resolve_chat(args.get("chat"), message)
                mid, emoji = self._as_int(args.get("message_id"), 0), str(args.get("emoji") or "")
                found = await self._client.get_messages(chat_id, ids=[mid])
                msg = found[0] if isinstance(found, list) and found else found
                if not msg:
                    return {"ok": False, "error": f"нет сообщения {mid}"}
                if hasattr(msg, "react"):
                    await msg.react(emoji)
                elif SendReactionRequest and ReactionEmoji:
                    peer = await self._client.get_input_entity(chat_id)
                    await self._client(SendReactionRequest(peer=peer, msg_id=mid, reaction=[ReactionEmoji(emoticon=emoji)]))
                else:
                    return {"ok": False, "error": "reactions не поддерживаются этой версией herokutl"}
                return {"ok": True, "chat_id": chat_id, "message_id": mid, "emoji": emoji}
            if name == "list_triggers":
                return {"ok": True, "triggers": self._triggers()}
            if name == "create_trigger":
                return self._tool_create(args, message)
            if name == "update_trigger":
                return self._tool_update(args, message)
            if name == "delete_trigger":
                tid = str(args.get("id") or "")
                lst = self._triggers()
                left = [t for t in lst if t.get("id") != tid]
                if len(left) == len(lst):
                    return {"ok": False, "error": f"нет триггера {tid}"}
                self._save_triggers(left)
                return {"ok": True, "deleted": tid}
            if name == "toggle_trigger":
                tid = str(args.get("id") or "")
                lst = self._triggers()
                for t in lst:
                    if t.get("id") == tid:
                        t["enabled"] = self._as_bool(args.get("enabled", True))
                        self._save_triggers(lst)
                        return {"ok": True, "id": tid, "enabled": t["enabled"]}
                return {"ok": False, "error": f"нет триггера {tid}"}
            return {"ok": False, "error": f"неизвестный инструмент {name}"}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": str(e)[:200]}

    def _triggers_list_html(self):
        lst = self._triggers()
        if not lst:
            return self.strings("no_triggers").format(prefix=self.get_prefix())
        rows = [self.strings("trig_head")]
        for t in lst:
            st = "🟢" if t.get("enabled", True) else "⚪"
            scope = "везде" if not t.get("chats") else f"чаты:{len(t['chats'])}"
            if t.get("exclude_chats"):
                scope += f" -{len(t['exclude_chats'])}"
            pat = f": {_h(', '.join(t.get('patterns') or []))}" if t.get("patterns") else ""
            extra = []
            if t.get("senders"):
                extra.append(f"senders:{len(t['senders'])}")
            if t.get("exclude_senders"):
                extra.append(f"-senders:{len(t['exclude_senders'])}")
            if t.get("only"):
                extra.append(t["only"])
            if t.get("ignore_admins"):
                extra.append("без админов")
            if t.get("max_per_hour"):
                extra.append(f"max/h:{t['max_per_hour']}")
            if t.get("provider"):
                extra.append(t["provider"])
            if t.get("search"):
                extra.append("web")
            if t.get("persona"):
                extra.append("персона")
            if t.get("prompt"):
                extra.append("шаблон")
            if t.get("action") != "reply":
                extra.append(t["action"])
            if t.get("static_reply"):
                extra.append("статик")
            if t.get("delete_after"):
                extra.append(f"delafter:{t['delete_after']}с")
            tail = (" · " + " ".join(extra)) if extra else ""
            rows.append(
                f"{st} <code>{t.get('id')}</code> <b>{_h(t.get('name', ''))}</b> "
                f"[{t.get('match')}{pat}] cd={t.get('cooldown', 0)}с · {scope}{tail}"
            )
        return "\n".join(rows)

    async def _trigger_matches(self, t, message, text):
        kind = t.get("match", "keyword")
        if kind == "any":
            return True
        if kind == "mention":
            return bool(getattr(message, "mentioned", False))
        if kind == "reply":
            if not getattr(message, "reply_to_msg_id", None):
                return False
            try:
                r = await message.get_reply_message()
            except Exception:
                return False
            return bool(r is not None and getattr(r, "out", False))
        if kind == "media":
            return bool(getattr(message, "media", None) or getattr(message, "photo", None) or getattr(message, "document", None))
        if kind == "link":
            urls = re.findall(r"https?://[^\s<>]+", text or "", re.IGNORECASE)
            return bool(urls) and (not t.get("patterns") or any(p.casefold() in u.casefold() for p in t["patterns"] for u in urls))
        if not text:
            return False
        return any(
            (rx := self._compiled(kind, pattern, t.get("match_case", False), t.get("whole_word", True)))
            and rx.search(text)
            for pattern in t.get("patterns") or []
        )

    async def _is_admin(self, message):
        try:
            p = await self._client.get_permissions(message.chat_id, message.sender_id)
            return bool(getattr(p, "is_admin", False) or getattr(p, "is_creator", False))
        except Exception:
            return False

    def _scope_matches(self, t, message):
        chat_id, sid = message.chat_id, getattr(message, "sender_id", None)
        if t["chats"] and chat_id not in t["chats"] or chat_id in t["exclude_chats"]:
            return False
        if t["senders"] and sid not in t["senders"] or sid in t["exclude_senders"]:
            return False
        is_pm = bool(getattr(message, "is_private", False))
        is_group = bool(getattr(message, "is_group", False) or getattr(message, "megagroup", False))
        is_channel = bool((getattr(message, "is_channel", False) or getattr(message, "broadcast", False)) and not is_group)
        if t["only"] == "pm" and not is_pm:
            return False
        if t["only"] == "groups" and not is_group:
            return False
        if t["only"] == "channels" and not is_channel:
            return False
        return True

    def _trigger_slot(self):
        now = time.monotonic()
        self._trg_actions = [x for x in self._trg_actions if now - x < 60]
        cap = self.config["trigger_action_rate"]
        if not cap or len(self._trg_actions) >= cap:
            if now - self._trg_rate_warned > 60:
                logger.warning("Gemini trigger action rate limit reached")
                self._trg_rate_warned = now
            return None
        self._trg_actions.append(now)
        return now

    def _undo_trigger_slot(self, slot):
        if slot in self._trg_actions:
            self._trg_actions.remove(slot)

    async def _delete_later(self, msg, seconds):
        await asyncio.sleep(seconds)
        try:
            await msg.delete()
        except Exception:
            logger.warning("не удалось удалить отложенный ответ триггера", exc_info=True)

    async def _trigger_reply(self, t, message, text):
        if t.get("static_reply"):
            reply = await message.respond(t["static_reply"])
        else:
            sender = getattr(message, "sender", None)
            name = getattr(sender, "first_name", None) or str(getattr(message, "sender_id", ""))
            prompt = t.get("prompt") or "Ответь на сообщение: {text}"
            prompt = prompt.format(text=text, name=name, chat=message.chat_id)
            reply = await self._ask(message, prompt, orig=(text[:200] or "…"), provider=t.get("provider"), use_search=t.get("search", False), system=t.get("persona"), allow_tools=False)
        if reply and t.get("delete_after", 0) > 0:
            asyncio.create_task(self._delete_later(reply, t["delete_after"]))
        return reply

    async def _execute_trigger_action(self, t, message, text):
        """Вернуть явный результат действия; caller откатывает кулдаун при ошибке."""
        slot = self._trigger_slot()
        if slot is None:
            return {"ok": False, "error": "глобальный лимит действий триггеров"}
        try:
            action = t["action"]
            if action == "reply":
                reply = await self._trigger_reply(t, message, text)
                if reply is None:
                    self._undo_trigger_slot(slot)
                    return {"ok": False, "error": "не удалось отправить ответ триггера"}
                return {"ok": True, "action": action, "message_id": getattr(reply, "id", None)}
            if action in ("delete", "warn", "delete_and_reply"):
                await self._client.delete_messages(message.chat_id, [message.id], revoke=True)
                check = await self._client.get_messages(message.chat_id, ids=[message.id])
                if check and check[0] is not None:
                    self._undo_trigger_slot(slot)
                    return {"ok": False, "action": action,
                            "error": "сообщение не удалено (юзербот не админ с правом удаления?)"}
                if action == "delete":
                    return {"ok": True, "action": action, "deleted": message.id}
                reply = await self._trigger_reply(t, message, text)
                if reply is None:
                    self._undo_trigger_slot(slot)
                    return {"ok": False, "error": "сообщение удалено, но ответ триггера не отправлен"}
                return {"ok": True, "action": action, "deleted": message.id, "message_id": getattr(reply, "id", None)}
            if action == "forward":
                sent = await self._client.forward_messages(t["action_chat"], [message.id], from_peer=message.chat_id)
                return {"ok": True, "action": action, "message_id": getattr(sent[0] if isinstance(sent, list) else sent, "id", None)}
            if action in ("mute", "ban"):
                if not self.config["allow_punitive_actions"]:
                    self._undo_trigger_slot(slot)
                    return {"ok": False, "error": "punitive actions выключены"}
                if not all((EditBannedRequest, ChatBannedRights)):
                    self._undo_trigger_slot(slot)
                    return {"ok": False, "error": "ban API недоступен в этой версии herokutl"}
                until = (0 if not t["ban_seconds"] else int(time.time()) + t["ban_seconds"]) if action == "ban" else int(time.time()) + t["mute_seconds"]
                rights = ChatBannedRights(until_date=until, **({"view_messages": True} if action == "ban" else {"send_messages": True}))
                await self._client(EditBannedRequest(channel=await self._client.get_input_entity(message.chat_id), participant=await self._client.get_input_entity(message.sender_id), banned_rights=rights))
                return {"ok": True, "action": action, "user_id": message.sender_id}
            if action == "react":
                result = await self._exec_tool("add_reaction", {"message_id": message.id, "emoji": t["reaction"], "chat": message.chat_id}, message)
                if not result.get("ok"):
                    self._undo_trigger_slot(slot)
                return result
            self._undo_trigger_slot(slot)
            return {"ok": False, "error": f"неизвестное действие {action}"}
        except Exception as e:  # noqa: BLE001
            self._undo_trigger_slot(slot)
            logger.warning("ошибка действия триггера %s", t.get("id"), exc_info=True)
            return {"ok": False, "error": str(e)[:200]}

    # ----------------------------------------------------------------- ask core
    async def _ask(
        self, message, question, *, orig=None, use_search=False,
        provider=None, system=None, with_context=True, allow_tools=None,
    ):
        orig = orig or question
        chat_id = message.chat_id
        provider = provider or self.config["provider"]
        system = system or (self.config["persona"] or DEFAULT_PERSONA)
        use_search = use_search or self.config["force_search"]
        work = await self._work_message(message)

        # Инструменты - только владельцу в .ga/.gaor; watcher передаёт allow_tools=False.
        owner = bool(getattr(message, "out", False))
        allow = owner if allow_tools is None else bool(allow_tools)
        # use_search больше НЕ отключает инструменты: grounding/web идёт рядом с
        # function-инструментами (см. _*_tool_loop). Иначе force_search или
        # .gasearch молча оставляли модель вообще без агентных тулзов.
        use_tools = (
            with_context and allow and self.config["tools_enabled"]
            and bool(self.config["api_key"] or self._or_key())
        )
        if use_tools:
            system = system + "\n\n" + TOOLS_GUIDE

        if provider == "openrouter" and not self._or_key():
            await self._safe_edit(
                work, self.strings("no_or_key").format(prefix=self.get_prefix()), parse_mode="html"
            )
            return None
        if provider == "gemini" and not self.config["api_key"]:
            await self._safe_edit(
                work, self.strings("no_key").format(prefix=self.get_prefix()), parse_mode="html"
            )
            return None

        media = []
        seen_media_ids = set()
        convo = []
        if with_context:
            reply_id = getattr(message, "reply_to_msg_id", None)
            reply_text = await self._get_reply_text(message)
            if reply_id:
                try:
                    rmsg = await message.get_reply_message()
                except Exception:
                    rmsg = None
                if rmsg is not None:
                    blob = await self._media_blob(rmsg)
                    if blob is not None:
                        media.append(blob)
                        seen_media_ids.add(rmsg.id)

            history, is_delta = await self._get_chat_history_delta(message, media, seen_media_ids)
            now = datetime.now().astimezone().strftime("%d.%m.%Y %H:%M")
            blocks = [f"Текущее время: {now}"]
            if history:
                label = "Новые сообщения с прошлого раза" if is_delta else "История чата"
                blocks.append(f"{label}:\n{history}")
            if reply_id:
                line = f"Реплай на сообщение (id={reply_id})"
                if reply_text:
                    line += f":\n{reply_text}"
                if media:
                    line += " [медиа приложено вложением]"
                blocks.append(line)
            prompt = "\n\n".join(blocks) + f"\n\nВопрос: {question}"
            convo = self._load_convo(chat_id)
        else:
            prompt = question

        animate = bool(getattr(message, "is_private", False)) and self.config["stream"] and not use_tools
        work = await self._safe_edit(
            work, self._render(orig, self.strings("thinking")), parse_mode="html"
        )

        chain = [provider]
        if provider == "gemini" and self.config["or_fallback"] and self._or_key():
            chain.append("openrouter")

        answer = None
        note = ""
        last_err = None
        for i, prov in enumerate(chain):
            try:
                async with self._client.action(chat_id, "typing"):
                    if use_tools:
                        loop_fn = (
                            self._openrouter_tool_loop if prov == "openrouter"
                            else self._gemini_tool_loop
                        )
                        answer = await loop_fn(message, prompt, media, convo, system, use_search)
                    else:
                        factory = (
                            self._make_openrouter_iter if prov == "openrouter"
                            else self._make_gemini_iter
                        )
                        make_iter = factory(prompt, media, convo, use_search, system)
                        answer = await self._pump(make_iter, work, orig, animate and i == 0)
                if (answer or "").strip():
                    if prov != provider:
                        note = (
                            f"<i>(Gemini молчит, ответ через OpenRouter · "
                            f"{_h(self.config['openrouter_model'])})</i>\n"
                        )
                    break
            except Exception as e:  # noqa: BLE001
                last_err = e
                if prov == "gemini" and i + 1 < len(chain) and _looks_quota(str(e)):
                    await self._safe_edit(
                        work, self._render(orig, "♻️ Gemini уперся в лимит, пробую OpenRouter…"),
                        parse_mode="html",
                    )
                    continue
                break

        if not (answer or "").strip():
            body = (
                self.strings("error").format(_h(str(last_err)[:300]))
                if last_err else self.strings("empty")
            )
            await self._safe_edit(work, self._render(orig, body), parse_mode="html")
            return None

        await self._safe_edit(
            work, self._render(orig, note + _to_html(answer[: self.config["max_chars"]])),
            parse_mode="html",
        )

        if with_context:
            latest = self._load_convo(chat_id)
            latest.append(("user", question[:4000]))
            latest.append(("model", answer[:4000]))
            self._save_convo(chat_id, latest)
        return work

    # ----------------------------------------------------------------- commands
    @loader.command(ru_doc="<ключ>|clear — сохранить или убрать ключ Google AI Studio")
    async def gakey(self, message):
        """<key>|clear — set or drop the Google AI Studio key"""
        args = utils.get_args_raw(message).strip()
        if not args:
            await utils.answer(
                message, self.strings("no_key").format(prefix=self.get_prefix())
            )
            return
        if args.lower() in {"clear", "reset", "-"}:
            self.config["api_key"] = None
            self._genai = self._genai_key = None
            await utils.answer(message, self.strings("key_cleared"))
            return
        self.config["api_key"] = args
        self._genai = self._genai_key = None
        # правим команду на месте -- ключ исчезает из чата, отдельный delete не нужен
        await utils.answer(message, self.strings("key_set"))

    @loader.command(ru_doc="<токен>|clear — сохранить или убрать токен OpenRouter")
    async def gorkey(self, message):
        """<token>|clear — set or drop the OpenRouter token"""
        args = utils.get_args_raw(message).strip()
        if not args:
            await utils.answer(
                message, self.strings("no_or_key").format(prefix=self.get_prefix())
            )
            return
        if args.lower() in {"clear", "reset", "-"}:
            self.config["openrouter_key"] = None
            await utils.answer(message, self.strings("key_cleared"))
            return
        self.config["openrouter_key"] = args
        await utils.answer(message, self.strings("key_set"))

    @loader.command(ru_doc="<запрос> — спросить Gemini: видит историю чата, реплай и медиа; умеет заводить триггеры по просьбе")
    async def ga(self, message):
        """<prompt> — ask Gemini with chat history, reply and media context; can manage triggers on request"""
        question = utils.get_args_raw(message).strip()
        if not question and not getattr(message, "reply_to_msg_id", None):
            await utils.answer(
                message, self.strings("usage_ga").format(prefix=self.get_prefix())
            )
            return
        if not question:
            question = "Отреагируй на сообщение, на которое я ответил."
        await self._ask(message, question)

    @loader.command(ru_doc="<запрос> — Gemini с историей чата и памятью, со стримингом, но без агентных инструментов")
    async def gac(self, message):
        """<prompt> — ask Gemini with chat context and streaming, without agent tools"""
        question = utils.get_args_raw(message).strip()
        if not question:
            await utils.answer(message, self.strings("usage_gc").format(prefix=self.get_prefix()))
            return
        await self._ask(message, question, allow_tools=False)

    @loader.command(ru_doc="<запрос> — спросить Gemini с поиском в Google")
    async def gasearch(self, message):
        """<query> — ask Gemini with Google Search grounding"""
        question = utils.get_args_raw(message).strip()
        if not question:
            await utils.answer(
                message, self.strings("usage_search").format(prefix=self.get_prefix())
            )
            return
        await self._ask(message, question, use_search=True)

    @loader.command(ru_doc="<запрос> — то же, что .ga, но принудительно через OpenRouter")
    async def gaor(self, message):
        """<prompt> — same as .ga but force the OpenRouter backend"""
        question = utils.get_args_raw(message).strip()
        if not question and not getattr(message, "reply_to_msg_id", None):
            await utils.answer(
                message, self.strings("usage_ga").format(prefix=self.get_prefix())
            )
            return
        if not question:
            question = "Отреагируй на сообщение, на которое я ответил."
        await self._ask(message, question, provider="openrouter")

    @loader.command(ru_doc="[gemini|openrouter] — показать или сменить провайдера по умолчанию")
    async def gaprovider(self, message):
        """[gemini|openrouter] — show or switch the default provider"""
        arg = utils.get_args_raw(message).strip().lower()
        if arg in ("gemini", "openrouter"):
            self.config["provider"] = arg
            await utils.answer(message, f"✅ <b>Провайдер по умолчанию:</b> <code>{arg}</code>")
            return
        await utils.answer(
            message,
            f"🔀 <b>Провайдер сейчас:</b> <code>{self.config['provider']}</code>\n"
            f"Модель Gemini: <code>{_h(self.config['model'])}</code>\n"
            f"Модель OpenRouter: <code>{_h(self.config['openrouter_model'])}</code>\n"
            f"Автофолбэк на OpenRouter: <b>{'вкл' if self.config['or_fallback'] else 'выкл'}</b>\n"
            f"Сменить: <code>{self.get_prefix()}gprovider gemini|openrouter</code>",
        )

    @loader.command(ru_doc="<текст> — перевести на русский, без контекста чата")
    async def gatr(self, message):
        """<text> — translate to Russian, no chat context"""
        text = utils.get_args_raw(message).strip()
        if not text and getattr(message, "reply_to_msg_id", None):
            reply = await message.get_reply_message()
            if reply is not None:
                text = reply.raw_text or ""
        if not text.strip():
            await utils.answer(
                message, self.strings("usage_tr").format(prefix=self.get_prefix())
            )
            return
        await self._ask(
            message,
            f"Переведи на русский. Только перевод, без пояснений и кавычек:\n\n{text}",
            orig=(text[:200] + ("…" if len(text) > 200 else "")),
            system="Ты - точный переводчик. Отдаёшь только перевод, без комментариев.",
            with_context=False,
            allow_tools=False,
        )

    @loader.command(ru_doc="<описание> — сгенерировать картинку (нужны платные кредиты)")
    async def gadraw(self, message):
        """<prompt> — generate an image (needs paid credits)"""
        prompt = utils.get_args_raw(message).strip()
        work = await self._work_message(message)
        if not prompt:
            await self._safe_edit(
                work, self.strings("usage_draw").format(prefix=self.get_prefix()), parse_mode="html"
            )
            return

        provider = self.config["provider"]
        if provider == "openrouter" and not self._or_key():
            await self._safe_edit(
                work, self.strings("no_or_key").format(prefix=self.get_prefix()), parse_mode="html"
            )
            return
        if provider == "gemini" and not self.config["api_key"]:
            await self._safe_edit(
                work, self.strings("no_key").format(prefix=self.get_prefix()), parse_mode="html"
            )
            return

        work = await self._safe_edit(work, self.strings("drawing"))

        chain = [provider]
        if provider == "gemini" and self.config["or_fallback"] and self._or_key():
            chain.append("openrouter")

        img, note, last_err = None, "", None
        for i, prov in enumerate(chain):
            fn = self._openrouter_image if prov == "openrouter" else self._gemini_image
            try:
                img, note = await asyncio.to_thread(fn, prompt)
                if img:
                    break
            except Exception as e:  # noqa: BLE001
                last_err = e
                if prov == "gemini" and i + 1 < len(chain) and _looks_quota(str(e)):
                    await self._safe_edit(work, "♻️ Gemini лимит на картинки, пробую OpenRouter…")
                    continue
                break

        if not img:
            await self._safe_edit(
                work, self.strings("no_image").format(_h(str(last_err)[:300])), parse_mode="html"
            )
            return

        bio = io.BytesIO(img)
        bio.name = "image.png"
        try:
            await self._client.send_file(
                message.chat_id,
                bio,
                caption=(note[:1000] or None),
                reply_to=getattr(message, "reply_to_msg_id", None),
            )
            await work.delete()
        except Exception as e:  # noqa: BLE001
            await self._safe_edit(work, self.strings("error").format(_h(str(e)[:300])), parse_mode="html")

    def _gemini_image(self, prompt):
        """Блокирующий -- звать через asyncio.to_thread. -> (bytes, подпись)."""
        client = self._client_or_none()
        resp = client.models.generate_content(
            model=self.config["image_model"], contents=prompt
        )
        img, note = None, ""
        for cand in getattr(resp, "candidates", None) or []:
            content = getattr(cand, "content", None)
            for part in getattr(content, "parts", None) or []:
                inline = getattr(part, "inline_data", None)
                if inline is not None and getattr(inline, "data", None):
                    img = inline.data
                elif getattr(part, "text", None):
                    note += part.text
        if not img:
            raise RuntimeError(note[:300] or "модель не вернула картинку")
        return img, note

    def _openrouter_image(self, prompt):
        """Блокирующий. OpenRouter отдаёт картинки в message.images как data:URL."""
        obj = self._openrouter_post(
            {
                "model": self.config["openrouter_image_model"],
                "messages": [{"role": "user", "content": prompt}],
                "modalities": ["image", "text"],
            }
        )
        msg = (obj.get("choices") or [{}])[0].get("message") or {}
        note = msg.get("content") or ""
        if isinstance(note, list):
            note = " ".join(p.get("text", "") for p in note if isinstance(p, dict))
        for im in msg.get("images") or []:
            url = (im.get("image_url") or {}).get("url") or ""
            if url.startswith("data:") and "," in url:
                return base64.b64decode(url.split(",", 1)[1]), note
        raise RuntimeError("в ответе OpenRouter нет картинки")

    @loader.command(ru_doc="очистить память диалога и якорь истории для этого чата")
    async def ganew(self, message):
        """reset the dialog memory and history anchor for this chat"""
        chat_id = message.chat_id
        self._convos.pop(chat_id, None)
        self.db.set("GeminiMod", f"convo_{chat_id}", [])
        for key in (f"last_seen_id_{chat_id}", f"last_seen_id_{chat_id}_{self._topic_of(message)}"):
            self.db.set("GeminiMod", key, None)
        await utils.answer(message, self.strings("ctx_cleared"))

    @loader.command(ru_doc="[reset|текст] — показать, задать или сбросить персону (можно реплаем на текст/файл)")
    async def gapersona(self, message):
        """[reset|text] — show, set or reset the persona (reply to text/file works)"""
        arg = utils.get_args_raw(message).strip()

        if arg.lower() == "reset":
            self.config["persona"] = DEFAULT_PERSONA
            await utils.answer(message, self.strings("persona_reset"))
            return

        new_text = arg or None
        if not new_text and getattr(message, "reply_to_msg_id", None):
            reply = await message.get_reply_message()
            if reply is not None and reply.document and (getattr(reply.document, "size", 0) or 0) <= 128 * 1024:
                try:
                    new_text = (await reply.download_media(bytes)).decode("utf-8")
                except Exception:
                    new_text = None
            elif reply is not None and (reply.raw_text or "").strip():
                new_text = reply.raw_text

        if new_text is None:
            await utils.answer(
                message, self.strings("persona_head").format(_h(self.config["persona"][:3500]))
            )
            return
        if not new_text.strip():
            await utils.answer(message, self.strings("persona_empty"))
            return
        self.config["persona"] = new_text.strip()
        await utils.answer(message, self.strings("persona_set"))

    @loader.command(ru_doc="[add|del|on|off|here|global|set ...] — руками управлять триггерами автоответа")
    async def gatrig(self, message):
        """[add|del|on|off|here|global|set ...] — manage auto-reply triggers by hand"""
        parts = utils.get_args_raw(message).split()
        prefix = self.get_prefix()
        if not parts:
            await utils.answer(
                message,
                self._triggers_list_html() + "\n\n" + self.strings("trig_help").format(prefix=prefix),
            )
            return

        sub = parts[0].lower()
        rest = parts[1:]
        try:
            if sub == "add":
                res = self._tool_create(
                    {"match": (rest[0].lower() if rest else ""), "patterns": [" ".join(rest[1:])]},
                    message,
                )
            elif sub in ("del", "rm", "delete"):
                res = await self._exec_tool("delete_trigger", {"id": rest[0]}, message)
            elif sub in ("on", "off"):
                res = await self._exec_tool(
                    "toggle_trigger", {"id": rest[0], "enabled": sub == "on"}, message
                )
            elif sub == "here":
                res = self._tool_update({"id": rest[0], "this_chat": True}, message)
            elif sub in ("global", "everywhere"):
                res = self._tool_update({"id": rest[0], "clear_chats": True}, message)
            elif sub == "set":
                res = self._tool_update(
                    {"id": rest[0], rest[1].lower(): " ".join(rest[2:])}, message
                )
            elif sub == "scope":
                res = self._tool_update({"id": rest[0], rest[1].lower(): " ".join(rest[2:])}, message)
            elif sub == "exclude":
                field = "exclude_" + rest[1].lower()
                res = self._tool_update({"id": rest[0], field: " ".join(rest[2:])}, message)
            elif sub == "action":
                values = {"id": rest[0], "action": rest[1].lower()}
                if len(rest) > 2:
                    target = {"forward": "action_chat", "mute": "mute_seconds", "ban": "ban_seconds", "react": "reaction"}.get(values["action"])
                    if target:
                        values[target] = " ".join(rest[2:])
                res = self._tool_update(values, message)
            else:
                await utils.answer(
                    message,
                    self._triggers_list_html() + "\n\n"
                    + self.strings("trig_help").format(prefix=prefix),
                )
                return
        except IndexError:
            await utils.answer(message, self.strings("trig_bad_args").format(prefix=prefix))
            return

        if res.get("ok") is False:
            await utils.answer(message, f"❌ <code>{_h(str(res.get('error')))}</code>")
            return
        await utils.answer(
            message,
            self._triggers_list_html()
            + f"\n\n<pre>{_h(json.dumps(res, ensure_ascii=False, indent=1))}</pre>",
        )

    # ------------------------------------------------------------------ watcher
    @loader.watcher(only_messages=True, no_commands=True)
    async def watcher(self, message):
        """triggers_enabled: смотрим весь входящий поток, по таблице триггеров
        отвечаем без команды. Первый совпавший триггер выигрывает, кулдаун -- на
        пару (trigger_id, чат, тема). Свои исходящие и команды игнорим -- петли
        нет. Инструменты триггеров в этом пути НЕ подключаются (allow_tools=False)."""
        if not self.config["triggers_enabled"] or getattr(message, "out", False):
            return
        triggers = [t for t in self._triggers() if t.get("enabled", True)]
        if not triggers:
            return
        text = (message.raw_text or "").strip()
        if text.startswith(self.get_prefix()):
            return

        chat_id = message.chat_id
        topic = self._topic_of(message)
        now = time.monotonic()

        for t in triggers:
            if not self._scope_matches(t, message):
                continue
            if t.get("ignore_admins") and await self._is_admin(message):
                continue
            cd = t.get("cooldown", 30)
            key = (t.get("id"), chat_id, topic)
            if cd and now - self._trg_last.get(key, 0.0) < cd:
                continue
            hour = [x for x in self._trg_hour.get(key, []) if now - x < 3600]
            if t.get("max_per_hour", 0) and len(hour) >= t["max_per_hour"]:
                self._trg_hour[key] = hour
                continue
            if not await self._trigger_matches(t, message, text):
                continue
            if not text and t.get("match") not in ("reply", "mention", "any", "media"):
                continue

            self._trg_last[key] = now
            result = await self._execute_trigger_action(t, message, text)
            if result.get("ok"):
                hour.append(now)
                self._trg_hour[key] = hour
            else:
                self._trg_last.pop(key, None)
                logger.warning("триггер %s не выполнен: %s", t.get("id"), result.get("error"))
            return  # первый совпавший триггер выигрывает
