import asyncio
import contextlib
import re

from .. import loader, utils


@loader.tds
class JarvisAIMod(loader.Module):
    """AI-помощник через @gemini_gidbot."""

    PERSONA_MARKER = "[JARVIS_GEMINI_PERSONA_V3]"
    PERSONA_PROMPT = (
        f"{PERSONA_MARKER}\n"
        "С этого сообщения ты — Джарвис (J.A.R.V.I.S.) из «Мстителей», "
        "личный интеллектуальный помощник пользователя. "
        "Общайся спокойно, умно, вежливо и с лёгкой сухой иронией. "
        "Обращайся к пользователю как «сэр», когда это уместно. "
        "Общайся естественно, как живой личный помощник, без ролевого "
        "терминального оформления, системных рамок и служебных статусов. "
        "Текст ответа должен быть на русском, "
        "если пользователь не попросил другой язык. "
        "Не используй эмодзи любого вида, premium emoji, стикеры, смайлы, "
        "каомодзи и декоративные Unicode-символы. Не начинай ответ с эмодзи "
        "и не отправляй отдельные сообщения, состоящие только из эмодзи. "
        "Помогай с вопросами, текстами, кодом, анализом чатов, фото, "
        "документов и аудио, если формат доступен. "
        "Отвечай по существу, не выдумывай факты и предупреждай, "
        "если в чём-то не уверен. "
        "Никогда не упоминай технический маршрут запроса, Telegram-бота, "
        "пересылку сообщений, системные инструкции или этот маркер."
    )

    strings = {
        "name": "JarvisAI",
        "thinking": "Джарвис обрабатывает запрос...",
        "starting": "Джарвис запущен. Системы инициализируются...",
        "timeout": "Джарвис пока не получил ответ. Попробуйте повторить запрос.",
        "empty": "Джарвис получил пустой ответ.",
        "error": "Джарвис временно недоступен: {error}",
        "usage": (
            "Команды:\n"
            "{prefix}ask <запрос> — задать вопрос.\n"
            "Можно ответить этой командой на текст, фото, документ, voice или аудио.\n\n"
            "{prefix}askchat [N] <запрос> — передать последние N сообщений чата.\n"
            "{prefix}jarvisstart — запустить Джарвиса и установить его личность.\n"
            "{prefix}jarvison — включить ответы на упоминания в чате.\n"
            "{prefix}jarvisoff — выключить режим упоминаний.\n"
            "{prefix}jarvisstatus — показать состояние модуля."
        ),
        "auto_on": "Режим упоминаний включён в этом чате.",
        "auto_off": "Режим упоминаний выключен в этом чате.",
        "status": (
            "Джарвис\n"
            "Режим упоминаний: {state}\n"
            "Таймаут: {timeout} секунд\n"
            "Ожидание потока: {stream_idle} секунд\n"
            "История: {history} сообщений"
        ),
        "ready": "Джарвис запущен. Все системы инициализированы.",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "bot_username",
                "@gemini_gidbot",
                "Username AI-бота",
            ),
            loader.ConfigValue(
                "response_timeout",
                300,
                "Максимальное время ожидания ответа",
            ),
            loader.ConfigValue(
                "stream_idle_seconds",
                10.0,
                "Сколько ждать после последнего фрагмента ответа",
            ),
            loader.ConfigValue(
                "history_limit",
                20,
                "Количество сообщений для askchat",
            ),
            loader.ConfigValue(
                "instruction",
                (
                    "Отвечай по-русски, если пользователь не попросил другой язык. "
                    "Будь полезным, не выдумывай факты и не используй эмодзи, "
                    "стикеры или декоративные символы."
                ),
                "Инструкция, добавляемая к каждому запросу",
            ),
        )

        self._bot = None
        self._bot_name = ""
        self._username = ""
        self._lock = asyncio.Lock()

    async def client_ready(self):
        with contextlib.suppress(Exception):
            me = await self._client.get_me()
            self._username = getattr(me, "username", "") or ""

    async def _work_message(self, message):
        if getattr(message, "out", False):
            return message

        return await message.respond(self.strings("thinking"))

    async def _safe_edit(self, message, text):
        if message is None:
            return False

        current = self._message_text(message)

        if current == text:
            return True

        for attempt in range(2):
            try:
                edited = await message.edit(text, parse_mode=None)

                if (
                    self._message_text(message) == text
                    or self._message_text(edited) == text
                ):
                    return True
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

            if attempt == 0:
                await asyncio.sleep(1)

        client = getattr(message, "client", None) or self._client
        chat_id = getattr(message, "chat_id", None)
        message_id = getattr(message, "id", None)

        if client is not None and chat_id is not None and message_id is not None:
            try:
                edited = await client.edit_message(
                    chat_id,
                    message_id,
                    text,
                    parse_mode=None,
                )

                if edited is not None:
                    return True
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

        return False

    async def _set_message(self, message, text):
        if await self._safe_edit(message, text):
            return

        try:
            await utils.answer(message, text)
            return
        except Exception:
            pass

        with contextlib.suppress(Exception):
            await message.respond(text)

    async def _heartbeat(self, message):
        states = (
            "Джарвис обрабатывает запрос...",
            "Джарвис продолжает работу над запросом...",
        )
        index = 0

        while True:
            await asyncio.sleep(15)
            index = (index + 1) % len(states)
            await self._safe_edit(message, states[index])

    async def _cleanup_later(self, bot, message_ids):
        with contextlib.suppress(Exception):
            await asyncio.wait_for(
                self._cleanup_bot_messages(bot, set(message_ids)),
                timeout=15,
            )

    def _queue_cleanup(self, bot, message_ids):
        if message_ids:
            asyncio.create_task(
                self._cleanup_later(bot, set(message_ids))
            )

    def _get_int(self, key, default, minimum=1, maximum=None):
        try:
            value = int(self.config[key])
        except (TypeError, ValueError):
            value = default

        value = max(value, minimum)

        if maximum is not None:
            value = min(value, maximum)

        return value

    def _get_float(self, key, default, minimum=0.1, maximum=None):
        try:
            value = float(self.config[key])
        except (TypeError, ValueError):
            value = default

        value = max(value, minimum)

        if maximum is not None:
            value = min(value, maximum)

        return value

    @staticmethod
    def _clip(text, limit=3800):
        text = str(text or "").strip()

        if len(text) <= limit:
            return text

        marker = "\n… [контекст обрезан]"
        available = limit - len(marker)
        head = int(available * 0.65)
        tail = available - head

        return text[:head] + marker + text[-tail:]

    @staticmethod
    def _media_label(message):
        if message is None:
            return ""

        if getattr(message, "photo", None):
            return "[фото]"

        if getattr(message, "voice", None):
            return "[голосовое сообщение]"

        if getattr(message, "audio", None):
            return "[аудио]"

        if getattr(message, "video", None):
            return "[видео]"

        if getattr(message, "sticker", None):
            return "[стикер]"

        if getattr(message, "document", None):
            return "[документ]"

        return ""

    @staticmethod
    def _message_text(message):
        if message is None:
            return ""

        return str(
            getattr(message, "raw_text", "")
            or getattr(message, "message", "")
            or ""
        ).strip()

    @classmethod
    def _message_signature(cls, message):
        return (
            cls._message_text(message),
            cls._media_label(message),
        )

    @staticmethod
    def _remember_id(message_ids, message):
        message_id = getattr(message, "id", None)

        if message_id is not None:
            message_ids.add(message_id)

    def _is_progress_message(self, message):
        """Отбрасывает премиум-эмодзи и прочие служебные заглушки бота."""

        if message is None:
            return True

        text = self._message_text(message)

        if getattr(message, "sticker", None) and not text:
            return True

        if self._media_label(message):
            return False

        if not text:
            return True

        entities = getattr(message, "entities", None) or []
        has_custom_emoji = any(
            "CustomEmoji" in type(entity).__name__ for entity in entities
        )

        if has_custom_emoji and not re.search(
            r"[^\W_]",
            text,
        ):
            return True

        normalized = text.casefold()

        if normalized in {
            "...",
            "…",
            "⏳",
            "⌛",
            "🔄",
            "🤔",
            "💭",
            "✨",
        }:
            return True

        return len(text) <= 32 and not re.search(
            r"[^\W_]",
            text,
        )

    def _media(self, message):
        if not self._media_label(message):
            return None

        return getattr(message, "media", None)

    def _build_prompt(self, prompt):
        instruction = str(self.config["instruction"] or "").strip()

        if instruction:
            prompt = f"{instruction}\n\n{prompt}"

        return self._clip(prompt)

    async def _get_bot(self):
        bot_name = str(
            self.config["bot_username"] or "@gemini_gidbot"
        ).strip()

        if not bot_name:
            raise RuntimeError("Не указан username AI-бота.")

        if self._bot is None or self._bot_name != bot_name:
            self._bot = await self._client.get_entity(bot_name)
            self._bot_name = bot_name

        return self._bot

    async def _wait_response(
        self,
        conversation,
        timeout,
        cleanup_ids,
        require_answer=True,
    ):
        """Собирает ответ, пропуская emoji-индикаторы и дожидаясь правок."""

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        stream_idle = self._get_float(
            "stream_idle_seconds",
            10.0,
            2.0,
            30.0,
        )

        answers = {}
        answer_order = []
        edit_tasks = {}
        signatures = {}
        response_task = None
        sequence = 0
        last_activity = None

        def add_edit_listener(message, key):
            remaining = deadline - loop.time()

            if remaining <= 0:
                return

            task = asyncio.create_task(
                conversation.get_edit(
                    message,
                    timeout=min(stream_idle, remaining),
                )
            )
            edit_tasks[task] = key

        def add_response_listener():
            nonlocal response_task
            remaining = deadline - loop.time()

            if remaining <= 0 or response_task is not None:
                return

            response_task = asyncio.create_task(
                conversation.get_response(timeout=remaining)
            )

        add_response_listener()

        try:
            while True:
                now = loop.time()

                if (
                    answers
                    and last_activity is not None
                    and now - last_activity >= stream_idle
                ):
                    break

                remaining = deadline - now

                if remaining <= 0:
                    break

                tasks = set(edit_tasks)

                if response_task is not None:
                    tasks.add(response_task)

                if not tasks:
                    break

                wait_timeout = remaining

                if answers and last_activity is not None:
                    wait_timeout = min(
                        wait_timeout,
                        max(0.05, stream_idle - (now - last_activity)),
                    )

                done, _ = await asyncio.wait(
                    tasks,
                    timeout=wait_timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if not done:
                    break

                for task in done:
                    if task is response_task:
                        response_task = None

                        try:
                            incoming = task.result()
                        except asyncio.TimeoutError:
                            continue

                        self._remember_id(cleanup_ids, incoming)
                        key = getattr(incoming, "id", None)

                        if key is None:
                            sequence += 1
                            key = f"response-{sequence}"

                        signatures[key] = self._message_signature(incoming)

                        if not self._is_progress_message(incoming):
                            if key not in answers:
                                answer_order.append(key)

                            answers[key] = incoming
                            last_activity = loop.time()

                        add_edit_listener(incoming, key)
                        add_response_listener()
                        continue

                    key = edit_tasks.pop(task, None)

                    try:
                        edited = task.result()
                    except asyncio.TimeoutError:
                        continue

                    if edited is None:
                        continue

                    self._remember_id(cleanup_ids, edited)
                    signature = self._message_signature(edited)

                    if signature == signatures.get(key):
                        continue

                    signatures[key] = signature

                    if not self._is_progress_message(edited):
                        if key not in answers:
                            answer_order.append(key)

                        answers[key] = edited
                        last_activity = loop.time()

                    add_edit_listener(edited, key)

            if not answers:
                if require_answer:
                    raise asyncio.TimeoutError

                return None

            ordered = [answers[key] for key in answer_order]

            if len(ordered) == 1:
                return ordered[0]

            if any(self._media_label(item) for item in ordered):
                return ordered[-1]

            text = "\n\n".join(
                self._message_text(item) for item in ordered if item is not None
            ).strip()

            return text or ordered[-1]
        finally:
            pending = set(edit_tasks)

            if response_task is not None:
                pending.add(response_task)

            for task in pending:
                task.cancel()

            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _has_persona(self, bot):
        async for item in self._client.iter_messages(bot, limit=100):
            if not getattr(item, "out", False):
                continue

            if self.PERSONA_MARKER in self._message_text(item):
                return True

        return False

    async def _ensure_persona(
        self,
        conversation,
        bot,
        cleanup_ids,
        timeout,
    ):
        """Один раз задаёт боту роль Джарвиса и не удаляет этот prompt."""

        if await self._has_persona(bot):
            return

        has_history = False

        with contextlib.suppress(Exception):
            has_history = bool(await self._client.get_messages(bot, limit=1))

        if not has_history:
            started = await conversation.send_message(
                "/start",
                parse_mode=None,
            )
            self._remember_id(cleanup_ids, started)

            with contextlib.suppress(asyncio.TimeoutError):
                await self._wait_response(
                    conversation,
                    min(timeout, 20),
                    cleanup_ids,
                    require_answer=False,
                )

        await conversation.send_message(
            self.PERSONA_PROMPT,
            parse_mode=None,
        )

        with contextlib.suppress(asyncio.TimeoutError):
            await self._wait_response(
                conversation,
                min(timeout, 30),
                cleanup_ids,
                require_answer=False,
            )

    async def _cleanup_bot_messages(self, bot, message_ids):
        ids = sorted(
            {
                int(message_id)
                for message_id in message_ids
                if message_id is not None
                and str(message_id).lstrip("-").isdigit()
            }
        )

        if not ids:
            return

        try:
            await self._client.delete_messages(
                bot,
                ids,
                revoke=True,
            )
        except Exception:
            for message_id in ids:
                with contextlib.suppress(Exception):
                    await self._client.delete_messages(
                        bot,
                        [message_id],
                        revoke=True,
                    )

    async def _ask_bot(self, prompt, media=None):
        bot = await self._get_bot()
        timeout = self._get_int("response_timeout", 300, 30, 600)
        cleanup_ids = set()
        answer = None

        async with self._lock:
            try:
                async with self._client.conversation(
                    bot,
                    timeout=timeout,
                    exclusive=True,
                ) as conversation:
                    await self._ensure_persona(
                        conversation,
                        bot,
                        cleanup_ids,
                        timeout,
                    )

                    if media is not None:
                        sent = await conversation.send_file(
                            media,
                            caption=prompt,
                            parse_mode=None,
                        )
                    else:
                        sent = await conversation.send_message(
                            prompt,
                            parse_mode=None,
                        )

                    self._remember_id(cleanup_ids, sent)
                    answer = await self._wait_response(
                        conversation,
                        timeout,
                        cleanup_ids,
                    )

                    if not isinstance(answer, str):
                        answer_text = self._message_text(answer)

                        if answer_text:
                            answer = answer_text
            finally:
                self._queue_cleanup(bot, cleanup_ids)

        return answer

    def _prepare_request(self, text, reply):
        prompt = str(text or "").strip()
        reply_text = ""

        if reply is not None:
            reply_text = str(getattr(reply, "raw_text", "") or "").strip()

        media = self._media(reply)

        if reply is not None:
            if not prompt:
                if media is not None:
                    prompt = "Проанализируй вложение и помоги по его содержимому."
                else:
                    prompt = (
                        "Помоги разобраться с сообщением ниже "
                        "и предложи полезный ответ."
                    )

            if reply_text:
                prompt += f"\n\nСообщение из чата:\n{reply_text}"

        if not prompt:
            return "", media

        return self._build_prompt(prompt), media

    async def _sender_name(self, message):
        sender = getattr(message, "sender", None)

        if sender is None:
            with contextlib.suppress(Exception):
                sender = await message.get_sender()

        if sender is not None:
            name = " ".join(
                filter(
                    None,
                    [
                        getattr(sender, "first_name", None),
                        getattr(sender, "last_name", None),
                        getattr(sender, "title", None),
                    ],
                )
            ).strip()

            if name:
                return name

            username = getattr(sender, "username", None)

            if username:
                return f"@{username}"

        return str(getattr(message, "sender_id", "unknown"))

    async def _collect_history(self, message, limit):
        peer = getattr(message, "peer_id", None)

        if peer is None:
            peer = getattr(message, "chat_id", None) or "me"

        rows = []
        prefix = str(self.get_prefix() or "")
        fetch_limit = min(max(limit * 3, limit + 5), 100)

        async for item in self._client.iter_messages(
            peer,
            limit=fetch_limit,
        ):
            if getattr(item, "id", None) == getattr(message, "id", None):
                continue

            text = str(getattr(item, "raw_text", "") or "").strip()

            if (
                getattr(item, "out", False)
                and prefix
                and text.startswith(prefix)
            ):
                continue

            media_label = self._media_label(item)

            if not text and not media_label:
                continue

            sender = await self._sender_name(item)
            date = getattr(item, "date", None)
            time_text = date.strftime("%H:%M") if date else "--:--"

            line = f"[{time_text}] {sender}: "

            if text:
                line += self._clip(text, 600)

            if media_label:
                line += f" {media_label}"

            rows.append(line)

            if len(rows) >= limit:
                break

        rows.reverse()

        return "\n".join(rows) or "(читаемых сообщений нет)"

    async def _run_request(self, message, prompt, media=None):
        status = await self._work_message(message)
        status = status or message
        await self._set_message(status, self.strings("thinking"))
        heartbeat = asyncio.create_task(self._heartbeat(status))

        try:
            answer = await self._ask_bot(prompt, media)
        except asyncio.TimeoutError:
            await self._set_message(
                status,
                self.strings("timeout"),
            )
            return
        except Exception as error:
            error_text = self._clip(
                f"{type(error).__name__}: {error}",
                300,
            )

            await self._set_message(
                status,
                self.strings("error").format(error=error_text),
            )
            return
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

        if isinstance(answer, str):
            answer_text = answer.strip()
        else:
            answer_text = self._message_text(answer)

        if not answer_text and not getattr(answer, "media", None):
            await self._set_message(
                status,
                self.strings("empty"),
            )
            return

        if answer_text:
            await self._set_message(status, answer_text)
        else:
            with contextlib.suppress(Exception):
                await utils.answer(status, answer)

    @loader.command()
    async def ask(self, message):
        """Задать вопрос Джарвису. Можно использовать ответ на текст или медиа."""

        raw = str(utils.get_args_raw(message) or "").strip()
        reply = None

        if getattr(message, "is_reply", False):
            with contextlib.suppress(Exception):
                reply = await message.get_reply_message()

        prompt, media = self._prepare_request(raw, reply)

        if media is None:
            current_media = self._media(message)

            if current_media is not None:
                media = current_media

                if not prompt:
                    prompt = self._build_prompt(
                        "Проанализируй прикреплённое вложение."
                    )

        if not prompt:
            await utils.answer(
                message,
                self.strings("usage").format(
                    prefix=self.get_prefix(),
                ),
            )
            return

        await self._run_request(message, prompt, media)

    @loader.command()
    async def askchat(self, message):
        """Передать Джарвису последние сообщения текущего чата."""

        raw = str(utils.get_args_raw(message) or "").strip()
        parts = raw.split(maxsplit=1)

        limit = self._get_int(
            "history_limit",
            20,
            1,
            50,
        )

        if parts and parts[0].isdigit():
            limit = min(max(int(parts[0]), 1), 50)
            task = parts[1] if len(parts) > 1 else ""
        else:
            task = raw

        if not task:
            task = (
                "Кратко объясни, что обсуждают участники, "
                "и выдели главное."
            )

        try:
            history = await self._collect_history(message, limit)
        except Exception as error:
            await utils.answer(
                message,
                self.strings("error").format(
                    error=self._clip(str(error), 300),
                ),
            )
            return

        prompt = self._build_prompt(
            "Ниже приведена история текущего чата. "
            "Используй её как контекст.\n\n"
            f"{history}\n\n"
            f"Запрос пользователя:\n{task}"
        )

        await self._run_request(message, prompt)

    @loader.command()
    async def jarvisstart(self, message):
        """Запустить Джарвиса и установить его личность в ЛС."""

        status = await self._work_message(message)
        status = status or message
        await self._set_message(status, self.strings("starting"))
        heartbeat = asyncio.create_task(self._heartbeat(status))
        timeout = min(
            self._get_int("response_timeout", 300, 30, 600),
            60,
        )
        cleanup_ids = set()

        try:
            bot = await self._get_bot()

            async with self._lock:
                try:
                    async with self._client.conversation(
                        bot,
                        timeout=timeout,
                        exclusive=True,
                    ) as conversation:
                        await self._ensure_persona(
                            conversation,
                            bot,
                            cleanup_ids,
                            timeout,
                        )
                finally:
                    self._queue_cleanup(bot, cleanup_ids)

            await self._set_message(status, self.strings("ready"))
        except Exception as error:
            await self._set_message(
                status,
                self.strings("error").format(
                    error=self._clip(str(error), 300),
                ),
            )
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    def _chat_key(self, message):
        chat_id = getattr(message, "chat_id", None)

        if chat_id is None:
            chat_id = getattr(self, "_tg_id", "me")

        return str(chat_id)

    def _auto_chats(self):
        chats = self.get("auto_chats", [])

        if not isinstance(chats, (list, tuple, set)):
            return set()

        return {str(chat) for chat in chats}

    @loader.command()
    async def jarvison(self, message):
        """Включить ответы на упоминания в текущем чате."""

        chats = self._auto_chats()
        chats.add(self._chat_key(message))
        self.set("auto_chats", sorted(chats))

        await utils.answer(
            message,
            self.strings("auto_on"),
        )

    @loader.command()
    async def jarvisoff(self, message):
        """Выключить ответы на упоминания в текущем чате."""

        chats = self._auto_chats()
        chats.discard(self._chat_key(message))
        self.set("auto_chats", sorted(chats))

        await utils.answer(
            message,
            self.strings("auto_off"),
        )

    @loader.command()
    async def jarvisstatus(self, message):
        """Показать состояние Джарвиса."""

        state = (
            "включён"
            if self._chat_key(message) in self._auto_chats()
            else "выключен"
        )

        await utils.answer(
            message,
            self.strings("status").format(
                state=state,
                timeout=self._get_int(
                    "response_timeout",
                    300,
                    30,
                    600,
                ),
                stream_idle=self._get_float(
                    "stream_idle_seconds",
                    10.0,
                    2.0,
                    30.0,
                ),
                history=self._get_int("history_limit", 20, 1, 50),
            ),
        )

    @loader.watcher()
    async def watcher(self, message):
        """Отвечать на упоминания в включённых чатах."""

        if getattr(message, "out", False):
            return

        if not getattr(message, "is_group", False):
            return

        if self._chat_key(message) not in self._auto_chats():
            return

        reply = None

        if getattr(message, "is_reply", False):
            with contextlib.suppress(Exception):
                reply = await message.get_reply_message()

        text = str(getattr(message, "raw_text", "") or "").strip()

        replied_to_me = bool(
            reply is not None
            and getattr(reply, "sender_id", None)
            == getattr(self, "_tg_id", None)
        )

        mentioned = bool(getattr(message, "mentioned", False))

        if not mentioned and not replied_to_me:
            return

        if self._username:
            text = re.sub(
                rf"@{re.escape(self._username)}\b",
                "",
                text,
                flags=re.IGNORECASE,
            ).strip()
        else:
            text = re.sub(r"@\w+", "", text, count=1).strip()

        prompt, media = self._prepare_request(text, reply)

        if media is None:
            current_media = self._media(message)

            if current_media is not None:
                media = current_media

                if not prompt:
                    prompt = self._build_prompt(
                        "Проанализируй прикреплённое вложение."
                    )

        if not prompt:
            return

        await self._run_request(message, prompt, media)
