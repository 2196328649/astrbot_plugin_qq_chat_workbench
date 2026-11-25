"""QQ conversation console with task-local tracing and reversible hooks."""

import asyncio
import base64
import io
import json
import sys
import time
import uuid
from pathlib import Path

from astrbot.api import logger
from astrbot.api.event import filter
from astrbot.api.star import Context, Star
from astrbot.api.web import error_response, json_response, request
from astrbot.core.pipeline.scheduler import PipelineScheduler
from astrbot.core.utils.astrbot_path import get_astrbot_data_path
from loguru import logger as loguru_logger
from PIL import Image
from quart import websocket

from .guidance import GuidanceMixin, ToolLoopAgentRunner, observed_api
from .media import MediaMixin
from .ordered_worker import OrderedWorker
from .store import Store, segments, trace_context

NAME = "astrbot_plugin_super_admin"
VERSION = "0.2.11"
# Console display preferences and their allowed values; the first is the default.
# The page runs in a sandboxed iframe without browser storage, so they live in
# console.db and are shared by every administrator.
SETTINGS = {
    "auto_images": (False, True),
    "avatars": (False, True),
    "send_key": ("ctrl_enter", "enter"),
    "reply_mode": ("confirm", "auto"),
    "message_actions": ("always", "hover"),
    # manual: only on request; viewed: images the page displayed; all: every
    # incoming image as it arrives, so a recalled one stays viewable.
    "image_save": ("manual", "viewed", "all"),
    # Megabytes of saved images; the oldest copies go first.
    "image_quota": (1024, 512, 2048, 4096),
}
SEND_ACTIONS = {
    "send_msg",
    "send_group_msg",
    "send_private_msg",
    "send_group_forward_msg",
    "send_private_forward_msg",
}
# Reload waits this long for plugin-owned work, then cancels what remains.
OWNER_DRAIN_SECONDS = 30
OWNER_CANCEL_SECONDS = 5
# Shared infrastructure tasks that merely inherit the trace of whichever
# pipeline created them. They are neither job work nor stop targets.
HOUSEKEEPING = (
    ("astrbot.core.utils.metrics", "Metric", "upload"),
    ("astrbot.core.utils.metrics", "Metric", "_flush_periodically"),
    ("curl_cffi.aio", "AsyncCurl", "_force_timeout"),
)


def housekeeping_codes():
    """Code objects of known housekeeping coroutines in already loaded modules."""
    codes = set()
    for module, owner, name in HOUSEKEEPING:
        method = getattr(getattr(sys.modules.get(module), owner, None), name, None)
        code = getattr(method, "__code__", None)
        if code is not None:
            codes.add(code)
    return codes


def task_trace(task):
    get_context = getattr(task, "get_context", None)
    return get_context().get(trace_context) if get_context else ""


def traced_tasks():
    """Yield (trace, task) for other live tasks that carry job work."""
    current = asyncio.current_task()
    excluded = housekeeping_codes()
    for task in asyncio.all_tasks():
        trace = task_trace(task)
        if (
            task is not current
            and trace
            and getattr(task.get_coro(), "cr_code", None) not in excluded
        ):
            yield trace, task


def validate_upload(image):
    """Validate manual uploads on the media lane; keep existing format/size limits."""
    if not isinstance(image, str) or len(image) > 2800000:
        raise ValueError()
    payload = base64.b64decode(image, validate=True)
    with Image.open(io.BytesIO(payload)) as img:
        if (
            img.format not in {"PNG", "JPEG", "GIF", "WEBP"}
            or img.width * img.height > 25000000
        ):
            raise ValueError()
        img.verify()


def chat_previews(rows):
    labels = {
        "image": "图片",
        "record": "语音",
        "audio": "语音",
        "video": "视频",
        "file": "文件",
        "reply": "引用",
        "at": "@",
        "face": "表情",
        "forward": "合并转发",
        "node": "合并转发",
    }
    for row in rows:
        body = json.loads(row.pop("last_body") or "[]")
        row["preview"] = " ".join(
            segment.get("data", {}).get("text", "")
            if segment.get("type") == "text"
            else "["
            + labels.get(segment.get("type"), segment.get("type", "附件"))
            + "]"
            for segment in body
        )[:240]
        if row["last_status"] == "recalled":
            row["preview"] = "[已撤回] " + row["preview"]
    return rows


def valid_setting(key, value):
    # Compare types too: True == 1 must not pass for a string or a number.
    return any(
        type(value) is type(allowed) and value == allowed
        for allowed in SETTINGS.get(key, ())
    )


def console_settings(rows):
    """Defaults overlaid with saved values; unknown or stale entries are ignored."""
    result = {key: allowed[0] for key, allowed in SETTINGS.items()}
    for row in rows:
        try:
            value = json.loads(row["value"])
        except (TypeError, ValueError):
            continue
        if valid_setting(row["key"], value):
            result[row["key"]] = value
    return result


class SuperAdmin(GuidanceMixin, MediaMixin, Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self.active = False
        self.hooks = []
        self.clients = {}
        self.dropped = 0
        self.queue = []
        self.send_lock = asyncio.Lock()
        self.tasks = {}
        self.cancelled = set()
        self.name_lookups = {}
        self.name_retry = {}
        self.name_limit = asyncio.Semaphore(2)
        self._ensure_workers()

    def _ensure_workers(self):
        # Lazy construction also supports isolated fixtures without Star.__init__.
        if not hasattr(self, "db_worker"):
            self.db_worker = OrderedWorker("super-admin-db", max_pending=32)
            self.media_worker = OrderedWorker("super-admin-media", max_pending=4)
            # Disk work for saved images; never shares a lane with ingestion.
            self.file_worker = OrderedWorker("super-admin-files", max_pending=8)
            self.stop_revisions = {}
            self.stopping_traces = set()
            self.preserve_pending_traces = set()
            self.review_waiter_traces = {}
            self.send_gates = {}
            self.send_owners = {}
            self.send_tails = {}
            self.hook_tasks = {}
            self.api_tasks = {}
            self.appending = {}
            self.terminating = False
            self.prune_progress = None

    async def db_call(self, function, *args, **kwargs):
        self._ensure_workers()
        return await self.db_worker.run(function, *args, **kwargs)

    async def media_call(self, function, *args, **kwargs):
        self._ensure_workers()
        return await self.media_worker.run(function, *args, **kwargs)

    async def record_message(
        self,
        chat,
        trace,
        direction,
        sender,
        body,
        status,
        remote_id="",
        *,
        created=None,
        sender_id="",
    ):
        prepared = await self.media_call(segments, body)
        return await self.db_call(
            self.store.message,
            chat,
            trace,
            direction,
            sender,
            prepared,
            status,
            remote_id,
            created=created,
            sender_id=sender_id,
            prepared=True,
        )

    async def close_workers(self):
        self._ensure_workers()
        await self.media_worker.close()
        await self.file_worker.close()
        await self.db_worker.close(self.store.close if hasattr(self, "store") else None)

    def console_setting(self, key):
        """Loop-side copy of a console setting; the default until it is loaded."""
        return getattr(self, "console_values", {}).get(key, SETTINGS[key][0])

    def enter_hook(self, registry=None):
        self._ensure_workers()
        registry = self.hook_tasks if registry is None else registry
        task = asyncio.current_task()
        registry[task] = registry.get(task, 0) + 1
        return task

    def leave_hook(self, task, registry=None):
        registry = self.hook_tasks if registry is None else registry
        count = registry.get(task, 1) - 1
        if count:
            registry[task] = count
        else:
            registry.pop(task, None)

    def mark_guided(self, trace):
        """Route a trace's sends through review from now on (grow-only).

        Call it before persisting a reply_mode. guide() instead holds
        appending_to() across its commit and marks only a committed append.
        """
        self.store.guided_traces.add(trace)

    def is_guided(self, trace):
        # An append still waiting for guidance_lock counts too, so the task's
        # sends queue behind it; a refused append leaves no permanent mark.
        guided = self.store.guided_traces if hasattr(self, "store") else ()
        return trace in guided or trace in self.appending

    def blocked(self, trace):
        """Stopped traces, and guided ones during reload, must not continue."""
        return trace in self.cancelled or (self.terminating and self.is_guided(trace))

    def guarded(self, trace):
        """Whether a late send of this trace must not reach the raw sender."""
        return trace in self.cancelled or self.is_guided(trace)

    async def initialize(self):
        self._ensure_workers()
        directory = Path(get_astrbot_data_path()) / "plugin_data" / NAME

        def open_store():
            directory.mkdir(parents=True, exist_ok=True)
            self.store = Store(directory / "console.db")
            self.store.repair_group_notices(directory)
            self.store.repair_background_messages(directory)
            self.store.execute(
                "UPDATE jobs SET status='interrupted' WHERE status IN ('running','stopping')"
            )

        try:
            await self.db_call(open_store)
            self.console_values = console_settings(
                await self.db_call(self.store.rows, "SELECT key,value FROM settings")
            )
        except BaseException:
            await self.close_workers()
            raise
        self.active = True
        self.initialize_guidance()
        self.loop = asyncio.get_running_loop()
        self.sink_id = loguru_logger.add(
            self.capture_log, level="DEBUG", enqueue=False, catch=True
        )
        self.original_execute = PipelineScheduler.execute
        original = self.original_execute

        async def traced(scheduler, event, *args, **kwargs):
            if not self.active or self.terminating:
                return await original(scheduler, event, *args, **kwargs)
            trace = uuid.uuid4().hex
            token = trace_context.set(trace)
            owner = self.enter_hook()
            # Register before the first observer await: stop never sees a job without
            # its owning task/event, even while thumbnail/database work is queued.
            self.tasks[trace] = owner
            self.live_events[trace] = event
            # A background completion inherits its parent's reply mode. Until the
            # parent lookup succeeds that mode is unknown, so sends stay reviewed.
            unresolved = False
            try:
                try:
                    get_extra = getattr(event, "get_extra", None)
                    unresolved = bool(
                        get_extra and get_extra("_reference_image_completion", False)
                    )
                    received = await self.receive(event, trace)
                    if received:
                        chat, source_message_id, prompt = received
                        await self.db_call(
                            self.store.execute,
                            "INSERT INTO jobs(trace,chat,umo,prompt,status,started,source_message_id,sender_id) VALUES(?,?,?,?,?,?,?,?)",
                            (
                                trace,
                                chat,
                                event.unified_msg_origin,
                                prompt,
                                "running",
                                time.time(),
                                source_message_id,
                                event.get_sender_id(),
                            ),
                        )
                        if unresolved:
                            parents = await self.db_call(
                                self.store.rows,
                                "SELECT trace,reply_mode FROM jobs WHERE chat=? AND source_message_id=? AND reply_mode!='' AND trace!=? ORDER BY started DESC LIMIT 1",
                                (chat, source_message_id, trace),
                            )
                            if not parents and source_message_id is None:
                                # Guidance without a source message stores no
                                # message id, so its completion is matched by chat:
                                # a recent one that asks for review decides, else the
                                # newest, so a mix of tasks is never sent unreviewed.
                                parents = await self.db_call(
                                    self.store.rows,
                                    "SELECT trace,reply_mode FROM jobs WHERE chat=? AND source_message_id IS NULL AND reply_mode!='' AND trace!=? AND started>? ORDER BY reply_mode='confirm' DESC,started DESC LIMIT 1",
                                    (chat, trace, time.time() - 6 * 3600),
                                )
                            if parents:
                                parent = parents[0]
                                self.mark_guided(trace)
                                event.set_extra("_super_admin_guided_trace", trace)
                                event.set_extra("enable_streaming", False)
                                await self.db_call(
                                    self.store.execute,
                                    "UPDATE jobs SET reply_mode=?,parent_trace=? WHERE trace=?",
                                    (parent["reply_mode"], parent["trace"], trace),
                                )
                                for instruction in await self.db_call(
                                    self.store.rows,
                                    "SELECT * FROM guidance_instructions WHERE trace=?",
                                    (parent["trace"],),
                                ):
                                    await self.db_call(
                                        self.store.execute,
                                        "INSERT INTO guidance_instructions VALUES(?,?,?,?,?,?,?)",
                                        (
                                            uuid.uuid4().hex,
                                            "",
                                            trace,
                                            instruction["reference_kind"],
                                            instruction["message_id"],
                                            instruction["guidance"],
                                            instruction["created"],
                                        ),
                                    )
                                if parent["trace"] in self.cancelled:
                                    self.cancelled.add(trace)
                    unresolved = False
                except Exception:  # noqa: BLE001 - Observer errors never block normal delivery.
                    self.dropped += 1
                    if unresolved:
                        self.mark_guided(trace)
                # Reload stops only reviewed work; ordinary messages keep going.
                if self.blocked(trace):
                    raise asyncio.CancelledError("Task stopped before pipeline start")
                self.safe_log(trace, "INFO", "pipeline", "Message processing started")
                return await original(scheduler, event, *args, **kwargs)
            except BaseException as exc:
                self.safe_log(
                    trace,
                    "INFO" if isinstance(exc, asyncio.CancelledError) else "ERROR",
                    "pipeline",
                    "Task cancelled"
                    if isinstance(exc, asyncio.CancelledError)
                    else f"{type(exc).__name__}: {exc}",
                )
                await self.observe_update(
                    "UPDATE jobs SET status=? WHERE trace=?",
                    (
                        "cancelled"
                        if isinstance(exc, asyncio.CancelledError)
                        else "failed",
                        trace,
                    ),
                )
                raise
            finally:
                try:
                    await self.observe_update(
                        "UPDATE jobs SET status=CASE WHEN status='running' THEN 'completed' ELSE status END, finished=? WHERE trace=?",
                        (time.time(), trace),
                    )
                    self.safe_log(
                        trace, "INFO", "pipeline", "Message processing finished"
                    )
                finally:
                    if self.tasks.get(trace) is owner:
                        self.tasks.pop(trace, None)
                    self.live_events.pop(trace, None)
                    self.guidance_states.pop(trace, None)
                    self.leave_hook(owner)
                    trace_context.reset(token)

        self.traced_execute = traced
        PipelineScheduler.execute = traced
        await self.platforms_loaded()
        for path, handler, methods in [
            ("state", self.state, ["GET"]),
            ("chats", self.chats, ["GET"]),
            ("messages", self.messages, ["GET"]),
            ("logs", self.logs, ["GET"]),
            ("send", self.send, ["POST"]),
            ("jobs", self.jobs, ["GET"]),
            ("stop", self.stop, ["POST"]),
            ("guide", self.guide, ["POST"]),
            ("review", self.review, ["GET"]),
            ("approve", self.approve, ["POST"]),
            ("discard", self.discard, ["POST"]),
            ("recall", self.recall, ["POST"]),
            ("settings", self.settings, ["GET", "POST"]),
            ("media", self.media, ["GET"]),
            ("media_save", self.media_save, ["POST"]),
            ("media_file", self.media_file, ["GET"]),
        ]:
            self.context.register_web_api(
                f"/{NAME}/{path}", handler, methods, "QQ 聊天工作台"
            )
        await self.start_media(directory)
        self.worker = asyncio.create_task(self.flush_logs())

    def safe_log(self, trace, level, source, body):
        if not self.active:
            return
        if len(self.queue) >= 5000:
            self.dropped += 1
            return
        self.queue.append((trace, level, source, str(body)[:24000]))

    async def observe_update(self, sql, args):
        """Persist observer state without changing the wrapped operation's result.

        Args:
            sql: Parameterized observer update.
            args: Values bound to the update.
        """
        try:
            (await self.db_call(self.store.execute, sql, args))
        except Exception:  # noqa: BLE001 - Recording must never break the observed task.
            self.dropped += 1

    def capture_log(self, message):
        record = message.record
        # AstrBot forwards standard logging records through its own handler, so
        # name/line point at that forwarder; the caller is kept in extra.
        extra = record["extra"]
        if extra.get("source_line") is not None:
            source = f"{extra.get('source_file')}:{extra['source_line']}"
        else:
            source = f"{record['name']}:{record['line']}"
        if extra.get("plugin_name"):
            source = f"[{extra['plugin_name']}] {source}"
        args = (
            trace_context.get(),
            record["level"].name,
            source,
            str(message)[:24000],
        )
        self.loop.call_soon_threadsafe(self.safe_log, *args)

    async def set_step(self, step, detail="", *, trace=None, expected=None):
        """Record an observed phase atomically on the DB lane, never on the loop."""
        trace = trace or trace_context.get()
        if not self.active or not trace or trace in self.cancelled:
            return None
        try:
            detail = str(detail or "")[:160]
            result = await self.db_call(
                self.store.record_step, trace, step, detail, expected
            )
            if result:
                self.safe_log(
                    trace,
                    "INFO",
                    "progress",
                    f"Processing step: {step}" + (f" ({detail})" if detail else ""),
                )
            return result
        except Exception:  # noqa: BLE001 - Observation must not break a reply.
            self.dropped += 1
            return None

    async def flush_logs(self):
        last_prune = time.monotonic()
        pruning = True  # Startup retention is cooperative too; never a full scan/lock.
        orphaned = False  # Saved images of pruned messages, one page per round.
        while self.active:
            await asyncio.sleep(0.5)
            batch, self.queue = self.queue[:128], self.queue[128:]
            try:
                if batch:
                    await self.db_call(self.write_logs, batch)
                if pruning or time.monotonic() - last_prune > 60:
                    self.prune_progress = await self.db_call(self.store.prune, 128)
                    pruning = self.prune_progress["more"]
                    if not pruning:
                        last_prune = time.monotonic()
                        orphaned = True
                elif orphaned:
                    orphaned = await self.prune_media()
            except Exception:  # noqa: BLE001 - Observer failures must not block delivery.
                self.dropped += len(batch)

    def write_logs(self, batch):
        """DB-worker-only bounded write; redact before taking the connection lock."""
        from .store import redact

        values = [
            (trace, level, source, redact(body), time.time())
            for trace, level, source, body in batch
        ]
        with self.store.lock, self.store.db:
            self.store.db.executemany(
                "INSERT INTO logs(trace,level,source,body,created) VALUES(?,?,?,?,?)",
                values,
            )

    async def receive(self, event, trace):
        if event.get_platform_name() != "aiocqhttp":
            return
        raw = event.message_obj.raw_message
        if not hasattr(raw, "get"):
            return
        if raw.get("post_type") == "notice" and raw.get("notice_type") in (
            "group_recall",
            "friend_recall",
        ):
            # The message stays readable; only its status says it was recalled.
            await self.db_call(
                self.store.mark_recalled,
                event.get_platform_id(),
                str(raw.get("self_id", event.message_obj.self_id)),
                str(raw.get("message_id", "")),
                str(raw.get("group_id") or "")
                if raw.get("notice_type") == "group_recall"
                else "",
                str(raw.get("user_id") or ""),
            )
            return
        # Notices/requests have group_id too, but are not private messages.
        if raw.get("post_type") not in (None, "message") or raw.get(
            "message_type"
        ) not in ("group", "private"):
            return
        platform = event.get_platform_id()
        account = str(raw.get("self_id", event.message_obj.self_id))
        kind = "group" if raw.get("message_type") == "group" else "private"
        group = str(raw.get("group_id") or "")
        if kind == "private" and (raw.get("sub_type") == "group" or group):
            kind = "temporary"
        target = str(
            raw.get("group_id")
            if kind == "group"
            else raw.get("user_id", event.get_sender_id())
        )
        sender_id = str(event.get_sender_id())
        sender_info = raw.get("sender") or {}
        if not isinstance(sender_info, dict):
            sender_info = {}
        sender_name = next(
            (
                str(value).strip()
                for value in (
                    sender_info.get("card"),
                    sender_info.get("nickname"),
                    sender_info.get("nick"),
                    event.get_sender_name(),
                )
                if value
                and str(value).strip() not in (sender_id, "N/A", "None", "unknown")
            ),
            "",
        )
        title = (
            (getattr(event.message_obj.group, "group_name", "") or target)
            if kind == "group"
            else sender_name or target
        )
        chat = await self.db_call(
            self.store.chat,
            platform,
            account,
            kind,
            target,
            group if kind == "temporary" else "",
            title,
        )
        if not sender_name and kind != "group":
            saved = (
                await self.db_call(
                    self.store.rows, "SELECT title FROM chats WHERE id=?", (chat,)
                )
            )[0]["title"]
            if saved and saved not in (target, "N/A", "None", "unknown"):
                sender_name = saved
        body = raw.get("message") or raw.get("raw_message") or event.get_message_str()
        remote_id = str(raw.get("message_id", ""))
        get_extra = getattr(event, "get_extra", None)
        completion = bool(get_extra and get_extra("_reference_image_completion", False))
        if completion or not body:
            original = await self.db_call(
                self.store.rows,
                "SELECT id,search FROM messages WHERE chat=? AND remote_id=? AND remote_id NOT IN ('','0') AND direction='in' AND TRIM(search)!='' ORDER BY id LIMIT 1",
                (chat, remote_id),
            )
            if original:
                self.safe_log(
                    trace,
                    "INFO",
                    "pipeline",
                    f"Background event linked to source message {original[0]['id']}",
                )
                return chat, original[0]["id"], original[0]["search"]
            if completion:
                facts = get_extra("_reference_image_completion_facts", {})
                prompt = (
                    facts.get("original_request", "") if isinstance(facts, dict) else ""
                )
                return chat, None, str(prompt or "[后台续答，原消息不在已采集记录中]")
        message_id = await self.record_message(
            chat,
            trace,
            "in",
            sender_name or sender_id,
            body,
            "received",
            raw.get("message_id", ""),
            created=raw.get("time", getattr(event.message_obj, "timestamp", None)),
            sender_id=sender_id,
        )
        self.queue_images(message_id, body)
        return chat, message_id, event.get_message_str()

    @filter.on_platform_loaded()
    async def platforms_loaded(self):
        if not self.active:
            return
        for platform in self.context.platform_manager.platform_insts:
            if platform.meta().name != "aiocqhttp":
                continue
            bot = platform.get_client()
            pid = platform.meta().id
            self.clients[pid] = bot
            if any(item[0] is bot for item in self.hooks):
                continue
            original = bot.call_action
            wrapped = self.wrap_sender(original, pid, bot)
            bot.call_action = wrapped
            self.hooks.append((bot, original, wrapped))

    @staticmethod
    def connected_account(client):
        """Mirror aiocqhttp's implicit WebSocket selection without doing I/O."""
        connections = getattr(client, "_wsr_api_clients", None)
        if not isinstance(connections, dict):
            return ""
        if websocket and websocket in getattr(client, "_wsr_event_clients", ()):
            account = websocket.headers.get("X-Self-ID", "")
            if account not in connections:
                return ""
        elif len(connections) == 1:
            account = next(iter(connections))
        else:
            return ""
        return account if isinstance(account, str) and account.isdecimal() else ""

    async def record_outgoing(
        self, platform, account, action, params, trace, status, remote_id=""
    ):
        is_group = action.startswith("send_group") or (
            action == "send_msg"
            and (
                params.get("message_type") == "group"
                or (params.get("group_id") and not params.get("user_id"))
            )
        )
        kind = (
            "group"
            if is_group
            else "temporary"
            if params.get("group_id")
            else "private"
        )
        target = str(params.get("group_id") if is_group else params.get("user_id", ""))
        chat = await self.db_call(
            self.store.chat,
            platform,
            account,
            kind,
            target,
            str(params.get("group_id", "")) if kind == "temporary" else "",
        )
        return await self.record_message(
            chat,
            trace,
            "out",
            "机器人",
            params.get("message", "[合并转发]"),
            status,
            remote_id,
        )

    def wrap_sender(self, original, platform, client=None):
        async def wrapped(action, **params):
            if action not in SEND_ACTIONS:
                return await original(action, **params)
            trace = trace_context.get() or uuid.uuid4().hex
            # A stale hook must still reject an already stopped owner after reload,
            # and a guided reply must never bypass review while the plugin unloads.
            if self.blocked(trace):
                self.safe_log(
                    trace, "WARNING", action, "Blocked send from a cancelled task"
                )
                raise asyncio.CancelledError("Task stopped by administrator")
            if not self.active:
                return await original(action, **params)
            token = trace_context.set(trace)
            owner = self.enter_hook()
            mid = pending_id = None
            attempted = accepted = False
            outcome = None
            implicit = False
            wire_account = ""
            result = None
            claimed = []
            try:
                pending_id = await self.hold_guided_send(
                    trace, platform, action, params
                )
                if pending_id is not None:
                    # Use the exact reviewed snapshot, not a mutable caller-owned list.
                    params = await self.approved_payload(
                        trace, pending_id, platform, action
                    )
                try:
                    account = str(params.get("self_id") or "")
                    implicit = not account
                    if account.isdecimal():
                        mid = await self.record_outgoing(
                            platform, account, action, params, trace, "sending"
                        )
                except Exception:  # noqa: BLE001 - Audit errors must not change a normal send.
                    self.dropped += 1
                self.safe_log(trace, "INFO", action, "Send requested")
                sending = await self.set_step("sending", trace=trace)
                if pending_id is None:
                    # An administrator can switch auto -> confirm while audit/media
                    # work yields. Recheck the mode after those observer awaits.
                    pending_id = await self.hold_guided_send(
                        trace, platform, action, params
                    )
                    if pending_id is not None:
                        params = await self.approved_payload(
                            trace, pending_id, platform, action
                        )
                if self.blocked(trace):
                    raise asyncio.CancelledError("Task stopped before send")
                if pending_id is not None:

                    def claim():
                        result = self.store.claim_pending_send(pending_id, trace)
                        claimed.append(result)
                        return result

                    if not await self.db_call(claim):
                        raise RuntimeError(
                            "Approved reply was already claimed or cancelled"
                        )
                # No observer await between this final stop check and the send attempt.
                if self.blocked(trace):
                    raise asyncio.CancelledError("Task stopped before send")
                if implicit:
                    try:
                        wire_account = str(
                            params.get("self_id")
                            or self.connected_account(
                                client
                                if client is not None
                                else self.clients.get(platform)
                            )
                        )
                    except Exception:  # noqa: BLE001 - An unfamiliar adapter must still receive its send.
                        wire_account = ""
                attempted = True
                result = await original(action, **params)
                failed = isinstance(result, dict) and (
                    result.get("status") == "failed" or result.get("retcode", 0) != 0
                )
                outcome = "failed" if failed else "accepted"
                accepted = not failed
                if pending_id is not None:
                    await self.observe_update(
                        "UPDATE pending_sends SET status=?,payload=CASE WHEN ? THEN payload ELSE '' END WHERE id=?",
                        (outcome, failed, pending_id),
                    )
                if failed:
                    await self.observe_update(
                        "UPDATE jobs SET status='failed' WHERE trace=? AND reply_mode!=''",
                        (trace,),
                    )
                if mid:
                    await self.observe_update(
                        "UPDATE messages SET status=?,remote_id=? WHERE id=?",
                        (
                            outcome,
                            str(result.get("message_id", ""))
                            if isinstance(result, dict)
                            else "",
                            mid,
                        ),
                    )
                self.safe_log(
                    trace,
                    "ERROR" if failed else "INFO",
                    action,
                    f"API response: {result}",
                )
                if sending and not failed:
                    previous, started = sending
                    resume = previous["step"] in ("tool", "model", "guided_model")
                    await self.set_step(
                        previous["step"] if resume else "sent",
                        previous["step_detail"] if resume else "",
                        trace=trace,
                        expected=started,
                    )
                return result
            except BaseException as exc:
                if attempted:
                    # A completed API result stays known even if audit persistence is
                    # cancelled afterwards. Only actual interrupted attempts are unknown.
                    status = outcome or "unknown"
                    if pending_id is not None:
                        await self.observe_update(
                            "UPDATE pending_sends SET status=?,payload=CASE WHEN ?='accepted' THEN '' ELSE payload END WHERE id=?",
                            (status, status, pending_id),
                        )
                else:
                    status = (
                        "cancelled"
                        if isinstance(exc, asyncio.CancelledError)
                        else "failed"
                    )
                    if (
                        pending_id is not None
                        and self.send_owners.get(pending_id) is owner
                    ):
                        try:
                            await self.release_unattempted(
                                trace,
                                [pending_id],
                                claimed=bool(claimed and claimed[0]),
                            )
                        except Exception:  # noqa: BLE001 - A closed observer must not replace the stop or failure.
                            # The row stays approved/sending and becomes unknown on
                            # restart; it is never resent automatically.
                            self.dropped += 1
                if mid:
                    await self.observe_update(
                        "UPDATE messages SET status=? WHERE id=?", (status, mid)
                    )
                self.safe_log(
                    trace,
                    "ERROR",
                    action,
                    f"{'Send failed or outcome unknown' if attempted else 'Stopped before send'}: {type(exc).__name__}: {exc}",
                )
                raise
            finally:
                try:
                    if implicit and attempted:
                        if wire_account.isdecimal():
                            remote_id = (
                                str(result.get("message_id", ""))
                                if isinstance(result, dict)
                                else ""
                            )
                            await self.record_outgoing(
                                platform,
                                wire_account,
                                action,
                                params,
                                trace,
                                outcome or "unknown",
                                remote_id,
                            )
                        else:
                            self.dropped += 1
                except Exception:  # noqa: BLE001 - An audit failure must not replace the OneBot result.
                    self.dropped += 1
                finally:
                    if (
                        pending_id is not None
                        and self.send_owners.get(pending_id) is owner
                    ):
                        self.send_owners.pop(pending_id, None)
                        self.complete_review_send(pending_id, accepted)
                    self.leave_hook(owner)
                    trace_context.reset(token)

        return wrapped

    async def state(self):
        self._ensure_workers()
        return json_response(
            {
                "active": self.active,
                "version": VERSION,
                "performance": {
                    "revision": 1,
                    "ordered_db": True,
                    "bounded_media": True,
                    "bounded_prune": True,
                    "db": self.db_worker.snapshot(),
                    "media": self.media_worker.snapshot(),
                    "prune": self.prune_progress,
                },
                "platforms": list(self.clients),
                "dropped": self.dropped,
                "retention_days": 14,
                "images": self.media_state(),
            }
        )

    @filter.on_llm_request()
    async def mark_ai(self, event, req):
        trace = trace_context.get()
        if trace:
            if event is not None:
                self.live_events[trace] = event
                # WakingCheckStage may have applied per-member group isolation.
                (
                    await self.observe_update(
                        "UPDATE jobs SET umo=? WHERE trace=?",
                        (event.unified_msg_origin, trace),
                    )
                )
            (
                await self.observe_update(
                    "INSERT OR IGNORE INTO ai_traces VALUES(?)", (trace,)
                )
            )
            (await self.set_step("model"))

    @filter.on_using_llm_tool()
    async def mark_tool_start(self, event, tool, tool_args):
        (await self.set_step("tool", getattr(tool, "name", "")))

    @filter.on_llm_tool_respond()
    async def mark_tool_end(self, event, tool, tool_args, tool_result):
        (await self.set_step("model"))

    @filter.on_llm_response()
    async def mark_model_response(self, event, response):
        (await self.set_step("response"))

    @filter.on_decorating_result()
    async def mark_decorating(self, event):
        (await self.set_step("decorating"))

    @observed_api
    async def recall(self):
        body = await request.json()
        if not isinstance(body, dict) or not isinstance(body.get("id"), int):
            return error_response("无效消息 ID")
        rows = await self.db_call(
            self.store.rows,
            "SELECT m.*, c.platform,c.account FROM messages m JOIN chats c ON c.id=m.chat WHERE m.id=? AND m.direction='out'",
            (body["id"],),
        )
        if not rows or not rows[0]["remote_id"]:
            return error_response("只能撤回具有协议消息 ID 的机器人消息")
        msg = rows[0]
        if msg["status"] == "recalled":
            return json_response({"status": "recalled"})
        bot = self.clients.get(msg["platform"])
        if not bot or not msg["account"].isdigit():
            return error_response("账号连接不可用")
        try:
            result = await asyncio.wait_for(
                bot.call_action(
                    "delete_msg",
                    message_id=msg["remote_id"],
                    self_id=int(msg["account"]),
                ),
                20,
            )
            if isinstance(result, dict) and (
                result.get("status") == "failed" or result.get("retcode", 0) != 0
            ):
                raise ValueError(str(result))
            (
                await self.db_call(
                    self.store.execute,
                    "UPDATE messages SET status='recalled' WHERE id=?",
                    (msg["id"],),
                )
            )
            self.safe_log(
                msg["trace"],
                "INFO",
                "recall",
                f"Recall accepted for message {msg['remote_id']}",
            )
            return json_response({"status": "recalled"})
        except Exception as exc:  # noqa: BLE001 - Adapter-specific failures are surfaced in trace logs.  # noqa: BLE001 - Adapter errors vary by provider.
            self.safe_log(
                msg["trace"], "ERROR", "recall", f"Recall failed or unknown: {exc}"
            )
            return error_response(
                "撤回失败或结果未知，可能超时或超过可撤回时限，请查看日志"
            )

    @observed_api
    async def jobs(self):
        running = set(self.tasks) | {trace for trace, _ in traced_tasks()}
        rows = await self.db_call(
            self.store.rows,
            "SELECT j.*,c.title FROM jobs j JOIN ai_traces a ON a.trace=j.trace LEFT JOIN chats c ON c.id=j.chat ORDER BY j.started DESC LIMIT 100",
        )
        pending_counts = {
            row["trace"]: row["n"]
            for row in (
                await self.db_call(
                    self.store.rows,
                    "SELECT trace,COUNT(*) AS n FROM pending_sends WHERE status='pending' GROUP BY trace",
                )
            )
        }
        # Include older tasks with background work or a persisted reply to review.
        present = {row["trace"] for row in rows}
        for trace in (running | set(pending_counts)) - present:
            rows.extend(
                await self.db_call(
                    self.store.rows,
                    "SELECT j.*,c.title FROM jobs j JOIN ai_traces a ON a.trace=j.trace LEFT JOIN chats c ON c.id=j.chat WHERE j.trace=?",
                    (trace,),
                )
            )
        for row in rows:
            row["pending_count"] = pending_counts.get(row["trace"], 0)
            if row["pending_count"]:
                row["status"] = "awaiting_approval"
            elif (
                row["trace"] in running
                and row["trace"] not in self.cancelled
                and row["status"] != "awaiting_approval"
            ):
                row["status"] = "running"
            elif row["status"] == "stopping" and row["trace"] not in running:
                row["status"] = "cancelled"
                (
                    await self.observe_update(
                        "UPDATE jobs SET status='cancelled' WHERE trace=?",
                        (row["trace"],),
                    )
                )
            control = self.guidance_states.get(row["trace"], {})
            runner = control.get("runner")
            row["can_append"] = (
                row["trace"] in self.tasks
                and row["status"] == "running"
                and row["trace"] not in self.cancelled
                and (runner is None or not runner.done())
            )
        rows.sort(
            key=lambda row: (
                row["status"] in ("running", "stopping", "awaiting_approval"),
                row["started"],
            ),
            reverse=True,
        )
        return await self.media_call(json_response, rows)

    async def stop_trace(self, trace, *, preserve_pending=False):
        self._ensure_workers()
        revision = self.stop_revisions.get(trace, 0) + 1
        self.stop_revisions[trace] = revision
        self.stopping_traces.add(trace)
        self.cancelled.add(trace)
        if preserve_pending:
            self.preserve_pending_traces.add(trace)
        else:
            self.preserve_pending_traces.discard(trace)
        event = self.live_events.get(trace)
        if event is not None:
            event.set_extra("agent_stop_requested", True)
            event.stop_event()
        task = self.tasks.get(trace)
        runner = self.guidance_states.get(trace, {}).get("runner")
        request_stop = getattr(runner, "request_stop", None)
        cooperative = event is not None and callable(request_stop)
        targets = (
            {task}
            if task and not task.done() and task is not asyncio.current_task()
            else set()
        )
        # Signal the owner and release waits BEFORE any database admission/wait.
        for pending_id, pending_trace in list(self.review_waiter_traces.items()):
            if pending_trace == trace:
                waiter = self.review_waiters.get(pending_id)
                if waiter is not None and not waiter.done():
                    waiter.set_result(None)
        for pending_id, gate in list(self.send_gates.items()):
            if gate[0] == trace:
                self.complete_review_send(pending_id, False)
        if cooperative:
            request_stop()
        else:
            # Shared metric/transport timers only inherited the trace; never
            # cancel them on behalf of one job.
            targets.update(child for owner, child in traced_tasks() if owner == trace)
            for target in targets:
                target.cancel()
        self.safe_log(
            trace,
            "INFO",
            "admin",
            "Stop requested; future traced sends are blocked"
            + ("; waiting for Agent cleanup" if cooperative else ""),
        )
        try:
            await self.db_call(
                self.store.execute,
                "UPDATE jobs SET status='stopping' WHERE trace=? AND status IN ('running','awaiting_approval')",
                (trace,),
            )
            if not preserve_pending:
                await self.db_call(
                    self.store.execute,
                    "UPDATE pending_sends SET status='discarded',payload='' WHERE trace=? AND status IN ('pending','approved')",
                    (trace,),
                )
            # Compatibility with waiters from an older in-memory observer fixture.
            for row in await self.db_call(
                self.store.rows, "SELECT id FROM pending_sends WHERE trace=?", (trace,)
            ):
                waiter = self.review_waiters.get(row["id"])
                if waiter is not None and not waiter.done():
                    waiter.set_result(None)
            if targets:
                _, pending = await asyncio.wait(targets, timeout=3)
            else:
                pending = set()
            if not preserve_pending:
                await self.db_call(
                    self.store.execute,
                    "UPDATE pending_sends SET status='discarded',payload='' WHERE trace=? AND status IN ('pending','approved')",
                    (trace,),
                )
            # Without live work to stop, do not relabel an already finished job.
            await self.db_call(
                self.store.execute,
                "UPDATE jobs SET status=?,finished=? WHERE trace=? AND (? OR status NOT IN ('completed','failed'))",
                (
                    "stopping" if pending else "cancelled",
                    time.time(),
                    trace,
                    bool(targets),
                ),
            )
            return not pending
        finally:
            if self.stop_revisions.get(trace) == revision:
                self.stopping_traces.discard(trace)

    @observed_api
    async def stop(self):
        body = await request.json()
        trace = str(body.get("trace", "")) if isinstance(body, dict) else ""
        if (
            trace not in self.tasks
            and trace not in self.live_events
            and not (
                await self.db_call(
                    self.store.rows, "SELECT trace FROM jobs WHERE trace=?", (trace,)
                )
            )
        ):
            return error_response("任务不存在")
        stopped = await self.stop_trace(trace)
        return json_response({"stopped": stopped, "trace": trace})

    async def resolve_name(self, chat):
        """Resolve a missing name without delaying message processing.

        Args:
            chat: Stored private or temporary conversation, including account routing.
        """
        try:
            async with self.name_limit:
                bot = self.clients.get(chat["platform"])
                if (
                    not bot
                    or not chat["account"].isdigit()
                    or not chat["target"].isdigit()
                ):
                    return
                params = {
                    "self_id": int(chat["account"]),
                    "user_id": int(chat["target"]),
                }
                actions = []
                if chat["kind"] == "temporary" and chat["group_id"].isdigit():
                    actions.append(
                        (
                            "get_group_member_info",
                            {**params, "group_id": int(chat["group_id"])},
                        )
                    )
                actions.append(("get_stranger_info", params))
                for action, options in actions:
                    try:
                        info = await asyncio.wait_for(
                            bot.call_action(action, **options), 5
                        )
                        if not isinstance(info, dict):
                            continue
                        if isinstance(info.get("data"), dict):
                            info = info["data"]
                        name = next(
                            (
                                str(value).strip()[:200]
                                for value in (
                                    info.get("card"),
                                    info.get("nickname"),
                                    info.get("nick"),
                                )
                                if value
                                and str(value).strip()
                                not in (chat["target"], "N/A", "None", "unknown")
                            ),
                            "",
                        )
                        if not name:
                            continue
                        (
                            await self.db_call(
                                self.store.execute,
                                "UPDATE chats SET title=? WHERE id=? AND (title IN ('','N/A','None','unknown') OR title=target)",
                                (name, chat["id"]),
                            )
                        )
                        (
                            await self.db_call(
                                self.store.execute,
                                "UPDATE messages SET sender=? WHERE chat=? AND direction='in' AND (sender IN ('','N/A','None','unknown') OR sender=?)",
                                (name, chat["id"], chat["target"]),
                            )
                        )
                        return
                    except Exception as exc:  # noqa: BLE001 - Optional profile lookup must not affect chat.
                        self.safe_log(
                            "",
                            "DEBUG",
                            "profile",
                            f"{action} lookup failed: {type(exc).__name__}",
                        )
        finally:
            self.name_lookups.pop(chat["id"], None)

    @observed_api
    async def chats(self):
        q = str(request.query.get("q", ""))[:200]
        chat_id = str(request.query.get("id", ""))
        rows = await self.db_call(
            self.store.rows,
            """SELECT c.*, m.sender AS last_sender, m.created AS last_time,
            m.direction AS last_direction, m.status AS last_status, m.body AS last_body
            FROM chats c LEFT JOIN messages m ON m.id=(SELECT id FROM messages WHERE chat=c.id AND direction IN ('in','out') ORDER BY id DESC LIMIT 1)
            WHERE (?='' OR c.id=?) AND (title LIKE ? OR target LIKE ?) ORDER BY updated DESC LIMIT 500""",
            (chat_id, chat_id, f"%{q}%", f"%{q}%"),
        )
        rows = await self.media_call(chat_previews, rows)
        for row in rows:
            if (
                self.active
                and not self.terminating
                and row["kind"] in ("private", "temporary")
                and row["title"] in (None, "", row["target"], "N/A", "None", "unknown")
                and row["id"] not in self.name_lookups
                and time.monotonic() >= self.name_retry.get(row["id"], 0)
            ):
                self.name_retry[row["id"]] = time.monotonic() + 600
                self.name_lookups[row["id"]] = asyncio.create_task(
                    self.resolve_name(dict(row))
                )
        return await self.media_call(json_response, rows)

    @observed_api
    async def messages(self):
        chat = str(request.query.get("chat", ""))
        before = request.query.get("before", 9223372036854775807, type=int)
        q = str(request.query.get("q", ""))[:200]
        trace = str(request.query.get("trace", ""))[:64]
        rows = await self.db_call(
            self.store.rows,
            "SELECT * FROM messages WHERE chat=? AND direction IN ('in','out') AND id<? AND search LIKE ? AND (?='' OR trace=? OR id IN (SELECT source_message_id FROM jobs WHERE trace=?)) ORDER BY id DESC LIMIT 100",
            (chat, before, f"%{q}%", trace, trace, trace),
        )
        saved = await self.db_call(self.store.media_states, [row["id"] for row in rows])
        for row in rows:
            row["media"] = saved.get(row["id"], {})
            row["body"] = await self.media_call(json.loads, row["body"])
            row["related_traces"] = (
                [
                    job["trace"]
                    for job in (
                        await self.db_call(
                            self.store.rows,
                            "SELECT trace FROM jobs WHERE source_message_id=? AND trace!=? ORDER BY started",
                            (row["id"], row["trace"]),
                        )
                    )
                ]
                if row["direction"] == "in"
                else []
            )
            row["ai_related"] = bool(
                await self.db_call(
                    self.store.rows,
                    "SELECT trace FROM ai_traces WHERE trace=? UNION SELECT trace FROM messages WHERE trace=? AND direction='out' UNION SELECT a.trace FROM ai_traces a JOIN jobs j ON j.trace=a.trace WHERE j.source_message_id=? LIMIT 1",
                    (row["trace"], row["trace"], row["id"]),
                )
            )
        return await self.media_call(json_response, list(reversed(rows)))

    @observed_api
    async def logs(self):
        trace = str(request.query.get("trace", ""))[:64]
        q = str(request.query.get("q", ""))[:200]
        level = str(request.query.get("level", ""))[:16]
        before = request.query.get("before", 9223372036854775807, type=int)
        rows = await self.db_call(
            self.store.rows,
            "SELECT * FROM logs WHERE (?='' OR trace=? OR trace IN (SELECT j.trace FROM jobs j JOIN messages m ON m.id=j.source_message_id WHERE m.trace=? AND m.direction='in')) AND (?='' OR level=?) AND body LIKE ? AND id<? ORDER BY id DESC LIMIT 300",
            (trace, trace, trace, level, level, f"%{q}%", before),
        )
        return await self.media_call(json_response, list(reversed(rows)))

    @observed_api
    async def settings(self):
        """GET returns all console settings; POST saves only the keys it sends."""
        if request.method == "POST":
            body = await request.json()
            if (
                not isinstance(body, dict)
                or not body
                or not all(valid_setting(key, value) for key, value in body.items())
            ):
                return error_response("无效设置")
            await self.db_call(self.store.save_settings, body)
        rows = await self.db_call(self.store.rows, "SELECT key,value FROM settings")
        self.console_values = console_settings(rows)
        if request.method == "POST" and "image_quota" in body:
            await self.trim_media()
        return json_response(self.console_values)

    @observed_api
    async def send(self):
        body = await request.json()
        if not isinstance(body, dict):
            return error_response("无效请求")
        chat_id = str(body.get("chat", ""))
        rows = await self.db_call(
            self.store.rows, "SELECT * FROM chats WHERE id=?", (chat_id,)
        )
        if not rows:
            return error_response("会话不存在，请等待该会话的新消息")
        chat = rows[0]
        bot = self.clients.get(chat["platform"])
        if bot is None or not chat["target"].isdigit() or not chat["account"].isdigit():
            return error_response("账号或连接不可用，无法确定发送目标")
        if chat["kind"] == "temporary" and not chat["group_id"].isdigit():
            return error_response("临时会话缺少来源群号，不能安全发送")
        text = body.get("text", "")
        if not isinstance(text, str) or len(text) > 10000:
            return error_response("消息最多 10000 字")
        message = [{"type": "text", "data": {"text": text}}] if text.strip() else []
        image = body.get("image", "")
        if image:
            try:
                await self.media_call(validate_upload, image)
                message.append({"type": "image", "data": {"file": "base64://" + image}})
            except Exception:  # noqa: BLE001 - Observer failures must not block message delivery.
                return error_response(
                    "请选择不超过 2 MB、2500 万像素的 PNG/JPEG/GIF/WebP 图片"
                )
        if not message:
            return error_response("请输入消息或选择图片")
        reference_id = body.get("reference_id")
        if reference_id is not None:
            if body.get("reference_kind", "message") not in ("user", "message"):
                return error_response("无效引用方式")
            if type(reference_id) is not int:
                return error_response("无效引用消息 ID")
            references = await self.db_call(
                self.store.rows,
                "SELECT remote_id,sender_id FROM messages WHERE id=? AND chat=? AND direction='in'",
                (reference_id, chat_id),
            )
            if not references:
                return error_response("只能引用当前会话收到的用户消息")
            reference = references[0]
            if body.get("reference_kind", "message") == "user":
                if chat["kind"] == "group":
                    if not reference["sender_id"].isdigit():
                        return error_response("所选群成员缺少 QQ 号，请选择其新消息")
                    message.insert(
                        0, {"type": "at", "data": {"qq": reference["sender_id"]}}
                    )
            elif reference["remote_id"] and reference["remote_id"] != "0":
                message.insert(
                    0, {"type": "reply", "data": {"id": reference["remote_id"]}}
                )
            else:
                return error_response("这条消息没有可用于 QQ 引用的消息 ID")
        rid = str(body.get("request_id", ""))
        try:
            uuid.UUID(rid)
        except ValueError:
            return error_response("缺少有效的发送请求 ID")
        async with self.send_lock:
            existing = await self.db_call(
                self.store.rows, "SELECT * FROM requests WHERE id=?", (rid,)
            )
            if existing:
                if existing[0]["chat"] != chat_id:
                    return error_response("发送请求 ID 已用于其他会话")
                # The page tells a delivered request from one whose result is
                # unknown; neither is sent again under the same request ID.
                outcome = await self.db_call(
                    self.store.rows,
                    "SELECT status FROM messages WHERE trace=? AND direction='out' ORDER BY id DESC LIMIT 1",
                    (existing[0]["trace"],),
                )
                return json_response(
                    {
                        "trace": existing[0]["trace"],
                        "duplicate": True,
                        "status": outcome[0]["status"] if outcome else "unknown",
                    }
                )
            trace = uuid.uuid4().hex
            (
                await self.db_call(
                    self.store.execute,
                    "INSERT INTO requests VALUES(?,?,?,?)",
                    (rid, chat_id, trace, time.time()),
                )
            )
        token = trace_context.set(trace)
        try:
            params = {"self_id": int(chat["account"]), "message": message}
            if chat["kind"] == "group":
                action = "send_group_msg"
                params["group_id"] = int(chat["target"])
            else:
                action = "send_private_msg"
                params["user_id"] = int(chat["target"])
                if chat["kind"] == "temporary":
                    params["group_id"] = int(chat["group_id"])
            self.safe_log(
                trace,
                "INFO",
                "admin",
                f"Manual send by {getattr(request, 'username', 'dashboard user')}",
            )
            result = await asyncio.wait_for(bot.call_action(action, **params), 60)
            if isinstance(result, dict) and (
                result.get("status") == "failed" or result.get("retcode", 0) != 0
            ):
                return error_response("发送失败，请查看消息日志", data={"trace": trace})
            return json_response({"trace": trace, "status": "accepted"})
        except Exception as exc:  # noqa: BLE001 - Adapter-specific failures are surfaced in trace logs.
            self.safe_log(trace, "ERROR", "admin", str(exc))
            return error_response(
                "发送失败或结果未知，请先检查聊天记录，勿重复发送",
                data={"trace": trace},
            )
        finally:
            trace_context.reset(token)

    def restore_hooks(self):
        """Restore only the wrappers this instance installed and still owns."""
        if PipelineScheduler.execute is getattr(self, "traced_execute", None):
            PipelineScheduler.execute = self.original_execute
        if ToolLoopAgentRunner._iter_llm_responses is getattr(
            self, "guided_llm_responses", None
        ):
            ToolLoopAgentRunner._iter_llm_responses = self.original_llm_responses
        if (
            hasattr(self, "history_stage")
            and self.history_stage._save_to_history is self.guided_save_history
        ):
            self.history_stage._save_to_history = self.original_save_history
        for bot, original, wrapped in self.hooks:
            if bot.call_action is wrapped:
                bot.call_action = original

    def plugin_owners(self):
        """Tasks whose observer state or send guard must settle before unload.

        A stopped task may still be cleaning up; restoring the raw sender under
        it would let a late reply out. Ordinary chat pipelines are not owners:
        they keep running unobserved after reload, and their observer calls
        already tolerate a closed lane.
        """
        owners = set(self.api_tasks) | set(self.name_lookups.values())
        owners.update(task for trace, task in self.tasks.items() if self.guarded(trace))
        owners.update(
            task for task in self.hook_tasks if self.guarded(task_trace(task))
        )
        owners.discard(asyncio.current_task())
        return {task for task in owners if not task.done()}

    async def settle_owners(self):
        """Wait a bounded time for plugin-owned work, then cancel what is left."""
        owners = self.plugin_owners()
        if not owners:
            return set()
        _, pending = await asyncio.wait(owners, timeout=OWNER_DRAIN_SECONDS)
        if not pending:
            return set()
        for task in pending:
            task.cancel()
        _, pending = await asyncio.wait(pending, timeout=OWNER_CANCEL_SECONDS)
        if pending:
            logger.warning(
                f"{NAME}: {len(pending)} plugin-owned task(s) ignored cancellation "
                "during reload; their send guards stay installed until restart."
            )
        return pending

    async def terminate(self):
        self._ensure_workers()
        self.terminating = True
        self.active = False
        for task in self.name_lookups.values():
            task.cancel()
        await self.stop_media()
        # Reload/disable stops only work that can hold a review, guidance or
        # approval gate. Ordinary replies continue without the console.
        traces = (
            {trace for trace in self.tasks if self.is_guided(trace)}
            | set(self.review_waiter_traces.values())
            | {gate[0] for gate in self.send_gates.values()}
        )
        await asyncio.gather(
            *(self.stop_trace(trace, preserve_pending=True) for trace in traces),
            return_exceptions=True,
        )
        # Do not close SQLite underneath an accepted handler or guided owner,
        # nor restore the raw sender under a stopped task.
        stragglers = await self.settle_owners()
        await self.close_media()
        if hasattr(self, "sink_id"):
            loguru_logger.remove(self.sink_id)
        # A straggler that ignored cancellation keeps the guards installed. They
        # pass every other call through and keep that trace blocked.
        # Background tool, handoff or image tasks keep their job's trace after
        # the pipeline returns. They are not cancelled here: shared resources can
        # inherit a trace too. While one of a guarded trace lives, the guards
        # stay so its late reply is blocked instead of sent unreviewed or after
        # a stop.
        lingering = [task for trace, task in traced_tasks() if self.guarded(trace)]
        if lingering and not stragglers:
            logger.warning(
                f"{NAME}: {len(lingering)} background task(s) of reviewed or "
                "stopped work are still running; their send guards stay installed "
                "until restart."
            )
        if not stragglers and not lingering:
            self.restore_hooks()
        self.context.registered_web_apis[:] = [
            r
            for r in self.context.registered_web_apis
            if not r[0].startswith(f"/{NAME}/")
        ]
        try:
            if hasattr(self, "worker"):
                await self.worker
            while self.queue:
                batch, self.queue = self.queue[:128], self.queue[128:]
                await self.db_call(self.write_logs, batch)
        finally:
            await self.close_workers()
