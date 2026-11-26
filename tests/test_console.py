import asyncio
import base64
import io
import json
import sqlite3
import tempfile
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from astrbot_plugin_qq_chat_workbench.main import QQChatWorkbench
from astrbot_plugin_qq_chat_workbench.store import Store, redact, trace_context
from PIL import Image


def drain_prune(store):
    for _ in range(10000):
        result = store.prune()
        if not result["more"]:
            return
    raise AssertionError("Retention cursor did not finish")


class ConsoleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.plugin = object.__new__(QQChatWorkbench)
        p = self.plugin
        p.store = Store(Path(self.temp.name) / "test.db")
        p.active = True
        p.queue = []
        p.dropped = 0
        p.tasks = {}
        p.cancelled = set()
        p.name_lookups = {}
        p.name_retry = {}
        p.name_limit = asyncio.Semaphore(2)
        p.clients = {}
        p.send_lock = asyncio.Lock()
        p.guidance_states = {}
        p.live_events = {}
        p.review_waiters = {}
        p.guidance_lock = asyncio.Lock()
        p.review_lock = asyncio.Lock()
        p.context = SimpleNamespace(
            get_config=lambda umo=None: {
                "provider_settings": {"agent_runner_type": "local"}
            }
        )
        p._ensure_workers()
        self.calls = []

        async def original(action, **params):
            self.calls.append((action, params))
            await asyncio.sleep(0)
            return {"message_id": 42}

        p.clients["qq"] = SimpleNamespace(call_action=p.wrap_sender(original, "qq"))

    async def asyncTearDown(self):
        pending = list(self.plugin.name_lookups.values()) + list(
            self.plugin.tasks.values()
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        from astrbot_plugin_qq_chat_workbench.guidance import ToolLoopAgentRunner

        if ToolLoopAgentRunner._iter_llm_responses is getattr(
            self.plugin, "guided_llm_responses", None
        ):
            ToolLoopAgentRunner._iter_llm_responses = self.plugin.original_llm_responses
        if (
            hasattr(self.plugin, "history_stage")
            and self.plugin.history_stage._save_to_history
            is self.plugin.guided_save_history
        ):
            self.plugin.history_stage._save_to_history = (
                self.plugin.original_save_history
            )
        await self.plugin.close_workers()

    async def post(self, method, body):
        from astrbot.api.web import PluginRequest, bind_request_context
        from starlette.requests import Request

        raw = json.dumps(body).encode()

        async def receive():
            return {"type": "http.request", "body": raw, "more_body": False}

        req = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/test",
                "query_string": b"",
                "headers": [],
                "scheme": "http",
                "server": ("localhost", 80),
            },
            receive,
        )
        with bind_request_context(PluginRequest(req, username="test-admin")):
            result = await getattr(self.plugin, method)()
        return result.status_code, json.loads(result.body)

    async def test_real_query_request(self):
        from astrbot.api.web import PluginRequest, bind_request_context
        from starlette.requests import Request

        req = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/test",
                "query_string": b"q=hello&before=invalid",
                "headers": [],
                "scheme": "http",
                "server": ("localhost", 80),
            }
        )
        with bind_request_context(PluginRequest(req, username="test-admin")):
            for method in ("chats", "messages", "logs", "jobs"):
                result = await getattr(self.plugin, method)()
                self.assertEqual(result.status_code, 200)

    async def get(self, method, query):
        from urllib.parse import urlencode

        from astrbot.api.web import PluginRequest, bind_request_context
        from starlette.requests import Request

        req = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/test",
                "query_string": urlencode(query).encode(),
                "headers": [],
                "scheme": "http",
                "server": ("localhost", 80),
            }
        )
        with bind_request_context(PluginRequest(req, username="test-admin")):
            response = await getattr(self.plugin, method)()
        self.assertEqual(response.status_code, 200)
        return json.loads(response.body)

    async def test_background_event_links_original_without_empty_bubble(self):
        p = self.plugin
        raw = {
            "post_type": "message",
            "message_type": "private",
            "self_id": 999,
            "user_id": 200,
            "message_id": -123,
            "message": "original request",
        }
        event = SimpleNamespace(
            get_platform_name=lambda: "aiocqhttp",
            get_platform_id=lambda: "qq",
            get_sender_id=lambda: "200",
            get_sender_name=lambda: "user",
            get_message_str=lambda: "original request",
            get_extra=lambda key, default=None: default,
            message_obj=SimpleNamespace(raw_message=raw, self_id="999", group=None),
        )
        chat, source_id, prompt = await p.receive(event, "original")
        raw["message"] = []
        event.get_message_str = lambda: ""
        event.get_extra = lambda key, default=None: (
            True if key == "_reference_image_completion" else default
        )
        self.assertEqual(
            (await p.receive(event, "background")), (chat, source_id, prompt)
        )
        self.assertEqual(len(p.store.rows("SELECT * FROM messages")), 1)
        raw["user_id"] = 201
        event.get_sender_id = lambda: "201"
        self.assertIsNone((await p.receive(event, "other-user"))[1])
        self.assertEqual(len(p.store.rows("SELECT * FROM messages")), 1)
        raw["user_id"] = 200
        raw["message_id"] = -124
        raw["raw_message"] = "fallback text"
        event.get_extra = lambda key, default=None: default
        (await p.receive(event, "fallback"))
        self.assertEqual(
            p.store.rows("SELECT search FROM messages ORDER BY id DESC")[0]["search"],
            "fallback text",
        )

    async def test_background_history_linking_and_trace_apis(self):
        p = self.plugin
        chat = p.store.chat("qq", "999", "private", "200", title="user")
        other = p.store.chat("qq", "888", "private", "200", title="other account")
        source = p.store.message(
            chat, "original", "in", "user", "actual message", "received", "123"
        )
        blank = p.store.message(chat, "background", "in", "user", [], "received", "123")
        unknown = p.store.message(other, "unknown", "in", "user", [], "received", "123")
        for trace, target in [
            ("original", chat),
            ("background", chat),
            ("unknown", other),
        ]:
            p.store.execute(
                "INSERT INTO jobs(trace,chat,prompt,status,started) VALUES(?,?,'','running',1)",
                (trace, target),
            )
        p.store.execute("INSERT INTO ai_traces VALUES('background')")
        for trace in ("original", "background", "unknown"):
            p.store.log(trace, "INFO", "test", trace)
        directory = Path(self.temp.name)
        self.assertEqual(p.store.repair_background_messages(directory), 1)
        self.assertEqual(p.store.repair_background_messages(directory), 0)
        self.assertEqual(
            len(list(directory.glob("console.before-background-link-*.db"))), 1
        )
        self.assertEqual(len(p.store.rows("SELECT * FROM messages")), 3)
        self.assertEqual(
            p.store.rows("SELECT direction FROM messages WHERE id=?", (blank,))[0][
                "direction"
            ],
            "internal",
        )
        self.assertEqual(
            p.store.rows("SELECT direction FROM messages WHERE id=?", (unknown,))[0][
                "direction"
            ],
            "in",
        )
        job = p.store.rows("SELECT * FROM jobs WHERE trace='background'")[0]
        self.assertEqual(
            (job["source_message_id"], job["prompt"]), (source, "actual message")
        )
        messages = await self.get("messages", {"chat": chat, "trace": "background"})
        self.assertEqual([m["id"] for m in messages], [source])
        self.assertEqual(messages[0]["related_traces"], ["background"])
        self.assertTrue(messages[0]["ai_related"])
        self.assertEqual(
            (await self.get("chats", {"id": chat}))[0]["preview"], "actual message"
        )
        self.assertEqual(
            [row["trace"] for row in await self.get("logs", {"trace": "original"})],
            ["original", "background"],
        )
        self.assertEqual(
            [row["trace"] for row in await self.get("logs", {"trace": "background"})],
            ["background"],
        )

    def test_prune_keeps_running_background_ai_trace_without_message(self):
        p = self.plugin
        p.store.execute(
            "INSERT INTO jobs(trace,status,started) VALUES('background','running',1)"
        )
        p.store.execute("INSERT INTO ai_traces VALUES('background')")
        drain_prune(p.store)
        self.assertEqual(
            p.store.rows("SELECT trace FROM ai_traces"), [{"trace": "background"}]
        )

    async def test_task_list_only_contains_ai_work(self):
        p = self.plugin
        chat = p.store.chat("qq", "999", "private", "200")
        for number in range(150):
            p.store.execute(
                "INSERT INTO jobs(trace,chat,prompt,status,started) VALUES(?,?,?,?,?)",
                (f"ordinary-{number}", chat, "ordinary chat", "completed", number + 10),
            )
        p.store.execute(
            "INSERT INTO jobs(trace,chat,prompt,status,started) VALUES(?,?,?,?,?)",
            ("ai", chat, "AI question", "completed", 1),
        )
        token = trace_context.set("ai")
        await p.mark_ai(None, None)
        trace_context.reset(token)
        p.tasks["ordinary-1"] = asyncio.current_task()
        response = await p.jobs()
        self.assertEqual([row["trace"] for row in json.loads(response.body)], ["ai"])

    async def test_exact_chat_and_trace_lookup(self):
        from urllib.parse import urlencode

        from astrbot.api.web import PluginRequest, bind_request_context
        from starlette.requests import Request

        p = self.plugin
        target = p.store.chat("qq", "999", "temporary", "200", "100")
        p.store.chat("qq", "999", "private", "200")
        p.store.message(target, "source", "in", "user", "original", "received")
        for _ in range(110):
            p.store.message(target, "other", "in", "user", "later", "received")
        for method, query in [
            ("chats", {"id": target}),
            ("messages", {"chat": target, "trace": "source"}),
        ]:
            req = Request(
                {
                    "type": "http",
                    "method": "GET",
                    "path": "/test",
                    "query_string": urlencode(query).encode(),
                    "headers": [],
                    "scheme": "http",
                    "server": ("localhost", 80),
                }
            )
            with bind_request_context(PluginRequest(req, username="test-admin")):
                response = await getattr(p, method)()
            rows = json.loads(response.body)
            self.assertEqual(len(rows), 1)
            self.assertEqual(
                rows[0]["id"] if method == "chats" else rows[0]["trace"],
                target if method == "chats" else "source",
            )

    async def test_chat_preview_sender_timestamp_and_attachment_summary(self):
        from astrbot.api.web import PluginRequest, bind_request_context
        from starlette.requests import Request

        p = self.plugin
        chat = p.store.chat("qq", "999", "group", "100")
        p.store.message(
            chat, "old", "in", "Old sender", "old", "received", created=1700000000
        )
        p.store.message(
            chat,
            "new",
            "in",
            "小明",
            [
                {
                    "type": "image",
                    "data": {"url": "https://example.invalid/private-image"},
                },
                {"type": "file", "data": {"name": "test.txt"}},
            ],
            "received",
            created=1700000010,
        )
        req = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/test",
                "query_string": b"",
                "headers": [],
                "scheme": "http",
                "server": ("localhost", 80),
            }
        )
        with bind_request_context(PluginRequest(req, username="test-admin")):
            response = await p.chats()
        row = json.loads(response.body)[0]
        self.assertEqual(row["last_sender"], "小明")
        self.assertEqual(row["last_time"], 1700000010)
        self.assertEqual(row["preview"], "[图片] [文件]")
        self.assertNotIn("private-image", response.body.decode())
        self.assertNotIn("last_body", row)

    async def test_missing_temporary_name_lookup_and_cache(self):
        p = self.plugin
        chat = p.store.chat("qq", "999", "temporary", "200", "100", "200")
        p.store.message(chat, "t", "in", "200", "hello", "received")
        p.store.message(chat, "t", "out", "机器人", "reply", "accepted")
        calls = []

        async def lookup(action, **params):
            calls.append((action, params))
            if action == "get_group_member_info":
                raise TimeoutError()
            return {"nickname": "小明"}

        p.clients["qq"].call_action = lookup
        await p.resolve_name(p.store.rows("SELECT * FROM chats WHERE id=?", (chat,))[0])
        self.assertEqual(
            p.store.rows("SELECT title FROM chats WHERE id=?", (chat,))[0]["title"],
            "小明",
        )

        self.assertEqual(
            [
                r["sender"]
                for r in p.store.rows("SELECT sender FROM messages ORDER BY id")
            ],
            ["小明", "机器人"],
        )
        self.assertEqual(
            calls[0],
            (
                "get_group_member_info",
                {"self_id": 999, "user_id": 200, "group_id": 100},
            ),
        )
        p.store.chat("qq", "999", "temporary", "200", "100", "200")
        event = SimpleNamespace(
            get_platform_name=lambda: "aiocqhttp",
            get_platform_id=lambda: "qq",
            get_sender_id=lambda: "200",
            get_sender_name=lambda: "200",
            get_message_str=lambda: "new",
            message_obj=SimpleNamespace(
                self_id="999",
                group=None,
                timestamp=1700000010,
                raw_message={
                    "self_id": 999,
                    "message_type": "private",
                    "sub_type": "group",
                    "group_id": 100,
                    "user_id": 200,
                    "sender": {},
                    "message": "new",
                },
            ),
        )
        (await p.receive(event, "next"))
        self.assertEqual(
            p.store.rows("SELECT sender FROM messages ORDER BY id DESC LIMIT 1")[0][
                "sender"
            ],
            "小明",
        )
        self.assertEqual(
            p.store.rows("SELECT title FROM chats WHERE id=?", (chat,))[0]["title"],
            "小明",
        )

    async def test_group_notice_does_not_create_private_conversation(self):
        p = self.plugin
        event = SimpleNamespace(
            get_platform_name=lambda: "aiocqhttp",
            message_obj=SimpleNamespace(
                raw_message={"post_type": "notice", "group_id": 100, "user_id": 200}
            ),
        )
        self.assertIsNone(await p.receive(event, "notice"))
        self.assertEqual(p.store.rows("SELECT * FROM chats"), [])
        event.message_obj.raw_message = {"group_id": 100, "user_id": 200}
        self.assertIsNone(await p.receive(event, "missing-type"))

    def test_group_notice_migration_preserves_real_private_messages(self):
        p = self.plugin
        chat = p.store.chat("qq", "999", "temporary", "200", "100", "小明")
        p.store.message(chat, "notice", "in", "小明", "", "received")
        p.store.message(
            chat, "private", "in", "小明", "real private message", "received"
        )
        for trace, umo in [
            ("notice", "qq:GroupMessage:100"),
            ("private", "qq:FriendMessage:200"),
        ]:
            p.store.execute(
                "INSERT INTO jobs(trace,chat,umo,status,started) VALUES(?,?,?,?,?)",
                (trace, chat, umo, "completed", 1),
            )
        p.store.log("notice", "INFO", "test", "retained log")
        directory = Path(self.temp.name)
        p.store.repair_group_notices(directory)
        rows = p.store.rows(
            "SELECT m.trace,c.kind,m.search FROM messages m JOIN chats c ON c.id=m.chat ORDER BY m.id"
        )
        self.assertEqual(
            rows,
            [
                {"trace": "notice", "kind": "group", "search": "[群通知]"},
                {
                    "trace": "private",
                    "kind": "temporary",
                    "search": "real private message",
                },
            ],
        )
        self.assertEqual(len(p.store.rows("SELECT * FROM logs")), 1)
        self.assertEqual(
            len(list(directory.glob("console.before-group-notice-fix-*.db"))), 1
        )
        p.store.repair_group_notices(directory)
        self.assertEqual(
            len(list(directory.glob("console.before-group-notice-fix-*.db"))), 1
        )

    async def test_routing_and_idempotency(self):
        for kind, target, group in [
            ("group", "100", ""),
            ("private", "200", ""),
            ("temporary", "200", "100"),
        ]:
            chat = self.plugin.store.chat("qq", "999", kind, target, group)
            body = {"chat": chat, "text": "hello", "request_id": str(uuid.uuid4())}
            status, result = await self.post("send", body)
            self.assertEqual(status, 200)
            self.assertEqual(result["status"], "accepted")
            await self.post("send", body)
            action, params = self.calls[-1]
            self.assertEqual(params["self_id"], 999)
            if kind == "group":
                self.assertEqual(action, "send_group_msg")
                self.assertEqual(params["group_id"], 100)
            else:
                self.assertEqual(action, "send_private_msg")
                self.assertEqual(params["user_id"], 200)
                self.assertEqual(params.get("group_id"), 100 if group else None)
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(len(self.plugin.store.rows("SELECT * FROM messages")), 3)

    async def test_concurrent_trace_and_cancelled_send(self):
        async def send(trace, user):
            token = trace_context.set(trace)
            try:
                await self.plugin.clients["qq"].call_action(
                    "send_private_msg", user_id=user, self_id=999, message=str(user)
                )
            finally:
                trace_context.reset(token)

        await asyncio.gather(send("a", 1), send("b", 2))
        rows = self.plugin.store.rows("SELECT trace,search FROM messages ORDER BY id")
        self.assertEqual(
            rows, [{"trace": "a", "search": "1"}, {"trace": "b", "search": "2"}]
        )
        self.plugin.cancelled.add("a")
        with self.assertRaises(asyncio.CancelledError):
            await send("a", 1)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(trace_context.get(), "")

    async def test_stop_task_and_child(self):
        async def processing():
            await asyncio.sleep(100)

        trace = "task"
        self.plugin.store.execute(
            "INSERT INTO jobs(trace,status,started) VALUES(?,?,?)",
            (trace, "running", 1),
        )
        token = trace_context.set(trace)
        parent = asyncio.create_task(processing())
        child = asyncio.create_task(processing())
        trace_context.reset(token)
        self.plugin.tasks[trace] = parent
        self.assertTrue(await self.plugin.stop_trace(trace))
        self.assertTrue(parent.cancelled())
        if hasattr(child, "get_context"):
            self.assertTrue(child.cancelled())
        else:
            child.cancel()
            await asyncio.gather(child, return_exceptions=True)

    async def test_guided_config_snapshot_excludes_locks_and_isolates_tasks(self):
        import copy
        import threading

        from astrbot.core.config import AstrBotConfig
        from astrbot.core.config.default import DEFAULT_CONFIG

        p = self.plugin
        config_path = Path(self.temp.name) / "locked-config.json"
        locked_config = AstrBotConfig(str(config_path), default_config={})
        locked_config.update(copy.deepcopy(DEFAULT_CONFIG))
        # Exercise runtime attributes even on core versions without save locks.
        object.__setattr__(locked_config, "_test_runtime_lock", threading.Lock())
        locked_config["platform_settings"]["reply_with_quote"] = True
        locked_config["platform_settings"]["reply_with_mention"] = False
        locked_config["provider_settings"].update(
            {
                "show_tool_use_status": True,
                "show_tool_call_result": True,
                "display_reasoning_text": True,
                "buffer_intermediate_messages": False,
                "streaming_response": True,
            }
        )
        locked_config["nested_probe"] = {"items": [{"values": ["original"]}]}
        original = copy.deepcopy(dict(locked_config))
        original_file = config_path.read_bytes()
        original_attributes = vars(locked_config).copy()
        with self.assertRaisesRegex(TypeError, "cannot pickle.*lock"):
            copy.deepcopy(locked_config)

        cases = [
            (kind, reference)
            for kind in ("private", "temporary", "group")
            for reference in ("user", "message")
        ]
        for source in (locked_config, copy.deepcopy(original)):
            with self.subTest(config_type=type(source).__name__):
                p.context = SimpleNamespace(
                    get_config=Mock(return_value=source),
                    conversation_manager=SimpleNamespace(),
                )
                snapshots = {}
                ready = asyncio.Event()

                async def execute(
                    scheduler, event, source=source, snapshots=snapshots, ready=ready
                ):
                    snapshot = scheduler.ctx.astrbot_config
                    self.assertIs(type(snapshot), dict)
                    self.assertIsNot(snapshot, source)
                    self.assertEqual(trace_context.get(), event.test_trace)
                    self.assertFalse(hasattr(snapshot, "_test_runtime_lock"))
                    self.assertNotIn("_save_state_lock", snapshot)
                    self.assertNotIn("_save_commit_lock", snapshot)
                    expected_platform = copy.deepcopy(original["platform_settings"])
                    if event.get_extra("_qq_chat_workbench_reference_kind") == "user":
                        expected_platform["reply_with_quote"] = False
                        if event.get_group_id():
                            expected_platform["reply_with_mention"] = True
                    self.assertEqual(snapshot["platform_settings"], expected_platform)
                    expected_provider = copy.deepcopy(original["provider_settings"])
                    expected_provider.update(
                        {
                            "show_tool_use_status": False,
                            "show_tool_call_result": False,
                            "display_reasoning_text": False,
                            "buffer_intermediate_messages": True,
                            "streaming_response": False,
                        }
                    )
                    self.assertEqual(snapshot["provider_settings"], expected_provider)
                    snapshots[event.test_trace] = snapshot
                    if len(snapshots) == len(cases):
                        ready.set()
                    await asyncio.wait_for(ready.wait(), 3)
                    values = snapshot["nested_probe"]["items"][0]["values"]
                    self.assertEqual(values, ["original"])
                    values.append(event.test_trace)
                    await asyncio.sleep(0)
                    self.assertEqual(values, ["original", event.test_trace])

                p.original_execute = execute
                events = []
                tasks = []
                for index, (kind, reference) in enumerate(cases):
                    trace = str(uuid.uuid4())
                    group = "100" if kind == "group" else ""
                    umo = (
                        f"qq:GroupMessage:100_{200 + index}"
                        if group
                        else f"qq:FriendMessage:{200 + index}"
                    )
                    event = SimpleNamespace(
                        unified_msg_origin=umo,
                        get_group_id=lambda group=group: group,
                        get_extra=lambda key, reference=reference: (
                            reference
                            if key == "_qq_chat_workbench_reference_kind"
                            else None
                        ),
                        test_trace=trace,
                    )
                    events.append(event)
                    p.store.execute(
                        "INSERT INTO jobs(trace,umo,status,started) VALUES(?,?,?,?)",
                        (trace, umo, "running", 1),
                    )
                    p.live_events[trace] = event
                    p.guidance_states[trace] = {"version": 1}
                    task = asyncio.create_task(p.run_guided_task(trace, event))
                    p.tasks[trace] = task
                    tasks.append(task)
                with patch.object(
                    AstrBotConfig,
                    "save_config",
                    side_effect=AssertionError("Guidance must not persist config"),
                ):
                    await asyncio.wait_for(asyncio.gather(*tasks), 5)
                self.assertEqual(
                    len(snapshots),
                    len(cases),
                    p.store.rows("SELECT body FROM logs WHERE level='ERROR'"),
                )
                for event in events:
                    row = p.store.rows(
                        "SELECT status,finished FROM jobs WHERE trace=?",
                        (event.test_trace,),
                    )[0]
                    self.assertEqual(row["status"], "completed")
                    self.assertIsNotNone(row["finished"])
                    self.assertNotIn(event.test_trace, p.tasks)
                    self.assertNotIn(event.test_trace, p.live_events)
                    self.assertNotIn(event.test_trace, p.guidance_states)
                self.assertEqual(
                    [
                        call.args[0]
                        for call in p.context.get_config.call_args_list
                        if call.args
                    ],
                    [event.unified_msg_origin for event in events],
                )
                self.assertEqual(dict(source), original)
                self.assertEqual(vars(locked_config), original_attributes)
                self.assertEqual(config_path.read_bytes(), original_file)
                self.assertEqual(self.calls, [])
                self.assertEqual(
                    p.store.rows("SELECT * FROM logs WHERE level='ERROR'"), []
                )

    async def test_guidance_uses_original_session_and_waits_for_review(self):
        import copy

        from astrbot.core.config import AstrBotConfig
        from astrbot.core.config.default import DEFAULT_CONFIG
        from astrbot.core.platform.platform_metadata import PlatformMetadata

        p = self.plugin
        chat = p.store.chat("qq", "999", "temporary", "200", "100", "user")
        message_id = p.store.message(
            chat,
            "old",
            "in",
            "user",
            "Original request",
            "received",
            "123",
            sender_id="200",
        )
        p.store.execute(
            "INSERT INTO jobs(trace,chat,umo,prompt,status,started,source_message_id) VALUES(?,?,?,?,?,?,?)",
            (
                "old",
                chat,
                "qq:FriendMessage:200",
                "Original request",
                "completed",
                1,
                message_id,
            ),
        )
        config = AstrBotConfig(
            str(Path(self.temp.name) / "guided-config.json"), default_config={}
        )
        config.update(copy.deepcopy(DEFAULT_CONFIG))
        meta = PlatformMetadata("aiocqhttp", "test", "qq")
        p.context = SimpleNamespace(
            get_platform_inst=lambda _: SimpleNamespace(meta=lambda: meta),
            get_config=lambda umo=None: config,
            conversation_manager=SimpleNamespace(),
        )
        ready = asyncio.Event()

        async def execute(scheduler, event):
            self.assertEqual(
                [type(stage).__name__ for stage in scheduler.stages],
                ["AgentRequestSubStage", "ResultDecorateStage", "RespondStage"],
            )
            self.assertEqual(event.unified_msg_origin, "qq:FriendMessage:200")
            self.assertEqual(event.get_sender_id(), "200")
            self.assertEqual(event.message_obj.raw_message["group_id"], 100)
            self.assertEqual(event.message_str, "Original request")
            self.assertTrue(event.get_extra("skip_follow_up_capture"))
            self.assertFalse(event.get_extra("enable_streaming"))
            self.assertFalse(
                scheduler.ctx.astrbot_config["provider_settings"][
                    "show_tool_use_status"
                ]
            )
            ready.set()
            await p.clients["qq"].call_action(
                "send_private_msg",
                self_id=999,
                user_id=200,
                group_id=100,
                message="Generated reply",
            )

        p.original_execute = execute
        body = {
            "chat": chat,
            "message_id": message_id,
            "mode": "new",
            "reference_kind": "message",
            "guidance": "Reply politely",
            "reply_mode": "confirm",
            "request_id": str(uuid.uuid4()),
        }
        status, result = await self.post("guide", body)
        self.assertEqual(status, 200)
        trace = result["trace"]
        task = p.tasks[trace]
        await asyncio.wait_for(ready.wait(), 5)
        # The mock pipeline has started, but observer publication is now async.
        async with asyncio.timeout(5):
            while trace not in p.review_waiter_traces.values():
                await asyncio.sleep(0.005)
        self.assertEqual(self.calls, [])
        self.assertEqual(len(p.store.rows("SELECT * FROM messages")), 1)
        job = p.store.rows("SELECT * FROM jobs WHERE trace=?", (trace,))[0]
        self.assertEqual(
            (job["status"], job["prompt"], job["reply_mode"]),
            ("awaiting_approval", "Original request", "confirm"),
        )
        review = await self.get("review", {"trace": trace})
        self.assertEqual(review["instructions"][0]["guidance"], "Reply politely")
        self.assertEqual(review["instructions"][0]["quoted_text"], "Original request")
        ids = [item["id"] for item in review["items"]]
        self.assertEqual(
            review["items"][0]["body"][0]["data"]["text"], "Generated reply"
        )
        self.assertEqual((await self.post("guide", body))[1]["trace"], trace)
        self.assertEqual(
            (await self.post("approve", {"trace": "old", "ids": ids}))[0], 400
        )
        self.assertEqual(
            (await self.post("approve", {"trace": trace, "ids": ids}))[0], 200
        )
        await asyncio.wait_for(task, 5)
        await self.post("approve", {"trace": trace, "ids": ids})
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0][1]["group_id"], 100)
        self.assertEqual(
            p.store.rows("SELECT status,payload FROM pending_sends")[0],
            {"status": "accepted", "payload": ""},
        )
        self.assertEqual(
            p.store.rows("SELECT status FROM jobs WHERE trace=?", (trace,))[0][
                "status"
            ],
            "completed",
        )
        self.assertEqual(
            p.store.rows("SELECT prompt FROM jobs WHERE trace='old'")[0]["prompt"],
            "Original request",
        )

    async def test_mid_request_guidance_restarts_only_the_model(self):
        from astrbot.core.agent.message import Message
        from astrbot_plugin_qq_chat_workbench.guidance import (
            GUIDANCE_MARKER,
            ToolLoopAgentRunner,
        )

        p = self.plugin
        chat = p.store.chat("qq", "999", "private", "200")
        message_id = p.store.message(
            chat, "live", "in", "user", "User words", "received", "123", sender_id="200"
        )
        p.store.execute(
            "INSERT INTO jobs(trace,chat,prompt,status,source_message_id,sender_id) VALUES('live',?,'User words','running',?,'200')",
            (chat, message_id),
        )
        p.store.execute("INSERT INTO ai_traces VALUES('live')")
        started = asyncio.Event()
        cancelled_model = asyncio.Event()
        calls = []
        runner = SimpleNamespace(
            streaming=True,
            done=lambda: False,
            run_context=SimpleNamespace(
                messages=[
                    Message(role="system", content="Existing persona"),
                    Message(role="user", content="User words"),
                    Message(
                        role="tool",
                        content="Completed tool result",
                        tool_call_id="tool-1",
                    ),
                ]
            ),
        )

        async def model(current, **kwargs):
            calls.append([m.content for m in current.run_context.messages])
            if len(calls) == 1:
                started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled_model.set()
                    raise
            yield SimpleNamespace(completion_text="Guided answer", is_chunk=False)

        with patch.object(ToolLoopAgentRunner, "_iter_llm_responses", model):
            p.initialize_guidance()
            p.live_events["live"] = SimpleNamespace(set_extra=lambda *args: None)

            async def collect():
                token = trace_context.set("live")
                try:
                    return [
                        r.completion_text
                        async for r in ToolLoopAgentRunner._iter_llm_responses(runner)
                    ]
                finally:
                    trace_context.reset(token)

            task = asyncio.create_task(collect())
            p.tasks["live"] = task
            await asyncio.wait_for(started.wait(), 3)
            body = {
                "chat": chat,
                "trace": "live",
                "message_id": message_id,
                "reference_kind": "message",
                "mode": "append",
                "guidance": "Administrator words",
                "reply_mode": "confirm",
                "request_id": str(uuid.uuid4()),
            }
            status, result = await self.post("guide", body)
            self.assertEqual(status, 200)
            self.assertEqual(result["trace"], "live")
            self.assertEqual(await asyncio.wait_for(task, 3), ["Guided answer"])
            self.assertTrue(cancelled_model.is_set())
            self.assertFalse(task.cancelled())
            self.assertEqual(len(calls), 2)
            self.assertEqual(calls[1][0], "Existing persona")
            self.assertIn("Completed tool result", calls[1])
            note = next(
                m
                for m in runner.run_context.messages
                if isinstance(m.content, str) and m.content.startswith(GUIDANCE_MARKER)
            )
            self.assertEqual(note.role, "system")
            self.assertTrue(note._no_save)
            self.assertIn('"guidance": "Administrator words"', note.content)
            self.assertIn('"search": "User words"', note.content)
            self.assertEqual(
                p.store.rows("SELECT prompt FROM jobs WHERE trace='live'")[0]["prompt"],
                "User words",
            )
            self.assertEqual(len(p.store.rows("SELECT * FROM jobs")), 1)
            ToolLoopAgentRunner._iter_llm_responses = model

    async def test_guided_pipeline_runs_tools_and_saves_only_conversation(self):
        import copy

        from aiocqhttp import Event as OneBotEvent
        from astrbot.core.agent.hooks import BaseAgentRunHooks
        from astrbot.core.agent.run_context import ContextWrapper
        from astrbot.core.agent.tool import FunctionTool, ToolSet
        from astrbot.core.astr_main_agent import _get_session_conv
        from astrbot.core.config import AstrBotConfig
        from astrbot.core.config.default import DEFAULT_CONFIG
        from astrbot.core.conversation_mgr import ConversationManager
        from astrbot.core.db.sqlite import SQLiteDatabase
        from astrbot.core.message.message_event_result import MessageChain
        from astrbot.core.pipeline.scheduler import PipelineScheduler
        from astrbot.core.platform.platform_metadata import PlatformMetadata
        from astrbot.core.provider.entities import LLMResponse, ProviderRequest
        from astrbot.core.utils.shared_preferences import SharedPreferences
        from astrbot_plugin_qq_chat_workbench.guidance import (
            GUIDANCE_MARKER,
            NO_USER_MESSAGE,
            ToolLoopAgentRunner,
        )
        from mcp.types import CallToolResult, TextContent

        p = self.plugin
        p.initialize_guidance()
        p.original_execute = PipelineScheduler.execute
        config = AstrBotConfig(
            str(Path(self.temp.name) / "guided-config.json"), default_config={}
        )
        config.update(copy.deepcopy(DEFAULT_CONFIG))
        config["platform_settings"]["segmented_reply"]["enable"] = False
        config["platform_settings"]["reply_prefix"] = ""
        config["platform_settings"]["reply_with_quote"] = True
        config["platform_settings"]["unique_session"] = True
        config["t2i"] = False
        config["provider_tts_settings"]["enable"] = False
        meta = PlatformMetadata("aiocqhttp", "test", "qq")
        db_path = Path(self.temp.name) / "actual-conversations.db"
        db = SQLiteDatabase(str(db_path))
        self.addAsyncCleanup(db.engine.dispose)
        await db.initialize()
        db.inited = True
        preferences = SharedPreferences(db, str(Path(self.temp.name) / "prefs.json"))
        self.addAsyncCleanup(preferences.close)
        await preferences.initialize()
        preference_patch = patch("astrbot.core.conversation_mgr.sp", preferences)
        preference_patch.start()
        self.addCleanup(preference_patch.stop)
        manager = ConversationManager(db)
        saved = AsyncMock(wraps=manager.update_conversation)
        manager.update_conversation = saved
        p.context = SimpleNamespace(
            get_platform_inst=lambda _: SimpleNamespace(meta=lambda: meta),
            get_config=lambda umo=None: config,
            conversation_manager=manager,
            get_using_tts_provider_async=AsyncMock(return_value=None),
        )
        client = p.clients["qq"]
        client.send_private_msg = lambda **params: client.call_action(
            "send_private_msg", **params
        )
        client.send_group_msg = lambda **params: client.call_action(
            "send_group_msg", **params
        )
        tool_calls = []

        class Executor:
            async def execute(self, tool, run_context, **kwargs):
                tool_calls.append((tool.name, kwargs))
                yield CallToolResult(
                    content=[TextContent(type="text", text="Tool evidence")]
                )

        for kind, target, group in (
            ("private", "200", ""),
            ("temporary", "200", "100"),
            ("group", "100", ""),
        ):
            with self.subTest(kind=kind):
                chat = p.store.chat("qq", "999", kind, target, group, "Test user")
                message_id = p.store.message(
                    chat,
                    "source-" + kind,
                    "in",
                    "Test user",
                    "User question",
                    "received",
                    "123",
                    sender_id="200",
                )
                expected_umo = (
                    "qq:GroupMessage:200_100"
                    if kind == "group"
                    else "qq:FriendMessage:200"
                )
                expected_cid = await manager.new_conversation(
                    expected_umo,
                    "qq",
                    content=[
                        {"role": "user", "content": "Earlier user message"},
                        {"role": "assistant", "content": "Earlier answer"},
                    ],
                    persona_id="existing-persona",
                )
                # A newer conversation must not replace the selected one.
                await manager.new_conversation(
                    expected_umo,
                    "qq",
                    content=[{"role": "user", "content": "Other conversation"}],
                )
                await manager.switch_conversation(expected_umo, expected_cid)
                manager.session_conversations.clear()
                with closing(sqlite3.connect(db_path)) as stored:
                    before_rows = stored.execute(
                        "SELECT conversation_id,content FROM conversations ORDER BY conversation_id"
                    ).fetchall()
                model_calls = []
                saved.reset_mock()
                self.calls.clear()
                runner = ToolLoopAgentRunner()

                async def text_chat(model_calls=model_calls, **payload):
                    contexts = [message.model_dump() for message in payload["contexts"]]
                    model_calls.append(copy.deepcopy(contexts))
                    self.assertEqual(payload["func_tool"].names(), ["lookup"])
                    self.assertEqual(contexts[0]["content"], "Existing persona")
                    self.assertIn("Internal guidance", json.dumps(contexts))
                    if len(model_calls) == 1:
                        return LLMResponse(
                            role="assistant",
                            tools_call_name=["lookup"],
                            tools_call_ids=["call-1"],
                            tools_call_args=[{"query": "test"}],
                        )
                    self.assertIn("Tool evidence", json.dumps(contexts))
                    return LLMResponse(
                        role="assistant",
                        result_chain=MessageChain().message("Guided answer"),
                    )

                provider = SimpleNamespace(
                    provider_config={
                        "id": "fake",
                        "api_base": "",
                        "max_context_tokens": 0,
                    },
                    text_chat=text_chat,
                    get_model=lambda: "fake",
                    meta=lambda: SimpleNamespace(type="fake"),
                )

                async def build(
                    *,
                    event,
                    plugin_context,
                    config,
                    apply_reset,
                    prepared_images=None,
                    runner=runner,
                    provider=provider,
                    expected_umo=expected_umo,
                    expected_cid=expected_cid,
                ):
                    self.assertIs(plugin_context, p.context)
                    self.assertFalse(apply_reset)
                    self.assertIn(prepared_images, (None, {}))
                    self.assertIsInstance(event.message_obj.raw_message, OneBotEvent)
                    self.assertEqual(event.get_sender_id(), "200")
                    self.assertFalse(config.streaming_response)
                    self.assertEqual(event.unified_msg_origin, expected_umo)
                    conversation = await _get_session_conv(event, plugin_context)
                    self.assertEqual(conversation.cid, expected_cid)
                    self.assertEqual(conversation.persona_id, "existing-persona")
                    request = ProviderRequest(
                        prompt=event.message_str,
                        system_prompt="Existing persona",
                        contexts=json.loads(conversation.history),
                        func_tool=ToolSet(
                            tools=[
                                FunctionTool(
                                    name="lookup",
                                    description="Read test evidence",
                                    parameters={
                                        "type": "object",
                                        "properties": {"query": {"type": "string"}},
                                    },
                                )
                            ]
                        ),
                    )
                    request.conversation = conversation
                    return SimpleNamespace(
                        agent_runner=runner,
                        provider=provider,
                        provider_request=request,
                        reset_coro=runner.reset(
                            provider=provider,
                            request=request,
                            run_context=ContextWrapper(
                                context=SimpleNamespace(event=event)
                            ),
                            tool_executor=Executor(),
                            agent_hooks=BaseAgentRunHooks(),
                            streaming=False,
                        ),
                    )

                internal = "astrbot.core.pipeline.process_stage.method.agent_sub_stages.internal"
                with (
                    patch.object(
                        manager,
                        "new_conversation",
                        side_effect=AssertionError(
                            "Guidance must reuse the selected conversation"
                        ),
                    ),
                    patch(internal + ".build_main_agent", build),
                    patch(internal + ".call_event_hook", AsyncMock(return_value=False)),
                    patch(internal + "._record_internal_agent_stats", AsyncMock()),
                    patch(
                        "astrbot.core.star.star_handler.star_handlers_registry.get_handlers_by_event_type",
                        return_value=[],
                    ),
                    patch("astrbot.core.utils.metrics.Metric.upload", AsyncMock()),
                    patch(
                        "astrbot.core.star.session_llm_manager.SessionServiceManager.should_process_llm_request",
                        AsyncMock(return_value=True),
                    ),
                ):
                    status, result = await self.post(
                        "guide",
                        {
                            "chat": chat,
                            "message_id": message_id if kind != "private" else None,
                            "guidance": "Internal guidance",
                            "mode": "new",
                            "reference_kind": "message"
                            if kind != "private"
                            else "user",
                            "reply_mode": "confirm",
                            "request_id": str(uuid.uuid4()),
                        },
                    )
                    self.assertEqual(status, 200)
                    trace = result["trace"]
                    task = p.tasks[trace]
                    for _ in range(100):
                        if (
                            p.store.rows(
                                "SELECT id FROM pending_sends WHERE trace=?", (trace,)
                            )
                            or task.done()
                        ):
                            break
                        await asyncio.sleep(0.01)
                    review = await self.get("review", {"trace": trace})
                    self.assertEqual(len(model_calls), 2, str(p.queue))
                    self.assertEqual(len(review["items"]), 1, str(p.queue))
                    self.assertIn("Guided answer", json.dumps(review))
                    self.assertFalse(self.calls)
                    saved.assert_not_awaited()
                    await self.post(
                        "approve", {"trace": trace, "ids": [review["items"][0]["id"]]}
                    )
                    await asyncio.wait_for(task, 3)
                    await asyncio.sleep(0)
                self.assertEqual(len(self.calls), 1)
                params = self.calls[0][1]
                self.assertEqual(params["self_id"], 999)
                if kind != "private":
                    self.assertEqual(params["group_id"], 100)
                if kind != "group":
                    self.assertEqual(params["user_id"], 200)
                if kind == "private":
                    self.assertFalse(
                        any(segment["type"] == "reply" for segment in params["message"])
                    )
                saved.assert_awaited_once()
                self.assertEqual(saved.call_args.args[:2], (expected_umo, expected_cid))
                persisted = await manager.get_conversation(expected_umo, expected_cid)
                self.assertEqual(persisted.persona_id, "existing-persona")
                self.assertEqual(
                    await manager.get_curr_conversation_id(expected_umo), expected_cid
                )
                with closing(sqlite3.connect(db_path)) as stored:
                    after_rows = stored.execute(
                        "SELECT conversation_id,content FROM conversations ORDER BY conversation_id"
                    ).fetchall()
                self.assertEqual(len(after_rows), len(before_rows))
                self.assertEqual(
                    [row for row in before_rows if row[0] != expected_cid],
                    [row for row in after_rows if row[0] != expected_cid],
                )
                history = persisted.history
                self.assertIn("Earlier user message", history)
                self.assertIn("Guided answer", history)
                self.assertIn("Tool evidence", history)
                self.assertNotIn(GUIDANCE_MARKER, history)
                self.assertNotIn("Internal guidance", history)
                # Saved history must never name the administrator, including
                # the stand-in turn of a task started for a user, not a message.
                turns = json.loads(history)
                self.assertNotIn("管理员", json.dumps(turns, ensure_ascii=False))
                if kind == "private":
                    self.assertEqual(turns[2]["role"], "user")
                    self.assertTrue(NO_USER_MESSAGE.strip())
                    # The stand-in says the bot speaks first and names no
                    # administrator, back end, task or guidance.
                    self.assertIn("主动", NO_USER_MESSAGE)
                    for word in ("管理", "后台", "任务", "引导", "系统"):
                        self.assertNotIn(word, NO_USER_MESSAGE)
                    self.assertIn(
                        NO_USER_MESSAGE, json.dumps(turns[2], ensure_ascii=False)
                    )
                    self.assertIn(
                        NO_USER_MESSAGE,
                        json.dumps(model_calls[0][-1], ensure_ascii=False),
                    )
        self.assertEqual(len(tool_calls), 3)

    def test_review_includes_forwarded_content_and_full_text(self):
        from astrbot_plugin_qq_chat_workbench.guidance import review_segments

        content = "Long reply " * 2000
        preview = review_segments(
            [
                {
                    "type": "node",
                    "data": {
                        "name": "Bot",
                        "uin": "999",
                        "content": [
                            {"type": "text", "data": {"text": content}},
                            {
                                "type": "image",
                                "data": {"file": "base64://not-an-image"},
                            },
                        ],
                    },
                }
            ]
        )
        self.assertIn("Bot", preview[0]["data"]["text"])
        self.assertEqual(preview[1]["data"]["text"], content)
        self.assertEqual(preview[2]["data"]["file"], "[image payload]")

    async def test_guided_auto_routes_and_review_discard(self):
        p = self.plugin
        for index, (kind, target, group) in enumerate(
            [("group", "100", ""), ("private", "200", ""), ("temporary", "200", "100")]
        ):
            chat = p.store.chat("qq", "999", kind, target, group)
            trace = f"automatic-{index}"
            p.store.execute(
                "INSERT INTO jobs(trace,chat,status,reply_mode) VALUES(?,?,'running','auto')",
                (trace, chat),
            )
            token = trace_context.set(trace)
            try:
                params = {"self_id": 999, "message": "Generated"}
                params["group_id" if kind == "group" else "user_id"] = int(target)
                if group:
                    params["group_id"] = int(group)
                await p.clients["qq"].call_action(
                    "send_group_msg" if kind == "group" else "send_private_msg",
                    **params,
                )
                with self.assertRaises(ValueError):
                    await p.clients["qq"].call_action(
                        "send_private_msg",
                        self_id=888,
                        user_id=201,
                        message="Wrong target",
                    )
            finally:
                trace_context.reset(token)
        self.assertEqual(len(self.calls), 3)
        self.assertFalse(p.store.rows("SELECT * FROM pending_sends"))
        p.store.execute(
            "UPDATE jobs SET reply_mode='confirm' WHERE trace='automatic-2'"
        )

        async def pending():
            token = trace_context.set("automatic-2")
            try:
                await p.clients["qq"].call_action(
                    "send_private_msg",
                    self_id=999,
                    user_id=200,
                    group_id=100,
                    message="Do not send",
                )
            finally:
                trace_context.reset(token)

        task = asyncio.create_task(pending())
        p.tasks["automatic-2"] = task
        async with asyncio.timeout(10):
            while "automatic-2" not in p.review_waiter_traces.values():
                await asyncio.sleep(0.005)
        self.assertEqual(
            p.store.rows("SELECT status FROM pending_sends")[0]["status"], "pending"
        )
        self.assertEqual((await self.post("discard", {"trace": "automatic-2"}))[0], 200)
        self.assertTrue(task.cancelled())
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(
            p.store.rows("SELECT status,payload FROM pending_sends")[0],
            {"status": "discarded", "payload": ""},
        )

    async def test_review_survives_restart_and_rejects_unseen_content(self):
        p = self.plugin
        first_started = asyncio.Event()
        release_first = asyncio.Event()

        async def send_in_order(action, **params):
            self.calls.append((action, params))
            if params["message"] == "First":
                first_started.set()
                await release_first.wait()
            return {"message_id": 42}

        p.clients["qq"].call_action = p.wrap_sender(send_in_order, "qq")
        chat = p.store.chat("qq", "999", "private", "200")
        p.store.execute(
            "INSERT INTO jobs(trace,chat,status,reply_mode,started) VALUES('recover',?,'interrupted','confirm',1)",
            (chat,),
        )
        p.store.execute("INSERT INTO ai_traces VALUES('recover')")
        ids = []
        for text in ("First", "Second"):
            ids.append(
                p.store.execute(
                    "INSERT INTO pending_sends(trace,platform,action,payload,preview,status,created) VALUES('recover','qq','send_private_msg',?,?,'pending',1)",
                    (
                        json.dumps({"self_id": 999, "user_id": 200, "message": text}),
                        json.dumps([{"type": "text", "data": {"text": text}}]),
                    ),
                )
            )
        drain_prune(p.store)
        rows = await self.get("jobs", {})
        self.assertEqual(rows[0]["status"], "awaiting_approval")
        self.assertEqual(
            (await self.post("approve", {"trace": "recover", "ids": ids[:1]}))[0], 400
        )
        self.assertFalse(self.calls)
        self.assertEqual(
            (await self.post("approve", {"trace": "recover", "ids": ids}))[0], 200
        )
        await asyncio.wait_for(first_started.wait(), 3)
        await asyncio.sleep(0)
        self.assertEqual(len(self.calls), 1)
        release_first.set()
        await asyncio.wait_for(p.tasks["recover"], 3)
        await self.post("approve", {"trace": "recover", "ids": ids})
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(
            [
                r["status"]
                for r in p.store.rows("SELECT status FROM pending_sends ORDER BY id")
            ],
            ["accepted", "accepted"],
        )

    async def test_direct_quote_is_scoped_to_user_message(self):
        p = self.plugin
        chat = p.store.chat("qq", "999", "private", "200")
        other = p.store.chat("qq", "888", "private", "200")
        source = p.store.message(
            chat,
            "source",
            "in",
            "user",
            "Original",
            "received",
            "-123",
            sender_id="200",
        )
        body = {
            "chat": other,
            "text": "Reply",
            "reference_id": source,
            "reference_kind": "message",
            "request_id": str(uuid.uuid4()),
        }
        self.assertEqual((await self.post("send", body))[0], 400)
        self.assertEqual(self.calls, [])
        body["chat"] = chat
        self.assertEqual((await self.post("send", body))[0], 200)
        self.assertEqual(
            self.calls[0][1]["message"][0], {"type": "reply", "data": {"id": "-123"}}
        )
        self.assertEqual(self.calls[0][1]["message"][1]["data"]["text"], "Reply")

    async def test_images_validation_and_unknown_send(self):
        chat = self.plugin.store.chat("qq", "999", "private", "200")
        body = {
            "chat": chat,
            "text": "",
            "image": "bad",
            "request_id": str(uuid.uuid4()),
        }
        self.assertEqual((await self.post("send", body))[0], 400)
        image = io.BytesIO()
        Image.new("RGB", (8, 8)).save(image, format="PNG")
        body["image"] = base64.b64encode(image.getvalue()).decode()
        self.assertEqual((await self.post("send", body))[0], 200)
        self.assertTrue(
            self.calls[-1][1]["message"][0]["data"]["file"].startswith("base64://")
        )

        async def broken(action, **params):
            raise TimeoutError("timeout")

        self.plugin.clients["qq"].call_action = self.plugin.wrap_sender(broken, "qq")
        body["request_id"] = str(uuid.uuid4())
        self.assertEqual((await self.post("send", body))[0], 400)
        self.assertEqual(
            self.plugin.store.rows(
                "SELECT status FROM messages ORDER BY id DESC LIMIT 1"
            )[0]["status"],
            "unknown",
        )

    def test_redaction(self):
        value = redact(
            'Authorization: Bearer secret-token api_key="secret-123" password=test sk-abcdefghijklmnopqrst'
        )
        for secret in [
            "secret-token",
            "secret-123",
            "password=test",
            "sk-abcdefghijklmnopqrst",
        ]:
            self.assertNotIn(secret, value)

    def test_progress_migration_preserves_existing_jobs(self):
        path = Path(self.temp.name) / "legacy.db"
        with closing(sqlite3.connect(path)) as db, db:
            db.execute(
                "CREATE TABLE jobs (trace TEXT PRIMARY KEY, chat TEXT, umo TEXT, prompt TEXT, status TEXT, started REAL, finished REAL, draft TEXT DEFAULT '')"
            )
            db.execute(
                "INSERT INTO jobs(trace,prompt,status) VALUES('old','keep me','completed')"
            )
        for _ in range(2):
            store = Store(path)
            try:
                row = store.rows("SELECT * FROM jobs")[0]
                self.assertEqual(row["prompt"], "keep me")
                self.assertEqual(row["step"], "")
                self.assertIsNone(row["step_started"])
            finally:
                store.db.close()

    async def test_ai_progress_hooks_and_cancellation(self):
        p = self.plugin
        p.store.execute(
            "INSERT INTO jobs(trace,prompt,status,started) VALUES('steps','','running',1)"
        )
        token = trace_context.set("steps")
        tool = SimpleNamespace(name="search_web")
        try:
            await p.mark_decorating(None)
            self.assertEqual(p.store.rows("SELECT step FROM jobs")[0]["step"], "")
            await p.mark_ai(None, None)
            model_started = p.store.rows("SELECT step_started FROM jobs")[0][
                "step_started"
            ]
            await p.mark_ai(None, None)
            self.assertEqual(
                p.store.rows("SELECT step_started FROM jobs")[0]["step_started"],
                model_started,
            )
            await p.mark_tool_start(None, tool, {"secret": "private argument"})
            job = json.loads((await p.jobs()).body)[0]
            self.assertEqual((job["step"], job["step_detail"]), ("tool", "search_web"))
            await p.mark_tool_end(None, tool, None, None)
            self.assertEqual(p.store.rows("SELECT step FROM jobs")[0]["step"], "model")
            await p.mark_model_response(None, None)
            await p.mark_decorating(None)
            self.assertEqual(
                p.store.rows("SELECT step FROM jobs")[0]["step"], "decorating"
            )
            self.assertNotIn("private argument", str(p.queue))
        finally:
            trace_context.reset(token)
        await p.stop_trace("steps")
        (await p.set_step("model", trace="steps"))
        job = json.loads((await p.jobs()).body)[0]
        self.assertEqual((job["status"], job["step"]), ("cancelled", "decorating"))

    async def test_send_progress_restores_tool_and_preserves_newer_phase(self):
        p = self.plugin
        p.store.execute("INSERT INTO jobs(trace,status) VALUES('send-steps','running')")
        token = trace_context.set("send-steps")
        try:
            await p.mark_ai(None, None)
            await p.mark_tool_start(None, SimpleNamespace(name="send_message"), None)

            async def original(action, **params):
                row = p.store.rows("SELECT step FROM jobs")[0]
                self.assertEqual(row["step"], "sending")
                return {"message_id": 1}

            await p.wrap_sender(original, "qq")(
                "send_private_msg", user_id=200, self_id=999, message="fake"
            )
            self.assertEqual(p.store.rows("SELECT step FROM jobs")[0]["step"], "tool")

            async def progressing(action, **params):
                await p.mark_model_response(None, None)
                return {"message_id": 2}

            await p.wrap_sender(progressing, "qq")(
                "send_private_msg", user_id=200, self_id=999, message="fake"
            )
            self.assertEqual(
                p.store.rows("SELECT step FROM jobs")[0]["step"], "response"
            )
            await p.wrap_sender(original, "qq")(
                "send_private_msg", user_id=200, self_id=999, message="fake"
            )
            self.assertEqual(p.store.rows("SELECT step FROM jobs")[0]["step"], "sent")
        finally:
            trace_context.reset(token)

    async def test_progress_observer_failure_does_not_block_hooks(self):
        p = self.plugin
        token = trace_context.set("broken-observer")
        try:
            with patch.object(
                p.store, "record_step", side_effect=sqlite3.OperationalError("locked")
            ):
                await p.mark_tool_start(None, SimpleNamespace(name="search"), None)
            self.assertEqual(p.dropped, 1)
        finally:
            trace_context.reset(token)

    async def test_recall_is_scoped_to_outgoing(self):
        chat = self.plugin.store.chat("qq", "999", "private", "200")
        incoming = self.plugin.store.message(
            chat, "t", "in", "user", "hello", "received", "1"
        )
        self.assertEqual((await self.post("recall", {"id": incoming}))[0], 400)
        outgoing = self.plugin.store.message(
            chat, "t", "out", "bot", "hi", "accepted", "2"
        )
        self.assertEqual((await self.post("recall", {"id": outgoing}))[0], 200)
        self.assertEqual(
            self.calls[-1], ("delete_msg", {"message_id": "2", "self_id": 999})
        )
        self.assertEqual(
            self.plugin.store.rows(
                "SELECT status FROM messages WHERE id=?", (outgoing,)
            )[0]["status"],
            "recalled",
        )
        await self.post("recall", {"id": outgoing})
        self.assertEqual(len(self.calls), 1)

    async def runner_for_test(self, event, *, provider=None, request=None):
        from astrbot.core.agent.hooks import BaseAgentRunHooks
        from astrbot.core.agent.run_context import ContextWrapper
        from astrbot.core.agent.tool import ToolSet
        from astrbot.core.provider.entities import ProviderRequest
        from astrbot_plugin_qq_chat_workbench.guidance import ToolLoopAgentRunner

        runner = ToolLoopAgentRunner()
        await runner.reset(
            provider=provider
            or SimpleNamespace(
                provider_config={"id": "fake", "max_context_tokens": 0},
                get_model=lambda: "fake",
            ),
            request=request
            or ProviderRequest(prompt="Test reply", func_tool=ToolSet()),
            run_context=ContextWrapper(context=SimpleNamespace(event=event)),
            tool_executor=SimpleNamespace(),
            agent_hooks=BaseAgentRunHooks(),
            streaming=False,
        )
        return runner

    async def test_group_guidance_uses_current_isolation_and_exact_member(self):
        from astrbot.core.platform.platform_metadata import PlatformMetadata

        p = self.plugin
        for unique in (True, False):
            for platform_id, account in (("qq", "999"), ("other-qq", "998")):
                for sender in ("200", "201"):
                    with self.subTest(
                        unique=unique, platform=platform_id, sender=sender
                    ):
                        meta = PlatformMetadata("aiocqhttp", "test", platform_id)
                        config = {"platform_settings": {"unique_session": unique}}
                        p.context = SimpleNamespace(
                            get_platform_inst=lambda _, meta=meta: SimpleNamespace(
                                meta=lambda: meta
                            ),
                            get_config=Mock(return_value=config),
                        )
                        p.clients[platform_id] = p.clients["qq"]
                        chat_id = p.store.chat(
                            platform_id, account, "group", "100", "", "Group"
                        )
                        chat = p.store.rows(
                            "SELECT * FROM chats WHERE id=?", (chat_id,)
                        )[0]
                        message = {
                            "sender_id": sender,
                            "sender": "Member",
                            "remote_id": "123",
                            "search": "Original words",
                            "body": "[]",
                        }
                        expected = f"{platform_id}:GroupMessage:{sender + '_' if unique else ''}100"
                        # Both ingress-only and historical isolated UMOs must use
                        # today's normal-message rule, never another member's ID.
                        for previous in ("100", "202_100", sender + "_100"):
                            event = await p.make_guided_event(
                                chat,
                                message,
                                f"{platform_id}:GroupMessage:{previous}",
                                "message",
                            )
                            self.assertEqual(event.unified_msg_origin, expected)
                            self.assertIs(event.get_extra("_session_isolated"), unique)
                            self.assertEqual(event.get_sender_id(), sender)
                            self.assertEqual(event.get_self_id(), account)
                            self.assertEqual(
                                event.message_obj.raw_message["group_id"], 100
                            )
                        self.assertEqual(
                            [
                                call.args[0]
                                for call in p.context.get_config.call_args_list
                            ],
                            [f"{platform_id}:GroupMessage:100"] * 3,
                        )

    async def test_normal_trace_records_the_resolved_session(self):
        p = self.plugin
        p.store.execute(
            "INSERT INTO jobs(trace,umo,status) VALUES('isolated','qq:GroupMessage:100','running')"
        )
        event = SimpleNamespace(unified_msg_origin="qq:GroupMessage:200_100")
        token = trace_context.set("isolated")
        try:
            await p.mark_ai(event, None)
        finally:
            trace_context.reset(token)
        self.assertEqual(
            p.store.rows("SELECT umo FROM jobs WHERE trace='isolated'"),
            [{"umo": "qq:GroupMessage:200_100"}],
        )

    async def test_cooperative_stop_preserves_history_write_and_shared_worker(self):
        from astrbot.core.db.sqlite import SQLiteDatabase
        from astrbot.core.message.message_event_result import MessageChain
        from astrbot.core.pipeline.context_utils import call_event_hook
        from astrbot.core.platform_message_history_mgr import (
            PlatformMessageHistoryManager,
        )
        from astrbot.core.star.star_handler import EventType

        p = self.plugin
        path = Path(self.temp.name) / "history-write.db"
        db = SQLiteDatabase(str(path))
        self.addAsyncCleanup(db.engine.dispose)
        await db.initialize()
        db.inited = True
        history = PlatformMessageHistoryManager(db)
        writer_started = asyncio.Event()
        history_saved = asyncio.Event()
        shared_release = asyncio.Event()
        event = SimpleNamespace(
            unified_msg_origin="qq:GroupMessage:200_100",
            plugins_name=None,
            trace=SimpleNamespace(record=Mock()),
            set_extra=Mock(),
            stop_event=Mock(),
        )
        event.is_stopped = lambda: event.stop_event.called
        runner = await self.runner_for_test(event)
        trace = "history-cleanup"
        p.live_events[trace] = event
        p.guidance_states[trace] = {"runner": runner}
        p.store.execute("INSERT INTO jobs(trace,status) VALUES(?,'running')", (trace,))

        async def persist(current, response):
            writer_started.set()
            await history.insert_message_chain(
                "qq",
                current.unified_msg_origin,
                MessageChain().message("Completed history"),
                "bot",
                sender_id="999",
            )
            history_saved.set()

        async def pipeline():
            await call_event_hook(event, EventType.OnLLMResponseEvent, None)
            if not event.is_stopped():
                await p.clients["qq"].call_action(
                    "send_group_msg", self_id=999, group_id=100, message="Must not send"
                )

        handler = SimpleNamespace(
            handler=persist,
            handler_module_path="test-history-hook",
            handler_name="persist",
            handler_full_name="test-history-hook.persist",
            event_type=EventType.OnLLMResponseEvent,
        )
        # Force the same aiosqlite await seen in the production traceback.
        locker = sqlite3.connect(path)
        locker.execute("BEGIN IMMEDIATE")
        try:
            with (
                patch(
                    "astrbot.core.pipeline.context_utils.star_handlers_registry.get_handlers_by_event_type",
                    return_value=[handler],
                ),
                patch.dict(
                    "astrbot.core.pipeline.context_utils.star_map",
                    {
                        "test-history-hook": SimpleNamespace(
                            name="test-history-hook", display_name="History hook"
                        )
                    },
                ),
                patch(
                    "astrbot.core.pipeline.context_utils.logger.error"
                ) as logged_error,
            ):
                token = trace_context.set(trace)
                task = asyncio.create_task(pipeline())
                shared = asyncio.create_task(shared_release.wait())
                trace_context.reset(token)
                p.tasks[trace] = task
                p.tasks["shared-maintenance"] = shared
                await asyncio.wait_for(writer_started.wait(), 2)
                stopping = asyncio.create_task(p.stop_trace(trace))
                p.tasks["admin-stop"] = stopping
                await asyncio.sleep(0.05)
                self.assertTrue(event.is_stopped())
                self.assertTrue(runner._is_stop_requested())
                self.assertFalse(task.done())
                self.assertEqual(task.cancelling(), 0)
                self.assertFalse(history_saved.is_set())
                locker.rollback()
                self.assertTrue(await asyncio.wait_for(stopping, 3))
                self.assertTrue(history_saved.is_set())
                self.assertFalse(task.cancelled())
                self.assertFalse(shared.done())
                logged_error.assert_not_called()
                event.set_extra.assert_called_with("agent_stop_requested", True)
                self.assertEqual(self.calls, [])
                self.assertEqual(
                    p.store.rows("SELECT status FROM jobs WHERE trace=?", (trace,)),
                    [{"status": "cancelled"}],
                )
        finally:
            locker.rollback()
            locker.close()
            shared_release.set()
        with closing(sqlite3.connect(path)) as stored:
            rows = stored.execute(
                "SELECT user_id,content FROM platform_message_history"
            ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], event.unified_msg_origin)
        self.assertIn("Completed history", rows[0][1])
        self.assertFalse(any(level == "ERROR" for _, level, _, _ in p.queue))

    async def test_cooperative_stop_aborts_model_but_keeps_completed_tool_results(self):
        from astrbot.core.agent.tool import ToolSet
        from astrbot.core.provider.entities import ProviderRequest

        p = self.plugin
        p.initialize_guidance()
        trace = "provider-stop"
        started = asyncio.Event()
        provider_cancelled = asyncio.Event()
        other_release = asyncio.Event()
        event = SimpleNamespace(set_extra=Mock(), stop_event=Mock())
        p.live_events[trace] = event
        p.store.execute("INSERT INTO jobs(trace,status) VALUES(?,'running')", (trace,))

        async def text_chat(**kwargs):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                provider_cancelled.set()
                raise

        provider = SimpleNamespace(
            provider_config={"id": "fake", "max_context_tokens": 0},
            text_chat=text_chat,
            get_model=lambda: "fake",
        )
        request = ProviderRequest(
            prompt="Continue the same task",
            contexts=[
                {"role": "user", "content": "Earlier conversation"},
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "done-1",
                            "type": "function",
                            "function": {"name": "lookup", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "done-1",
                    "content": "Already completed evidence",
                },
            ],
            func_tool=ToolSet(),
        )
        runner = await self.runner_for_test(event, provider=provider, request=request)

        async def run():
            while not runner.done():
                async for _ in runner.step():
                    pass

        token = trace_context.set(trace)
        task = asyncio.create_task(run())
        trace_context.reset(token)
        other = asyncio.create_task(other_release.wait())
        p.tasks[trace] = task
        p.tasks["independent"] = other
        try:
            await asyncio.wait_for(started.wait(), 2)
            self.assertTrue(await p.stop_trace(trace))
            self.assertTrue(provider_cancelled.is_set())
            self.assertTrue(runner.was_aborted())
            self.assertFalse(task.cancelled())
            self.assertFalse(other.done())
            history = json.dumps(
                [message.model_dump() for message in runner.run_context.messages]
            )
            self.assertIn("Earlier conversation", history)
            self.assertIn("Already completed evidence", history)
            self.assertEqual(self.calls, [])
            token = trace_context.set(trace)
            try:
                with self.assertRaises(asyncio.CancelledError):
                    await p.clients["qq"].call_action(
                        "send_group_msg", group_id=100, message="blocked"
                    )
            finally:
                trace_context.reset(token)
        finally:
            other_release.set()

    async def test_slow_cooperative_cleanup_remains_stopping_without_forced_cancel(
        self,
    ):
        p = self.plugin
        trace = "slow-cleanup"
        release = asyncio.Event()
        event = SimpleNamespace(set_extra=Mock(), stop_event=Mock())
        runner = SimpleNamespace(request_stop=Mock(), done=lambda: False)
        p.live_events[trace] = event
        p.guidance_states[trace] = {"runner": runner}
        p.store.execute("INSERT INTO jobs(trace,status) VALUES(?,'running')", (trace,))
        p.store.execute("INSERT INTO ai_traces VALUES(?)", (trace,))
        task = asyncio.create_task(release.wait())
        p.tasks[trace] = task
        try:
            self.assertFalse(await p.stop_trace(trace))
            self.assertEqual(task.cancelling(), 0)
            self.assertFalse(task.done())
            self.assertEqual(
                p.store.rows("SELECT status FROM jobs WHERE trace=?", (trace,)),
                [{"status": "stopping"}],
            )
        finally:
            release.set()
        await task
        # The production pipeline removes its owner entry in its finally block.
        p.tasks.pop(trace)
        rows = json.loads((await p.jobs()).body)
        self.assertEqual(rows[0]["status"], "cancelled")
        runner.request_stop.assert_called_once()

    async def test_cancelled_guidance_cannot_start_or_cancel_a_fresh_task(self):
        import copy

        from astrbot.core.config.default import DEFAULT_CONFIG

        p = self.plugin
        p.original_execute = AsyncMock()
        p.context = SimpleNamespace(
            get_config=lambda umo=None: copy.deepcopy(DEFAULT_CONFIG),
            conversation_manager=SimpleNamespace(),
        )
        event = SimpleNamespace(
            unified_msg_origin="qq:FriendMessage:200",
            get_extra=lambda key: None,
            get_group_id=lambda: "",
        )
        for trace in ("old-cancelled", "fresh-guidance"):
            p.store.execute(
                "INSERT INTO jobs(trace,status) VALUES(?,'running')", (trace,)
            )
        p.cancelled.add("old-cancelled")
        with self.assertRaises(asyncio.CancelledError):
            await p.run_guided_task("old-cancelled", event)
        p.original_execute.assert_not_awaited()
        await p.run_guided_task("fresh-guidance", event)
        p.original_execute.assert_awaited_once()
        self.assertEqual(
            p.store.rows("SELECT status FROM jobs WHERE trace='fresh-guidance'"),
            [{"status": "completed"}],
        )
        self.assertEqual(
            p.store.rows("SELECT status FROM jobs WHERE trace='old-cancelled'"),
            [{"status": "cancelled"}],
        )
        self.assertFalse(any(level == "ERROR" for _, level, _, _ in p.queue))

    async def test_cooperative_discard_wakes_only_its_review_waiter(self):

        p = self.plugin
        chat = p.store.chat("qq", "999", "private", "200")
        for trace in ("discarded-review", "unrelated-review"):
            p.store.execute(
                "INSERT INTO jobs(trace,chat,status,reply_mode) VALUES(?,?,'running','confirm')",
                (trace, chat),
            )
            p.live_events[trace] = SimpleNamespace(set_extra=Mock(), stop_event=Mock())
            p.guidance_states[trace] = {
                "runner": await self.runner_for_test(p.live_events[trace])
            }
            token = trace_context.set(trace)
            p.tasks[trace] = asyncio.create_task(
                p.clients["qq"].call_action(
                    "send_private_msg", self_id=999, user_id=200, message=trace
                )
            )
            trace_context.reset(token)
        for _ in range(50):
            if len(p.review_waiters) == 2:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(len(p.review_waiters), 2)
        self.assertEqual(
            (await self.post("discard", {"trace": "discarded-review"}))[0], 200
        )
        self.assertTrue(p.tasks["discarded-review"].cancelled())
        self.assertFalse(p.tasks["unrelated-review"].done())
        rows = p.store.rows(
            "SELECT trace,status,payload FROM pending_sends ORDER BY trace"
        )
        self.assertEqual((rows[0]["status"], rows[0]["payload"]), ("discarded", ""))
        self.assertEqual(rows[1]["status"], "pending")
        self.assertTrue(rows[1]["payload"])
        self.assertEqual(self.calls, [])

    async def test_real_hooks_restore_and_trace(self):
        from astrbot.core.pipeline.scheduler import PipelineScheduler

        p = self.plugin
        p.context = SimpleNamespace(
            platform_manager=SimpleNamespace(platform_insts=[]), registered_web_apis=[]
        )
        p.context.register_web_api = lambda *args: p.context.registered_web_apis.append(
            args
        )
        p.hooks = []
        p.store.db.close()
        original = AsyncMock(return_value="original-result")
        event = SimpleNamespace(get_platform_name=lambda: "other")
        with (
            patch(
                "astrbot_plugin_qq_chat_workbench.main.get_astrbot_data_path",
                return_value=self.temp.name,
            ),
            patch.object(PipelineScheduler, "execute", original),
        ):
            await p.initialize()
            # The settings dialog saves through the same registered route.
            self.assertIn(
                (
                    "/astrbot_plugin_qq_chat_workbench/settings",
                    p.settings,
                    ["GET", "POST"],
                    "QQ 聊天工作台",
                ),
                p.context.registered_web_apis,
            )
            result = await PipelineScheduler.execute(None, event)
            self.assertEqual(result, "original-result")
            original.side_effect = asyncio.CancelledError("requested stop")
            with self.assertRaises(asyncio.CancelledError):
                await PipelineScheduler.execute(None, event)
            self.assertFalse(any(level == "ERROR" for _, level, _, _ in p.queue))
            self.assertTrue(
                any(
                    level == "INFO" and body == "Task cancelled"
                    for _, level, _, body in p.queue
                )
            )
            original.side_effect = RuntimeError("real failure")
            with self.assertRaisesRegex(RuntimeError, "real failure"):
                await PipelineScheduler.execute(None, event)
            self.assertTrue(
                any(
                    level == "ERROR" and "real failure" in body
                    for _, level, _, body in p.queue
                )
            )
            self.assertEqual(trace_context.get(), "")
            await p.terminate()
            self.assertIs(PipelineScheduler.execute, original)
            self.assertEqual(p.context.registered_web_apis, [])


if __name__ == "__main__":
    unittest.main()
