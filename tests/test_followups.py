"""Send outcome, log source, Agent runner, append and reload regressions."""

import asyncio
import copy
import sys
import threading
import time
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import test_console as console_tests
from astrbot_plugin_qq_chat_workbench.store import trace_context
from loguru import logger as loguru_logger


async def until(predicate, timeout=3):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


class FollowupTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = console_tests.ConsoleTests.asyncSetUp
    asyncTearDown = console_tests.ConsoleTests.asyncTearDown
    post = console_tests.ConsoleTests.post

    def bot(self):
        """Install a fake QQ client the way platforms_loaded() does."""
        p = self.plugin
        raw = AsyncMock(return_value={"message_id": 7})
        bot = SimpleNamespace()
        bot.call_action = p.wrap_sender(raw, "qq")
        p.clients["qq"] = bot
        p.hooks = [(bot, raw, bot.call_action)]
        p.context.registered_web_apis = []
        return bot, raw

    async def send_as(self, trace, text):
        token = trace_context.set(trace)
        try:
            return await self.plugin.clients["qq"].call_action(
                "send_private_msg", self_id=999, user_id=200, message=text
            )
        finally:
            trace_context.reset(token)

    async def test_failed_send_report_stays_unknown_and_deduplicated(self):
        from aiocqhttp.exceptions import ActionFailed

        p = self.plugin
        chat = p.store.chat("qq", "999", "private", "200", title="User")
        # aiocqhttp raises for status=failed. SnowLuma reports every handler
        # error that way, including one after the message reached QQ.
        raw = AsyncMock(
            side_effect=ActionFailed({"status": "failed", "retcode": 100, "data": None})
        )
        p.clients["qq"] = SimpleNamespace(call_action=p.wrap_sender(raw, "qq"))
        body = {"chat": chat, "text": "hello", "request_id": str(uuid.uuid4())}
        status, result = await self.post("send", body)
        self.assertEqual(status, 400)
        self.assertIn("结果未知", result["message"])
        self.assertNotIn("retryable", result["data"])
        self.assertEqual(
            [
                row["status"]
                for row in p.store.rows(
                    "SELECT status FROM messages WHERE direction='out'"
                )
            ],
            ["unknown"],
        )
        # Retrying the same request cannot send it a second time.
        self.assertTrue((await self.post("send", body))[1]["duplicate"])
        self.assertEqual(raw.await_count, 1)

    async def capture(self, emit):
        """Pass one log call through the plugin's own loguru sink."""
        p = self.plugin
        p.loop = asyncio.get_running_loop()
        marker = uuid.uuid4().hex
        sink = loguru_logger.add(p.capture_log, level="DEBUG", catch=False)
        try:
            line = emit(marker)
        finally:
            loguru_logger.remove(sink)
        await asyncio.sleep(0)
        rows = [row for row in p.queue if marker in row[3]]
        self.assertEqual(len(rows), 1)
        return rows[0][2], line

    async def test_forwarded_logs_keep_the_caller_location(self):
        from astrbot.core import logger

        def emit(marker):
            line = sys._getframe().f_lineno + 1
            logger.warning("followup %s", marker)
            return line

        source, line = await self.capture(emit)
        self.assertEqual(source, f"tests.test_followups:{line}")

    async def test_plugin_logs_name_the_plugin(self):
        from astrbot.core.log import PLUGIN_LOGGER_PREFIX, LogManager

        plugin_logger = LogManager.GetLogger(PLUGIN_LOGGER_PREFIX + "followup_probe")

        def emit(marker):
            line = sys._getframe().f_lineno + 1
            plugin_logger.warning("followup %s", marker)
            return line

        source, line = await self.capture(emit)
        self.assertEqual(source, f"[followup_probe] tests.test_followups:{line}")

    async def test_records_without_caller_fields_keep_their_own_location(self):
        # A direct loguru call has no forwarded caller; AstrBot's console sink
        # cannot format it either, so feed the plugin sink a bare record.
        from typing import ClassVar

        class Message:
            record: ClassVar[dict] = {
                "level": SimpleNamespace(name="WARNING"),
                "name": "some_plugin.worker",
                "line": 42,
                "extra": {},
            }

            def __str__(self):
                return "bare record"

        p = self.plugin
        p.loop = asyncio.get_running_loop()
        p.capture_log(Message())
        await asyncio.sleep(0)
        self.assertEqual(p.queue[-1][1:3], ("WARNING", "some_plugin.worker:42"))

    def test_runner_type_follows_the_4_29_setting(self):
        from astrbot.core.config.default import DEFAULT_CONFIG
        from astrbot.core.utils.migra_helper import _migrate_agent_runner_config
        from astrbot_plugin_qq_chat_workbench.guidance import runner_type

        self.assertEqual(runner_type(DEFAULT_CONFIG), "local")
        # Loading a pre-4.29 dify configuration moves the setting and drops the
        # old key, which the 0.2.4 check read as "local".
        legacy = {"provider_settings": {"agent_runner_type": "dify"}}
        _migrate_agent_runner_config(legacy, copy.deepcopy(DEFAULT_CONFIG))
        self.assertNotIn("agent_runner_type", legacy["provider_settings"])
        self.assertEqual(runner_type(legacy), "dify")
        self.assertEqual(
            runner_type({"provider_settings": {"agent_runner_type": "coze"}}), "coze"
        )
        self.assertIsNone(runner_type({"agent_runner": {}}))

    async def test_guidance_is_refused_for_a_migrated_third_party_runner(self):
        p = self.plugin
        chat = p.store.chat("qq", "999", "private", "200", title="User")
        migrated = {"provider_settings": {}, "agent_runner": {"runner_type": "dify"}}
        p.context.get_config = lambda umo=None: migrated
        p.make_guided_event = AsyncMock(
            return_value=SimpleNamespace(unified_msg_origin="qq:FriendMessage:200")
        )
        p.store.execute(
            "INSERT INTO jobs(trace,chat,umo,status,started) VALUES('live',?,?,'running',?)",
            (chat, "qq:FriendMessage:200", time.time()),
        )
        p.tasks["live"] = asyncio.create_task(asyncio.Event().wait())
        for extra in ({"mode": "new"}, {"mode": "append", "trace": "live"}):
            status, result = await self.post(
                "guide",
                {
                    "chat": chat,
                    "guidance": "Ask first",
                    "reference_kind": "user",
                    "request_id": str(uuid.uuid4()),
                    **extra,
                },
            )
            self.assertEqual(status, 400, extra)
            self.assertIn("第三方 Agent", result["message"])
        self.assertEqual(p.store.rows("SELECT * FROM guidance_instructions"), [])
        self.assertEqual(len(p.store.rows("SELECT * FROM jobs")), 1)
        self.assertFalse(p.is_guided("live"), "A refused append left a review mark")

    def live_job(self, status="running"):
        """An ordinary reply in progress, as traced() registers it."""
        p = self.plugin
        chat = p.store.chat("qq", "999", "private", "200", title="User")
        p.store.execute(
            "INSERT INTO jobs(trace,chat,umo,status,started) VALUES('live',?,?,?,?)",
            (chat, "qq:FriendMessage:200", status, time.time()),
        )
        p.tasks["live"] = asyncio.create_task(asyncio.Event().wait())
        return chat

    def append(self, chat):
        return self.post(
            "guide",
            {
                "chat": chat,
                "trace": "live",
                "mode": "append",
                "guidance": "Ask the user first",
                "reference_kind": "user",
                "reply_mode": "confirm",
                "request_id": str(uuid.uuid4()),
            },
        )

    async def test_refused_append_leaves_the_reply_ordinary(self):
        p = self.plugin
        _bot, raw = self.bot()
        chat = self.live_job(status="interrupted")
        other = p.store.chat("qq", "999", "private", "201", title="Other")
        status, result = await self.append(other)
        self.assertEqual(status, 400)
        self.assertIn("只能加入当前会话的任务", result["message"])
        # The task is alive, but the database refuses the instruction.
        status, result = await self.append(chat)
        self.assertEqual(status, 400)
        self.assertIn("已不在生成中", result["message"])
        self.assertFalse(p.is_guided("live"))
        async with p.guidance_lock:  # its reply still needs neither lock nor review
            await asyncio.wait_for(self.send_as("live", "reply"), 2)
        await asyncio.wait_for(p.terminate(), 5)
        self.assertFalse(p.tasks["live"].done(), "Reload stopped an ordinary reply")
        self.assertEqual(
            [call.kwargs["message"] for call in raw.await_args_list], ["reply"]
        )

    async def test_append_committing_during_reload_is_still_stopped(self):
        p = self.plugin
        self.bot()
        chat = self.live_job()
        entered, release = threading.Event(), threading.Event()
        add_guidance = p.store.add_guidance

        def slow_add_guidance(*args):
            add_guidance(*args)
            entered.set()
            release.wait(5)

        p.store.add_guidance = slow_add_guidance
        append = asyncio.create_task(self.append(chat))
        stopping = None
        try:
            await until(entered.is_set)
            # Reload chooses what to stop before this commit returns.
            stopping = asyncio.create_task(p.terminate())
            await until(lambda: "live" in p.cancelled)
        finally:
            release.set()
        status, result = await asyncio.wait_for(append, 3)
        self.assertEqual(status, 400)
        self.assertIn("任务已停止", result["message"])
        await asyncio.wait_for(stopping, 5)
        self.assertTrue(p.tasks["live"].cancelled())

    async def test_reload_stops_a_task_whose_append_waits_for_the_lock(self):
        p = self.plugin
        self.bot()
        chat = self.live_job()
        stopping = None
        async with p.guidance_lock:  # an earlier guide() still holds the lock
            append = asyncio.create_task(self.append(chat))
            try:
                await until(lambda: p.is_guided("live"))
                # Nothing is committed yet; reload must still stop this task.
                stopping = asyncio.create_task(p.terminate())
                await until(lambda: "live" in p.cancelled)
            finally:
                if stopping is None:
                    append.cancel()
        await asyncio.wait_for(stopping, 5)
        self.assertEqual((await asyncio.wait_for(append, 3))[0], 400)
        self.assertTrue(p.tasks["live"].cancelled())
        self.assertNotIn("live", p.store.guided_traces)

    async def test_append_arriving_during_reload_leaves_replies_ordinary(self):
        p = self.plugin
        _bot, raw = self.bot()
        self.live_job()
        # Reload has already chosen what to stop; a request still reading its
        # body then reaches guide() and must not mark the task.
        p.terminating = True
        async with p.appending_to("live"):
            self.assertFalse(p.is_guided("live"))
            await asyncio.wait_for(self.send_as("live", "reply"), 2)
        self.assertEqual(
            [call.kwargs["message"] for call in raw.await_args_list], ["reply"]
        )

    async def late_background_reply(self, mark):
        """Reload while a job's background work outlives its pipeline task."""
        p = self.plugin
        bot, raw = self.bot()
        mark("late")
        release = asyncio.Event()

        async def background(trace):
            # A background tool or handoff whose job pipeline already returned:
            # it inherits the trace but is no longer in self.tasks.
            token = trace_context.set(trace)
            try:
                await release.wait()
                return await self.send_as(trace, f"{trace} reply")
            finally:
                trace_context.reset(token)

        late = asyncio.create_task(background("late"))
        plain = asyncio.create_task(background("plain"))
        warning = Mock()
        try:
            await asyncio.sleep(0)
            with patch(
                "astrbot_plugin_qq_chat_workbench.main.logger",
                SimpleNamespace(warning=warning),
            ):
                await asyncio.wait_for(p.terminate(), 5)
            warning.assert_called_once()
            self.assertIsNot(bot.call_action, raw)
        finally:
            release.set()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(late, 3)
        self.assertEqual(await asyncio.wait_for(plain, 3), {"message_id": 7})
        self.assertEqual(
            [call.kwargs["message"] for call in raw.await_args_list],
            ["plain reply"],
        )

    async def test_reload_keeps_the_guard_while_reviewed_background_work_runs(self):
        await self.late_background_reply(self.plugin.mark_guided)

    async def test_reload_keeps_the_guard_while_stopped_background_work_runs(self):
        # A cooperative stop asks the Agent to finish; it does not cancel the
        # job's background tasks.
        await self.late_background_reply(self.plugin.cancelled.add)


if __name__ == "__main__":
    unittest.main()
