"""Reload, console.db failure and phantom-task regressions for 0.2.4."""

import asyncio
import sqlite3
import sys
import threading
import time
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import test_console as console_tests
from astrbot_plugin_super_admin.store import Store, trace_context
from loguru import logger as loguru_logger


async def until(predicate, timeout=3):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


class ReliabilityTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = console_tests.ConsoleTests.asyncSetUp
    asyncTearDown = console_tests.ConsoleTests.asyncTearDown
    post = console_tests.ConsoleTests.post
    get = console_tests.ConsoleTests.get

    def job(self, trace, mode="", status="running"):
        p = self.plugin
        chat = p.store.chat("qq", "999", "private", "200", title="User")
        p.store.execute(
            "INSERT INTO jobs(trace,chat,umo,status,reply_mode,started) VALUES(?,?,?,?,?,?)",
            (trace, chat, "qq:FriendMessage:200", status, mode, time.time()),
        )
        p.store.execute("INSERT INTO ai_traces VALUES(?)", (trace,))
        return chat

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

    def pipeline(self, trace, release, text):
        """A traced() owner that sends once `release` is set."""
        p = self.plugin

        async def run():
            token = trace_context.set(trace)
            owner = p.enter_hook()
            p.tasks[trace] = owner
            try:
                await release.wait()
                return await p.clients["qq"].call_action(
                    "send_private_msg", self_id=999, user_id=200, message=text
                )
            finally:
                if p.tasks.get(trace) is owner:
                    p.tasks.pop(trace, None)
                p.leave_hook(owner)
                trace_context.reset(token)

        return asyncio.create_task(run())

    async def initialized(self):
        """Run the real initialize() against the isolated data directory."""
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
        for patcher in (
            patch(
                "astrbot_plugin_super_admin.main.get_astrbot_data_path",
                return_value=self.temp.name,
            ),
            patch.object(PipelineScheduler, "execute", original),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        await p.initialize()

        def remove_sink():
            try:
                loguru_logger.remove(p.sink_id)
            except ValueError:
                pass

        self.addCleanup(remove_sink)
        return PipelineScheduler, original

    async def test_reload_keeps_ordinary_replies_and_stops_only_reviewed_work(self):
        p = self.plugin
        bot, raw = self.bot()
        self.job("reviewed", mode="confirm")
        hold, go = asyncio.Event(), asyncio.Event()
        go.set()
        ordinary = self.pipeline("ordinary", hold, "ordinary reply")
        reviewed = self.pipeline("reviewed", go, "needs review")
        await until(lambda: "reviewed" in p.review_waiter_traces.values())
        await until(lambda: "ordinary" in p.tasks)
        await asyncio.wait_for(p.terminate(), 5)
        self.assertFalse(ordinary.done(), "Reload interrupted an ordinary reply")
        with self.assertRaises(asyncio.CancelledError):
            await reviewed
        self.assertIs(bot.call_action, raw)
        self.assertTrue(p.db_worker.snapshot()["closed"])
        hold.set()
        self.assertEqual(await asyncio.wait_for(ordinary, 3), {"message_id": 7})
        self.assertEqual(
            [call.kwargs["message"] for call in raw.await_args_list],
            ["ordinary reply"],
        )
        with closing(sqlite3.connect(Path(self.temp.name) / "test.db")) as db:
            self.assertEqual(
                db.execute(
                    "SELECT status,payload!='' FROM pending_sends WHERE trace='reviewed'"
                ).fetchall(),
                [("pending", 1)],
            )

    async def test_reload_lets_a_message_in_receive_reach_the_pipeline(self):
        p = self.plugin
        scheduler, original = await self.initialized()
        entered, release = asyncio.Event(), asyncio.Event()

        async def receive(event, trace):
            entered.set()
            await release.wait()

        p.receive = receive
        event = SimpleNamespace(set_extra=Mock(), stop_event=Mock())
        message = asyncio.create_task(scheduler.execute(None, event))
        await asyncio.wait_for(entered.wait(), 3)
        await asyncio.wait_for(p.terminate(), 5)
        self.assertFalse(message.done(), "Reload dropped a message in receive")
        release.set()
        self.assertEqual(await asyncio.wait_for(message, 3), "original-result")
        original.assert_awaited_once()

    async def test_unresolved_background_completion_is_held_during_reload(self):
        p = self.plugin
        scheduler, original = await self.initialized()
        entered, release = asyncio.Event(), asyncio.Event()

        async def receive(event, trace):
            entered.set()
            await release.wait()
            raise RuntimeError("Observer worker is closing")

        p.receive = receive
        event = SimpleNamespace(
            set_extra=Mock(),
            stop_event=Mock(),
            get_extra=lambda key, default=None: (
                True if key == "_reference_image_completion" else default
            ),
        )
        completion = asyncio.create_task(scheduler.execute(None, event))
        await asyncio.wait_for(entered.wait(), 3)
        await asyncio.wait_for(p.terminate(), 5)
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(completion, 3)
        original.assert_not_awaited()

    async def test_reload_is_bounded_when_reviewed_work_ignores_cancellation(self):
        p = self.plugin
        bot, raw = self.bot()
        p.mark_guided("stubborn")
        started, release = asyncio.Event(), asyncio.Event()

        async def stubborn():
            token = trace_context.set("stubborn")
            owner = p.enter_hook()
            started.set()
            try:
                while not release.is_set():
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        continue
            finally:
                p.leave_hook(owner)
                trace_context.reset(token)

        task = asyncio.create_task(stubborn())
        await asyncio.wait_for(started.wait(), 3)
        warning = Mock()
        try:
            with (
                patch("astrbot_plugin_super_admin.main.OWNER_DRAIN_SECONDS", 0.05),
                patch("astrbot_plugin_super_admin.main.OWNER_CANCEL_SECONDS", 0.05),
                patch(
                    "astrbot_plugin_super_admin.main.logger",
                    SimpleNamespace(warning=warning),
                ),
            ):
                await asyncio.wait_for(p.terminate(), 3)
            warning.assert_called_once()
            # The straggler keeps the guard; every other trace passes through it.
            self.assertIsNot(bot.call_action, raw)
            for trace in ("stubborn", "other"):
                token = trace_context.set(trace)
                try:
                    send = bot.call_action(
                        "send_private_msg", self_id=999, user_id=200, message=trace
                    )
                    if trace == "stubborn":
                        with self.assertRaises(asyncio.CancelledError):
                            await send
                    else:
                        await send
                finally:
                    trace_context.reset(token)
            self.assertEqual(
                [call.kwargs["message"] for call in raw.await_args_list], ["other"]
            )
        finally:
            release.set()
            await asyncio.wait_for(task, 3)

    async def test_reload_keeps_the_send_guard_until_a_stopped_task_exits(self):
        p = self.plugin
        bot, raw = self.bot()
        release = asyncio.Event()
        stopped = self.pipeline("stopped", release, "late reply")
        await until(lambda: "stopped" in p.tasks)
        # An earlier stop returned "stopping" while the task was still cleaning up.
        p.cancelled.add("stopped")
        stopping = asyncio.create_task(p.terminate())
        await asyncio.sleep(0.05)
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(stopped, 3)
        await asyncio.wait_for(stopping, 3)
        self.assertIs(bot.call_action, raw)
        raw.assert_not_awaited()

    async def test_reload_still_drains_an_accepted_dashboard_request(self):
        from astrbot_plugin_super_admin.main import chat_previews

        p = self.plugin
        self.bot()
        p.store.chat("qq", "999", "group", "100", title="Group")
        entered, release = threading.Event(), threading.Event()

        def previews(rows):
            entered.set()
            release.wait(5)
            return chat_previews(rows)

        try:
            with patch("astrbot_plugin_super_admin.main.chat_previews", previews):
                request = asyncio.create_task(self.get("chats", {}))
                await until(entered.is_set)
                stopping = asyncio.create_task(p.terminate())
                await asyncio.sleep(0.05)
                self.assertFalse(stopping.done(), "SQLite closed under a request")
                release.set()
                rows = await asyncio.wait_for(request, 3)
                await asyncio.wait_for(stopping, 3)
        finally:
            release.set()
        self.assertEqual([row["kind"] for row in rows], ["group"])

    async def test_ordinary_send_needs_neither_review_lock_nor_console_db(self):
        p = self.plugin
        bot, raw = self.bot()
        self.job("reviewed", mode="confirm")
        p.store.db.close()  # every console.db read and write now fails

        async def send(trace):
            token = trace_context.set(trace)
            try:
                return await bot.call_action(
                    "send_private_msg", self_id=999, user_id=200, message=trace
                )
            finally:
                trace_context.reset(token)

        # For example guide() waiting up to 15 s for get_msg under the lock.
        async with p.guidance_lock:
            self.assertEqual(
                await asyncio.wait_for(send("ordinary"), 2), {"message_id": 7}
            )
        with self.assertRaises(sqlite3.ProgrammingError):
            await asyncio.wait_for(send("reviewed"), 2)
        self.assertEqual(
            [call.kwargs["message"] for call in raw.await_args_list], ["ordinary"]
        )
        self.assertGreater(p.dropped, 0)

    async def test_ordinary_model_request_survives_console_db_failure(self):
        from astrbot.core.agent.message import Message
        from astrbot_plugin_super_admin.guidance import ToolLoopAgentRunner

        p = self.plugin
        runner = SimpleNamespace(
            streaming=True,
            done=lambda: False,
            run_context=SimpleNamespace(messages=[Message(role="user", content="Hi")]),
        )

        async def model(current, **kwargs):
            yield SimpleNamespace(completion_text="answer")

        async def collect():
            token = trace_context.set("plain")
            try:
                return [
                    response.completion_text
                    async for response in ToolLoopAgentRunner._iter_llm_responses(
                        runner
                    )
                ]
            finally:
                trace_context.reset(token)

        with patch.object(ToolLoopAgentRunner, "_iter_llm_responses", model):
            p.initialize_guidance()
            p.store.db.close()
            task = asyncio.create_task(collect())
            p.tasks["plain"] = task
            self.assertEqual(await asyncio.wait_for(task, 3), ["answer"])

    async def test_append_routes_the_live_reply_to_review_before_the_lock(self):
        p = self.plugin
        chat = self.job("live")
        p.tasks["live"] = asyncio.create_task(asyncio.Event().wait())
        async with p.guidance_lock:  # an earlier guide() still holds the lock
            append = asyncio.create_task(
                self.post(
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
            )
            await until(lambda: p.is_guided("live"))
            token = trace_context.set("live")
            try:
                reply = asyncio.create_task(
                    p.clients["qq"].call_action(
                        "send_private_msg",
                        self_id=999,
                        user_id=200,
                        message="early reply",
                    )
                )
            finally:
                trace_context.reset(token)
            await asyncio.sleep(0.05)
            self.assertFalse(reply.done(), "Reply overtook the pending append")
        self.assertEqual((await asyncio.wait_for(append, 3))[0], 200)
        await until(lambda: "live" in p.review_waiter_traces.values())
        self.assertEqual(self.calls, [])
        await p.stop_trace("live")
        with self.assertRaises(asyncio.CancelledError):
            await reply

    def test_every_persisted_reply_mode_marks_the_trace(self):
        path = Path(self.temp.name) / "index.db"
        marked = {"auto", "later", "instructed", "reviewed"}
        store = Store(path)
        try:
            store.execute("INSERT INTO jobs(trace,status) VALUES('plain','running')")
            store.execute("UPDATE jobs SET reply_mode='' WHERE trace='plain'")
            store.execute(
                "INSERT INTO jobs(trace,status,reply_mode) VALUES('auto','running','auto')"
            )
            store.execute("INSERT INTO jobs(trace,status) VALUES('later','running')")
            store.execute("UPDATE jobs SET reply_mode='confirm' WHERE trace='later'")
            store.execute(
                "INSERT INTO guidance_instructions(request_id,trace) VALUES('r','instructed')"
            )
            store.execute(
                "INSERT INTO pending_sends(trace,status) VALUES('reviewed','pending')"
            )
            self.assertEqual(store.guided_traces, marked)
        finally:
            store.close()
        reopened = Store(path)
        try:
            self.assertEqual(reopened.guided_traces, marked)
            self.assertEqual(
                reopened.rows("SELECT name FROM sqlite_master WHERE type='trigger'"), []
            )
        finally:
            reopened.close()

    async def test_shared_timers_neither_keep_jobs_running_nor_get_cancelled(self):
        from astrbot.core.utils.metrics import Metric

        p = self.plugin
        self.job("finished", status="completed")
        self.job("busy", status="completed")
        timer_started = asyncio.Event()

        class AsyncCurl:
            async def _force_timeout(self):
                timer_started.set()
                await asyncio.Event().wait()

        with (
            patch.dict(
                sys.modules, {"curl_cffi.aio": SimpleNamespace(AsyncCurl=AsyncCurl)}
            ),
            patch.object(Metric, "_is_disabled", staticmethod(lambda: False)),
            patch.object(Metric, "_has_uploaded_once", True),
            patch.object(Metric, "_pending_metrics", {}),
            patch.object(Metric, "_flush_task", None),
            patch.object(Metric, "_save_platform_stats", AsyncMock()),
            patch.object(Metric, "_post_metrics", AsyncMock()),
        ):
            token = trace_context.set("finished")
            try:
                # As AstrMessageEvent.send() does inside a traced pipeline.
                await asyncio.create_task(
                    Metric.upload(msg_event_tick=1, adapter_name="qq")
                )
                timer = asyncio.create_task(AsyncCurl()._force_timeout())
            finally:
                trace_context.reset(token)
            token = trace_context.set("busy")
            try:
                work = asyncio.create_task(asyncio.Event().wait())
            finally:
                trace_context.reset(token)
            flush = Metric._flush_task
            try:
                await asyncio.wait_for(timer_started.wait(), 3)
                self.assertFalse(flush.done())
                statuses = {
                    row["trace"]: row["status"] for row in await self.get("jobs", {})
                }
                self.assertEqual(statuses, {"finished": "completed", "busy": "running"})
                self.assertTrue(await p.stop_trace("finished"))
                self.assertTrue(await p.stop_trace("busy"))
                self.assertFalse(flush.done())
                self.assertFalse(timer.done())
                self.assertTrue(work.cancelled())
                self.assertEqual(
                    {
                        row["trace"]: row["status"]
                        for row in p.store.rows("SELECT trace,status FROM jobs")
                    },
                    {"finished": "completed", "busy": "cancelled"},
                )
            finally:
                for task in (flush, timer, work):
                    task.cancel()
                await asyncio.gather(flush, timer, work, return_exceptions=True)

    async def test_closed_observer_never_replaces_a_stop_with_an_error(self):
        p = self.plugin
        bot, raw = self.bot()
        p.release_unattempted = AsyncMock(
            side_effect=RuntimeError("Observer worker is closing")
        )

        async def approved(trace, pending_id, platform, action):
            p.send_owners[pending_id] = asyncio.current_task()
            return {"self_id": 999, "user_id": 200, "message": "approved"}

        p.hold_guided_send = AsyncMock(return_value=5)
        p.approved_payload = approved
        p.set_step = AsyncMock(side_effect=asyncio.CancelledError())
        token = trace_context.set("approved")
        try:
            with self.assertRaises(asyncio.CancelledError):
                await bot.call_action(
                    "send_private_msg", self_id=999, user_id=200, message="approved"
                )
        finally:
            trace_context.reset(token)
        p.release_unattempted.assert_awaited_once()
        raw.assert_not_awaited()
        p.cancelled.add("recovered")
        with self.assertRaises(asyncio.CancelledError):
            await p.send_recovered_replies([{"id": 6, "trace": "recovered"}])
        self.assertEqual(p.release_unattempted.await_count, 2)

    async def test_guidance_note_stays_out_of_history_while_unloading(self):
        from astrbot.core.agent.message import Message
        from astrbot.core.pipeline.process_stage.method.agent_sub_stages.internal import (
            InternalAgentSubStage,
        )
        from astrbot_plugin_super_admin.guidance import GUIDANCE_MARKER

        p = self.plugin
        saved = AsyncMock()
        note = Message(role="system", content=GUIDANCE_MARKER + "\nAsk first")
        note._no_save = True
        messages = [
            Message(role="system", content="Persona"),
            note,
            Message(role="user", content="Hi"),
        ]
        event = SimpleNamespace(
            get_extra=lambda key, default=None: (
                "guided" if key == "_super_admin_guided_trace" else default
            )
        )
        with patch.object(InternalAgentSubStage, "_save_to_history", saved):
            p.initialize_guidance()
            p.active = False  # terminate() has begun; a stopped task still saves
            await InternalAgentSubStage._save_to_history(
                object(), event, None, None, messages, None, user_aborted=True
            )
        self.assertEqual(
            [message.content for message in saved.await_args.args[4]], ["Persona", "Hi"]
        )


if __name__ == "__main__":
    unittest.main()
