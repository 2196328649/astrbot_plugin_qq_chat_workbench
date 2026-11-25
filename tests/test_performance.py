"""Worker, retention and async review races against real SQLite and the live-baseline fixtures."""

import asyncio
import contextvars
import json
import sqlite3
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import test_console as console_tests
from astrbot_plugin_super_admin.guidance import approved_send, prepare_pending
from astrbot_plugin_super_admin.ordered_worker import OrderedWorker
from astrbot_plugin_super_admin.store import (
    PENDING_REPLY_LIMIT,
    PENDING_TOTAL_LIMIT,
    segments,
    trace_context,
)


async def until(predicate, timeout=3):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_order_context_and_failure_accounting(self):
        lane = OrderedWorker("test-worker", 3)
        token = contextvars.ContextVar("token", default="missing")
        values = []
        try:
            tasks = []
            for i in range(8):
                token.set(i)
                tasks.append(
                    asyncio.create_task(lane.run(lambda: values.append(token.get())))
                )
            await asyncio.gather(*tasks)
            self.assertEqual(values, list(range(8)))
            with self.assertRaisesRegex(ValueError, "test failure"):
                await lane.run(
                    lambda: (_ for _ in ()).throw(ValueError("test failure"))
                )
            self.assertEqual(lane.snapshot()["failed"], 1)
            self.assertEqual(lane.snapshot()["completed"], 9)
        finally:
            await lane.close()

    async def test_bounded_admission_cancel_before_and_after_acceptance(self):
        lane = OrderedWorker("test-bounded", 1)
        entered, release = threading.Event(), threading.Event()
        calls = []

        def slow():
            entered.set()
            release.wait(5)
            calls.append("accepted")

        first = asyncio.create_task(lane.run(slow))
        try:
            await until(entered.is_set)
            second = asyncio.create_task(lane.run(calls.append, "must not run"))
            await until(lambda: lane.snapshot()["waiting"] == 1)
            second.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await second
            first.cancel()
            await asyncio.sleep(0.02)
            first.cancel()  # Repeated cancellation must not release an accepted transaction.
            await asyncio.sleep(0.02)
            self.assertFalse(first.done())
            self.assertEqual(lane.snapshot()["pending"], 1)
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await first
            self.assertEqual(calls, ["accepted"])
            self.assertEqual(lane.snapshot()["pending"], 0)
        finally:
            release.set()
            await lane.close()

    async def test_shutdown_drains_and_rejects_new_and_queued_work(self):
        lane = OrderedWorker("test-close", 1)
        entered, release = threading.Event(), threading.Event()
        events = []

        def slow():
            entered.set()
            release.wait(5)
            events.append("work")

        task = asyncio.create_task(lane.run(slow))
        try:
            await until(entered.is_set)
            queued = asyncio.create_task(lane.run(events.append, "unexpected"))
            await until(lambda: lane.waiting == 1)
            close = asyncio.create_task(lane.close(lambda: events.append("close")))
            await until(lambda: lane.snapshot()["closing"])
            with self.assertRaisesRegex(RuntimeError, "closing"):
                await lane.run(lambda: None)
            close.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await close
            release.set()
            await task
            with self.assertRaisesRegex(RuntimeError, "closing"):
                await queued
            await lane.close()
            await lane.close()
            self.assertEqual(events, ["work", "close"])
            self.assertTrue(lane.snapshot()["closed"])
        finally:
            release.set()
            await lane.close()


class PerformanceTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = console_tests.ConsoleTests.asyncSetUp
    asyncTearDown = console_tests.ConsoleTests.asyncTearDown
    post = console_tests.ConsoleTests.post
    get = console_tests.ConsoleTests.get

    def job(self, trace="race", mode="confirm"):
        p = self.plugin
        chat = p.store.chat("qq", "999", "private", "200", title="User")
        p.store.execute(
            "INSERT INTO jobs(trace,chat,umo,status,reply_mode,started) VALUES(?,?,?,'running',?,?)",
            (trace, chat, "qq:FriendMessage:200", mode, time.time()),
        )
        p.store.execute("INSERT INTO ai_traces VALUES(?)", (trace,))
        return chat

    def saved_reply(self, trace="race", text="Saved", status="pending"):
        return self.plugin.store.execute(
            "INSERT INTO pending_sends(trace,platform,action,payload,preview,status,created) VALUES(?,'qq','send_private_msg',?,?,?,?)",
            (
                trace,
                json.dumps({"self_id": 999, "user_id": 200, "message": text}),
                json.dumps([{"type": "text", "data": {"text": text}}]),
                status,
                time.time(),
            ),
        )

    def sending(self, trace="race", text="Reply", bot=None):
        p = self.plugin
        token = trace_context.set(trace)
        try:
            task = asyncio.create_task(
                (bot or p.clients["qq"]).call_action(
                    "send_private_msg", self_id=999, user_id=200, message=text
                )
            )
        finally:
            trace_context.reset(token)
        p.tasks[trace] = task
        return task

    async def test_db_lock_never_blocks_loop_or_immediate_stop(self):
        p = self.plugin
        self.job(mode="auto")
        entered, release = threading.Event(), threading.Event()
        stopped = asyncio.Event()
        event = SimpleNamespace(set_extra=Mock(), stop_event=Mock())
        p.live_events["race"] = event
        p.guidance_states["race"] = {
            "runner": SimpleNamespace(request_stop=stopped.set)
        }
        p.tasks["race"] = asyncio.create_task(stopped.wait())

        def hold_lock():
            with p.store.lock:
                entered.set()
                release.wait(5)

        blocker = asyncio.create_task(p.db_call(hold_lock))
        try:
            await until(entered.is_set)
            reading = asyncio.create_task(p.db_call(p.store.rows, "SELECT 1"))
            stop = asyncio.create_task(self.post("stop", {"trace": "race"}))
            await asyncio.wait_for(stopped.wait(), 0.5)
            event.stop_event.assert_called_once()
            # Coarse check that the loop keeps iterating while the worker holds
            # the lock. Measure the longest gap, not a tick count: Windows rounds
            # each 5 ms sleep up to its ~15.6 ms timer. Database access on the
            # loop itself is caught above and by the reading check below.
            ticks, longest = 0, 0.0
            last = time.monotonic()
            end = last + 0.08
            while last < end:
                ticks += 1
                await asyncio.sleep(0.005)
                now = time.monotonic()
                longest, last = max(longest, now - last), now
            self.assertGreaterEqual(ticks, 2)
            self.assertLess(longest, 0.25)
            self.assertFalse(reading.done())
            release.set()
            await blocker
            await reading
            self.assertEqual((await stop)[0], 200)
        finally:
            release.set()
            await blocker

    async def test_media_lane_does_not_hold_db_or_loop(self):
        p = self.plugin
        self.job(mode="auto")
        entered, release = threading.Event(), threading.Event()

        def thumbnail(value):
            entered.set()
            release.wait(5)
            return segments(value)

        try:
            with patch("astrbot_plugin_super_admin.main.segments", thumbnail):
                task = self.sending()
                await until(entered.is_set)
                rows = await asyncio.wait_for(
                    p.db_call(p.store.rows, "SELECT 42 AS value"), 0.5
                )
                self.assertEqual(rows[0]["value"], 42)
                self.assertFalse(self.calls)
                self.assertEqual(p.media_worker.snapshot()["pending"], 1)
                release.set()
                await task
                self.assertEqual(len(self.calls), 1)
        finally:
            release.set()

    async def test_stop_during_approved_thumbnail_never_attempts_send(self):
        p = self.plugin
        self.job()
        entered, release = threading.Event(), threading.Event()

        def thumbnail(value):
            entered.set()
            release.wait(5)
            return segments(value)

        try:
            with patch("astrbot_plugin_super_admin.main.segments", thumbnail):
                task = self.sending()
                await until(lambda: bool(p.review_waiters))
                pending_id = next(iter(p.review_waiters))
                self.assertEqual(
                    (
                        await self.post(
                            "approve", {"trace": "race", "ids": [pending_id]}
                        )
                    )[0],
                    200,
                )
                await until(entered.is_set)
                stop = asyncio.create_task(p.stop_trace("race"))
                await until(lambda: "race" in p.cancelled)
                self.assertFalse(self.calls)
                release.set()
                await stop
                await asyncio.gather(task, return_exceptions=True)
            self.assertFalse(self.calls)
            row = p.store.rows("SELECT status,payload FROM pending_sends")[0]
            self.assertEqual(row, {"status": "discarded", "payload": ""})
        finally:
            release.set()

    async def test_cancel_after_send_claim_preserves_unsent_on_reload(self):
        p = self.plugin
        self.job()
        entered, release = threading.Event(), threading.Event()
        original = p.store.claim_pending_send

        def claim(*args):
            value = original(*args)
            entered.set()
            release.wait(5)
            return value

        try:
            with patch.object(p.store, "claim_pending_send", claim):
                task = self.sending()
                await until(lambda: bool(p.review_waiters))
                pending_id = next(iter(p.review_waiters))
                await self.post("approve", {"trace": "race", "ids": [pending_id]})
                await until(entered.is_set)
                stop = asyncio.create_task(p.stop_trace("race", preserve_pending=True))
                await until(lambda: "race" in p.cancelled)
                release.set()
                await stop
                await asyncio.gather(task, return_exceptions=True)
            self.assertFalse(self.calls)
            row = p.store.rows("SELECT status,payload FROM pending_sends")[0]
            self.assertEqual(row["status"], "pending")
            self.assertTrue(row["payload"])
        finally:
            release.set()

    async def test_stop_during_actual_send_is_unknown_and_never_retried(self):
        p = self.plugin
        self.job()
        entered = asyncio.Event()

        async def network(action, **params):
            self.calls.append((action, params))
            entered.set()
            await asyncio.Future()

        p.clients["qq"].call_action = p.wrap_sender(network, "qq")
        task = self.sending()
        await until(lambda: bool(p.review_waiters))
        pending_id = next(iter(p.review_waiters))
        await self.post("approve", {"trace": "race", "ids": [pending_id]})
        await asyncio.wait_for(entered.wait(), 3)
        await p.stop_trace("race")
        await asyncio.gather(task, return_exceptions=True)
        row = p.store.rows("SELECT status,payload FROM pending_sends")[0]
        self.assertEqual(row["status"], "unknown")
        self.assertTrue(row["payload"])
        self.assertEqual(
            (await self.post("approve", {"trace": "race", "ids": [pending_id]}))[0], 400
        )
        self.assertEqual(len(self.calls), 1)

    async def test_exact_review_snapshot_not_mutable_original(self):
        p = self.plugin
        self.job()
        message = [{"type": "text", "data": {"text": "Reviewed original"}}]
        task = self.sending(text=message)
        await until(lambda: bool(p.review_waiters))
        pending_id = next(iter(p.review_waiters))
        message[0]["data"]["text"] = "Changed after review"
        await self.post("approve", {"trace": "race", "ids": [pending_id]})
        await task
        self.assertEqual(
            self.calls[0][1]["message"][0]["data"]["text"], "Reviewed original"
        )

    async def test_approval_cannot_overtake_reservation_waiter_publication(self):
        p = self.plugin
        self.job()
        entered, release = threading.Event(), threading.Event()
        original = p.store.reserve_pending
        receipt = []

        def reserve(*args):
            value = original(*args)
            receipt.append(value)
            entered.set()
            release.wait(5)
            return value

        try:
            with patch.object(p.store, "reserve_pending", reserve):
                task = self.sending()
                await until(entered.is_set)
                approval = asyncio.create_task(
                    self.post("approve", {"trace": "race", "ids": receipt[:]})
                )
                await asyncio.sleep(0.03)
                self.assertFalse(approval.done())
                self.assertFalse(self.calls)
                release.set()
                self.assertEqual((await approval)[0], 200)
                await task
            self.assertEqual(len(self.calls), 1)
            self.assertFalse(p.review_waiters)
        finally:
            release.set()

    async def test_cancelled_pending_publication_is_reviewable_without_live_waiter(
        self,
    ):
        p = self.plugin
        self.job()
        entered, release = threading.Event(), threading.Event()
        original = p.store.reserve_pending

        def reserve(*args):
            value = original(*args)
            entered.set()
            release.wait(5)
            return value

        try:
            with patch.object(p.store, "reserve_pending", reserve):
                task = self.sending()
                await until(entered.is_set)
                task.cancel()
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertFalse(p.review_waiters)
            self.assertFalse(self.calls)
            row = p.store.rows("SELECT id,status FROM pending_sends")[0]
            self.assertEqual(row["status"], "pending")
            await self.post("approve", {"trace": "race", "ids": [row["id"]]})
            await p.tasks["race"]
            self.assertEqual(len(self.calls), 1)
        finally:
            release.set()

    async def test_stop_overtakes_committed_approval_without_resurrection(self):
        p = self.plugin
        self.job()
        pending_id = self.saved_reply()
        entered, release = threading.Event(), threading.Event()
        original = p.store.approve_pending

        def approve(*args):
            result = original(*args)
            entered.set()
            release.wait(5)
            return result

        try:
            with patch.object(p.store, "approve_pending", approve):
                approval = asyncio.create_task(
                    self.post("approve", {"trace": "race", "ids": [pending_id]})
                )
                await until(entered.is_set)
                stop = asyncio.create_task(p.stop_trace("race"))
                await until(lambda: "race" in p.cancelled)
                release.set()
                self.assertEqual((await approval)[0], 400)
                await stop
            self.assertIn("race", p.cancelled)
            self.assertFalse(self.calls)
            self.assertEqual(
                p.store.rows("SELECT status FROM pending_sends")[0]["status"],
                "discarded",
            )
        finally:
            release.set()

    async def test_cancelled_approval_http_restores_review_instead_of_sending(self):
        p = self.plugin
        self.job()
        pending_id = self.saved_reply()
        entered, release = threading.Event(), threading.Event()
        original = p.store.approve_pending

        def approve(*args):
            result = original(*args)
            entered.set()
            release.wait(5)
            return result

        try:
            with patch.object(p.store, "approve_pending", approve):
                approval = asyncio.create_task(
                    self.post("approve", {"trace": "race", "ids": [pending_id]})
                )
                await until(entered.is_set)
                approval.cancel()
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await approval
            self.assertFalse(self.calls)
            self.assertEqual(
                p.store.rows("SELECT status FROM pending_sends")[0]["status"], "pending"
            )
            await self.post("approve", {"trace": "race", "ids": [pending_id]})
            await p.tasks["race"]
            self.assertEqual(len(self.calls), 1)
        finally:
            release.set()

    async def test_mixed_recovered_and_live_replies_preserve_order(self):
        p = self.plugin
        self.job()
        first = self.saved_reply(text="First")
        entered, release = asyncio.Event(), asyncio.Event()

        async def network(action, **params):
            self.calls.append((action, params))
            if params["message"] == "First":
                entered.set()
                await release.wait()
            return {"message_id": len(self.calls)}

        p.clients["qq"].call_action = p.wrap_sender(network, "qq")
        second_task = self.sending(text="Second")
        try:
            await until(lambda: bool(p.review_waiters))
            second = next(iter(p.review_waiters))
            await self.post("approve", {"trace": "race", "ids": [first, second]})
            recovered = p.tasks["race"]
            await asyncio.wait_for(entered.wait(), 3)
            self.assertEqual([call[1]["message"] for call in self.calls], ["First"])
            release.set()
            await asyncio.gather(recovered, second_task)
            self.assertEqual(
                [call[1]["message"] for call in self.calls], ["First", "Second"]
            )
            self.assertFalse(p.send_gates)
            self.assertFalse(p.send_owners)
        finally:
            release.set()

    async def test_duplicate_live_senders_cannot_claim_same_approval(self):
        p = self.plugin
        self.job()
        pending_id = self.saved_reply(status="approved")
        entered, release = asyncio.Event(), asyncio.Event()

        async def network(action, **params):
            self.calls.append((action, params))
            entered.set()
            await release.wait()
            return {"message_id": 1}

        p.clients["qq"].call_action = p.wrap_sender(network, "qq")
        token = approved_send.set(pending_id)
        try:
            first = self.sending(text="Ignored")
            await asyncio.wait_for(entered.wait(), 3)
            second = self.sending(text="Ignored too")
            with self.assertRaisesRegex(RuntimeError, "live sender"):
                await second
            self.assertEqual(
                p.store.rows("SELECT status FROM pending_sends")[0]["status"], "sending"
            )
            release.set()
            await first
            self.assertEqual(len(self.calls), 1)
        finally:
            approved_send.reset(token)
            release.set()

    async def test_known_api_success_survives_cancelled_audit(self):
        p = self.plugin
        self.job()
        entered, release = threading.Event(), threading.Event()
        original = p.store.execute

        def audit(sql, args=()):
            value = original(sql, args)
            if sql.startswith(
                "UPDATE pending_sends SET status=?,payload=CASE WHEN ? THEN"
            ):
                entered.set()
                release.wait(5)
            return value

        try:
            with patch.object(p.store, "execute", audit):
                task = self.sending()
                await until(lambda: bool(p.review_waiters))
                pending_id = next(iter(p.review_waiters))
                await self.post("approve", {"trace": "race", "ids": [pending_id]})
                await until(entered.is_set)
                task.cancel()
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertEqual(len(self.calls), 1)
            self.assertEqual(
                p.store.rows("SELECT status,payload FROM pending_sends")[0],
                {"status": "accepted", "payload": ""},
            )
        finally:
            release.set()

    async def test_pending_utf8_caps_are_atomic_and_unchanged(self):
        import astrbot_plugin_super_admin.store as module

        self.assertEqual(PENDING_REPLY_LIMIT, 32 * 1024 * 1024)
        self.assertEqual(PENDING_TOTAL_LIMIT, 128 * 1024 * 1024)
        self.job()
        payload, preview, size = prepare_pending(
            "send_private_msg", {"message": "你好" * 15}
        )
        self.assertEqual(size, len(payload.encode("utf-8")))
        self.assertGreater(size, len(payload))
        p = self.plugin
        with (
            patch.object(module, "PENDING_REPLY_LIMIT", size),
            patch.object(module, "PENDING_TOTAL_LIMIT", size),
        ):
            results = await asyncio.gather(
                *(
                    p.db_call(
                        p.store.reserve_pending,
                        "race",
                        "qq",
                        "send_private_msg",
                        payload,
                        preview,
                        size,
                    )
                    for _ in range(2)
                ),
                return_exceptions=True,
            )
            self.assertEqual(sum(type(value) is int for value in results), 1)
            self.assertEqual(sum(isinstance(value, ValueError) for value in results), 1)
            with self.assertRaises(ValueError):
                await p.db_call(
                    p.store.reserve_pending,
                    "race",
                    "qq",
                    "send_private_msg",
                    payload,
                    preview,
                    size + 1,
                )
        self.assertEqual(len(p.store.rows("SELECT id FROM pending_sends")), 1)

    async def test_log_enqueue_is_bounded_and_truncates_before_retaining(self):
        p = self.plugin
        p.safe_log("trace", "INFO", "test", "x" * 1000000)
        self.assertEqual(len(p.queue[0][3]), 24000)
        for _ in range(6000):
            p.safe_log("trace", "INFO", "test", "small")
        self.assertEqual(len(p.queue), 5000)
        self.assertEqual(p.dropped, 1001)

    async def test_state_does_not_wait_for_the_db(self):
        p = self.plugin
        entered, release = threading.Event(), threading.Event()

        def slow():
            entered.set()
            release.wait(5)

        blocker = asyncio.create_task(p.db_call(slow))
        try:
            await until(entered.is_set)
            state = json.loads((await asyncio.wait_for(p.state(), 0.3)).body)
            self.assertEqual(state["version"], "0.2.11")
            self.assertEqual(state["performance"]["db"]["pending"], 1)
            self.assertEqual(state["performance"]["media"]["limit"], 4)
        finally:
            release.set()
            await blocker

    async def test_guidance_mutation_gates_ready_response_and_only_retries_commits(
        self,
    ):
        import uuid

        from astrbot.core.agent.message import Message
        from astrbot_plugin_super_admin.guidance import ToolLoopAgentRunner

        p = self.plugin
        for mode in ("commit", "fail", "cancel"):
            with self.subTest(mode=mode):
                trace = "mutation-" + mode
                chat = self.job(trace, mode="auto")
                started, response_ready = asyncio.Event(), asyncio.Event()
                entered, release = threading.Event(), threading.Event()
                calls = []
                runner = SimpleNamespace(
                    streaming=True,
                    done=lambda: False,
                    run_context=SimpleNamespace(
                        messages=[Message(role="user", content="User text")]
                    ),
                )

                async def model(
                    current,
                    calls=calls,
                    started=started,
                    response_ready=response_ready,
                    **kwargs,
                ):
                    calls.append(len(calls) + 1)
                    if len(calls) == 1:
                        started.set()
                        await response_ready.wait()
                    yield SimpleNamespace(
                        completion_text="old" if len(calls) == 1 else "new"
                    )

                original = p.store.add_guidance

                def mutation(
                    *args,
                    mode=mode,
                    original=original,
                    entered=entered,
                    release=release,
                ):
                    if mode != "fail":
                        original(*args)
                    entered.set()
                    release.wait(5)
                    if mode == "fail":
                        raise ValueError("Simulated rejected transaction")

                async def collect(trace=trace, runner=runner):
                    token = trace_context.set(trace)
                    try:
                        return [
                            r.completion_text
                            async for r in ToolLoopAgentRunner._iter_llm_responses(
                                runner
                            )
                        ]
                    finally:
                        trace_context.reset(token)

                try:
                    with (
                        patch.object(ToolLoopAgentRunner, "_iter_llm_responses", model),
                        patch.object(p.store, "add_guidance", mutation),
                    ):
                        p.initialize_guidance()
                        p.live_events[trace] = SimpleNamespace(set_extra=Mock())
                        task = asyncio.create_task(collect())
                        p.tasks[trace] = task
                        await asyncio.wait_for(started.wait(), 3)
                        append = asyncio.create_task(
                            self.post(
                                "guide",
                                {
                                    "chat": chat,
                                    "trace": trace,
                                    "mode": "append",
                                    "guidance": "New instructions",
                                    "reference_kind": "user",
                                    "reply_mode": "confirm",
                                    "request_id": str(uuid.uuid4()),
                                },
                            )
                        )
                        await until(entered.is_set)
                        response_ready.set()
                        await asyncio.sleep(0.03)
                        self.assertFalse(
                            task.done(),
                            "Response escaped an in-flight guidance mutation",
                        )
                        if mode == "cancel":
                            append.cancel()
                        release.set()
                        if mode == "cancel":
                            with self.assertRaises(asyncio.CancelledError):
                                await append
                        else:
                            self.assertEqual(
                                (await append)[0], 400 if mode == "fail" else 200
                            )
                        self.assertEqual(
                            await asyncio.wait_for(task, 10),
                            ["old" if mode == "fail" else "new"],
                        )
                        self.assertEqual(len(calls), 1 if mode == "fail" else 2)
                finally:
                    release.set()
                    response_ready.set()

    async def test_late_chat_response_does_not_start_lookups_during_shutdown(self):
        from astrbot_plugin_super_admin.main import chat_previews

        p = self.plugin
        p.store.chat("qq", "999", "private", "200")
        entered, release = threading.Event(), threading.Event()

        def previews(rows):
            entered.set()
            release.wait(5)
            return chat_previews(rows)

        try:
            with patch("astrbot_plugin_super_admin.main.chat_previews", previews):
                request_task = asyncio.create_task(self.get("chats", {}))
                await until(entered.is_set)
                p.terminating = True
                p.active = False
                release.set()
                await request_task
                self.assertFalse(p.name_lookups)
                self.assertFalse(self.calls)
        finally:
            release.set()

    async def test_pending_and_phase_publish_in_one_transaction_and_reject_stop(self):
        p = self.plugin
        self.job()
        payload, preview, size = prepare_pending(
            "send_private_msg", {"message": "review"}
        )
        pending_id = await p.db_call(
            p.store.reserve_pending,
            "race",
            "qq",
            "send_private_msg",
            payload,
            preview,
            size,
        )
        job = p.store.rows("SELECT status,step FROM jobs WHERE trace='race'")[0]
        self.assertEqual(
            job, {"status": "awaiting_approval", "step": "awaiting_approval"}
        )
        self.assertTrue(pending_id)
        await p.stop_trace("race")
        with self.assertRaisesRegex(ValueError, "stopped"):
            await p.db_call(
                p.store.reserve_pending,
                "race",
                "qq",
                "send_private_msg",
                payload,
                preview,
                size,
            )
        self.assertEqual(len(p.store.rows("SELECT id FROM pending_sends")), 1)

    async def test_no_synchronous_store_calls_in_async_production_methods(self):
        import ast

        root = Path(__file__).resolve().parents[1]
        violations = []

        class Visitor(ast.NodeVisitor):
            def __init__(self):
                self.functions = []

            def visit_AsyncFunctionDef(self, node):
                self.functions.append(node)
                self.generic_visit(node)
                self.functions.pop()

            def visit_FunctionDef(self, node):
                self.functions.append(node)
                self.generic_visit(node)
                self.functions.pop()

            def visit_Call(self, node):
                if (
                    self.functions
                    and isinstance(self.functions[-1], ast.AsyncFunctionDef)
                    and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Attribute)
                    and isinstance(node.func.value.value, ast.Name)
                    and node.func.value.value.id == "self"
                    and node.func.value.attr == "store"
                ):
                    violations.append(
                        (self.functions[-1].name, node.lineno, node.func.attr)
                    )
                self.generic_visit(node)

        for name in ("main.py", "guidance.py"):
            Visitor().visit(ast.parse((root / name).read_text(encoding="utf-8")))
        self.assertEqual(violations, [])


class RetentionTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = console_tests.ConsoleTests.asyncSetUp
    asyncTearDown = console_tests.ConsoleTests.asyncTearDown

    def drain(self, batch=17):
        pages = []
        for _ in range(10000):
            page = self.plugin.store.prune(batch)
            pages.append(page)
            self.assertLessEqual(page["examined"], batch)
            self.assertLessEqual(page["deleted"], batch)
            if not page["more"]:
                return pages
        self.fail("Retention did not progress past protected rows")

    @staticmethod
    def original_policy(db, now):
        """The previous retention SQL as a policy oracle, not an imported runtime."""
        cutoff = now - 14 * 86400
        commands = [
            (
                "DELETE FROM logs WHERE created<? OR id <= (SELECT COALESCE(MAX(id),0)-100000 FROM logs)",
                (cutoff,),
            ),
            (
                "DELETE FROM messages WHERE created<? OR id <= (SELECT COALESCE(MAX(id),0)-20000 FROM messages)",
                (cutoff,),
            ),
            ("DELETE FROM requests WHERE created<?", (cutoff,)),
            (
                "DELETE FROM pending_sends WHERE created<? AND status!='pending'",
                (cutoff,),
            ),
            (
                "DELETE FROM guidance_instructions WHERE trace NOT IN (SELECT trace FROM jobs)",
                (),
            ),
            (
                "DELETE FROM ai_traces WHERE trace NOT IN (SELECT trace FROM messages) AND trace NOT IN (SELECT trace FROM jobs WHERE started>=? OR status IN ('running','stopping','awaiting_approval')) AND trace NOT IN (SELECT trace FROM pending_sends WHERE status='pending')",
                (cutoff,),
            ),
            (
                "DELETE FROM jobs WHERE started<? AND status NOT IN ('running','stopping','awaiting_approval') AND trace NOT IN (SELECT trace FROM pending_sends WHERE status='pending')",
                (cutoff,),
            ),
            (
                "DELETE FROM chats WHERE updated<? AND id NOT IN (SELECT chat FROM messages) AND id NOT IN (SELECT chat FROM jobs)",
                (cutoff,),
            ),
        ]
        with db:
            for sql, args in commands:
                db.execute(sql, args)

    async def test_bounded_retention_matches_previous_policy_for_normal_rows(self):
        store = self.plugin.store
        now = 10000000.0
        cutoff = now - 14 * 86400
        with patch("astrbot_plugin_super_admin.store.time.time", return_value=now):
            old_chat = store.chat("qq", "999", "private", "200")
            kept_chat = store.chat("qq", "999", "private", "201")
            store.execute("UPDATE chats SET updated=?", (cutoff - 1,))
            with store.db:
                store.db.executemany(
                    "INSERT INTO logs(trace,created) VALUES('old',?)",
                    [(cutoff - 1,)] * 401,
                )
                store.db.execute(
                    "INSERT INTO logs(id,trace,created) VALUES(100500,'new',?)", (now,)
                )
                store.db.executemany(
                    "INSERT INTO messages(chat,trace,created) VALUES(?,'old',?)",
                    [(old_chat, cutoff - 1)] * 133,
                )
                store.db.execute(
                    "INSERT INTO messages(id,chat,trace,created) VALUES(20200,?,'recent',?)",
                    (kept_chat, now),
                )
                for index, status in enumerate(
                    (
                        "running",
                        "stopping",
                        "awaiting_approval",
                        "completed",
                        "failed",
                        "interrupted",
                    )
                ):
                    trace = "job-" + str(index)
                    store.db.execute(
                        "INSERT INTO jobs(trace,chat,started,status) VALUES(?,?,?,?)",
                        (trace, kept_chat, cutoff - 10, status),
                    )
                    store.db.execute("INSERT INTO ai_traces VALUES(?)", (trace,))
                    store.db.execute(
                        "INSERT INTO guidance_instructions(request_id,trace,created) VALUES(?,?,?)",
                        (trace, trace, cutoff - 1),
                    )
                store.db.execute("INSERT INTO ai_traces VALUES('orphan')")
                store.db.execute(
                    "INSERT INTO guidance_instructions(request_id,trace) VALUES('orphan','orphan')"
                )
                store.db.execute(
                    "INSERT INTO requests VALUES('expired',?,'job-0',?)",
                    (kept_chat, cutoff - 1),
                )
                store.db.execute(
                    "INSERT INTO requests VALUES('recent',?,'job-0',?)",
                    (kept_chat, now),
                )
                store.db.execute(
                    "INSERT INTO pending_sends(trace,status,created) VALUES('job-5','pending',?)",
                    (cutoff - 1,),
                )
                store.db.execute(
                    "INSERT INTO pending_sends(trace,status,created) VALUES('job-4','accepted',?)",
                    (cutoff - 1,),
                )
            oracle = sqlite3.connect(":memory:")
            try:
                store.db.backup(oracle)
                self.original_policy(oracle, now)
                pages = self.drain()
                self.assertGreater(len(pages), 20)
                for table in (
                    "logs",
                    "messages",
                    "requests",
                    "pending_sends",
                    "guidance_instructions",
                    "ai_traces",
                    "jobs",
                    "chats",
                ):
                    expected = oracle.execute(
                        f"SELECT * FROM {table} ORDER BY rowid"
                    ).fetchall()
                    actual = [
                        tuple(row)
                        for row in store.db.execute(
                            f"SELECT * FROM {table} ORDER BY rowid"
                        )
                    ]
                    self.assertEqual(actual, expected, table)
            finally:
                oracle.close()

    async def test_protected_pages_advance_and_foreground_write_survives(self):
        store = self.plugin.store
        with store.db:
            store.db.executemany(
                "INSERT INTO pending_sends(trace,status,created) VALUES(?,'pending',1)",
                [(str(i),) for i in range(301)],
            )
            store.db.executemany(
                "INSERT INTO jobs(trace,status,started) VALUES(?,'running',1)",
                [(str(i),) for i in range(301)],
            )
        first = store.prune(17)
        self.assertTrue(first["more"])
        store.log("foreground", "INFO", "test", "New log during retention")
        pages = self.drain()
        self.assertTrue(
            any(
                page["table"] == "pending_sends"
                and page["examined"] == 17
                and page["deleted"] == 0
                for page in pages
            )
        )
        self.assertEqual(len(store.rows("SELECT trace FROM jobs")), 301)
        self.assertEqual(len(store.rows("SELECT id FROM pending_sends")), 301)
        self.assertEqual(
            len(store.rows("SELECT id FROM logs WHERE trace='foreground'")), 1
        )

    async def test_indexes_and_bounded_keyset_plans(self):
        store = self.plugin.store
        indexes = {
            r["name"]
            for r in store.rows("SELECT name FROM sqlite_master WHERE type='index'")
        }
        self.assertTrue(
            {
                "logs_created_id",
                "messages_created_id",
                "requests_created_id",
                "pending_created_id",
                "pending_status_trace",
                "jobs_started_trace",
                "jobs_chat",
                "chats_updated_id",
            }
            <= indexes
        )
        for table, key, date, index in (
            ("logs", "id", "created", "logs_created_id"),
            ("messages", "id", "created", "messages_created_id"),
            ("requests", "id", "created", "requests_created_id"),
            ("pending_sends", "id", "created", "pending_created_id"),
            ("jobs", "trace", "started", "jobs_started_trace"),
            ("chats", "id", "updated", "chats_updated_id"),
        ):
            plan = str(
                store.rows(
                    f"EXPLAIN QUERY PLAN SELECT {key},{date} FROM {table} WHERE {key} IS NOT NULL AND {date}<? AND ({date},{key})>(?,?) ORDER BY {date},{key} LIMIT ?",
                    (1000, 0, "", 128),
                )
            )
            self.assertIn(index, plan)
            self.assertNotIn("USE TEMP B-TREE", plan)
            self.assertNotIn("SCAN " + table, plan)
        self.assertEqual(store.prune(0)["batch_size"], 1)
        self.assertEqual(store.prune(10000)["batch_size"], 256)

    async def test_legacy_nulls_are_conservatively_retained(self):
        store = self.plugin.store
        store.execute("INSERT INTO ai_traces VALUES(NULL)")
        store.execute(
            "INSERT INTO guidance_instructions(request_id,trace) VALUES(NULL,NULL)"
        )
        store.execute(
            "INSERT INTO jobs(trace,status,started) VALUES(NULL,'completed',1)"
        )
        store.execute("INSERT INTO requests(id,created) VALUES(NULL,1)")
        self.drain()
        for table in ("ai_traces", "guidance_instructions", "jobs", "requests"):
            self.assertEqual(len(store.rows(f"SELECT * FROM {table}")), 1)


if __name__ == "__main__":
    unittest.main()
