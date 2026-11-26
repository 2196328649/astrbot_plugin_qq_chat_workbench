"""Administrator-triggered proactive replies to a selected message."""

import asyncio
import copy
import json
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import test_console as console_tests
from astrbot_plugin_qq_chat_workbench import guidance as guided

ROUTES = {"group": ("100", ""), "private": ("300", ""), "temporary": ("300", "100")}


class ProactiveReplyTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = console_tests.ConsoleTests.asyncSetUp
    asyncTearDown = console_tests.ConsoleTests.asyncTearDown
    post = console_tests.ConsoleTests.post
    get = console_tests.ConsoleTests.get

    def use_session(self, name, quote, mention):
        from astrbot.core.config import AstrBotConfig
        from astrbot.core.config.default import DEFAULT_CONFIG
        from astrbot.core.platform.platform_metadata import PlatformMetadata

        config = AstrBotConfig(str(Path(self.temp.name) / name), default_config={})
        config.update(copy.deepcopy(DEFAULT_CONFIG))
        settings = config["platform_settings"]
        settings["reply_with_quote"] = quote
        settings["reply_with_mention"] = mention
        settings["reply_prefix"] = ""
        settings["segmented_reply"]["enable"] = False
        config["t2i"] = False
        config["provider_tts_settings"]["enable"] = False
        meta = PlatformMetadata("aiocqhttp", "test", "qq")
        self.plugin.context = SimpleNamespace(
            get_platform_inst=lambda _: SimpleNamespace(meta=lambda: meta),
            get_config=lambda umo=None: config,
            conversation_manager=SimpleNamespace(),
            get_using_tts_provider_async=AsyncMock(return_value=None),
        )
        return config

    async def test_reply_quotes_the_selected_message_without_guidance(self):
        from astrbot.core.agent.message import Message
        from astrbot.core.message.message_event_result import (
            MessageEventResult,
            ResultContentType,
        )
        from astrbot.core.pipeline.bootstrap import ensure_builtin_stages_registered
        from astrbot.core.pipeline.scheduler import PipelineScheduler

        # AstrBot's stage modules import each other; load them in its order.
        ensure_builtin_stages_registered()
        from astrbot.core.pipeline.process_stage.method.agent_request import (
            AgentRequestSubStage,
        )

        p = self.plugin
        p.original_execute = PipelineScheduler.execute
        client = p.clients["qq"]
        client.send_private_msg = lambda **params: client.call_action(
            "send_private_msg", **params
        )
        client.send_group_msg = lambda **params: client.call_action(
            "send_group_msg", **params
        )
        events = []
        seen = []

        async def model(runner, **kwargs):
            seen.append([message.content for message in runner.run_context.messages])
            yield SimpleNamespace(completion_text="Generated reply", is_chunk=False)

        async def process(stage, event):
            # Replaces only the paid model call; the guidance hook still runs.
            events.append(event)
            runner = SimpleNamespace(
                streaming=False,
                done=lambda: False,
                run_context=SimpleNamespace(
                    messages=[
                        Message(role="system", content="Existing persona"),
                        Message(role="user", content=event.message_str),
                    ]
                ),
            )
            responses = guided.ToolLoopAgentRunner._iter_llm_responses(runner)
            text = "".join([response.completion_text async for response in responses])
            event.set_result(
                MessageEventResult()
                .message(text)
                .set_result_content_type(ResultContentType.LLM_RESULT)
            )
            yield

        # kind, mode, reply_mode, guidance, session quote, session mention
        cases = (
            ("group", "reply", "confirm", "", False, False),
            ("private", "reply", "auto", " Keep it short ", False, False),
            ("temporary", "reply", "confirm", "", False, False),
            ("group", "reply", "auto", "", True, True),
            ("group", "new", "auto", "Answer", False, False),
            ("private", "new", "confirm", "Answer", False, False),
        )
        with (
            patch.object(guided.ToolLoopAgentRunner, "_iter_llm_responses", model),
            patch.object(AgentRequestSubStage, "process", process),
            patch(
                "astrbot.core.star.star_handler.star_handlers_registry.get_handlers_by_event_type",
                return_value=[],
            ),
            patch("astrbot.core.utils.metrics.Metric.upload", AsyncMock()),
        ):
            p.initialize_guidance()
            for index, case in enumerate(cases):
                kind, mode, reply_mode, guidance, quote, mention = case
                with self.subTest(case=case):
                    config = self.use_session(f"reply-{index}.json", quote, mention)
                    before = copy.deepcopy(dict(config))
                    target, group = ROUTES[kind]
                    chat = p.store.chat("qq", "999", kind, target, group, "Member")
                    remote = str(456 + index)
                    message_id = p.store.message(
                        chat,
                        f"source-{index}",
                        "in",
                        "Member",
                        "Did anyone see this?",
                        "received",
                        remote,
                        sender_id="300",
                    )
                    events.clear()
                    seen.clear()
                    self.calls.clear()
                    body = {
                        "chat": chat,
                        "message_id": message_id,
                        "mode": mode,
                        "reference_kind": "message",
                        "guidance": guidance,
                        "reply_mode": reply_mode,
                        "request_id": str(uuid.uuid4()),
                    }
                    status, result = await self.post("guide", body)
                    self.assertEqual(status, 200, result)
                    self.assertEqual(
                        (result["mode"], result["reply_mode"]), (mode, reply_mode)
                    )
                    trace = result["trace"]
                    task = p.tasks[trace]
                    note = guidance.strip()
                    stored = (
                        guided.PROACTIVE_REPLY_GUIDANCE
                        + ("\n管理员补充要求：" + note if note else "")
                        if mode == "reply"
                        else note
                    )
                    # One Reply even when the session already quotes; mention
                    # follows the session, and the adapter adds a space after At.
                    expected = (
                        (["reply"] if mode == "reply" or quote else [])
                        + (["at", "text"] if mention and kind == "group" else [])
                        + ["text"]
                    )
                    if reply_mode == "confirm":
                        async with asyncio.timeout(5):
                            while not p.store.rows(
                                "SELECT id FROM pending_sends WHERE trace=?", (trace,)
                            ):
                                self.assertFalse(
                                    task.done(),
                                    p.store.rows(
                                        "SELECT * FROM logs WHERE level='ERROR'"
                                    ),
                                )
                                await asyncio.sleep(0.005)
                        review = await self.get("review", {"trace": trace})
                        self.assertEqual(self.calls, [])
                        held = review["items"][0]["body"]
                        self.assertEqual(
                            [segment["type"] for segment in held], expected
                        )
                        self.assertEqual(
                            [
                                (
                                    item["reference_kind"],
                                    item["message_id"],
                                    item["guidance"],
                                    item["quoted_text"],
                                )
                                for item in review["instructions"]
                            ],
                            [("message", message_id, stored, "Did anyone see this?")],
                        )
                        approval = {"trace": trace, "ids": [review["items"][0]["id"]]}
                        self.assertEqual((await self.post("approve", approval))[0], 200)
                    await asyncio.wait_for(task, 5)
                    self.assertEqual(len(self.calls), 1, self.calls)
                    action, params = self.calls[0]
                    sent = params["message"]
                    self.assertEqual([segment["type"] for segment in sent], expected)
                    if "reply" in expected:
                        self.assertEqual(
                            sent[0], {"type": "reply", "data": {"id": remote}}
                        )
                    if "at" in expected:
                        self.assertEqual(sent[1]["data"]["qq"], "300")
                    self.assertEqual(
                        sent[-1]["data"]["text"].strip(), "Generated reply"
                    )
                    self.assertEqual(params["self_id"], 999)
                    if kind == "group":
                        self.assertEqual(
                            (action, params["group_id"]), ("send_group_msg", 100)
                        )
                    else:
                        self.assertEqual(
                            (action, params["user_id"], params.get("group_id")),
                            (
                                "send_private_msg",
                                300,
                                100 if kind == "temporary" else None,
                            ),
                        )
                    self.assertEqual(
                        p.store.rows(
                            "SELECT guidance,reference_kind,message_id FROM guidance_instructions WHERE trace=?",
                            (trace,),
                        ),
                        [
                            {
                                "guidance": stored,
                                "reference_kind": "message",
                                "message_id": message_id,
                            }
                        ],
                    )
                    # The model sees the fixed instruction and the selected message.
                    self.assertEqual(len(seen), 1)
                    persona, instruction, words = seen[0]
                    self.assertEqual(
                        (persona, words), ("Existing persona", "Did anyone see this?")
                    )
                    marker, _, payload = instruction.split("\n", 2)
                    self.assertEqual(marker, guided.GUIDANCE_MARKER)
                    self.assertEqual(
                        json.loads(payload),
                        [
                            {
                                "guidance": stored,
                                "reference_kind": "message",
                                "sender": "Member",
                                "search": "Did anyone see this?",
                            }
                        ],
                    )
                    self.assertEqual(len(events), 1)
                    self.assertEqual(
                        bool(events[0].get_extra("_qq_chat_workbench_quote_reply")),
                        mode == "reply",
                    )
                    self.assertEqual(dict(config), before)
                    async with asyncio.timeout(5):
                        while (
                            p.store.rows(
                                "SELECT status FROM jobs WHERE trace=?", (trace,)
                            )[0]["status"]
                            != "completed"
                        ):
                            await asyncio.sleep(0.005)
                    self.assertEqual(
                        p.store.rows(
                            "SELECT prompt,reply_mode,source_message_id FROM jobs WHERE trace=?",
                            (trace,),
                        ),
                        [
                            {
                                "prompt": "Did anyone see this?",
                                "reply_mode": reply_mode,
                                "source_message_id": message_id,
                            }
                        ],
                    )
                    self.assertEqual(
                        await self.post("guide", body),
                        (200, {"trace": trace, "duplicate": True}),
                    )
                    self.assertEqual(
                        len(
                            p.store.rows(
                                "SELECT trace FROM jobs WHERE source_message_id=?",
                                (message_id,),
                            )
                        ),
                        1,
                    )
                    self.assertEqual(len(self.calls), 1)

    async def test_reply_requires_a_quotable_received_message(self):
        p = self.plugin
        chat = p.store.chat("qq", "999", "group", "100")
        elsewhere_chat = p.store.chat("qq", "999", "group", "101")

        def incoming(target, remote):
            return p.store.message(
                target,
                "in",
                "in",
                "Member",
                "Question",
                "received",
                remote,
                sender_id="300",
            )

        quotable = incoming(chat, "456")
        missing = incoming(chat, "")
        zero = incoming(chat, "0")
        elsewhere = incoming(elsewhere_chat, "457")
        outgoing = p.store.message(chat, "out", "out", "机器人", "Bot", "sent", "458")
        cases = (
            ({"reference_kind": "user"}, "无效任务方式或发送方式"),
            ({"reply_mode": "later"}, "无效任务方式或发送方式"),
            ({"message_id": None}, "请先选择要引用的用户消息"),
            ({"message_id": missing}, "没有可用于 QQ 引用的消息 ID"),
            ({"message_id": zero}, "没有可用于 QQ 引用的消息 ID"),
            ({"message_id": elsewhere}, "引用必须是当前会话中真实收到的用户消息"),
            ({"message_id": outgoing}, "引用必须是当前会话中真实收到的用户消息"),
            ({"guidance": 5}, "请输入不超过 10000 字的引导词"),
            ({"guidance": "x" * 10001}, "请输入不超过 10000 字的引导词"),
            ({"mode": "new"}, "请输入不超过 10000 字的引导词"),
        )
        for change, message in cases:
            with self.subTest(change=str(change)[:60]):
                body = {
                    "chat": chat,
                    "message_id": quotable,
                    "mode": "reply",
                    "reference_kind": "message",
                    "guidance": "",
                    "reply_mode": "confirm",
                    "request_id": str(uuid.uuid4()),
                    **change,
                }
                status, result = await self.post("guide", body)
                self.assertEqual(status, 400, result)
                self.assertIn(message, result["message"])
        self.assertEqual(p.store.rows("SELECT * FROM jobs"), [])
        self.assertEqual(p.store.rows("SELECT * FROM guidance_instructions"), [])
        self.assertEqual((p.tasks, self.calls, p.store.guided_traces), ({}, [], set()))


if __name__ == "__main__":
    unittest.main()
