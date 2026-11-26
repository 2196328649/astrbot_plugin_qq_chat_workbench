import asyncio
import hashlib
import json
import shutil
import tempfile
import unittest
import uuid
from contextlib import asynccontextmanager, closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import test_console
import test_media
from astrbot_plugin_qq_chat_workbench import main, media
from astrbot_plugin_qq_chat_workbench.store import Store


class UpgradeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(
            patch.object(main, "get_astrbot_data_path", return_value=self.root)
        )
        self.legacy = self.root / "plugin_data" / "astrbot_plugin_super_admin"
        self.current = self.legacy.with_name("astrbot_plugin_qq_chat_workbench")
        self.routes = []
        self.context = SimpleNamespace(
            platform_manager=SimpleNamespace(platform_insts=[]),
            registered_web_apis=self.routes,
            register_web_api=lambda *args: self.routes.append(args),
        )

    @asynccontextmanager
    async def started(self):
        plugin = main.QQChatWorkbench(self.context)
        await plugin.initialize()
        try:
            yield plugin
        finally:
            await plugin.terminate()

    async def refused(self, reason):
        original = main.PipelineScheduler.execute
        routes = self.routes.copy()
        plugin = main.QQChatWorkbench(self.context)
        with self.assertRaisesRegex(RuntimeError, reason):
            await plugin.initialize()
        self.assertIs(main.PipelineScheduler.execute, original)
        self.assertEqual(self.routes, routes)
        self.assertFalse(plugin.active)
        self.assertFalse(hasattr(plugin, "store"))
        self.assertTrue(plugin.db_worker.snapshot()["closed"])

    async def test_fresh_install_only_uses_new_identity(self):
        async with self.started() as plugin:
            self.assertEqual(plugin.media_dir, self.current / "media")
            self.assertTrue((self.current / "console.db").is_file())
            self.assertFalse(self.legacy.exists())
            paths = [entry[0] for entry in self.routes]
            self.assertIn(f"/{self.current.name}/state", paths)
            self.assertTrue(
                all(path.startswith(f"/{self.current.name}/") for path in paths)
            )
        self.assertEqual(self.routes, [])

    async def test_migrates_database_with_wal_media_backup_and_pending_reply_once(self):
        self.legacy.mkdir(parents=True)
        image = test_media.picture()
        digest = media.write_copy(self.legacy / "media", image)
        backup = "console.before-background-link-1.db"
        (self.legacy / backup).write_bytes(b"previous backup")
        payload = '{"user_id":200,"message":"review me"}'
        with closing(Store(self.root / "console.db")) as store:
            store.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            store.db.execute("PRAGMA wal_autocheckpoint=0")
            chat = store.chat("qq", "999", "private", "200", title="User")
            message = store.message(
                chat,
                "t",
                "in",
                "User",
                [{"type": "image", "data": {}}],
                "received",
                "55",
            )
            store.media_record(message, 0, digest, "image/png", len(image), "saved", "")
            store.execute(
                "INSERT INTO jobs(trace,chat,status) VALUES('t',?,'running')", (chat,)
            )
            store.reserve_pending(
                "t", "qq", "send_private_msg", payload, "review me", len(payload)
            )
            for filename in ("console.db", "console.db-wal", "console.db-shm"):
                shutil.copyfile(self.root / filename, self.legacy / filename)

        for unused in range(2):
            async with self.started() as plugin:
                self.assertFalse(self.legacy.exists())
                self.assertEqual(
                    plugin.store.rows("SELECT chat,id FROM messages"),
                    [{"chat": chat, "id": message}],
                )
                self.assertEqual(
                    plugin.store.rows("SELECT status,payload FROM pending_sends"),
                    [{"status": "pending", "payload": payload}],
                )
                self.assertEqual(
                    plugin.store.media_states([message]), {message: {"0": "saved"}}
                )
                self.assertEqual(
                    media.copy_path(plugin.media_dir, digest).read_bytes(), image
                )
                self.assertEqual(
                    (self.current / backup).read_bytes(), b"previous backup"
                )

    async def test_conflicting_data_directories_remain_untouched(self):
        for directory in (self.legacy, self.current):
            directory.mkdir(parents=True)
            (directory / "console.db").write_text(directory.name)
        await self.refused("新旧数据目录同时存在")
        for directory in (self.legacy, self.current):
            self.assertEqual(
                [path.name for path in directory.iterdir()], ["console.db"]
            )
            self.assertEqual((directory / "console.db").read_text(), directory.name)

    async def test_separate_old_install_blocks_even_before_its_routes_load(self):
        old_code = self.root / "plugins" / self.legacy.name / "main.py"
        old_code.parent.mkdir(parents=True)
        old_code.touch()
        self.legacy.mkdir(parents=True)
        await self.refused("另一份工作台插件")
        self.assertTrue(self.legacy.is_dir())
        self.assertFalse(self.current.exists())

    async def test_existing_routes_block_even_if_old_code_directory_moved(self):
        self.routes.append((f"/{self.legacy.name}/state", None, ["GET"], ""))
        await self.refused("仍在运行")
        self.assertFalse(self.current.exists())

    async def test_in_place_update_can_run_from_previous_install_folder(self):
        current_code = self.root / "plugins" / self.legacy.name / "main.py"
        current_code.parent.mkdir(parents=True)
        current_code.touch()
        with patch.object(main, "__file__", str(current_code)):
            async with self.started() as plugin:
                self.assertEqual(plugin.media_dir, self.current / "media")


class MarketTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_console.ConsoleTests.asyncSetUp
    asyncTearDown = test_console.ConsoleTests.asyncTearDown
    post = test_console.ConsoleTests.post
    call = test_media.MediaTests.call
    ready = test_media.MediaTests.ready
    image_message = test_media.MediaTests.image_message

    async def test_first_push_and_next_incoming_message_share_one_repliable_chat(self):
        from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
            AiocqhttpMessageEvent,
        )

        plugin = self.plugin
        bot = plugin.clients["qq"]
        bot._wsr_api_clients = {"999": object()}
        bot.send_private_msg = lambda **params: bot.call_action(
            "send_private_msg", **params
        )
        outgoing = [{"type": "text", "data": {"text": "scheduled"}}]
        await AiocqhttpMessageEvent._dispatch_send(bot, None, False, "200", outgoing)
        await plugin.receive(
            test_media.message_event(
                {
                    "message_type": "private",
                    "self_id": 999,
                    "user_id": 200,
                    "message_id": 101,
                    "message": "hello",
                }
            ),
            "received",
        )
        await AiocqhttpMessageEvent._dispatch_send(bot, None, False, "200", outgoing)
        chats = plugin.store.rows("SELECT id,account FROM chats")
        self.assertEqual([row["account"] for row in chats], ["999"])
        self.assertEqual(
            [
                row["direction"]
                for row in plugin.store.rows(
                    "SELECT direction FROM messages ORDER BY id"
                )
            ],
            ["out", "in", "out"],
        )
        status, result = await self.post(
            "send",
            {"chat": chats[0]["id"], "text": "reply", "request_id": str(uuid.uuid4())},
        )
        self.assertEqual((status, result["status"]), (200, "accepted"))
        self.assertEqual(self.calls[-1][1]["self_id"], 999)
        self.assertNotIn("self_id", self.calls[0][1])

    async def test_ambiguous_or_disconnected_send_does_not_use_historical_account(self):
        plugin = self.plugin
        bot = plugin.clients["qq"]
        plugin.store.chat("qq", "888", "private", "200")
        for connections in ({}, {"999": object(), "888": object()}, {"*": object()}):
            bot._wsr_api_clients = connections
            self.assertEqual(
                await bot.call_action(
                    "send_private_msg", user_id=200, message="scheduled"
                ),
                {"message_id": 42},
            )
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(plugin.store.rows("SELECT * FROM messages"), [])
        self.assertEqual(plugin.dropped, 3)

    async def test_implicit_group_uses_connected_event_socket_and_current_client(self):
        plugin = self.plugin
        bot = plugin.clients["qq"]

        socket = unittest.mock.Mock(headers={"X-Self-ID": "888"})
        bot._wsr_event_clients = {socket}
        bot._wsr_api_clients = {"999": object(), "888": object()}
        with patch("astrbot_plugin_qq_chat_workbench.main.websocket", socket):
            await bot.call_action("send_group_msg", group_id=100, message="one")
            del bot._wsr_api_clients["888"]
            await bot.call_action("send_group_msg", group_id=100, message="two")
        await bot.call_action("send_group_msg", group_id=100, message="three")
        rows = plugin.store.rows(
            "SELECT c.account,c.kind,m.search FROM messages m JOIN chats c ON m.chat=c.id ORDER BY m.id"
        )
        self.assertEqual(
            [(row["account"], row["kind"], row["search"]) for row in rows],
            [("888", "group", "one"), ("999", "group", "three")],
        )

    async def test_missing_shared_image_is_invalidated_and_repaired_by_one_download(
        self,
    ):
        plugin = self.ready()
        _, first = self.image_message()
        _, second = self.image_message(remote_id="56")
        content = test_media.picture()
        plugin.fetch_image = AsyncMock(return_value=(content, "image/png"))
        for identifier in (first, second):
            await plugin.save_media(identifier, 1)
        digest = hashlib.sha256(content).hexdigest()
        path = media.copy_path(plugin.media_dir, digest)
        path.unlink()
        response = await self.call("media_file", {"id": first, "index": 1})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(plugin.media_state()["files"], 0)
        self.assertEqual(
            plugin.store.media_states([first, second]),
            {first: {"1": "missing"}, second: {"1": "missing"}},
        )
        before = plugin.fetch_image.await_count
        results = await asyncio.gather(
            *(plugin.save_media(first, 1, retry=True) for unused in range(2))
        )
        self.assertTrue(all(result["status"] == "saved" for result in results))
        self.assertEqual(plugin.fetch_image.await_count, before + 1)
        self.assertEqual(
            plugin.store.media_states([first, second]),
            {first: {"1": "saved"}, second: {"1": "saved"}},
        )
        self.assertEqual(path.read_bytes(), content)
        self.assertEqual(plugin.media_state()["files"], 1)

    async def test_explicit_retry_does_not_report_saved_if_source_expired(self):
        plugin = self.ready()
        _, identifier = self.image_message()
        content = test_media.picture()
        plugin.fetch_image = AsyncMock(return_value=(content, "image/png"))
        await plugin.save_media(identifier, 1)
        path = media.copy_path(plugin.media_dir, hashlib.sha256(content).hexdigest())
        path.write_bytes(b"incomplete")
        plugin.fetch_image.side_effect = media.MediaError("原地址已过期")
        status, result = await self.post(
            "media_save", {"id": identifier, "index": 1, "retry": True}
        )
        self.assertEqual(
            (status, result["status"], result["reason"]),
            (200, "failed", "原地址已过期"),
        )
        self.assertEqual(plugin.media_state()["bytes"], 0)

    async def test_database_only_restore_does_not_download_on_startup(self):
        plugin = self.ready()
        _, identifier = self.image_message()
        content = test_media.picture()
        digest = hashlib.sha256(content).hexdigest()
        plugin.store.media_record(
            identifier, 1, digest, "image/png", len(content), "saved", ""
        )
        plugin.fetch_image = AsyncMock(return_value=(content, "image/png"))
        await plugin.start_media(Path(self.temp.name))
        try:
            self.assertEqual(plugin.media_state()["files"], 0)
            self.assertEqual(
                plugin.store.media_states([identifier]), {identifier: {"1": "missing"}}
            )
            plugin.fetch_image.assert_not_awaited()
            status, result = await self.post(
                "media_save", {"id": identifier, "index": 1, "retry": True}
            )
            self.assertEqual((status, result["status"]), (200, "saved"))
            response = await self.call("media", {"id": identifier, "index": 1})
            self.assertTrue(
                json.loads(response.body)["url"].startswith("data:image/png;")
            )
        finally:
            await plugin.stop_media()
