"""Saved chat images, incoming recall marking and related 0.2.9 fixes."""

import asyncio
import base64
import hashlib
import io
import json
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import test_console as console_tests
import test_reliability as reliability_tests
from astrbot_plugin_super_admin import media as media_module
from astrbot_plugin_super_admin.main import SETTINGS, task_trace
from astrbot_plugin_super_admin.media import (
    MediaError,
    allowed_address,
    image_address,
    inline_copy,
    sniff,
    sweep_copies,
)
from astrbot_plugin_super_admin.store import trace_context
from PIL import Image

IMAGE_URL = "https://multimedia.nt.qq.com.cn/download?appid=1407&fileid=abc"


def picture(colour="red", size=(8, 8), image_format="PNG"):
    output = io.BytesIO()
    Image.new("RGB", size, colour).save(output, format=image_format)
    return output.getvalue()


async def until(predicate, timeout=3):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


def message_event(raw, extras=None):
    extras = extras or {}
    return SimpleNamespace(
        get_platform_name=lambda: "aiocqhttp",
        get_platform_id=lambda: "qq",
        get_sender_id=lambda: str(raw.get("user_id", "")),
        get_sender_name=lambda: "user",
        get_message_str=lambda: "",
        get_extra=lambda key, default=None: extras.get(key, default),
        set_extra=Mock(),
        stop_event=Mock(),
        unified_msg_origin="qq:FriendMessage:200",
        message_obj=SimpleNamespace(raw_message=raw, self_id="999", group=None),
    )


class FakeResponse:
    def __init__(self, status=200, body=b"", headers=None, length=None):
        self.status = status
        self.headers = headers or {}
        self.content_length = len(body) if length is None else length
        self.content = SimpleNamespace(iter_chunked=self.chunks)
        self.body = body

    async def chunks(self, size):
        for start in range(0, len(self.body), size):
            yield self.body[start : start + size]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class FakeSession:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requested = []
        self.closed = False

    def get(self, url, allow_redirects=True):
        assert allow_redirects is False
        self.requested.append(url)
        return self.responses.pop(0)

    async def close(self):
        self.closed = True


class MediaTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = console_tests.ConsoleTests.asyncSetUp
    asyncTearDown = console_tests.ConsoleTests.asyncTearDown
    post = console_tests.ConsoleTests.post
    get = console_tests.ConsoleTests.get
    initialized = reliability_tests.ReliabilityTests.initialized

    async def call(self, method, query):
        """GET a route and return (status, json), also for error responses."""
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
        return response

    def ready(self, mode="manual"):
        p = self.plugin
        p._ensure_media()
        p.media_dir = Path(self.temp.name) / "media"
        p.console_values = {"image_save": mode}
        return p

    def image_message(self, url=IMAGE_URL, remote_id="55", direction="in"):
        p = self.plugin
        chat = p.store.chat("qq", "999", "private", "200", title="User")
        message_id = p.store.message(
            chat,
            "t-" + uuid.uuid4().hex,
            direction,
            "User",
            [
                {"type": "text", "data": {"text": "look"}},
                {"type": "image", "data": {"url": url, "file": "a.jpg"}},
            ],
            "received",
            remote_id,
        )
        return chat, message_id

    def copies(self):
        root = Path(self.temp.name) / "media"
        return sorted(p.name for p in root.rglob("*") if p.is_file())

    # --- pure helpers -----------------------------------------------------

    def test_only_qq_image_hosts_are_contacted(self):
        for url in (
            IMAGE_URL,
            "http://gchat.qpic.cn/gchatpic_new/0/0-0-A/0",
            "https://gxh.vip.qq.com/club/item/parcel/item/a/b.png",
            "https://QQ.COM/x",
        ):
            self.assertTrue(allowed_address(url), url)
        for url in (
            "https://example.com/a.jpg",
            "https://qq.com.cn.evil.example/a.jpg",
            "https://evilqq.com/a.jpg",
            "https://multimedia.nt.qq.com.cn:8443/a.jpg",
            "https://user:pw@multimedia.nt.qq.com.cn/a.jpg",
            "ftp://multimedia.nt.qq.com.cn/a.jpg",
            "file:///etc/passwd",
            "http://127.0.0.1/a.jpg",
            "http://169.254.169.254/latest/meta-data",
            "https://[::1]/a.jpg",
            "http://[bad/a.jpg",
            # A name the resolver would cut short, an address literal with a
            # zone, and spellings that two URL parsers read differently.
            "http://evil.example\x00.qq.com/a.jpg",
            "http://127.0.0.1\x00.qq.com/a.jpg",
            "http://[::ffff:127.0.0.1%25.qq.com]/a.jpg",
            "http://evil.example\\.qq.com/a.jpg",
            "http://evil.example\\@multimedia.nt.qq.com.cn/a.jpg",
            "http://multimedia.nt.qq.com.cn@evil.example/a.jpg",
            "http://evil.example#@multimedia.nt.qq.com.cn/a.jpg",
            "http://evil.example/?x=@multimedia.nt.qq.com.cn/a.jpg",
            "http://evil.example /a.jpg#.qq.com",
            "http://qq_com.qq.com/a.jpg",
            "base64://AAAA",
            "",
            None,
        ):
            self.assertFalse(allowed_address(url), url)
        self.assertEqual(image_address({"file": "a.jpg", "url": IMAGE_URL}), IMAGE_URL)
        self.assertEqual(image_address({"file": IMAGE_URL}), IMAGE_URL)
        self.assertEqual(image_address({"file": "a.jpg"}), "")
        self.assertEqual(image_address(None), "")

    def test_type_is_decided_by_content(self):
        self.assertEqual(sniff(picture()), "image/png")
        self.assertEqual(sniff(picture(image_format="JPEG")), "image/jpeg")
        self.assertEqual(sniff(picture(image_format="GIF")), "image/gif")
        self.assertEqual(sniff(picture(image_format="WEBP")), "image/webp")
        for payload in (b"", b"<html>expired</html>", b"RIFFxxxxWAVE", b"\xff\xd8"):
            self.assertEqual(sniff(payload), "")

    # --- storage ------------------------------------------------------------

    async def test_schema_adds_only_the_media_table_and_indexes(self):
        names = {
            row["name"]
            for row in self.plugin.store.rows(
                "SELECT name FROM sqlite_master WHERE name LIKE 'media%'"
            )
        }
        self.assertEqual(names, {"media", "media_sha", "media_created"})

    async def test_manual_save_stores_one_file_and_reports_it(self):
        p = self.ready()
        chat, message_id = self.image_message()
        payload = picture()
        p.fetch_image = AsyncMock(return_value=(payload, "image/png"))
        status, result = await self.post("media_save", {"id": message_id, "index": 1})
        self.assertEqual(status, 200)
        self.assertEqual(
            result,
            {
                "status": "saved",
                "reason": "",
                "mime": "image/png",
                "bytes": len(payload),
            },
        )
        # The address comes from the stored message, never from the request.
        p.fetch_image.assert_awaited_once_with(IMAGE_URL)
        digest = hashlib.sha256(payload).hexdigest()
        self.assertEqual(self.copies(), [digest])
        self.assertEqual(
            (Path(self.temp.name) / "media" / digest[:2] / digest).read_bytes(), payload
        )
        # Saving again reuses the copy without another download.
        status, again = await self.post("media_save", {"id": message_id, "index": 1})
        self.assertEqual((status, again["status"]), (200, "saved"))
        p.fetch_image.assert_awaited_once()
        rows = await self.get("messages", {"chat": chat})
        self.assertEqual(rows[0]["media"], {"1": "saved"})
        self.assertEqual(p.media_state()["files"], 1)
        self.assertEqual(p.media_state()["bytes"], len(payload))

    async def test_save_rejects_what_is_not_a_stored_incoming_image(self):
        p = self.ready()
        p.fetch_image = AsyncMock(return_value=(picture(), "image/png"))
        _chat, message_id = self.image_message()
        # The bot's own image, even with a QQ address, is not downloaded.
        _, sent = self.image_message(remote_id="56", direction="out")
        for body in (
            {"id": message_id, "index": 0},  # a text segment
            {"id": message_id, "index": 2},
            {"id": message_id, "index": -1},
            {"id": message_id + 99, "index": 1},
            {"id": sent, "index": 1},
        ):
            status, result = await self.post("media_save", body)
            self.assertEqual(status, 400, body)
        for body in (
            {"id": str(message_id), "index": 1},
            {"id": message_id, "index": True},
            {"id": message_id, "index": 1.0},
            {"index": 1},
            ["id"],
        ):
            status, result = await self.post("media_save", body)
            self.assertEqual((status, result["message"]), (400, "无效请求"), body)
        # A client-supplied address is ignored.
        await self.post(
            "media_save",
            {"id": message_id, "index": 1, "url": "https://example.com/x.png"},
        )
        p.fetch_image.assert_awaited_once_with(IMAGE_URL)

    async def test_address_outside_qq_is_recorded_as_failed_without_a_request(self):
        p = self.ready()
        p.fetch_image = AsyncMock()
        chat, message_id = self.image_message(url="https://example.com/a.png")
        status, result = await self.post("media_save", {"id": message_id, "index": 1})
        self.assertEqual((status, result["status"]), (200, "failed"))
        self.assertIn("没有可下载的地址", result["reason"])
        p.fetch_image.assert_not_awaited()
        self.assertEqual(
            (await self.get("messages", {"chat": chat}))[0]["media"], {"1": "failed"}
        )

    async def test_failed_download_is_not_repeated_until_asked(self):
        p = self.ready()
        chat, message_id = self.image_message()
        p.fetch_image = AsyncMock(
            side_effect=MediaError("QQ 图片服务器返回 404，图片可能已过期")
        )
        first = await p.save_media(message_id, 1)
        self.assertEqual(first["status"], "failed")
        self.assertIn("404", first["reason"])
        # Automatic saving does not hammer an expired address ...
        self.assertEqual((await p.save_media(message_id, 1))["status"], "failed")
        p.fetch_image.assert_awaited_once()
        # ... the 保存 button does retry, and a later success replaces the failure.
        p.fetch_image = AsyncMock(return_value=(picture(), "image/png"))
        status, result = await self.post(
            "media_save", {"id": message_id, "index": 1, "retry": True}
        )
        self.assertEqual((status, result["status"]), (200, "saved"))
        self.assertEqual(
            (await self.get("messages", {"chat": chat}))[0]["media"], {"1": "saved"}
        )

    async def test_network_errors_become_a_readable_failure(self):
        import aiohttp

        p = self.ready()
        for error in (
            aiohttp.ClientError("boom"),
            asyncio.TimeoutError(),
            OSError(),
            ValueError("Forbidden control character detected in headers"),
        ):
            _chat, message_id = self.image_message(remote_id=uuid.uuid4().hex)
            p.fetch_image = AsyncMock(side_effect=error)
            result = await p.save_media(message_id, 1)
            self.assertEqual(result["status"], "failed")
            self.assertIn("下载失败", result["reason"])
        self.assertEqual(self.copies(), [])

    async def test_reload_refuses_new_downloads_without_recording_a_failure(self):
        import aiohttp

        p = self.ready()
        _, message_id = self.image_message()
        p.fetch_image = AsyncMock(return_value=(picture(), "image/png"))
        p.terminating = True
        result = await p.save_media(message_id, 1)
        self.assertEqual(result["status"], "failed")
        self.assertIn("正在重载", result["reason"])
        p.fetch_image.assert_not_awaited()
        # A download cut off by the reload is not held against the image either.
        p.terminating = False

        async def cut(url):
            p.terminating = True
            raise aiohttp.ClientError("session closed")

        p.fetch_image = AsyncMock(side_effect=cut)
        self.assertIn("正在重载", (await p.save_media(message_id, 1))["reason"])
        self.assertEqual(p.store.rows("SELECT * FROM media"), [])
        p.terminating = False
        p.fetch_image = AsyncMock(return_value=(picture(), "image/png"))
        self.assertEqual((await p.save_media(message_id, 1))["status"], "saved")

    async def test_session_is_closed_only_after_handlers_have_settled(self):
        p = self.plugin
        await self.initialized()
        session = p.media_session = FakeSession()
        order = []
        settle = p.settle_owners

        async def settled():
            order.append(("settle", session.closed))
            return await settle()

        p.settle_owners = settled
        await asyncio.wait_for(p.terminate(), 5)
        self.assertEqual(order, [("settle", False)])
        self.assertTrue(session.closed)
        self.assertIsNone(p.media_session)
        self.assertEqual(p.media_tasks, [])

    async def test_identical_images_share_one_file(self):
        p = self.ready()
        payload = picture("blue")
        p.fetch_image = AsyncMock(return_value=(payload, "image/png"))
        _, first = self.image_message(remote_id="1")
        _, second = self.image_message(remote_id="2")
        await p.save_media(first, 1)
        await p.save_media(second, 1)
        self.assertEqual(len(self.copies()), 1)
        self.assertEqual(p.media_state()["files"], 1)
        self.assertEqual(p.media_state()["bytes"], len(payload))
        # The shared file goes only with its last message.
        p.store.execute("DELETE FROM messages WHERE id=?", (first,))
        self.assertFalse(await p.prune_media())
        self.assertEqual(len(self.copies()), 1)
        p.store.execute("DELETE FROM messages WHERE id=?", (second,))
        await p.prune_media()
        self.assertEqual(self.copies(), [])
        self.assertEqual(p.store.rows("SELECT * FROM media"), [])
        self.assertEqual(p.media_state()["files"], 0)

    async def test_concurrent_requests_download_once(self):
        p = self.ready()
        _, message_id = self.image_message()
        entered, release = asyncio.Event(), asyncio.Event()

        async def fetch(url):
            entered.set()
            await release.wait()
            return picture(), "image/png"

        p.fetch_image = AsyncMock(side_effect=fetch)
        first = asyncio.create_task(p.save_media(message_id, 1))
        await entered.wait()
        second = asyncio.create_task(p.save_media(message_id, 1, retry=True))
        await asyncio.sleep(0.02)
        release.set()
        self.assertEqual((await first)["status"], "saved")
        self.assertEqual((await second)["status"], "saved")
        p.fetch_image.assert_awaited_once()
        self.assertEqual(p.media_inflight, {})

    async def test_low_disk_space_pauses_saving_without_recording_a_failure(self):
        p = self.ready()
        _chat, message_id = self.image_message()
        p.fetch_image = AsyncMock(return_value=(picture(), "image/png"))
        with patch.object(
            media_module, "free_bytes", return_value=media_module.FREE_FLOOR - 1
        ):
            result = await p.save_media(message_id, 1)
        self.assertEqual(result["status"], "failed")
        self.assertIn("磁盘空间不足", result["reason"])
        p.fetch_image.assert_not_awaited()
        self.assertEqual(p.store.rows("SELECT * FROM media"), [])
        # Once space is back the same image saves at once.
        self.assertEqual((await p.save_media(message_id, 1))["status"], "saved")

    async def test_oldest_copies_go_first_when_over_the_limit(self):
        p = self.ready()
        ids = []
        for index, colour in enumerate(("red", "green", "blue")):
            _, message_id = self.image_message(remote_id=str(index))
            p.fetch_image = AsyncMock(
                return_value=(picture(colour, (16 + index, 16)), "image/png")
            )
            await p.save_media(message_id, 1)
            p.store.execute(
                "UPDATE media SET created=? WHERE message_id=?",
                (1000 + index, message_id),
            )
            ids.append(message_id)
        sizes = {
            row["message_id"]: row["bytes"]
            for row in p.store.rows("SELECT * FROM media")
        }
        removed = p.store.media_trim(sizes[ids[1]] + sizes[ids[2]])
        self.assertEqual(len(removed), 1)
        self.assertEqual(
            [
                row["message_id"]
                for row in p.store.rows(
                    "SELECT message_id FROM media ORDER BY message_id"
                )
            ],
            ids[1:],
        )
        # The settings route applies a new limit immediately.
        self.assertEqual(len(self.copies()), 3)
        with patch.dict(SETTINGS, {"image_quota": (1024, 0)}):
            status, body = await self.post("settings", {"image_quota": 0})
        self.assertEqual((status, body["image_quota"]), (200, 0))
        self.assertEqual(p.store.rows("SELECT * FROM media"), [])
        self.assertEqual(p.media_state()["bytes"], 0)
        # Only the file trimmed directly through the store above is left for the sweep.
        self.assertEqual(len(self.copies()), 1)
        self.assertEqual(sweep_copies(p.media_dir, set()), 1)
        self.assertEqual(self.copies(), [])

    async def test_shared_file_is_trimmed_with_its_newest_message(self):
        p = self.ready()
        shared, other = picture("blue", (40, 40)), picture("red", (41, 40))
        ids = []
        # Oldest to newest: shared, other, shared again.
        for index, payload in enumerate((shared, other, shared)):
            _, message_id = self.image_message(remote_id=str(index))
            p.fetch_image = AsyncMock(return_value=(payload, "image/png"))
            await p.save_media(message_id, 1)
            p.store.execute(
                "UPDATE media SET created=? WHERE message_id=?",
                (1000 + index, message_id),
            )
            ids.append(message_id)

        def kept():
            return [
                row["message_id"]
                for row in p.store.rows(
                    "SELECT message_id FROM media ORDER BY message_id"
                )
            ]

        # The oldest row shares its file with the newest one: dropping it would
        # free nothing, so the unshared file goes and both messages keep theirs.
        self.assertEqual(
            p.store.media_trim(len(shared)), [hashlib.sha256(other).hexdigest()]
        )
        self.assertEqual(kept(), [ids[0], ids[2]])
        # When the shared file's own turn comes, every row using it goes with it.
        self.assertEqual(
            p.store.media_trim(len(shared) - 1), [hashlib.sha256(shared).hexdigest()]
        )
        self.assertEqual(kept(), [])
        self.assertEqual(p.store.media_trim(0), [])

    async def test_copies_of_pruned_messages_are_removed_in_pages(self):
        p = self.ready()
        _, kept = self.image_message(remote_id="keep")
        _, gone = self.image_message(remote_id="gone")
        p.fetch_image = AsyncMock(return_value=(picture("red"), "image/png"))
        await p.save_media(kept, 1)
        p.fetch_image = AsyncMock(return_value=(picture("green"), "image/png"))
        await p.save_media(gone, 1)
        p.store.media_record(gone, 5, "", "", 0, "failed", "x")
        p.store.execute("DELETE FROM messages WHERE id=?", (gone,))
        real = p.store.media_orphans
        p.store.media_orphans = lambda size: real(1)
        # One row per round: the saved copy, the failed attempt, then nothing.
        self.assertTrue(await p.prune_media())
        self.assertTrue(await p.prune_media())
        self.assertFalse(await p.prune_media())
        self.assertEqual(
            [row["message_id"] for row in p.store.rows("SELECT message_id FROM media")],
            [kept],
        )
        self.assertEqual(self.copies(), [hashlib.sha256(picture("red")).hexdigest()])

    async def test_start_sweeps_leftovers_and_workers_carry_no_trace(self):
        p = self.ready()
        _, message_id = self.image_message()
        p.fetch_image = AsyncMock(return_value=(picture(), "image/png"))
        await p.save_media(message_id, 1)
        kept = self.copies()
        folder = Path(self.temp.name) / "media" / "zz"
        folder.mkdir(parents=True)
        (folder / "abc.123.part").write_bytes(b"partial")
        (folder / ("f" * 64)).write_bytes(b"orphan")
        token = trace_context.set("some-job")
        try:
            await p.start_media(Path(self.temp.name))
        finally:
            trace_context.reset(token)
        try:
            self.assertEqual(self.copies(), kept)
            self.assertEqual(len(p.media_tasks), media_module.WORKERS)
            # A download worker is never part of a job: stop and the idle gate skip it.
            self.assertEqual([task_trace(task) for task in p.media_tasks], [None, None])
            self.assertEqual(p.media_state()["files"], 1)
        finally:
            await p.stop_media()
        self.assertEqual(p.media_tasks, [])

    # --- automatic saving ---------------------------------------------------

    async def test_all_mode_queues_incoming_images_without_waiting(self):
        p = self.ready("all")
        p.media_tasks = [object()]
        p.queue_images(
            7, ["junk", {"type": "text"}, {"type": "image"}, {"type": "image"}]
        )
        self.assertEqual(
            [p.media_queue.get_nowait() for _ in range(2)], [(7, 1), (7, 2)]
        )
        p.queue_images(8, "plain [CQ:image,file=x]")
        self.assertTrue(p.media_queue.empty())
        for mode in ("manual", "viewed"):
            p.console_values = {"image_save": mode}
            p.queue_images(9, [{"type": "image"}])
        self.assertTrue(p.media_queue.empty())
        # Not running (during reload): nothing is queued.
        p.console_values = {"image_save": "all"}
        p.media_tasks = []
        p.queue_images(9, [{"type": "image"}])
        self.assertTrue(p.media_queue.empty())

    async def test_full_queue_drops_instead_of_blocking(self):
        p = self.ready("all")
        p.media_tasks = [object()]
        p.media_queue = asyncio.Queue(1)
        p.queue_images(1, [{"type": "image"}, {"type": "image"}])
        self.assertEqual(p.media_queue.qsize(), 1)
        self.assertEqual(p.media_state()["skipped"], 1)

    async def test_received_image_is_saved_in_the_background_and_survives_a_recall(
        self,
    ):
        p = self.ready("all")
        payload = picture("purple")
        fetched = asyncio.Event()

        async def fetch(url):
            fetched.set()
            return payload, "image/png"

        p.fetch_image = AsyncMock(side_effect=fetch)
        await p.start_media(Path(self.temp.name))
        try:
            raw = {
                "post_type": "message",
                "message_type": "group",
                "self_id": 999,
                "user_id": 200,
                "group_id": 123,
                "message_id": 4242,
                "message": [
                    {"type": "image", "data": {"url": IMAGE_URL, "file": "a.png"}}
                ],
            }
            chat, message_id, _ = await p.receive(message_event(raw), "incoming")
            await asyncio.wait_for(fetched.wait(), 3)
            await until(
                lambda: p.store.rows("SELECT 1 FROM media WHERE status='saved'")
            )
            # The sender recalls it: the record is marked, the copy stays.
            notice = {
                "post_type": "notice",
                "notice_type": "group_recall",
                "self_id": 999,
                "group_id": 123,
                "user_id": 200,
                "operator_id": 200,
                "message_id": 4242,
            }
            self.assertIsNone(await p.receive(message_event(notice), "notice"))
        finally:
            await p.stop_media()
        rows = await self.get("messages", {"chat": chat})
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            (rows[0]["status"], rows[0]["media"]), ("recalled", {"0": "saved"})
        )
        self.assertEqual(rows[0]["body"][0]["data"]["url"], IMAGE_URL)
        response = await self.call("media", {"id": message_id, "index": 0})
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.body)
        self.assertEqual(
            data["url"], "data:image/png;base64," + base64.b64encode(payload).decode()
        )
        chats = await self.get("chats", {})
        self.assertTrue(chats[0]["preview"].startswith("[已撤回]"))

    # --- recall notices -------------------------------------------------------

    async def test_recall_marks_only_the_named_message_in_its_own_chat(self):
        p = self.plugin
        group = p.store.chat("qq", "999", "group", "123", title="Group")
        other_group = p.store.chat("qq", "999", "group", "456", title="Other")
        friend = p.store.chat("qq", "999", "private", "200", title="User")
        temporary = p.store.chat("qq", "999", "temporary", "300", "123", "Temp")
        other_account = p.store.chat("qq", "111", "private", "200", title="User")
        # A group and a friend can have the same number.
        group_numbered_like_friend = p.store.chat("qq", "999", "group", "200")
        friend_numbered_like_group = p.store.chat("qq", "999", "private", "123")
        ids = {
            name: p.store.message(chat, "t", "in", "User", "hello", "received", "777")
            for name, chat in (
                ("group", group),
                ("other_group", other_group),
                ("friend", friend),
                ("temporary", temporary),
                ("other_account", other_account),
                ("group_numbered_like_friend", group_numbered_like_friend),
                ("friend_numbered_like_group", friend_numbered_like_group),
            )
        }
        before = p.store.rows("SELECT COUNT(*) AS n FROM messages")[0]["n"]

        def statuses():
            return {
                name: p.store.rows("SELECT status FROM messages WHERE id=?", (i,))[0][
                    "status"
                ]
                for name, i in ids.items()
            }

        notice = {
            "post_type": "notice",
            "notice_type": "group_recall",
            "self_id": 999,
            "group_id": 123,
            "user_id": 200,
            "message_id": 777,
        }
        self.assertIsNone(await p.receive(message_event(notice), "n1"))
        self.assertEqual(
            statuses(),
            {
                "group": "recalled",
                "other_group": "received",
                "friend": "received",
                "temporary": "received",
                "other_account": "received",
                "group_numbered_like_friend": "received",
                "friend_numbered_like_group": "received",
            },
        )
        notice = {
            "post_type": "notice",
            "notice_type": "friend_recall",
            "self_id": 999,
            "user_id": 200,
            "message_id": 777,
        }
        self.assertIsNone(await p.receive(message_event(notice), "n2"))
        self.assertEqual(statuses()["friend"], "recalled")
        self.assertEqual(statuses()["other_account"], "received")
        self.assertEqual(statuses()["temporary"], "received")
        self.assertEqual(statuses()["group_numbered_like_friend"], "received")
        self.assertEqual(statuses()["friend_numbered_like_group"], "received")
        # A recall creates no message, chat or job, and unknown ids change nothing.
        for message_id in ("", 0, 999999):
            notice["message_id"] = message_id
            self.assertIsNone(await p.receive(message_event(notice), "n3"))
        self.assertEqual(
            p.store.rows("SELECT COUNT(*) AS n FROM messages")[0]["n"], before
        )
        self.assertEqual(
            p.store.rows("SELECT body FROM messages WHERE id=?", (ids["group"],))[0][
                "body"
            ],
            json.dumps(
                [{"type": "text", "data": {"text": "hello"}}], ensure_ascii=False
            ),
        )
        # Other notices are still ignored.
        poke = {
            "post_type": "notice",
            "notice_type": "notify",
            "sub_type": "poke",
            "self_id": 999,
            "group_id": 456,
            "user_id": 200,
        }
        self.assertIsNone(await p.receive(message_event(poke), "n4"))
        self.assertEqual(statuses()["other_group"], "received")

    # --- routes ---------------------------------------------------------------

    async def test_saved_copy_routes(self):
        p = self.ready()
        _, message_id = self.image_message()
        payload = picture(image_format="JPEG")
        p.fetch_image = AsyncMock(return_value=(payload, "image/jpeg"))
        for route in ("media", "media_file"):
            response = await self.call(route, {"id": message_id, "index": 1})
            self.assertEqual(response.status_code, 400, route)
        await p.save_media(message_id, 1)
        response = await self.call("media_file", {"id": message_id, "index": 1})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(Path(response.path).read_bytes(), payload)
        self.assertEqual(response.media_type, "image/jpeg")
        self.assertIn(
            f"qq-image-{message_id}-1.jpg", response.headers["content-disposition"]
        )
        # Only whole numbers select a copy; nothing in the request reaches the path.
        for query in ({"id": "../../x", "index": 1}, {"id": message_id}, {}):
            for route in ("media", "media_file"):
                self.assertEqual((await self.call(route, query)).status_code, 400)
        # A copy deleted behind the plugin's back is reported, not served.
        Path(response.path).unlink()
        for route in ("media", "media_file"):
            self.assertEqual(
                (await self.call(route, {"id": message_id, "index": 1})).status_code,
                400,
            )

    async def test_large_copy_is_downscaled_for_the_page_only(self):
        big = picture("orange", (2400, 1800))
        folder = Path(self.temp.name)
        (folder / "big").write_bytes(big)
        with patch.object(media_module, "INLINE_LIMIT", 1000):
            url = inline_copy(folder / "big", "image/png")
        self.assertTrue(url.startswith("data:image/jpeg;base64,"))
        with Image.open(io.BytesIO(base64.b64decode(url.split(",", 1)[1]))) as shown:
            self.assertEqual(shown.size, (1600, 1200))
        self.assertEqual((folder / "big").read_bytes(), big)
        self.assertEqual(inline_copy(folder / "missing", "image/png"), "")
        (folder / "broken").write_bytes(b"x" * 2000)
        with patch.object(media_module, "INLINE_LIMIT", 1000):
            self.assertEqual(inline_copy(folder / "broken", "image/png"), "")
        # Too many pixels to decode on a small server: download only.
        with (
            patch.object(media_module, "INLINE_LIMIT", 1000),
            patch.object(media_module, "PREVIEW_PIXELS", 2400 * 1800 - 1),
        ):
            self.assertEqual(inline_copy(folder / "big", "image/png"), "")
        # A JPEG is decoded reduced, so the same pixel limit still shows it.
        photo = io.BytesIO()
        Image.new("RGB", (4000, 3000), "navy").save(photo, format="JPEG")
        (folder / "photo").write_bytes(photo.getvalue())
        with (
            patch.object(media_module, "INLINE_LIMIT", 1000),
            patch.object(media_module, "PREVIEW_PIXELS", 2000 * 1500),
        ):
            url = inline_copy(folder / "photo", "image/jpeg")
        with Image.open(io.BytesIO(base64.b64decode(url.split(",", 1)[1]))) as shown:
            self.assertEqual(shown.size, (1600, 1200))

    async def test_state_reports_saved_images_without_the_database(self):
        p = self.ready("viewed")
        state = json.loads((await p.state()).body)
        self.assertEqual(
            state["images"],
            {
                "mode": "viewed",
                "quota_mb": 1024,
                "files": 0,
                "bytes": 0,
                "queued": 0,
                "skipped": 0,
            },
        )

    async def test_image_settings_are_validated_and_cached(self):
        p = self.ready()
        status, body = await self.post(
            "settings", {"image_save": "all", "image_quota": 2048}
        )
        self.assertEqual(status, 200)
        self.assertEqual((body["image_save"], body["image_quota"]), ("all", 2048))
        self.assertEqual(p.console_setting("image_save"), "all")
        self.assertEqual(p.console_setting("image_quota"), 2048)
        for bad in (
            {"image_save": "everything"},
            {"image_quota": "2048"},
            {"image_quota": 100},
            {"image_quota": True},
            {"image_quota": 2048.0},
        ):
            status, _ = await self.post("settings", bad)
            self.assertEqual(status, 400, bad)
        self.assertEqual(p.console_setting("image_quota"), 2048)

    # --- downloading ----------------------------------------------------------

    async def test_download_follows_redirects_only_to_qq_hosts(self):
        p = self.ready()
        payload = picture()
        p.media_session = FakeSession(
            FakeResponse(302, headers={"Location": "/real?x=1"}),
            FakeResponse(200, payload),
        )
        self.assertEqual(await p.fetch_image(IMAGE_URL), (payload, "image/png"))
        self.assertEqual(
            p.media_session.requested,
            [IMAGE_URL, "https://multimedia.nt.qq.com.cn/real?x=1"],
        )
        session = p.media_session = FakeSession(
            FakeResponse(302, headers={"Location": "http://169.254.169.254/latest"}),
            FakeResponse(200, payload),
        )
        with self.assertRaises(MediaError):
            await p.fetch_image(IMAGE_URL)
        self.assertEqual(session.requested, [IMAGE_URL])
        p.media_session = FakeSession(
            *(FakeResponse(302, headers={"Location": IMAGE_URL}) for _ in range(4))
        )
        with self.assertRaisesRegex(MediaError, "跳转次数过多"):
            await p.fetch_image(IMAGE_URL)

    async def test_download_rejects_errors_oversize_and_non_images(self):
        p = self.ready()
        p.media_session = FakeSession(FakeResponse(404, b"gone"))
        with self.assertRaisesRegex(MediaError, "404"):
            await p.fetch_image(IMAGE_URL)
        p.media_session = FakeSession(FakeResponse(200, b"<html>expired</html>"))
        with self.assertRaisesRegex(MediaError, "不是图片"):
            await p.fetch_image(IMAGE_URL)
        with patch.object(media_module, "IMAGE_LIMIT", 100):
            # Declared too large: refused before reading.
            p.media_session = FakeSession(FakeResponse(200, picture(), length=101))
            with self.assertRaisesRegex(MediaError, "16 MB"):
                await p.fetch_image(IMAGE_URL)
            # Undeclared size: reading stops at the limit.
            p.media_session = FakeSession(
                FakeResponse(200, picture("red", (300, 300)) + b"x" * 200, length=0)
            )
            with self.assertRaisesRegex(MediaError, "16 MB"):
                await p.fetch_image(IMAGE_URL)
        session = p.media_session
        await p.close_media()
        self.assertTrue(session.closed)
        self.assertIsNone(p.media_session)

    # --- guided completion without a source message --------------------------

    async def completion(
        self, parent_started, parent_chat_target="200", mode="confirm", newer=None
    ):
        p = self.plugin
        scheduler, original = await self.initialized()
        chat = p.store.chat("qq", "999", "private", parent_chat_target, title="User")
        p.store.execute(
            "INSERT INTO jobs(trace,chat,umo,prompt,status,started,source_message_id,sender_id,reply_mode) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                "parent",
                chat,
                "qq:FriendMessage:200",
                "画一只猫",
                "completed",
                parent_started,
                None,
                "200",
                mode,
            ),
        )
        if newer:
            p.store.execute(
                "INSERT INTO jobs(trace,chat,umo,prompt,status,started,source_message_id,sender_id,reply_mode) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    "newer",
                    chat,
                    "qq:FriendMessage:200",
                    "画一只狗",
                    "completed",
                    parent_started + 30,
                    None,
                    "200",
                    newer,
                ),
            )
        p.store.execute(
            "INSERT INTO guidance_instructions VALUES(?,?,?,?,?,?,?)",
            ("req-1", "fp", "parent", "user", None, "为用户画一只猫", parent_started),
        )
        raw = {
            "post_type": "message",
            "message_type": "private",
            "self_id": 999,
            "user_id": 200,
            "message_id": 0,
            "message": [],
        }
        event = message_event(
            raw,
            {
                "_reference_image_completion": True,
                "_reference_image_completion_facts": {"original_request": "画一只猫"},
            },
        )
        seen = {}

        async def pipeline(scheduler_, event_, *args, **kwargs):
            seen["trace"] = trace_context.get()
            seen["guided"] = p.is_guided(seen["trace"])
            return "ran"

        original.side_effect = pipeline
        self.assertEqual(await scheduler.execute(None, event), "ran")
        job = p.store.rows("SELECT * FROM jobs WHERE trace=?", (seen["trace"],))[0]
        copied = p.store.rows(
            "SELECT guidance FROM guidance_instructions WHERE trace=?", (seen["trace"],)
        )
        await asyncio.wait_for(p.terminate(), 5)
        return seen, job, copied, event

    async def test_completion_of_guidance_without_a_message_is_reviewed(self):
        seen, job, copied, event = await self.completion(time.time() - 120)
        self.assertTrue(seen["guided"])
        self.assertEqual(
            (job["reply_mode"], job["parent_trace"]), ("confirm", "parent")
        )
        self.assertIsNone(job["source_message_id"])
        self.assertEqual([row["guidance"] for row in copied], ["为用户画一只猫"])
        event.set_extra.assert_any_call("_super_admin_guided_trace", seen["trace"])
        event.set_extra.assert_any_call("enable_streaming", False)

    async def test_completion_among_mixed_tasks_is_reviewed(self):
        # A task waiting for review and a newer automatic one in the same chat:
        # which of them finished cannot be told, so the result is reviewed.
        seen, job, _, _ = await self.completion(time.time() - 120, newer="auto")
        self.assertTrue(seen["guided"])
        self.assertEqual(
            (job["reply_mode"], job["parent_trace"]), ("confirm", "parent")
        )

    async def test_completion_follows_the_newest_task_of_one_kind(self):
        _seen, job, _, _ = await self.completion(
            time.time() - 120, mode="auto", newer="auto"
        )
        self.assertEqual((job["reply_mode"], job["parent_trace"]), ("auto", "newer"))

    async def test_completion_does_not_inherit_from_an_old_or_other_chat_task(self):
        seen, job, copied, _ = await self.completion(time.time() - 7 * 3600)
        self.assertFalse(seen["guided"])
        self.assertEqual((job["reply_mode"], job["parent_trace"]), ("", ""))
        self.assertEqual(copied, [])

    async def test_completion_in_another_chat_is_left_alone(self):
        seen, job, _copied, _ = await self.completion(time.time() - 60, "201")
        self.assertFalse(seen["guided"])
        self.assertEqual(job["reply_mode"], "")

    # --- repeated send ----------------------------------------------------------

    async def test_repeated_request_reports_how_the_first_attempt_ended(self):
        p = self.plugin
        chat = p.store.chat("qq", "999", "private", "200", title="User")
        body = {"chat": chat, "text": "hello", "request_id": str(uuid.uuid4())}
        status, first = await self.post("send", body)
        self.assertEqual((status, first["status"]), (200, "accepted"))
        status, again = await self.post("send", body)
        self.assertEqual(status, 200)
        self.assertEqual(
            again,
            {"trace": first["trace"], "duplicate": True, "status": "accepted"},
        )
        self.assertEqual(len(self.calls), 1)
        # A first attempt that failed is reported as such and still not re-sent.
        failing = AsyncMock(side_effect=RuntimeError("socket closed"))
        p.clients["qq"] = SimpleNamespace(call_action=p.wrap_sender(failing, "qq"))
        body = {"chat": chat, "text": "second", "request_id": str(uuid.uuid4())}
        status, _ = await self.post("send", body)
        self.assertEqual(status, 400)
        status, again = await self.post("send", body)
        self.assertEqual((status, again["duplicate"]), (200, True))
        self.assertNotEqual(again["status"], "accepted")
        failing.assert_awaited_once()

    async def test_routes_are_registered_and_removed_with_the_plugin(self):
        p = self.plugin
        await self.initialized()
        paths = {route[0] for route in p.context.registered_web_apis}
        for name in ("media", "media_save", "media_file"):
            self.assertIn(f"/astrbot_plugin_super_admin/{name}", paths)
        self.assertEqual(len(p.media_tasks), media_module.WORKERS)
        self.assertEqual(
            p.media_dir,
            Path(self.temp.name)
            / "plugin_data"
            / "astrbot_plugin_super_admin"
            / "media",
        )
        tasks = list(p.media_tasks)
        await asyncio.wait_for(p.terminate(), 5)
        self.assertTrue(all(task.done() for task in tasks))
        self.assertEqual(p.context.registered_web_apis, [])
        self.assertTrue(p.file_worker.snapshot()["closed"])
