"""Server-side copies of chat images: on request, when viewed, or all incoming.

QQ image addresses expire, and a recalled image cannot be loaded again. A copy
kept under plugin_data/<plugin>/media stays viewable for as long as its message
is retained.

Downloads never run inside a message pipeline. receive() only queues a
(message id, segment index) pair; long-lived workers started in an empty
context fetch it, so no job trace is inherited and neither the idle gate nor a
stop sees them as task work. The address always comes from the stored message,
never from the page, and only QQ's own image hosts are contacted.
"""

import asyncio
import base64
import contextvars
import hashlib
import io
import os
import re
import shutil
import time
from pathlib import Path
from stat import S_ISREG
from urllib.parse import urljoin, urlsplit

import aiohttp
from astrbot.api.web import error_response, file_response, json_response, request
from PIL import Image
from yarl import URL

from .guidance import observed_api

# Hosts QQ serves chat images from (the name itself or any subdomain).
IMAGE_HOSTS = ("qq.com.cn", "qq.com", "qpic.cn")
# A host name is dotted labels of letters, digits and hyphens: never an address
# literal, and never a control character that a resolver would cut the name at.
HOST_NAME = re.compile(r"[a-z0-9-]+(\.[a-z0-9-]+)+")
IMAGE_LIMIT = 16 * 1024 * 1024
# Copies up to this size are shown as they are; larger ones are downscaled for
# the page, and the original stays available through the download route.
INLINE_LIMIT = 3 * 1024 * 1024
# Most pixels decoded for such a preview (about 48 MB of memory); a larger
# image is only offered as a download.
PREVIEW_PIXELS = 12_000_000
# Nothing is saved while the disk has less free space than this.
FREE_FLOOR = 2 * 1024**3
# A failed download is not repeated automatically within this time.
RETRY_SECONDS = 600
QUEUE_LIMIT = 256
WORKERS = 2
EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
}


class MediaError(Exception):
    """A download that cannot be kept; the text is shown to the administrator."""


def sniff(head):
    """Image type by content; the address and the response headers are not trusted."""
    if head[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image/webp"
    return ""


def allowed_address(url):
    """Whether the address is plain http(s) on one of QQ's image hosts.

    The HTTP client parses the address itself; one that it reads differently
    from this check is refused.
    """
    url = str(url)
    try:
        parts = urlsplit(url)
        host = (parts.hostname or "").lower().rstrip(".")
        port = parts.port
        client = URL(url)
    except ValueError:
        return False
    return (
        parts.scheme in ("http", "https")
        and not parts.username
        and not parts.password
        and port in (None, 80, 443)
        and HOST_NAME.fullmatch(host) is not None
        and any(host == name or host.endswith("." + name) for name in IMAGE_HOSTS)
        and (client.host or "").lower().rstrip(".") == host
        and client.port in (80, 443)
        and not client.user
        and not client.password
    )


def image_address(data):
    """The downloadable address of a stored image segment, or ''."""
    for key in ("url", "file"):
        value = str((data or {}).get(key) or "")
        if allowed_address(value):
            return value
    return ""


def copy_path(directory, digest):
    return Path(directory) / digest[:2] / digest


def copy_available(directory, digest, size):
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        return False
    try:
        info = copy_path(directory, digest).lstat()
    except (FileNotFoundError, NotADirectoryError):
        return False
    return S_ISREG(info.st_mode) and info.st_size == size


def reconcile_copies(directory, inventory):
    sweep_copies(directory, inventory)
    present = {
        digest
        for digest, size in inventory.items()
        if copy_available(directory, digest, size)
    }
    return present, inventory.keys() - present


def write_copy(directory, payload):
    """File lane: store one payload under its hash; identical images share a file."""
    digest = hashlib.sha256(payload).hexdigest()
    target = copy_path(directory, digest)
    try:
        if target.stat().st_size == len(payload):
            return digest
    except OSError:
        pass
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f"{digest}.{time.time_ns()}.part")
    try:
        temporary.write_bytes(payload)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return digest


def remove_copies(directory, digests):
    for digest in digests:
        try:
            copy_path(directory, digest).unlink(missing_ok=True)
        except OSError:
            pass


def free_bytes(directory):
    try:
        Path(directory).mkdir(parents=True, exist_ok=True)
        return shutil.disk_usage(directory).free
    except OSError:
        return 0


def sweep_copies(directory, known):
    """File lane, at start: drop unfinished downloads and files no row refers to."""
    removed = 0
    root = Path(directory)
    # Created at start, so an unwritable data directory shows up at once.
    root.mkdir(parents=True, exist_ok=True)
    for folder in root.iterdir():
        if not folder.is_dir() or folder.is_symlink():
            continue
        for item in folder.iterdir():
            if item.name not in known:
                try:
                    item.unlink()
                    removed += 1
                except OSError:
                    pass
    return removed


def inline_copy(path, mime):
    """File lane: a data URL the sandboxed page can show, or '' if unreadable."""
    try:
        payload = Path(path).read_bytes()
    except OSError:
        return ""
    if len(payload) > INLINE_LIMIT:
        try:
            with Image.open(io.BytesIO(payload)) as img:
                # A JPEG is decoded at a fraction of its size where possible.
                ratio = min(1600 / img.width, 1600 / img.height, 1)
                img.draft(
                    "RGB",
                    (max(1, int(img.width * ratio)), max(1, int(img.height * ratio))),
                )
                if img.width * img.height > PREVIEW_PIXELS:
                    return ""
                img.thumbnail((1600, 1600))
                output = io.BytesIO()
                img.convert("RGB").save(output, format="JPEG", quality=85)
                payload, mime = output.getvalue(), "image/jpeg"
        except (OSError, ValueError, Image.DecompressionBombError):
            return ""
    return f"data:{mime};base64," + base64.b64encode(payload).decode()


def public(row):
    """What the page may know about a copy."""
    return {
        "status": row["status"],
        "reason": row.get("reason") or "",
        "mime": row.get("mime") or "",
        "bytes": row.get("bytes") or 0,
    }


class MediaMixin:
    def _ensure_media(self):
        # Lazy construction also supports isolated fixtures without initialize().
        if not hasattr(self, "media_inflight"):
            self.media_inflight = {}
            self.media_limit = asyncio.Semaphore(WORKERS)
            self.media_commit = asyncio.Lock()
            self.media_tasks = []
            self.media_queue = asyncio.Queue(QUEUE_LIMIT)
            self.media_session = None
            self.media_usage = {"files": 0, "bytes": 0}
            self.media_skipped = 0

    async def file_call(self, function, *args, **kwargs):
        self._ensure_workers()
        return await self.file_worker.run(function, *args, **kwargs)

    async def start_media(self, directory):
        """Sweep leftovers, then start the download workers outside any trace."""
        self._ensure_media()
        self.media_dir = Path(directory) / "media"
        try:
            async with self.media_commit:
                inventory = await self.db_call(self.store.media_inventory)
                present, missing = await self.file_call(
                    reconcile_copies, self.media_dir, inventory
                )
                await self.db_call(self.store.set_media_available, missing, False)
                await self.db_call(self.store.set_media_available, present, True)
                await self.refresh_media_usage()
        except Exception:  # noqa: BLE001 - Saved images are optional; the console still starts.
            self.dropped += 1
        loop = asyncio.get_running_loop()
        self.media_tasks = [
            loop.create_task(self.media_loop(), context=contextvars.Context())
            for _ in range(WORKERS)
        ]

    async def stop_media(self):
        self._ensure_media()
        tasks, self.media_tasks = self.media_tasks, []
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def close_media(self):
        """After the last handler has settled, so no download loses its session."""
        self._ensure_media()
        session, self.media_session = self.media_session, None
        if session is not None:
            await session.close()

    async def media_loop(self):
        while True:
            message_id, seg = await self.media_queue.get()
            try:
                await self.save_media(message_id, seg)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - One bad image must not stop the worker.
                self.media_skipped += 1

    def queue_images(self, message_id, body):
        """Mode 全部: queue the images of a just recorded message; never waits."""
        queue = getattr(self, "media_queue", None)
        if (
            queue is None
            or not getattr(self, "media_tasks", None)
            or self.console_setting("image_save") != "all"
            or not isinstance(body, list)
        ):
            return
        index = -1
        # Same numbering as store.segments(): non-dict items are not stored.
        for item in body[:100]:
            if not isinstance(item, dict):
                continue
            index += 1
            if item.get("type") != "image":
                continue
            try:
                queue.put_nowait((message_id, index))
            except asyncio.QueueFull:
                self.media_skipped += 1

    async def fetch_image(self, url):
        """Download one image from QQ's hosts; redirects are followed only there."""
        if self.media_session is None:
            self.media_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30, connect=10),
                trust_env=False,
            )
        for _ in range(4):
            if not allowed_address(url):
                raise MediaError("图片地址不是 QQ 的图片服务器")
            async with self.media_session.get(url, allow_redirects=False) as response:
                if response.status in (301, 302, 303, 307, 308):
                    url = urljoin(url, response.headers.get("Location", ""))
                    continue
                if response.status != 200:
                    raise MediaError(
                        f"QQ 图片服务器返回 {response.status}，图片可能已过期"
                    )
                if (response.content_length or 0) > IMAGE_LIMIT:
                    raise MediaError("图片超过 16 MB，未保存")
                chunks, size = [], 0
                async for chunk in response.content.iter_chunked(65536):
                    size += len(chunk)
                    if size > IMAGE_LIMIT:
                        raise MediaError("图片超过 16 MB，未保存")
                    chunks.append(chunk)
            payload = b"".join(chunks)
            mime = sniff(payload)
            if not mime:
                raise MediaError("下载到的内容不是图片，地址可能已过期")
            return payload, mime
        raise MediaError("图片地址跳转次数过多")

    async def save_media(self, message_id, seg, retry=False):
        """Make or reuse the server copy of one stored image and return its state."""
        self._ensure_media()
        key = (message_id, seg)
        waiter = self.media_inflight.get(key)
        if waiter is not None:
            return await asyncio.shield(waiter)
        waiter = asyncio.get_running_loop().create_future()
        self.media_inflight[key] = waiter
        result = {"status": "failed", "reason": "保存被中断，请重试"}
        try:
            result = await self._save_media(message_id, seg, retry)
        finally:
            self.media_inflight.pop(key, None)
            waiter.set_result(result)
        return result

    async def _save_media(self, message_id, seg, retry):
        source = await self.db_call(self.store.media_source, message_id, seg)
        if source is None:
            return {"status": "missing"}
        saved = source["saved"]
        if not hasattr(self, "media_dir"):
            return {"status": "failed", "reason": "图片保存尚未就绪，请稍后重试"}
        if saved:
            if saved["status"] in ("saved", "missing"):
                async with self.media_commit:
                    if await self.check_saved_copy(saved):
                        return public({**saved, "status": "saved", "reason": ""})
            elif not retry and time.time() - saved["created"] < RETRY_SECONDS:
                return public(saved)
        url = image_address(source["data"])
        if not url:
            return await self.media_failed(message_id, seg, "这张图片没有可下载的地址")
        async with self.media_limit:
            if getattr(self, "terminating", False):
                # Not recorded: a reload must not leave a failure behind.
                return {"status": "failed", "reason": "插件正在重载，请稍后重试"}
            if await self.file_call(free_bytes, self.media_dir) < FREE_FLOOR:
                # Not recorded: the same image can be saved once space is back.
                return {
                    "status": "failed",
                    "reason": "服务器剩余磁盘空间不足 2 GB，暂停保存图片",
                }
            try:
                payload, mime = await self.fetch_image(url)
            except MediaError as exc:
                return await self.media_failed(message_id, seg, str(exc))
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError, ValueError):
                if getattr(self, "terminating", False):
                    return {
                        "status": "failed",
                        "reason": "插件正在重载，请稍后重试",
                    }
                return await self.media_failed(
                    message_id, seg, "下载失败，图片可能已过期或网络不通"
                )
            async with self.media_commit:
                digest = await self.file_call(write_copy, self.media_dir, payload)
                await self.db_call(
                    self.store.media_record,
                    message_id,
                    seg,
                    digest,
                    mime,
                    len(payload),
                    "saved",
                    "",
                )
                await self.db_call(self.store.set_media_available, (digest,), True)
                await self.trim_media(locked=True)
        return {"status": "saved", "reason": "", "mime": mime, "bytes": len(payload)}

    async def media_failed(self, message_id, seg, reason):
        await self.db_call(
            self.store.media_record, message_id, seg, "", "", 0, "failed", reason
        )
        return {"status": "failed", "reason": reason, "mime": "", "bytes": 0}

    async def trim_media(self, locked=False):
        """Keep saved copies within the chosen limit, oldest first."""
        self._ensure_media()
        if not hasattr(self, "media_dir"):
            return
        if not locked:
            async with self.media_commit:
                return await self.trim_media(locked=True)
        limit = self.console_setting("image_quota") * 1024 * 1024
        removed = await self.db_call(self.store.media_trim, limit)
        if removed:
            await self.file_call(remove_copies, self.media_dir, removed)
        await self.refresh_media_usage()

    async def prune_media(self):
        """Drop copies whose message has been pruned; one bounded page per call."""
        self._ensure_media()
        if not hasattr(self, "media_dir"):
            return False
        async with self.media_commit:
            result = await self.db_call(self.store.media_orphans, 256)
            if result["files"]:
                await self.file_call(remove_copies, self.media_dir, result["files"])
            if result["deleted"]:
                await self.refresh_media_usage()
        return result["more"]

    async def refresh_media_usage(self):
        self.media_usage = await self.db_call(self.store.media_usage)

    async def check_saved_copy(self, row):
        available = await self.file_call(
            copy_available, self.media_dir, row["sha256"], row["bytes"]
        )
        if available != (row["status"] == "saved"):
            await self.db_call(
                self.store.set_media_available, (row["sha256"],), available
            )
            await self.refresh_media_usage()
        return available

    def media_state(self):
        """Loop-only snapshot for the state route; never waits for the database."""
        self._ensure_media()
        return {
            "mode": self.console_setting("image_save"),
            "quota_mb": self.console_setting("image_quota"),
            "files": self.media_usage["files"],
            "bytes": self.media_usage["bytes"],
            "queued": self.media_queue.qsize(),
            "skipped": self.media_skipped,
        }

    async def saved_copy(self):
        """The saved row and file path named by the request's id/index, or None."""
        message_id = request.query.get("id", 0, type=int)
        seg = request.query.get("index", -1, type=int)
        if not hasattr(self, "media_dir"):
            return None
        async with self.media_commit:
            rows = await self.db_call(
                self.store.rows,
                "SELECT * FROM media WHERE message_id=? AND seg=? AND status IN ('saved','missing')",
                (message_id, seg),
            )
            if not rows or not await self.check_saved_copy(rows[0]):
                return None
            return rows[0], copy_path(self.media_dir, rows[0]["sha256"])

    @observed_api
    async def media_save(self):
        body = await request.json()
        if (
            not isinstance(body, dict)
            or type(body.get("id")) is not int
            or type(body.get("index")) is not int
        ):
            return error_response("无效请求")
        result = await self.save_media(
            body["id"], body["index"], retry=body.get("retry") is True
        )
        if result["status"] == "missing":
            return error_response("这条消息里没有这张图片，消息可能已过期")
        return json_response(result)

    @observed_api
    async def media(self):
        """A saved copy as a data URL; the sandboxed page cannot load plugin files."""
        found = await self.saved_copy()
        if found is None:
            return error_response("服务器上没有这张图片的副本")
        row, path = found
        url = await self.file_call(inline_copy, path, row["mime"])
        if not url:
            return error_response("副本无法显示，请用“下载”取回原图")
        return await self.media_call(
            json_response, {"url": url, "mime": row["mime"], "bytes": row["bytes"]}
        )

    @observed_api
    async def media_file(self):
        """The original file, for the dashboard's authenticated download."""
        found = await self.saved_copy()
        if found is None or not await self.file_call(found[1].is_file):
            return error_response("服务器上没有这张图片的副本")
        row, path = found
        return file_response(
            path,
            filename=f"qq-image-{row['message_id']}-{row['seg']}"
            + EXTENSIONS.get(row["mime"], ""),
            content_type=row["mime"] or "application/octet-stream",
        )
