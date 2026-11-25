"""Bounded, persistent conversation and trace storage."""

import base64
import io
import json
import re
import sqlite3
import threading
import time
from contextlib import closing
from contextvars import ContextVar

from PIL import Image

PENDING_REPLY_LIMIT = 32 * 1024 * 1024
PENDING_TOTAL_LIMIT = 128 * 1024 * 1024

trace_context = ContextVar("super_admin_trace", default="")


def redact(value):
    text = str(value)
    text = re.sub(r"(?i)(bearer\s+)[\w.\-]+", r"\1[REDACTED]", text)
    text = re.sub(
        r"(?i)((?:api[_-]?key|access[_-]?token|authorization|password|secret)[\"']?\s*[:=]\s*[\"']?)[^\s,\"'}]+",
        r"\1[REDACTED]",
        text,
    )
    text = re.sub(r"\bsk-[A-Za-z0-9_-]{12,}", "[REDACTED]", text)
    return text[:24000]


def segments(value):
    if isinstance(value, str):
        return [{"type": "text", "data": {"text": value[:16000]}}]
    result = []
    for item in (value or [])[:100]:
        if not isinstance(item, dict):
            continue
        data = item.get("data") or {}
        clean = {}
        for key, val in data.items():
            val = str(val)
            clean[str(key)] = (
                "[image payload]" if val.startswith("base64://") else val[:16000]
            )
            if (
                val.startswith("base64://")
                and item.get("type") == "image"
                and len(val) < 16000000
            ):
                try:
                    with Image.open(
                        io.BytesIO(base64.b64decode(val[9:], validate=True))
                    ) as img:
                        if img.width * img.height <= 25000000:
                            img.thumbnail((320, 320))
                            output = io.BytesIO()
                            img.convert("RGB").save(output, format="JPEG", quality=70)
                            clean["preview"] = (
                                "data:image/jpeg;base64,"
                                + base64.b64encode(output.getvalue()).decode()
                            )
                except (OSError, ValueError, Image.DecompressionBombError):
                    pass
        result.append({"type": str(item.get("type", "unknown")), "data": clean})
    return result


class Store:
    def __init__(self, path):
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS chats (
              id TEXT PRIMARY KEY, platform TEXT, account TEXT, kind TEXT,
              target TEXT, group_id TEXT, title TEXT, updated REAL);
            CREATE TABLE IF NOT EXISTS messages (
              id INTEGER PRIMARY KEY AUTOINCREMENT, chat TEXT REFERENCES chats(id),
              trace TEXT, direction TEXT, sender TEXT, body TEXT, search TEXT,
              status TEXT, remote_id TEXT, created REAL);
            CREATE INDEX IF NOT EXISTS messages_chat ON messages(chat,id);
            CREATE INDEX IF NOT EXISTS messages_trace ON messages(trace);
            CREATE TABLE IF NOT EXISTS logs (
              id INTEGER PRIMARY KEY AUTOINCREMENT, trace TEXT, level TEXT,
              source TEXT, body TEXT, created REAL);
            CREATE INDEX IF NOT EXISTS logs_trace ON logs(trace,id);
            CREATE TABLE IF NOT EXISTS requests (
              id TEXT PRIMARY KEY, chat TEXT, trace TEXT, created REAL);
            CREATE TABLE IF NOT EXISTS jobs (
              trace TEXT PRIMARY KEY, chat TEXT, umo TEXT, prompt TEXT,
              status TEXT, started REAL, finished REAL, draft TEXT DEFAULT '');
            CREATE TABLE IF NOT EXISTS ai_traces (trace TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS guidance_instructions (
              request_id TEXT PRIMARY KEY, fingerprint TEXT, trace TEXT,
              reference_kind TEXT, message_id INTEGER, guidance TEXT, created REAL);
            CREATE INDEX IF NOT EXISTS guidance_trace ON guidance_instructions(trace,created);
            CREATE TABLE IF NOT EXISTS pending_sends (
              id INTEGER PRIMARY KEY AUTOINCREMENT, trace TEXT, platform TEXT,
              action TEXT, payload TEXT, preview TEXT, status TEXT, created REAL);
            CREATE INDEX IF NOT EXISTS pending_trace ON pending_sends(trace,id);
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS media (
              message_id INTEGER, seg INTEGER, sha256 TEXT, mime TEXT, bytes INTEGER,
              status TEXT, reason TEXT, created REAL, PRIMARY KEY(message_id, seg));
            CREATE INDEX IF NOT EXISTS media_sha ON media(sha256);
            CREATE INDEX IF NOT EXISTS media_created ON media(created,message_id,seg);
        """)
        columns = {row["name"] for row in self.db.execute("PRAGMA table_info(jobs)")}
        for name, definition in (
            ("step", "TEXT DEFAULT ''"),
            ("step_detail", "TEXT DEFAULT ''"),
            ("step_started", "REAL"),
            ("source_message_id", "INTEGER"),
            ("sender_id", "TEXT DEFAULT ''"),
            ("reply_mode", "TEXT DEFAULT ''"),
            ("parent_trace", "TEXT DEFAULT ''"),
        ):
            if name not in columns:
                self.db.execute(f"ALTER TABLE jobs ADD COLUMN {name} {definition}")
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS jobs_source_message ON jobs(source_message_id)"
        )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS messages_remote ON messages(chat,remote_id)"
        )
        message_columns = {
            row["name"] for row in self.db.execute("PRAGMA table_info(messages)")
        }
        if "sender_id" not in message_columns:
            self.db.execute("ALTER TABLE messages ADD COLUMN sender_id TEXT DEFAULT ''")
        self.db.execute(
            "UPDATE pending_sends SET status='unknown' WHERE status IN ('approved','sending')"
        )
        self.db.executescript("""
            CREATE INDEX IF NOT EXISTS logs_created_id ON logs(created,id);
            CREATE INDEX IF NOT EXISTS messages_created_id ON messages(created,id);
            CREATE INDEX IF NOT EXISTS requests_created_id ON requests(created,id);
            CREATE INDEX IF NOT EXISTS pending_created_id ON pending_sends(created,id);
            CREATE INDEX IF NOT EXISTS pending_status_trace ON pending_sends(status,trace);
            CREATE INDEX IF NOT EXISTS jobs_started_trace ON jobs(started,trace);
            CREATE INDEX IF NOT EXISTS jobs_chat ON jobs(chat);
            CREATE INDEX IF NOT EXISTS chats_updated_id ON chats(updated,id);
        """)
        self.db.commit()
        self._prune_state = None
        # Grow-only index of traces that may carry a reply mode or review rows.
        # Ordinary sends consult it instead of taking the guidance lock and
        # reading console.db. TEMP triggers exist only on this connection, so
        # they leave the database schema unchanged for older plugin versions.
        # The trigger function adds from the database worker thread: event-loop
        # code may test membership or take a copy, never iterate the live set.
        self.guided_traces = set()
        self.db.create_function("super_admin_mark_guided", 1, self._mark_guided)
        self.db.executescript("""
            CREATE TEMP TRIGGER IF NOT EXISTS super_admin_jobs_mode_insert
            AFTER INSERT ON main.jobs WHEN NEW.reply_mode!=''
            BEGIN SELECT super_admin_mark_guided(NEW.trace); END;
            CREATE TEMP TRIGGER IF NOT EXISTS super_admin_jobs_mode_update
            AFTER UPDATE OF reply_mode ON main.jobs WHEN NEW.reply_mode!=''
            BEGIN SELECT super_admin_mark_guided(NEW.trace); END;
            CREATE TEMP TRIGGER IF NOT EXISTS super_admin_guidance_insert
            AFTER INSERT ON main.guidance_instructions
            BEGIN SELECT super_admin_mark_guided(NEW.trace); END;
            CREATE TEMP TRIGGER IF NOT EXISTS super_admin_pending_insert
            AFTER INSERT ON main.pending_sends
            BEGIN SELECT super_admin_mark_guided(NEW.trace); END;
        """)
        self.guided_traces.update(
            row[0]
            for row in self.db.execute(
                "SELECT trace FROM jobs WHERE reply_mode!='' UNION "
                "SELECT trace FROM guidance_instructions UNION "
                "SELECT trace FROM pending_sends"
            )
            if row[0]
        )

    def _mark_guided(self, trace):
        if trace:
            self.guided_traces.add(str(trace))

    def execute(self, sql, args=()):
        with self.lock, self.db:
            return self.db.execute(sql, args).lastrowid

    def repair_group_notices(self, directory):
        """Rehome previously misclassified group events without discarding history.

        Args:
            directory: Database directory used for the pre-migration backup.
        """
        with self.lock:
            affected = self.rows("""SELECT DISTINCT c.id,c.platform,c.account,c.group_id
                FROM chats c JOIN jobs j ON j.chat=c.id
                WHERE c.kind='temporary' AND c.group_id!=''
                AND j.umo LIKE c.platform || ':GroupMessage:%'""")
            if not affected:
                return
            with closing(
                sqlite3.connect(
                    directory / f"console.before-group-notice-fix-{time.time_ns()}.db"
                )
            ) as backup:
                self.db.backup(backup)
            with self.db:
                for chat in affected:
                    destination = json.dumps(
                        [
                            chat["platform"],
                            chat["account"],
                            "group",
                            chat["group_id"],
                            "",
                        ],
                        separators=(",", ":"),
                    )
                    self.db.execute(
                        "INSERT OR IGNORE INTO chats VALUES(?,?,?,?,?,?,?,?)",
                        (
                            destination,
                            chat["platform"],
                            chat["account"],
                            "group",
                            chat["group_id"],
                            "",
                            chat["group_id"],
                            time.time(),
                        ),
                    )
                    pattern = chat["platform"] + ":GroupMessage:%"
                    self.db.execute(
                        "UPDATE messages SET body=?,search=? WHERE chat=? AND search='' AND trace IN (SELECT trace FROM jobs WHERE chat=? AND umo LIKE ?)",
                        (
                            json.dumps(
                                [{"type": "text", "data": {"text": "[群通知]"}}],
                                ensure_ascii=False,
                            ),
                            "[群通知]",
                            chat["id"],
                            chat["id"],
                            pattern,
                        ),
                    )
                    self.db.execute(
                        "UPDATE messages SET chat=? WHERE chat=? AND trace IN (SELECT trace FROM jobs WHERE chat=? AND umo LIKE ?)",
                        (destination, chat["id"], chat["id"], pattern),
                    )
                    self.db.execute(
                        "UPDATE jobs SET chat=? WHERE chat=? AND umo LIKE ?",
                        (destination, chat["id"], pattern),
                    )
                    self.db.execute(
                        "UPDATE chats SET updated=COALESCE((SELECT MAX(created) FROM messages WHERE chat=?),updated) WHERE id=?",
                        (destination, destination),
                    )
                    self.db.execute(
                        "DELETE FROM chats WHERE id=? AND NOT EXISTS(SELECT 1 FROM messages WHERE chat=?) AND NOT EXISTS(SELECT 1 FROM jobs WHERE chat=?)",
                        (chat["id"], chat["id"], chat["id"]),
                    )

    def repair_background_messages(self, directory):
        """Link empty duplicate events to their original messages without deleting history.

        Args:
            directory: Directory for the backup before reclassifying records.
        """
        with self.lock:
            duplicates = self.rows("""SELECT m.id,m.trace,s.id AS source_id,s.search
                FROM messages m JOIN messages s ON s.id=(
                  SELECT id FROM messages WHERE chat=m.chat AND remote_id=m.remote_id
                  AND direction='in' AND id<m.id AND TRIM(search)!='' ORDER BY id LIMIT 1)
                WHERE m.direction='in' AND TRIM(m.search)=''
                AND m.remote_id NOT IN ('','0')""")
            if duplicates:
                with closing(
                    sqlite3.connect(
                        directory
                        / f"console.before-background-link-{time.time_ns()}.db"
                    )
                ) as backup:
                    self.db.backup(backup)
            with self.db:
                for row in duplicates:
                    self.db.execute(
                        "UPDATE jobs SET source_message_id=?,prompt=CASE WHEN TRIM(COALESCE(prompt,''))='' THEN ? ELSE prompt END WHERE trace=?",
                        (row["source_id"], row["search"], row["trace"]),
                    )
                    self.db.execute(
                        "UPDATE messages SET direction='internal' WHERE id=?",
                        (row["id"],),
                    )
                self.db.execute("""UPDATE jobs SET source_message_id=(
                    SELECT id FROM messages WHERE trace=jobs.trace AND chat=jobs.chat
                    AND direction='in' ORDER BY id LIMIT 1) WHERE source_message_id IS NULL""")
            return len(duplicates)

    def rows(self, sql, args=()):
        with self.lock:
            return [dict(row) for row in self.db.execute(sql, args).fetchall()]

    def save_settings(self, values):
        """Store validated console settings in one transaction."""
        with self.lock, self.db:
            self.db.executemany(
                "INSERT INTO settings(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [(key, json.dumps(value)) for key, value in values.items()],
            )

    def mark_recalled(self, platform, account, remote_id, group, user):
        """Mark the stored message a recall notice names; its content is kept."""
        if remote_id in ("", "0", "None"):
            return 0
        with self.lock, self.db:
            return self.db.execute(
                "UPDATE messages SET status='recalled' WHERE remote_id=? AND status!='recalled' "
                "AND direction IN ('in','out') AND chat IN (SELECT id FROM chats WHERE platform=? "
                "AND account=? AND target=? AND CASE WHEN ? THEN kind='group' ELSE kind!='group' END)",
                (remote_id, platform, account, group or user, bool(group)),
            ).rowcount

    def media_source(self, message_id, seg):
        """The received image segment a copy is made from, and its copy if any."""
        with self.lock:
            message = self.db.execute(
                "SELECT body FROM messages WHERE id=? AND direction='in'",
                (message_id,),
            ).fetchone()
            saved = self.db.execute(
                "SELECT sha256,mime,bytes,status,reason,created FROM media WHERE message_id=? AND seg=?",
                (message_id, seg),
            ).fetchone()
        if message is None:
            return None
        try:
            body = json.loads(message["body"])
        except ValueError:
            return None
        if (
            not isinstance(body, list)
            or not 0 <= seg < len(body)
            or not isinstance(body[seg], dict)
            or body[seg].get("type") != "image"
        ):
            return None
        return {
            "data": body[seg].get("data") or {},
            "saved": dict(saved) if saved else None,
        }

    def media_record(self, message_id, seg, sha256, mime, size, status, reason):
        with self.lock, self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO media VALUES(?,?,?,?,?,?,?,?)",
                (message_id, seg, sha256, mime, size, status, reason, time.time()),
            )

    def media_states(self, message_ids):
        """Per message: segment index (as text, ascending) to saved / failed."""
        result = {}
        if not message_ids:
            return result
        marks = ",".join("?" for _ in message_ids)
        with self.lock:
            rows = self.db.execute(
                f"SELECT message_id,seg,status FROM media WHERE message_id IN ({marks}) ORDER BY message_id,seg",
                tuple(message_ids),
            ).fetchall()
        for row in rows:
            result.setdefault(row["message_id"], {})[str(row["seg"])] = row["status"]
        return result

    def media_inventory(self):
        with self.lock:
            return dict(
                self.db.execute(
                    "SELECT sha256,MAX(bytes) FROM media WHERE status IN ('saved','missing') GROUP BY sha256"
                )
            )

    def set_media_available(self, digests, available):
        old, new = ("missing", "saved") if available else ("saved", "missing")
        reason = "" if available else "服务器副本缺失，请重新保存"
        with self.lock, self.db:
            self.db.executemany(
                "UPDATE media SET status=?,reason=? WHERE sha256=? AND status=?",
                ((new, reason, digest, old) for digest in digests),
            )

    def media_usage(self):
        """Distinct saved files and their size; identical images count once."""
        with self.lock:
            row = self.db.execute(
                "SELECT COUNT(*),COALESCE(SUM(size),0) FROM (SELECT MAX(bytes) AS size "
                "FROM media WHERE status='saved' GROUP BY sha256)"
            ).fetchone()
        return {"files": row[0], "bytes": row[1]}

    def _media_unreferenced(self, sha256):
        return (
            self.db.execute(
                "SELECT 1 FROM media WHERE sha256=? AND status='saved' LIMIT 1",
                (sha256,),
            ).fetchone()
            is None
        )

    def media_trim(self, limit):
        """Drop the oldest files until the rest fit; return hashes to delete.

        A file several messages share is as old as its newest use. Until that
        one's turn comes every message keeps its copy, since dropping an older
        row alone would free nothing.
        """
        removed = []
        with self.lock, self.db:
            usage = self.media_usage()["bytes"]
            after = (-1.0, -1, -1)
            while usage > limit:
                rows = self.db.execute(
                    "SELECT message_id,seg,sha256,bytes,created FROM media "
                    "WHERE status='saved' AND (created,message_id,seg)>(?,?,?) "
                    "ORDER BY created,message_id,seg LIMIT 64",
                    after,
                ).fetchall()
                if not rows:
                    break
                for row in rows:
                    after = (row["created"], row["message_id"], row["seg"])
                    if self.db.execute(
                        "SELECT 1 FROM media WHERE sha256=? AND status='saved' "
                        "AND (created,message_id,seg)>(?,?,?) LIMIT 1",
                        (row["sha256"], *after),
                    ).fetchone():
                        continue
                    self.db.execute(
                        "DELETE FROM media WHERE sha256=? AND status='saved'",
                        (row["sha256"],),
                    )
                    removed.append(row["sha256"])
                    usage -= row["bytes"]
                    if usage <= limit:
                        break
        return removed

    def media_orphans(self, batch_size=256):
        """Delete one page of copies whose message is gone; never part of prune()."""
        removed = []
        with self.lock, self.db:
            rows = self.db.execute(
                "SELECT message_id,seg,sha256,status FROM media WHERE NOT EXISTS "
                "(SELECT 1 FROM messages WHERE messages.id=media.message_id) "
                "ORDER BY message_id,seg LIMIT ?",
                (batch_size,),
            ).fetchall()
            for row in rows:
                self.db.execute(
                    "DELETE FROM media WHERE message_id=? AND seg=?",
                    (row["message_id"], row["seg"]),
                )
                if row["status"] == "saved" and self._media_unreferenced(row["sha256"]):
                    removed.append(row["sha256"])
        return {
            "more": len(rows) == batch_size,
            "deleted": len(rows),
            "files": removed,
        }

    def chat(self, platform, account, kind, target, group_id="", title=""):
        key = json.dumps(
            [str(platform), str(account), kind, str(target), str(group_id)],
            separators=(",", ":"),
        )
        self.execute(
            """INSERT INTO chats VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id)
            DO UPDATE SET updated=excluded.updated,
            title=CASE WHEN excluded.title IN ('','N/A','None','unknown') OR excluded.title=excluded.target THEN chats.title ELSE excluded.title END""",
            (
                key,
                str(platform),
                str(account),
                kind,
                str(target),
                str(group_id),
                title,
                time.time(),
            ),
        )
        return key

    def message(
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
        prepared=False,
    ):
        body = body if prepared else segments(body)
        return self.execute(
            "INSERT INTO messages(chat,trace,direction,sender,body,search,status,remote_id,created,sender_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                chat,
                trace,
                direction,
                str(sender),
                json.dumps(body, ensure_ascii=False),
                " ".join(x["data"].get("text", "[" + x["type"] + "]") for x in body),
                status,
                str(remote_id),
                created
                if isinstance(created, (int, float)) and 0 < created < 32503680000
                else time.time(),
                str(sender_id),
            ),
        )

    def log(self, trace, level, source, body):
        self.execute(
            "INSERT INTO logs(trace,level,source,body,created) VALUES(?,?,?,?,?)",
            (trace, level, source, redact(body), time.time()),
        )

    def close(self):
        with self.lock:
            self.db.close()

    def record_step(self, trace, step, detail, expected=None):
        """Read and update a phase in one worker transaction, without an await gap."""
        with self.lock, self.db:
            row = self.db.execute(
                "SELECT j.* FROM jobs j JOIN ai_traces a ON a.trace=j.trace "
                "WHERE j.trace=? AND j.status IN ('running','completed')",
                (trace,),
            ).fetchone()
            if row is None:
                return None
            previous = dict(row)
            if expected is not None and previous["step_started"] != expected:
                return None
            if (previous["step"], previous["step_detail"]) == (step, detail):
                return None
            started = time.time()
            self.db.execute(
                "UPDATE jobs SET step=?,step_detail=?,step_started=? WHERE trace=?",
                (step, detail, started, trace),
            )
            return previous, started

    def reserve_pending(self, trace, platform, action, payload, preview, payload_bytes):
        """Atomically enforce review limits and publish the job's pending phase."""
        with self.lock, self.db:
            job = self.db.execute(
                "SELECT status FROM jobs WHERE trace=?", (trace,)
            ).fetchone()
            if job is None or job["status"] in ("stopping", "cancelled", "interrupted"):
                raise ValueError("Task stopped before review publication")
            retained = self.db.execute(
                "SELECT COALESCE(SUM(LENGTH(CAST(payload AS BLOB))),0) FROM "
                "pending_sends WHERE status='pending'"
            ).fetchone()[0]
            if (
                payload_bytes > PENDING_REPLY_LIMIT
                or retained + payload_bytes > PENDING_TOTAL_LIMIT
            ):
                raise ValueError(
                    "Pending replies exceed the review storage limit; review or discard existing replies first"
                )
            pending_id = self.db.execute(
                "INSERT INTO pending_sends(trace,platform,action,payload,preview,status,created) "
                "VALUES(?,?,?,?,?,'pending',?)",
                (trace, platform, action, payload, preview, time.time()),
            ).lastrowid
            self.db.execute(
                "UPDATE jobs SET status='awaiting_approval',step='awaiting_approval',step_detail='',step_started=? WHERE trace=?",
                (time.time(), trace),
            )
            return pending_id

    def approve_pending(self, trace, selected_ids, connected_platforms):
        """Validate the exact reviewed set and claim it in one transaction."""
        with self.lock, self.db:
            rows = [
                dict(row)
                for row in self.db.execute(
                    "SELECT id,trace,platform,action,status FROM pending_sends WHERE trace=? ORDER BY id",
                    (trace,),
                ).fetchall()
            ]
            selected = [row for row in rows if row["id"] in selected_ids]
            if len(selected) != len(set(selected_ids)):
                raise ValueError("待发回复不属于这个任务")
            if any(
                row["status"] not in ("pending", "approved", "sending", "accepted")
                for row in selected
            ):
                raise ValueError("部分回复已取消或发送结果未知，请查看日志")
            pending = [row for row in rows if row["status"] == "pending"]
            if pending and {row["id"] for row in pending} != {
                row["id"] for row in selected if row["status"] == "pending"
            }:
                raise ValueError("待发内容已变化，请重新查看后确认")
            if not pending:
                return []
            if any(row["platform"] not in connected_platforms for row in pending):
                raise ValueError("发送账号已断开，请恢复连接后确认")
            self.db.execute("UPDATE jobs SET status='running' WHERE trace=?", (trace,))
            self.db.executemany(
                "UPDATE pending_sends SET status='approved' WHERE id=? AND status='pending'",
                [(row["id"],) for row in pending],
            )
            return pending

    def claim_pending_send(self, pending_id, trace):
        with self.lock, self.db:
            return (
                self.db.execute(
                    "UPDATE pending_sends SET status='sending' WHERE id=? AND trace=? AND status='approved'",
                    (pending_id, trace),
                ).rowcount
                == 1
            )

    def release_unattempted(self, trace, ids, *, discarded=False, claimed=False):
        """Return only this caller's unattempted approvals to review, never unknown sends."""
        if not ids:
            return
        marks = ",".join("?" for _ in ids)
        statuses = "('approved','sending')" if claimed else "('approved')"
        with self.lock, self.db:
            self.db.execute(
                f"UPDATE pending_sends SET status=?,payload=CASE WHEN ? THEN '' ELSE payload END "
                f"WHERE trace=? AND id IN ({marks}) AND status IN {statuses}",
                ("discarded" if discarded else "pending", discarded, trace, *ids),
            )
            self.db.execute(
                "UPDATE jobs SET status='awaiting_approval' WHERE trace=? AND status NOT IN ('stopping','cancelled') "
                "AND EXISTS(SELECT 1 FROM pending_sends WHERE trace=? AND status='pending')",
                (trace, trace),
            )

    def add_guidance(self, instruction, reply_mode, new_job=None):
        """Commit an instruction, cap check and job/mode update as one mutation."""
        trace = instruction[2]
        with self.lock, self.db:
            if new_job is None:
                job = self.db.execute(
                    "SELECT status FROM jobs WHERE trace=?", (trace,)
                ).fetchone()
                if job is None or job["status"] in (
                    "awaiting_approval",
                    "stopping",
                    "cancelled",
                    "interrupted",
                    "failed",
                ):
                    raise ValueError("该任务已不在生成中，请新建任务或确认待发回复")
            if (
                self.db.execute(
                    "SELECT COUNT(*) FROM guidance_instructions WHERE trace=?", (trace,)
                ).fetchone()[0]
                >= 20
            ):
                raise ValueError("此任务已添加 20 条引导，请新建任务")
            if new_job is not None:
                self.db.execute(
                    "INSERT INTO jobs(trace,chat,umo,prompt,status,started,source_message_id,sender_id,reply_mode) VALUES(?,?,?,?,?,?,?,?,?)",
                    new_job,
                )
                self.db.execute("INSERT OR IGNORE INTO ai_traces VALUES(?)", (trace,))
            self.db.execute(
                "INSERT INTO guidance_instructions VALUES(?,?,?,?,?,?,?)", instruction
            )
            self.db.execute(
                "UPDATE jobs SET reply_mode=? WHERE trace=?", (reply_mode, trace)
            )

    def prune(self, batch_size=128):
        """Process one indexed keyset page; never hold the lock for a whole cycle.

        The loop schedules another page after yielding to foreground work. Cutoffs
        and row-ID caps are frozen per cycle. Anti-join NULL guards conservatively
        retain legacy NULL-key rows (which normal plugin writes never create).
        """
        batch_size = max(1, min(int(batch_size), 256))
        with self.lock, self.db:
            if self._prune_state is None:
                self._prune_state = {
                    "cutoff": time.time() - 14 * 86400,
                    "logs_cap": self.db.execute(
                        "SELECT COALESCE(MAX(id),0)-100000 FROM logs"
                    ).fetchone()[0],
                    "messages_cap": self.db.execute(
                        "SELECT COALESCE(MAX(id),0)-20000 FROM messages"
                    ).fetchone()[0],
                    "phase": 0,
                    "cursor": None,
                }
            state = self._prune_state
            cutoff = state["cutoff"]
            active = "('running','stopping','awaiting_approval')"
            # table, key, ordered column (None means primary-key traversal), upper
            # bound, deletion guard and parameters. All identifiers are constants.
            phases = [
                ("logs", "id", "created", cutoff, "1", ()),
                ("logs", "id", None, state["logs_cap"], "1", ()),
                ("messages", "id", "created", cutoff, "1", ()),
                ("messages", "id", None, state["messages_cap"], "1", ()),
                ("requests", "id", "created", cutoff, "1", ()),
                ("pending_sends", "id", "created", cutoff, "status!='pending'", ()),
                (
                    "guidance_instructions",
                    "request_id",
                    None,
                    None,
                    (
                        "trace IS NOT NULL AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.trace=guidance_instructions.trace) "
                        "AND NOT EXISTS (SELECT 1 FROM jobs WHERE trace IS NULL)"
                    ),
                    (),
                ),
                (
                    "ai_traces",
                    "trace",
                    None,
                    None,
                    (
                        "trace IS NOT NULL AND NOT EXISTS (SELECT 1 FROM messages WHERE messages.trace=ai_traces.trace) "
                        "AND NOT EXISTS (SELECT 1 FROM messages WHERE trace IS NULL) "
                        f"AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.trace=ai_traces.trace AND (started>=? OR status IN {active})) "
                        f"AND NOT EXISTS (SELECT 1 FROM jobs WHERE trace IS NULL AND (started>=? OR status IN {active})) "
                        "AND NOT EXISTS (SELECT 1 FROM pending_sends WHERE status='pending' AND pending_sends.trace=ai_traces.trace) "
                        "AND NOT EXISTS (SELECT 1 FROM pending_sends WHERE status='pending' AND trace IS NULL)"
                    ),
                    (cutoff, cutoff),
                ),
                (
                    "jobs",
                    "trace",
                    "started",
                    cutoff,
                    (
                        f"status NOT IN {active} AND trace IS NOT NULL "
                        "AND NOT EXISTS (SELECT 1 FROM pending_sends WHERE status='pending' AND pending_sends.trace=jobs.trace) "
                        "AND NOT EXISTS (SELECT 1 FROM pending_sends WHERE status='pending' AND trace IS NULL)"
                    ),
                    (),
                ),
                (
                    "chats",
                    "id",
                    "updated",
                    cutoff,
                    (
                        "id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM messages WHERE messages.chat=chats.id) "
                        "AND NOT EXISTS (SELECT 1 FROM messages WHERE chat IS NULL) "
                        "AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.chat=chats.id) "
                        "AND NOT EXISTS (SELECT 1 FROM jobs WHERE chat IS NULL)"
                    ),
                    (),
                ),
            ]
            table, key, ordered, upper, guard, guard_args = phases[state["phase"]]
            where, args = [f"{key} IS NOT NULL"], []
            if ordered is not None:
                where.append(f"{ordered}<?")
                args.append(upper)
                if state["cursor"] is not None:
                    where.append(f"({ordered},{key})>(?,?)")
                    args.extend(state["cursor"])
                columns, ordering = f"{key},{ordered}", f"{ordered},{key}"
            else:
                if upper is not None:
                    where.append(f"{key}<=?")
                    args.append(upper)
                if state["cursor"] is not None:
                    where.append(f"{key}>?")
                    args.append(state["cursor"])
                columns = ordering = key
            query = f"SELECT {columns} FROM {table} WHERE {' AND '.join(where)} ORDER BY {ordering} LIMIT ?"
            rows = self.db.execute(query, (*args, batch_size)).fetchall()
            deleted = 0
            if rows:
                keys = [row[0] for row in rows]
                marks = ",".join("?" for _ in keys)
                deleted = self.db.execute(
                    f"DELETE FROM {table} WHERE {key} IN ({marks}) AND ({guard})",
                    (*keys, *guard_args),
                ).rowcount
                state["cursor"] = (rows[-1][1], rows[-1][0]) if ordered else rows[-1][0]
            phase = state["phase"]
            if len(rows) < batch_size:
                state["phase"] += 1
                state["cursor"] = None
            more = state["phase"] < len(phases)
            if not more:
                self._prune_state = None
            return {
                "more": more,
                "phase": phase,
                "table": table,
                "examined": len(rows),
                "deleted": deleted,
                "batch_size": batch_size,
            }
