"""SQLite persistence with all-or-nothing version writes.

Each version row and its attachment blob are inserted in the same explicit
transaction. sqlite3's isolation_level is disabled so we control BEGIN/COMMIT
and never leave autocommitted fragments behind. A write-ahead log plus
synchronous=FULL makes an interrupted process leave either a complete version
or no version at all (SQLite rolls the partial transaction back on recovery).
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterable

from .domain import (
    ArchiveStorageError,
    VersionRecord,
    utc_now_iso,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS announcements (
    number          TEXT PRIMARY KEY,
    first_seen_at   TEXT NOT NULL,
    last_checked_at TEXT NOT NULL,
    source_retracted INTEGER NOT NULL DEFAULT 0
                      CHECK (source_retracted IN (0, 1)),
    status_updated_at TEXT
);

CREATE TABLE IF NOT EXISTS versions (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    announcement_number     TEXT NOT NULL
                            REFERENCES announcements(number),
    revision                INTEGER NOT NULL,
    body_revision           INTEGER NOT NULL,
    attachment_revision     INTEGER NOT NULL,
    title                   TEXT NOT NULL,
    body                    TEXT NOT NULL,
    body_sha256             TEXT NOT NULL,
    content_hash            TEXT NOT NULL,
    attachment_filename     TEXT NOT NULL,
    attachment_content_type TEXT NOT NULL,
    attachment_size         INTEGER NOT NULL,
    attachment_sha256       TEXT NOT NULL,
    attachment_blob         BLOB NOT NULL,
    source_retracted        INTEGER NOT NULL CHECK (source_retracted IN (0, 1)),
    source_url              TEXT NOT NULL,
    source_updated_at       TEXT,
    captured_at             TEXT NOT NULL,
    UNIQUE (announcement_number, revision)
);

CREATE INDEX IF NOT EXISTS versions_number_captured
    ON versions (announcement_number, revision DESC);

CREATE TABLE IF NOT EXISTS capture_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    announcement_number TEXT,
    outcome         TEXT NOT NULL,
    revision        INTEGER,
    detail          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    -- Site retraction state *after* this event, when the event determines it.
    -- NULL on attempts that do not change holdings and on legacy rows whose
    -- state cannot be established; NULL never means "not retracted".
    source_retracted INTEGER CHECK (source_retracted IN (0, 1))
);

CREATE INDEX IF NOT EXISTS capture_log_replay
    ON capture_log (outcome, created_at, id);
"""


class ArchiveStore:
    def __init__(self, db_path: str | os.PathLike[str]):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    # Backfill for older archives: a ``created`` event's retraction state is
    # recoverable from the immutable version row it produced. A legacy
    # ``retracted_update`` event recorded only a detail message, so it stays
    # NULL and the holdings view reports that state as undetermined instead of
    # reusing today's announcement flag.
    _BACKFILL_CREATED_STATUS = """
        UPDATE capture_log
           SET source_retracted = (
                SELECT v.source_retracted FROM versions v
                 WHERE v.announcement_number = capture_log.announcement_number
                   AND v.revision = capture_log.revision
           )
         WHERE outcome = 'created'
           AND source_retracted IS NULL
           AND EXISTS (
                SELECT 1 FROM versions v
                 WHERE v.announcement_number = capture_log.announcement_number
                   AND v.revision = capture_log.revision
           )
    """

    def initialize(self) -> None:
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = FULL")
            conn.executescript(SCHEMA)
            # Additive migration for databases created by earlier builds.
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(announcements)")
            }
            if "source_retracted" not in columns:
                conn.execute(
                    "ALTER TABLE announcements ADD COLUMN "
                    "source_retracted INTEGER NOT NULL DEFAULT 0"
                )
            if "status_updated_at" not in columns:
                conn.execute(
                    "ALTER TABLE announcements ADD COLUMN status_updated_at TEXT"
                )

            log_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(capture_log)")
            }
            if "source_retracted" not in log_columns:
                conn.execute(
                    "ALTER TABLE capture_log ADD COLUMN "
                    "source_retracted INTEGER "
                    "CHECK (source_retracted IN (0, 1))"
                )
            # Idempotent: a created event's state is always provable from the
            # immutable version row it produced, so backfill even if an
            # earlier migration was interrupted after the ALTER.
            conn.execute(self._BACKFILL_CREATED_STATUS)
        except sqlite3.Error as exc:
            raise ArchiveStorageError(f"无法初始化档案数据库：{exc}") from exc
        finally:
            conn.close()

    # ---------------------------------------------------------------- reads

    def holdings_at(self, cutoff_event_id: int | None = None) -> dict | None:
        """Reconstruct the archive-wide holdings as of one capture event.

        ``cutoff_event_id=None`` selects the newest capture record inside the
        same snapshot. One read transaction is held for every query, so a
        capture committed concurrently cannot leak into a list that has
        already selected an earlier cutoff. Events are replayed in submission
        order ``(created_at, id)``: the log id breaks ties inside one second,
        which is exactly the commit order. Only events that changed holdings
        count — a failed capture is a check, never a state change.

        Returns ``None`` when an explicitly requested cutoff does not exist.
        Otherwise: ``cutoff`` (log row or None), ``entries`` (one per
        announcement, highest revision visible at the cut), and
        ``selectable`` (capture records that can serve as cutoffs, newest
        first).
        """
        conn = self._connect()
        try:
            # Pin a single WAL snapshot for the whole reconstruction, including
            # the dropdown of possible cutoffs.
            conn.execute("BEGIN")
            if cutoff_event_id is None:
                cutoff = conn.execute(
                    "SELECT * FROM capture_log "
                    "ORDER BY created_at DESC, id DESC LIMIT 1"
                ).fetchone()
            else:
                cutoff = conn.execute(
                    "SELECT * FROM capture_log WHERE id = ?",
                    (cutoff_event_id,),
                ).fetchone()
                if cutoff is None:
                    return None

            selectable = conn.execute(
                "SELECT * FROM capture_log "
                "ORDER BY created_at DESC, id DESC"
            ).fetchall()

            if cutoff is None:
                return {
                    "cutoff": None,
                    "entries": [],
                    "selectable": [],
                }

            events = conn.execute(
                """
                SELECT * FROM capture_log
                WHERE outcome IN ('created', 'retracted_update')
                  AND (created_at < ?
                       OR (created_at = ? AND id <= ?))
                ORDER BY created_at, id
                """,
                (cutoff["created_at"], cutoff["created_at"], cutoff["id"]),
            ).fetchall()

            created_revisions: dict[str, set[int]] = {}
            for event in events:
                if event["outcome"] == "created" and event["revision"] is not None:
                    created_revisions.setdefault(
                        event["announcement_number"], set()
                    ).add(event["revision"])

            wanted = [
                (number, revision)
                for number, revisions in created_revisions.items()
                for revision in revisions
            ]
            rows_by_key: dict[tuple[str, int], sqlite3.Row] = {}
            # Only metadata is displayed. Deliberately omit attachment_blob so
            # viewing the holdings list never loads archived file bytes.
            columns = (
                "announcement_number, revision, title, captured_at, "
                "source_retracted"
            )
            # SQLite builds allow a limited number of bound variables per
            # statement; query in bounded chunks so large archives still work.
            chunk_size = 200
            for start in range(0, len(wanted), chunk_size):
                chunk = wanted[start : start + chunk_size]
                clauses = " OR ".join(
                    ["(announcement_number = ? AND revision = ?)"] * len(chunk)
                )
                params: list = [value for pair in chunk for value in pair]
                for row in conn.execute(
                    f"SELECT {columns} FROM versions WHERE {clauses}", params
                ):
                    rows_by_key[
                        (row["announcement_number"], row["revision"])
                    ] = row

            # Replay state-changing events per announcement.
            state: dict[str, bool | None] = {}
            known: dict[str, bool] = {}
            highest: dict[str, int] = {}
            for event in events:
                number = event["announcement_number"]
                if number is None:
                    continue
                if event["outcome"] == "created":
                    revision = event["revision"]
                    if revision is None:
                        continue
                    previous = highest.get(number)
                    if previous is not None and revision <= previous:
                        # An older revision cannot be the "highest held" and
                        # must not overwrite the newer revision's state.
                        continue
                    highest[number] = revision
                    value = event["source_retracted"]
                    row = rows_by_key.get((number, revision))
                    if value is None and row is not None:
                        # Pre-migration row not yet backfilled: the immutable
                        # version row still records the state at capture.
                        value = row["source_retracted"]
                    if row is None and value is None:
                        # The log claims a version the archive no longer has:
                        # do not pretend to know its status.
                        known[number] = False
                        state[number] = None
                    else:
                        state[number] = bool(value)
                        known[number] = True
                else:  # retracted_update
                    value = event["source_retracted"]
                    if value is None:
                        # Legacy status-only update without a recorded state.
                        # Never substitute the announcement's current flag.
                        known[number] = False
                        state[number] = None
                    else:
                        known[number] = True
                        state[number] = bool(value)

            # Legacy archives may hold rows whose creation event was never
            # written by an older build. Only those rows use capture time as
            # evidence; any row with its own created event is handled by the
            # replay above, so a version committed after the cutoff cannot be
            # pulled in by comparing timestamps. A legacy row captured strictly
            # before the cutoff second was held at the cut. Failed captures
            # never enter here.
            legacy_entries: dict[str, sqlite3.Row] = {}
            legacy_rows = conn.execute(
                """
                SELECT v.announcement_number, v.revision, v.title,
                       v.captured_at, v.source_retracted
                 FROM versions v
                 WHERE v.captured_at < ?
                   AND NOT EXISTS (
                        SELECT 1 FROM capture_log c
                         WHERE c.announcement_number = v.announcement_number
                           AND c.revision = v.revision
                           AND c.outcome = 'created'
                   )
                 ORDER BY v.announcement_number, v.revision
                """,
                (cutoff["created_at"],),
            ).fetchall()
            for row in legacy_rows:
                number = row["announcement_number"]
                if number in highest:
                    continue
                current = legacy_entries.get(number)
                if current is None or row["revision"] > current["revision"]:
                    legacy_entries[number] = row

            entries = []
            for number in sorted(set(highest) | set(legacy_entries)):
                revision = highest.get(number)
                row = (
                    rows_by_key.get((number, revision))
                    if revision is not None
                    else None
                )
                if revision is None:
                    row = legacy_entries[number]
                    revision = row["revision"]
                    # Only fill in what the replay could not establish; a
                    # legacy status event that was undecidable stays unknown
                    # rather than being overwritten by the capture-time flag.
                    if number not in known:
                        known[number] = True
                        state[number] = bool(row["source_retracted"])
                entries.append(
                    {
                        "number": number,
                        "revision": revision,
                        "title": row["title"] if row else None,
                        "captured_at": row["captured_at"] if row else None,
                        "retracted": state.get(number),
                        "retraction_known": known.get(number, False),
                    }
                )

            return {
                "cutoff": dict(cutoff),
                "entries": entries,
                "selectable": [dict(row) for row in selectable],
            }
        finally:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            conn.close()

    def get_version(
        self, number: str, revision: int
    ) -> VersionRecord | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM versions WHERE announcement_number = ? "
                "AND revision = ?",
                (number, revision),
            ).fetchone()
        finally:
            conn.close()
        return self._row_to_record(row) if row else None

    def list_versions(self, number: str) -> list[VersionRecord]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM versions WHERE announcement_number = ? "
                "ORDER BY revision DESC",
                (number,),
            ).fetchall()
        finally:
            conn.close()
        return [self._row_to_record(r) for r in rows]

    def latest_version(self, number: str) -> VersionRecord | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM versions WHERE announcement_number = ? "
                "ORDER BY revision DESC LIMIT 1",
                (number,),
            ).fetchone()
        finally:
            conn.close()
        return self._row_to_record(row) if row else None

    def announcement_exists(self, number: str) -> bool:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM announcements WHERE number = ?", (number,)
            ).fetchone()
        finally:
            conn.close()
        return row is not None

    def list_announcements(self) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT a.number, a.first_seen_at, a.last_checked_at,
                       a.source_retracted, a.status_updated_at,
                       COUNT(v.id) AS version_count,
                       (
                           SELECT v2.revision FROM versions v2
                           WHERE v2.announcement_number = a.number
                           ORDER BY v2.revision DESC LIMIT 1
                       ) AS latest_revision,
                       (
                           SELECT v3.title FROM versions v3
                           WHERE v3.announcement_number = a.number
                           ORDER BY v3.revision DESC LIMIT 1
                       ) AS latest_title
                FROM announcements a
                LEFT JOIN versions v ON v.announcement_number = a.number
                GROUP BY a.number
                ORDER BY a.last_checked_at DESC, a.number
                """
            ).fetchall()
        finally:
            conn.close()
        return [dict(r) for r in rows]

    def get_announcement(self, number: str) -> dict | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM announcements WHERE number = ?", (number,)
            ).fetchone()
        finally:
            conn.close()
        return dict(row) if row else None

    def recent_events(self, limit: int = 30) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM capture_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        finally:
            conn.close()
        return [dict(r) for r in rows]

    def events_for(self, number: str, limit: int = 50) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM capture_log WHERE announcement_number = ? "
                "ORDER BY id DESC LIMIT ?",
                (number, limit),
            ).fetchall()
        finally:
            conn.close()
        return [dict(r) for r in rows]

    # --------------------------------------------------------------- writes

    def insert_version(
        self, record: VersionRecord, outcome: str, detail: str
    ) -> bool:
        """Atomically archive one version.

        Returns False when that source revision was already archived (no new
        version row is created). Raises ArchiveStorageError on any storage
        failure, rolling back so no partial version can be observed.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT 1 FROM versions WHERE announcement_number = ? "
                "AND revision = ?",
                (record.announcement_number, record.revision),
            ).fetchone()
            if existing:
                conn.execute("ROLLBACK")
                return False

            # Stamp the log inside the write transaction rather than reusing
            # the capture time read before the lock. Two captures that commit
            # in the same second are ordered by the log id; a timestamp taken
            # before acquiring the write lock could otherwise let a later
            # commit carry an earlier second and invert replay order.
            now = utc_now_iso()
            conn.execute(
                """
                INSERT INTO announcements
                    (number, first_seen_at, last_checked_at,
                     source_retracted, status_updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(number) DO UPDATE SET
                    last_checked_at = excluded.last_checked_at,
                    source_retracted = excluded.source_retracted,
                    status_updated_at = excluded.status_updated_at
                """,
                (
                    record.announcement_number,
                    now,
                    now,
                    int(record.source_retracted),
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO versions (
                    announcement_number, revision, body_revision,
                    attachment_revision, title, body, body_sha256,
                    content_hash, attachment_filename,
                    attachment_content_type, attachment_size,
                    attachment_sha256, attachment_blob, source_retracted,
                    source_url, source_updated_at, captured_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.announcement_number,
                    record.revision,
                    record.body_revision,
                    record.attachment_revision,
                    record.title,
                    record.body,
                    record.body_sha256,
                    record.content_hash,
                    record.attachment_filename,
                    record.attachment_content_type,
                    record.attachment_size,
                    record.attachment_sha256,
                    sqlite3.Binary(record.attachment_blob),
                    int(record.source_retracted),
                    record.source_url,
                    record.source_updated_at,
                    record.captured_at or now,
                ),
            )
            conn.execute(
                """
                INSERT INTO capture_log
                    (announcement_number, outcome, revision, detail,
                     created_at, source_retracted)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    record.announcement_number,
                    outcome,
                    record.revision,
                    detail,
                    now,
                    int(record.source_retracted),
                ),
            )
            conn.execute("COMMIT")
            return True
        except sqlite3.Error as exc:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise ArchiveStorageError(f"存档写入失败，未保存任何内容：{exc}") from exc
        finally:
            conn.close()

    def touch_checked(
        self, number: str, outcome: str, detail: str, revision: int | None
    ) -> None:
        """Record a capture attempt that did not create a version."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            # Timestamp inside the write transaction so same-second ordering
            # by log id matches commit order.
            now = utc_now_iso()
            conn.execute(
                """
                INSERT INTO capture_log
                    (announcement_number, outcome, revision, detail, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (number, outcome, revision, detail, now),
            )
            conn.execute(
                "UPDATE announcements SET last_checked_at = ? WHERE number = ?",
                (now, number),
            )
            conn.execute("COMMIT")
        except sqlite3.Error as exc:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise ArchiveStorageError(f"无法记录采集结果：{exc}") from exc
        finally:
            conn.close()

    def update_source_retracted(
        self, number: str, revision: int, retracted: bool
    ) -> None:
        """Record the site's retraction state without altering any version.

        Version rows (text + attachment bytes) are immutable after capture.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = utc_now_iso()
            cursor = conn.execute(
                "UPDATE announcements SET source_retracted = ?, "
                "status_updated_at = ?, last_checked_at = ? WHERE number = ?",
                (int(retracted), now, now, number),
            )
            if cursor.rowcount != 1:
                raise ArchiveStorageError("撤稿状态更新失败：目标公告不存在。")
            conn.execute(
                """
                INSERT INTO capture_log
                    (announcement_number, outcome, revision, detail,
                     created_at, source_retracted)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    number,
                    "retracted_update",
                    revision,
                    "源站撤稿状态变化，已更新标记" if retracted else "源站恢复发布，已更新标记",
                    now,
                    int(retracted),
                ),
            )
            conn.execute("COMMIT")
        except sqlite3.Error as exc:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise ArchiveStorageError(f"无法更新撤稿状态：{exc}") from exc
        finally:
            conn.close()

    def log_event(
        self, number: str | None, outcome: str, detail: str, revision: int | None
    ) -> None:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = utc_now_iso()
            conn.execute(
                """
                INSERT INTO capture_log
                    (announcement_number, outcome, revision, detail, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (number, outcome, revision, detail, now),
            )
            conn.execute("COMMIT")
        except sqlite3.Error as exc:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise ArchiveStorageError(f"无法记录采集事件：{exc}") from exc
        finally:
            conn.close()

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> VersionRecord:
        blob: Iterable[int] | bytes = row["attachment_blob"]
        return VersionRecord(
            announcement_number=row["announcement_number"],
            revision=row["revision"],
            body_revision=row["body_revision"],
            attachment_revision=row["attachment_revision"],
            title=row["title"],
            body=row["body"],
            body_sha256=row["body_sha256"],
            content_hash=row["content_hash"],
            attachment_filename=row["attachment_filename"],
            attachment_content_type=row["attachment_content_type"],
            attachment_size=row["attachment_size"],
            attachment_sha256=row["attachment_sha256"],
            attachment_blob=bytes(blob),
            source_retracted=bool(row["source_retracted"]),
            source_url=row["source_url"],
            source_updated_at=row["source_updated_at"],
            captured_at=row["captured_at"],
        )
