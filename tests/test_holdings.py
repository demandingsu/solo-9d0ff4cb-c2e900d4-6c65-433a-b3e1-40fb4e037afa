"""Point-in-time holdings reconstruction ("当时持有清单")."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app
from app.store import ArchiveStore

NUMBER = "ZFCG-2026-0001"
NUMBER2 = "ZFCG-2026-0002"


def _archive_app(tmp_path: Path, demo):
    return create_app(
        db_path=str(tmp_path / "archive-holdings.db"),
        source_base_url="http://demo.test",
        source_client=demo,
    )


def _events(store: ArchiveStore) -> list[dict]:
    conn = store._connect()
    try:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM capture_log ORDER BY created_at, id"
            )
        ]
    finally:
        conn.close()


def _last_event(store: ArchiveStore, outcome: str, number: str = NUMBER) -> dict:
    matches = [
        e
        for e in _events(store)
        if e["outcome"] == outcome and e["announcement_number"] == number
    ]
    assert matches, f"no {outcome} event for {number}"
    return matches[-1]


def _edit(demo, number=NUMBER, body="正文 v2"):
    response = demo.post(
        f"/notice/{number}/edit",
        data={"title": "修改后的标题", "body": body},
        follow_redirects=False,
    )
    assert response.status_code == 303


def _retract(demo, action: str, number=NUMBER):
    response = demo.post(
        f"/notice/{number}/retract", data={"action": action},
        follow_redirects=False,
    )
    assert response.status_code == 303


def test_holdings_lists_highest_revision_per_notice_at_cutoff(tmp_path, demo):
    app = _archive_app(tmp_path, demo)
    store = app.state.store
    service = app.state.service
    with TestClient(app) as client:
        service.capture(NUMBER)
        service.capture(NUMBER2)
        first_cut = _last_event(store, "created", NUMBER2)

        _edit(demo)
        service.capture(NUMBER)
        service.capture(NUMBER)  # unchanged: same r2
        r2_cut = _last_event(store, "created")

        early = client.get(f"/holdings?cutoff={first_cut['id']}")
        assert early.status_code == 200
        assert "当时持有清单" in early.text
        assert f"记录 #{first_cut['id']}" in early.text
        # Both notices at r1; no r2 in the holdings table yet.
        assert "查看 r1 正文" in early.text
        assert "查看 r2 正文" not in early.text
        assert f"/notice/{NUMBER}/version/1/attachment" in early.text

        late = client.get(f"/holdings?cutoff={r2_cut['id']}")
        assert late.status_code == 200
        assert "查看 r2 正文" in late.text
        assert f"/notice/{NUMBER}/version/2/attachment" in late.text
        # NUMBER2 is still held at r1 at the later cutoff.
        assert f"/notice/{NUMBER2}/version/1/attachment" in late.text


def test_holdings_replays_retraction_and_restore(tmp_path, demo):
    app = _archive_app(tmp_path, demo)
    store = app.state.store
    service = app.state.service
    with TestClient(app) as client:
        service.capture(NUMBER)
        _retract(demo, "retract")
        retract_event = service.capture(NUMBER)
        assert retract_event.status == "retracted_update"
        retracted_cut = _last_event(store, "retracted_update")

        _retract(demo, "restore")
        restored = service.capture(NUMBER)
        assert restored.status == "retracted_update"
        restored_cut = _last_event(store, "retracted_update")

        at_retract = client.get(f"/holdings?cutoff={retracted_cut['id']}")
        assert "当时已撤稿" in at_retract.text
        assert "当时在发" not in at_retract.text

        at_restore = client.get(f"/holdings?cutoff={restored_cut['id']}")
        assert "当时在发" in at_restore.text
        assert "当时已撤稿" not in at_restore.text
        # The retracted revision's original remains downloadable at both cuts.
        assert client.get(f"/notice/{NUMBER}/version/1/attachment").status_code == 200


def test_same_second_events_are_ordered_by_submission(
    tmp_path, demo, monkeypatch
):
    app = _archive_app(tmp_path, demo)
    store = app.state.store
    service = app.state.service
    with TestClient(app) as client:
        service.capture(NUMBER)
        # Freeze the clock so retract and restore land in the same second.
        monkeypatch.setattr("app.store.utc_now_iso", lambda: "2026-11-17T09:00:00Z")
        _retract(demo, "retract")
        service.capture(NUMBER)
        _retract(demo, "restore")
        service.capture(NUMBER)

        events = [
            e for e in _events(store) if e["outcome"] == "retracted_update"
        ]
        assert len(events) == 2
        assert events[0]["created_at"] == events[1]["created_at"]
        assert events[0]["id"] < events[1]["id"]

        first = client.get(f"/holdings?cutoff={events[0]['id']}")
        second = client.get(f"/holdings?cutoff={events[1]['id']}")
        assert "当时已撤稿" in first.text
        assert "当时在发" not in first.text
        assert "当时在发" in second.text
        assert "当时已撤稿" not in second.text


def test_failed_capture_does_not_change_holdings(tmp_path, demo):
    app = _archive_app(tmp_path, demo)
    store = app.state.store
    service = app.state.service
    with TestClient(app) as client:
        service.capture(NUMBER)
        service.capture(NUMBER2)
        before = client.get("/holdings")

        # Same revision, different content: rejected, logged as conflict.
        demo.post(
            f"/demo/{NUMBER}/conflict",
            data={"body": "同号改写正文"},
            follow_redirects=False,
        )
        assert service.capture(NUMBER).status == "conflict"
        conflict_cut = _last_event(store, "conflict")

        after = client.get("/holdings")
        assert "当时持有清单" in after.text
        assert f"/notice/{NUMBER2}/version/1/attachment" in after.text
        # Same holdings as before the failed attempt.
        assert after.text.count("查看 r1 正文") == before.text.count("查看 r1 正文") == 2

        # Even when the failed attempt itself is chosen as the cutoff.
        at_conflict = client.get(f"/holdings?cutoff={conflict_cut['id']}")
        assert at_conflict.status_code == 200
        assert "查看 r1 正文" in at_conflict.text
        assert NUMBER in at_conflict.text and NUMBER2 in at_conflict.text


def test_page_stays_at_selected_cutoff_when_new_version_arrives(
    tmp_path, demo, monkeypatch
):
    """A capture committed after the cutoff was fixed must not leak in."""
    app = _archive_app(tmp_path, demo)
    store = app.state.store
    service = app.state.service
    service.capture(NUMBER)
    cutoff = _last_event(store, "created")

    real_connect = store._connect
    injected = {"done": False}

    class InjectingConnection:
        def __init__(self, conn):
            self._conn = conn

        def __getattr__(self, name):
            return getattr(self._conn, name)

        def execute(self, sql, *params):
            cursor = self._conn.execute(sql, *params)
            bound = params[0] if len(params) == 1 and isinstance(
                params[0], (tuple, list)
            ) else params
            if (
                not injected["done"]
                and "FROM capture_log WHERE id = ?" in sql
                and cutoff["id"] in bound
            ):
                injected["done"] = True
                # Another window captures r2 while this page is being built.
                _edit(demo)
                service.capture(NUMBER)
            return cursor

    monkeypatch.setattr(store, "_connect", lambda: InjectingConnection(real_connect()))
    page = TestClient(app).get(f"/holdings?cutoff={cutoff['id']}")
    monkeypatch.setattr(store, "_connect", real_connect)

    assert injected["done"], "injection hook did not run"
    # The new r2 was committed, but this page still reflects the cutoff r1.
    assert len(store.list_versions(NUMBER)) == 2
    assert "r2" not in page.text
    assert "查看 r1 正文" in page.text
    assert f"记录 #{cutoff['id']}" in page.text


def test_unknown_cutoff_is_not_found(tmp_path, demo):
    app = _archive_app(tmp_path, demo)
    with TestClient(app) as client:
        response = client.get("/holdings?cutoff=999999")
        assert response.status_code == 404
        assert "采集记录不存在" in response.text


def _legacy_db(path: Path) -> None:
    """Build an archive exactly as an older build left it."""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE announcements (
            number TEXT PRIMARY KEY,
            first_seen_at TEXT NOT NULL,
            last_checked_at TEXT NOT NULL,
            source_retracted INTEGER NOT NULL DEFAULT 0,
            status_updated_at TEXT
        );
        CREATE TABLE versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            announcement_number TEXT NOT NULL,
            revision INTEGER NOT NULL,
            body_revision INTEGER NOT NULL,
            attachment_revision INTEGER NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            body_sha256 TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            attachment_filename TEXT NOT NULL,
            attachment_content_type TEXT NOT NULL,
            attachment_size INTEGER NOT NULL,
            attachment_sha256 TEXT NOT NULL,
            attachment_blob BLOB NOT NULL,
            source_retracted INTEGER NOT NULL,
            source_url TEXT NOT NULL,
            source_updated_at TEXT,
            captured_at TEXT NOT NULL,
            UNIQUE (announcement_number, revision)
        );
        CREATE TABLE capture_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            announcement_number TEXT,
            outcome TEXT NOT NULL,
            revision INTEGER,
            detail TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO announcements VALUES (?,?,?,?,?)",
        ("OLD-1", "2026-01-01T00:00:00Z", "2026-01-03T00:00:00Z", 1,
         "2026-01-03T00:00:00Z"),
    )
    conn.execute(
        "INSERT INTO versions (announcement_number, revision, body_revision,"
        " attachment_revision, title, body, body_sha256, content_hash,"
        " attachment_filename, attachment_content_type, attachment_size,"
        " attachment_sha256, attachment_blob, source_retracted, source_url,"
        " source_updated_at, captured_at)"
        " VALUES ('OLD-1',1,1,1,'旧标题','旧正文','bh','ch','a.txt',"
        " 'text/plain',3,'blobhash',X'616263',0,'u',NULL,"
        " '2026-01-01T00:00:00Z')",
    )
    # Legacy rows: a creation event and a status-only retraction, neither with
    # a machine-readable state column. Both happen before the cutoff below.
    conn.execute(
        "INSERT INTO capture_log (announcement_number, outcome, revision,"
        " detail, created_at) VALUES ('OLD-1','created',1,'首次采集',"
        " '2026-01-01T00:00:00Z')",
    )
    conn.execute(
        "INSERT INTO capture_log (announcement_number, outcome, revision,"
        " detail, created_at) VALUES ('OLD-1','retracted_update',1,"
        " '源站撤稿状态变化，已更新标记','2026-01-02T00:00:00Z')",
    )
    conn.execute(
        "INSERT INTO capture_log (announcement_number, outcome, revision,"
        " detail, created_at) VALUES ('OLD-1','unchanged',1,"
        " '无变化','2026-01-03T00:00:00Z')",
    )
    conn.commit()
    conn.close()


def test_legacy_created_state_is_backfilled(tmp_path):
    db = tmp_path / "legacy.db"
    _legacy_db(db)
    store = ArchiveStore(db)

    conn = store._connect()
    try:
        rows = conn.execute(
            "SELECT outcome, source_retracted FROM capture_log ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    assert rows[0]["source_retracted"] == 0
    # The legacy status-only row has no provable state and stays NULL.
    assert rows[1]["source_retracted"] is None

    snapshot = store.holdings_at(1)  # cutoff at the creation event
    assert snapshot["entries"][0]["retraction_known"] is True
    assert snapshot["entries"][0]["retracted"] is False


def test_legacy_unprovable_state_is_marked_not_current_flag(tmp_path):
    db = tmp_path / "legacy2.db"
    _legacy_db(db)
    store = ArchiveStore(db)

    latest = store.holdings_at(None)
    entry = latest["entries"][0]
    # Current announcement flag is retracted (1), but the state at the cut is
    # undetermined and must not inherit it.
    assert store.get_announcement("OLD-1")["source_retracted"] == 1
    assert entry["retraction_known"] is False
    assert entry["retracted"] is None
    assert entry["revision"] == 1

    page = create_app(
        db_path=str(db), source_base_url="http://demo.test", source_client=None
    )
    with TestClient(page) as client:
        html = client.get("/holdings")
        assert html.status_code == 200
        assert "无法判定" in html.text
        assert "未套用当前标记" in html.text
        assert "当时已撤稿" not in html.text


def test_legacy_version_without_log_is_still_held(tmp_path):
    """A version row with no creation log row (older build) is still held."""
    db = tmp_path / "legacy3.db"
    _legacy_db(db)
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO versions (announcement_number, revision, body_revision,"
        " attachment_revision, title, body, body_sha256, content_hash,"
        " attachment_filename, attachment_content_type, attachment_size,"
        " attachment_sha256, attachment_blob, source_retracted, source_url,"
        " source_updated_at, captured_at)"
        " VALUES ('OLD-2',1,1,1,'无日志标题','正文','bh','ch','b.txt',"
        " 'text/plain',3,'blobhash',X'646566',0,'u',NULL,"
        " '2026-01-01T12:00:00Z')",
    )
    conn.commit()
    conn.close()

    store = ArchiveStore(db)
    latest = store.holdings_at(None)
    numbers = {entry["number"] for entry in latest["entries"]}
    assert numbers == {"OLD-1", "OLD-2"}
    old2 = next(e for e in latest["entries"] if e["number"] == "OLD-2")
    assert old2["title"] == "无日志标题"
    assert old2["retraction_known"] is True
    assert old2["retracted"] is False
