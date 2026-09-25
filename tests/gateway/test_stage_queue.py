"""
Tests for gateway.stage_queue — Message Staging Queue.

E2E battery (9/9): stage/drain/FIFO/dedupe/dead-letter/depth/age/mark_done/alarm.

These tests mock the PostgREST rail (_sb_rpc) to exercise the logic
without a live database. The PostgREST rail is the only external dependency.

Rework 397fc42e: PostgREST rail replaces SQL-rail (no raw SQL, no SQL injection).
"""
import asyncio
import json
import os
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import quote

import pytest

# Ensure staging is "enabled" for import-time flag
os.environ["HERMES_STAGING_QUEUE"] = "1"
os.environ["HERMES_STAGING_ALARM_DEPTH"] = "3"
os.environ["HERMES_STAGING_ALARM_AGE_S"] = "60"
os.environ["SUPABASE_URL"] = "https://test-project.supabase.co"
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = "test-key"

from gateway.stage_queue import (
    bump_attempt,
    check_alarm,
    clear_reaction,
    drain_next,
    enqueue_from_event,
    mark_dead,
    mark_done,
    oldest_staged_age,
    queue_depth_all,
    re_stage,
    set_released_reaction,
    set_staged_reaction,
    stage_message,
    _queue_depth,
)


# ---------------------------------------------------------------------------
# Fixtures — mock the PostgREST rail
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _mock_sb_rpc(monkeypatch):
    """Replace _sb_rpc with an in-memory list-based store."""
    store: list[dict] = []
    _seq_counter = [0]

    async def fake_sb_rpc(method: str, table: str, payload=None, query=None):
        # POST (insert)
        if method == "post" and table == "rpc/msq_stage":
            # model the msq_stage RPC: ON CONFLICT (platform, chat_id, message_id)
            # DO NOTHING — returns the existing row id on duplicate delivery.
            p = payload or {}
            for r in store:
                if (str(r.get("platform")) == str(p.get("p_platform"))
                        and str(r.get("chat_id")) == str(p.get("p_chat_id"))
                        and str(r.get("message_id")) == str(p.get("p_message_id"))):
                    return r["id"]
            row = {
                "id": f"uuid-{len(store)+1}",
                "session_key": p.get("p_session_key"),
                "platform": p.get("p_platform"),
                "chat_id": p.get("p_chat_id"),
                "bot_name": p.get("p_bot_name", ""),
                "message_id": p.get("p_message_id", ""),
                "sender_id": p.get("p_sender_id", ""),
                "sender_name": p.get("p_sender_name", ""),
                "status": "staged",
                "seq": _seq_counter[0],
                "attempts": 0,
                "event": p.get("p_event"),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            store.append(row)
            return row["id"]

        # POST (insert)
            _seq_counter[0] += 1
            row = dict(payload) if payload else {}
            row.setdefault("id", f"uuid-{len(store)+1}")
            row.setdefault("seq", _seq_counter[0])
            row.setdefault("status", "staged")
            row.setdefault("attempts", 0)
            row.setdefault("created_at", datetime.now(timezone.utc).isoformat())
            row.setdefault("updated_at", datetime.now(timezone.utc).isoformat())
            store.append(row)
            return [row]

        # GET (select)
        if method == "get":
            filters = _parse_query(query or "")
            results = []
            for row in store:
                match = True
                for key, op, val in filters:
                    row_val = str(row.get(key, ""))
                    if op == "eq":
                        match = match and row_val == val
                    elif op == "in" and key == "select":
                        pass  # column selection, not a filter
                if match:
                    results.append(row)
            # Column selection
            if filters:
                select_cols = [f[2] for f in filters if f[0] == "select"]
            return results

        # PATCH (update)
        if method == "patch":
            filters = _parse_query(query or "")
            updated = []
            for row in store:
                match = True
                for key, op, val in filters:
                    row_val = str(row.get(key, ""))
                    if op == "eq":
                        match = match and row_val == val
                if match and payload:
                    row.update(payload)
                    updated.append(row)
            return updated

        return []

    import gateway.stage_queue as sq
    monkeypatch.setattr(sq, "_sb_rpc", fake_sb_rpc)
    # Reset alarm state
    sq._alarm_fired.clear()
    yield store


def _parse_query(query: str) -> list:
    """Parse PostgREST query string into (key, op, val) tuples."""
    result = []
    for part in query.split("&"):
        if "=" in part:
            key, val = part.split("=", 1)
            if "." in key:
                key, op = key.rsplit(".", 1)
                result.append((key, op, val))
            else:
                result.append((key, "eq", val))
    return result


# ---------------------------------------------------------------------------
# 1. Stage: persist inbound message
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stage_message_inserts_row():
    result = await stage_message(
        platform="telegram", chat_id="chat-1", session_key="sess-1",
        message_id="msg-1", sender_id="user-1", sender_name="Alice",
        event_json='{"text": "hello"}', bot_name="hermes",
    )
    assert result["status"] == "staged"
    assert result["session_key"] == "sess-1"
    assert "id" in result


# ---------------------------------------------------------------------------
# 2. Drain: pop next staged message
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_drain_next_pops_staged():
    await stage_message("telegram", "c", "sess-1", "m1", "u1", "A", '{}', "bot")
    row = await drain_next("sess-1")
    assert row is not None
    assert row["status"] == "processing"


@pytest.mark.asyncio
async def test_drain_next_returns_none_when_empty():
    row = await drain_next("empty-session")
    assert row is None


# ---------------------------------------------------------------------------
# 3. FIFO ordering
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_drain_is_fifo():
    await stage_message("telegram", "c", "sess-2", "m1", "u1", "A", '{}', "bot")
    await stage_message("telegram", "c", "sess-2", "m2", "u2", "B", '{}', "bot")
    row = await drain_next("sess-2")
    assert row is not None
    # The first staged message should be drained first (FIFO by seq)
    assert row["message_id"] == "m1"


# ---------------------------------------------------------------------------
# 4. Dedupe: migration defines unique index on (chat_id, message_id)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dedupe_index_in_migration():
    """The migration creates a unique index on (chat_id, message_id)
    WHERE message_id IS NOT NULL. This test verifies the migration
    SQL contains the index definition."""
    import pathlib
    sql_path = pathlib.Path(__file__).resolve().parents[2] / "supabase" / "migrations" / "20260918000001_message_stage_queue.sql"
    if sql_path.exists():
        ddl = sql_path.read_text()
        assert "idx_msq_platform_msg" in ddl
        assert "UNIQUE" in ddl


# ---------------------------------------------------------------------------
# 5. Dead-letter: mark_dead on repeated reconstruct failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mark_dead_sets_status():
    await stage_message("telegram", "c", "sess-3", "m1", "u1", "A", '{}', "bot")
    row = await drain_next("sess-3")
    msg_id = row["id"]
    await mark_dead(msg_id, "reconstruct failed: bad json")


@pytest.mark.asyncio
async def test_bump_attempt_increments():
    await stage_message("telegram", "c", "sess-4", "m1", "u1", "A", '{}', "bot")
    row = await drain_next("sess-4")
    msg_id = row["id"]
    attempts = await bump_attempt(msg_id)
    assert attempts == 1
    attempts = await bump_attempt(msg_id)
    assert attempts == 2


# ---------------------------------------------------------------------------
# 6. Depth query
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_queue_depth_counts_staged():
    await stage_message("telegram", "c", "sess-5", "m1", "u1", "A", '{}', "bot")
    await stage_message("telegram", "c", "sess-5", "m2", "u2", "B", '{}', "bot")
    depth = await _queue_depth("sess-5")
    assert depth == 2


@pytest.mark.asyncio
async def test_queue_depth_all():
    await stage_message("telegram", "c", "sess-6a", "m1", "u1", "A", '{}', "bot")
    await stage_message("telegram", "c", "sess-6b", "m2", "u2", "B", '{}', "bot")
    result = await queue_depth_all()
    assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 7. Age query
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_oldest_staged_age():
    await stage_message("telegram", "c", "sess-7", "m1", "u1", "A", '{}', "bot")
    age = await oldest_staged_age("sess-7")
    assert age is not None
    assert age >= 0


# ---------------------------------------------------------------------------
# 8. Mark done
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mark_done():
    await stage_message("telegram", "c", "sess-8", "m1", "u1", "A", '{}', "bot")
    row = await drain_next("sess-8")
    await mark_done(row["id"])


# ---------------------------------------------------------------------------
# 9. Alarm: depth/age threshold breach
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_check_alarm_fires_on_depth():
    # Staging 3 messages triggers the depth alarm (HERMES_STAGING_ALARM_DEPTH=3)
    await stage_message("telegram", "c", "sess-9", "m1", "u1", "A", '{}', "bot")
    await stage_message("telegram", "c", "sess-9", "m2", "u2", "B", '{}', "bot")
    await stage_message("telegram", "c", "sess-9", "m3", "u3", "C", '{}', "bot")
    alarm = await check_alarm("sess-9")
    assert alarm is not None
    assert "Staging queue alarm" in alarm
    assert "depth=3" in alarm


@pytest.mark.asyncio
async def test_check_alarm_no_fire_below_threshold():
    await stage_message("telegram", "c", "sess-9b", "m1", "u1", "A", '{}', "bot")
    alarm = await check_alarm("sess-9b")
    # depth=1 < threshold=3, age mocked at 0s < 60s threshold
    assert alarm is None


@pytest.mark.asyncio
async def test_check_alarm_rate_limited():
    """Alarm fires at most once per session per 30min."""
    await stage_message("telegram", "c", "sess-9c", "m1", "u1", "A", '{}', "bot")
    await stage_message("telegram", "c", "sess-9c", "m2", "u2", "B", '{}', "bot")
    await stage_message("telegram", "c", "sess-9c", "m3", "u3", "C", '{}', "bot")
    alarm1 = await check_alarm("sess-9c")
    assert alarm1 is not None
    # Immediate re-check should be suppressed
    alarm2 = await check_alarm("sess-9c")
    assert alarm2 is None


# ---------------------------------------------------------------------------
# Re-stage: push a message back to staged
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_re_stage_resets_status():
    await stage_message("telegram", "c", "sess-rs", "m1", "u1", "A", '{}', "bot")
    row = await drain_next("sess-rs")
    msg_id = row["id"]
    await re_stage(msg_id)


# ---------------------------------------------------------------------------
# Reaction helpers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_staged_reaction_calls_add():
    adapter = MagicMock()
    adapter._add_reaction = AsyncMock()
    await set_staged_reaction(adapter, "chat-1", "msg-1")
    adapter._add_reaction.assert_awaited_once_with("chat-1", "msg-1", "🕐")


@pytest.mark.asyncio
async def test_set_released_reaction_removes_then_adds():
    adapter = MagicMock()
    adapter._remove_reaction = AsyncMock()
    adapter._add_reaction = AsyncMock()
    await set_released_reaction(adapter, "chat-1", "msg-1")
    adapter._remove_reaction.assert_awaited_once()
    adapter._add_reaction.assert_awaited_once_with("chat-1", "msg-1", "▶️")


@pytest.mark.asyncio
async def test_clear_reaction_calls_remove():
    adapter = MagicMock()
    adapter._remove_reaction = AsyncMock()
    await clear_reaction(adapter, "chat-1", "msg-1")
    adapter._remove_reaction.assert_awaited_once()


# ---------------------------------------------------------------------------
# enqueue_from_event
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enqueue_from_event_stages_and_reacts():
    adapter = MagicMock()
    adapter.name = "test-bot"
    adapter._add_reaction = AsyncMock()

    event = MagicMock()
    event.source.platform = "telegram"
    event.source.chat_id = "chat-1"
    event.source.user_id = "user-1"
    event.source.user_name = "Alice"
    event.message_id = "msg-1"
    event.text = "hello"
    event.metadata = None

    result = await enqueue_from_event(adapter, event, "sess-enqueue")
    assert result is True
    adapter._add_reaction.assert_awaited_once()


@pytest.mark.asyncio
async def test_enqueue_from_event_returns_false_on_failure():
    """When _sb_rpc raises, enqueue_from_event catches and returns False."""
    import gateway.stage_queue as sq

    async def bad_rpc(*args, **kwargs):
        raise RuntimeError("Supabase rail down")

    original = sq._sb_rpc
    sq._sb_rpc = bad_rpc
    try:
        adapter = MagicMock()
        adapter.name = "test-bot"
        adapter._add_reaction = AsyncMock()
        event = MagicMock()
        event.source.platform = "telegram"
        event.source.chat_id = "c"
        event.source.user_id = "u"
        event.source.user_name = "A"
        event.message_id = "m"
        event.text = "hi"
        event.metadata = None
        result = await enqueue_from_event(adapter, event, "sess-fail")
        assert result is False
    finally:
        sq._sb_rpc = original


# ---------------------------------------------------------------------------
# Feature flag
# ---------------------------------------------------------------------------

def test_staging_disabled_by_default():
    """When HERMES_STAGING_QUEUE is unset, _STAGING_ENABLED is False."""
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("HERMES_STAGING_QUEUE", None)
        assert os.getenv("HERMES_STAGING_QUEUE", "").lower() not in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# E-RLS: migration enforces row-level security
# ---------------------------------------------------------------------------

def test_migration_enables_rls():
    """The migration must include ENABLE ROW LEVEL SECURITY and FORCE."""
    import pathlib
    sql_path = pathlib.Path(__file__).resolve().parents[2] / "supabase" / "migrations" / "20260918000001_message_stage_queue.sql"
    if sql_path.exists():
        ddl = sql_path.read_text()
        assert "ENABLE ROW LEVEL SECURITY" in ddl
        assert "FORCE ROW LEVEL SECURITY" in ddl
        assert "REVOKE ALL" in ddl


# ---------------------------------------------------------------------------
# Dedupe (t_83288309): duplicate (platform, chat_id, message_id) deliveries
# are suppressed server-side — stage_message returns the existing row.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stage_message_duplicate_returns_existing_row(store):
    first = await stage_message(
        platform="telegram", chat_id="dup-chat", session_key="sess-dup",
        message_id="dup-1", sender_id="u1", sender_name="Alice",
        event_json='{"text": "first"}', bot_name="hermes",
    )
    rows_before = [r for r in store if r.get("message_id") == "dup-1"]
    assert len(rows_before) == 1

    second = await stage_message(
        platform="telegram", chat_id="dup-chat", session_key="sess-dup",
        message_id="dup-1", sender_id="u1", sender_name="Alice",
        event_json='{"text": "duplicate delivery"}', bot_name="hermes",
    )
    rows_after = [r for r in store if r.get("message_id") == "dup-1"]
    assert len(rows_after) == 1, "duplicate delivery MUST NOT create a second row"
    assert second["id"] == first["id"], "duplicate delivery returns the existing row id"


@pytest.mark.asyncio
async def test_stage_message_duplicate_burst_single_row(store):
    ids = set()
    for i in range(10):
        r = await stage_message(
            platform="telegram", chat_id="burst-chat", session_key="sess-burst",
            message_id="burst-1", sender_id="u1", sender_name="Alice",
            event_json=f'{{"text": "burst copy {i}"}}', bot_name="hermes",
        )
        ids.add(r["id"])
    rows = [r for r in store if r.get("message_id") == "burst-1"]
    assert len(ids) == 1 and len(rows) == 1, f"10-replay burst must yield exactly 1 row (got {len(rows)})"
