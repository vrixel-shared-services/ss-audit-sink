from __future__ import annotations

from typing import Any

import psycopg
import pytest

from audit_sink import postgres
from audit_sink.chain import GENESIS, link

from .conftest import event, record


def sink(db: Any) -> postgres.PostgresSink:
    postgres.setup(db)
    postgres.setup(db)  # idempotent
    return postgres.PostgresSink(db)


def test_events_are_stored_queryable_and_chained(db: Any) -> None:
    s = sink(db)
    e0, e1 = event(), event("tool.create_task", ("p-1", "t-9"))
    out = s.write([record(1, e1), record(0, e0)])
    assert (out.events, out.rejects, out.duplicates) == (2, 0, 0)
    rows = db.execute(
        'SELECT "offset", event_id, action, actor_id, resource_type, resource_ids, data_class, trace_id,'
        ' prev_hash, hash FROM audit_events ORDER BY "offset"'
    ).fetchall()
    assert [(r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in rows] == [
        (0, e0["id"], "tool.find_patient", "nurse-7", "patient", ["p-1"], "phi"),
        (1, e1["id"], "tool.create_task", "nurse-7", "patient", ["p-1", "t-9"], "phi"),
    ]
    assert rows[0][7] == "ab" * 16
    assert rows[0][8] == GENESIS and rows[0][9] == link(GENESIS, "audit.v1", 0, 0, e0)
    assert rows[1][8] == rows[0][9]
    # Who touched patient p-1, from the index:
    assert (
        db.execute("SELECT count(*) FROM audit_events WHERE resource_ids @> ARRAY['p-1']").fetchone()[0] == 2
    )
    assert postgres.verify(db, "audit.v1", 0) == (2, None)


def test_redelivered_records_are_not_stored_twice_and_the_chain_continues(db: Any) -> None:
    s = sink(db)
    s.write([record(0), record(1)])
    out = s.write([record(1), record(2), record(0, partition=1)])  # a redelivery, then new records
    assert (out.events, out.duplicates) == (2, 1)
    assert postgres.verify(db, "audit.v1", 0) == (3, None)
    assert postgres.verify(db, "audit.v1", 1) == (1, None)
    assert postgres.partitions(db) == [("audit.v1", 0), ("audit.v1", 1)]


def test_messages_that_arent_audit_events_are_kept_aside(db: Any) -> None:
    s = sink(db)
    out = s.write(
        [record(0, b"not json"), record(1, {"type": "other"}), record(2), record(3, {**event(), "id": ""})]
    )
    assert (out.events, out.rejects) == (1, 3)
    reasons = [r[0] for r in db.execute('SELECT reason FROM audit_rejects ORDER BY "offset"')]
    assert (
        reasons[0].startswith("not JSON")
        and "not a vrixel.audit.v1 event" in reasons[1]
        and reasons[2] == "missing id"
    )
    assert s.write([record(0, b"not json")]).duplicates == 1


def test_the_table_is_append_only(db: Any) -> None:
    s = sink(db)
    s.write([record(0)])
    for stmt in ("UPDATE audit_events SET action = 'x'", "DELETE FROM audit_events", "TRUNCATE audit_events"):
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            db.execute(stmt)


def test_verify_finds_a_changed_or_missing_row(db: Any) -> None:
    s = sink(db)
    s.write([record(i) for i in range(4)])
    db.execute(
        "ALTER TABLE audit_events DISABLE TRIGGER audit_events_no_change"
    )  # an attacker with owner rights
    db.execute(
        """UPDATE audit_events SET event = jsonb_set(event, '{data,actor,id}', '"someone-else"')"""
        """ WHERE "offset" = 2"""
    )
    n, broken = postgres.verify(db, "audit.v1", 0)
    assert n == 2 and broken is not None and broken.offset == 2 and "changed" in broken.reason
    db.execute("""DELETE FROM audit_events WHERE "offset" = 2""")
    n, broken = postgres.verify(db, "audit.v1", 0)
    assert broken is not None and broken.offset == 3 and "missing" in broken.reason
