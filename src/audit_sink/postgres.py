"""The queryable copy: an append-only, hash-chained Postgres table.

Rows are keyed by their Kafka coordinates, so a redelivered batch (after a crash before the
offsets were committed) inserts nothing twice. Within a partition each row's hash covers the
previous row's (chain.py); writers take a per-partition advisory lock, so two replicas briefly
holding the same partition during a rebalance can't fork the chain. Triggers refuse UPDATE,
DELETE and TRUNCATE; grant the sink's database role INSERT and SELECT only, and keep table
ownership with a separate role.

Messages that aren't audit events are kept in `audit_rejects` with the reason, not dropped.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from .chain import GENESIS, link
from .records import Record

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS audit_events (
        topic         text        NOT NULL,
        partition     integer     NOT NULL,
        "offset"      bigint      NOT NULL,
        event_id      text        NOT NULL,
        time          timestamptz NOT NULL,
        product       text        NOT NULL DEFAULT '',
        service       text        NOT NULL DEFAULT '',
        environment   text        NOT NULL DEFAULT '',
        data_class    text        NOT NULL DEFAULT '',
        action        text        NOT NULL,
        actor_id      text        NOT NULL DEFAULT '',
        actor_type    text        NOT NULL DEFAULT '',
        on_behalf_of  text        NOT NULL DEFAULT '',
        resource_type text        NOT NULL DEFAULT '',
        resource_ids  text[]      NOT NULL DEFAULT '{}',
        outcome       text        NOT NULL DEFAULT '',
        trace_id      text        NOT NULL DEFAULT '',
        event         jsonb       NOT NULL,
        prev_hash     text        NOT NULL,
        hash          text        NOT NULL,
        received_at   timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (topic, partition, "offset")
    )
    """,
    "CREATE INDEX IF NOT EXISTS audit_events_resource ON audit_events USING gin (resource_ids)",
    "CREATE INDEX IF NOT EXISTS audit_events_actor ON audit_events (actor_id, time)",
    "CREATE INDEX IF NOT EXISTS audit_events_time ON audit_events (time)",
    "CREATE INDEX IF NOT EXISTS audit_events_trace ON audit_events (trace_id) WHERE trace_id <> ''",
    "CREATE INDEX IF NOT EXISTS audit_events_event_id ON audit_events (event_id)",
    """
    CREATE TABLE IF NOT EXISTS audit_rejects (
        topic       text        NOT NULL,
        partition   integer     NOT NULL,
        "offset"    bigint      NOT NULL,
        reason      text        NOT NULL,
        raw         bytea       NOT NULL,
        received_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (topic, partition, "offset")
    )
    """,
    """
    CREATE OR REPLACE FUNCTION audit_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        RAISE EXCEPTION 'the audit trail is append-only: % on % is not allowed', TG_OP, TG_TABLE_NAME;
    END $$
    """,
    "DROP TRIGGER IF EXISTS audit_events_no_change ON audit_events",
    """CREATE TRIGGER audit_events_no_change BEFORE UPDATE OR DELETE ON audit_events
       FOR EACH ROW EXECUTE FUNCTION audit_append_only()""",
    "DROP TRIGGER IF EXISTS audit_events_no_truncate ON audit_events",
    """CREATE TRIGGER audit_events_no_truncate BEFORE TRUNCATE ON audit_events
       FOR EACH STATEMENT EXECUTE FUNCTION audit_append_only()""",
)


def setup(conn: psycopg.Connection[Any]) -> None:
    with conn.transaction():
        for stmt in SCHEMA:
            conn.execute(stmt)


@dataclass
class Written:
    events: int = 0
    rejects: int = 0
    duplicates: int = 0


class PostgresSink:
    name = "postgres"

    def __init__(self, conn: psycopg.Connection[Any]) -> None:
        self.conn = conn

    def write(self, records: Iterable[Record]) -> Written:
        """Insert a batch in one transaction; the caller commits Kafka offsets after this returns."""
        out = Written()
        by_partition: dict[tuple[str, int], list[Record]] = {}
        for r in records:
            by_partition.setdefault((r.topic, r.partition), []).append(r)
        with self.conn.transaction():
            for (topic, partition), rows in sorted(by_partition.items()):
                self.conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s), %s)", (topic, partition))
                prev = self._last_hash(topic, partition)
                for r in sorted(rows, key=lambda r: r.offset):
                    if self._seen(r):
                        out.duplicates += 1
                        continue
                    try:
                        event = r.event()
                    except ValueError as exc:
                        self.conn.execute(
                            'INSERT INTO audit_rejects (topic, partition, "offset", reason, raw)'
                            " VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            (topic, partition, r.offset, str(exc)[:500], r.raw),
                        )
                        out.rejects += 1
                        continue
                    h = link(prev, topic, partition, r.offset, event)
                    self._insert(r, event, prev, h)
                    prev = h
                    out.events += 1
        return out

    def _last_hash(self, topic: str, partition: int) -> str:
        row = self.conn.execute(
            "SELECT hash FROM audit_events WHERE topic = %s AND partition = %s"
            ' ORDER BY "offset" DESC LIMIT 1',
            (topic, partition),
        ).fetchone()
        return str(row[0]) if row else GENESIS

    def _seen(self, r: Record) -> bool:
        return (
            self.conn.execute(
                'SELECT 1 FROM audit_events WHERE topic = %s AND partition = %s AND "offset" = %s'
                " UNION ALL SELECT 1 FROM audit_rejects"
                ' WHERE topic = %s AND partition = %s AND "offset" = %s',
                (r.topic, r.partition, r.offset) * 2,
            ).fetchone()
            is not None
        )

    def _insert(self, r: Record, e: dict[str, Any], prev: str, h: str) -> None:
        d = e["data"]
        actor = d.get("actor") or {}
        resource = d.get("resource") or {}
        self.conn.execute(
            """INSERT INTO audit_events (topic, partition, "offset", event_id, time, product, service,
                   environment, data_class, action, actor_id, actor_type, on_behalf_of, resource_type,
                   resource_ids, outcome, trace_id, event, prev_hash, hash)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                r.topic,
                r.partition,
                r.offset,
                str(e["id"]),
                e["time"],
                str(d.get("product") or ""),
                str(d.get("service") or ""),
                str(d.get("environment") or ""),
                str(d.get("data_class") or ""),
                str(d.get("action") or ""),
                str(actor.get("id") or ""),
                str(actor.get("type") or ""),
                str(d.get("on_behalf_of") or ""),
                str(resource.get("type") or ""),
                [str(i) for i in resource.get("ids") or []],
                str(d.get("outcome") or ""),
                str(d.get("trace_id") or ""),
                Jsonb(e),
                prev,
                h,
            ),
        )


@dataclass(frozen=True)
class Break:
    topic: str
    partition: int
    offset: int
    reason: str


def verify(conn: psycopg.Connection[Any], topic: str, partition: int) -> tuple[int, Break | None]:
    """Recompute a partition's chain. Returns how many rows were checked and the first break."""
    prev, n = GENESIS, 0
    for offset, event, stored_prev, stored in _rows(conn, topic, partition):
        if stored_prev != prev:
            return n, Break(
                topic, partition, offset, "previous hash doesn't match: a row is missing or was replaced"
            )
        expected = link(prev, topic, partition, offset, event)
        if stored != expected:
            return n, Break(topic, partition, offset, "hash doesn't match the event: the row was changed")
        prev, n = stored, n + 1
    return n, None


def _rows(conn: psycopg.Connection[Any], topic: str, partition: int) -> Iterator[tuple[int, Any, str, str]]:
    with conn.transaction(), conn.cursor(name="audit_verify") as cur:
        cur.execute(
            'SELECT "offset", event, prev_hash, hash FROM audit_events'
            ' WHERE topic = %s AND partition = %s ORDER BY "offset"',
            (topic, partition),
        )
        for offset, event, prev, h in cur:
            yield int(offset), event if not isinstance(event, str) else json.loads(event), str(prev), str(h)


def partitions(conn: psycopg.Connection[Any]) -> list[tuple[str, int]]:
    rows = conn.execute("SELECT DISTINCT topic, partition FROM audit_events ORDER BY 1, 2").fetchall()
    return [(str(t), int(p)) for t, p in rows]
