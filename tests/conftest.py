from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from audit_sink.records import Record

DB = os.environ.get("AUDIT_SINK_TEST_DATABASE_URL", "")


def event(action: str = "tool.find_patient", ids: tuple[str, ...] = ("p-1",), **data: Any) -> dict[str, Any]:
    return {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "source": "emr/care",
        "type": "vrixel.audit.v1",
        "time": "2026-11-04T06:00:00+00:00",
        "subject": f"patient/{','.join(ids)}",
        "datacontenttype": "application/json",
        "data": {
            "product": "emr",
            "service": "care",
            "environment": "test",
            "data_class": "phi",
            "action": action,
            "actor": {"id": "nurse-7", "type": "user", "client": "web"},
            "on_behalf_of": "",
            "resource": {"type": "patient", "ids": list(ids)},
            "outcome": "success",
            "reason": "",
            "trace_id": "ab" * 16,
            "span_id": "cd" * 8,
            "details": {},
            **data,
        },
    }


def record(offset: int, value: Any = None, *, topic: str = "audit.v1", partition: int = 0) -> Record:
    raw = value if isinstance(value, bytes) else json.dumps(value or event()).encode()
    return Record(topic, partition, offset, 1_793_772_000_000 + offset, "patient/p-1", raw)


@pytest.fixture
def db() -> Iterator[Any]:
    """A fresh database per test (AUDIT_SINK_TEST_DATABASE_URL; CI starts Postgres)."""
    if not DB:
        pytest.skip("set AUDIT_SINK_TEST_DATABASE_URL to run the Postgres tests")
    import psycopg

    name = f"audit_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(DB, autocommit=True) as admin:
        admin.execute(f"CREATE DATABASE {name}")
    url = DB.rsplit("/", 1)[0] + f"/{name}"
    conn = psycopg.connect(url, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()
        with psycopg.connect(DB, autocommit=True) as admin:
            admin.execute(f"DROP DATABASE {name} WITH (FORCE)")


def url_of(conn: Any) -> str:
    i = conn.info
    auth = f"{i.user}:{i.password}" if i.password else i.user
    return f"postgresql://{auth}@{i.host}:{i.port}/{i.dbname}"
