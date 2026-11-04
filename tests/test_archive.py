from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime
from typing import Any

import boto3
import pytest
from moto import mock_aws

from audit_sink.archive import ArchiveSink, read
from audit_sink.records import Record

from .conftest import event, record


@pytest.fixture
def s3() -> Any:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="emr-audit", ObjectLockEnabledForBucket=True)
        yield client


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_records_are_archived_per_partition_as_locked_files(s3: Any) -> None:
    clock = Clock()
    a = ArchiveSink(
        s3,
        "emr-audit",
        batch_events=3,
        batch_seconds=60,
        clock=clock,
        now=lambda: datetime(2026, 11, 4, tzinfo=UTC),
        retention_days=10,
    )
    a.add([record(0), record(1), record(0, partition=1)])
    assert a.due() == []
    a.add([record(2, b"broken")])
    assert a.due() == [("audit.v1", 0)]  # three records
    key, last = a.flush(("audit.v1", 0))
    assert (
        last == 2
        and key
        == "audit/audit.v1/partition=0/date=2026-11-04/00000000000000000000-00000000000000000002.ndjson.gz"
    )
    obj = s3.get_object(Bucket="emr-audit", Key=key)
    assert (
        obj["ObjectLockMode"] == "COMPLIANCE"
        and obj["ObjectLockRetainUntilDate"].date().isoformat() == "2026-11-14"
    )
    assert obj["Metadata"]["records"] == "3"
    lines = [json.loads(x) for x in gzip.decompress(obj["Body"].read()).splitlines()]
    assert [x["offset"] for x in lines] == [0, 1, 2] and lines[2]["value"] == "broken"  # kept as sent
    assert lines[0]["value"]["type"] == "vrixel.audit.v1"
    clock.t = 61
    assert a.due() == [("audit.v1", 1)]  # old enough
    assert a.due(force=True) == [("audit.v1", 1)]
    a.drop([("audit.v1", 1)])
    assert a.pending == {}


def test_the_archive_reads_back_in_order(s3: Any) -> None:
    a = ArchiveSink(s3, "emr-audit", lock_mode="", batch_events=2)
    events = [event(ids=(f"p-{i}",)) for i in range(5)]
    for i, e in enumerate(events):
        a.add([record(i, e)])
        for p in a.due():
            a.flush(p)
    a.add([record(0, event(), partition=1)])
    a.flush(("audit.v1", 1))
    for p in a.due(force=True):
        a.flush(p)
    back = list(read(s3, "emr-audit", "audit/", "audit.v1"))
    assert [(r.partition, r.offset) for r in back] == [(0, 0), (0, 1), (0, 2), (0, 3), (0, 4), (1, 0)]
    assert [r.event()["id"] for r in back[:5]] == [e["id"] for e in events]
    assert back[0].timestamp_ms == record(0).timestamp_ms and back[0].key == "patient/p-1"


def test_paged_listings_are_followed(s3: Any) -> None:
    a = ArchiveSink(s3, "emr-audit", lock_mode="", batch_events=1)
    for i in range(3):
        a.add([record(i)])
        a.flush(("audit.v1", 0))

    class Paged:
        def __init__(self, inner: Any) -> None:
            self.inner = inner

        def list_objects_v2(self, **kw: Any) -> Any:
            return self.inner.list_objects_v2(MaxKeys=1, **kw)

        def get_object(self, **kw: Any) -> Any:
            return self.inner.get_object(**kw)

    assert [r.offset for r in read(Paged(s3), "emr-audit", "audit/", "audit.v1")] == [0, 1, 2]


def test_a_record_from_kafka() -> None:
    class Msg:
        def topic(self) -> str:
            return "audit.v1"

        def partition(self) -> int:
            return 3

        def offset(self) -> int:
            return 7

        def timestamp(self) -> tuple[int, int]:
            return (1, 1_000)

        def key(self) -> bytes | None:
            return None

        def value(self) -> bytes | None:
            return b"{}"

    assert Record.from_message(Msg()) == Record("audit.v1", 3, 7, 1_000, "", b"{}")
    with pytest.raises(ValueError, match="time"):
        Record("t", 0, 0, 0, "", json.dumps({**event(), "time": "yesterday"}).encode()).event()
