from __future__ import annotations

import json
import threading
import time
from typing import Any

import boto3
from fastapi.testclient import TestClient
from moto import mock_aws

from audit_sink import postgres
from audit_sink.app import create_app
from audit_sink.archive import ArchiveSink
from audit_sink.records import Record
from audit_sink.worker import ArchiveDestination, PostgresDestination, Worker

from .conftest import event


class Err:
    def __init__(self, code: int) -> None:
        self._code = code

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return f"error {self._code}"


class Msg:
    def __init__(self, offset: int, value: Any = None, partition: int = 0, error: Any = None) -> None:
        self._offset, self._partition, self._error = offset, partition, error
        self._value = json.dumps(value or event()).encode()

    def error(self) -> Any:
        return self._error

    def topic(self) -> str:
        return "audit.v1"

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def timestamp(self) -> tuple[int, int]:
        return (1, 1_793_772_000_000)

    def key(self) -> bytes:
        return b"patient/p-1"

    def value(self) -> bytes:
        return self._value


class FakeConsumer:
    def __init__(self, batches: list[list[Msg]]) -> None:
        self.batches = batches
        self.commits: list[list[tuple[str, int, int]]] = []
        self.closed = False

    def consume(self, num_messages: int, timeout: float) -> list[Msg]:
        return self.batches.pop(0) if self.batches else []

    def commit(self, offsets: list[Any], asynchronous: bool) -> None:
        assert asynchronous is False
        self.commits.append([(o.topic, o.partition, o.offset) for o in offsets])

    def assignment(self) -> list[Any]:
        from confluent_kafka import TopicPartition

        return [TopicPartition("audit.v1", 0)]

    def close(self) -> None:
        self.closed = True


class Flaky:
    """A destination that fails a given number of times first."""

    name = "flaky"

    def __init__(self, failures: int) -> None:
        self.failures, self.seen = failures, []

    def handle(self, records: list[Record]) -> dict[tuple[str, int], int]:
        self.seen.append([r.offset for r in records])
        if self.failures:
            self.failures -= 1
            raise ConnectionError("database down")
        return {("audit.v1", 0): records[-1].offset + 1} if records else {}

    def revoked(self, partitions: list[tuple[str, int]]) -> dict[tuple[str, int], int]:
        return {}


def test_offsets_are_committed_only_after_postgres_has_the_records(db: Any) -> None:
    postgres.setup(db)
    consumer = FakeConsumer([[Msg(0), Msg(1), Msg(0, partition=1), Msg(2, error=Err(-191))], []])
    w = Worker(consumer, PostgresDestination(postgres.PostgresSink(db)), sleep=lambda s: None)
    w.step()
    assert consumer.commits == [[("audit.v1", 0, 2), ("audit.v1", 1, 1)]]
    w.step()  # nothing new: nothing to commit
    assert len(consumer.commits) == 1
    assert db.execute("SELECT count(*) FROM audit_events").fetchone()[0] == 3


def test_a_failed_write_is_retried_before_reading_more() -> None:
    consumer = FakeConsumer([[Msg(0), Msg(1)], [Msg(2)]])
    dest = Flaky(failures=1)
    slept: list[float] = []
    w = Worker(consumer, dest, sleep=slept.append, backoff=3)
    w.step()
    assert w.failing and "database down" in w.error and slept == [3] and consumer.commits == []
    w.step()  # retries without reading
    assert dest.seen == [[0, 1], []] and not w.failing  # (Flaky doesn't buffer; PostgresDestination does)
    w.step()
    assert consumer.commits[-1] == [("audit.v1", 0, 3)]


def test_postgres_keeps_a_failed_batch_and_writes_it_on_retry(db: Any) -> None:
    postgres.setup(db)

    class Down(postgres.PostgresSink):
        def __init__(self, conn: Any) -> None:
            super().__init__(conn)
            self.down = True

        def write(self, records: Any) -> postgres.Written:
            if self.down:
                self.down = False
                raise ConnectionError("down")
            return super().write(records)

    consumer = FakeConsumer([[Msg(0), Msg(1)]])
    dest = PostgresDestination(Down(db))
    w = Worker(consumer, dest, sleep=lambda s: None)
    w.step()
    assert consumer.commits == [] and len(dest.buffer) == 2
    w.step()
    assert consumer.commits == [[("audit.v1", 0, 2)]] and dest.buffer == []
    dest.buffer = [Record.from_message(Msg(5, partition=4))]
    assert dest.revoked([("audit.v1", 4)]) == {} and dest.buffer == []


def test_the_archive_commits_what_each_file_holds() -> None:
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="bkt", ObjectLockEnabledForBucket=True)
        consumer = FakeConsumer([[Msg(0), Msg(1), Msg(2), Msg(0, partition=1)]])
        w = Worker(consumer, ArchiveDestination(ArchiveSink(s3, "bkt", batch_events=2)), sleep=lambda s: None)
        w.step()
        assert consumer.commits == [[("audit.v1", 0, 3)]]  # partition 1 waits for more
        # On shutdown (or a rebalance) the rest is written and committed.
        w.on_revoke(consumer, [type("TP", (), {"topic": "audit.v1", "partition": 1})()])
        assert consumer.commits[-1] == [("audit.v1", 1, 1)]
        assert len(s3.list_objects_v2(Bucket="bkt")["Contents"]) == 2


def test_run_stops_cleanly() -> None:
    consumer = FakeConsumer([[Msg(0)]])
    dest = Flaky(failures=0)
    w = Worker(consumer, dest, sleep=lambda s: None, poll_timeout=0.01)
    stop = threading.Event()

    def step_then_stop() -> None:
        Worker.step(w)
        stop.set()

    w.step = step_then_stop  # type: ignore[method-assign]
    w.run(stop)
    assert consumer.closed and consumer.commits == [[("audit.v1", 0, 1)]]


def test_health_readiness_metrics_and_verify(db: Any) -> None:
    postgres.setup(db)
    postgres.PostgresSink(db).write([Record.from_message(Msg(i)) for i in range(3)])
    consumer = FakeConsumer([])
    dest = Flaky(0)
    w = Worker(consumer, dest, sleep=lambda s: time.sleep(0.01), poll_timeout=0.01)
    with TestClient(create_app([w], verify_conn=db)) as c:
        assert c.get("/healthz").json() == {"status": "ok"}
        r = c.get("/readyz")
        assert r.status_code == 200 and r.json()["workers"]["flaky"]["alive"]
        dest.failures = 10**9  # the destination goes down
        deadline = time.monotonic() + 5
        while not w.error and time.monotonic() < deadline:
            time.sleep(0.01)
        r = c.get("/readyz")
        assert r.status_code == 503 and "database down" in r.json()["workers"]["flaky"]["failing"]
        dest.failures = 0
        assert "audit_sink_records_total" in c.get("/metrics").text
        v = c.get("/verify").json()
        assert v["intact"] and v["partitions"] == [
            {"topic": "audit.v1", "partition": 0, "checked": 3, "break": None}
        ]
        db.execute("ALTER TABLE audit_events DISABLE TRIGGER audit_events_no_change")
        db.execute('DELETE FROM audit_events WHERE "offset" = 1')
        assert c.get("/verify").status_code == 409
    with TestClient(create_app([])) as c:
        assert c.get("/verify").status_code == 404
