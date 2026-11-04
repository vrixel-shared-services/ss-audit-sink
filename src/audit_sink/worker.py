"""The loop: read the audit topics, hand records to a destination, commit offsets only for what
the destination has durably written. Each destination has its own consumer group, so a slow or
failing archive never holds up Postgres (and the reverse); Kafka keeps the records until both
have them. Delivery is at least once; both destinations ignore records they already hold.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any, Protocol

from prometheus_client import Counter, Gauge

from .archive import ArchiveSink
from .postgres import PostgresSink
from .records import Record

log = logging.getLogger("audit_sink")

EVENTS = Counter("audit_sink_records_total", "Records handled", ["destination", "result"])
FAILURES = Counter("audit_sink_failures_total", "Writes that failed and will be retried", ["destination"])
LAST_OK = Gauge("audit_sink_last_success_timestamp_seconds", "Last successful write", ["destination"])

Offsets = dict[tuple[str, int], int]  # (topic, partition) -> next offset to read


class Destination(Protocol):
    name: str

    def handle(self, records: list[Record]) -> Offsets: ...
    def revoked(self, partitions: list[tuple[str, int]]) -> Offsets: ...


class PostgresDestination:
    name = "postgres"

    def __init__(self, sink: PostgresSink) -> None:
        self.sink = sink
        self.buffer: list[Record] = []  # records not yet written (kept across a failed attempt)

    def handle(self, records: list[Record]) -> Offsets:
        self.buffer += records
        if not self.buffer:
            return {}
        out = self.sink.write(self.buffer)
        EVENTS.labels(self.name, "written").inc(out.events)
        EVENTS.labels(self.name, "rejected").inc(out.rejects)
        EVENTS.labels(self.name, "duplicate").inc(out.duplicates)
        done: Offsets = {}
        for r in self.buffer:
            done[(r.topic, r.partition)] = max(done.get((r.topic, r.partition), 0), r.offset + 1)
        self.buffer = []
        return done

    def revoked(self, partitions: list[tuple[str, int]]) -> Offsets:
        gone = set(partitions)
        self.buffer = [r for r in self.buffer if (r.topic, r.partition) not in gone]
        return {}


class ArchiveDestination:
    name = "archive"

    def __init__(self, sink: ArchiveSink) -> None:
        self.sink = sink

    def handle(self, records: list[Record]) -> Offsets:
        self.sink.add(records)
        return self._flush(self.sink.due())

    def revoked(self, partitions: list[tuple[str, int]]) -> Offsets:
        mine = [p for p in partitions if p in self.sink.pending]
        try:
            return self._flush(mine)
        finally:
            self.sink.drop(mine)  # unwritten ones are read again by the partition's next owner

    def _flush(self, partitions: list[tuple[str, int]]) -> Offsets:
        done: Offsets = {}
        for p in partitions:
            n = len(self.sink.pending[p])
            _, last = self.sink.flush(p)
            EVENTS.labels(self.name, "written").inc(n)
            done[p] = last + 1
        return done


class Worker:
    def __init__(
        self,
        consumer: Any,
        destination: Destination,
        *,
        poll_batch: int = 500,
        poll_timeout: float = 1.0,
        backoff: float = 5.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.consumer, self.destination = consumer, destination
        self.poll_batch, self.poll_timeout, self.backoff, self.sleep = (
            poll_batch,
            poll_timeout,
            backoff,
            sleep,
        )
        self.failing = False
        self.last_loop = time.monotonic()
        self.error = ""

    def step(self) -> None:
        """One poll and write. A failed write is retried on the next step, before reading more."""
        self.last_loop = time.monotonic()
        records: list[Record] = []
        if not self.failing:
            for msg in self.consumer.consume(num_messages=self.poll_batch, timeout=self.poll_timeout) or []:
                err = msg.error()
                if err is not None:
                    if getattr(err, "code", lambda: None)() != _PARTITION_EOF:
                        log.warning("kafka: %s", err)
                    continue
                records.append(Record.from_message(msg))
        try:
            done = self.destination.handle(records)
        except Exception as exc:
            FAILURES.labels(self.destination.name).inc()
            self.failing, self.error = True, f"{type(exc).__name__}: {exc}"[:300]
            log.exception("audit sink %s: write failed, retrying", self.destination.name)
            self.sleep(self.backoff)
            return
        self.failing, self.error = False, ""
        if done:
            self.commit(done)
            LAST_OK.labels(self.destination.name).set_to_current_time()

    def commit(self, done: Offsets) -> None:
        from confluent_kafka import TopicPartition

        self.consumer.commit(
            offsets=[TopicPartition(t, p, o) for (t, p), o in sorted(done.items())], asynchronous=False
        )

    def on_revoke(self, consumer: Any, partitions: list[Any]) -> None:
        try:
            done = self.destination.revoked([(tp.topic, tp.partition) for tp in partitions])
            if done:
                self.commit(done)
        except Exception:
            log.exception("audit sink %s: couldn't finish revoked partitions", self.destination.name)

    def run(self, stop: threading.Event) -> None:
        while not stop.is_set():
            self.step()
        self.on_revoke(self.consumer, self.consumer.assignment())
        self.consumer.close()


_PARTITION_EOF = -191  # confluent_kafka.KafkaError._PARTITION_EOF


def kafka_consumer(
    bootstrap: str, group: str, topics: list[str], config: dict[str, Any], worker: Worker
) -> Any:
    from confluent_kafka import Consumer

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            "client.id": group,
            **config,
        }
    )
    c.subscribe(topics, on_revoke=worker.on_revoke)
    return c
