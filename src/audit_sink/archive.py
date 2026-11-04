"""The backup: the audit topic, as written, in object storage.

Records are buffered per partition and written as gzipped NDJSON files, one line per Kafka
record (coordinates plus the event exactly as sent, including any that aren't valid events):

    <prefix><topic>/partition=<n>/date=<YYYY-MM-DD>/<first offset>-<last offset>.ndjson.gz

Each file is written with S3 Object Lock (retention set from AUDIT_SINK_ARCHIVE_RETENTION_DAYS),
so in COMPLIANCE mode no one, including the account's administrators, can delete or overwrite it
before then. The bucket needs Object Lock enabled (which also turns on versioning). Any S3 API
store works: AWS S3, MinIO, Ceph; `restore` replays the files into Postgres.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

from .records import Record


class ArchiveSink:
    name = "archive"

    def __init__(
        self,
        s3: Any,
        bucket: str,
        *,
        prefix: str = "audit/",
        lock_mode: str = "COMPLIANCE",
        retention_days: int = 2190,
        batch_events: int = 5000,
        batch_seconds: int = 300,
        clock: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.s3, self.bucket, self.prefix = s3, bucket, prefix
        self.lock_mode, self.retention_days = lock_mode, retention_days
        self.batch_events, self.batch_seconds = batch_events, batch_seconds
        self.clock, self.now = clock, now
        self.pending: dict[tuple[str, int], list[Record]] = {}
        self.since: dict[tuple[str, int], float] = {}

    def add(self, records: Iterable[Record]) -> None:
        for r in records:
            key = (r.topic, r.partition)
            self.pending.setdefault(key, []).append(r)
            self.since.setdefault(key, self.clock())

    def due(self, force: bool = False) -> list[tuple[str, int]]:
        now = self.clock()
        return [
            k
            for k, rows in self.pending.items()
            if rows and (force or len(rows) >= self.batch_events or now - self.since[k] >= self.batch_seconds)
        ]

    def flush(self, partition: tuple[str, int]) -> tuple[str, int]:
        """Write one partition's buffered records; returns (object key, last offset written)."""
        rows = sorted(self.pending[partition], key=lambda r: r.offset)
        topic, n = partition
        body = gzip.compress(
            b"".join(json.dumps(r.line(), separators=(",", ":")).encode() + b"\n" for r in rows), mtime=0
        )
        day = datetime.fromtimestamp(rows[0].timestamp_ms / 1000, UTC).strftime("%Y-%m-%d")
        span = f"{rows[0].offset:020d}-{rows[-1].offset:020d}"
        key = f"{self.prefix}{topic}/partition={n}/date={day}/{span}.ndjson.gz"
        extra: dict[str, Any] = {}
        if self.lock_mode:
            extra = {
                "ObjectLockMode": self.lock_mode,
                "ObjectLockRetainUntilDate": self.now() + timedelta(days=self.retention_days),
            }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
            ChecksumAlgorithm="SHA256",
            Metadata={"records": str(len(rows)), "sha256": hashlib.sha256(body).hexdigest()},
            **extra,
        )
        del self.pending[partition]
        del self.since[partition]
        return key, rows[-1].offset

    def drop(self, partitions: Iterable[tuple[str, int]]) -> None:
        """Forget buffered records of partitions this process no longer owns (after a rebalance
        they are read again by the new owner from the last committed offset)."""
        for p in list(partitions):
            self.pending.pop(p, None)
            self.since.pop(p, None)


def read(s3: Any, bucket: str, prefix: str, topic: str) -> Iterator[Record]:
    """Every archived record of a topic, partition by partition, in offset order."""
    keys: list[str] = []
    token: dict[str, str] = {}
    while True:
        page = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}{topic}/", **token)
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".ndjson.gz")]
        if not page.get("IsTruncated"):
            break
        token = {"ContinuationToken": page["NextContinuationToken"]}

    def order(k: str) -> tuple[int, str]:
        part = int(k.split("/partition=", 1)[1].split("/", 1)[0])
        return part, k.rsplit("/", 1)[1]

    for key in sorted(keys, key=order):
        body = gzip.decompress(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        for line in body.splitlines():
            if line.strip():
                yield Record.from_line(json.loads(line))
