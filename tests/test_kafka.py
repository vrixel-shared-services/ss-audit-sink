"""End to end against a real broker: CI starts Kafka and sets AUDIT_SINK_TEST_KAFKA."""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any

import boto3
import pytest
from moto import mock_aws

from audit_sink.app import build_workers
from audit_sink.settings import Settings

from .conftest import event, url_of

KAFKA = os.environ.get("AUDIT_SINK_TEST_KAFKA", "")
pytestmark = [pytest.mark.kafka, pytest.mark.skipif(not KAFKA, reason="set AUDIT_SINK_TEST_KAFKA")]


def test_audit_records_flow_from_kafka_to_postgres_and_the_archive(
    db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    topic = f"audit.test.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": KAFKA})  # keep a reference: its futures need the handle
    deadline = time.monotonic() + 60
    while True:  # the broker container may still be starting
        try:
            admin.list_topics(timeout=5)
            break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)
    admin.create_topics([NewTopic(topic, 2, 1)])[topic].result(30)
    p = Producer({"bootstrap.servers": KAFKA, "enable.idempotence": True, "acks": "all"})
    sent = [event(ids=(f"p-{i}",)) for i in range(6)]
    for e in sent:
        p.produce(topic, key=e["subject"].encode(), value=json.dumps(e).encode())
    assert p.flush(30) == 0

    monkeypatch.setenv("AUDIT_SINK_DATABASE_URL", url_of(db))
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(
            Bucket="emr-audit", ObjectLockEnabledForBucket=True
        )
        settings = Settings(
            kafka_bootstrap=KAFKA,
            topics=[topic],
            group_prefix=f"sink-{uuid.uuid4().hex[:6]}",
            archive_bucket="emr-audit",
            archive_region="us-east-1",
            archive_batch_seconds=0,
        )
        workers = build_workers(settings)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            for w in workers:
                w.step()
            stored = db.execute("SELECT count(*) FROM audit_events").fetchone()[0]
            archived = (
                boto3.client("s3", region_name="us-east-1")
                .list_objects_v2(Bucket="emr-audit")
                .get("KeyCount", 0)
            )
            if stored == len(sent) and archived:
                break
        for w in workers:
            assert not w.error, w.error
        ids = {r[0] for r in db.execute("SELECT event_id FROM audit_events")}
        assert ids == {e["id"] for e in sent}
        committed = workers[0].consumer.committed(workers[0].consumer.assignment(), timeout=10)
        assert sum(tp.offset for tp in committed if tp.offset >= 0) == len(sent)
        for w in workers:
            w.consumer.close()
