from __future__ import annotations

from typing import Any

import boto3
import pytest
from moto import mock_aws

from audit_sink import app as app_mod
from audit_sink.archive import ArchiveSink
from audit_sink.cli import main
from audit_sink.settings import Settings

from .conftest import record, url_of


def env(monkeypatch: pytest.MonkeyPatch, db: Any) -> None:
    monkeypatch.setenv("AUDIT_SINK_DATABASE_URL", url_of(db))
    monkeypatch.setenv("AUDIT_SINK_ARCHIVE_BUCKET", "emr-audit")
    monkeypatch.setenv("AUDIT_SINK_ARCHIVE_REGION", "us-east-1")


def test_setup_restore_and_verify(db: Any, monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    env(monkeypatch, db)
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="emr-audit", ObjectLockEnabledForBucket=True)
        a = ArchiveSink(s3, "emr-audit", batch_events=2)
        a.add([record(i) for i in range(3)] + [record(0, partition=1), record(3, b"junk")])
        for p in a.due(force=True):
            a.flush(p)
        assert main(["setup"]) == 0
        assert main(["restore", "--topic", "audit.v1"]) == 0
        assert '"events": 4, "rejects": 1, "duplicates": 0' in capsys.readouterr().out
        assert main(["restore", "--topic", "audit.v1"]) == 0  # again: nothing new
        assert '"events": 0, "rejects": 0, "duplicates": 5' in capsys.readouterr().out
        assert main(["verify"]) == 0
        db.execute("ALTER TABLE audit_events DISABLE TRIGGER audit_events_no_change")
        db.execute("UPDATE audit_events SET action = 'x' WHERE \"offset\" = 0 AND partition = 0")
        db.execute(
            "UPDATE audit_events SET event = jsonb_set(event, '{data,action}', '\"x\"')"
            ' WHERE "offset" = 0 AND partition = 0'
        )
        assert main(["verify"]) == 1


def test_configuration_is_checked() -> None:
    with pytest.raises(RuntimeError, match="KAFKA_BOOTSTRAP"):
        app_mod.build_workers(Settings(kafka_bootstrap=""))
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        app_mod.connect(Settings())
    with pytest.raises(RuntimeError, match="ARCHIVE_BUCKET"):
        app_mod.archive_sink(Settings())
    with pytest.raises(RuntimeError, match="Unknown destination"):
        app_mod.build_workers(Settings(kafka_bootstrap="127.0.0.1:9", destinations=["syslog"]))


def test_workers_get_one_consumer_group_per_destination(db: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    env(monkeypatch, db)
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="emr-audit")
        workers = app_mod.build_workers(Settings(kafka_bootstrap="127.0.0.1:9", poll_batch=7))
        assert [w.destination.name for w in workers] == ["postgres", "archive"]
        assert all(w.poll_batch == 7 for w in workers)
        for w in workers:
            w.consumer.close()
