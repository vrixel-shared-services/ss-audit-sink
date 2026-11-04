"""Configuration from the environment (AUDIT_SINK_*). Credentials come from OpenChoreo secret
references; nothing secret has a default."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AUDIT_SINK_")

    # Kafka: the product's audit topics, read by one consumer group per destination.
    kafka_bootstrap: str = ""
    kafka_config: dict[str, str] = Field(default_factory=dict)  # security.protocol, sasl.* ...
    topics: list[str] = Field(default_factory=lambda: ["audit.v1"])
    group_prefix: str = "audit-sink"
    # Which destinations this process writes: postgres, archive, or both.
    destinations: list[str] = Field(default_factory=lambda: ["postgres", "archive"])
    # Postgres: the queryable, hash-chained copy.
    database_url: str = ""
    # Object storage (S3 API: AWS S3, MinIO, Ceph, GCS interoperability): the backup.
    archive_bucket: str = ""
    archive_prefix: str = "audit/"
    archive_endpoint_url: str = ""  # MinIO or another S3-compatible store; empty for AWS
    archive_region: str = ""
    archive_lock_mode: str = "COMPLIANCE"  # COMPLIANCE | GOVERNANCE | "" (bucket default)
    archive_retention_days: int = 2190  # six years, the HIPAA documentation period
    archive_batch_events: int = 5000  # write a file when a partition has this many records,
    archive_batch_seconds: int = 300  # or when its oldest unwritten record is this old
    poll_batch: int = 500
    port: int = 8080
