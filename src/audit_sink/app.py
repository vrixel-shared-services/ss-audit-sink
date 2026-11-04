"""The service: one worker per destination, plus health, metrics and chain verification.

GET /healthz   liveness
GET /readyz    every worker is looping and none is stuck failing
GET /metrics   Prometheus: records written, rejected, duplicates; failures; last success
GET /verify    recompute the hash chain of every partition in Postgres
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import postgres
from .settings import Settings
from .worker import Worker

log = logging.getLogger("audit_sink")


def build_workers(settings: Settings) -> list[Worker]:
    if not settings.kafka_bootstrap:
        raise RuntimeError("Set AUDIT_SINK_KAFKA_BOOTSTRAP")
    from .worker import ArchiveDestination, PostgresDestination, kafka_consumer

    workers: list[Worker] = []
    for dest in settings.destinations:
        if dest == "postgres":
            workers.append(Worker(None, PostgresDestination(postgres.PostgresSink(connect(settings)))))
        elif dest == "archive":
            workers.append(Worker(None, ArchiveDestination(archive_sink(settings))))
        else:
            raise RuntimeError(f"Unknown destination {dest!r}: use postgres or archive")
        w = workers[-1]
        w.consumer = kafka_consumer(
            settings.kafka_bootstrap,
            f"{settings.group_prefix}-{dest}",
            settings.topics,
            dict(settings.kafka_config),
            w,
        )
        w.poll_batch = settings.poll_batch
    return workers


def connect(settings: Settings) -> Any:
    import psycopg

    if not settings.database_url:
        raise RuntimeError("Set AUDIT_SINK_DATABASE_URL")
    conn = psycopg.connect(settings.database_url, autocommit=True)
    postgres.setup(conn)
    return conn


def archive_sink(settings: Settings) -> Any:
    import boto3

    from .archive import ArchiveSink

    if not settings.archive_bucket:
        raise RuntimeError("Set AUDIT_SINK_ARCHIVE_BUCKET")
    s3 = boto3.client(
        "s3",
        endpoint_url=settings.archive_endpoint_url or None,
        region_name=settings.archive_region or None,
    )
    return ArchiveSink(
        s3,
        settings.archive_bucket,
        prefix=settings.archive_prefix,
        lock_mode=settings.archive_lock_mode,
        retention_days=settings.archive_retention_days,
        batch_events=settings.archive_batch_events,
        batch_seconds=settings.archive_batch_seconds,
    )


def create_app(workers: list[Worker], *, verify_conn: Any = None, stale_after: float = 120.0) -> FastAPI:
    stop = threading.Event()
    threads: list[threading.Thread] = []

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        for w in workers:
            t = threading.Thread(target=w.run, args=(stop,), name=f"sink-{w.destination.name}", daemon=True)
            t.start()
            threads.append(t)
        yield
        stop.set()
        for t in threads:
            t.join(30)

    app = FastAPI(title="Vrixel audit sink", lifespan=lifespan, docs_url=None, redoc_url=None)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz() -> Response:
        now = time.monotonic()
        state = {
            w.destination.name: {
                "alive": all(t.is_alive() for t in threads if t.name == f"sink-{w.destination.name}"),
                "looping": now - w.last_loop < stale_after,
                "failing": w.error,
            }
            for w in workers
        }
        ok = all(s["alive"] and s["looping"] and not s["failing"] for s in state.values())
        return JSONResponse(
            {"status": "ready" if ok else "not ready", "workers": state}, status_code=200 if ok else 503
        )

    @app.get("/metrics")
    def metrics() -> Response:
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/verify")
    def verify() -> Response:
        if verify_conn is None:
            return JSONResponse({"error": "this process doesn't write to Postgres"}, status_code=404)
        results, ok = [], True
        for topic, partition in postgres.partitions(verify_conn):
            n, broken = postgres.verify(verify_conn, topic, partition)
            ok = ok and broken is None
            results.append(
                {
                    "topic": topic,
                    "partition": partition,
                    "checked": n,
                    "break": broken.__dict__ if broken else None,
                }
            )
        return JSONResponse({"intact": ok, "partitions": results}, status_code=200 if ok else 409)

    return app
