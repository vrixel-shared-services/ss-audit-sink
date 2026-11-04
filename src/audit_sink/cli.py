"""audit-sink run | setup | verify | restore --topic T

run       consume the audit topics into the configured destinations (the container's command)
setup     create the Postgres tables and append-only triggers
verify    recompute every partition's hash chain; exits 1 on a break
restore   replay a topic from the object-storage archive into Postgres (rows already there are
          skipped), e.g. after losing the database
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from . import postgres
from .settings import Settings


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="audit-sink")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run")
    sub.add_parser("setup")
    sub.add_parser("verify")
    r = sub.add_parser("restore")
    r.add_argument("--topic", required=True)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='{"log.level":"%(levelname)s","message":"%(message)s"}')
    settings = Settings()
    from .app import archive_sink, build_workers, connect, create_app

    if args.cmd == "run":  # pragma: no cover - exercised by the container
        import uvicorn

        workers = build_workers(settings)
        verify_conn = connect(settings) if "postgres" in settings.destinations else None
        uvicorn.run(
            create_app(workers, verify_conn=verify_conn),
            host="0.0.0.0",  # noqa: S104 - a container listens on all interfaces
            port=settings.port,
            log_config=None,
        )
        return 0
    conn = connect(settings)
    if args.cmd == "setup":
        print("audit tables ready")
        return 0
    if args.cmd == "verify":
        ok = True
        for topic, partition in postgres.partitions(conn):
            n, broken = postgres.verify(conn, topic, partition)
            ok = ok and broken is None
            print(
                json.dumps(
                    {
                        "topic": topic,
                        "partition": partition,
                        "checked": n,
                        "break": broken.__dict__ if broken else None,
                    }
                )
            )
        return 0 if ok else 1
    from .archive import read

    arch = archive_sink(settings)
    sink = postgres.PostgresSink(conn)
    total = postgres.Written()
    batch: list = []  # type: ignore[type-arg]
    for rec in read(arch.s3, arch.bucket, arch.prefix, args.topic):
        batch.append(rec)
        if len(batch) >= 1000:
            _add(total, sink.write(batch))
            batch = []
    if batch:
        _add(total, sink.write(batch))
    print(json.dumps(total.__dict__))
    return 0


def _add(total: postgres.Written, w: postgres.Written) -> None:
    total.events += w.events
    total.rejects += w.rejects
    total.duplicates += w.duplicates


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
