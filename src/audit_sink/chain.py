"""The hash chain. Each record's hash covers the previous record's hash in the same partition,
its Kafka coordinates and the event itself, so a changed, removed or reordered row breaks every
hash after it. `verify` recomputes a partition's chain."""

from __future__ import annotations

import hashlib
import json
from typing import Any

GENESIS = "0" * 64


def canonical(event: Any) -> bytes:
    return json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def link(prev: str, topic: str, partition: int, offset: int, event: Any) -> str:
    h = hashlib.sha256()
    h.update(prev.encode())
    h.update(f"|{topic}|{partition}|{offset}|".encode())
    h.update(canonical(event))
    return h.hexdigest()
