"""What the sink reads from Kafka: a message's coordinates and its audit event."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

EVENT_TYPE = "vrixel.audit.v1"


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    key: str
    raw: bytes

    def event(self) -> dict[str, Any]:
        """The audit event, or ValueError if this isn't one."""
        try:
            e = json.loads(self.raw)
        except ValueError as exc:
            raise ValueError(f"not JSON: {exc}") from exc
        if not isinstance(e, dict) or e.get("type") != EVENT_TYPE or not isinstance(e.get("data"), dict):
            raise ValueError(f"not a {EVENT_TYPE} event")
        for f in ("id", "time"):
            if not e.get(f):
                raise ValueError(f"missing {f}")
        try:
            datetime.fromisoformat(str(e["time"]))
        except ValueError as exc:
            raise ValueError("time is not an ISO 8601 timestamp") from exc
        return e

    def line(self) -> dict[str, Any]:
        """The record as one line of an archive file: the event as sent, with where it came from."""
        try:
            value: Any = json.loads(self.raw)
        except ValueError:
            value = self.raw.decode("utf-8", errors="replace")
        return {
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "timestamp": datetime.fromtimestamp(self.timestamp_ms / 1000, UTC).isoformat(),
            "key": self.key,
            "value": value,
        }

    @classmethod
    def from_line(cls, line: dict[str, Any]) -> Record:
        v = line["value"]
        raw = v.encode() if isinstance(v, str) else json.dumps(v, separators=(",", ":")).encode()
        ts = int(datetime.fromisoformat(line["timestamp"]).timestamp() * 1000)
        return cls(line["topic"], int(line["partition"]), int(line["offset"]), ts, line.get("key") or "", raw)

    @classmethod
    def from_message(cls, msg: Any) -> Record:
        _, ts = msg.timestamp()
        key = msg.key()
        return cls(
            msg.topic(),
            msg.partition(),
            msg.offset(),
            int(ts or 0),
            key.decode("utf-8", errors="replace") if key else "",
            msg.value() or b"",
        )
