# Audit sink

A Vrixel shared service. It reads a product's audit topic from Kafka and keeps two copies:

- **Postgres**: queryable, append-only and hash-chained, for investigations and access reports.
- **Object storage**: gzipped files under S3 Object Lock, the backup that no one can delete before
  its retention date.

Like every shared service it runs inside each product's own runtime, next to the product's Kafka
and database; shared services own no runtime of their own.

```
services (template audit.py) ──> Kafka: <product> audit topic ──┬─> consumer group audit-sink-postgres ──> audit_events (hash chain)
                                                                 └─> consumer group audit-sink-archive  ──> s3://<bucket>/audit/... (Object Lock)
```

Each destination has its own consumer group and commits offsets only after it has durably
written the records, so a slow or failing archive never holds up Postgres, and Kafka keeps
records until both have them. Set the topic's retention longer than the longest outage you
expect of either destination (seven days is a sensible floor).

## Records

Services publish [CloudEvents](https://cloudevents.io) of type `vrixel.audit.v1` with the
templates' `audit.py`: who (`actor`, and `on_behalf_of` when an agent acts for a person), what
(`action`), which records (`resource.type`, `resource.ids`), the `outcome` and `reason`, the
`trace_id` of the request, and the product's `data_class`. Records hold ids, never field values.

Messages that aren't valid audit events are kept in `audit_rejects` with the reason, and
archived as sent; nothing is dropped.

## Tamper evidence

- Each row's `hash` is SHA-256 over the previous row's hash in the same partition, the row's
  Kafka coordinates and the event, so a changed, missing or reordered row breaks the chain from
  there on. `GET /verify` and `audit-sink verify` recompute every partition's chain.
- Triggers refuse `UPDATE`, `DELETE` and `TRUNCATE` on `audit_events`. Grant the sink's role
  `INSERT, SELECT` only and keep table ownership with a separate role, so disabling the triggers
  takes a different credential.
- Archive files are written with Object Lock (`COMPLIANCE` mode by default) and kept for
  `AUDIT_SINK_ARCHIVE_RETENTION_DAYS` (six years by default, the HIPAA documentation period).
  The bucket must be created with Object Lock enabled.

## Questions it answers

```sql
-- Who accessed patient p-102, and through which service?
SELECT time, actor_id, actor_type, on_behalf_of, service, action, outcome
FROM audit_events WHERE resource_ids @> ARRAY['p-102'] ORDER BY time;

-- Everything one person did last week
SELECT time, action, resource_type, resource_ids, outcome
FROM audit_events WHERE actor_id = 'nurse-7' AND time > now() - interval '7 days' ORDER BY time;

-- Refusals (missing scopes, rate limits, blocked model requests)
SELECT time, service, action, actor_id, event->'data'->>'reason' FROM audit_events WHERE outcome = 'denied';
```

`trace_id` links each record to its trace in OpenChoreo's observability view.

## Run it

```bash
uv sync
uv run audit-sink setup      # create the tables and triggers (run also does this)
uv run audit-sink run        # consume; serves /healthz, /readyz, /metrics, /verify on :8080
uv run audit-sink verify     # exits 1 if any partition's chain is broken
uv run audit-sink restore --topic emr.audit.v1   # rebuild Postgres from the archive
```

| Variable | Default | |
| --- | --- | --- |
| `AUDIT_SINK_KAFKA_BOOTSTRAP` | | Required |
| `AUDIT_SINK_KAFKA_CONFIG` | `{}` | JSON of extra client settings (`security.protocol`, `sasl.*`) |
| `AUDIT_SINK_TOPICS` | `["audit.v1"]` | JSON list of audit topics |
| `AUDIT_SINK_GROUP_PREFIX` | `audit-sink` | Consumer groups are `<prefix>-postgres` and `<prefix>-archive` |
| `AUDIT_SINK_DESTINATIONS` | `["postgres","archive"]` | Run them in one process, or one per deployment |
| `AUDIT_SINK_DATABASE_URL` | | The product's audit database |
| `AUDIT_SINK_ARCHIVE_BUCKET`, `AUDIT_SINK_ARCHIVE_PREFIX` | `audit/` | S3 API bucket (AWS S3, MinIO, Ceph) |
| `AUDIT_SINK_ARCHIVE_ENDPOINT_URL`, `AUDIT_SINK_ARCHIVE_REGION` | | For MinIO and other S3-compatible stores; credentials come from the standard AWS variables or workload identity |
| `AUDIT_SINK_ARCHIVE_LOCK_MODE` | `COMPLIANCE` | `GOVERNANCE`, or empty to use the bucket's default |
| `AUDIT_SINK_ARCHIVE_RETENTION_DAYS` | `2190` | |
| `AUDIT_SINK_ARCHIVE_BATCH_EVENTS`, `AUDIT_SINK_ARCHIVE_BATCH_SECONDS` | `5000`, `300` | A partition's file is written when either is reached |

Metrics: `audit_sink_records_total{destination,result}` (written, rejected, duplicate),
`audit_sink_failures_total{destination}`, `audit_sink_last_success_timestamp_seconds{destination}`.
Alert when a destination hasn't succeeded for longer than its batch interval, and on consumer lag.

## Tests

`uv run pytest` runs the unit tests. Postgres tests need `AUDIT_SINK_TEST_DATABASE_URL`, and the
end-to-end test needs a broker at `AUDIT_SINK_TEST_KAFKA`; CI starts both. Object storage is
simulated with moto.
