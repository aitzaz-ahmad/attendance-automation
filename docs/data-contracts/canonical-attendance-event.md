# Canonical Attendance Event

The canonical attendance event is the normalised internal representation of one attendance record.
It is produced after extraction and ingestion, during transformation/canonicalisation, and is intended
for downstream publication, processing, and storage.

This contract describes the target canonical payload shape. Current device-specific decoding may still
emit transitional fields until transformation work adopts this contract end to end.

## Schema

| field | type | required/optional | description | example |
| --- | --- | --- | --- | --- |
| `event_id` | string | required | Canonical event identifier. It should be deterministic when the source guarantees uniqueness; otherwise it is generated during transformation/canonicalisation. | `att-dev-dk-01-10042-2026-05-27T08:59:12Z` |
| `source_device_id` | string | required | Configured or logical identifier for the biometric device that produced the source record. | `att-dev-dk-01` |
| `employee_id` | string | required | Employee or device user identifier from the source device. | `10042` |
| `event_timestamp` | string | required | ISO 8601 timestamp for when the attendance event occurred. | `2026-05-27T08:59:12Z` |
| `event_type` | string | required | Canonical event type. Supported baseline values are `clock_in`, `clock_out`, and `unknown`. | `clock_in` |
| `ingested_at` | string | required | ISO 8601 timestamp for when the source record was ingested. | `2026-05-27T09:00:03Z` |
| `source_record_id` | string | optional | Original source-device record identifier, if the device or extraction layer provides one. | `zk-879221` |
| `employee_name` | string | optional | Enrichment or display name associated with the employee. | `Ayesha Khan` |
| `site_id` | string | optional | Logical office or site identifier associated with the source device. | `dk` |
| `raw_event_type` | string | optional | Original source-device event or status value before canonicalisation. | `Check In` |
| `metadata` | object | optional | Extension object for implementation-specific fields that should not become top-level contract fields yet. | `{"source_format":"zkteco"}` |

## Minimal Payload

```json
{
  "event_id": "att-dev-dk-01-10042-2026-05-27T08:59:12Z",
  "source_device_id": "att-dev-dk-01",
  "employee_id": "10042",
  "event_timestamp": "2026-05-27T08:59:12Z",
  "event_type": "clock_in",
  "ingested_at": "2026-05-27T09:00:03Z"
}
```

## Full Payload

```json
{
  "event_id": "att-dev-dk-01-10042-2026-05-27T08:59:12Z",
  "source_device_id": "att-dev-dk-01",
  "employee_id": "10042",
  "event_timestamp": "2026-05-27T08:59:12Z",
  "event_type": "clock_in",
  "ingested_at": "2026-05-27T09:00:03Z",
  "source_record_id": "zk-879221",
  "employee_name": "Ayesha Khan",
  "site_id": "dk",
  "raw_event_type": "Check In",
  "metadata": {
    "source_format": "zkteco",
    "source_timezone": "Asia/Karachi"
  }
}
```

## Non-Goals

- This document defines the canonical attendance event contract only.
- Runtime validation and enforcement are future work.
- Device-specific raw payload formats are outside this document.
