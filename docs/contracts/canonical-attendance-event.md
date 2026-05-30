# Canonical Attendance Event

The canonical attendance event is the normalised internal representation of one attendance event reported by
a source biometric device. It is produced after extraction and source-specific normalisation, during
canonicalisation, and is intended for downstream publication, processing, and persistence.

This contract describes the target canonical payload shape. Current device-specific decoding and publishing
may still emit transitional fields until transformation work adopts this contract end to end.

`attendance_etl.models.AttendanceEvent` is the Python dataclass representation of this contract for internal
code. Its conversion helpers are available for model/dictionary conversion, but current runtime publishing may
continue to use transitional dictionaries until canonical adoption is completed separately.

For pipeline stage sequencing, see [Data Pipeline](../data-pipeline.md). For runtime adoption limits, see
[Architecture](../architecture.md).

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

## Event Semantics

One canonical attendance event represents one attendance interaction extracted from a source biometric
device after normalisation/canonicalisation. It does not represent a review period, an employee schedule, an aggregate
attendance day, or a storage update result.

Canonical events are intended to be the stable internal payload passed to publication, backend processing,
and persistence boundaries. Runtime adoption is still transitional: current ingestion and storage paths may
publish or consume legacy dictionaries until the transformation layer fully emits this contract.

## Field Semantics

### Event Identity

`event_id` identifies one canonical attendance event and should be stable and deterministic where possible. If
the source provides a stable record identifier, that value may contribute to the canonical identity through
`source_record_id` or the event ID derivation. If the source does not provide a stable record identifier, the
canonicalisation layer may derive identity from stable source facts such as `source_device_id`, `employee_id`,
`event_timestamp`, and source metadata.

`event_id` must not depend on transient processing time alone. `ingested_at` can help with observability and
ordering, but it is not sufficient as the only identity input.

### Timestamps

`event_timestamp` is the time the attendance punch or source event occurred. `ingested_at` is the time the
source record entered the pipeline. These fields have different meanings and must not be conflated.

### Event Types

`event_type` is the canonical event type used by baseline consumers. The accepted initial values are
conservative: `clock_in`, `clock_out`, and `unknown`.

`raw_event_type` preserves the source-specific status, label, or code before canonicalisation when that value
is available. Future canonical `event_type` values broaden the contract and should be introduced through the
schema evolution guidance below.

## Schema Evolution

- Additive optional fields are the preferred backward-compatible evolution path.
- New required fields are breaking changes because baseline consumers may not provide or understand them.
- Renaming or removing fields is breaking and requires explicit migration planning.
- Broadening canonical enum-like values, including `event_type`, must be documented so consumers can decide
  whether to reject, ignore, or handle the new value.
- Consumers should tolerate unknown optional fields where reasonable.
- `metadata` may carry implementation-specific extensions, but required business semantics should graduate to
  documented top-level fields instead of remaining hidden inside `metadata`.

## Required And Optional Fields

Required fields define the minimum stable contract for a canonical attendance event. Optional fields enrich
the event for display, debugging, source traceability, or implementation-specific use, but baseline consumers
must not require optional fields to process a valid minimal payload.

Promoting an optional field to required is a breaking change. Deprecations should be documented before
removal, with enough migration guidance for producers and consumers to move away from the deprecated field.

## Versioning

Versioning is currently documentation-level only. This contract does not define a runtime `schema_version`
field.

A future runtime `schema_version` may be introduced if multiple incompatible canonical event shapes need to
coexist. Until then, changes to this contract are governed by repository review and the repository changelog or
commit history.

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
