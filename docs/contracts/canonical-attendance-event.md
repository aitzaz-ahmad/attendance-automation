# Canonical Attendance Event

## Status

Accepted

## Lifecycle

Active

The canonical attendance event is the normalised internal representation of one
attendance event reported by a source biometric device. It is produced after
extraction and source-specific normalisation, during transformation and normalisation, and is
intended for downstream publication, processing, and persistence.

This contract distinguishes the current ETLP-30 internal model from the current
backend-compatible Pub/Sub payload. ETLP-30 does not introduce a richer canonical
wire schema.

For pipeline stage sequencing, see [Data Pipeline](../data-pipeline.md). For
runtime adoption limits, see [Architecture](../architecture.md).

## Current ETLP-30 Internal Model Contract

`attendance_etl.models.AttendanceEvent` is the Python dataclass representation
of the canonical internal attendance event model.

Expected structure:

```python
@dataclass(frozen=True)
class AttendanceEvent:
    site_id: str
    employee: Employee
    event_type: EventType
    timestamp: datetime
```

Current model fields:

| field | type | required/optional | description | example |
| --- | --- | --- | --- | --- |
| `site_id` | string | required | Logical office or site identifier associated with the source device. | `dk` |
| `employee` | Employee | required | Normalised employee identity composed into the attendance event. | `Employee(id="10042", name="Ayesha Khan")` |
| `event_type` | EventType | required | Attendance event type used for the current backend-compatible payload. | `EventType.CLOCK_IN` |
| `timestamp` | datetime | required | Timestamp for when the attendance event occurred. | `datetime(2026, 5, 27, 8, 59, 12)` |

`AttendanceEvent` composes `Employee` rather than duplicating employee attributes
as flattened fields.

## Current Backend-Compatible Pub/Sub Payload

`AttendanceEvent.to_dict()` intentionally serialises to the Pub/Sub payload
shape expected by the current backend:

```json
{
  "username": "Ayesha Khan",
  "timestamp": "27-05-2026 08:59:12",
  "entry": "Check In",
  "device": "dk"
}
```

The serialisation contract is:

```python
{
    "username": self.employee.name,
    "timestamp": self.timestamp.strftime(ATTENDANCE_TIMESTAMP_FORMAT),
    "entry": self.event_type.value,
    "device": self.site_id,
}
```

Timestamp serialisation currently uses:

```python
ATTENDANCE_TIMESTAMP_FORMAT = "%d-%m-%Y %H:%M:%S"
```

Current `EventType` values are intentionally backend-compatible:

```python
EventType.CLOCK_IN.value == "Check In"
EventType.CLOCK_OUT.value == "Check Out"
```

The current wire payload remains transitional/backend-compatible. It must not be
treated as the final canonical wire schema.

## Event Semantics

One canonical attendance event represents one attendance interaction extracted
from a source biometric device after normalisation/canonicalisation. It does not
represent a review period, an employee schedule, an aggregate attendance day, or
a storage update result.

The ETLP-30 internal model is canonical for transformation-layer output. The
current Pub/Sub payload remains shaped for backend compatibility until a
dedicated contract migration introduces a richer canonical wire payload.

## Deferred Canonical Payload Evolution

The following fields and semantics are deferred contract evolution items. They
are not current ETLP-30 `AttendanceEvent` fields and are not emitted by the
current ETLP-30 Pub/Sub payload.

### event_id

`event_id` is a future unique canonical event identifier.

Current architectural preference is generation at the messaging/publication boundary, while allowing future design work to reassign ownership if required.

### ingested_at

`ingested_at` is a future publication or ingestion timestamp.

Prefer generation at the messaging/publication boundary.

### event_timestamp

`event_timestamp` is the future canonical wire-payload name for the attendance
event timestamp.

The current ETLP-30 payload keeps `"timestamp"` for backend compatibility.

### source_device_id

`source_device_id` is a future canonical `AttendanceEvent` provenance field.

It should be introduced once biometric device identity is available to the
transformation layer.

source_device_id provides event provenance and enables traceability back to the originating biometric device.

### Timezone-Aware Timestamp Semantics

Future canonical timestamps should preserve timezone information.

Current ETLP-30 serialisation uses `ATTENDANCE_TIMESTAMP_FORMAT` for backend
compatibility and does not preserve timezone information in the wire payload.

### EventType Wire Values

Current values remain:

```python
"Check In"
"Check Out"
```

Future canonical wire values should migrate toward:

```python
"Clock In"
"Clock Out"
```

Any migration of EventType wire values must be introduced through a dedicated
contract migration issue.

## Schema Evolution

- The current ETLP-30 model fields are `site_id`, `employee`, `event_type`, and
  `timestamp`.
- The current ETLP-30 Pub/Sub payload fields are `username`, `timestamp`,
  `entry`, and `device`.
- Deferred canonical wire-payload fields must not be treated as currently
  implemented fields.
- Adding fields to `AttendanceEvent` is a contract change and should be
  introduced through a scoped issue.
- Renaming current payload keys is a contract migration and must include backend
  compatibility planning.
- Broadening or changing `EventType` wire values must be documented so consumers
  can decide whether to reject, ignore, or handle the new value.

## Versioning

Versioning is currently documentation-level only. This contract does not define
a runtime `schema_version` field.

A future runtime `schema_version` may be introduced if multiple incompatible
canonical event shapes need to coexist. Until then, changes to this contract are
governed by repository review and the repository changelog or commit history.

## Non-Goals

- This document defines the canonical attendance event contract only.
- ETLP-30 does not introduce a richer canonical wire schema.
- Runtime validation and enforcement are future work.
- Device-specific raw payload formats are outside this document.
