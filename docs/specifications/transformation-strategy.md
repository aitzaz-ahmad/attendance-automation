# TransformationStrategy Specification

## Status

Accepted

## Lifecycle

Active

The transformation strategy is implemented as the current Milestone 4 baseline.
This specification remains the active authority for transformation behaviour;
the canonical attendance-event data contract remains owned by
[Canonical Attendance Event](../contracts/canonical-attendance-event.md).

## Purpose

Define the technical contract for the transformation layer introduced by Milestone 4.

The transformation layer converts raw biometric-device data into canonical attendance events.
The canonical attendance-event model and backend-compatible payload schema are owned by
[Canonical Attendance Event](../contracts/canonical-attendance-event.md).

## Related Documents

- [ADR-0003: Device And Transformation Layer Boundaries](../decisions/0003-device-and-transformation-layer-boundaries.md)
- [Transformation Layer](../proposals/transformation-layer.md)
- [BiometricDevice specification](biometric-device.md)
- [ZKTecoDevice specification](zkteco-device.md)
- [Canonical Attendance Event](../contracts/canonical-attendance-event.md)
- [Transformation strategy diagram](../diagrams/transformation-strategy.puml)

## Scope

This specification covers:

- transformation strategy ownership
- transformation template method design
- transformation request structure
- normalised attendance representation
- validation of normalised attendance
- canonicalisation into AttendanceEvent
- filtering of canonical attendance events

This specification does not cover:

- biometric device communication
- SDK connection lifecycle
- SDK exception handling during device access
- Pub/Sub publication
- backend payload delivery
- persistence
- runtime orchestration

## Model Placement

The transformation-layer model hierarchy shall be organised as follows:

    src/attendance_etl/models/
        employee.py
        attendance_event.py

    src/attendance_etl/transform/
        normalised_attendance.py
        transformation_request.py

Model ownership:

- Employee -> src/attendance_etl/models/employee.py
- AttendanceEvent -> src/attendance_etl/models/attendance_event.py
- EventType -> src/attendance_etl/models/attendance_event.py
- NormalisedAttendance -> src/attendance_etl/transform/normalised_attendance.py
- TransformationRequest -> src/attendance_etl/transform/transformation_request.py
- ExtractedBiometricData -> src/attendance_etl/transform/transformation_request.py
- TimeRange -> src/attendance_etl/transform/transformation_request.py

The existing implementation in:

    src/attendance_etl/models/employee.py

does not satisfy the Milestone 4 contract.

Milestone 4 shall discard the existing Employee implementation in its entirety.

The existing implementation shall not be used as a basis for incremental modification or extension.

A new Employee implementation shall be written from the Milestone 4 specification and shall replace the existing implementation completely.

Only the resulting public contract defined by this specification is considered authoritative.

The existing implementation in:

    src/attendance_etl/models/attendance_event.py

does not satisfy the Milestone 4 contract.

Milestone 4 shall discard the existing AttendanceEvent implementation in its entirety.

The existing implementation shall not be used as a basis for incremental modification or extension.

A new AttendanceEvent implementation shall satisfy the Milestone 4 transformation behaviour and the canonical
attendance event contract, replacing the existing implementation completely.

This specification is authoritative for transformation behaviour. The `AttendanceEvent` model, current
backend-compatible Pub/Sub payload, timestamp serialisation, event-type serialisation, and deferred schema
evolution notes are defined by
[Canonical Attendance Event](../contracts/canonical-attendance-event.md).

## Terminology

Legacy code currently refers to extracted biometric-device data as:

- users
- records

Within the transformation layer these should be interpreted as:

- raw employees
- raw attendance records

For ZKTeco:

- raw employee type: zk.User
- raw attendance type: zk.Attendance

## Design Rule

The transformation layer owns:

- normalisation
- validation
- canonicalisation
- filtering

Only AttendanceEvent objects cross the transformation boundary.

## Transformation Pipeline

The transformation pipeline follows this order:

    normalise
        ↓
    validate
        ↓
    canonicalise
        ↓
    filter
        ↓
    List[AttendanceEvent]

This order is intentional.

Normalisation converts vendor-specific raw streams into a single vendor-neutral stream.

Validation operates on that single normalised stream.

Canonicalisation converts validated normalised attendance into project-owned attendance events.

Filtering operates on canonical attendance events.

## Template Method

TransformationStrategy.transform(...) is the public template method.

Concrete strategies should not normally override transform(...).

Concrete strategies are expected to implement vendor-specific normalisation.

The base strategy should own the default validation, canonicalisation, and filtering behaviour unless a concrete strategy has a justified design reason to override one of those steps.

Expected shape:

    transform(request: TransformationRequest) -> List[AttendanceEvent]

Expected ownership:

    transform(...)      -> base implementation
    normalise(...)      -> abstract
    validate(...)       -> base implementation
    canonicalise(...)   -> base implementation
    filter(...)         -> base implementation

TransformationRequest groups all transformation inputs into a single contract.

Expected structure:

    @dataclass(frozen=True)
    class TransformationRequest:
        raw_data: ExtractedBiometricData
        site_id: str
        time_range: TimeRange

### ExtractedBiometricData

ExtractedBiometricData represents data extracted from a biometric device before transformation begins.

Expected structure:

    @dataclass(frozen=True)
    class ExtractedBiometricData:
        employees: Sequence[Any]
        attendance_records: Sequence[Any]

The class name communicates provenance.

Within TransformationRequest the same object is referenced as raw_data to communicate its role as transformation input.

### TimeRange

TimeRange defines the filtering boundary applied during transformation.

Expected structure:

    @dataclass(frozen=True)
    class TimeRange:
        start_time: datetime
        end_time: Optional[datetime] = None

`start_time` represents the ingestion watermark / last stored timestamp.
Records equal to this watermark have already been stored and must be excluded
to avoid duplicate processing.

If end_time is omitted, filtering should treat the upper bound as the current
time. The current time is resolved inside `TransformationStrategy.filter(...)`;
TimeRange remains a passive data structure.

## NormalisedAttendance

NormalisedAttendance is an internal transformation-layer model.

It represents vendor-neutral attendance data before canonicalisation.

NormalisedAttendance is the output of the normalisation stage.

NormalisedAttendance is the input to the validation stage.

NormalisedAttendance is the input to canonicalisation.

Expected structure:

    @dataclass(frozen=True)
    class NormalisedAttendance:
        employee: Employee
        punch: EventType
        timestamp: datetime

NormalisedAttendance must not cross the transformation boundary.

### Employee

Employee represents normalised employee identity.

Employee is a normalised transformation-layer model.

Employee must not be treated as a canonical attendance event.

Employee is a project-owned domain model.

The transformation layer constructs and consumes Employee instances but does not own the Employee abstraction.

Expected structure:

    @dataclass(frozen=True)
    class Employee:
        id: str
        name: str

Employee shall implement `ISerializable`.

Employee must provide:

    to_dict() -> Dict[str, Any]

Employee shall not implement:

- `from_dict(...)`
- vendor-specific factory methods
- ZKTeco-specific construction helpers

The field names intentionally avoid redundant prefixes such as employee_id and employee_name.

Preferred call-sites:

    employee.id
    employee.name
    attendance.employee.id
    attendance.employee.name

## AttendanceEvent

AttendanceEvent is the canonical attendance event model produced by transformation.

The transformation layer constructs AttendanceEvent objects during canonicalisation. The active
`AttendanceEvent` model contract, current backend-compatible Pub/Sub payload, serialisation details, and
deferred schema evolution notes are owned by
[Canonical Attendance Event](../contracts/canonical-attendance-event.md).

AttendanceEvent is the only model produced by the transformation layer for downstream publication.

## EventType

EventType represents canonical attendance event types.

Vendor-specific punch values must be normalised into EventType.
The active EventType wire serialisation contract is owned by
[Canonical Attendance Event](../contracts/canonical-attendance-event.md).

## Normalisation

Normalisation receives:

    ExtractedBiometricData

For ZKTeco this contains:

    Sequence[zk.User]
    Sequence[zk.Attendance]

Normalisation produces:

    List[NormalisedAttendance]

Normalisation owns vendor-specific interpretation, including:

- mapping raw employees to Employee
- correlating raw attendance records with employees
- interpreting vendor punch values
- producing vendor-neutral attendance data

## Validation

Validation receives:

    Sequence[NormalisedAttendance]

Validation checks that the normalised attendance stream satisfies the transformation-layer contract before canonicalisation.

Validation must verify:

- employee.id is present and is a string
- employee.name is present and is a string
- punch is a valid EventType
- timestamp is present and is a datetime

Validation should not perform device communication or SDK-specific error handling.

## Canonicalisation

Canonicalisation receives:

    Sequence[NormalisedAttendance]

Canonicalisation is responsible for introducing site context into the canonical attendance model.

site_id does not originate from vendor attendance records and must be supplied from runtime or device context.

Canonicalisation converts normalised attendance into canonical attendance events.

## Filtering

Filtering receives:

    Sequence[AttendanceEvent]

Filtering applies date-time boundaries to canonical attendance events.

Filtering uses the TimeRange supplied in TransformationRequest.

Filtering uses an exclusive lower bound and inclusive upper bound:

    time_range.start_time < event.timestamp <= effective_end_time

`effective_end_time` is `time_range.end_time` when provided. When
`time_range.end_time` is omitted, `TransformationStrategy.filter(...)` resolves
`effective_end_time` with `datetime.now()`.

Filtering does not operate on raw SDK records.

## ZKTecoTransformationStrategy

ZKTecoTransformationStrategy is the concrete transformation strategy for ZKTeco raw data.

It owns ZKTeco-specific normalisation.

It may depend on:

- ZKTeco raw user shape
- ZKTeco raw attendance record shape
- Employee
- NormalisedAttendance
- EventType

It must not depend on:

- ZKTeco device connection logic
- Pub/Sub messaging
- runtime state
- persistence helpers
- backend delivery code

## Device Exception Boundary

ZKTeco SDK device-access exceptions belong to the biometric device layer.

The transformation layer should not handle SDK connection or device-operation failures.

If malformed raw data is received after extraction succeeds, that is a transformation concern.

## Acceptance Checks

The transformation boundary is complete when:

- TransformationStrategy defines transform(...) as a template method
- TransformationStrategy consumes TransformationRequest
- concrete strategies provide vendor-specific normalisation
- normalised attendance is represented by NormalisedAttendance
- the existing Employee implementation is discarded completely
- a new Employee implementation is written from this specification
- src/attendance_etl/models/employee.py contains only the Milestone 4 Employee contract
- Employee exists only in src/attendance_etl/models/employee.py
- Employee implements ISerializable
- AttendanceEvent construction conforms to the canonical attendance event contract
- the existing AttendanceEvent implementation is discarded completely
- src/attendance_etl/models/attendance_event.py remains aligned with the canonical attendance event contract
- no second Employee implementation is introduced
- validation operates on NormalisedAttendance
- canonicalisation produces AttendanceEvent
- filtering operates on AttendanceEvent
- only AttendanceEvent crosses the transformation boundary
- shared user_mapping structures do not cross runtime or workflow boundaries
- runtime orchestration remains device-agnostic
- no SDK connection logic enters the transformation layer
