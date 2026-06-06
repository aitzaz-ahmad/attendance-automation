# TransformationStrategy Specification

## Status

Draft

## Purpose

Define the technical contract for the transformation layer introduced by Milestone 4.

The transformation layer converts raw biometric-device data into canonical attendance events.

## Related Documents

- docs/decisions/0003-device-and-transformation-layer-boundaries.md
- docs/proposals/transformation-layer.md
- docs/specifications/biometric-device.md
- docs/specifications/zkteco-device.md
- docs/contracts/canonical-attendance-event.md
- docs/diagrams/transformation-strategy.puml

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

Milestone 4 shall discard the existing AttendanceEvent implementation in its entirety, except for the timestamp serialisation contract documented below.

The existing implementation shall not be used as a basis for incremental modification or extension.

A new AttendanceEvent implementation shall be written from the Milestone 4 specification and shall replace the existing implementation completely.

Only the resulting public contract defined by this specification is considered authoritative.

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

If end_time is omitted, filtering should treat the upper bound as the current time.

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

AttendanceEvent is the canonical attendance event model.

AttendanceEvent shall compose Employee rather than duplicating employee attributes as flattened fields.

Expected structure:

    @dataclass(frozen=True)
    class AttendanceEvent:
        site_id: str
        employee: Employee
        event_type: EventType
        timestamp: datetime

AttendanceEvent shall implement `ISerializable`.

AttendanceEvent must provide:

    to_dict() -> Dict[str, Any]

AttendanceEvent shall preserve the existing Pub/Sub serialisation timestamp format:

    ATTENDANCE_TIMESTAMP_FORMAT = "%d-%m-%Y %H:%M:%S"

The timestamp value emitted by `to_dict()` shall be formatted using:

    self.timestamp.strftime(ATTENDANCE_TIMESTAMP_FORMAT)

AttendanceEvent shall serialise to the Pub/Sub payload shape currently expected by the messaging layer:

    {
        "username": self.employee.name,
        "timestamp": self.timestamp.strftime(ATTENDANCE_TIMESTAMP_FORMAT),
        "entry": self.event_type.value,
        "device": self.site_id,
    }


The specification does not require any timestamp deserialisation helper.

AttendanceEvent shall not implement:

- `from_dict(...)`
- legacy transitional payload support
- flattened employee fields such as `employee_id` or `employee_name`
- `source_device_id`
- `raw_event_type`
- `metadata`

AttendanceEvent is the only model produced by the transformation layer for downstream publication.

## EventType

EventType represents canonical attendance event types.

Initial expected values:

    CLOCK_IN
    CLOCK_OUT

Vendor-specific punch values must be normalised into EventType.

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
- AttendanceEvent implements ISerializable
- the existing AttendanceEvent implementation is discarded completely except for the timestamp serialisation contract
- a new AttendanceEvent implementation is written from this specification
- src/attendance_etl/models/attendance_event.py contains only the Milestone 4 AttendanceEvent contract
- AttendanceEvent preserves the Pub/Sub payload shape required by the messaging layer
- no second Employee implementation is introduced
- validation operates on NormalisedAttendance
- canonicalisation produces AttendanceEvent
- filtering operates on AttendanceEvent
- only AttendanceEvent crosses the transformation boundary
- shared user_mapping structures do not cross runtime or workflow boundaries
- runtime orchestration remains device-agnostic
- no SDK connection logic enters the transformation layer
