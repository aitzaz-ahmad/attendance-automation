# ADR-0003: Device And Transformation Layer Boundaries

## Status

Accepted

## Context

The Attendance Automation ingestion pipeline currently contains device-specific extraction logic, transformation logic, runtime orchestration, and SDK interaction within closely related modules.

ETLP-24 introduced domain models:

- Employee
- AttendanceEvent
- RuntimeState
- ReviewPeriod

The next architectural step is to establish clear boundaries between:

- runtime orchestration
- device access
- data transformation

while preserving the existing runtime behaviour.

## Architectural Layering

### High-Level Policy

- PiRuntime
- IngestionWorkflow

### Boundary Abstractions

- BiometricDeviceConfig
- VendorOptions
- BiometricDevice
- TransformationStrategy

### Domain Models

- Employee
- AttendanceEvent
- RuntimeState
- ReviewPeriod

### Low-Level Implementations

- device SDKs
- device-specific record types
- device-specific user types
- vendor-specific connection logic

Dependencies shall point downward only.

### Layering Diagram

    PiRuntime
        ↓
    IngestionWorkflow
        ↓
    --------------------------------
        ↓
    BiometricDevice        TransformationStrategy
        ↓                          ↓
    ZKTecoDevice           ZKTecoTransformationStrategy
        ↓                          ↓
    ZKTeco SDK             ZKTeco Record Types

    --------------------------------

    AttendanceEvent
    Employee
    RuntimeState
    ReviewPeriod

## Decision 1: Runtime Depends Only On Stable Project-Owned Abstractions

The runtime and workflow layers shall depend only on project-owned abstractions and domain models.

The runtime should not directly depend on:

- device SDKs
- SDK record types
- SDK user types
- vendor-specific connection logic

The runtime should operate on:

- BiometricDeviceConfig
- BiometricDevice
- Employee
- AttendanceEvent
- RuntimeState
- ReviewPeriod

## Decision 2: Low-Level Dependencies Remain Hidden Behind Concrete Implementations

Device SDKs are considered low-level implementation details.

All device-specific imports, connection logic, SDK types, and vendor-specific behaviour shall remain encapsulated within concrete implementations.

Examples:

- ZKTeco SDK imports
- ZKTeco attendance record types
- ZKTeco user types

shall not appear in runtime or workflow modules.

### Rationale

This follows dependency inversion.

High-level policy should not depend on low-level details.

Low-level details should be hidden behind stable project-owned contracts.

## Decision 3: Biometric Device Configuration Keeps Deployment Metadata At The Root

Biometric device configuration shall keep deployment/domain metadata and
vendor-neutral fields at the root level.

`BiometricDeviceConfig` must not expose ZKTeco-specific fields directly.

The root configuration representation shall contain:

- `site_id: str`
- `vendor: str`
- `device_options: VendorOptions`

`site_id` identifies the office, site, or location from which attendance records
are extracted.

`site_id` is deployment/domain metadata. It is independent of the biometric
device vendor and communication mechanism, and it may later be propagated into
canonical attendance records as source-site metadata.

Every `BiometricDevice` instance represents a commissioned biometric terminal
deployed at a known office site. `BiometricDeviceConfig.site_id` is the source of
that runtime identity. Concrete biometric devices shall receive `site_id` during
construction and expose it through the read-only `BiometricDevice.site_id`
property.

Because a commissioned `BiometricDevice` already owns its site identity,
`extract_attendance_records(...)` shall not require `site_id` as a parameter.
Concrete device implementations shall use their own `site_id` when decoding or
emitting existing transitional attendance dictionaries.

`vendor` selects the biometric device implementation.

`device_options` contains vendor-specific connection metadata.

Vendor-specific configuration belongs behind the `VendorOptions` abstraction.

The current ZKTeco-specific configuration belongs in `ZKTecoOptions`.

ZKTeco-specific fields include:

- `ip_address: str`
- `comm_port: int`
- `timeout: Optional[int]`
- `force_udp: Optional[bool]`
- `ommit_ping: Optional[bool]`

These fields must not live in global ingestion-client configuration.

These fields also must not be used to carry root deployment metadata such as
`site_id`.

ZKTeco implementation defaults belong to `ZKTecoDevice`, not to global `config.py`.
When optional ZKTeco options are absent from the device configuration file,
`ZKTecoDevice` shall initialise them using its own implementation-level defaults.

Runtime startup may depend on the project-owned `BiometricDeviceConfig` and
`BiometricDevice` abstractions.

Runtime startup shall not depend on `ZKTecoDevice` directly.

Configuration loading and device construction are separate responsibilities:

- a configuration builder or loader reads JSON, validates it, and constructs the
  correct `VendorOptions` object;
- `BiometricDeviceFactory` consumes `BiometricDeviceConfig` and constructs the
  concrete `BiometricDevice`.

### Rationale

The root configuration contract is part of the runtime boundary.

Keeping deployment metadata such as `site_id` at the root prevents office,
site, or location identity from being coupled to a vendor-specific options
object. Keeping vendor-specific connection details behind `VendorOptions`
prevents ZKTeco details from leaking into runtime orchestration and keeps future
vendor support from requiring new top-level connection fields.

Separating configuration loading from concrete device construction also keeps
validation, file I/O, and object creation in distinct ownership boundaries.

## Decision 4: BiometricDeviceFactory Is The Composition Boundary

### Context

The project intentionally adopts a polymorphic device architecture.

Runtime orchestration should never reference concrete biometric device implementations directly.

### Decision

Concrete device construction shall occur exclusively through BiometricDeviceFactory.

The runtime shall not directly construct concrete biometric devices.

### Rationale

The purpose of the factory is not convenience.

The purpose is to isolate concrete object creation from high-level orchestration code.

Adding a new device should require:

- creating a new BiometricDevice implementation
- creating a matching TransformationStrategy implementation
- extending BiometricDeviceFactory

without modifying runtime orchestration code.

The factory consumes validated `BiometricDeviceConfig` and returns the
`BiometricDevice` abstraction.

It does not read JSON configuration files and it does not own configuration
validation.

The factory is responsible for passing `BiometricDeviceConfig.site_id` into the
concrete commissioned device instance it constructs.

## Decision 5: Device Access And Data Transformation Are Separate Concerns

Device access and data transformation shall remain separate responsibilities.

### Device Access Owns

- connection lifecycle
- extraction
- device communication
- record clearing

### Transformation Owns

- normalisation
- validation
- canonicalisation
- filtering
- construction of canonical AttendanceEvent objects

## Decision 6: Runtime Orchestration Coordinates Device Access And Transformation

### Decision

Runtime orchestration is responsible for coordinating device access and transformation.

The workflow obtains raw data from a `BiometricDevice` and supplies that data to a `TransformationStrategy`.

Example:

    IngestionWorkflow
        -> BiometricDevice
        -> TransformationStrategy

not:

    BiometricDevice
        -> TransformationStrategy

### Rationale

Device access and data transformation are independent concerns.

A biometric device is responsible for extracting raw data.

A transformation strategy is responsible for interpreting and converting that data into canonical attendance events.

Keeping both abstractions as siblings coordinated by runtime orchestration preserves separation of concerns and avoids coupling device implementations to transformation implementations.

## Decision 7: User Mapping Is Removed By The Transformation Layer

### Context

The current implementation maintains:

    user_id -> employee_name

This structure was introduced deliberately.

Attendance records contain employee identifiers.

Backend payloads required employee names.

The lookup structure provided O(1) enrichment and avoided repeated scans of user collections.

### Decision

The lookup structure is no longer required as an architectural or implementation-level contract.

Milestone 4 shall remove the shared user-mapping logic.

Employee correlation shall be owned by transformation normalisation.

Concrete transformation strategies may use local lookup structures internally while normalising raw vendor data, but such structures must remain private implementation details and must not survive beyond the normalisation step.

The runtime and workflow layers shall not maintain or pass around a shared user mapping.

The transformation layer shall produce `NormalisedAttendance` objects that compose `Employee` directly.

### Acceptance Requirement

Milestone 4 is not complete while workflow-level or shared transformation APIs still expose:

- `user_mapping`
- `user_id -> employee_name`
- `Dict[user_id, employee_name]`
- equivalent shared employee-name lookup structures

## Decision 8: Runtime Must Remain Device-Agnostic

Adding a new device should not require changes to:

- PiRuntime
- IngestionWorkflow
- RuntimeState
- AttendanceEvent
- Employee

Only:

- BiometricDevice implementations
- TransformationStrategy implementations
- BiometricDeviceFactory

should require modification.

## Decision 9: Transformation Strategy Template Method

### Decision

The transformation layer shall use a Template Method design.

`TransformationStrategy.transform(...)` defines the complete transformation pipeline.

The pipeline order is:

    normalise
        ↓
    validate
        ↓
    canonicalise
        ↓
    filter

Concrete strategies are responsible for vendor-specific normalisation.

Validation, canonicalisation, and filtering should remain default base-class behaviour unless a concrete strategy has a justified design reason to override them.

### Normalised Models

The transformation layer shall introduce the following vendor-neutral intermediate model:

    Employee
        id: str
        name: str

and:

    NormalisedAttendance
        employee: Employee
        punch: EventType
        timestamp: datetime

`NormalisedAttendance` is an internal transformation-layer model.

It must not cross the transformation boundary.

### Canonical Output

The transformation layer produces canonical `AttendanceEvent` objects.

`AttendanceEvent` shall compose an `Employee` rather than duplicating employee attributes as flattened fields.

`AttendanceEvent` is the only attendance model that crosses the transformation boundary.

Canonical attendance events shall include:

- `site_id`
- `employee`
- `event_type`
- `timestamp`

### Rationale

The Template Method establishes a consistent transformation pipeline while allowing vendor-specific data interpretation to remain isolated within concrete strategies.

Normalisation converts vendor-specific raw employee and attendance streams into a vendor-neutral representation.

Validation operates on a single normalised stream.

Canonicalisation converts validated normalised attendance into project-owned attendance events.

Filtering operates on canonical attendance events rather than vendor SDK record types.

## Consequences

### Positive

- Clear separation of responsibilities.
- Strong dependency inversion.
- Device SDK isolation.
- Easier testing.
- Simpler multi-device support.
- Device extraction and transformation remain independently replaceable.
- Shared user-mapping logic is removed from runtime and workflow boundaries.

### Negative

- Additional abstraction layers.
- Additional constructor wiring.
- BiometricDeviceFactory maintenance when new devices are introduced.

## Rejected Alternatives

| Alternative | Reason Rejected |
|------------|-----------------|
| Runtime depends directly on concrete biometric device implementations | Violates dependency inversion and requires orchestration changes when new devices are introduced. |
| Transformation logic inside runtime | Mixes orchestration concerns with data interpretation and canonicalisation. |
| Transformation logic inside workflow | Couples workflow coordination to device-specific data semantics. |
| Shared user_mapping contract | Superseded by transformation normalisation producing `NormalisedAttendance` with composed `Employee`; retaining it would preserve a legacy lookup as an architectural boundary. |
| BiometricDevice directly constructs its transformation strategy | Introduces tight coupling between device access and transformation implementations and makes testing more difficult. |
| Runtime imports vendor SDKs directly | Leaks low-level implementation details into high-level policy code and increases coupling to vendor-specific dependencies. |
| ZKTeco fields on `BiometricDeviceConfig` | Leaks vendor-specific connection details into the root runtime configuration contract. |
| `site_id` inside `VendorOptions` or `ZKTecoOptions` | Couples deployment/domain metadata to vendor-specific connection configuration. |
| Passing `site_id` into `extract_attendance_records(...)` | Treats commissioned device identity as per-call input even though the device instance already owns that runtime identity. |
| `BiometricDeviceFactory` reads JSON directly | Mixes file loading, validation, and concrete object construction in one boundary. |

## References

- [ADR-0001: Repository Modernisation](0001-repo-modernisation-design.md)
- [ADR-0002: Model Adoption Principles](0002-model-adoption-principles.md)
- [Proposal: Device Layer Abstraction](../proposals/device-layer-abstraction.md)
- [Proposal: Transformation Layer](../proposals/transformation-layer.md)
