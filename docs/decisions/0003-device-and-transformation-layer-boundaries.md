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
    BiometricDevice
        ↓
    TransformationStrategy
        ↓
    Domain Models

    --------------------------------

    ZKTecoDevice
        ↓
    ZKTeco SDK

    ZKTecoTransformationStrategy
        ↓
    ZKTeco Record Types

## Decision 1: Runtime Depends Only On Stable Project-Owned Abstractions

The runtime and workflow layers shall depend only on project-owned abstractions and domain models.

The runtime should not directly depend on:

- device SDKs
- SDK record types
- SDK user types
- vendor-specific connection logic

The runtime should operate on:

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

## Decision 3: BiometricDeviceFactory Is The Composition Boundary

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

## Decision 4: Device Access And Data Transformation Are Separate Concerns

Device access and data transformation shall remain separate responsibilities.

### Device Access Owns

- connection lifecycle
- extraction
- device communication
- record clearing

### Transformation Owns

- canonicalisation
- validation
- normalisation
- model construction

## Decision 5: Biometric Devices Depend On Transformation Strategy Abstractions

### Decision

Biometric devices shall depend on TransformationStrategy abstractions rather than concrete transformation implementations.

Transformation strategies shall be injected through constructors.

Example:

    BiometricDevice
        -> TransformationStrategy

not:

    BiometricDevice
        -> ConcreteTransformationStrategy

### Rationale

This preserves loose coupling between device access and data transformation.

The factory remains responsible for wiring concrete implementations together.

## Decision 6: Existing User Mapping Is An Implementation Optimisation

### Context

The current implementation maintains:

    user_id -> employee_name

This structure was introduced deliberately.

Attendance records contain employee identifiers.

Backend payloads require employee names.

The lookup structure provided O(1) enrichment and avoided repeated scans of user collections.

### Decision

The structure is not an architectural contract.

Milestone 4 should attempt to eliminate it.

If efficient correlation still requires a lookup structure, it may remain as a private implementation detail inside a concrete transformation strategy.

It shall not become:

- a shared runtime contract
- a workflow dependency
- a public abstraction

## Decision 7: Runtime Must Remain Device-Agnostic

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

## Consequences

### Positive

- Clear separation of responsibilities.
- Strong dependency inversion.
- Device SDK isolation.
- Easier testing.
- Simpler multi-device support.

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
| Shared user_mapping contract | Treats an implementation optimisation as a public architectural boundary. |
| BiometricDevice directly constructs its transformation strategy | Introduces tight coupling between device access and transformation implementations and makes testing more difficult. |
| Runtime imports vendor SDKs directly | Leaks low-level implementation details into high-level policy code and increases coupling to vendor-specific dependencies. |

## References

- [ADR-0001: Repository Modernisation](0001-repo-modernisation-design.md)
- [ADR-0002: Model Adoption Principles](0002-model-adoption-principles.md)
- [Proposal: Device Layer Abstraction](../proposals/device-layer-abstraction.md)
- [Proposal: Transformation Layer](../proposals/transformation-layer.md)
