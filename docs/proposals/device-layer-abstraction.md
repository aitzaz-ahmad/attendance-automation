# Device Layer Abstraction

## Status

Approved

## Summary

Introduce a device abstraction layer that hides vendor-specific SDKs behind stable project-owned interfaces.

The objective of this milestone is to isolate device access from runtime orchestration while preserving existing runtime behaviour.

This milestone establishes the device boundary described in ADR-0003.

## Context

The current implementation contains device-specific extraction logic that is tightly coupled to the ZKTeco SDK.

As a result:

- device-specific imports leak into higher-level modules
- orchestration code is aware of device-specific implementation details
- introducing a new device vendor would require changes outside the device layer

Milestone 3 introduces a stable abstraction boundary that allows runtime orchestration to remain independent of concrete biometric device implementations.

## Goals

Milestone 3 shall:

- establish a BiometricDevice abstraction
- encapsulate all device SDK interaction
- remove concrete biometric device references from runtime code
- introduce a composition boundary through BiometricDeviceFactory
- move extraction responsibilities behind the device boundary

The milestone should enable future support for multiple biometric device vendors without requiring orchestration changes.

## Non-Goals

Milestone 3 does not introduce:

- canonicalisation
- validation
- data normalisation
- AttendanceEvent construction rules
- Employee construction rules
- transformation strategies
- multi-device runtime support

Those concerns belong to Milestone 4.

## Relationship To ADR-0003

This proposal implements the architectural decisions documented in ADR-0003.

In particular:

- runtime depends on abstractions rather than concrete biometric devices
- device SDKs remain hidden behind concrete implementations
- BiometricDeviceFactory acts as the composition boundary
- device access and transformation remain separate concerns

## Current Architecture

Current dependencies resemble:

    PiRuntime
        ↓
    IngestionWorkflow
        ↓
    ZKTeco SDK

Device-specific knowledge currently exists outside a dedicated device boundary.

## Target Architecture

The target architecture after Milestone 3 is:

    PiRuntime
        ↓
    IngestionWorkflow
        ↓
    BiometricDevice

    BiometricDeviceFactory
        ↓
    ZKTecoDevice
        ↓
    ZKTeco SDK

Runtime orchestration should depend only on BiometricDevice.

Concrete device construction should occur exclusively through BiometricDeviceFactory.

## Dependency Graph

Allowed dependencies:

    PiRuntime
        ↓
    IngestionWorkflow
        ↓
    BiometricDevice

    BiometricDeviceFactory
        ↓
    ZKTecoDevice

    ZKTecoDevice
        ↓
    ZKTeco SDK

Forbidden dependencies:

    PiRuntime
        ↓
    ZKTeco SDK

    IngestionWorkflow
        ↓
    ZKTeco SDK

## Device Boundary

The device layer owns:

- device connection lifecycle
- extraction
- device communication
- attendance record clearing

The device layer does not own:

- canonicalisation
- validation
- normalisation
- runtime orchestration

The device layer returns raw device data.

Interpretation of device data belongs to Milestone 4.

## BiometricDevice Contract

### Responsibilities

BiometricDevice is responsible for:

- connecting to devices
- disconnecting from devices
- extracting users
- extracting attendance records
- clearing attendance records

### Allowed Dependencies

- device SDKs
- device-specific record types
- device-specific user types

### Forbidden Dependencies

- runtime state
- workflow logic
- backend APIs
- attendance normalisation logic
- validation logic

### Design Notes

The interface should remain focused on device access.

It should not own transformation responsibilities.

Exact method signatures are intentionally left flexible and should be derived from the current SDK integration during ETLP-25.

## BiometricDeviceFactory Contract

BiometricDeviceFactory acts as the composition boundary.

### Responsibilities

- create BiometricDevice implementations
- wire required dependencies
- hide concrete biometric device construction from runtime code

### Non-Responsibilities

- runtime orchestration
- SDK communication
- transformation
- validation
- normalisation

### Design Notes

BiometricDeviceFactory exists to support a polymorphic architecture.

Its purpose is not convenience.

Its purpose is to prevent high-level code from depending on concrete implementations.

The factory should remain intentionally simple.

The milestone shall not introduce:

- plugin systems
- registries
- dependency injection containers
- abstract factories

## ZKTecoDevice Responsibilities

ZKTecoDevice owns:

- ZKTeco SDK imports
- device connection lifecycle
- user extraction
- attendance extraction
- attendance record clearing

ZKTecoDevice must not own:

- canonicalisation
- validation
- normalisation
- backend payload construction

## ETLP-25: Create Device Interface

### Scope

Introduce the BiometricDevice abstraction.

### Expected Deliverables

- BiometricDevice interface or abstract base class
- Documentation of responsibilities and boundaries
- Runtime dependencies updated to target BiometricDevice

### Acceptance Criteria

- Runtime depends on BiometricDevice rather than concrete biometric device implementations
- Device responsibilities are clearly defined
- No runtime behaviour changes

## ETLP-26: Implement ZKTeco Concrete Biometric Device

### Scope

Create ZKTecoDevice as the concrete implementation of BiometricDevice.

### Expected Deliverables

- ZKTecoDevice
- Encapsulation of ZKTeco SDK imports
- Encapsulation of ZKTeco extraction logic

### Acceptance Criteria

- ZKTeco SDK interaction occurs only within ZKTecoDevice
- Existing extraction behaviour is preserved
- Runtime behaviour remains unchanged

## ETLP-27: Implement Device Factory

### Scope

Introduce BiometricDeviceFactory as the composition boundary.

### Expected Deliverables

- BiometricDeviceFactory
- Centralised construction of BiometricDevice implementations

### Acceptance Criteria

- Runtime does not instantiate concrete biometric devices directly
- BiometricDeviceFactory constructs the correct concrete implementation
- No runtime behaviour changes

## ETLP-28: Move Extraction Logic

### Scope

Move extraction responsibilities behind the device boundary.

### Expected Deliverables

- Device extraction logic moved into ZKTecoDevice
- Runtime orchestration simplified

### Acceptance Criteria

- Runtime and workflow no longer contain device-specific extraction logic
- Extraction behaviour remains unchanged
- Existing tests continue to pass

## Implementation Phases

### Phase 1

ETLP-25 — Create Device Interface

### Phase 2

ETLP-26 — Implement Concrete ZKTeco Biometric Device

### Phase 3

ETLP-27 — Implement Biometric Device Factory

### Phase 4

ETLP-28 — Move Extraction Logic

## Acceptance Criteria

Milestone 3 is considered complete when:

- BiometricDevice exists as the runtime-facing abstraction
- ZKTeco SDK interaction is isolated inside ZKTecoDevice
- Runtime orchestration contains no direct device SDK dependencies
- BiometricDeviceFactory acts as the sole composition boundary
- Extraction logic resides within the device layer
- Existing runtime behaviour remains unchanged

## Risks

### Over-Abstraction

Avoid introducing:

- plugin systems
- registries
- dependency injection containers
- abstract factories

The factory should remain minimal.

### Transformation Leakage

Avoid introducing:

- canonicalisation
- validation
- normalisation

Those concerns belong to Milestone 4.

## Deferred Work

Deferred to Milestone 4:

- TransformationStrategy
- AttendanceEvent construction
- Employee construction
- validation
- normalisation
- user_mapping removal evaluation

## References

- [ADR-0001: Repository Modernisation](../decisions/0001-repo-modernisation-design.md)
- [ADR-0002: Model Adoption Principles](../decisions/0002-model-adoption-principles.md)
- [ADR-0003: Device & Transformation Layer Boundaries](../decisions/0003-device-and-transformation-layer-boundaries.md)
- [Proposal: Transformation Layer](transformation-layer.md)
