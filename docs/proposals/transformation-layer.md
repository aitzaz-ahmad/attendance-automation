# Transformation Layer

## Status

Approved

## Summary

Introduce a transformation layer responsible for converting device-specific data into project-owned domain models.

The objective of this milestone is to establish a canonical representation of attendance data while keeping transformation concerns separate from device access and runtime orchestration.

This milestone implements the transformation boundary defined by ADR-0003.

## Context

Milestone 3 introduces a stable device abstraction layer.

After that work is complete, biometric devices will be responsible only for:

- device communication
- extraction
- connection lifecycle
- attendance record clearing

The extracted data remains device-specific.

Milestone 4 introduces a transformation layer responsible for converting that device-specific data into stable project-owned domain models.

## Goals

- introduce a dedicated transformation layer
- establish AttendanceEvent as the canonical attendance model
- establish Employee as a normalised employee model used during transformation
- perform normalisation of device-specific data
- perform validation of normalised attendance data
- perform canonicalisation into AttendanceEvent objects
- perform filtering of canonical attendance events
- isolate device-specific interpretation logic from runtime orchestration

The milestone should allow multiple device vendors to coexist while producing identical domain models.

## Non-Goals

Milestone 4 does not introduce:

- device communication
- device lifecycle management
- device discovery
- runtime orchestration changes
- backend communication changes
- persistence changes

Those concerns belong to other architectural layers.

## Relationship To ADR-0003

This proposal implements the transformation boundary defined in ADR-0003.

In particular:

- device access and transformation remain separate concerns
- runtime orchestration coordinates biometric devices and transformation strategies
- runtime remains device-agnostic
- device-specific optimisation structures are not architectural contracts

## Current Architecture

Current transformation logic is tightly coupled to the ZKTeco implementation.

The current implementation:

- performs enrichment
- performs lookup operations
- constructs backend payloads
- performs employee correlation
- performs attendance filtering
- performs attendance validation

using device-specific assumptions.

The transformation rules are not isolated behind a dedicated abstraction.

## Target Architecture

The target architecture after Milestone 4 is:

    IngestionWorkflow
            ↓
     ┌─────────────────┐
     │ BiometricDevice │
     └─────────────────┘
            ↓
      raw employees
      raw attendance
            ↓
     ┌──────────────────────┐
     │ Transformation Layer │
     └──────────────────────┘
            ↓
      AttendanceEvent

Concrete example:

    ZKTecoDevice
            ↓
    raw ZKTeco data
            ↓
    Transformation Layer
            ↓
    AttendanceEvent

Runtime orchestration coordinates extraction and transformation.

The workflow obtains raw employee and attendance data from a biometric device and supplies that data to the transformation layer.

Runtime orchestration should consume only project-owned models.

## Dependency Graph

Allowed dependencies:

    Employee
        ↓
    NormalisedAttendance
        ↓
    AttendanceEvent

    Transformation Layer
        ↓
    Employee

    Transformation Layer
        ↓
    NormalisedAttendance

    Transformation Layer
        ↓
    AttendanceEvent

Forbidden dependencies:

    PiRuntime
        ↓
    ZKTeco record types

    IngestionWorkflow
        ↓
    ZKTeco user types

    RuntimeState
        ↓
    Device-specific data

## Transformation Boundary

The transformation layer owns:

- normalisation
- validation
- canonicalisation
- filtering
- construction of canonical AttendanceEvent objects
- device-specific data interpretation
- employee correlation

The transformation layer does not own:

- device communication
- extraction
- device lifecycle management
- runtime orchestration
- persistence

## Transformation Responsibilities

### Responsibilities

The transformation layer is responsible for:

- interpreting device-specific data
- correlating employees and attendance records
- constructing Employee models
- constructing NormalisedAttendance models
- validating normalised attendance
- constructing AttendanceEvent models
- filtering canonical attendance events

### Allowed Dependencies

- device-specific record types
- device-specific user types
- Employee
- NormalisedAttendance
- AttendanceEvent

### Forbidden Dependencies

- runtime orchestration
- persistence
- backend APIs
- device communication

### Design Notes

The transformation layer owns interpretation.

The concrete biometric device owns extraction.

The transformation layer receives raw data and produces canonical attendance events.

## Normalised Employee Model

Employee becomes the normalised employee representation.

The transformation layer should depend on Employee rather than device-specific user objects.

Employee exists to represent vendor-neutral employee identity during transformation.

Employee construction belongs to the transformation layer.

## NormalisedAttendance Model

NormalisedAttendance is an internal transformation-layer model.

It represents vendor-neutral attendance information before canonicalisation.

NormalisedAttendance is the output of the normalisation stage.

NormalisedAttendance is the input to the validation stage.

NormalisedAttendance is the input to canonicalisation.

Expected responsibilities:

- compose an Employee
- represent a normalised punch value
- represent a normalised timestamp

NormalisedAttendance must not cross the transformation boundary.

## Canonical AttendanceEvent Model

AttendanceEvent becomes the canonical attendance representation.

The remainder of the application should depend on AttendanceEvent rather than device-specific attendance record types.

AttendanceEvent construction belongs to the transformation layer.

AttendanceEvent shall include site_id as part of the canonical event context.

AttendanceEvent is the only attendance model that crosses the transformation boundary.

AttendanceEvent shall compose Employee rather than duplicating employee attributes as flattened fields.

## User Mapping Treatment

### Current Implementation

The current implementation maintains:

    user_id -> employee_name

### Purpose

The structure was introduced as a deliberate optimisation.

Attendance records contain employee identifiers.

Backend payloads require employee names.

The lookup structure provided O(1) enrichment and avoided repeated scans of user collections.

### Target State

The shared user mapping is no longer considered an architectural contract.

Milestone 4 shall remove shared runtime and workflow ownership of:

    user_id -> employee_name

Employee correlation becomes an internal responsibility of transformation normalisation.

Concrete transformation strategies may use temporary lookup structures internally.

Such structures remain private implementation details and must not appear in public contracts, workflow APIs, runtime state, or transformation boundaries.

## Phase 1 — Transformation Layer Foundation

Related Issues:

- ETLP-29

### Scope

Introduce the internal transformation-layer foundation.

### Expected Deliverables

- Employee
- EventType
- TransformationRequest
- ExtractedBiometricData
- TimeRange
- NormalisedAttendance
- TransformationStrategy interface or abstract base class
- Transformation package structure
- Documentation of transformation responsibilities

### Acceptance Criteria

- Internal transformation-layer foundation artifacts are defined
- Transformation responsibilities are clearly defined
- Runtime remains unaware of device-specific transformation details
- No runtime behaviour changes

## Phase 2 — Canonical Domain Models

Related Issues:

- ETLP-30

### Scope

Define the canonical attendance model and external payload contracts produced by transformation.

### Expected Deliverables

- AttendanceEvent model construction
- AttendanceEvent serialisation contract
- AttendanceEvent Pub/Sub payload contract

### Acceptance Criteria

- Canonical AttendanceEvent objects are produced
- AttendanceEvent serialisation and Pub/Sub payload contracts are defined
- Existing behaviour is preserved

## Phase 3 — Transformation Pipeline

Related Issues:

- ETLP-31

### Scope

Introduce validation during transformation.

### Expected Deliverables

Validation of:

- employee identifiers
- timestamps
- event types
- required fields

Filtering of canonical attendance events using the stable ingestion watermark
semantics:

- exclusive lower bound for `last_stored_timestamp`
- inclusive upper bound for the effective end time
- `TimeRange.end_time=None` resolved to the current time inside the strategy filter

### Acceptance Criteria

- Invalid data is detected consistently
- Validation rules are isolated within the transformation layer
- Filtering uses `start_time < timestamp <= effective_end_time`
- Records equal to the ingestion watermark are excluded to avoid duplicate processing

## Phase 4 — Vendor Integration

Related Issues:

- ETLP-32

### Scope

Introduce vendor-specific transformation strategy integration.

### Expected Deliverables

Normalisation of:

- timestamps
- event types
- vendor-specific values

### Acceptance Criteria

- Equivalent data from different vendors produces equivalent domain models
- Normalisation logic is isolated within the transformation layer

## Acceptance Criteria

Milestone 4 is considered complete when:

- TransformationStrategy exists as a dedicated abstraction
- Employee exists as a normalised employee model
- NormalisedAttendance exists as an internal transformation model
- AttendanceEvent is the canonical attendance model
- AttendanceEvent includes site_id as canonical event context
- Filtering occurs within the transformation layer
- Shared user_mapping has been removed from runtime and workflow boundaries
- Runtime remains device-agnostic
- Existing runtime behaviour remains unchanged

## Risks

### Premature Canonicalisation

Avoid introducing vendor-specific assumptions into canonical models.

Canonical models should represent project-owned concepts.

### Runtime Leakage

Avoid allowing device-specific types to escape the transformation boundary.

Runtime should consume only project-owned models.

### Over-Preservation Of Legacy Structures

Avoid turning optimisation structures into architectural contracts.

Examples:

- user_mapping
- vendor-specific lookup structures

## Deferred Work

Deferred beyond Milestone 4:

- support for additional device vendors
- cross-device reconciliation
- advanced event correlation
- multi-device runtime orchestration

## References

- [ADR-0001: Repository Modernisation](../decisions/0001-repo-modernisation-design.md)
- [ADR-0002: Model Adoption Principles](../decisions/0002-model-adoption-principles.md)
- [ADR-0003: Device & Transformation Layer Boundaries](../decisions/0003-device-and-transformation-layer-boundaries.md)
- [Proposal: Device Layer Abstraction](device-layer-abstraction.md)
