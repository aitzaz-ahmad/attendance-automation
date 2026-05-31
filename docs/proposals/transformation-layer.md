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

Milestone 4 shall:

- introduce a TransformationStrategy abstraction
- establish AttendanceEvent as the canonical attendance model
- establish Employee as the canonical employee model
- perform canonicalisation of device-specific data
- perform validation of transformed data
- perform normalisation of transformed data
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
- transformation strategies are injected into biometric devices
- runtime remains device-agnostic
- device-specific optimisation structures are not architectural contracts

## Current Architecture

Current transformation logic is tightly coupled to the ZKTeco implementation.

The current implementation:

- performs enrichment
- performs lookup operations
- constructs backend payloads

using device-specific assumptions.

The transformation rules are not isolated behind a dedicated abstraction.

## Target Architecture

The target architecture after Milestone 4 is:

    BiometricDevice
        ↓
    TransformationStrategy
        ↓
    Employee

    BiometricDevice
        ↓
    TransformationStrategy
        ↓
    AttendanceEvent

Concrete example:

    ZKTecoDevice
        ↓
    ZKTecoTransformationStrategy
        ↓
    Employee

    ZKTecoDevice
        ↓
    ZKTecoTransformationStrategy
        ↓
    AttendanceEvent

Runtime orchestration should consume only project-owned models.

## Dependency Graph

Allowed dependencies:

    BiometricDevice
        ↓
    TransformationStrategy

    TransformationStrategy
        ↓
    Employee

    TransformationStrategy
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

- canonicalisation
- validation
- normalisation
- model construction
- device-specific data interpretation

The transformation layer does not own:

- device communication
- extraction
- device lifecycle management
- runtime orchestration
- persistence

## TransformationStrategy Contract

### Responsibilities

TransformationStrategy is responsible for:

- interpreting device-specific data
- constructing Employee models
- constructing AttendanceEvent models
- validating transformed data
- normalising transformed data

### Allowed Dependencies

- device-specific record types
- device-specific user types
- Employee
- AttendanceEvent

### Forbidden Dependencies

- runtime orchestration
- persistence
- backend APIs
- device communication

### Design Notes

TransformationStrategy represents a behavioural abstraction.

The strategy owns interpretation.

The concrete biometric device owns extraction.

## Canonical Employee Model

Employee becomes the canonical employee representation.

The remainder of the application should depend on Employee rather than device-specific user objects.

Employee construction belongs to the transformation layer.

## Canonical AttendanceEvent Model

AttendanceEvent becomes the canonical attendance representation.

The remainder of the application should depend on AttendanceEvent rather than device-specific attendance record types.

AttendanceEvent construction belongs to the transformation layer.

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

The structure is not considered an architectural contract.

Milestone 4 shall attempt to eliminate this structure.

If efficient correlation still requires a lookup mechanism, it may remain as a private implementation detail inside ZKTecoTransformationStrategy.

The structure shall not become:

- a shared runtime contract
- a workflow dependency
- a public abstraction

## ETLP-29: Create Transformation Module

### Scope

Introduce the TransformationStrategy abstraction.

### Expected Deliverables

- TransformationStrategy interface or abstract base class
- Transformation package structure
- Documentation of transformation responsibilities

### Acceptance Criteria

- Transformation responsibilities are clearly defined
- Runtime remains unaware of device-specific transformation details
- No runtime behaviour changes

## ETLP-30: Implement Canonical Transformer

### Scope

Implement ZKTecoTransformationStrategy.

### Expected Deliverables

- ZKTecoTransformationStrategy
- Employee model construction
- AttendanceEvent model construction

### Acceptance Criteria

- Device-specific records are transformed into domain models
- Runtime receives project-owned models
- Existing behaviour is preserved

## ETLP-31: Add Validation Logic

### Scope

Introduce validation during transformation.

### Expected Deliverables

Validation of:

- employee identifiers
- timestamps
- event types
- required fields

### Acceptance Criteria

- Invalid data is detected consistently
- Validation rules are isolated within the transformation layer

## ETLP-32: Implement Data Normalisation

### Scope

Introduce normalisation during transformation.

### Expected Deliverables

Normalisation of:

- timestamps
- event types
- vendor-specific values

### Acceptance Criteria

- Equivalent data from different vendors produces equivalent domain models
- Normalisation logic is isolated within the transformation layer

## Implementation Phases

### Phase 1

ETLP-29 — Create Transformation Module

### Phase 2

ETLP-30 — Implement Canonical Transformer

### Phase 3

ETLP-31 — Add Validation Logic

### Phase 4

ETLP-32 — Implement Data Normalisation

## Acceptance Criteria

Milestone 4 is considered complete when:

- TransformationStrategy exists as a dedicated abstraction
- Employee is the canonical employee model
- AttendanceEvent is the canonical attendance model
- Validation occurs within the transformation layer
- Normalisation occurs within the transformation layer
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
