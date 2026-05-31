# DeviceClient Specification

## Status

Draft

## Purpose

Define the technical contract for the `DeviceClient` abstraction introduced by ETLP-25.

This specification exists to keep the implementation grounded in current runtime needs while aligning with ADR-0003 and the Milestone 3 device-layer proposal.

## Related Documents

- `docs/decisions/0003-device-and-transformation-layer-boundaries.md`
- `docs/proposals/device-layer-abstraction.md`
- `docs/proposals/transformation-layer.md`

## Scope

This specification covers:

- the runtime-facing device abstraction
- device access responsibilities
- dependency rules for device clients
- audit requirements before finalising the interface
- testing expectations for ETLP-25

This specification does not cover:

- concrete ZKTeco implementation
- device factory implementation
- transformation strategy implementation
- canonicalisation
- validation
- normalisation
- backend payload construction

## Design Rule

The `DeviceClient` API must be derived from audited current usage.

Do not invent methods for anticipated future devices.

Each method on the abstraction must be traceable to current runtime, workflow, or device-access requirements.

## Current Usage Audit

Before finalising the interface, inspect the existing implementation and identify:

- which code currently connects to the biometric device
- which code currently disconnects from the biometric device
- which code currently extracts users
- which code currently extracts attendance records
- which code currently clears attendance records
- which higher-level modules currently depend on concrete device behaviour

The audit result should determine the minimal public interface.

## Responsibilities

`DeviceClient` owns the runtime-facing device access contract.

It may represent operations such as:

- establishing device access
- releasing device access
- extracting device users
- extracting attendance records
- clearing attendance records

Only include operations that are required by current behaviour.

## Non-Responsibilities

`DeviceClient` does not own:

- canonicalisation
- validation
- normalisation
- `AttendanceEvent` construction
- `Employee` construction
- backend payload construction
- runtime state persistence
- review-period persistence
- messaging
- workflow transition logic

Those concerns belong to other layers.

## Dependency Rules

The base abstraction must not import:

- ZKTeco SDK modules
- concrete device clients
- SDK-specific user types
- SDK-specific attendance record types
- backend APIs
- messaging clients
- persistence helpers

Concrete device clients may import vendor SDKs.

Runtime and workflow code should depend on the abstraction rather than concrete device clients.

## Interface Shape

The exact method signatures must be determined during ETLP-25 after auditing current usage.

The interface may be implemented as either:

- an abstract base class
- a protocol

The chosen form should be simple, Python 3.8-compatible, and easy to test.

The interface should remain minimal.

## Raw Data Boundary

Milestone 3 may still expose raw device data through the device abstraction.

Transformation into `Employee` and `AttendanceEvent` belongs to Milestone 4.

Do not force canonical model construction into ETLP-25.

## Testing Expectations

ETLP-25 tests should verify:

- the abstraction exists
- the abstraction can be imported without vendor SDK dependencies
- runtime-facing code can type against the abstraction
- no concrete device implementation is required to import the abstraction
- the abstraction does not introduce transformation responsibilities

Avoid tests that require live biometric hardware.

## Migration Notes

ETLP-25 should define the abstraction only.

Later Milestone 3 issues will:

- implement the ZKTeco concrete client
- introduce factory-based construction
- move extraction logic behind the device boundary

## Acceptance Checks

ETLP-25 is complete when:

- `DeviceClient` exists
- the interface is minimal
- each method is justified by current usage
- the abstraction is free of vendor SDK imports
- transformation concerns are excluded
- existing runtime behaviour remains unchanged
