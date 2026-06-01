# BiometricDevice Specification

## Status

Draft

## Purpose

Define the technical contract for the `BiometricDevice` abstraction.

This specification exists to keep the implementation grounded in current runtime needs while aligning with ADR-0003 and the Milestone 3 device-layer proposal.

## Related Documents

- `docs/decisions/0003-device-and-transformation-layer-boundaries.md`
- `docs/proposals/device-layer-abstraction.md`
- `docs/proposals/transformation-layer.md`
- `docs/specifications/device-factory.md`

## Terminology

The project uses the term:

    BiometricDevice

for the runtime-facing abstraction.

Concrete implementations represent vendor-specific biometric devices.

Examples:

    BiometricDevice
        ↑
        |
    ZKTecoDevice

Future implementations may include:

    SupremaDevice
    HikvisionDevice
    AnvizDevice

The abstraction belongs in:

    devices/biometric_device.py

Concrete biometric device implementations live alongside the abstraction in the same package.

## Scope

This specification covers:

- the runtime-facing biometric device abstraction
- biometric device access responsibilities
- dependency rules for biometric devices
- audit requirements before finalising the interface
- testing expectations

This specification does not cover:

- concrete ZKTeco implementation
- biometric device factory implementation
- transformation strategy implementation
- canonicalisation
- validation
- normalisation
- backend payload construction

## Design Rule

The `BiometricDevice` API must be derived from audited current usage.

Do not invent methods for anticipated future biometric devices.

Each method on the abstraction must be traceable to current runtime, workflow, or device-access requirements.

## Current Usage Audit

Before finalising the interface, inspect the existing implementation and identify:

- which code currently extracts users
- which code currently extracts attendance records
- which code currently clears attendance records
- which higher-level modules currently depend on concrete biometric device behaviour

The audit result should determine the minimal public interface.

## Responsibilities

`BiometricDevice` owns the runtime-facing biometric device access contract.

Only operations required by current runtime behaviour should appear on the abstraction.

## Non-Responsibilities

`BiometricDevice` does not own:

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

- vendor SDK modules
- concrete biometric devices
- SDK-specific user types
- SDK-specific attendance record types
- backend APIs
- messaging clients
- persistence helpers

Concrete biometric devices may import vendor SDKs.

Runtime and workflow code should depend on the abstraction rather than concrete biometric devices.

## Interface Shape

The interface should remain minimal.

The current audited interface is:

- `pull_records()`
- `clear_records()`

Additional methods require justification through demonstrated runtime usage.

## Raw Data Boundary

Milestone 3 may still expose raw device data through the biometric device abstraction.

Transformation into `Employee` and `AttendanceEvent` belongs to Milestone 4.

Do not force canonical model construction into Milestone 3.

## Testing Expectations

Tests should verify:

- the abstraction exists
- the abstraction can be imported without vendor SDK dependencies
- runtime-facing code can type against the abstraction
- no concrete biometric device implementation is required to import the abstraction
- the abstraction does not introduce transformation responsibilities

Avoid tests that require live biometric hardware.

## Migration Notes

ETLP-25 introduced the runtime-facing abstraction using the name:

    DeviceClient

ETLP-27 standardises the project vocabulary by renaming the abstraction to:

    BiometricDevice

ETLP-27 also renames the abstraction module from:

    devices/device_client.py

to:

    devices/biometric_device.py

This is a forward correction intended to align the abstraction with its actual responsibility.

The terminology change must be applied consistently across:

- source code
- tests
- documentation
- module names
- import paths

## Acceptance Checks

The abstraction is complete when:

- `BiometricDevice` exists
- the interface is minimal
- each method is justified by current usage
- the abstraction is free of vendor SDK imports
- transformation concerns are excluded
- existing runtime behaviour remains unchanged
