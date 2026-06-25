# BiometricDevice Specification

## Status

Accepted

## Lifecycle

Active

## Purpose

Define the technical contract for the `BiometricDevice` abstraction.

This specification exists to keep the implementation grounded in current runtime needs while aligning with
[ADR-0003](../decisions/0003-device-and-transformation-layer-boundaries.md) and the
[Milestone 3 device-layer proposal](../proposals/device-layer-abstraction.md).

## Related Documents

- [ADR-0003: Device And Transformation Layer Boundaries](../decisions/0003-device-and-transformation-layer-boundaries.md)
- [Device Layer Abstraction](../proposals/device-layer-abstraction.md)
- [Transformation Layer](../proposals/transformation-layer.md)
- [Biometric Device Configuration specification](biometric-device-configuration.md)
- [BiometricDeviceFactory specification](device-factory.md)

## Terminology

The project uses the term:

    BiometricDevice

for the runtime-facing abstraction. A `BiometricDevice` represents a
commissioned biometric terminal deployed at a known office site.

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
- commissioned device runtime identity
- biometric device access responsibilities
- dependency rules for biometric devices
- audit requirements before finalising the interface
- testing expectations

This specification does not cover:

- concrete ZKTeco implementation
- biometric device configuration shape or startup validation
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

Every `BiometricDevice` instance owns read-only `site_id` runtime identity. The
value originates from `BiometricDeviceConfig.site_id` and is provided during
construction by the composition boundary. Configuration shape and validation are
defined in the
[Biometric Device Configuration specification](biometric-device-configuration.md).

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

- `site_id`
- `extract_attendance_records(from_date, to_date=None)`
- `pull_records()`
- `clear_records()`

Additional methods require justification through demonstrated runtime usage.

`extract_attendance_records(...)` must not accept `site_id` as a parameter. The
device already owns the site identity for the commissioned terminal.

## Raw Data Boundary

Milestone 3 may still expose raw device data through the biometric device abstraction.

Transformation into `Employee` and `AttendanceEvent` belongs to Milestone 4.

Do not force canonical model construction into Milestone 3.

## Testing Expectations

Tests should verify:

- the abstraction exists
- the abstraction can be imported without vendor SDK dependencies
- the abstraction exposes read-only `site_id`
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
- `site_id` is read-only runtime identity provided at construction time
- each method is justified by current usage
- the abstraction is free of vendor SDK imports
- transformation concerns are excluded
- existing runtime behaviour remains unchanged
