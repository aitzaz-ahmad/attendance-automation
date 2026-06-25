# ZKTecoDevice Specification

## Status

Accepted

## Lifecycle

Active

## Purpose

Define the technical contract for the concrete ZKTeco implementation of BiometricDevice.

This specification exists to ensure that ZKTeco-specific SDK interaction remains isolated behind the device
abstraction boundary established by
[ADR-0003](../decisions/0003-device-and-transformation-layer-boundaries.md).

The project uses the term BiometricDevice for the runtime-facing abstraction and concrete biometric device names for vendor-specific implementations.

Therefore, ZKTecoDevice represents the concrete implementation while BiometricDevice remains the abstraction consumed by higher-level orchestration code.
As a BiometricDevice, each ZKTecoDevice instance represents a commissioned
terminal deployed at a known office site.

## Related Documents

- [ADR-0003: Device And Transformation Layer Boundaries](../decisions/0003-device-and-transformation-layer-boundaries.md)
- [Device Layer Abstraction](../proposals/device-layer-abstraction.md)
- [Biometric Device Configuration specification](biometric-device-configuration.md)
- [BiometricDevice specification](biometric-device.md)

## Scope

This specification covers:

- the ZKTeco concrete biometric device implementation
- migration from the current `attendance_etl.device` package to the preferred `attendance_etl.devices` package
- ZKTeco SDK ownership
- ZKTeco-specific connection option ownership
- extraction responsibilities
- record-clearing responsibilities
- dependency rules
- testing expectations

This specification does not cover:

- BiometricDeviceFactory
- biometric device configuration shape or startup validation
- TransformationStrategy
- canonicalisation
- validation
- normalisation
- AttendanceEvent construction
- Employee construction
- backend payload creation

## Package Structure

Concrete device implementations and device abstractions should reside under:

    src/attendance_etl/devices/

Preferred layout:

    devices/
    ├── __init__.py
    ├── biometric_device.py
    ├── zkteco_device.py
    ├── suprema_device.py
    └── hikvision_device.py

The project avoids generic module names such as:

- `base.py`
- `interfaces.py`

Module names should describe the responsibility they contain.

The `BiometricDevice` abstraction belongs in:

    devices/biometric_device.py

The concrete ZKTeco implementation belongs in:

    devices/zkteco_device.py

ETLP-27 finalises the device-layer vocabulary by using `devices/biometric_device.py` for the abstraction and `devices/zkteco_device.py` for the concrete ZKTeco implementation.

Compatibility aliases should not be retained unless required to keep existing imports passing during the migration. If temporary aliases are required, they must be removed before Milestone 3 is considered complete.

## Relationship To BiometricDevice

ZKTecoDevice is the first concrete implementation of BiometricDevice.

It must satisfy the BiometricDevice contract without expanding the public runtime-facing interface.

The runtime should interact with BiometricDevice rather than ZKTecoDevice directly.

This follows the dependency inversion principles established by
[ADR-0003](../decisions/0003-device-and-transformation-layer-boundaries.md).

High-level code depends on BiometricDevice.

Concrete device implementations satisfy that contract.

## Responsibilities

ZKTecoDevice owns:

- ZKTeco SDK imports
- read-only commissioned device `site_id` inherited from BiometricDevice
- device connection lifecycle
- device communication
- ZKTeco-specific connection defaults
- extraction of raw users
- extraction of raw attendance records
- clearing attendance records

**Note**
Connection lifecycle ownership refers to managing connections internally as part of device operations. It does not imply that BiometricDevice must expose public connect() or disconnect() methods.

The implementation should preserve existing runtime behaviour.

ETLP-26 also owns the package/name alignment required to establish the final device-layer vocabulary:

- `BiometricDevice` remains the abstraction.
- `ZKTecoDevice` is the concrete implementation.
- the package is `attendance_etl.devices`.

## Non-Responsibilities

ZKTecoDevice does not own:

- canonicalisation
- validation
- normalisation
- Employee construction
- AttendanceEvent construction
- runtime orchestration
- persistence
- backend communication
- global ingestion-client configuration

These concerns belong to other architectural layers.

## ZKTeco Configuration Ownership

ZKTeco connection configuration is supplied through `ZKTecoOptions` as defined
by the
[Biometric Device Configuration specification](biometric-device-configuration.md).

ZKTecoDevice owns vendor-specific behaviour around those options:

- consuming `ZKTecoOptions` supplied through construction
- keeping ZKTeco SDK interaction isolated inside the concrete device
- initialising absent optional ZKTeco options with implementation-level defaults

`site_id` is not a ZKTeco option. Its configuration ownership is defined in the
[Biometric Device Configuration specification](biometric-device-configuration.md).

`BiometricDeviceFactory` passes `BiometricDeviceConfig.site_id` into
`ZKTecoDevice` construction. `ZKTecoDevice` inherits the read-only `site_id`
property from `BiometricDevice` and uses that identity when decoding extracted
attendance records. Its public `extract_attendance_records(...)` method must
not require `site_id` as a parameter.

## Dependency Rules

ZKTecoDevice may depend on:

- ZKTeco SDK
- BiometricDevice
- ZKTecoOptions

ZKTecoDevice must not depend on:

- runtime orchestration
- workflow state transitions
- backend APIs
- messaging clients
- transformation strategies

## SDK Boundary

The ZKTeco SDK is considered a low-level implementation detail.

SDK imports and SDK-specific behaviour shall remain isolated within ZKTecoDevice.

Runtime and workflow modules should not import ZKTeco SDK types directly.

## Connection Lifecycle Ownership

ZKTecoDevice owns the device connection lifecycle.

Ownership does not require exposing connection-management operations through the BiometricDevice interface.

The current implementation establishes and releases connections internally for each operation.

ETLP-26 shall preserve the existing lifecycle model unless a demonstrated requirement exists for long-lived device connections.

## Exception Handling

Existing exception behaviour should be preserved.

Optional local improvements are permitted if:

- behaviour remains unchanged
- improvements remain local to the concrete biometric device

Introduction of a broader exception hierarchy is outside the scope of ETLP-26.

## Encapsulation

The concrete biometric device implementation should own its behaviour.

Device-specific operations should reside within the concrete biometric device implementation rather than in module-level helper functions.

The concrete biometric device implementation may use private helper methods where appropriate.

This improves encapsulation and keeps device-specific behaviour local to the concrete biometric device.

## Testing Expectations

ETLP-26 tests should verify:

- ZKTecoDevice satisfies BiometricDevice
- ZKTecoDevice inherits read-only site_id from BiometricDevice
- SDK interaction remains isolated
- existing extraction behaviour remains unchanged
- existing record-clearing behaviour remains unchanged

Tests should not require live biometric hardware.

## Migration Notes

ETLP-25 and ETLP-26 documentation previously used:

    DeviceClient

for the runtime-facing abstraction.

ETLP-27 standardises that terminology to:

    BiometricDevice

ETLP-26 aligned the existing implementation with the abstraction name used at that time.

ETLP-26 should migrate the already-merged ETLP-25 abstraction from:

    src/attendance_etl/device/base.py

into:

    src/attendance_etl/devices/device_client.py

ETLP-27 renames that module to:

    src/attendance_etl/devices/biometric_device.py

ETLP-26 should migrate the existing ZKTeco implementation from:

    src/attendance_etl/device/zkteco.py

into:

    src/attendance_etl/devices/zkteco_device.py

All imports and tests should be updated to the new package path.

This issue does not introduce:

- BiometricDeviceFactory
- TransformationStrategy
- extraction migration
- runtime orchestration changes

Those concerns belong to later issues.

## Acceptance Checks

ETLP-26 is complete when:

- ZKTecoDevice conforms to BiometricDevice
- ZKTecoDevice receives site_id at construction and inherits the site_id property
- `BiometricDevice` lives in `attendance_etl.devices.biometric_device`
- `ZKTecoDevice` lives in `attendance_etl.devices.zkteco_device`
- no new code imports from the old `attendance_etl.device.base` path
- no new code imports from the old `attendance_etl.device.zkteco` path
- SDK interaction remains isolated
- runtime behaviour remains unchanged
- existing tests continue to pass
