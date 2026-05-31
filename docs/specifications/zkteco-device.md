# ZKTecoDevice Specification

## Status

Draft

## Purpose

Define the technical contract for the concrete ZKTeco implementation of DeviceClient.

This specification exists to ensure that ZKTeco-specific SDK interaction remains isolated behind the device abstraction boundary established by ADR-0003.

The project uses the term DeviceClient for the runtime-facing abstraction and Device for concrete biometric device implementations.

Therefore, ZKTecoDevice represents the concrete implementation while DeviceClient remains the abstraction consumed by higher-level orchestration code.

## Related Documents

- docs/decisions/0003-device-and-transformation-layer-boundaries.md
- docs/proposals/device-layer-abstraction.md
- docs/specifications/device-client.md

## Scope

This specification covers:

- the ZKTeco concrete device implementation
- migration from the current `attendance_etl.device` package to the preferred `attendance_etl.devices` package
- ZKTeco SDK ownership
- extraction responsibilities
- record-clearing responsibilities
- dependency rules
- testing expectations

This specification does not cover:

- DeviceFactory
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
    ├── device_client.py
    ├── zkteco_device.py
    ├── suprema_device.py
    └── hikvision_device.py

The project avoids generic module names such as:

- `base.py`
- `interfaces.py`

Module names should describe the responsibility they contain.

The `DeviceClient` abstraction belongs in:

    devices/device_client.py

The concrete ZKTeco implementation belongs in:

    devices/zkteco_device.py

ETLP-25 previously introduced `DeviceClient` under the singular `device` package. ETLP-26 should correct this forward by moving the abstraction into `devices/device_client.py` and moving the concrete ZKTeco implementation into `devices/zkteco_device.py`.

This is a forward correction, not a history rewrite.

Compatibility aliases should not be retained unless required to keep existing imports passing during the migration. If temporary aliases are required, they must be removed before Milestone 3 is considered complete.

## Relationship To DeviceClient

ZKTecoDevice is the first concrete implementation of DeviceClient.

It must satisfy the DeviceClient contract without expanding the public runtime-facing interface.

The runtime should interact with DeviceClient rather than ZKTecoDevice directly.

This follows the dependency inversion principles established by ADR-0003.

High-level code depends on DeviceClient.

Concrete device implementations satisfy that contract.

## Responsibilities

ZKTecoDevice owns:

- ZKTeco SDK imports
- device connection lifecycle
- device communication
- extraction of raw users
- extraction of raw attendance records
- clearing attendance records

**Note**
Connection lifecycle ownership refers to managing connections internally as part of device operations. It does not imply that DeviceClient must expose public connect() or disconnect() methods.

The implementation should preserve existing runtime behaviour.

ETLP-26 also owns the package/name alignment required to establish the final device-layer vocabulary:

- `DeviceClient` remains the abstraction.
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

These concerns belong to other architectural layers.

## Dependency Rules

ZKTecoDevice may depend on:

- ZKTeco SDK
- DeviceClient

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

Ownership does not require exposing connection-management operations through the DeviceClient interface.

The current implementation establishes and releases connections internally for each operation.

ETLP-26 shall preserve the existing lifecycle model unless a demonstrated requirement exists for long-lived device connections.

## Exception Handling

Existing exception behaviour should be preserved.

Optional local improvements are permitted if:

- behaviour remains unchanged
- improvements remain local to the adapter

Introduction of a broader exception hierarchy is outside the scope of ETLP-26.

## Encapsulation

The concrete device implementation should own its behaviour.

Device-specific operations should reside within the concrete device implementation rather than in module-level helper functions.

The concrete device implementation may use private helper methods where appropriate.

This improves encapsulation and keeps device-specific behaviour local to the adapter.

## Testing Expectations

ETLP-26 tests should verify:

- ZKTecoDevice satisfies DeviceClient
- SDK interaction remains isolated
- existing extraction behaviour remains unchanged
- existing record-clearing behaviour remains unchanged

Tests should not require live biometric hardware.

## Migration Notes

ETLP-26 aligns the existing implementation with DeviceClient.

ETLP-26 should migrate the already-merged ETLP-25 abstraction from:

    src/attendance_etl/device/base.py

into:

    src/attendance_etl/devices/device_client.py

ETLP-26 should migrate the existing ZKTeco implementation from:

    src/attendance_etl/device/zkteco.py

into:

    src/attendance_etl/devices/zkteco_device.py

All imports and tests should be updated to the new package path.

This issue does not introduce:

- DeviceFactory
- TransformationStrategy
- extraction migration
- runtime orchestration changes

Those concerns belong to later issues.

## Acceptance Checks

ETLP-26 is complete when:

- ZKTecoDevice conforms to DeviceClient
- `DeviceClient` lives in `attendance_etl.devices.device_client`
- `ZKTecoDevice` lives in `attendance_etl.devices.zkteco_device`
- no new code imports from the old `attendance_etl.device.base` path
- no new code imports from the old `attendance_etl.device.zkteco` path
- SDK interaction remains isolated
- runtime behaviour remains unchanged
- existing tests continue to pass
