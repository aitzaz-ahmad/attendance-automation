# BiometricDeviceFactory Specification

## Status

Draft

## Purpose

Define the technical contract for the biometric device factory introduced by ETLP-27.

The factory is the composition boundary for concrete biometric device construction.

It prevents runtime orchestration from depending directly on concrete biometric device implementations.

## Related Documents

- docs/decisions/0003-device-and-transformation-layer-boundaries.md
- docs/proposals/device-layer-abstraction.md
- docs/specifications/biometric-device.md
- docs/specifications/zkteco-device.md

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

The abstraction module is:

    devices/biometric_device.py

The factory returns the abstraction rather than concrete biometric device implementation types.

## Scope

This specification covers:

- biometric device construction
- biometric device selection
- factory responsibilities
- dependency rules
- testing expectations

This specification does not cover:

- plugin systems
- registry frameworks
- dependency injection containers
- transformation strategies
- canonicalisation
- validation
- normalisation
- additional biometric device vendors

## Design Intent

The factory exists to centralise concrete biometric device construction.

The runtime should depend on the BiometricDevice abstraction.

The factory may depend on concrete biometric device implementations.

This keeps concrete construction knowledge in one place.

## Responsibilities

BiometricDeviceFactory owns:

- selecting the concrete biometric device implementation
- constructing concrete biometric device instances
- returning instances as BiometricDevice

BiometricDeviceFactory does not own:

- runtime orchestration
- device communication
- extraction
- transformation
- validation
- normalisation
- backend payload construction

## Dependency Rules

BiometricDeviceFactory may import:

- BiometricDevice
- ZKTecoDevice
- configuration required to construct the selected biometric device

BiometricDeviceFactory must not import:

- runtime workflow state
- messaging clients
- persistence helpers
- transformation strategies
- backend APIs

## Vendor Selection

Biometric device selection is driven by the configuration field:

    device_vendor

Example:

    device_vendor = "zkteco"

Unsupported vendors should fail clearly.

The factory should never silently fall back to another implementation.

## Configuration

ETLP-27 introduces explicit vendor selection.

Example:

    device_vendor = "zkteco"

The current implementation supports only:

    zkteco

Additional vendors may be added in future milestones.

The factory should remain simple.

No plugin mechanism, registry framework, or dynamic import system should be introduced.

## Factory API

The preferred factory API is:

    BiometricDeviceFactory.create(...) -> BiometricDevice

The factory should return the abstraction rather than a concrete implementation type.

The exact constructor parameters should be derived from the current runtime configuration.

The factory should remain simple and explicit.

Do not introduce additional abstraction layers.

## Runtime Integration

After ETLP-27, runtime construction should flow through the factory.

Runtime should no longer construct ZKTecoDevice directly.

Expected dependency direction:

    PiRuntime
        ↓
    BiometricDeviceFactory
        ↓
    ZKTecoDevice

Runtime receives a BiometricDevice.

## Error Handling

Unsupported device vendors should fail clearly.

Example:

    Unsupported device vendor: hikvision

The factory should not silently substitute another implementation.

## Testing Expectations

ETLP-27 tests should verify:

- factory returns a BiometricDevice
- factory can construct the current ZKTecoDevice
- unsupported vendor selections fail clearly
- runtime no longer imports or constructs ZKTecoDevice directly
- no plugin system, registry framework, or dependency injection container is introduced

Tests should not require live biometric hardware.

## Migration Notes

ETLP-25 introduced the runtime-facing abstraction using the name:

    DeviceClient

ETLP-27 standardises the project vocabulary by renaming the abstraction to:

    BiometricDevice

ETLP-27 also renames the abstraction module from:

    devices/device_client.py

to:

    devices/biometric_device.py

Earlier planning used the factory name:

    DeviceFactory

ETLP-27 standardises the composition boundary name as:

    BiometricDeviceFactory

This is a forward correction intended to align the abstraction with its actual responsibility.

The terminology change must be applied consistently across:

- source code
- tests
- documentation
- module names
- import paths

ETLP-27 introduces the composition boundary.

Runtime construction currently looks like:

    PiRuntime
        ↓
    ZKTecoDevice

After ETLP-27:

    PiRuntime
        ↓
    BiometricDeviceFactory
        ↓
    ZKTecoDevice

This issue should not introduce:

- additional device vendors
- transformation strategies
- extraction migration
- runtime behaviour changes

Those concerns belong to later issues.

## Acceptance Checks

ETLP-27 is complete when:

- biometric device construction is centralised
- runtime no longer constructs concrete biometric devices directly
- BiometricDeviceFactory.create(...) returns BiometricDevice
- BiometricDevice is the runtime-facing abstraction
- BiometricDeviceFactory returns BiometricDevice
- no new code uses previous device-layer terminology outside explicit migration notes
- vendor selection logic is not scattered
- unsupported vendors fail clearly
- existing runtime behaviour remains unchanged
