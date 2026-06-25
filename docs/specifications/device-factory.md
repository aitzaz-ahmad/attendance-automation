# BiometricDeviceFactory Specification

## Status

Accepted

## Lifecycle

Active

## Purpose

Define the technical contract for the biometric device factory introduced by ETLP-27.

The factory is the composition boundary for concrete biometric device construction.

It prevents runtime orchestration from depending directly on concrete biometric device implementations.

## Related Documents

- [ADR-0003: Device And Transformation Layer Boundaries](../decisions/0003-device-and-transformation-layer-boundaries.md)
- [Device Layer Abstraction](../proposals/device-layer-abstraction.md)
- [Biometric Device Configuration specification](biometric-device-configuration.md)
- [BiometricDevice specification](biometric-device.md)
- [ZKTecoDevice specification](zkteco-device.md)

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
- relationship to validated biometric device configuration
- dependency rules
- testing expectations

This specification does not cover:

- biometric device configuration shape
- JSON configuration loading
- startup configuration validation
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
- passing root `BiometricDeviceConfig.site_id` into commissioned device instances
- returning instances as BiometricDevice

BiometricDeviceFactory does not own:

- runtime orchestration
- JSON configuration loading
- device communication
- extraction
- transformation
- validation
- normalisation
- backend payload construction

## Dependency Rules

BiometricDeviceFactory may import:

- BiometricDevice
- BiometricDeviceConfig
- ZKTecoDevice
- configuration required to construct the selected biometric device

BiometricDeviceFactory must not import:

- runtime workflow state
- messaging clients
- persistence helpers
- transformation strategies
- backend APIs

## Configuration Contract

The factory consumes a validated `BiometricDeviceConfig` as defined by the
[Biometric Device Configuration specification](biometric-device-configuration.md).

The factory owns construction behaviour after validation:

- selecting the concrete biometric device implementation from the validated
  vendor value
- deriving constructor inputs from validated vendor options
- passing root `BiometricDeviceConfig.site_id` into the concrete
  `BiometricDevice`
- returning the constructed instance as `BiometricDevice`

The factory does not own the configuration schema, JSON loading, required-field
validation, vendor-specific option validation, or optional ZKTeco default
initialisation.

For ZKTeco, root `site_id` remains separate from `ZKTecoOptions`; it must not be
folded into vendor options.

## Vendor Selection

Biometric device selection is driven by the validated
`BiometricDeviceConfig.vendor` value.

Unsupported vendors should fail clearly.

The factory should never silently fall back to another implementation.

## Configuration

ETLP-27 introduces explicit vendor selection. The current supported vendor value
is defined in the
[Biometric Device Configuration specification](biometric-device-configuration.md).

Additional vendors may be added in future milestones.

The factory should remain simple.

No plugin mechanism, registry framework, or dynamic import system should be introduced.

## Factory API

The preferred factory API is:

    BiometricDeviceFactory.create(config: BiometricDeviceConfig) -> BiometricDevice

The factory should return the abstraction rather than a concrete implementation type.

The exact constructor parameters should be derived from the validated
`BiometricDeviceConfig` and its `device_options`.

The factory should remain simple and explicit.

Do not introduce additional abstraction layers.

## Runtime Integration

After ETLP-27, runtime construction should flow through the factory.

Runtime should no longer construct ZKTecoDevice directly.

Expected dependency direction:

    PiRuntime
        ↓
    BiometricDeviceConfigBuilder
        ↓
    BiometricDeviceConfig
        ↓
    BiometricDeviceFactory
        ↓
    ZKTecoDevice

Runtime receives a BiometricDevice.

Runtime should not import or construct ZKTecoDevice directly.

Runtime should not pass raw ZKTeco option fields around as top-level runtime
configuration.

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
- factory consumes `BiometricDeviceConfig` rather than raw ZKTeco fields
- factory passes BiometricDeviceConfig.site_id into the returned BiometricDevice
- configuration loading and validation are tested outside the factory boundary
  against the
  [Biometric Device Configuration specification](biometric-device-configuration.md)
- site_id remains root-level BiometricDeviceConfig metadata and is not passed as
  a ZKTeco connection option
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

After ETLP-27 and configuration alignment:

    PiRuntime
        ↓
    BiometricDeviceConfigBuilder
        ↓
    BiometricDeviceConfig
        ↓
    BiometricDeviceFactory
        ↓
    ZKTecoDevice

This issue should not introduce:

- additional device vendors
- plugin discovery
- importlib-based loading
- dependency injection containers
- transformation strategies
- canonicalisation
- extraction migration
- runtime behaviour changes
- GitHub project mutations

Those concerns belong to later issues.

## Acceptance Checks

ETLP-27 is complete when:

- biometric device construction is centralised
- runtime no longer constructs concrete biometric devices directly
- BiometricDeviceFactory.create(config: BiometricDeviceConfig) returns BiometricDevice
- BiometricDevice is the runtime-facing abstraction
- BiometricDeviceFactory returns BiometricDevice
- no new code uses previous device-layer terminology outside explicit migration notes
- vendor selection logic is not scattered
- unsupported vendors fail clearly
- the factory respects the
  [Biometric Device Configuration specification](biometric-device-configuration.md)
- constructed BiometricDevice instances expose read-only site_id
- configuration loading is separate from concrete device construction
- existing runtime behaviour remains unchanged
