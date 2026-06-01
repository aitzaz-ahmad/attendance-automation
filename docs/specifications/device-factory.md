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

- JSON configuration loading
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

The factory consumes a validated `BiometricDeviceConfig`.

`BiometricDeviceConfig` is vendor-neutral at the root level and contains:

    site_id: str
    vendor: str
    device_options: VendorOptions

`BiometricDeviceConfig` must not expose ZKTeco-specific fields directly.

`site_id` identifies the office, site, or location from which attendance records
are extracted. It is deployment/domain metadata and does not belong inside
`VendorOptions` or `ZKTecoOptions`.

`vendor` selects the biometric device implementation.

`device_options` contains vendor-specific connection metadata.

Vendor-specific configuration belongs behind `VendorOptions`.

The current ZKTeco implementation uses `ZKTecoOptions` for ZKTeco-specific
connection options:

    ip_address: str
    comm_port: int
    timeout: Optional[int]
    force_udp: Optional[bool]
    ommit_ping: Optional[bool]

Configuration loading and validation are owned by `BiometricDeviceConfigBuilder`
or an equivalent loader, not by the factory.

The loader is responsible for:

- reading the JSON configuration file
- rejecting missing or unreadable files
- rejecting invalid JSON
- validating required root fields
- validating supported vendors
- validating vendor-specific options
- constructing the correct `VendorOptions` object

## Vendor Selection

Biometric device selection is driven by the configuration field:

    vendor

Example:

    vendor = "zkteco"

Unsupported vendors should fail clearly.

The factory should never silently fall back to another implementation.

## Configuration

ETLP-27 introduces explicit vendor selection.

Example:

    vendor = "zkteco"

The current implementation supports only:

    zkteco

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
- factory consumes BiometricDeviceConfig rather than raw ZKTeco fields
- configuration loading and validation are tested outside the factory boundary
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
- root biometric device configuration remains vendor-neutral
- site_id remains root-level deployment/domain metadata
- ZKTeco-specific options do not live in global ingestion-client configuration
- configuration loading is separate from concrete device construction
- existing runtime behaviour remains unchanged
