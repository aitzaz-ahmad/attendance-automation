# Biometric Device Configuration Specification

## Status

Accepted

## Lifecycle

Active

## Purpose

Define the canonical documentation contract for biometric device configuration.

This specification is the sole active owner for biometric device configuration
shape, required fields, vendor-specific option ownership, startup validation,
and fail-fast configuration rules.

## Related Documents

- [BiometricDevice specification](biometric-device.md)
- [BiometricDeviceFactory specification](device-factory.md)
- [ZKTecoDevice specification](zkteco-device.md)
- [Reliability model specification](reliability-model.md)
- [Architecture](../architecture.md)

## Scope

This specification covers:

- `BiometricDeviceConfig`
- `VendorOptions`
- `ZKTecoOptions`
- `site_id`
- required and optional biometric device configuration fields
- vendor-specific option ownership
- startup configuration validation expectations
- fail-fast rules for invalid configuration
- the relationship between validated configuration and device factory construction
- the relationship between configuration failures and the reliability model

This specification does not cover:

- `BiometricDevice` runtime behaviour
- `BiometricDeviceFactory` construction logic beyond its configuration boundary
- ZKTeco SDK interaction behaviour
- transformation, canonicalisation, or backend payload construction
- messaging, persistence, or FSM transition policy

## Configuration File

The Raspberry Pi ingestion runtime loads biometric device configuration from:

```text
biometric_device_config.json
```

The current ZKTeco device path uses this shape:

```json
{
  "site_id": "munich-office",
  "vendor": "zkteco",
  "device_options": {
    "ip_address": "192.168.1.201",
    "comm_port": 4370,
    "timeout": 10,
    "force_udp": false,
    "ommit_ping": false
  }
}
```

No additional fields are defined by this specification.

## Object Model

The root biometric device configuration model is vendor-neutral:

```text
VendorOptions
    ^
    |
ZKTecoOptions

BiometricDeviceConfig
    site_id: str
    vendor: str
    device_options: VendorOptions
```

`BiometricDeviceConfig` is the runtime-facing configuration representation. It
contains root deployment/domain metadata, vendor selection metadata, and
vendor-specific options behind the `VendorOptions` abstraction.

`BiometricDeviceConfig` must not expose ZKTeco-specific fields directly.

`VendorOptions` is the base abstraction for vendor-specific configuration.

`ZKTecoOptions` is the concrete vendor-options object for the current ZKTeco
device path.

## Root Fields

| Field | Required? | Owner | Meaning |
| --- | --- | --- | --- |
| `site_id` | Required | `BiometricDeviceConfig` | Office, site, or location from which attendance records are extracted. |
| `vendor` | Required | `BiometricDeviceConfig` | Selects the concrete biometric device implementation. |
| `device_options` | Required | `BiometricDeviceConfig` | Contains vendor-specific connection metadata as a `VendorOptions` value. |

`site_id` is deployment/domain metadata. It is independent of the biometric
device vendor and communication mechanism.

`site_id` must not be placed inside `VendorOptions` or `ZKTecoOptions`.

`BiometricDeviceConfig.site_id` is the source of the commissioned device runtime
identity. `BiometricDeviceFactory` passes this value into concrete
`BiometricDevice` instances during construction. Because a `BiometricDevice`
already owns its site identity, runtime callers must not pass `site_id` into
`extract_attendance_records(...)`.

`site_id` may later be propagated into canonical attendance records as
source-site metadata, but that propagation is outside this configuration
contract.

The current supported `vendor` value is:

```text
zkteco
```

Unsupported vendors must fail clearly. The runtime must not silently fall back
to another implementation.

## Vendor-Specific Options

Vendor-specific configuration belongs behind `VendorOptions`.

Vendor-specific options must not live:

- as root fields on `BiometricDeviceConfig`;
- in global ingestion-client configuration; or
- in runtime orchestration modules.

The current ZKTeco implementation uses `ZKTecoOptions` for ZKTeco-specific
connection metadata.

`ZKTecoOptions` represents ZKTeco-specific values loaded from the biometric
device configuration file.

| Field | Required? | Owner | Meaning |
| --- | --- | --- | --- |
| `ip_address` | Required | `ZKTecoOptions` | ZKTeco device network address. |
| `comm_port` | Required | `ZKTecoOptions` | ZKTeco device communication port. |
| `timeout` | Optional | `ZKTecoOptions` / `ZKTecoDevice` defaults | ZKTeco connection timeout. |
| `force_udp` | Optional | `ZKTecoOptions` / `ZKTecoDevice` defaults | ZKTeco SDK UDP connection option. |
| `ommit_ping` | Optional | `ZKTecoOptions` / `ZKTecoDevice` defaults | ZKTeco SDK ping option. |

The `ommit_ping` spelling follows the pyzk API.

If optional ZKTeco options are absent from `biometric_device_config.json`,
`ZKTecoDevice` owns initialising them with implementation-level defaults.

## Startup Validation

Configuration loading and validation are owned by
`BiometricDeviceConfigBuilder` or an equivalent loader, not by
`BiometricDeviceFactory`, `BiometricDevice`, or `ZKTecoDevice`.

The configuration loader is responsible for:

- reading `biometric_device_config.json`;
- rejecting missing or unreadable configuration files;
- rejecting invalid JSON;
- validating required root fields;
- validating supported vendors;
- validating vendor-specific options; and
- constructing the correct `VendorOptions` object.

Runtime startup requires a valid biometric device configuration before concrete
device construction or device communication can occur.

## Fail-Fast Rules

Configuration defects are deterministic startup failures.

Startup must fail explicitly if:

- the configuration file is missing;
- the configuration file is unreadable;
- the configuration file contains invalid JSON;
- required fields are missing;
- the vendor is unsupported; or
- vendor-specific options are invalid.

The runtime must not:

- retry invalid configuration as though it were a transient transport failure;
- substitute a default vendor;
- silently drop invalid vendor options;
- move ZKTeco options into root configuration; or
- fall back to global ingestion-client configuration for vendor-specific device
  connection options.

## Relationship To Device Factory

`BiometricDeviceFactory` consumes a validated `BiometricDeviceConfig`.

The factory owns concrete device construction after validation:

- selecting the concrete device implementation from `vendor`;
- deriving constructor inputs from `device_options`;
- passing root `BiometricDeviceConfig.site_id` into the concrete
  `BiometricDevice`; and
- returning the constructed instance as `BiometricDevice`.

The factory does not own JSON loading, required-field validation,
vendor-specific option validation, or implementation-level defaults for optional
ZKTeco connection options.

For ZKTeco, the factory passes root `site_id` separately from `ZKTecoOptions`.
`site_id` must not be folded into vendor options.

## Relationship To Reliability Model

The [Reliability Model](reliability-model.md) owns the reliability meaning of
configuration failure.

This configuration specification owns the configuration shape and validation
expectations that cause fail-fast startup behaviour.

Invalid biometric device configuration is not a retryable transport or workflow
failure. It must be surfaced before device construction, device communication,
or workflow progress begins.

## Acceptance Checks

The biometric device configuration contract is complete when:

- `BiometricDeviceConfig` remains vendor-neutral at the root level;
- `site_id`, `vendor`, and `device_options` are required root fields;
- `site_id` remains root-level deployment/domain metadata;
- `VendorOptions` owns the vendor-specific option abstraction;
- `ZKTecoOptions` owns ZKTeco-specific connection options;
- optional ZKTeco options are initialised by `ZKTecoDevice` defaults when absent;
- startup validation fails explicitly for invalid configuration;
- `BiometricDeviceFactory` consumes a validated `BiometricDeviceConfig`; and
- no configuration semantics are duplicated as active authority in other
  documents.
