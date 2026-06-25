# Architecture

## Status

Accepted

## Lifecycle

Active

## System Overview

Attendance Automation is an ETL-oriented attendance pipeline for moving records from a ZKTeco biometric
device into reviewer-facing Google Sheets output.

The repository is currently a structured migration target rather than a completed production platform. The
implemented path is device-oriented and Google Sheets-backed, while the documented canonical attendance event
contract defines the target internal record shape for future transformation work.

## Architecture Diagram

The current Mermaid system flow shows the source device, ingestion runtime, canonical event target, Pub/Sub
messaging, Cloud Function processing, Google Sheets output, and future PostgreSQL storage boundary.

```mermaid
flowchart LR
  subgraph SD[Source Device]
    ZK[ZKTeco biometric device]
  end

  subgraph IR[Ingestion Runtime]
    PI[src/pi4/pi4_client.py compatibility Pi client]
    ING[attendance_etl.ingestion compatibility facade]
    PI4[attendance_etl.pi4 runtime and workflow]
    EVENT[Canonical attendance event contract target]
  end

  subgraph MSG[Messaging]
    PUBSUB[Google Pub/Sub topics]
  end

  subgraph SP[Serverless Processing]
    WRAP[Google Cloud Functions deployment wrappers]
    FUNC[attendance_etl.functions]
  end

  subgraph OUT[Storage and Review Output]
    SHEETS[Google Sheets attendance review output]
  end

  subgraph FUT[Future Storage]
    PG[PostgreSQL storage]
  end

  ZK --> PI
  PI --> ING
  ING --> PI4
  PI4 --> EVENT
  EVENT --> PUBSUB
  PUBSUB --> WRAP
  WRAP --> FUNC
  FUNC --> SHEETS
  FUNC -. future .-> PG
```

The README also includes a high-level [architecture diagram](diagrams/high-level-architecture.png). The ingestion reliability [state machine](diagrams/pi4-client-fsm.png) is documented separately and summarized in
[Reliability Model](specifications/reliability-model.md).

## Component Model

### Biometric Source Device

The source system is a ZKTeco biometric attendance device. The current ingestion code connects to the device,
reads users and attendance records, and decodes device-specific fields before publishing downstream work.

### Ingestion Runtime And Pi Compatibility Client

The legacy Raspberry Pi 4 execution path remains available at `src/pi4/pi4_client.py`. It is a compatibility
entry point that delegates to the `attendance_etl.ingestion.client` compatibility facade. The facade delegates
to `attendance_etl.pi4.runtime`, preserving the current runnable device workflow while the project moves
toward more explicit ingestion boundaries.

### `attendance_etl.ingestion`

`src/attendance_etl/ingestion/client.py` is the compatibility facade for existing callers. It preserves
`attendance_etl.ingestion.client.main()` and delegates runtime execution to `attendance_etl.pi4.runtime`.

### Ingestion Support Modules

The Raspberry Pi ingestion client is split into focused package modules:

- `attendance_etl.devices.biometric_device` owns the runtime-facing biometric device abstraction. A
  `BiometricDevice` is a commissioned biometric terminal deployed at a known office site, and every instance
  exposes its configured `site_id` as read-only runtime identity.
- `attendance_etl.devices.biometric_device_factory` owns concrete biometric device construction.
- `attendance_etl.devices.zkteco_device` owns ZKTeco SDK integration, ZKTeco connection lifecycle, device
  reads, attendance clearing, and ZKTeco implementation-level defaults.
- Biometric device configuration shape, vendor option ownership, startup validation, and fail-fast
  configuration rules are defined in
  [Biometric Device Configuration](specifications/biometric-device-configuration.md).
- `attendance_etl.transform.zkteco_transformation_strategy` owns ZKTeco user correlation and punch
  normalisation. Timestamp filtering is owned by `TransformationStrategy.filter(...)`, and
  backend-compatible payload serialisation is owned by `AttendanceEvent.to_dict()`.
- `attendance_etl.models` owns lightweight dataclass representations for core domain contracts such as
  attendance events, employees, review periods, and persisted runtime state.
- `attendance_etl.config` owns shared runtime configuration constants such as Pub/Sub names, local runtime
  files, and polling intervals. Vendor-specific biometric device options do not belong in global
  ingestion-client configuration.
- `attendance_etl.messaging` owns the project messaging boundary. The target public abstraction is
  `Messenger`, with Google Pub/Sub isolated behind `GooglePubSubMessenger`; transport contracts are defined
  in the [Messaging Model](specifications/messaging-model.md).
- `attendance_etl.storage.snapshot` and `attendance_etl.storage.review_period` own the existing JSON files.
- `attendance_etl.pi4.state`, `attendance_etl.pi4.workflow`, and `attendance_etl.pi4.runtime` own Pi state,
  finite-state-machine behavior, and runtime composition.
- `attendance_etl.logging_utils` owns shared logging setup and responsibility-oriented logger acquisition.

The ingestion runtime currently publishes transitional attendance dictionaries. Runtime enforcement of the
canonical attendance event contract is future work.

### Biometric Device Configuration Topology

Architecture owns where biometric device configuration participates in the
system. Configuration shape, required fields, vendor-specific options, startup
validation, and fail-fast rules are defined in
[Biometric Device Configuration](specifications/biometric-device-configuration.md).

Runtime orchestration composes the workflow after loading validated biometric
device configuration:

```text
Pi runtime
    ↓
BiometricDeviceConfigBuilder or equivalent loader
    ↓
BiometricDeviceConfig
    ↓
BiometricDeviceFactory
    ↓
BiometricDevice
```

Runtime may depend on `BiometricDeviceConfig` and `BiometricDevice`, but it
should not depend on `ZKTecoDevice` directly. Configuration loading/building and
concrete device construction remain separate responsibilities.

### Canonical Attendance Event Contract

The attendance event payload contract is documented in
[Canonical Attendance Event](contracts/canonical-attendance-event.md). Architecture records where that
contract sits in the system; field definitions, serialisation shape, and schema evolution belong to the
contract document.

### Google Pub/Sub Messaging

Google Pub/Sub is the current broker between the ingestion runtime and serverless backend. Architecture owns
that topology and adapter placement only. Messaging topics, routing, receive timeout behaviour, transport
validation, and ACK policy are defined in the [Messaging Model](specifications/messaging-model.md).
Reliability ownership, including retry and recovery boundaries, is defined in the
[Reliability Model](specifications/reliability-model.md).

### Cloud Function Deployment Wrappers

The deployable Google Cloud Function entry points live under `src/backend/*/main.py`. These wrappers provide
the per-function deployment shape and delegate implementation work to package modules under
`attendance_etl.functions`.

Each backend function currently carries its own deployment dependencies. Shared dependency isolation across
functions remains a current deployment constraint.

### `attendance_etl.functions`

`src/attendance_etl/functions/` contains the canonical Python implementation modules for serverless behavior.
Current modules process review-period lookup, review-sheet creation, and attendance-record storage workflows.

These modules parse current Pub/Sub payloads and interact with Google APIs. Payload schema ownership remains
with [Canonical Attendance Event](contracts/canonical-attendance-event.md) and related contracts.

### Google Sheets Review Output

Google Sheets is the current implemented persistence and HR review surface. The storage workflow appends raw
attendance records, updates daily attendance data, updates weekly summary data, and returns the latest stored
timestamp used by ingestion to avoid resending already-persisted records.

### Future PostgreSQL Storage

PostgreSQL appears only as future storage in the architecture diagram and roadmap. It is not part of the
current implemented persistence path.

## Sequence View

The simplified sequence below shows the intended flow without claiming that every boundary already enforces
the final canonical contract at runtime.

```mermaid
sequenceDiagram
  participant Client as Ingestion Client
  participant Device as ZKTeco Device
  participant PubSub as Google Pub/Sub
  participant Function as Cloud Function
  participant Sheets as Google Sheets

  Client->>Device: Poll users and attendance records
  Device-->>Client: Return device records
  Client->>Client: Filter and normalise records
  Client->>Client: Target canonical attendance event shape
  Client->>PubSub: Publish review or attendance message
  PubSub->>Function: Deliver Pub/Sub event
  Function->>Sheets: Create or update review sheet output
  Sheets-->>Function: Return update result
  Function->>PubSub: Publish workflow response
  PubSub-->>Client: Return response through messaging boundary
```

## Codebase Structure

```text
src/
├── attendance_etl/
│   ├── devices/       # Biometric device abstractions, factory, and concrete devices
│   ├── transform/     # Source-specific record transformation
│   ├── messaging/     # Pub/Sub integration helpers
│   ├── storage/       # Local JSON persistence helpers
│   ├── pi4/           # Pi runtime, state, and workflow
│   ├── ingestion/     # Compatibility facade for existing ingestion callers
│   └── functions/     # Reusable Cloud Function implementation modules
├── backend/           # Google Cloud Function deployment entry points
└── pi4/               # Raspberry Pi compatibility runtime

docs/                  # Architecture, contracts, workflows, diagrams
tests/                 # Unit/integration test scaffold
```

The important boundary is between deployment wrappers and canonical package modules. `src/backend/*/main.py`
files are deployment entry points; reusable implementation should live under `src/attendance_etl/`.

## Architecture Boundaries And Current Limitations

- Runtime canonical attendance event enforcement is future work.
- Current attendance payloads still use transitional dictionaries in parts of the ingestion and storage path.
- Google Cloud Function dependencies remain organized per function under `src/backend/*/requirements.txt`.
- Google Sheets is the current implemented persistence and review output.
- PostgreSQL is documented as future storage only.
- The reliability and finite state machine behavior is implemented under `attendance_etl.pi4`.
- The source extraction path remains ZKTeco-specific.
- The root biometric device configuration model is vendor-neutral even though the only current concrete
  device implementation is ZKTeco.

## Related Documents

- [README](../README.md)
- [Data Pipeline](data-pipeline.md)
- [Biometric Device Configuration](specifications/biometric-device-configuration.md)
- [Canonical Attendance Event](contracts/canonical-attendance-event.md)
- [Messaging Model](specifications/messaging-model.md)
- [Reliability Model](specifications/reliability-model.md)
