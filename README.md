# Attendance Automation

Attendance Automation is an ETL-oriented Python project for collecting attendance records from a ZKTeco
biometric device, moving them through Google Pub/Sub and Google Cloud Functions, and preparing Google Sheets
output for HR attendance review.

[![CI](https://github.com/aitzaz-ahmad/attendance-automation/actions/workflows/ci.yml/badge.svg)](https://github.com/aitzaz-ahmad/attendance-automation/actions/workflows/ci.yml)

## Problem Statement

Month-end attendance review is a manual, repetitive process for HR teams. Attendance records need to be
extracted from biometric devices, checked against the relevant review period, prepared in a usable format,
and stored where reviewers can work with them.

This repository automates the collection and preparation path for attendance records from a ZKTeco
biometric attendance device. The current implementation is not presented as a finished production platform;
it is a structured ETL codebase and migration target for making the original attendance workflow easier to
run, validate, and evolve.

## System Overview

The system is organized around the canonical Python package namespace `attendance_etl`, with ingestion
runtime modules split across `attendance_etl.device`, `attendance_etl.transform`, `attendance_etl.messaging`,
`attendance_etl.storage`, and `attendance_etl.pi4`. Google Cloud Function logic lives under
`attendance_etl.functions`.

At a high level, the ingestion client extracts records from a ZKTeco biometric device, filters and decodes
the device data, and publishes work through Google Pub/Sub. Google Cloud Functions process review period,
review sheet, and attendance record messages. Google Sheets remains the storage and review output used by HR.

The legacy Raspberry Pi 4 execution path is preserved as a compatibility wrapper at `src/pi4/pi4_client.py`.
That wrapper delegates through `attendance_etl.ingestion.client` to the Pi runtime modules, keeping the
current device-oriented workflow runnable while the package moves toward more explicit ingestion boundaries.

## Architecture Diagram

![High-level architecture diagram](docs/diagrams/high-level-architecture.png "High-level architecture diagram")

For the canonical system architecture, including the Mermaid topology diagram, component boundaries, and
current limitations, see [Architecture](docs/architecture.md).

## Data Pipeline

The intended data flow is:

```mermaid
flowchart LR
  Extract --> Canonicalise --> Publish --> Process --> StoreReview[Store / Review]
```

For the ordered stage-by-stage pipeline, including current implementation paths and known limitations, see
[Data Pipeline](docs/data-pipeline.md).

## Biometric Device Configuration

The Raspberry Pi ingestion runtime loads biometric device configuration from
`biometric_device_config.json`.

For the current ZKTeco device path, the configuration shape is:

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

`site_id` identifies the office, site, or location from which attendance records
are extracted. It is deployment/domain metadata on `BiometricDeviceConfig`,
independent of the biometric device vendor and communication mechanism.

`vendor` selects the biometric device implementation. `device_options` contains
vendor-specific connection metadata; for ZKTeco devices, `ip_address`,
`comm_port`, `timeout`, `force_udp`, and `ommit_ping` remain ZKTeco-specific
options.

## Reliability Mechanisms

The ingestion workflow uses a finite state machine with checkpoint persistence, retry paths, and interruption
recovery behaviour designed to resume from the last checkpointed non-waiting state.

![Raspberry Pi client finite state machine](docs/diagrams/pi4-client-fsm.png "Raspberry Pi client finite state machine")

For the detailed recovery model, see [Reliability Model](docs/reliability.md).

## Current Implementation

The repository currently contains:

- A Python `src/` layout with the canonical package namespace `attendance_etl`.
- Ingestion runtime modules under `src/attendance_etl/{device,transform,messaging,storage,pi4}/`, with a
  compatibility facade at `src/attendance_etl/ingestion/client.py`.
- Google Cloud Function implementation modules under `src/attendance_etl/functions/`.
- Google Cloud Function deployment wrappers under `src/backend/*/main.py`.
- A Raspberry Pi compatibility entry point at `src/pi4/pi4_client.py`.
- Static quality tooling configured through `pyproject.toml` for Ruff, Black, and MyPy.
- A GitHub Actions CI workflow at `.github/workflows/ci.yml` that runs lint, format, and type gates.
- A test directory scaffold under `tests/`; project pytest execution is still pending in the validation
  workflow.

## Future Roadmap

Near-term work should stay focused on making the existing ETL system easier to reason about, test, and
extend. For the post-MVP roadmap, including future PostgreSQL persistence, API layer, and reporting and
analytics direction, see [Future Work](docs/future-work.md).

## Contributing

For setup, dependency installation, validation commands, and contribution workflow, see:

- [Local Development](docs/workflows/local-development.md)

Key repository references:

- [Agent operating instructions](AGENTS.md)
- [Documentation index](docs/README.md)
- [Architecture](docs/architecture.md)
- [Data pipeline](docs/data-pipeline.md)
- [Reliability model](docs/reliability.md)
- [Canonical attendance event contract](docs/contracts/canonical-attendance-event.md)
- [Future work](docs/future-work.md)
- [CI workflow](.github/workflows/ci.yml)
