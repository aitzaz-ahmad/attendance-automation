# Attendance Automation

Attendance Automation is an ETL-oriented Python project for collecting attendance records from a ZKTeco
biometric device, moving them through Google Cloud Pub/Sub and Google Cloud Functions, and preparing
Google Sheets output for HR attendance review.

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
logic under `attendance_etl.ingestion` and Google Cloud Function logic under `attendance_etl.functions`.

At a high level, the ingestion client extracts records from a ZKTeco biometric device, filters and decodes
the device data, and publishes work through Google Cloud Pub/Sub. Google Cloud Functions process review
period, review sheet, and attendance record messages. Google Sheets remains the storage and review output
used by HR.

The legacy Raspberry Pi 4 execution path is preserved as a compatibility wrapper at `src/pi4/pi4_client.py`.
That wrapper delegates to `attendance_etl.ingestion.client`, which keeps the current device-oriented workflow
runnable while the package moves toward more hardware-neutral ingestion terminology and boundaries.

## Architecture Diagram

![High-level architecture diagram](docs/diagrams/high-level-architecture.png "High-level architecture diagram")

The architecture uses a pull-publish model between the ingestion client and the serverless backend. The
client pulls attendance records from the biometric device and publishes messages to Pub/Sub. Cloud Function
handlers consume those messages, interact with review-period and review-sheet data, and write attendance
review output to Google Sheets.

## Data Pipeline

The intended data flow is:

```text
Extract -> Transform/Canonicalise -> Publish -> Process -> Store/Review
```

- Extract: the ingestion client connects to the ZKTeco device and retrieves users and attendance records.
- Transform/Canonicalise: current code decodes device records into Python dictionaries containing timestamp,
  employee name, device identifier, and entry type. A more explicit canonical attendance event schema remains
  future work.
- Publish: the ingestion client publishes JSON payloads to Google Cloud Pub/Sub topics for review-period,
  review-sheet, and attendance-record workflows.
- Process: Google Cloud Function modules under `attendance_etl.functions` process Pub/Sub events and coordinate
  review-period lookup, review-sheet creation, and record storage.
- Store/Review: processed attendance records are appended to Google Sheets worksheets used for attendance
  review.

## Reliability Mechanisms

The ingestion workflow is modeled as a finite state machine with explicit states for review-period lookup,
review-sheet creation, attendance relay, last-stored timestamp handling, expired review periods, and alarm
paths.

![Raspberry Pi client finite state machine](docs/diagrams/pi4-client-fsm.png "Raspberry Pi client finite state machine")

Current reliability mechanisms visible in the code and diagrams include:

- A local `snapshot.json` checkpoint file that stores the client state, system flags, review sheet ID, and
  last stored timestamp.
- Checkpointing on non-waiting states so the client can resume after shutdown without re-entering a Pub/Sub
  waiting state.
- Timeout and power-failure recovery paths represented in the finite state machine.
- An explicit final-alarm path for missing review-period information, with notification delivery still marked
  as a TODO in code.
- A deep-sleep path when the next review period is unavailable.
- Last-stored timestamp tracking to avoid resending records that were already persisted.

## Current Implementation

The repository currently contains:

- A Python `src/` layout with the canonical package namespace `attendance_etl`.
- Ingestion code at `src/attendance_etl/ingestion/client.py`.
- Google Cloud Function implementation modules under `src/attendance_etl/functions/`.
- Google Cloud Function deployment wrappers under `src/backend/*/main.py`.
- A Raspberry Pi compatibility entry point at `src/pi4/pi4_client.py`.
- Static quality tooling configured through `pyproject.toml` for Ruff, Black, and MyPy.
- A GitHub Actions CI workflow at `.github/workflows/ci.yml` that runs lint, format, and type gates.
- A test directory scaffold under `tests/`; project pytest execution is still pending in the validation
  workflow.

## Future Roadmap

Near-term work should stay focused on making the existing ETL system easier to reason about, test, and
extend:

- Define a canonical attendance event schema.
- Document architecture and data contracts.
- Add focused tests around ingestion and function behavior.
- Isolate the transformation layer.
- Introduce a device abstraction around biometric attendance extraction.
- Introduce messaging and storage abstractions around Pub/Sub and Google Sheets boundaries.
- Isolate reliability and finite state machine behavior for testability.
- Continue repository and portfolio documentation polish.

## Contributing

For setup, dependency installation, validation commands, and contribution workflow, see:

- [Local Development](docs/workflows/local-development.md)

Key repository references:

- [Agent operating instructions](AGENTS.md)
- [CI workflow](.github/workflows/ci.yml)
