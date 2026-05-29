# Data Pipeline

## Overview

The attendance pipeline moves attendance records from a ZKTeco biometric device through the ingestion
runtime, Google Pub/Sub, Google Cloud Functions, and Google Sheets attendance review output.

The intended sequence is:

```text
Device polling -> Raw record normalisation -> Canonicalisation -> Event publication -> Backend processing -> Persistence / review output
```

This sequence is consistent with the Mermaid system flow in [Architecture](architecture.md): source device,
ingestion runtime, canonical attendance event contract target, messaging, serverless processing, and Google
Sheets output.

## Pipeline Stages

| Stage | Component | Input | Output | Status |
| --- | --- | --- | --- | --- |
| 1. Device polling / extraction | `attendance_etl.device.zkteco`, orchestrated by `attendance_etl.pi4.workflow` and available through `src/pi4/pi4_client.py` compatibility wrapper | ZKTeco biometric device users and attendance records | Device user list and raw attendance records | Implemented for the current ZKTeco device path |
| 2. Raw record normalisation | `attendance_etl.transform.zkteco_records` | Raw ZKTeco users and attendance records filtered by review timestamp | Transitional Python dictionaries with `username`, `timestamp`, `entry`, and `device` fields | Implemented as device-specific decoding |
| 3. Canonicalisation | Target contract in `docs/contracts/canonical-attendance-event.md` | Normalised source/device records | Canonical attendance event payload | Documented target; not fully adopted by runtime publishing yet |
| 4. Event publication | `attendance_etl.messaging.pubsub`, orchestrated by `attendance_etl.pi4.workflow` | Review-period requests, review-sheet requests, and attendance record payloads | JSON messages on Google Pub/Sub topics | Implemented with transitional attendance record payloads |
| 5. Backend / serverless processing | Google Cloud Functions deployment wrappers under `src/backend/*/main.py`, delegating to `attendance_etl.functions` | Google Pub/Sub event payloads | Review-period responses, review-sheet metadata, stored attendance updates, and last-stored timestamps | Implemented for current review-period, review-sheet, and attendance-record workflows |
| 6. Persistence / review output | `src/attendance_etl/functions/store_attend_records.py` and Google Sheets APIs | Attendance record messages and review sheet metadata | Google Sheets raw data, daily attendance, weekly summary, and last-stored timestamp response | Implemented for Google Sheets attendance review output |

## Stage Details

### 1. Device Polling / Extraction

The ingestion client polls the ZKTeco biometric device for users and attendance records. The legacy
Raspberry Pi 4 entry point remains available as `src/pi4/pi4_client.py` and delegates through
`attendance_etl.ingestion.client` to the Pi runtime modules.

- Purpose: collect device users and attendance punches from the biometric source.
- Current implementation: `attendance_etl.device.zkteco.pull_records_from_device()` connects to the ZKTeco
  device, disables it during reads, fetches users and attendance records, then re-enables and disconnects.
- Input: configured device IP, communication port, and device identifier.
- Output: raw user records and raw attendance records from the device library.
- Limitation: the current extraction path is ZKTeco-specific and still tied to the device-oriented ingestion
  workflow.

### 2. Raw Record Normalisation

The ingestion client filters records for the active review window and decodes the device-specific shape into
Python dictionaries used by the current storage workflow.

- Purpose: remove already-stored records and make ZKTeco records usable by downstream code.
- Current implementation: `filter_records()`, `convert_to_map()`, `convert_to_dict()`, and
  `decode_zk_format()` in `attendance_etl.transform.zkteco_records`.
- Input: raw device users, raw attendance records, review start timestamp, optional review end timestamp, and
  configured device identifier.
- Output: transitional records with timestamp, employee display name, device identifier, and entry type.
- Limitation: this stage produces the current storage payload shape, not the full canonical attendance event
  contract.

### 3. Canonicalisation

Canonicalisation is the intended transformation from source/device records into the documented
[canonical attendance event](contracts/canonical-attendance-event.md) contract.

- Purpose: provide a stable internal event shape for downstream publication, processing, and future storage.
- Current status: the canonical attendance event contract is documented, and the architecture diagram
  references it as the intended canonical event target.
- Input: normalised source/device records.
- Output: canonical attendance event fields such as `event_id`, `source_device_id`, `employee_id`,
  `event_timestamp`, `event_type`, and `ingested_at`.
- Limitation: runtime publishing still sends transitional attendance record dictionaries, so canonicalisation is
  not yet enforced end to end.

### 4. Event Publication

The ingestion client publishes JSON payloads to Google Pub/Sub topics for backend processing.

- Purpose: hand off review-period, review-sheet, and attendance-record work to serverless handlers.
- Current implementation: `attendance_etl.messaging.pubsub.publish_message_to_topic()` serializes
  dictionaries as JSON and publishes to Google Pub/Sub topics such as `get_review_period`,
  `create_review_sheet`, and `store_attend_records`.
- Input: workflow request data from the ingestion state machine.
- Output: Google Pub/Sub messages consumed by Google Cloud Functions.
- Limitation: message payloads are plain JSON dictionaries; schema validation and canonical event enforcement
  are future work.

### 5. Backend / Serverless Processing

Google Cloud Functions deployment wrappers receive Pub/Sub events and delegate to implementation modules under
`attendance_etl.functions`.

- Purpose: process review-period lookup, review-sheet creation, attendance storage, and ingestion feedback.
- Current implementation: wrappers in `src/backend/get_review_period/main.py`,
  `src/backend/create_review_sheet/main.py`, and `src/backend/store_attend_records/main.py` delegate to
  `src/attendance_etl/functions/*.py`.
- Input: base64-encoded Google Pub/Sub event payloads.
- Output: new review-period messages, new review-sheet messages, Google Sheets updates, and last-stored
  timestamp messages.
- Limitation: handlers parse expected JSON keys directly and do not currently enforce the canonical attendance
  event schema.

### 6. Persistence / Review Output

The current persistence path is Google Sheets attendance review output.

- Purpose: store raw attendance records and update reviewer-facing daily and weekly attendance worksheets.
- Current implementation: `store_attendance_records()`, `update_daily_attendance()`, and
  `update_weekly_summary()` in `src/attendance_etl/functions/store_attend_records.py`.
- Input: attendance record payloads, review sheet ID, device identifier, and review start date.
- Output: appended raw-data rows, updated daily attendance rows, updated weekly summary rows, and a
  last-stored timestamp response.
- Limitation: Google Sheets is the current implemented persistence and review surface. PostgreSQL appears only
  as future storage in the architecture diagram.

## Validation And Canonicalisation

Current runtime validation is limited to operational checks and expected field access. Examples include timestamp filtering in the ingestion client, date parsing for review periods, duplicate avoidance based on the latest stored timestamp, and worksheet existence handling in Google Sheets workflows.

The canonical attendance event is the intended internal payload contract. It is documented separately, but runtime code does not yet validate or require all canonical fields before publication or backend processing. Future adoption should make canonicalisation explicit before event publication and add schema enforcement at the appropriate boundary.

## Current Limitations

- Runtime attendance payloads still use transitional fields rather than the full canonical attendance event.
- Schema validation for published attendance events is not implemented.
- The ingestion path is still ZKTeco-specific.
- Google Sheets is the current implemented persistence and review output; PostgreSQL appears only as
  future storage in the architecture documentation.
