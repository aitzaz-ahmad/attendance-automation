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
| 1. Device polling / extraction | `attendance_etl.devices.zkteco_device`, orchestrated by `attendance_etl.pi4.workflow` and available through `src/pi4/pi4_client.py` compatibility wrapper | ZKTeco biometric device users and attendance records | `ExtractedBiometricData` with raw device users and attendance records | Implemented for the current ZKTeco device path |
| 2. Raw record normalisation | `attendance_etl.transform.zkteco_transformation_strategy` | `ExtractedBiometricData` and review timestamp range supplied through `TransformationRequest` | `NormalisedAttendance` records with composed `Employee` and project-owned `EventType` values | Implemented as device-specific normalisation |
| 3. Canonicalisation and filtering | `attendance_etl.transform.transformation_strategy` | Normalised source/device records and `TimeRange` | Filtered `AttendanceEvent` objects | Implemented by the transformation strategy template method |
| 4. Event publication | `attendance_etl.messaging.pubsub`, orchestrated by `attendance_etl.pi4.workflow` | Review-period requests, review-sheet requests, and attendance record payloads | JSON messages on Google Pub/Sub topics | Implemented with transitional attendance record payloads |
| 5. Backend / serverless processing | Google Cloud Functions deployment wrappers under `src/backend/*/main.py`, delegating to `attendance_etl.functions` | Google Pub/Sub event payloads | Review-period responses, review-sheet metadata, stored attendance updates, and last-stored timestamps | Implemented for current review-period, review-sheet, and attendance-record workflows |
| 6. Persistence / review output | `src/attendance_etl/functions/store_attend_records.py` and Google Sheets APIs | Attendance record messages and review sheet metadata | Google Sheets raw data, daily attendance, weekly summary, and last-stored timestamp response | Implemented for Google Sheets attendance review output |

## Stage Details

### 1. Device Polling / Extraction

The ingestion client polls the ZKTeco biometric device for users and attendance records. The legacy
Raspberry Pi 4 entry point remains available as `src/pi4/pi4_client.py` and delegates through
`attendance_etl.ingestion.client` to the Pi runtime modules.

- Purpose: collect device users and attendance punches from the biometric source.
- Current implementation: `ZKTecoDevice.extract_biometric_data()` connects to the ZKTeco device, disables it
  during reads, fetches users and attendance records, then re-enables and disconnects.
- Input: configured `site_id`, device vendor, and vendor-specific connection options.
- Output: `ExtractedBiometricData` containing raw user records and raw attendance records from the device
  library.
- Limitation: the current extraction path is ZKTeco-specific and still tied to the device-oriented ingestion
  workflow.

### 2. Raw Record Normalisation

The workflow supplies raw biometric data to `TransformationStrategy.transform(...)`. ZKTeco-specific
normalisation is performed by `ZKTecoTransformationStrategy.normalise(...)`; timestamp filtering is performed
by `TransformationStrategy.filter(...)`.

- Purpose: correlate raw ZKTeco attendance records with raw ZKTeco users and convert ZKTeco punch values into
  project-owned attendance event types.
- Current implementation: `ZKTecoTransformationStrategy.normalise(...)` uses a local temporary lookup for user
  correlation, and `TransformationStrategy.filter(...)` applies the timestamp range.
- Input: raw device users, raw attendance records, review start timestamp, optional effective end timestamp,
  and configured `site_id`.
- Output: filtered `AttendanceEvent` objects.

### 3. Canonicalisation And Filtering

Canonicalisation converts normalised attendance into the documented
[canonical attendance event](contracts/canonical-attendance-event.md) internal model.

- Purpose: provide a stable internal event shape for downstream publication, processing, and future storage.
- Current status: the canonical `AttendanceEvent` model is produced by transformation and then serialised by
  `AttendanceEvent.to_dict()` for the current backend-compatible Pub/Sub payload.
- Input: normalised source/device records.
- Output: canonical internal fields `site_id`, `employee`, `event_type`, and `timestamp`.
- Limitation: runtime publishing still sends backend-compatible attendance record dictionaries produced by
  `AttendanceEvent.to_dict()`, not a richer future canonical wire payload.

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
- Input: attendance record payloads, review sheet ID, `site_id`, and review start date.
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
