# Reliability Model

## Overview

The ingestion client is a long-running, stateful workflow for collecting attendance records from a ZKTeco
biometric device and publishing work through Google Pub/Sub. The current runtime compatibility entry point is
`src/pi4/pi4_client.py`, which delegates to the canonical implementation in
`attendance_etl.ingestion.client`, then to the Pi runtime modules under `attendance_etl.pi4`.

This reliability model is intentionally limited to the behavior currently visible in the implementation and
repository diagrams. It should not be read as a production-grade fault-tolerance or exactly-once delivery
guarantee.

## Stateful Ingestion Workflow

The ingestion client models the device workflow as a state machine in `attendance_etl.pi4.workflow`. The main
loop dispatches the handler for the current Pi state, and each handler moves the client to the next state with
`transition_state()`.

The implemented state handlers cover the following workflow:

- `FETCH_REVIEW_PERIOD` publishes a request for the next review period.
- `AWAIT_REVIEW_PERIOD` waits for a targeted review-period response from Google Pub/Sub.
- `REQUEST_REVIEW_SHEET` publishes a request to create the review sheet for the active review period.
- `AWAIT_REVIEW_SHEET` waits for the review-sheet response and stores the returned sheet ID.
- `RELAY_ATTENDANCE_RECORDS` sleeps until the next polling interval, reads attendance records from the
  device, filters records after the review start or last stored timestamp, and publishes new records.
- `AWAIT_LAST_STORED_TIMESTAMP` waits for the backend response containing the latest stored timestamp.
- `REVIEW_PERIOD_EXPIRED` checks for unsaved records before clearing attendance records from the device and
  returning to review-period lookup.
- `RAISE_FINAL_ALARM` and `NO_REVIEW_PERIOD` represent the missing-review-period recovery path. The alarm
  notification itself is still marked as a TODO in code; the alarm handler records the alarm flag before the
  deep-sleep retry cycle.

The client uses `POLLING_DELAY` of 15 minutes for normal attendance polling and `DEEP_SLEEP_DURATION` of one
hour when review metadata is unavailable. The finite state machine diagram is referenced from the README as
`docs/diagrams/pi4-client-fsm.png`.

## Snapshot Persistence

Snapshot persistence lets the ingestion client resume after process interruption or power loss without
starting from an empty in-memory state. The current snapshot file is `snapshot.json`, loaded during
`bootstrap_pi4()` through `attendance_etl.storage.snapshot`.

The implementation persists these fields:

- `pi4_state`: the current client state.
- `sys_flags`: system flags such as `FINAL_ALARM_RAISED`.
- `sheet_id`: the active review sheet ID.
- `last_stored_timestamp`: the latest attendance timestamp confirmed by the backend.

Snapshots are captured by `capture_snapshot()` when `transition_state()` enters a non-waiting state. The
client deliberately does not checkpoint `AWAIT_REVIEW_PERIOD`, `AWAIT_REVIEW_SHEET`, or
`AWAIT_LAST_STORED_TIMESTAMP`. This avoids restarting directly inside a Pub/Sub waiting state after shutdown,
where the corresponding request or response may already have moved on.

The review-period metadata is stored separately in `review_period.json` through
`attendance_etl.storage.review_period`, and device connection settings are loaded from
`biometric_device_config.json`.

## Startup Configuration Invariant

The ingestion client is configuration-driven.

A valid biometric device configuration file is required before runtime startup
can proceed.

Startup must fail fast if:

- the configuration file is missing
- the configuration file is unreadable
- the configuration file contains invalid JSON
- required fields are missing
- the vendor is unsupported
- vendor-specific options are invalid

The runtime must not instantiate or communicate with a biometric device when
configuration loading or validation fails.

Configuration loading and validation are separate from concrete device
construction. The configuration builder or equivalent loader owns reading JSON
and producing a valid `BiometricDeviceConfig`; `BiometricDeviceFactory` owns
constructing the concrete `BiometricDevice` from that validated configuration.

Required root biometric device configuration fields are `site_id`, `vendor`,
and `device_options`. `site_id` identifies the office, site, or location from
which attendance records are extracted. It is deployment/domain metadata and is
independent of vendor selection and vendor-specific connection metadata.

For ZKTeco devices, `device_options` contains ZKTeco-specific connection options
such as `ip_address`, `comm_port`, `timeout`, `force_udp`, and `ommit_ping`.

## Timeout-Based Recovery

Pub/Sub response waits are bounded at the individual pull-call level by `PULL_MSG_TIMEOUT`, currently 30
seconds. `sync_pull_message()` performs repeated bounded pulls until it receives a message
targeted to the configured `site_id`, then acknowledges received messages and returns the
decoded JSON payload.

The state machine also uses sleep and retry paths to avoid tight loops:

- Normal attendance relay waits for `POLLING_DELAY` before polling the device again.
- When review-period metadata is missing, the `NO_REVIEW_PERIOD` path sleeps for `DEEP_SLEEP_DURATION` before
  returning to `FETCH_REVIEW_PERIOD`.
- After restart, snapshot recovery restores the client to the last checkpointed non-waiting state, allowing
  the request, relay, or recovery path to be re-entered explicitly.

The current implementation does not persist a separate timeout counter, retry budget, or transactional
message-delivery marker. Backend responses that are delayed or missing keep the client in repeated bounded
pulls until a targeted response arrives, while the documented FSM includes timeout and recovery paths around
review-period lookup and sleep/retry behavior.

## Failure Scenarios And Mitigations

| Failure scenario | Mitigation |
| --- | --- |
| Power loss / process interruption | Snapshot-based resume from `snapshot.json`, using the last checkpointed non-waiting state. |
| Backend response missing or delayed | Bounded Pub/Sub pull timeout with repeated waits; missing review-period metadata can move into the alarm and deep-sleep retry path. |
| Missing, unreadable, malformed, unsupported, or invalid biometric device configuration | Runtime startup fails fast before concrete device construction or device communication. |
| Missing review metadata | `RAISE_FINAL_ALARM` / `NO_REVIEW_PERIOD` path is documented for recording the alarm flag and retrying review-period lookup after deep sleep; notification dispatch remains TODO. |
| Device polling failure | Device access is wrapped in `try` / `finally`; when a connection exists, the client disconnects during cleanup. Successful polling disables the device during reads and re-enables it before cleanup. |
| Duplicate/replayed records | Attendance records are filtered after the review start or `last_stored_timestamp`, and the backend returns the latest stored timestamp after storage. |

## Current Limitations

- The reliability behavior is implemented under `attendance_etl.pi4.workflow`; the compatibility facade remains
  at `attendance_etl.ingestion.client`.
- The final alarm notification path is present but not fully implemented.
- Pub/Sub waits use bounded pull calls, but the await handlers continue retrying until a targeted message is
  received.
- Snapshot persistence records workflow metadata, not a transactional guarantee for Google Pub/Sub or Google
  Sheets writes.
- Runtime attendance payloads still use transitional dictionaries rather than the full canonical attendance
  event contract.
