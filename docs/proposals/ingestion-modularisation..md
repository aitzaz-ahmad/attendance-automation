# ETLP-21 Ingestion Modularisation Plan

## Summary

Refactor the current monolithic ingestion client into focused modules while preserving runtime behavior, Pub/Sub payloads/topics, snapshot semantics, timeout/retry behavior, and the `src/pi4/pi4_client.py` compatibility entry point. Do not adopt the canonical attendance event contract in ETLP-21; keep current transitional record dictionaries.

## Current Responsibility Map

| Area | Current Owner | Current Responsibility |
| --- | --- | --- |
| Pi entry point | `src/pi4/pi4_client.py` | Adds `src` to `sys.path`, delegates to `attendance_etl.ingestion.client.main()` |
| Device access | `attendance_etl.ingestion.client` | ZKTeco connect/disable/read/clear/enable/disconnect |
| Record transform | `attendance_etl.ingestion.client` | User map, timestamp filtering, `username`/`timestamp`/`entry`/`device` dictionaries |
| Messaging | `attendance_etl.ingestion.client` | Pub/Sub publish, subscription creation, targeted pull loop, ACK handling |
| Local storage | `attendance_etl.ingestion.client` | Reads/writes `snapshot.json`, `review_period.json`, `biometric_device_config.json` |
| Pi state/FSM | `attendance_etl.ingestion.client` | State constants, flags, globals, handler table, transitions, sleeps |
| Runtime bootstrap | `attendance_etl.ingestion.client` | Loads config/review period/snapshot, then runs endless dispatch loop |

## Proposed Package Structure

```text
attendance_etl/
├── device/
│   ├── __init__.py
│   ├── interfaces.py        # AttendanceDevice protocol only
│   └── zkteco.py            # ZKTecoDevice and device timeout
├── transform/
│   ├── __init__.py
│   └── zkteco_records.py    # Stateless current-format decoding/filtering
├── messaging/
│   ├── __init__.py
│   └── pubsub.py            # Pub/Sub constants and PubSubMessenger
├── storage/
│   ├── __init__.py
│   ├── snapshot.py          # SnapshotStore
│   └── review_period.py     # ReviewPeriodStore
├── pi4/
│   ├── __init__.py
│   ├── runtime.py           # PiRuntime composition/bootstrap/run loop
│   ├── state.py             # State IDs, flags, PiState
│   └── workflow.py          # IngestionWorkflow and FSM handlers
└── ingestion/
    └── client.py            # Compatibility facade
```

## Proposed Classes And Interfaces

| Symbol | Module | Responsibility |
| --- | --- | --- |
| `AttendanceDevice` | `device.interfaces` | Python 3.8-compatible `Protocol` with `pull_records()` and `clear_records()`; no vendor registry |
| `ZKTecoDevice` | `device.zkteco` | Owns ZKTeco connection lifecycle and preserves `ZK(... timeout=10, force_udp=False, ommit_ping=False)` behavior |
| `PubSubMessenger` | `messaging.pubsub` | Owns publish, subscription existence/creation, targeted sync pull, ACK behavior, constants |
| `SnapshotStore` | `storage.snapshot` | Reads/writes existing `snapshot.json` keys exactly: `pi4_state`, `sys_flags`, `sheet_id`, `last_stored_timestamp` |
| `ReviewPeriodStore` | `storage.review_period` | Reads/writes existing review-period JSON unchanged |
| `PiState` | `pi4.state` | Replaces mutable module globals with one runtime state object |
| `IngestionWorkflow` | `pi4.workflow` | Owns transition rules, handler table, review-period timing policy, and record relay orchestration |
| `PiRuntime` | `pi4.runtime` | Composition root: load config, instantiate concrete device/messenger/stores/workflow, run forever |
| Stateless functions | `transform.zkteco_records` | Keep `convert_to_map`, `convert_to_dict`, `decode_zk_format`, `filter_records`; no class |

## Function-To-Module Migration Table

| Current Function(s) | Target |
| --- | --- |
| `clear_records_from_device`, `pull_records_from_device` | `ZKTecoDevice.clear_records`, `ZKTecoDevice.pull_records`; facade wrappers may remain |
| `convert_to_map`, `convert_to_dict`, `decode_zk_format`, `filter_records` | `transform.zkteco_records` unchanged in behavior |
| `get_attendance_records` | `IngestionWorkflow.get_attendance_records`, using `AttendanceDevice` plus transform functions |
| `review_period_expired`, `wait_duration_before_next_pull` | `pi4.workflow` pure helpers or workflow methods, preserving Friday-to-Sunday extension and local `datetime.today()` behavior |
| `publish_message_to_topic`, `subscription_exists`, `create_subscription`, `is_directed_to_me`, `sync_pull_message` | `PubSubMessenger` methods |
| `save_review_period` | `ReviewPeriodStore.save` called by workflow |
| `capture_snapshot` | `SnapshotStore.save` called only through workflow transition policy |
| `transition_state` | `IngestionWorkflow.transition_state` |
| All `handler_*` functions | Bound methods on `IngestionWorkflow` |
| `setup_device_info`, `setup_review_period_info`, `setup_handler_table`, `load_snapshot` | `PiRuntime.bootstrap` plus store/state helpers |
| `bootstrap_pi4`, `run`, `main` | `PiRuntime.bootstrap`, `PiRuntime.run_forever`, facade `main()` |

## Dependency Graph

```text
src/pi4/pi4_client.py
  -> attendance_etl.ingestion.client
    -> attendance_etl.pi4.runtime
      -> attendance_etl.device.zkteco
      -> attendance_etl.messaging.pubsub
      -> attendance_etl.storage.snapshot
      -> attendance_etl.storage.review_period
      -> attendance_etl.pi4.workflow
        -> attendance_etl.pi4.state
        -> attendance_etl.device.interfaces
        -> attendance_etl.transform.zkteco_records
```

Dependency rules: transform imports no infrastructure; device/messaging/storage do not import workflow; runtime is the only place that chooses `ZKTecoDevice`; workflow depends on the `AttendanceDevice` seam, not a multi-vendor abstraction.

## Behaviour-Preservation Risks

- Do not change Pub/Sub topic names, subscription format, project ID, ACK deadline, TTL, max messages, or `PULL_MSG_TIMEOUT`.
- Preserve current JSON payload shapes and date formats, especially attendance records with `username`, `timestamp`, `entry`, and `device`.
- Preserve snapshot policy: write snapshots only for non-waiting states, using the same file name and keys.
- Preserve relative file-path behavior for `snapshot.json`, `review_period.json`, and `biometric_device_config.json`.
- Preserve current filtering boundaries: `record.timestamp > from_timestamp` and `<= to_timestamp` when an upper bound exists.
- Preserve retry behavior: Pub/Sub waits repeat bounded pulls until a targeted message arrives.
- Characterize before moving the existing quirk where `handler_await_review_period` assigns `FINAL_ALARM_RAISED` as a next state value; do not silently “fix” it during modularisation.
- Characterize current no-record relay behavior: `RELAY_ATTENDANCE_RECORDS` remains in the same state when no new records are found.
- Do not introduce canonical attendance event fields, validation, schema versioning, or backend payload changes.

## Recommended Implementation Phases

1. Add characterization tests around pure transforms, review-period timing, snapshot transition rules, and Pub/Sub behavior using fakes/mocks.
2. Extract stateless transform functions and constants first; keep facade imports/wrappers in `attendance_etl.ingestion.client`.
3. Extract `ZKTecoDevice` and `PubSubMessenger` without changing call order, constructor arguments, topics, or payload serialization.
4. Introduce `PiState`, stores, `IngestionWorkflow`, and `PiRuntime`; migrate handlers as bound methods while preserving the state table.
5. Reduce `attendance_etl.ingestion.client` to the compatibility facade and keep `src/pi4/pi4_client.py` delegating to it.
6. Update architecture/data-pipeline/reliability docs only to describe the new module ownership; do not document canonical adoption as implemented.
7. Run required validation and produce an acceptance-criteria audit.

## Acceptance Criteria For Actual Refactor

- `src/pi4/pi4_client.py` remains a working compatibility entry point delegating to `attendance_etl.ingestion.client.main()`.
- Existing Pub/Sub topics, subscription names, message payload keys, record fields, timestamp formats, sleep durations, timeout values, and retry loops are unchanged.
- Snapshot and review-period file semantics are unchanged, including checkpoint exclusion for waiting states.
- The new package boundaries match this plan and avoid vendor-generalization beyond `AttendanceDevice`.
- Canonical attendance event adoption is not implemented in ETLP-21.
- Focused tests cover transform behavior, workflow transition/snapshot policy, review-period timing, and Pub/Sub wrapper behavior with mocked clients.
- `./scripts/validate.sh` passes before reporting completion; any targeted pytest command added for ETLP-21 also passes.
- Final implementation report includes a PASS/FAIL acceptance-criteria audit and identifies any deliberately preserved legacy quirks.
