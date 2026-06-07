# ETLP-21 Ingestion Modularisation Plan

## Summary

Refactor the current monolithic ingestion client into focused modules while preserving runtime behavior, Pub/Sub payloads/topics, snapshot semantics, timeout/retry behavior, and the `src/pi4/pi4_client.py` compatibility entry point. Do not adopt the canonical attendance event contract in ETLP-21; keep current transitional record dictionaries.

## Status

Implemented with later corrections.

This proposal records the original ETLP-21 implementation plan. Some proposed abstractions were later rejected or superseded by ADR-0001 and ADR-0002.

## Superseded Planning Notes

The following names appeared in the original ETLP-21 proposal but are no longer accepted implementation targets:

- `AttendanceDevice`: deferred to Milestone 3 — Device Layer Abstraction.
- `SnapshotStore`: rejected during ETLP-24; use `load_snapshot()` / `save_snapshot(RuntimeState)`.
- `ReviewPeriodStore`: rejected during ETLP-24; use `load_review_period()` / `save_review_period(ReviewPeriod)`.

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
│   ├── interfaces.py        # Original proposal; device abstraction deferred to Milestone 3
│   └── zkteco.py            # ZKTecoDevice and device timeout
├── transform/
│   ├── __init__.py
│   └── zkteco_records.py    # Stateless current-format decoding/filtering
├── messaging/
│   ├── __init__.py
│   └── pubsub.py            # Pub/Sub constants and PubSubMessenger
├── storage/
│   ├── __init__.py
│   ├── snapshot.py          # Function-based RuntimeState persistence
│   └── review_period.py     # Function-based ReviewPeriod persistence
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
| `AttendanceDevice` | `device.interfaces` | Original proposal; later deferred to Milestone 3 — Device Layer Abstraction |
| `ZKTecoDevice` | `device.zkteco` | Owns ZKTeco connection lifecycle and preserves `ZK(... timeout=10, force_udp=False, ommit_ping=False)` behavior |
| `PubSubMessenger` | `messaging.pubsub` | Owns publish, subscription existence/creation, targeted sync pull, ACK behavior, constants |
| `Snapshot persistence helpers` | `storage.snapshot` | Superseded by ETLP-24; function-based load_snapshot()/save_snapshot(RuntimeState) persistence |
| `ReviewPeriod persistence helpers` | `storage.review_period` | Superseded by ETLP-24; function-based load_review_period()/save_review_period(ReviewPeriod) persistence |
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
| `save_review_period` | `save_review_period(ReviewPeriod) called by workflow` |
| `capture_snapshot` | `save_snapshot(RuntimeState) called only through workflow transition policy` |
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
        -> attendance_etl.transform.zkteco_records
```

Note: the original proposal referenced an `AttendanceDevice` abstraction. This was later deferred to Milestone 3 — Device Layer Abstraction and should not be treated as implemented architecture.

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
4. Introduce PiState, persistence helpers, IngestionWorkflow, and PiRuntime; migrate handlers as bound methods while preserving the state table.
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

## ETLP-24 Review Findings

### Summary

The initial ETLP-24 implementation successfully introduced the required domain model classes but did not fully satisfy the acceptance criterion:

    Models are used as shared data contracts between modules.

Several models were created without fully replacing the existing dictionary-based contracts used throughout the ingestion pipeline.

### Model Adoption Principle

Creating a model is not sufficient.

A model migration is considered complete only when internal module boundaries exchange model instances rather than dictionaries.

Dictionaries should exist only at:

- persistence boundaries
- serialization boundaries
- deserialization boundaries
- external integration boundaries

### Serialization Contracts

Domain models should implement:

    ISerializable
    IDeserializable

ISerializable:

    to_dict() -> Dict[str, Any]

IDeserializable:

    from_dict(payload: Dict[str, Any])

Models own serialization and deserialization.

Persistence components own file I/O.

### AttendanceEvent

AttendanceEvent has been introduced but is not yet fully adopted throughout the ingestion pipeline.

ETLP-24 should:

- define AttendanceEvent ownership
- define AttendanceEvent serialization semantics
- define AttendanceEvent deserialization semantics

ETLP-24 should not prematurely consume Transformation Layer scope.

Full canonical attendance-event adoption is deferred.

### Employee

Employee should become the canonical representation of a device user.

Preferred representation:

    Dict[user_id, Employee]

Employee objects should remain alive throughout the ingestion workflow rather than being flattened back into dictionaries or primitive values.

### RuntimeState

RuntimeState should become the canonical representation of persisted ingestion runtime state.

RuntimeState owns:

- serialization
- deserialization

Snapshot persistence owns:

- file I/O
- storage mechanics

Target API:

    load_snapshot() -> RuntimeState
    save_snapshot(RuntimeState)

### ReviewPeriod

ReviewPeriod should become the canonical representation of review-period information.

ReviewPeriod owns:

- serialization
- deserialization

Review-period persistence owns:

- file I/O
- storage mechanics

Target API:

    load_review_period() -> ReviewPeriod
    save_review_period(ReviewPeriod)

### Anti-Patterns Identified

The following are considered migration anti-patterns:

- creating models and immediately converting them back into dictionaries
- preserving old dictionary contracts internally after introducing models
- introducing compatibility helpers that become the primary code path
- introducing architectural abstractions not justified by the roadmap, ticket, or approved design review

### Remaining ETLP-24 Work

The following work remains within ETLP-24 scope:

#### Phase A — Model Contract Foundation

Objectives:

- Introduce ISerializable
- Introduce IDeserializable
- Review model field ownership
- Define explicit serialization contracts

Applies to:

- AttendanceEvent
- Employee
- ReviewPeriod
- RuntimeState

#### Phase B — RuntimeState And ReviewPeriod Adoption

Objectives:

- Remove internal RuntimeState dictionaries
- Remove internal ReviewPeriod dictionaries
- Use RuntimeState and ReviewPeriod as shared data contracts

#### Phase C — Employee Adoption

Objectives:

Replace:

    Dict[user_id, str]

with:

    Dict[user_id, Employee]

throughout the ingestion client.

#### Phase D — AttendanceEvent Preparation

Objectives:

- Finalise AttendanceEvent serialization semantics
- Finalise AttendanceEvent deserialization semantics
- Prepare AttendanceEvent for future canonical-pipeline adoption

### Deferred Work

The following work is intentionally deferred because dedicated roadmap items already exist.

#### Device Abstraction Evolution

Milestone:

    3. Device Layer Abstraction

Relevant Issues:

- Create Device Interface
- Implement ZKTeco Adapter
- Implement Device Factory
- Move Extraction Logic

#### Canonical Attendance Event Adoption

Milestone:

    4. Transformation Layer

Relevant transformation capabilities:

- Introduce transformation layer
- Introduce canonical attendance models
- Introduce validation pipeline
- Introduce normalisation pipeline

Reason:

Canonical attendance-event adoption belongs to the Transformation Layer milestone and should not be consumed by ETLP-24.

#### Messaging Abstraction Evolution

Milestone:

    5. Messaging Abstraction

Relevant Issues:

- Create Messaging Interface
- Implement Pub/Sub Adapter
- Move Messaging Logic
- Implement Retry Handling

Reason:

Messaging redesign belongs to the Messaging Abstraction milestone and should not be consumed by ETLP-24.

### ETLP-24 Exit Criteria

ETLP-24 should only be considered complete once:

- RuntimeState is the runtime persistence contract.
- ReviewPeriod is the review-period persistence contract.
- Employee is the user identity contract.
- Model serialization/deserialization contracts are defined.
- Models are used as shared data contracts between modules.
