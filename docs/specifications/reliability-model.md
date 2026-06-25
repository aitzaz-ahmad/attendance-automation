# Reliability Model Specification

## Status

Accepted

## Lifecycle

Active

## Overview

This specification defines how reliability works across the ingestion client,
messaging boundary, workflow/FSM boundary, local persistence, transformation,
device access, backend processing, and storage side effects.

This document is not an ADR. ADR-0004, ADR-0005, and ADR-0006 are accepted and
authoritative. If current implementation behaviour contradicts those ADRs, the
ADRs define the target model.

The current implementation is best-effort. It uses checkpoint-assisted restart
from the last persisted non-waiting state and partial duplicate suppression
through timestamp filtering and last-stored-timestamp feedback. These mechanisms
reduce duplicate work after interruption, but they do not provide exactly-once
transport delivery or exactly-once business processing.

The target reliability model separates transport delivery from business effects:

- transport delivery is at-least-once;
- business effects target effectively-once behaviour through idempotent
  processing;
- ACK is broker-facing and confirms transport acceptance only;
- durable business effects must be protected by workflow, FSM, persistence,
  backend, and idempotency rules above the transport boundary.

## Reliability Layer Model

```text
+-------------------------------+
| FSM                           |
| retry / wait / recovery       |
+-------------------------------+
                |
                v
+-------------------------------+
| Workflow / Action Services    |
| execute one business attempt  |
| emit outcomes                 |
+-------------------------------+
        |      |       |
        |      |       |
        v      v       v
+-----------+ +-------------+ +-----------+
| Device    | | Persistence | | Messenger |
+-----------+ +-------------+ +-----------+
                                   |
                                   v
                        +------------------+
                        | Google Pub/Sub   |
                        +------------------+
                                   |
                                   v
                        +------------------+
                        | Backend Services |
                        +------------------+
                                   |
                                   v
                        +------------------+
                        | Google Sheets    |
                        +------------------+
```

Business retry belongs above `Messenger`. A messenger implementation may own
bounded transport retry for send operations, but it must not hide workflow
progress retry, receive-timeout retry, wait policy, or recovery policy.

## Terminology

| Term | Meaning |
| --- | --- |
| Transport operation | A broker-facing operation such as publish, receive, decode, route, or ACK. |
| Business attempt | One workflow/action-service attempt to perform a business operation, such as requesting a review period, requesting a review sheet, publishing attendance records, or awaiting a backend response. |
| Transport-valid message | A message that decodes successfully, has a mapping payload, and satisfies required routing metadata for its topic. |
| Business-valid response | A transport-valid payload that satisfies the expected business contract for the workflow action consuming it. |
| Receive timeout | A bounded receive attempt that completes without an accepted message before its total timeout expires. |
| Waiting state | A state in which the client is waiting for an external response and must not persist that state as a restart target. |
| Non-waiting state | A state that can safely be checkpointed after its associated business attempt completes successfully. |
| Checkpoint | Persisted runtime state used to resume after process interruption or power loss. |
| Effectively-once business effect | A target property where duplicate delivery or repeated workflow attempts do not create incorrect durable effects. |
| Poison message | A malformed or transport-invalid message that may redeliver repeatedly until dead-letter handling is designed. |

## Reliability Principles

1. Explicit ownership boundaries

   Each layer owns only its own reliability concerns. Messenger owns transport
   mechanics. Workflow/action services execute business attempts and translate
   outcomes. The FSM owns transition, retry, wait, recovery, and failure
   decisions. Persistence owns serialisation and file I/O. Backend handlers own
   backend processing and storage side effects.

2. Visible failures

   Reliability failures must be surfaced as explicit exceptions, workflow
   outcomes, FSM events, or logged backend failures. Implementations must not
   silently convert failed operations into successful outcomes or hidden
   fallback behaviour.

3. Bounded operations

   Transport operations that can block workflow progress must be bounded.
   `Messenger.receive(...)` requires a finite total timeout. Concrete messenger
   implementations may perform provider-specific loops inside that timeout, but
   must not hide indefinite receive retry. Send retry must also be finite.

4. Recoverability

   The Pi4 workflow must recover from interruption by restarting from the last
   checkpointed non-waiting state. Waiting states must not be persisted as
   restart targets.

5. Idempotency over exactly-once transport

   The system must tolerate duplicate transport delivery and repeated workflow
   attempts. The project must not claim exactly-once transport. Business
   correctness comes from idempotency and duplicate-safe durable effects above
   the broker boundary.

## Action Service Responsibilities

An action service owns one business attempt.

An action service may:

- invoke `Messenger` operations;
- invoke device operations;
- invoke persistence operations;
- invoke transformation operations;
- validate business responses;
- translate results into workflow outcomes.

An action service must not:

- implement transport retry policy;
- implement workflow retry policy;
- perform state transitions;
- decide checkpoint eligibility.

## Transport Exceptions

The messaging abstraction must define these project-owned transport failures.

### `MessageReceiveTimeout`

`MessageReceiveTimeout` is raised when one bounded receive attempt completes
without an accepted message before the supplied total timeout expires.

This is a transport outcome, not a business outcome. Workflow/action services
must translate it into workflow outcomes that the FSM can use for retry, wait,
recovery, or failure decisions.

### `MessageSendFailed`

`MessageSendFailed` is raised when a concrete `Messenger` exhausts configured
send retry attempts.

It is raised after bounded retry/backoff fails. Workflow/action services must
translate it into workflow outcomes that the FSM can use for retry, recovery, or
failure decisions.

Additional exception taxonomy remains deferred beyond these two project-owned
failures.

## Retryable And Non-Retryable Failures

Not all failures are retryable.

Transient transport failures may be retried.

Deterministic failures such as invalid configuration, unsupported device
implementations, corrupt payload contracts, malformed business requests, or
invalid local state should be surfaced immediately to workflow/FSM without
transport retry.

Retrying deterministic failures is not considered a reliability mechanism.

## Reliability Ownership Matrix

| Item | Owner | Responsibility | Non-responsibilities |
| --- | --- | --- | --- |
| Transport retry | Concrete `Messenger` implementation | Retry transient transport send failures with finite backoff and explicit exhaustion failure. | Workflow retry, receive timeout retry, FSM transition policy, business validation. |
| Receive timeout | `Messenger` detects; workflow/FSM reacts | Enforce finite total receive timeout and report `MessageReceiveTimeout` when no accepted message arrives. | Deciding whether to retry, resend, wait, recover, or fail after timeout. |
| Workflow retry | FSM policy, executed by workflow/action services | Re-enter a business action when the FSM decides another attempt is allowed. | Hidden loops inside messenger adapters or action services. |
| FSM retry | FSM | Decide retry, wait, recovery, compensation, and terminal failure transitions. | Transport calls, persistence file I/O, device extraction, backend storage writes. |
| Checkpointing | FSM for eligibility; workflow/runtime for timing; storage for file I/O | Classify checkpoint-eligible states, coordinate checkpoint writes after successful business completion, and persist checkpoint data only for eligible states. | Persisting waiting states as restart targets; checkpointing before business completion; deciding transport ACK policy. |
| Persistence | `attendance_etl.storage` and backend storage integrations | Serialise, load, and save local runtime/review-period state; perform backend durable writes where applicable. | Transition policy, retry budgets, routing validation, business workflow progress. |
| Routing validation | Concrete `Messenger` implementation | Apply topic-aware broadcast/targeted routing policy and required transport metadata validation. | Business payload validation or review-period/review-sheet semantics. |
| Business validation | Workflow/action services and domain-specific layers | Validate response payload meaning, transformation output, backend request contracts, and action preconditions. | ACK policy, broker redelivery mechanics, provider-specific routing. |
| Idempotency | Business and storage layers | Prevent duplicate transport delivery or repeated attempts from creating incorrect durable effects. | Transport duplicate suppression or exactly-once broker delivery. |
| Dead-letter handling | Deferred reliability design | Define poison-message counters, DLQ routing, redrive, and operator handling. | Required implementation in ETLP-33 through ETLP-36. |

## Failure Classification

| Category | Failure | Detection layer | Owner | Recovery mechanism |
| --- | --- | --- | --- | --- |
| Transport | Send failure | Concrete `Messenger.send(...)` implementation | Concrete messenger adapter | Retry transient send failures with bounded backoff; raise `MessageSendFailed` after exhaustion. FSM/workflow decide business recovery. |
| Transport | Receive timeout | `Messenger.receive(...)` | Messenger detects; workflow/FSM owns reaction | Raise `MessageReceiveTimeout` after the finite total timeout. Workflow/action service emits an outcome; FSM decides retry, wait, recover, or fail. |
| Transport | Routing failure | Concrete messenger routing validation | Concrete messenger adapter | For targeted topics, accept matching messages and ACK valid non-target messages. Do not ACK messages missing required routing metadata. DLQ handling is deferred. |
| Transport | Malformed message | Concrete messenger transport validation | Concrete messenger adapter | Do not ACK malformed JSON or non-mapping payloads. Message may redeliver until poison-message/DLQ support is designed. |
| Workflow | Invalid response | Workflow/action service | Workflow/action service | Translate to an invalid-response outcome. FSM decides whether to re-request, retry, recover, or fail. |
| Workflow | Business validation failure | Workflow/action service or domain layer | Workflow/action service/domain layer | Reject as a business outcome, not broker redelivery. FSM decides transition. |
| Workflow | Retry exhaustion | FSM | FSM | Transition to configured failure, recovery, or operator-visible state. Exact budgets and persisted counters are deferred. |
| Persistence | Snapshot failure | `attendance_etl.storage.snapshot` during save/load | Storage layer reports; FSM/workflow reacts | Surface failure explicitly. Atomic snapshot writes are deferred. |
| Persistence | Review-period persistence failure | `attendance_etl.storage.review_period` during save/load | Storage layer reports; workflow/FSM reacts | Surface failure explicitly and prevent treating the business action as successful unless persistence completed. |
| Backend | Sheets write failure | Backend Cloud Function / Google Sheets integration | Backend processing layer | Fail visibly. Duplicate delivery or repeated invocation must be tolerated by idempotent backend/storage behaviour. Exact backend retry policy is deferred. |
| Backend | Backend processing failure | Backend Cloud Function implementation | Backend processing layer | Fail visibly before publishing success responses. Pi4 workflow may timeout and recover according to FSM policy. |
| Device | Extraction failure | Concrete `BiometricDevice` implementation | Device layer reports; workflow/FSM reacts | Surface extraction failure to the workflow/action service. FSM decides retry, wait, recover, or fail. |
| Device | Clear failure | Concrete `BiometricDevice.clear_records()` implementation | Device layer reports; workflow/FSM reacts | Surface failure explicitly. The workflow must not treat attendance records as safely cleared unless clear completed. |
| Transformation | Validation failure | `TransformationStrategy.validate(...)` | Transformation layer | Raise validation failure to workflow/action service. FSM decides failure or recovery path. |
| Transformation | Normalisation failure | Concrete transformation strategy | Transformation layer | Surface failure explicitly. Deterministic data defects should not be hidden as empty successful output unless the strategy explicitly defines that behaviour. |

## Messaging Reliability

Messenger owns transport reliability only.

The project-owned `Messenger` abstraction exposes application topics and decoded
payload mappings. Workflow code must not depend on raw Pub/Sub topic paths,
subscription names, ACK IDs, message envelopes, provider attributes, or provider
pull mechanics.

### Bounded Receive Semantics

`Messenger.receive(topic, timeout)` performs one bounded receive attempt for a
`MessageTopic`.

The timeout is a finite total timeout. If no accepted message is received before
that timeout expires, the messenger raises `MessageReceiveTimeout`.

Concrete adapters may perform provider-specific pull calls internally, but the
complete receive operation must not exceed the supplied total timeout.

The adapter must not hide an indefinite receive loop. Retries across receive
timeouts belong above the messenger boundary.

### Bounded Send Retry

Concrete messenger implementations shall retry transient send failures with
bounded backoff where the concrete transport supports retryable failure
classification.

After retry exhaustion, send must raise `MessageSendFailed`.

Send retry must be finite, configurable/testable, and visible in implementation
tests. Messenger implementations must not contain indefinite send retry loops.

### Transport Validation

Transport-level validation includes:

- JSON decoding succeeds;
- decoded payload is a mapping;
- required routing metadata exists for targeted topics.

Transport-level validation excludes:

- `ReviewPeriod` semantics;
- review-sheet payload semantics;
- attendance payload semantics;
- last-stored-timestamp business meaning;
- transformation validation;
- backend storage correctness.

Business validation belongs above the messaging transport boundary.

### ACK Semantics

ACK is broker-facing. It is not the business transaction boundary.

For broadcast response topics:

- ACK accepted broadcast messages after transport-level validation.

For targeted response topics:

- ACK accepted targeted messages after routing match and transport-level
  validation.
- ACK valid non-target targeted-topic messages so they do not block the
  subscription.

The adapter must not ACK:

- malformed JSON;
- non-mapping payloads;
- targeted messages missing required routing metadata.

Until poison-message and dead-letter handling are designed, malformed or
transport-invalid messages may redeliver according to broker behaviour.

### Routing Policy Interaction

Routing policy is topic-aware and separate from `MessageTopic`.

Broadcast response topics:

- `MessageTopic.REVIEW_PERIOD_RESPONSE`
- `MessageTopic.REVIEW_SHEET_RESPONSE`

Targeted response topics:

- `MessageTopic.LAST_STORED_TIMESTAMP_RESPONSE`

Targeted routing uses Pi4 `site_id` as the application identity. For Google
Pub/Sub, targeted routing is implemented with provider metadata such as the
internal `location` attribute, owned by `GooglePubSubMessenger`.

`TopicResolver` resolves topic names only. It must not infer or own routing
behaviour.

## Workflow Reliability

Workflow or action services execute one business attempt.

A business attempt may call transport, device, transformation, persistence, or
backend-facing operations, but it must not hide retry loops that affect workflow
progress. It translates outcomes into workflow results that the FSM can consume.

Workflow/action services own:

- invoking business side effects through project-owned abstractions;
- validating business response payloads;
- distinguishing valid empty business responses from receive timeouts;
- translating transport failures, timeouts, invalid responses, persistence
  failures, device failures, and transformation failures into explicit workflow
  outcomes;
- coordinating checkpoint timing according to FSM checkpoint eligibility.

Workflow/action services do not own:

- FSM transition policy;
- retry budgets;
- waiting-state classification;
- checkpoint eligibility;
- provider-specific transport mechanics;
- broker ACK policy.

### Example: Review-Period Request Attempt

One review-period request attempt can:

1. Send `REVIEW_PERIOD_REQUEST`.
2. Receive `REVIEW_PERIOD_RESPONSE` once with a finite timeout.
3. Interpret a valid non-empty payload as a new review period.
4. Interpret a valid empty payload as "no review period currently available".
5. Translate `MessageReceiveTimeout`, `MessageSendFailed`, malformed business
   shape, or persistence failure into explicit workflow outcomes.

The workflow action does not decide whether to resend indefinitely. The FSM
decides the next transition.

### Example: Review-Sheet Request Attempt

One review-sheet request attempt can:

1. Send `REVIEW_SHEET_REQUEST`.
2. Receive `REVIEW_SHEET_RESPONSE` once with a finite timeout.
3. Validate that required business fields such as sheet identifier and sheet
   name are present.
4. Emit success or invalid-response outcomes.

The workflow action must not depend on raw subscription names, ACK IDs, Pub/Sub
attributes, or topic paths.

### Example: Attendance Relay Attempt

One attendance relay attempt can:

1. Extract biometric data from the configured device.
2. Transform and filter attendance events after the review start or last stored
   timestamp.
3. If no new records exist, emit a no-new-records outcome.
4. If new records exist, send `ATTENDANCE_PUBLISH`.
5. Receive `LAST_STORED_TIMESTAMP_RESPONSE` once with a finite timeout.
6. Validate and store the returned timestamp.

Extraction, transformation, send, receive, timestamp validation, and timestamp
persistence failures must be explicit workflow outcomes.

## FSM Reliability

The FSM owns reliability policy for workflow progress.

The FSM owns:

- retry decisions;
- wait decisions;
- recovery decisions;
- failure transitions;
- transition validation;
- guard conditions;
- waiting-state classification;
- checkpoint eligibility.

The FSM does not own:

- transport calls;
- device extraction;
- transformation;
- record publication;
- snapshot file I/O;
- backend payload construction;
- Google Sheets writes.

The FSM may grow beyond the current state catalogue. Additional states may be
introduced to model wait, retry, recovery, invalid response, compensation, or
terminal failure paths. The target model is not constrained to the current
nine-state implementation.

### Current State Catalogue

Current Pi4 reliability behaviour is implemented under
`attendance_etl.pi4.workflow`. The Raspberry Pi compatibility entry point at
`src/pi4/pi4_client.py` delegates through `attendance_etl.ingestion.client` to
the Pi4 runtime modules.

The current workflow states are:

- `FETCH_REVIEW_PERIOD`
- `AWAIT_REVIEW_PERIOD`
- `REQUEST_REVIEW_SHEET`
- `AWAIT_REVIEW_SHEET`
- `RELAY_ATTENDANCE_RECORDS`
- `AWAIT_LAST_STORED_TIMESTAMP`
- `REVIEW_PERIOD_EXPIRED`
- `RAISE_FINAL_ALARM`
- `NO_REVIEW_PERIOD`

`RAISE_FINAL_ALARM` and `NO_REVIEW_PERIOD` represent the current
missing-review-period recovery path. Alarm notification dispatch remains a
current implementation TODO; the current alarm handler records the alarm flag
before the deep-sleep retry cycle.

### Example Flow: Receive Timeout

1. Workflow action calls `Messenger.receive(...)`.
2. Messenger raises `MessageReceiveTimeout`.
3. Workflow action emits a receive-timeout outcome.
4. FSM decides whether to retry the request, wait, recover from a prior
   checkpoint, or enter a failure state.

### Example Flow: Send Failure

1. Concrete messenger retries transient send failures with bounded backoff.
2. Messenger raises `MessageSendFailed` after exhaustion.
3. Workflow action emits a send-failed outcome.
4. FSM decides whether to retry the business attempt, move to recovery, or fail.

### Example Flow: Invalid Business Response

1. Messenger accepts and ACKs a transport-valid message.
2. Workflow action determines that the payload is business-invalid.
3. Workflow action emits an invalid-response outcome.
4. FSM decides whether to request again, wait, recover, or fail.

### Example Flow: No Review Period

1. Workflow action receives a valid empty review-period response.
2. Workflow action emits a no-review-period outcome.
3. FSM decides whether to raise the final alarm path, enter deep sleep, or retry
   review-period lookup according to its state policy.

## Checkpoint And Recovery Model

Checkpoint-assisted restart is the current recovery mechanism and remains part
of the target architecture.

A checkpoint shall only be written after successful completion of the business
attempt associated with the current non-waiting state.

Checkpointing before successful business completion is prohibited because it can
make recovery skip necessary work. For example, persisting the next state before
a request is actually sent, a response is successfully applied, or a timestamp
is safely stored can cause restart recovery to bypass required business work.

The current waiting states are:

- `AWAIT_REVIEW_PERIOD`
- `AWAIT_REVIEW_SHEET`
- `AWAIT_LAST_STORED_TIMESTAMP`

Waiting states must not be persisted as restart targets.

FSM owns checkpoint eligibility. Persistence owns serialisation and file I/O.
Workflow/runtime coordinates checkpoint timing according to FSM policy.

### Checkpoint Ownership

Checkpointing is split across three layers:

- FSM owns checkpoint eligibility.
- Workflow/runtime owns checkpoint timing.
- Persistence owns checkpoint storage.

No single layer owns checkpointing end-to-end.

At runtime startup, `Pi4Runtime` loads persisted state and composes the workflow.
Restart resumes from the last checkpointed non-waiting state, not from an
in-flight wait.

### Current Persistence Inputs

The current runtime checkpoint file is `snapshot.json`, loaded through
`attendance_etl.storage.snapshot` during Pi4 runtime bootstrap.

The snapshot persists:

- `pi4_state`: the current client state;
- `sys_flags`: system flags such as `FINAL_ALARM_RAISED`;
- `sheet_id`: the active review sheet ID; and
- `last_stored_timestamp`: the latest attendance timestamp confirmed by the
  backend.

Review-period metadata is stored separately in `review_period.json` through
`attendance_etl.storage.review_period`.

Biometric device settings are loaded from `biometric_device_config.json`.

### Startup Configuration Invariant

Runtime startup requires a valid biometric device configuration before concrete
device construction or device communication can occur. Configuration shape,
required fields, vendor-specific option ownership, and validation expectations
are defined in the
[Biometric Device Configuration specification](biometric-device-configuration.md).

From a reliability perspective, invalid biometric device configuration is a
deterministic startup failure. It must fail explicitly and visibly instead of
being retried as a transient workflow, transport, or device communication
failure.

Configuration loading and validation are separate from concrete device
construction. The configuration builder or equivalent loader owns producing a
valid `BiometricDeviceConfig`. `BiometricDeviceFactory` owns constructing the
concrete `BiometricDevice` from that validated configuration.

### Current Wait Timing Inputs

The current Pi4 workflow uses `POLLING_DELAY` for normal attendance polling and
`DEEP_SLEEP_DURATION` when review metadata is unavailable.

At the time this specification superseded the retired reliability page,
`POLLING_DELAY` was 15 minutes and `DEEP_SLEEP_DURATION` was one hour.

Those values describe current implementation timing inputs, not the full target
retry policy. Future FSM retry and wait budgets remain deferred unless a scoped
issue promotes them.

### Checkpoint Eligibility

| State class | Checkpoint eligible? | Reason |
| --- | --- | --- |
| Waiting state | No | Restarting inside a response wait may deadlock or wait for a request/response that is no longer valid. |
| Non-waiting state | Yes, after successful business completion | Re-entering the state after a completed business attempt allows the workflow to resend, recover, or continue from an explicit business boundary. |
| Future retry/recovery state | FSM-defined | The FSM must classify each new state explicitly and the workflow/runtime must coordinate timing after successful business completion. |

### Sequence Example: Crash Before Send

1. Last checkpoint is a non-waiting request state from a previously completed
   business boundary.
2. Process crashes before the next send operation begins.
3. Restart loads the same non-waiting state.
4. Workflow attempts the send.

No transport message is expected to have been published before the crash.

### Sequence Example: Crash After Send

1. Last checkpoint is a non-waiting request state.
2. Workflow sends a request successfully.
3. Workflow transitions toward a waiting state, which is not checkpointed.
4. Process crashes before a later non-waiting checkpoint.
5. Restart loads the previous request state.
6. Workflow may resend the request.

This can create duplicate transport delivery or duplicate backend work. The
target model tolerates this through at-least-once transport assumptions and
idempotent business effects.

### Sequence Example: Crash While Waiting

1. Workflow is waiting for a response in a waiting state.
2. The waiting state is not checkpointed.
3. Process crashes.
4. Restart loads the previous checkpointed non-waiting state.
5. Workflow re-enters the request or recovery path.

A previous response may still be delivered, duplicated, or no longer useful.
Transport routing and workflow business validation decide whether a received
message is accepted. The FSM decides retry or recovery.

### Sequence Example: Crash After Response

1. Messenger receives and ACKs a transport-valid response.
2. Workflow validates and applies business state.
3. Checkpointing occurs only after the associated business attempt completes
   successfully and the FSM policy marks the resulting non-waiting state as
   checkpoint eligible.
4. If the process crashes before that checkpoint, restart resumes from the
   previous checkpoint and may repeat the business attempt.
5. If the process crashes after that checkpoint, restart resumes from the new
   checkpointed state.

Repeated attempts after this boundary must be tolerated by business and storage
idempotency.

## Delivery Guarantees

### Transport: At Least Once

Transport implementations must assume messages may be delivered more than once.

At-least-once delivery is acceptable and expected because dropped business work
is less acceptable than duplicate transport delivery that can be handled above
the transport boundary.

### Business: Effectively Once

The target business model is effectively-once business effects through
idempotent processing.

Duplicate transport messages and repeated workflow attempts must not create
incorrect durable effects. This applies to local persistence, backend
processing, Google Sheets writes, and device-clearing safety.

### Not Supported

The reliability model does not support or claim:

- exactly-once transport delivery;
- exactly-once broker-backed business processing;
- an at-most-once target model.

Exactly-once transport is not a project-owned broker guarantee. At-most-once
delivery is rejected because it risks permanently losing business work.

## Idempotency Strategy

Idempotency is the long-term reliability mechanism for duplicate delivery and
repeated workflow attempts.

Exactly-once transport delivery is intentionally not a project goal because
business idempotency is still required to protect durable effects after crashes,
restarts, duplicate responses, and partial processing.

Idempotency is owned by business and storage layers. Transport layers do not
provide idempotency guarantees. Messenger implementations are not responsible
for duplicate suppression.

Backend and storage layers must tolerate duplicate transport delivery and
repeated workflow attempts. They must avoid incorrect durable effects when the
same request, response, attendance event, or workflow action is observed more
than once.

### Current Idempotency

Current duplicate suppression is partial:

- the Pi4 workflow filters attendance records after the review start or
  `last_stored_timestamp`;
- the backend returns the latest stored timestamp after storage;
- the ingestion snapshot persists `last_stored_timestamp`;
- backend storage filters records against existing stored timestamps in Google
  Sheets before appending;
- daily attendance and weekly summary updates merge derived data into existing
  sheets.

These mechanisms are useful mitigations. They are not complete idempotency
guarantees.

### Future Idempotency

Future reliability work should introduce stronger idempotency contracts,
including:

- event IDs for attendance events;
- correlation IDs for request/response flows;
- backend idempotency keys;
- Google Sheets deduplication strategy;
- stronger canonical event contracts across messaging and backend processing.

These items are deferred. They must not be added to ETLP-36 scope.

## Retry Strategy

| Operation | Owner | Retry? | Bounded? | Notes |
| --- | --- | --- | --- | --- |
| Send | Concrete `Messenger` implementation | Yes, for transient transport send failures | Yes | ETLP-36 owns bounded send retry/backoff for `GooglePubSubMessenger`; raise `MessageSendFailed` after exhaustion. |
| Receive | `Messenger` for one bounded attempt; workflow/FSM for repeated attempts | No retry across receive timeouts inside messenger | Yes | `receive(...)` raises `MessageReceiveTimeout` after the supplied total timeout. Provider pull loops must stay inside that timeout. |
| Workflow retry | FSM policy, executed by workflow/action services | Yes, when FSM re-enters an action | Must be bounded by FSM policy when designed | Action services execute one attempt and emit outcomes; they must not hide indefinite retry loops. |
| FSM retry | FSM | Yes, as transition policy | Target is bounded; exact budgets deferred | FSM decides retry, wait, recovery, and terminal failure paths. Persisted retry counters are deferred. |
| Backend retry | Backend/platform and future backend reliability design | Deferred | Deferred | Backend handlers must tolerate duplicate delivery. Backend retry/backoff and idempotency improvements are not part of ETLP-36. |

## Deferred Reliability Work

The following reliability work is explicitly deferred:

- dead-letter queues;
- poison-message handling;
- event IDs;
- correlation IDs;
- backend idempotency improvements;
- Google Sheets deduplication strategy;
- atomic snapshot writes;
- persisted retry counters;
- exact retry budgets and backoff values beyond ETLP-36 send retry;
- exact exception taxonomy beyond the project-owned failures required by
  accepted ADRs;
- backend messaging abstraction migration.

Deferred items must not be smuggled into ETLP-36.

## Relationship To ETLP Issues

| Issue | Reliability specification guidance |
| --- | --- |
| ETLP-33: Create Messaging Interface | Define the project-owned messaging boundary: `MessageTopic`, `TopicResolver`, `Messenger`, finite receive timeout contract, transport failure types, and testable abstractions. Business logic must be able to depend on the interface instead of concrete Pub/Sub implementation. |
| ETLP-34: Implement Pub/Sub Adapter | Implement `GoogleTopicResolver` and `GooglePubSubMessenger` as the concrete transport adapter. Own Google-specific topic paths, subscription creation, JSON serialisation/deserialisation, pull mechanics, ACK handling, routing metadata, topic-aware routing policy, and transport validation. Do not move business validation into the adapter. |
| ETLP-35: Move Messaging Logic | Remove direct Pub/Sub details from Pi4 workflow/orchestration code. Workflow/action services should depend on `Messenger`, `MessageTopic`, and workflow outcomes rather than raw topic strings, subscriptions, ACK IDs, or provider message envelopes. Do not move transition or retry policy into the messenger. |
| ETLP-36: Implement Retry Handling | Implement bounded send retry and backoff in the concrete messenger, focused on `GooglePubSubMessenger`. Send failure after retry exhaustion must be explicit, such as `MessageSendFailed`. ETLP-36 excludes receive retry, FSM retry, workflow retry redesign, DLQ, poison-message handling, event IDs, correlation IDs, backend idempotency, snapshot changes, and Google Sheets idempotency. |
