# Messaging Abstraction Implementation Proposal

> Status: Accepted.
> Lifecycle: Active implementation strategy for ETLP-33 through ETLP-36.

## Status

Accepted

## Lifecycle

Active

## Authority

This proposal describes how to implement the accepted messaging model. It does
not re-decide the architecture.

Authoritative inputs:

- [ADR-0004: FSM, Workflow, Runtime, And Messaging Boundary](../decisions/0004-fsm-isolation-and-execution-boundaries.md)
- [ADR-0005: Messaging Abstraction And Routing Boundary](../decisions/0005-messaging-abstraction-and-routing-boundary.md)
- [ADR-0006: Reliability And Recovery](../decisions/0006-reliability-and-recovery.md)
- [Messaging Model Specification](../specifications/messaging-model.md)
- [Reliability Model](../specifications/reliability-model.md)
- [Canonical Attendance Event](../contracts/canonical-attendance-event.md)

If this proposal conflicts with an accepted ADR or specification, the ADR or
specification wins.

## Implementation Context

The current Pi4 messaging implementation is the legacy
`attendance_etl.messaging.pubsub.PubSubMessenger` helper surface.

The current workflow calls Pub/Sub helper methods directly:

- `publish_message_to_topic(topic_name, data)` for request and publish topics;
- `subscription_name(topic_name)` to construct subscription names;
- `create_subscription(topic_name, subscription_name)` before waiting for
  responses; and
- `sync_pull_message(subscription_name)` to pull until a targeted message is
  received.

The current helper implementation owns publishing, subscription creation, pull
loops, ACKs, JSON decoding, and `location == site_id` targeting. It applies
targeted routing to every received topic, which contradicts ADR-0005 because
review-period and review-sheet responses are broadcast.

The implementation target is a project-owned messaging interface:

- workflow depends on `MessageTopic` and `Messenger`;
- Google Pub/Sub mechanics move behind `GooglePubSubMessenger`;
- topic resolution moves into `GoogleTopicResolver`;
- response routing uses the explicit `MessageTopic -> RoutingPolicy` map; and
- retry/recovery ownership remains aligned with the reliability model.

## Legacy Helper Mapping

| Legacy surface | Target owner | Notes |
| --- | --- | --- |
| `config.GET_REVIEW_PERIOD_TOPIC` | `MessageTopic.REVIEW_PERIOD_REQUEST` and `GoogleTopicResolver` | Preserve compatibility string `get_review_period`. |
| `config.NEW_REVIEW_PERIOD_TOPIC` | `MessageTopic.REVIEW_PERIOD_RESPONSE` and `GoogleTopicResolver` | Broadcast response; must not require `location`. |
| `config.CREATE_REVIEW_SHEET_TOPIC` | `MessageTopic.REVIEW_SHEET_REQUEST` and `GoogleTopicResolver` | Preserve compatibility string `create_review_sheet`. |
| `config.NEW_REVIEW_SHEET_TOPIC` | `MessageTopic.REVIEW_SHEET_RESPONSE` and `GoogleTopicResolver` | Broadcast response; must not require `location`. |
| `config.STORE_ATTEND_RECORDS_TOPIC` | `MessageTopic.ATTENDANCE_PUBLISH` and `GoogleTopicResolver` | Payload schema remains owned by the canonical attendance event contract. |
| `config.LAST_STORED_TIMESTAMP_TOPIC` | `MessageTopic.LAST_STORED_TIMESTAMP_RESPONSE` and `GoogleTopicResolver` | Targeted response using internal Google `location` metadata. |
| `PubSubMessenger.publish_message_to_topic(...)` | `GooglePubSubMessenger.send(...)` | Build topic path and JSON-encode internally. |
| `PubSubMessenger.subscription_name(...)` | `GooglePubSubMessenger` internal subscription naming | Do not expose subscription names to workflow. |
| `PubSubMessenger.create_subscription(...)` | `GooglePubSubMessenger.initialise()` or receive preparation | Preserve subscription settings unless a scoped change says otherwise. |
| `PubSubMessenger.sync_pull_message(...)` | `GooglePubSubMessenger.receive(...)` | Replace indefinite targeted loop with one bounded receive attempt. |
| `publish_message_to_topic(...)` helper | Internal Google adapter helper or removed helper | Must not remain the workflow-facing API. |
| `subscription_exists(...)` helper | Internal Google adapter helper | Implementation detail. |
| `create_subscription(...)` helper | Internal Google adapter helper | Implementation detail. |
| `is_directed_to_device(...)` helper | `GooglePubSubMessenger` routing policy handling | Apply only to targeted topics. |
| `sync_pull_message(...)` helper | `GooglePubSubMessenger.receive(...)` internals | ACK behaviour must follow the messaging spec. |

The old `PubSubMessenger` name should not become the project-owned abstraction
name. ADR-0005 reserves `Messenger` for the application interface and
`GooglePubSubMessenger` for the Google adapter.

## Migration Sequence

### ETLP-33: Create Messaging Interface

Introduce the stable interface package without changing workflow behaviour yet.

Expected deliverables:

- `MessageTopic`;
- `RoutingPolicy`;
- `MESSAGE_ROUTING_POLICY`;
- `TopicResolver`;
- `Messenger`;
- `MessageReceiveTimeout`; and
- `MessageSendFailed`.

Acceptance focus:

- topic values match the current compatibility strings;
- routing policy is separate from `MessageTopic`;
- request topics do not require inbound routing policy;
- `TopicResolver` resolves topic names only; and
- workflow-facing types expose no Google Pub/Sub details.

### ETLP-34: Implement Pub/Sub Adapter

Introduce the Google implementations behind the accepted interface.

Expected deliverables:

- `GoogleTopicResolver`;
- `GooglePubSubMessenger`;
- internal topic path construction;
- internal subscription path construction;
- inbound response subscription creation or verification;
- JSON encoding and decoding;
- bounded receive mechanics;
- ACK policy implementation;
- topic-aware broadcast/targeted routing; and
- `MessageReceiveTimeout` and `MessageSendFailed` translation.

Acceptance focus:

- broadcast responses are accepted without `location`;
- targeted last-stored-timestamp responses require matching `location`;
- valid non-target targeted messages are ACKed but not returned to workflow;
- malformed JSON, non-mapping payloads, and targeted messages missing routing
  metadata are not ACKed;
- `receive(...)` is bounded by the supplied total timeout; and
- provider details remain internal to the Google adapter.

### ETLP-35: Move Messaging Logic Out Of Workflow

Migrate Pi4 workflow calls from raw Pub/Sub helpers to `Messenger`.

Expected workflow replacements:

| Current workflow action | Target call |
| --- | --- |
| Publish review-period request with `config.GET_REVIEW_PERIOD_TOPIC` | `send(MessageTopic.REVIEW_PERIOD_REQUEST, payload)` |
| Pull review-period response through a subscription name | `receive(MessageTopic.REVIEW_PERIOD_RESPONSE, timeout)` |
| Publish review-sheet request with `config.CREATE_REVIEW_SHEET_TOPIC` | `send(MessageTopic.REVIEW_SHEET_REQUEST, payload)` |
| Pull review-sheet response through a subscription name | `receive(MessageTopic.REVIEW_SHEET_RESPONSE, timeout)` |
| Publish attendance records with `config.STORE_ATTEND_RECORDS_TOPIC` | `send(MessageTopic.ATTENDANCE_PUBLISH, payload)` |
| Pull last-stored timestamp through a subscription name | `receive(MessageTopic.LAST_STORED_TIMESTAMP_RESPONSE, timeout)` |

Workflow should no longer construct subscription names, create subscriptions, or
see raw Pub/Sub attributes, ACK IDs, topic paths, or message envelopes.

Workflow may still validate business response shape after receive. Business
validation remains outside the messenger boundary.

### ETLP-36: Implement Retry Handling

Implement bounded send retry and backoff in the concrete messenger according to
the reliability model.

Scope includes:

- bounded retry for transient `GooglePubSubMessenger.send(...)` failures;
- visible retry configuration;
- `MessageSendFailed` after configured send handling is exhausted; and
- tests for retry exhaustion and non-retryable failures.

Scope excludes:

- receive retry across `MessageReceiveTimeout`;
- workflow retry;
- FSM retry;
- dead-letter queues;
- poison-message handling;
- correlation IDs;
- event IDs;
- backend idempotency;
- snapshot/checkpoint changes; and
- Google Sheets idempotency changes.

## Proposed Module Layout

```text
attendance_etl/
└── messaging/
    ├── __init__.py
    ├── exceptions.py        # MessageReceiveTimeout, MessageSendFailed
    ├── interfaces.py        # MessageTopic, RoutingPolicy, TopicResolver, Messenger
    ├── routing.py           # MESSAGE_ROUTING_POLICY
    └── google_pubsub.py     # GoogleTopicResolver, GooglePubSubMessenger
```

Alternative layout with a subpackage is acceptable if the public API remains
clear:

```text
attendance_etl/
└── messaging/
    ├── __init__.py
    ├── exceptions.py
    ├── interfaces.py
    ├── routing.py
    └── google/
        ├── __init__.py
        └── pubsub.py
```

The implementation should keep imports ergonomic for workflow and runtime code,
for example:

```python
from attendance_etl.messaging import MessageTopic, Messenger
from attendance_etl.messaging.google_pubsub import GooglePubSubMessenger
```

## Test Strategy

Tests should be focused on the new contracts and on behaviour that changes at
the messaging boundary.

| Area | Coverage |
| --- | --- |
| Interface vocabulary | `MessageTopic` compatibility strings, `RoutingPolicy`, and routing map. |
| Resolver | `GoogleTopicResolver` returns bare topic names, fails explicitly for unsupported topics, and does not infer routing policy. |
| Send | `GooglePubSubMessenger.send(...)` resolves topics, builds paths internally, JSON-encodes mapping payloads, publishes through the Google client, and raises `MessageSendFailed` on configured exhaustion. |
| Receive timeout | `receive(...)` requires a finite total timeout, returns only accepted mapping payloads, and raises `MessageReceiveTimeout` when no accepted message arrives in time. |
| Routing and ACK | Broadcast topics accept messages without `location`; targeted topics require `location`; valid non-target targeted messages are ACKed but not returned; transport-invalid messages are not ACKed. |
| Workflow migration | Pi4 workflow calls `send(...)` and `receive(...)` with `MessageTopic` and no longer touches subscription names or raw Pub/Sub details. |
| Runtime construction | Runtime constructs resolver and messenger dependencies and calls idempotent initialisation without advancing workflow progress. |
| Payload boundary | Messaging tests assert mapping transport shape only; business payload validation stays in workflow/domain tests. |
| Retry | ETLP-36 covers bounded send retry/backoff and `MessageSendFailed`; receive retry remains outside messenger tests. |

Implementation tests should use fakes or mocks for Google clients. They should
not require live Google Pub/Sub access.

## Workflow Migration Notes

Workflow migration should be mechanical and narrow.

`Pi4Workflow` should depend on the `Messenger` interface rather than
`PubSubMessenger`.

Workflow should:

- call `send(...)` for request/publication actions;
- call `receive(...)` once per waiting action with a finite total timeout;
- interpret `MessageReceiveTimeout` and `MessageSendFailed` as transport
  outcomes;
- continue to validate business response shape outside the messenger;
- preserve current review-period, review-sheet, attendance relay, and
  last-stored-timestamp business semantics unless a scoped issue changes them;
  and
- keep checkpoint and retry policy aligned with ADR-0004 and the reliability
  model.

Workflow should not:

- call `subscription_name(...)`;
- call `create_subscription(...)`;
- call `sync_pull_message(...)`;
- depend on raw config topic constants for messaging operations;
- inspect Pub/Sub attributes;
- ACK messages; or
- implement Google Pub/Sub pull mechanics.

## Runtime And Factory Construction Notes

`Pi4Runtime` remains the composition root.

Runtime should construct:

- `GoogleTopicResolver` from the current topic configuration;
- `GooglePubSubMessenger` with the resolver, routing map, project settings,
  subscription settings, and Pi4 `site_id`;
- device and transformation dependencies as it does today; and
- `Pi4Workflow` with the `Messenger` interface.

Runtime may call `initialise()` before injecting the messenger into workflow,
provided initialisation stays idempotent and does not publish, pull, sleep,
advance workflow progress, or transition FSM state.

If a factory is introduced, keep it narrow. A messaging factory may assemble
Google-specific dependencies, but it must not own transition policy, workflow
retry policy, or business payload validation.

## Backend Migration Deferral

Backend Cloud Functions may continue publishing directly to Google Pub/Sub for
this milestone.

The Pi4 adapter must remain compatible with current backend behaviour:

- review-period responses are published to `new_review_period` without
  `location` and are broadcast;
- review-sheet responses are published to `new_review_sheet` without
  `location` and are broadcast; and
- last-stored-timestamp responses are published to `last_stored_timestamp` with
  `location=device_id` and are targeted.

Backend migration to `Messenger` is a follow-up concern. It should not be mixed
into ETLP-33 through ETLP-36 unless a scoped issue promotes it.

## Implementation Risks

| Risk | Mitigation |
| --- | --- |
| Accidentally changing topic names | Keep `MessageTopic` values equal to current compatibility strings and cover them in tests. |
| Treating broadcast responses as targeted | Drive receive routing through `MESSAGE_ROUTING_POLICY`; test broadcast responses without `location`. |
| Hiding receive retry inside the adapter | Implement `receive(...)` as one bounded attempt and raise `MessageReceiveTimeout` on exhaustion. |
| ACKing transport-invalid messages | Separate JSON/mapping/routing validation from business validation and test negative ACK paths. |
| Moving business validation into messenger | Keep messenger validation to transport shape and routing metadata only. |
| Duplicating payload schemas in messaging tests | Reference canonical attendance event and workflow/domain contracts instead. |
| Letting send retry become workflow retry | Limit ETLP-36 to bounded transport send retry and `MessageSendFailed`. |
| Creating a broad factory abstraction | Keep runtime composition explicit unless a small factory removes real duplication. |

## Follow-Up Cleanup Tasks

After implementation and review:

- remove workflow dependence on raw messaging config constants;
- remove or make private legacy helper functions that are no longer used;
- update architecture and reliability narrative docs to link to the new
  messaging spec;
- update proposal indexes if the repository keeps proposal navigation current;
- audit diagrams for references to the old `PubSubMessenger` helper surface;
- audit backend function docs for the deferred publisher migration boundary;
- confirm stale ADR references point at the current reliability specification;
  and
- remove accidental assistant-note text from ADR files in a dedicated cleanup.

These cleanup tasks are not part of this proposal creation step.
