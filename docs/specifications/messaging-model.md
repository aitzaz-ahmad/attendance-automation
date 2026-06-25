# Messaging Model Specification

## Status

Accepted

## Lifecycle

Active

This specification is the accepted messaging transport baseline for the
repository.

## Authority

This document defines the stable transport contract for the project-owned
messaging model.

Accepted ADRs remain authoritative for decision rationale and architectural
ownership:

- [ADR-0004: FSM, Workflow, Runtime, And Messaging Boundary](../decisions/0004-fsm-isolation-and-execution-boundaries.md)
- [ADR-0005: Messaging Abstraction And Routing Boundary](../decisions/0005-messaging-abstraction-and-routing-boundary.md)
- [ADR-0006: Reliability And Recovery](../decisions/0006-reliability-and-recovery.md)

If this specification conflicts with an accepted ADR, the ADR wins until this
specification is corrected.

Related specifications own adjacent contracts:

- [Reliability Model](reliability-model.md) owns retry ownership, delivery
  guarantees, recovery, checkpointing, and idempotency.
- [Canonical Attendance Event](../contracts/canonical-attendance-event.md) owns
  attendance payload schemas and schema evolution.

## Scope

This specification owns the messaging transport contract used by the Pi4
workflow and future workflow action services.

It defines:

- the `MessageTopic` vocabulary and compatibility strings;
- `RoutingPolicy` and the topic routing map;
- the `TopicResolver` contract;
- the `Messenger` contract;
- `MessageReceiveTimeout` and `MessageSendFailed`;
- the `GoogleTopicResolver` contract;
- the `GooglePubSubMessenger` contract;
- topic-aware routing, transport validation, ACK policy, and receive timeout
  semantics;
- payload boundary references; and
- backend publisher compatibility expectations.

## Non-Goals

This specification does not define:

- migration sequence or ETLP issue planning;
- implementation rollout strategy;
- a required test matrix;
- payload schemas;
- the full reliability model;
- workflow retry, receive retry, FSM retry, or checkpoint policy;
- dead-letter queues or poison-message handling;
- correlation identifiers or richer request/response envelopes;
- backend publisher migration to `Messenger`; or
- exactly-once or at-most-once transport guarantees.

## Messaging Layer Responsibilities

Messaging owns transport mechanics.

Messaging owns:

- the application topic vocabulary exposed to workflow code;
- resolving application topics to provider topic names;
- sending mapping payloads to a transport;
- receiving decoded mapping payloads from a transport;
- enforcing finite receive attempts;
- transport-level decoding and validation;
- topic-aware routing validation;
- provider-specific topic and subscription mechanics;
- provider-specific ACK mechanics;
- provider-specific routing metadata; and
- provider-specific JSON encoding and decoding.

Messaging does not own:

- FSM transition policy;
- workflow retry policy;
- receive retry across timeout outcomes;
- checkpoint eligibility;
- snapshot persistence;
- business payload validation;
- attendance payload schema evolution;
- review-period, review-sheet, or last-stored-timestamp business semantics;
- backend durable effects; or
- idempotency strategy.

Workflow or future workflow action services execute business attempts and
translate messaging outcomes into workflow outcomes. The FSM decides progress,
retry, wait, recovery, and failure transitions.

## Terminology

| Term | Meaning |
| --- | --- |
| Messaging model | The project-owned transport contract defined by this specification. |
| Application topic | A `MessageTopic` value used by workflow or action-service code. |
| Provider topic name | The configured provider topic name resolved from a `MessageTopic`, such as `get_review_period`. |
| Provider topic path | A provider-specific resource path, such as a Google Pub/Sub topic path. |
| Topic resolver | Project-owned interface that maps `MessageTopic` values to configured provider topic names. |
| Messenger | Project-owned interface used by workflow/action services to send and receive messages. |
| Routing policy | Application-level policy that classifies response topics as broadcast or targeted. |
| Transport-valid message | A message that decodes successfully, has a mapping payload, and satisfies required routing metadata for its topic. |
| Business-valid response | A transport-valid payload that satisfies the expected business contract for the workflow action consuming it. |
| ACK | Broker-facing acknowledgement that a transport-valid message has been accepted at the transport boundary. |
| Receive timeout | A bounded receive attempt that completes without an accepted message before the supplied total timeout expires. |

## MessageTopic Vocabulary

`MessageTopic` is the application-level topic vocabulary.

Workflow code and future workflow action services shall depend on
`MessageTopic`, not raw provider topic strings.

`MessageTopic` shall define these topics:

| `MessageTopic` | Compatibility string | Direction | Routing policy |
| --- | --- | --- | --- |
| `REVIEW_PERIOD_REQUEST` | `get_review_period` | Pi4 to backend | Not applicable |
| `REVIEW_PERIOD_RESPONSE` | `new_review_period` | Backend to Pi4 | `RoutingPolicy.BROADCAST` |
| `REVIEW_SHEET_REQUEST` | `create_review_sheet` | Pi4 to backend | Not applicable |
| `REVIEW_SHEET_RESPONSE` | `new_review_sheet` | Backend to Pi4 | `RoutingPolicy.BROADCAST` |
| `ATTENDANCE_PUBLISH` | `store_attend_records` | Pi4 to backend | Not applicable |
| `LAST_STORED_TIMESTAMP_RESPONSE` | `last_stored_timestamp` | Backend to Pi4 | `RoutingPolicy.TARGETED` |

The compatibility strings intentionally match the current Google Pub/Sub topic
names from `attendance_etl.config`. They must not be renamed without a dedicated
backend compatibility migration.

Expected shape:

```python
class MessageTopic(Enum):
    REVIEW_PERIOD_REQUEST = "get_review_period"
    REVIEW_PERIOD_RESPONSE = "new_review_period"
    REVIEW_SHEET_REQUEST = "create_review_sheet"
    REVIEW_SHEET_RESPONSE = "new_review_sheet"
    ATTENDANCE_PUBLISH = "store_attend_records"
    LAST_STORED_TIMESTAMP_RESPONSE = "last_stored_timestamp"
```

`MessageTopic` must not embed routing behaviour directly. Routing behaviour is
defined by a separate `MessageTopic -> RoutingPolicy` map.

## RoutingPolicy

`RoutingPolicy` is an application-level routing contract.

Expected shape:

```python
class RoutingPolicy(Enum):
    BROADCAST = "broadcast"
    TARGETED = "targeted"
```

The current routing map is:

```python
MESSAGE_ROUTING_POLICY = {
    MessageTopic.REVIEW_PERIOD_RESPONSE: RoutingPolicy.BROADCAST,
    MessageTopic.REVIEW_SHEET_RESPONSE: RoutingPolicy.BROADCAST,
    MessageTopic.LAST_STORED_TIMESTAMP_RESPONSE: RoutingPolicy.TARGETED,
}
```

Request topics do not require inbound response routing policy.

Routing policy is consumed by concrete messenger implementations when receiving
messages. `TopicResolver` must not return or infer routing policy.

## TopicResolver Contract

`TopicResolver` resolves application topics to provider topic names.

Expected interface:

```python
class TopicResolver(Protocol):
    def initialise(self) -> bool:
        ...

    def resolve(self, topic: MessageTopic) -> str:
        ...
```

`initialise()` shall be idempotent.

`resolve(topic)` returns the configured provider topic name for the supplied
`MessageTopic`.

For Google Pub/Sub, `resolve()` returns the bare configured topic name, such as
`get_review_period`, not a full Google Pub/Sub resource path.

`TopicResolver` owns:

- verifying that supported application topics can be resolved; and
- returning configured provider topic names.

`TopicResolver` does not own:

- routing policy;
- provider topic path construction;
- subscription creation;
- ACK handling;
- send mechanics;
- receive mechanics;
- JSON encoding or decoding;
- provider client construction; or
- workflow retry policy.

A resolver must fail explicitly if it cannot resolve a supported
`MessageTopic`.

## Messenger Contract

`Messenger` is the project-owned messaging interface used by workflow/action
services.

Expected interface:

```python
class Messenger(Protocol):
    def initialise(self) -> bool:
        ...

    def send(self, topic: MessageTopic, payload: Mapping[str, Any]) -> None:
        ...

    def receive(
        self,
        topic: MessageTopic,
        timeout: timedelta,
    ) -> Mapping[str, Any]:
        ...
```

The public `Messenger` interface shall expose only application topics and
decoded mapping payloads.

The public interface shall not expose:

- raw provider topic strings;
- provider topic paths;
- subscription names;
- raw Pub/Sub attributes;
- raw provider message envelopes;
- ACK IDs;
- pull request objects;
- provider-specific clients;
- public `target` parameters; or
- provider-specific routing metadata.

### `initialise()`

`Messenger.initialise()` shall be idempotent.

Calling `initialise()` more than once must not publish messages, pull messages,
sleep, perform workflow retry, advance workflow progress, transition FSM state,
or create duplicate externally visible workflow effects.

Concrete adapters may verify dependencies and prepare transport resources
required for later send/receive operations.

### `send()`

`send(topic, payload)` sends one mapping payload to the topic identified by
`MessageTopic`.

`send()` must not require workflow code to provide provider topic paths,
subscription names, ACK IDs, raw attributes, or provider message envelopes.

If send failure remains after configured transport send handling is exhausted,
the messenger shall raise `MessageSendFailed`.

The reliability model owns send retry ownership and delivery guarantees.

### `receive()`

`receive(topic, timeout)` performs one bounded receive attempt for a
`MessageTopic`.

`timeout` is the finite total timeout for the complete receive operation. A
messenger implementation may perform provider-specific pull calls internally,
but the complete receive operation must not exceed the supplied total timeout.

`receive()` shall raise `MessageReceiveTimeout` when no accepted message is
received before the supplied total timeout expires.

A valid empty response payload is not a receive timeout. For example, an empty
review-period response can mean that no review period is currently available.
Business interpretation of empty payloads belongs above the messaging boundary.

Retries across receive timeouts belong above `Messenger`.

## Transport Exceptions

The messaging model defines these project-owned transport exceptions:

```python
class MessageReceiveTimeout(Exception):
    ...
```

Raised when one bounded receive attempt completes without an accepted message
before the supplied total timeout expires.

```python
class MessageSendFailed(Exception):
    ...
```

Raised when a concrete messenger implementation cannot complete `send()` after
its configured transport send handling.

These exceptions are transport outcomes. Workflow/action services translate
them into workflow outcomes. FSM policy decides retry, recovery, wait, or
failure.

Additional exception taxonomy is deferred to the reliability model.

## GoogleTopicResolver Contract

`GoogleTopicResolver` is the Google Pub/Sub implementation of `TopicResolver`.

It resolves `MessageTopic` values to configured Google Pub/Sub topic names.

`GoogleTopicResolver.resolve(...)` returns bare topic names, not full Google
resource paths.

| `MessageTopic` | `GoogleTopicResolver.resolve(...)` |
| --- | --- |
| `MessageTopic.REVIEW_PERIOD_REQUEST` | `get_review_period` |
| `MessageTopic.REVIEW_PERIOD_RESPONSE` | `new_review_period` |
| `MessageTopic.REVIEW_SHEET_REQUEST` | `create_review_sheet` |
| `MessageTopic.REVIEW_SHEET_RESPONSE` | `new_review_sheet` |
| `MessageTopic.ATTENDANCE_PUBLISH` | `store_attend_records` |
| `MessageTopic.LAST_STORED_TIMESTAMP_RESPONSE` | `last_stored_timestamp` |

Provider resource path construction belongs to `GooglePubSubMessenger`, not to
`GoogleTopicResolver`.

`GoogleTopicResolver` must not infer routing policy.

## GooglePubSubMessenger Contract

`GooglePubSubMessenger` is the Google Pub/Sub implementation of `Messenger`.

It owns Google-specific transport mechanics, including:

- Google Pub/Sub publisher client usage;
- Google Pub/Sub subscriber client usage;
- topic path construction;
- subscription path construction;
- Pi4 inbound response subscription creation or verification;
- subscription naming;
- JSON encoding and decoding;
- pull mechanics;
- ACK handling;
- Google Pub/Sub routing metadata;
- applying `RoutingPolicy` during receive;
- enforcing transport-level validation; and
- raising project-owned transport exceptions.

`GooglePubSubMessenger` consumes:

- a `TopicResolver`;
- the `MessageTopic -> RoutingPolicy` map;
- Google project configuration;
- Pi4 `site_id` for targeted receive routing; and
- provider configuration needed for subscriptions, ACK deadline, pull batch
  size, pull timeout slices, and subscription TTL.

`GooglePubSubMessenger` does not own:

- business payload validation;
- review-period, review-sheet, or attendance event semantics;
- last-stored timestamp business meaning;
- workflow retry;
- receive retry across timeout outcomes;
- FSM transition policy;
- checkpoint policy;
- backend storage effects; or
- backend publisher migration.

## Routing Model

Response routing is topic-aware.

The current backend routing model is mixed:

- `MessageTopic.REVIEW_PERIOD_RESPONSE` is broadcast.
- `MessageTopic.REVIEW_SHEET_RESPONSE` is broadcast.
- `MessageTopic.LAST_STORED_TIMESTAMP_RESPONSE` is targeted.

Broadcast response topics are accepted after transport-level validation. They
must not require provider routing metadata such as a Google Pub/Sub `location`
attribute.

Targeted response topics are accepted locally only when routing metadata targets
the current Pi4 site identity.

For Google Pub/Sub, targeted routing uses an internal `location` attribute owned
by `GooglePubSubMessenger`.

The Pi4 `site_id` is the application identity used for targeted routing.

## Transport Validation

Transport-level validation is performed by the concrete messenger adapter before
a received message is accepted.

Transport-level validation includes:

- JSON decoding succeeds;
- decoded payload is a mapping;
- required routing metadata exists for targeted topics;
- targeted routing metadata matches the current Pi4 site when accepting a
  targeted message for local processing; and
- the topic being received is known and has defined routing policy when routing
  policy is required.

Transport-level validation excludes:

- `ReviewPeriod` semantics;
- review-sheet payload semantics;
- attendance payload semantics;
- last-stored timestamp business meaning;
- transformation validation;
- backend storage correctness;
- Google Sheets write correctness; and
- idempotency decisions.

Business validation belongs above the messaging boundary.

## ACK Policy

ACK is broker-facing. It confirms that a transport-valid message has been
accepted at the transport boundary. It is not the business transaction boundary.

For broadcast response topics, `GooglePubSubMessenger` shall ACK accepted
broadcast messages after transport-level validation.

For targeted response topics, `GooglePubSubMessenger` shall ACK accepted
targeted messages after routing match and transport-level validation.

For targeted response topics, `GooglePubSubMessenger` shall ACK valid non-target
messages so they do not block the subscription.

A valid non-target message is a message whose payload is transport-valid and
whose required routing metadata exists, but whose target does not match the
current Pi4 `site_id`.

The adapter shall not ACK:

- malformed JSON;
- decoded payloads that are not mappings; or
- targeted messages missing required routing metadata.

Malformed or transport-invalid messages may redeliver until poison-message and
dead-letter handling are designed.

## Timeout Semantics

`Messenger.receive(topic, timeout)` requires a finite total timeout.

The supplied timeout bounds the complete receive operation. Provider-specific
pull calls may be shorter than the total timeout, but all internal receive work
must stay within the supplied total timeout.

If no accepted message is received before the total timeout expires,
`receive(...)` raises `MessageReceiveTimeout`.

The messenger must not perform indefinite receive retry or hide workflow
progress policy inside provider adapters.

Workflow/FSM reaction to receive timeouts belongs to the reliability model.

## Payload Boundaries

The messaging boundary transports mapping payloads. It does not own business
payload schemas.

Payload ownership:

| Payload area | Owner |
| --- | --- |
| Attendance event internal model | [Canonical Attendance Event](../contracts/canonical-attendance-event.md) |
| Current backend-compatible attendance Pub/Sub payload | [Canonical Attendance Event](../contracts/canonical-attendance-event.md) |
| Review-period business response meaning | Workflow/action services and review-period domain contracts |
| Review-sheet business response meaning | Workflow/action services and backend function contracts |
| Last-stored timestamp business meaning | Workflow/action services and storage/reliability contracts |
| JSON encoding and decoding mechanics | Concrete messenger adapter |
| Mapping payload transport validation | Concrete messenger adapter |

The messaging adapter validates that payloads are decodable mappings. It does
not validate business fields inside those mappings.

## Backend Publisher Compatibility

Backend Cloud Functions may continue publishing directly to Google Pub/Sub for
this milestone.

Current backend-compatible response routing:

| Backend response | Topic | Current provider metadata | Routing model |
| --- | --- | --- | --- |
| Review-period response | `new_review_period` | No `location` attribute | Broadcast |
| Review-sheet response | `new_review_sheet` | No `location` attribute | Broadcast |
| Last-stored-timestamp response | `last_stored_timestamp` | `location=device_id` | Targeted |

The messaging model documents backend-compatible contracts and routing
requirements, but it does not require backend publishers to depend on
`Messenger`.
