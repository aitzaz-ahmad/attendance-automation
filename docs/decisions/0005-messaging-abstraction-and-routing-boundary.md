# ADR-0005: Messaging Abstraction And Routing Boundary

## Status

Accepted

## Context

ADR-0004 accepted the FSM, workflow, runtime, persistence, and messaging
ownership boundaries.

Those decisions require Messaging Abstraction work to preserve the following
separation:

- FSM owns state policy.
- Workflow actions own business side effects.
- Runtime owns composition.
- Persistence owns storage.
- Messaging owns transport mechanics.
- Messaging must not hide workflow retry loops.
- Receive semantics must be bounded and explicit.

The current Pi4 workflow still calls Google Pub/Sub helper methods directly for
topic names, subscription names, subscription creation, pulls, and ACK handling.

A routing audit found that current backend response routing is mixed:

- review-period responses are broadcast;
- review-sheet responses are broadcast;
- last-stored-timestamp responses are targeted to the Pi4 site.

The current Pi4 receive implementation contradicts this model. It applies
`location == site_id` to all received topics. Because review-period and
review-sheet responses are currently published without `location`, valid
broadcast responses can be ACKed and discarded.

This ADR records the messaging boundary required before implementing Milestone 5.

## Decision 1: Introduce `MessageTopic`

The project shall introduce `MessageTopic` as the application-level topic
vocabulary.

`MessageTopic` shall define these six topics:

```python
REVIEW_PERIOD_REQUEST = "get_review_period"
REVIEW_PERIOD_RESPONSE = "new_review_period"
REVIEW_SHEET_REQUEST = "create_review_sheet"
REVIEW_SHEET_RESPONSE = "new_review_sheet"
ATTENDANCE_PUBLISH = "store_attend_records"
LAST_STORED_TIMESTAMP_RESPONSE = "last_stored_timestamp"
```

Workflow code shall depend on `MessageTopic`, not raw provider topic strings.

The existing string values remain unchanged to preserve backend compatibility.

## Decision 2: Introduce `TopicResolver`

The project shall introduce a project-owned `TopicResolver` abstraction.

The interface shall expose:

```python
initialise() -> bool
resolve(topic: MessageTopic) -> str
```

`resolve()` returns the configured provider topic name.

For Google Pub/Sub, this means the bare configured topic name such as
`"get_review_period"`, not a full Google resource path.

Provider-specific path construction belongs to the concrete messaging adapter.

`TopicResolver` shall not own routing behaviour.

## Decision 3: Introduce `Messenger`

The project shall introduce a project-owned `Messenger` abstraction.

The interface shall expose:

```python
initialise() -> bool
send(topic: MessageTopic, payload: Mapping[str, Any]) -> None
receive(topic: MessageTopic, timeout: timedelta) -> Mapping[str, Any]
```

`receive()` shall require a finite total timeout.

`receive()` shall raise a project-owned `MessageReceiveTimeout` when no accepted
message is received before that total timeout expires.

`MessageReceiveTimeout` is a transport outcome, not a business outcome.

A valid empty response payload remains a successful business response. For
example, an empty review-period response can mean that no review period is
currently available. That must not be conflated with a receive timeout.

The public messaging interface shall not expose:

- `target`
- raw Pub/Sub attributes
- subscription names
- topic paths
- ACK IDs
- raw message envelopes
- provider-specific pull mechanics

Those details belong to concrete adapters.

## Decision 4: Introduce Google Implementations

The project shall introduce:

- `GoogleTopicResolver`
- `GooglePubSubMessenger`

`GoogleTopicResolver` shall resolve `MessageTopic` values to configured Google
Pub/Sub topic names.

`GooglePubSubMessenger` shall own Google-specific transport mechanics, including:

- topic path construction
- subscription creation and lookup
- subscription naming
- Pub/Sub clients
- JSON encoding
- JSON decoding
- pull mechanics
- ACK handling
- Google Pub/Sub routing metadata

Workflow code shall not see topic paths, subscription names, ACKs, pull
mechanics, Pub/Sub attributes, or raw topic strings.

## Decision 5: Use Mixed Response Routing

Response routing shall be topic-aware.

The following response topics are broadcast:

- `MessageTopic.REVIEW_PERIOD_RESPONSE`
- `MessageTopic.REVIEW_SHEET_RESPONSE`

The following response topic is targeted:

- `MessageTopic.LAST_STORED_TIMESTAMP_RESPONSE`

Targeted routing uses Pi4 `site_id` as the application identity.

For Google Pub/Sub, targeted routing is implemented with the `location`
attribute internally owned by `GooglePubSubMessenger`.

The current implementation defect shall be corrected during Messaging
Abstraction implementation: broadcast response topics must not be rejected merely
because they do not include `location`.

## Decision 6: Separate Routing Policy From `MessageTopic`

The project shall introduce explicit routing policy separate from `MessageTopic`.

The routing policy vocabulary shall include:

```python
RoutingPolicy.BROADCAST
RoutingPolicy.TARGETED
```

A `MessageTopic -> RoutingPolicy` map shall define routing behaviour.

`RoutingPolicy` is an application-level contract.

`GooglePubSubMessenger` shall consume the routing policy when deciding whether a
received message is broadcast, targeted to this Pi4 site, or targeted elsewhere.

`TopicResolver` shall not return or infer routing information.

Routing behaviour shall not be embedded directly in `MessageTopic`.

This keeps the topic vocabulary stable while allowing transport policy to remain
explicit, testable, and reviewable.

## Decision 7: Define ACK Policy

ACK handling shall be topic-aware and transport-validation-aware.

Transport-level validation includes:

- the message JSON decodes successfully;
- the decoded payload is a mapping;
- required routing metadata exists for targeted topics.

Transport-level validation does not include validating business contracts such as:

- `ReviewPeriod`
- review-sheet payload semantics
- attendance payload semantics
- last-stored timestamp business meaning

Those contracts belong above the messaging transport boundary.

For broadcast response topics:

- ACK accepted broadcast messages after transport-level validation.

For targeted response topics:

- ACK accepted targeted messages after routing match and transport-level
  validation.
- ACK valid non-target targeted-topic messages so they do not block the
  subscription.

The adapter shall not ACK:

- malformed JSON
- non-mapping payloads
- targeted messages missing required routing metadata

This policy intentionally leaves poison-message and dead-letter behaviour
deferred. Until dead-letter handling is designed, malformed or transport-invalid
messages may redeliver.

## Decision 8: Define `initialise()` Semantics

`initialise()` shall be idempotent.

For the Google adapter, `initialise()` shall verify resolver and client setup and
create or verify Pi4 inbound response subscriptions.

`initialise()` shall not:

- publish messages
- pull messages
- sleep
- perform workflow retry
- advance workflow progress

Runtime composition may call `initialise()` before injecting the messenger into
workflow actions.

## Decision 9: Defer Backend Messaging Migration

Backend Cloud Functions may continue using direct Google Pub/Sub publication in
this milestone.

ADR-0005 documents backend-compatible contracts and routing requirements, but it
does not require backend publishers to depend on `Messenger`.

Backend messaging abstraction migration remains deferred to follow-up work.
Implementation sequencing and issue-level scope boundaries belong to the
[Messaging Abstraction Implementation Proposal](../proposals/messaging-abstraction.md).

## Consequences

### Positive

- Workflow depends on application topics instead of raw provider details.
- Google Pub/Sub mechanics are isolated behind the Google adapter.
- Receive semantics become bounded and compatible with ADR-0004 FSM policy.
- The current broadcast-versus-targeted routing contradiction is explicit and
  fixable.
- Existing backend-compatible topic names and payload shapes are preserved.
- Routing behaviour becomes a documented contract rather than incidental helper
  logic.

### Negative

- The existing messaging helper must be replaced, renamed, or reduced behind the
  new abstraction.
- Implementation verification belongs to the proposal-owned test strategy.
- Malformed or transport-invalid messages may redeliver until poison-message and
  dead-letter behaviour is designed.
- Backend publishers remain direct Google Pub/Sub users temporarily.
- The routing policy map is a new contract that must stay aligned with the topic
  vocabulary.

## Rejected Alternatives

| Alternative | Reason Rejected |
| --- | --- |
| Keep raw topic strings in workflow | Preserves provider details in workflow code and weakens the application messaging boundary. |
| Name the abstraction `PubSubMessenger` | Leaks the provider technology into the project-owned application abstraction. |
| Expose public `target` or raw attributes | Leaks provider routing mechanics into workflow actions and makes Google Pub/Sub metadata part of the project API. |
| Introduce a public message envelope for Milestone 5 | Adds abstraction before there is a demonstrated need; current workflow only requires decoded payload mappings. |
| Keep indefinite receive loops inside the adapter | Contradicts ADR-0004 by hiding workflow retry and progress policy in transport code. |
| Make all responses targeted | Contradicts current backend behaviour for review-period and review-sheet responses and would require additional backend identity propagation. |
| Make all responses broadcast | Contradicts current last-stored-timestamp routing, where the backend already publishes a device-specific response. |
| Migrate backend publishers in the same milestone | Expands scope beyond the Pi4 messaging boundary and increases migration risk. |
| Embed routing policy inside `MessageTopic` | Couples topic identity to routing behaviour and makes routing harder to audit independently. |
| Put routing behaviour in `TopicResolver` | Mixes address resolution with delivery policy and makes routing ownership ambiguous. |

## Deferred Decisions

- Dead-letter and poison-message handling.
- Message correlation and request/response correlation identifiers.
- Pub/Sub subscription filters.
- Backend messaging abstraction migration.
- Receive retry and workflow retry policy.
- Send retry and backoff details.
- Richer canonical wire payloads beyond current backend-compatible payloads.

## References

- [ADR-0003: Device And Transformation Layer Boundaries](0003-device-and-transformation-layer-boundaries.md)
- [ADR-0004: FSM, Workflow, Runtime, And Messaging Boundary](0004-fsm-isolation-and-execution-boundaries.md)
- [Messaging Model](../specifications/messaging-model.md)
- [Reliability Model](../specifications/reliability-model.md)
- [Messaging Abstraction Implementation Proposal](../proposals/messaging-abstraction.md)
- [Data Pipeline](../data-pipeline.md)
