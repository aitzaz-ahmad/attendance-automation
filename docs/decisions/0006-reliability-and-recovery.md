# ADR-0006: Reliability And Recovery

## Status

Accepted

## Context

ADR-0004 accepted the FSM, workflow, runtime, persistence, and messaging
ownership boundaries.

ADR-0005 accepted the project-owned messaging abstraction, bounded receive
semantics, routing policy, and ACK boundary.

Those decisions leave the project needing an explicit reliability and recovery
model before ETLP-36 adds Pub/Sub send retry and backoff.

The current implementation is best-effort. It uses checkpoint-assisted restart
from the last persisted non-waiting state and partial duplicate suppression
through timestamp filtering and last-stored-timestamp feedback. That behaviour
helps recovery after interruption, but it is not an exactly-once delivery or
exactly-once business-processing guarantee.

This ADR records the current reliability baseline, the target reliability model,
and the immediate ETLP-36 scope.

## Decision 1: Current Behaviour Is Best Effort

Current system behaviour is best-effort with checkpoint-assisted restart and
partial duplicate suppression.

Checkpoint-assisted restart helps the Pi4 workflow resume from the last
persisted non-waiting state after interruption.

Timestamp filtering and last-stored-timestamp feedback partially suppress
duplicate attendance records.

These mechanisms are current mitigations. They are not exactly-once transport
delivery, exactly-once business processing, or the final reliability model.

## Decision 2: Target Transport Delivery Is At Least Once

The project shall treat at-least-once transport delivery as acceptable and
expected.

Transport implementations must assume that messages may be delivered more than
once.

The project shall not claim exactly-once transport delivery.

The target transport model is not at-most-once delivery, because dropped
business work is less acceptable than duplicate transport delivery that can be
handled above the transport boundary.

## Decision 3: Target Business Effects Are Effectively Once

The target business model is effectively-once business effects through
idempotent processing.

Duplicate transport messages and repeated workflow attempts must be tolerated by
business and storage layers.

This does not mean the broker provides exactly-once delivery. It means business
handlers, storage integrations, and workflow recovery paths must be designed so
that duplicate or repeated attempts do not create incorrect durable effects.

## Decision 4: ACK Is Broker-Facing

ACK confirms that a transport-valid message intended for the recipient has been
accepted by the consumer.

ACK is a broker-facing boundary. It is not the business transaction boundary.

Business-invalid but transport-valid messages shall be handled through
workflow/FSM outcomes, not broker redelivery.

Malformed or transport-invalid messages remain governed by ADR-0005 ACK policy:
until poison-message and dead-letter handling are designed, such messages may
redeliver.

## Decision 5: Send Retry Belongs To Concrete Messenger Implementations

Concrete `Messenger` implementations shall own bounded transport retry and backoff
for send operations.

`GooglePubSubMessenger` shall implement bounded send retry and backoff for
transient publish failures.

After retry exhaustion, send shall raise a project-owned failure such as
`MessageSendFailed`.

Send retry must be finite and visible through configuration and tests.

Messenger implementations must not contain indefinite send retry loops.

## Decision 6: Receive Retry Belongs Above Messenger

`Messenger.receive(...)` performs one bounded receive attempt within the
supplied total timeout.

When no accepted message is received within that timeout, the messenger shall
report `MessageReceiveTimeout` to workflow or action services.

Retries across receive timeouts belong to workflow/FSM policy, not the messenger
adapter.

Provider-specific receive mechanics may be used inside the adapter, but they
must not exceed the supplied total timeout or hide workflow progress decisions.

## Decision 7: Workflow And FSM Own Business Retry Policy

Workflow or action services execute one business attempt.

They translate transport failures, receive timeouts, invalid business responses,
and other action results into workflow outcomes.

The FSM owns whether the next transition should retry, resend, recover, wait, or
fail.

Additional FSM states may be introduced to model wait, retry, recovery, invalid
response, compensation, or terminal failure paths.

The final FSM is not constrained to the current nine-state model.

## Decision 8: Idempotency Is The Long-Term Reliability Mechanism

Idempotency is the long-term reliability mechanism for repeated transport
delivery and repeated workflow attempts.

The current timestamp-based duplicate suppression is partial. It is useful as a
current mitigation, but it is not sufficient as the final reliability model.

Correlation IDs, event IDs, stronger backend idempotency, and Google Sheets
deduplication remain deferred.

## Decision 9: Poison-Message And Dead-Letter Handling Are Deferred

Poison-message and dead-letter handling are deferred.

Until that support is designed, malformed or transport-invalid messages may
redeliver according to broker behaviour and ADR-0005 ACK policy.

This ADR does not require dead-letter queues, poison-message counters, or
redrive policy in the current milestone.

## Decision 10: ETLP-36 Scope Is Bounded Messenger Send Retry

ETLP-36 shall be scoped to bounded `Messenger` send retry and backoff.

Issue-level sequencing, exclusions, acceptance focus, and test strategy belong
to the [Messaging Abstraction Implementation Proposal](../proposals/messaging-abstraction.md).

ETLP-36 must not use send retry work as a vehicle for broader reliability,
recovery, or backend-storage redesign.

## Consequences

### Positive

- Transport reliability and business reliability have separate ownership.
- The project can tolerate at-least-once delivery without claiming exactly-once
  transport semantics.
- ACK policy stays aligned with ADR-0005 and does not become a hidden business
  transaction boundary.
- Send retry can be added to `GooglePubSubMessenger` without moving receive
  retry or FSM retry into the messaging adapter.
- Idempotency becomes the explicit long-term reliability direction.
- The immediate send-retry work stays bounded.

### Negative

- The current system remains only partially protected against duplicate durable
  effects until stronger idempotency work is designed.
- Duplicate transport delivery and repeated workflow attempts are expected and
  must be handled by downstream business and storage layers.
- Receive timeout retry policy still requires future FSM/workflow design.
- Malformed or transport-invalid messages may redeliver until poison-message and
  dead-letter handling are designed.
- Exact retry budgets, exception taxonomy, and persisted retry state remain
  unresolved.

## Rejected Alternatives

| Alternative | Reason Rejected |
| --- | --- |
| Exactly-once transport delivery | The project should not claim a broker-level guarantee it does not own, and business idempotency is still required for durable effects. |
| Best-effort as the target model | Preserves the current limitation instead of defining a recoverable target model. |
| At-most-once delivery as the target model | Risks losing business work permanently and conflicts with restart and recovery goals. |
| Hidden indefinite retry loops inside `Messenger` | Hides progress policy in transport adapters and makes failures difficult to observe or test. |
| Receive retry inside `Messenger` | Retries across receive timeouts affect workflow progress and belong to workflow/FSM policy. |
| ACK after full business processing | Couples broker acknowledgement to business transaction success and would redeliver business-invalid but transport-valid messages through the broker. |
| Implement DLQ, correlation IDs, or backend idempotency in ETLP-36 | Expands ETLP-36 beyond bounded Pub/Sub send retry and backoff. |

## Deferred Decisions

- Exact retry budgets and backoff values.
- Additional exception taxonomy beyond `MessageReceiveTimeout` and `MessageSendFailed`.
- Persisted retry counters.
- Correlation IDs and event IDs.
- Dead-letter and poison-message handling.
- Backend messaging abstraction migration.
- Backend and Google Sheets idempotency design.
- Atomic snapshot writes.

## References

- [ADR-0004: FSM, Workflow, Runtime, And Messaging Boundary](0004-fsm-isolation-and-execution-boundaries.md)
- [ADR-0005: Messaging Abstraction And Routing Boundary](0005-messaging-abstraction-and-routing-boundary.md)
- [Messaging Model](../specifications/messaging-model.md)
- [Reliability Model](../specifications/reliability-model.md)
- [Messaging Abstraction Implementation Proposal](../proposals/messaging-abstraction.md)
- [Data Pipeline](../data-pipeline.md)
