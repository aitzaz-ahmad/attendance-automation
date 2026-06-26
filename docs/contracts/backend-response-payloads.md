# Backend Response Payloads

## Metadata

| Field | Value |
| --- | --- |
| Status | Accepted |
| Lifecycle | Active |
| Owner | Backend-to-Pi4 response payload contracts for Attendance Automation v2 |
| Related ADRs | [ADR-0005](../decisions/0005-messaging-abstraction-and-routing-boundary.md), [ADR-0006](../decisions/0006-reliability-and-recovery.md) |
| Related specifications | [Production Readiness](../specifications/production-readiness.md), [Operational Architecture](../specifications/operational-architecture.md), [Runtime Bootstrap](../specifications/runtime-bootstrap.md), [Reliability Model](../specifications/reliability-model.md), [Messaging Model](../specifications/messaging-model.md) |
| Related contracts | [Canonical Attendance Event](canonical-attendance-event.md) |
| Related proposals, if any | None for this canonical owner |
| Related audits | [Architecture Artefact Inventory](../audits/architecture-artefact-inventory.md), [Architecture Readiness Audit](../audits/architecture-readiness-audit.md), [Repository Gap Analysis](../audits/repository-gap-analysis.md) |

## Purpose

This contract defines the canonical backend-to-Pi4 response payload contracts
for Attendance Automation v2.

It owns the JSON field expectations for review-period responses, review-sheet
responses, and last-stored timestamp responses consumed by the Pi4 workflow.
It also defines empty/no-review-period semantics, validation expectations, and
the relationship between backend response payloads and the existing messaging,
reliability, and attendance-event owners.

## Scope

This contract covers:

- review period response payload;
- review sheet response payload;
- last stored timestamp response payload;
- empty/no-review-period response semantics;
- success payload ownership;
- failure/error response expectations;
- JSON field ownership;
- validation expectations;
- compatibility with current Pub/Sub payloads;
- relationship to the [Messaging Model](../specifications/messaging-model.md);
- relationship to the [Reliability Model](../specifications/reliability-model.md);
- relationship to the [Canonical Attendance Event](canonical-attendance-event.md);
- v2 mandatory contract requirements; and
- v3/future contract boundary.

This contract does not cover:

- implementation proposals for backend refactoring;
- transport mechanics owned by the [Messaging Model](../specifications/messaging-model.md);
- attendance event fields owned by the [Canonical Attendance Event](canonical-attendance-event.md);
- retry, checkpoint, or idempotency rules owned by the
  [Reliability Model](../specifications/reliability-model.md); or
- Google Sheets storage abstraction design.

## Ownership

This contract owns backend response payload fields and business response
semantics for backend-to-Pi4 responses.

Adjacent ownership remains:

| Concern | Canonical owner |
| --- | --- |
| Messaging topics, routing model, ACK policy, provider metadata, and timeout semantics | [Messaging Model](../specifications/messaging-model.md) |
| Retry, recovery, checkpoint, idempotency, and visible failure expectations | [Reliability Model](../specifications/reliability-model.md) |
| Attendance event internal model and backend-compatible attendance Pub/Sub payload | [Canonical Attendance Event](canonical-attendance-event.md) |
| Operational runtime roles | [Operational Architecture](../specifications/operational-architecture.md) |
| Pi4 seed-file and bootstrap expectations | [Runtime Bootstrap](../specifications/runtime-bootstrap.md) |

This contract defines JSON mappings after successful transport decoding. It
does not define raw Pub/Sub envelopes, base64 encoding, ACK IDs, topic paths,
subscription names, or provider routing attributes.

## v2 Requirements

Backend-to-Pi4 response payloads must be JSON objects that decode to mappings.
Transport-level decoding and mapping validation are owned by the messaging
adapter. Business-field validation is owned by workflow/action services and
domain-specific layers using this contract.

### Review Period Response

The review-period response is the backend response to a Pi4 review-period
request. It is published on the response topic documented by the
[Messaging Model](../specifications/messaging-model.md).

A valid non-empty review-period response must include:

| Field | Required? | Type | Meaning |
| --- | --- | --- | --- |
| `start_date` | Required | string | Review-period start date in the current backend-compatible date format. |
| `end_date` | Required | string | Review-period end date in the current backend-compatible date format. |

The response may include current compatibility metadata:

| Field | Required? | Type | Meaning |
| --- | --- | --- | --- |
| `month` | Optional | string | Review month label used by the current backend workflow. |
| `duration_weeks` | Optional | number or string | Review-period duration as returned from the review-period source sheet. |
| `sheetname` | Optional | string | Source review-period worksheet name, currently the review year. |
| `sheet_id` | Optional | string | Existing review sheet identifier when already known. |

Example valid non-empty response:

```json
{
  "month": "May",
  "start_date": "05/01/2026",
  "end_date": "05/31/2026",
  "duration_weeks": 4,
  "sheetname": "2026"
}
```

The empty JSON object is the valid v2 no-review-period business response:

```json
{}
```

An empty review-period response means that no review period is currently
available. It is not a transport timeout. The workflow/FSM decides whether to
retry, raise the final alarm path, enter deep sleep, recover, or fail according
to the [Reliability Model](../specifications/reliability-model.md).

### Review Sheet Response

The review-sheet response is the backend response to a Pi4 review-sheet request.

A valid review-sheet response must include:

| Field | Required? | Type | Meaning |
| --- | --- | --- | --- |
| `id` | Required | string | Google Sheets identifier for the attendance review sheet. |
| `name` | Required | string | Human-readable review sheet name/title. |

Example valid response:

```json
{
  "id": "1AbCDefGhIjKlMnOpQrStUvWxYz",
  "name": "Attendance Review - May 2026"
}
```

The review-sheet response is a success payload only after the backend has
created or found the required review sheet and can return both required fields.
Missing `id` or `name` is a business-invalid response.

### Last Stored Timestamp Response

The last-stored timestamp response is the backend response after attendance
records have been stored and review-output updates have completed.

A valid last-stored timestamp response must include:

| Field | Required? | Type | Meaning |
| --- | --- | --- | --- |
| `timestamp` | Required | string | Latest attendance timestamp confirmed by backend storage in the current backend-compatible timestamp format. |

The current backend-compatible response also includes:

| Field | Required? | Type | Meaning |
| --- | --- | --- | --- |
| `device_id` | Optional for business validation; currently emitted | string | Device/site identity used by the current backend publisher. |

Example valid response:

```json
{
  "device_id": "dk",
  "timestamp": "27-05-2026 08:59:12"
}
```

The `timestamp` value is consumed by the Pi4 runtime as the latest backend
storage watermark. The current timestamp string format remains compatible with
the runtime state and attendance payload formats already used by the repository.

Targeted routing for this response is owned by the
[Messaging Model](../specifications/messaging-model.md). Current Google Pub/Sub
publisher metadata uses `location=device_id`, but this contract does not make
provider metadata part of the JSON payload contract.

### Success Payload Ownership

These payloads are success payload contracts. A backend function must not
publish a success response before the durable side effects required for that
response have completed.

Success means:

- review-period response: the backend has either found the next review period
  and returned its fields, or has determined that no review period is currently
  available and returned `{}`;
- review-sheet response: the backend has found or created the review sheet and
  returned `id` and `name`;
- last-stored timestamp response: the backend has completed the required
  storage/review-output updates and returned the latest confirmed `timestamp`.

### Failure And Error Expectations

V2 does not define a backend-to-Pi4 error payload envelope.

Backend processing failures must be visible and must not be hidden by publishing
success payloads with missing or placeholder fields. If a backend function
cannot complete the side effects required for a success response, the failure
must surface through backend failure behaviour, logs, timeout/recovery on the
Pi4 side, or a later accepted error contract.

Pi4 workflow/action services must reject malformed business payloads as
business-invalid responses and translate them into explicit workflow outcomes.
Business-invalid payloads are not transport timeouts.

### JSON Field Ownership

This contract owns the JSON fields listed in this document for backend-to-Pi4
responses. It does not own attendance event request fields, backend request
payloads, provider envelopes, or raw Google Pub/Sub attributes.

Renaming, removing, or changing required response fields is a contract
migration and must preserve compatibility across backend publishers, messaging,
workflow consumers, tests, and documentation.

### Validation Expectations

Validation must distinguish:

- transport-invalid messages, owned by the messaging adapter;
- transport-valid but business-invalid response payloads, owned by
  workflow/action-service validation;
- valid empty review-period business responses, owned by this contract and the
  workflow/FSM behaviour described by the [Reliability Model](../specifications/reliability-model.md);
- backend failures before success publication, owned by backend processing and
  reliability expectations; and
- attendance payload validation, owned by the
  [Canonical Attendance Event](canonical-attendance-event.md) and transformation
  owners.

Business validation must reject:

- non-mapping decoded payloads after transport validation has passed them up;
- review-period responses missing `start_date` or `end_date` when non-empty;
- review-sheet responses missing `id` or `name`;
- last-stored timestamp responses missing `timestamp`; and
- timestamp or date values that cannot be parsed by the consuming v2 workflow
  path.

Consumers must ignore unknown JSON fields unless a future accepted contract
explicitly states otherwise. This applies only after successful transport
decoding and business validation. It does not introduce version fields,
schema-evolution mechanisms, or alternate payload shapes.

### Compatibility With Current Pub/Sub Payloads

This contract preserves the current backend-compatible response shapes:

| Response | JSON payload compatibility | Routing compatibility |
| --- | --- | --- |
| Review period | `{}` or mapping with `start_date` and `end_date` plus metadata | Broadcast response topic |
| Review sheet | mapping with `id` and `name` | Broadcast response topic |
| Last stored timestamp | mapping with `timestamp` and currently emitted `device_id` | Targeted response topic |

Topic names, broadcast/targeted routing, ACK policy, and provider metadata are
owned by the [Messaging Model](../specifications/messaging-model.md).

## v3 / Future Boundary

The following concerns are outside v2 response payload contracts unless
promoted by a later accepted repository authority:

- public error response envelope;
- correlation identifiers;
- request/response envelope redesign;
- schema version fields;
- event identifiers;
- richer canonical wire payloads;
- backend idempotency keys;
- poison-message or dead-letter payloads;
- PostgreSQL storage response contracts; and
- backend publisher migration to depend on `Messenger`.

Undecided payload evolution is implementation-defined until a future accepted
contract, specification, ADR, or proposal owns it.

## Non-Goals

This contract does not:

- define backend refactoring work;
- define transport mechanics;
- define Pub/Sub topics or subscriptions;
- define ACK policy;
- define attendance event payload fields;
- define Google Sheets storage abstraction;
- define retry budgets or recovery transitions;
- define a v2 error envelope; or
- update README files, diagrams, GitHub issues, or milestones.

## Validation

Changes to this contract must validate that:

- response payload fields remain compatible with current backend publishers and
  Pi4 workflow consumers;
- valid empty review-period responses remain distinct from receive timeouts;
- success payloads are not documented as valid before required backend side
  effects complete;
- transport, reliability, and attendance-event ownership are referenced rather
  than duplicated;
- local Markdown links resolve; and
- repository validation follows [Validation Gates](../workflows/validation-gates.md).
