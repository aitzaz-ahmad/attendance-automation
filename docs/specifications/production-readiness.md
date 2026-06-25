# Production Readiness Specification

## Metadata

| Field | Value |
| --- | --- |
| Status | Accepted |
| Lifecycle | Active |
| Owner | Production-readiness criteria for Attendance Automation v2 |
| Related ADRs | [ADR-0001](../decisions/0001-repo-modernisation-design.md), [ADR-0002](../decisions/0002-model-adoption-principles.md), [ADR-0003](../decisions/0003-device-and-transformation-layer-boundaries.md), [ADR-0004](../decisions/0004-fsm-isolation-and-execution-boundaries.md), [ADR-0005](../decisions/0005-messaging-abstraction-and-routing-boundary.md), [ADR-0006](../decisions/0006-reliability-and-recovery.md) |
| Related specifications | [Biometric Device Configuration](biometric-device-configuration.md), [BiometricDevice](biometric-device.md), [BiometricDeviceFactory](device-factory.md), [Messaging Model](messaging-model.md), [Reliability Model](reliability-model.md), [TransformationStrategy](transformation-strategy.md), [ZKTecoDevice](zkteco-device.md) |
| Related contracts | [Canonical Attendance Event](../contracts/canonical-attendance-event.md) |
| Related proposals | [Ingestion Modularisation](../proposals/ingestion-modularisation.md), [Device Layer Abstraction](../proposals/device-layer-abstraction.md), [Transformation Layer](../proposals/transformation-layer.md), [Messaging Abstraction Implementation Proposal](../proposals/messaging-abstraction.md) |
| Related workflows | [Issue Lifecycle](../workflows/issue-lifecycle.md), [Validation Gates](../workflows/validation-gates.md) |

## Purpose

This specification defines the project-specific benchmark for evaluating
whether Attendance Automation v2 is production-ready.

It defines readiness criteria only. It does not assess whether the repository
currently satisfies those criteria, redesign the architecture, or create an
implementation roadmap.

Later audits, including ETLP-101, ETLP-102, and ETLP-103, should use this
specification as the benchmark when classifying v2 readiness evidence.

## Scope

This specification covers production-readiness criteria for:

- architecture readiness;
- implementation readiness;
- testing and verification;
- CI/CD and quality gates;
- runtime configuration;
- operational deployment;
- reliability and recovery; and
- documentation readiness.

This specification does not cover:

- commercial SLA commitments;
- enterprise monitoring dashboards;
- full DevOps automation;
- advanced v3 features;
- redesign of the existing architecture;
- detailed payload schemas owned by contracts;
- detailed messaging or reliability rules owned by existing specifications; or
- GitHub issue creation, milestone changes, or roadmap mutation.

## Production Readiness Target

Attendance Automation v2 is production-ready when it is practical and
portfolio-grade:

- deployable by a competent developer or operator;
- architecturally coherent with the accepted ADRs and active architecture
  documentation;
- testable through repository validation and executable tests;
- documented sufficiently for development, validation, deployment, and
  recovery;
- recoverable from expected interruptions within the documented v2 reliability
  model;
- capable of safe replay within the documented v2 reliability model; and
- explicit about controlled failures rather than hiding invalid inputs,
  unsupported configuration, or failed side effects.

The v2 target is not enterprise-grade production. It does not require commercial
SLAs, full infrastructure-as-code, multi-site operational dashboards, advanced
replay tooling, or a complete v3 idempotency model.

## Readiness Categories

### 1. Architecture Readiness

Canonical owner document:
[Architecture](../architecture.md), with rationale owned by accepted ADRs.

Criteria:

- Runtime topology, component placement, messaging/backend/storage placement,
  and documented limitations are coherent with the active architecture
  document.
- ADR constraints are reflected in implementation boundaries.
- Architecture documentation distinguishes current v2 behaviour from future
  storage, payload, deployment, or operational ambitions.

V2 minimum:

- The implemented runtime path follows the documented Pi4 runtime, workflow,
  messaging, backend, and Google Sheets topology.
- Accepted ADRs are not contradicted by implementation, tests, or active
  specifications.
- Known future boundaries remain labelled as future rather than implied v2
  guarantees.

V3/future boundary:

- New topology, PostgreSQL-first storage, enterprise deployment architecture,
  and multi-site operational architecture remain outside v2 readiness unless a
  later accepted ADR or specification promotes them.

### 2. Documentation Readiness

Canonical owner document:
[Documentation Governance](../governance/documentation.md).

Criteria:

- Active behaviour is documented by the correct owner type: contracts for data,
  specifications for required behaviour, ADRs for rationale, architecture for
  responsibility placement, and workflows for process.
- Documents outside the canonical owner reference the owner instead of
  duplicating detailed rules.
- Historical or evolved material is labelled so it cannot compete with active
  authority.

V2 minimum:

- Operators and maintainers can locate the accepted v2 architecture,
  behavioural specifications, data contracts, validation workflow, and
  deployment/provisioning instructions.
- Documentation required for setup, validation, configuration, operation, and
  recovery exists and uses the repository lifecycle taxonomy.

V3/future boundary:

- Full operations manuals, commercial runbooks, enterprise monitoring
  playbooks, and exhaustive support procedures are future concerns unless
  explicitly promoted into v2 scope.

### 3. Implementation Readiness

Canonical owner documents:
active specifications, accepted ADRs, and the active architecture document.

Criteria:

- Implementation modules satisfy the public contracts defined by their
  canonical specifications.
- Runtime composition respects the boundaries between device access,
  transformation, messaging, persistence, workflow/FSM, backend handlers, and
  deployment wrappers.
- Unsupported inputs, missing configuration, invalid payloads, and failed side
  effects fail explicitly.

V2 minimum:

- The runnable v2 path can execute through the documented ingestion,
  messaging, backend, and Google Sheets review-output flow.
- Compatibility entry points remain coherent with the architecture while
  delegating to the canonical package modules.
- Implementation does not depend on provider-specific details outside their
  owner boundaries.

V3/future boundary:

- Additional device vendors, plugin registries, generic dependency-injection
  frameworks, and new execution platforms are not required for v2 readiness.

### 4. Testing Readiness

Canonical owner documents:
active specifications and [Validation Gates](../workflows/validation-gates.md).

Criteria:

- Tests verify the required behaviours claimed by v2 specifications,
  contracts, ADRs, and workflow boundaries.
- Tests avoid live biometric hardware or live cloud dependencies unless the
  relevant workflow explicitly defines those as integration tests.
- Production-readiness claims are supported by executable tests, not only by
  documentation.

V2 minimum:

- Unit tests cover v2 domain, transformation, messaging, configuration,
  workflow/FSM, persistence, and backend validation behaviours where those
  behaviours are part of production-readiness claims.
- Tests execute in CI through the repository's required validation workflow.
- Test evidence can be mapped back to issue acceptance criteria and canonical
  owners.

V3/future boundary:

- Full end-to-end hardware tests, cloud-hosted staging environments, load
  testing, chaos testing, and long-running soak tests are future concerns unless
  separately accepted into v2 scope.

### 5. CI/CD Readiness

Canonical owner document:
[Validation Gates](../workflows/validation-gates.md).

Criteria:

- Required validation commands are defined in one workflow owner and enforced
  before review completion.
- CI runs the same mandatory quality gates expected locally.
- Validation failures are explicit and are not suppressed or reported as
  success.

V2 minimum:

- Linting, formatting, typing, whitespace checks, and executable tests required
  for v2 readiness run in CI.
- The validation workflow documents mandatory commands and distinguishes any
  pending commands from active requirements.
- Completion reports include validation results and an acceptance-criteria
  audit.

V3/future boundary:

- Release automation, deployment promotion pipelines, environment approvals,
  rollback automation, and full continuous delivery are not required for v2
  readiness.

### 6. Configuration Readiness

Canonical owner documents:
[Biometric Device Configuration](biometric-device-configuration.md), the active
architecture document, and any future accepted runtime configuration owner.

Criteria:

- Required runtime configuration is documented, validated at startup, and tied
  to the component that owns it.
- Vendor-specific biometric configuration remains behind the vendor-options
  boundary.
- Deterministic configuration failures fail fast and visibly.

V2 minimum:

- Biometric device configuration, runtime identity, messaging configuration,
  backend configuration, storage/review-output configuration, and local runtime
  file expectations are documented or referenced from the appropriate owner.
- Runtime seed/bootstrap expectations define which files or values must be
  provided, which may be generated, and how invalid or missing inputs fail.

V3/future boundary:

- Centralised secrets management, environment-specific provisioning systems,
  dynamic configuration services, and enterprise policy enforcement are future
  concerns unless promoted into a v2 owner.

### 7. Messaging Readiness

Canonical owner document:
[Messaging Model](messaging-model.md).

Criteria:

- Workflow code depends on `MessageTopic`, `TopicResolver`, and `Messenger`
  contracts rather than raw provider topics, subscriptions, ACK IDs, or
  provider envelopes.
- Send and receive semantics, routing policy, transport validation, ACK policy,
  and project-owned messaging exceptions follow the messaging model.
- Messaging does not own payload schemas, workflow retry, FSM transition
  policy, checkpointing, or business idempotency.

V2 minimum:

- The messaging abstraction is implemented according to the Messaging Model.
- Google Pub/Sub remains isolated behind `GooglePubSubMessenger`.
- Broadcast and targeted response routing preserve the documented routing
  policy.
- Receive operations are bounded and expose `MessageReceiveTimeout`; exhausted
  send attempts expose `MessageSendFailed`.

V3/future boundary:

- DLQ and poison-message handling, correlation identifiers, richer
  request/response envelopes, backend publisher migration beyond v2 scope, and
  exactly-once transport claims are deferred.

### 8. Storage Readiness

Canonical owner document:
an accepted active storage abstraction specification or contract once created.
Architecture owns storage placement; reliability owns recovery interaction with
local persistence.

Criteria:

- Storage responsibilities, durable side effects, local runtime files, review
  output, and storage idempotency expectations are owned by explicit
  documentation.
- Storage implementations surface failures explicitly and do not silently
  convert failed writes into successful workflow outcomes.
- Storage behaviour supports safe replay within the v2 reliability model.

V2 minimum:

- Storage abstraction is specified and implemented according to its canonical
  owner once created.
- Google Sheets remains the documented v2 review-output surface unless a later
  accepted owner changes that boundary.
- Local persistence required for checkpoint/recovery is documented and aligned
  with the Reliability Model.

V3/future boundary:

- PostgreSQL storage, advanced storage migration tooling, stronger backend
  idempotency beyond the documented v2 model, and Google Sheets deduplication
  strategy beyond v2 are deferred.

### 9. Runtime State, Checkpoint, And Recovery Readiness

Canonical owner document:
[Reliability Model](reliability-model.md), with checkpoint rationale in
[ADR-0004](../decisions/0004-fsm-isolation-and-execution-boundaries.md).

Criteria:

- FSM owns checkpoint eligibility, workflow/runtime owns checkpoint timing, and
  persistence owns serialisation and file I/O.
- Waiting states are not persisted as restart targets.
- Recovery resumes from the last checkpointed non-waiting state.
- Repeated attempts after interruption are tolerated within the documented v2
  reliability model.

V2 minimum:

- FSM isolation follows ADR-0004.
- Checkpoint/recovery semantics are preserved.
- Runtime startup loads required persisted state and bootstrap inputs through
  documented owners.
- Safe replay is supported without claiming exactly-once transport or
  exactly-once business processing.

V3/future boundary:

- Persisted retry counters, advanced replay/recovery tooling, atomic snapshot
  redesign, correlation IDs, and a stronger final idempotency model are future
  concerns unless promoted into v2 scope.

### 10. Backend Ingestion Readiness

Canonical owner documents:
[Canonical Attendance Event](../contracts/canonical-attendance-event.md),
[Reliability Model](reliability-model.md), [Messaging Model](messaging-model.md),
and active backend/storage owners where they exist.

Criteria:

- Backend handlers validate business payloads separately from messaging
  transport validation.
- Backend failures are visible and do not publish success responses before
  required durable side effects complete.
- Backend processing tolerates duplicate delivery or repeated workflow attempts
  within the documented v2 model.

V2 minimum:

- Backend ingestion validates payloads and handles controlled failures.
- Attendance payload shape and serialisation expectations reference the
  Canonical Attendance Event contract instead of redefining fields.
- Backend responses consumed by the Pi4 workflow have documented validation
  expectations.

V3/future boundary:

- Rich canonical wire-payload evolution, event IDs, correlation IDs, stronger
  backend idempotency beyond the v2 model, and full poison-message workflows
  are deferred.

### 11. Controlled Failure And Validation Readiness

Canonical owner documents:
active specifications, active contracts, and [Reliability Model](reliability-model.md).

Criteria:

- Deterministic failures are rejected explicitly.
- Transient failures are bounded by the owner layer that is allowed to retry
  them.
- Business-invalid but transport-valid messages are handled as workflow or
  backend outcomes, not hidden as successful processing.

V2 minimum:

- Invalid configuration, unsupported vendors, invalid transformation input,
  malformed business payloads, receive timeouts, exhausted send attempts,
  storage failures, and backend write failures are observable through explicit
  exceptions, outcomes, validation errors, or logs.
- Validation behaviour is covered by tests where it is part of a v2 readiness
  claim.

V3/future boundary:

- Exhaustive exception taxonomies, operator-guided remediation tools,
  poison-message redrive, and automated compensation workflows are future
  concerns.

### 12. Logging And Diagnostics Readiness

Canonical owner documents:
accepted architecture and implementation-level logging utilities. No separate
logging specification is created by this document.

Criteria:

- Runtime, workflow, messaging, backend, configuration, and storage failures are
  diagnosable by a competent developer/operator.
- Logs identify the responsible component and enough context to distinguish
  configuration, transport, business validation, persistence, and backend
  failures.
- Diagnostics do not require changing architecture boundaries or exposing
  provider internals through public project abstractions.

V2 minimum:

- Important startup, validation, send/receive, workflow transition, checkpoint,
  backend processing, and storage failure paths emit useful diagnostics.
- Logs avoid treating expected controlled failures as silent success.

V3/future boundary:

- Enterprise observability platforms, distributed tracing, alert routing,
  multi-site dashboards, and commercial incident-management workflows are
  future concerns.

### 13. Operational Deployment Readiness

Canonical owner document:
an accepted active deployment or operations owner once created. Architecture
owns deployment placement; workflow documents own validation process.

Criteria:

- A competent developer/operator can provision required local files, deploy or
  run the Pi4 path, deploy backend wrappers, configure Google Pub/Sub and Google
  Sheets dependencies, run validation, and recover from expected interruptions.
- Deployment documentation distinguishes required v2 setup from optional or
  future automation.
- Operational steps preserve documented architecture and reliability
  boundaries.

V2 minimum:

- Deployment/provisioning documentation exists for the v2 path.
- Required runtime seed/bootstrap expectations are defined.
- Manual but repeatable deployment is acceptable when the commands, inputs, and
  validation checks are documented.

V3/future boundary:

- Full infrastructure-as-code, automated environment promotion, production
  secrets platforms, blue/green deployment, commercial rollback guarantees, and
  enterprise monitoring dashboards are deferred.

### 14. GitHub Roadmap Readiness

Canonical owner document:
[Issue Lifecycle](../workflows/issue-lifecycle.md), with validation completion
governed by [Validation Gates](../workflows/validation-gates.md).

Criteria:

- Production-readiness audits classify evidence against this specification
  without creating or mutating GitHub issues.
- Issue acceptance criteria, labels, milestones, and project states are used as
  traceability evidence only when verified through the relevant workflow.
- Completion claims require acceptance-criteria audit and validation evidence.

V2 minimum:

- ETLP-101, ETLP-102, and ETLP-103 can map findings to the categories and
  statuses defined in this specification.
- Roadmap evidence distinguishes implemented criteria, planned criteria, missing
  criteria, and explicitly deferred v3/future work.

V3/future boundary:

- Automated roadmap generation, release train management, commercial portfolio
  reporting, and GitHub project automation beyond the documented workflow are
  future concerns.

## Definition Of Done Relationship

Production readiness requires implementation to satisfy:

- issue acceptance criteria;
- related specifications and contracts;
- ADR constraints;
- tests;
- documentation updates; and
- validation gates.

If verification against the Definition of Done fails, production readiness has
not been achieved.

The Definition of Done relationship is cumulative. Passing one category does
not compensate for failure against a canonical contract, accepted ADR, required
test, documentation owner, or validation workflow.

## V2 Mandatory Capabilities

The following capabilities must exist for Attendance Automation v2 production
readiness:

- messaging abstraction implemented according to the
  [Messaging Model](messaging-model.md);
- storage abstraction specified and implemented according to its canonical owner
  once created;
- FSM isolation according to
  [ADR-0004](../decisions/0004-fsm-isolation-and-execution-boundaries.md);
- checkpoint/recovery semantics preserved according to the
  [Reliability Model](reliability-model.md);
- backend ingestion validates payloads and handles controlled failures;
- tests execute in CI;
- deployment/provisioning documentation exists;
- runtime seed/bootstrap expectations are defined;
- biometric device configuration fails fast according to the
  [Biometric Device Configuration](biometric-device-configuration.md)
  specification;
- transformation output and filtering follow the
  [TransformationStrategy](transformation-strategy.md) specification; and
- attendance payload shape references the
  [Canonical Attendance Event](../contracts/canonical-attendance-event.md)
  contract.

## V3 / Future Work Boundary

The following capabilities are explicitly deferred beyond v2 production
readiness unless later promoted by accepted repository authority:

- enterprise monitoring dashboards;
- full infrastructure-as-code;
- DLQ and poison-message handling;
- event/correlation IDs;
- stronger backend idempotency beyond the documented v2 model;
- Google Sheets deduplication strategy beyond v2;
- advanced replay/recovery tooling;
- multi-site operational dashboards;
- full commercial production SLA;
- PostgreSQL-first persistence;
- automated release promotion; and
- commercial support runbooks.

Deferred items may be valuable future work. They must not be treated as v2
production-readiness failures when an audit is explicitly evaluating only the
v2 benchmark defined by this specification.

## Readiness Classification

Later audits shall use only these readiness statuses when classifying evidence
against this specification.

| Status | Meaning |
| --- | --- |
| Ready | Evidence shows the v2 criterion is satisfied by implementation, tests, documentation, and validation where applicable. |
| Partially Ready | Evidence satisfies some but not all v2 requirements for the criterion, or satisfies the requirement without enough test, documentation, or validation support. |
| Planned | Repository authority identifies the criterion as intended or in progress, but readiness evidence is not complete. |
| Missing | No sufficient evidence was found for a v2 criterion that this specification requires. |
| Deferred | The criterion belongs to the v3/future boundary or another explicitly deferred scope and is not required for v2 readiness. |

Status use in later audits:

- Classify the smallest practical criterion, not an entire subsystem, when the
  evidence is mixed.
- Cite the canonical owner document before classifying a criterion as failed.
- Use `Deferred` only when the item is outside the v2 boundary or explicitly
  deferred by accepted repository authority.
- Use `Planned` only when verified repository evidence shows planned scope.
- Do not use these statuses to create implementation tasks automatically.

## Non-Goals

Production readiness does not mean:

- enterprise-grade operations;
- commercial SLA readiness;
- exactly-once transport delivery;
- exactly-once business processing;
- fully automated cloud infrastructure;
- support for every biometric device vendor;
- PostgreSQL production storage;
- a redesigned architecture;
- absence of all future work;
- hidden fallback behaviour for invalid inputs; or
- completion of v3 reliability, observability, deployment, or storage ambitions.

For this project, production readiness means that the v2 system is coherent,
testable, documented, deployable by a competent developer/operator, and
recoverable within the accepted v2 reliability model.

## Validation

Before this specification or a later audit reports production-readiness
completion, verification must confirm that:

- the document or audit defines criteria only when it is not explicitly tasked
  with assessment;
- current status is not assessed unless the task is an audit;
- roadmap recommendations or implementation tasks are not created unless
  explicitly requested;
- existing canonical owners are referenced instead of duplicated;
- v2 criteria and v3/future boundaries remain separate;
- validation follows [Validation Gates](../workflows/validation-gates.md); and
- completion reporting confirms the modified-file scope.
