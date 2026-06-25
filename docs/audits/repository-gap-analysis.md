# Repository Gap Analysis

## Metadata

| Field | Value |
| --- | --- |
| Status | Accepted |
| Lifecycle | Active |
| Owner | Repository gap analysis and implementation planning for Attendance Automation v2 |
| Related audits | [Architecture Artefact Inventory](architecture-artefact-inventory.md), [Architecture Readiness Audit](architecture-readiness-audit.md) |
| Related specifications | [Production Readiness](../specifications/production-readiness.md), [Reliability Model](../specifications/reliability-model.md), [Messaging Model](../specifications/messaging-model.md), [Biometric Device Configuration](../specifications/biometric-device-configuration.md), [BiometricDevice](../specifications/biometric-device.md), [BiometricDeviceFactory](../specifications/device-factory.md), [TransformationStrategy](../specifications/transformation-strategy.md), [ZKTecoDevice](../specifications/zkteco-device.md) |
| Capability matrix | [Architecture Capability Matrix](../architecture-capability-matrix.md) |
| Related architecture | [Architecture](../architecture.md), [Data Pipeline](../data-pipeline.md) |
| Related ADRs | [ADR-0001](../decisions/0001-repo-modernisation-design.md), [ADR-0002](../decisions/0002-model-adoption-principles.md), [ADR-0003](../decisions/0003-device-and-transformation-layer-boundaries.md), [ADR-0004](../decisions/0004-fsm-isolation-and-execution-boundaries.md), [ADR-0005](../decisions/0005-messaging-abstraction-and-routing-boundary.md), [ADR-0006](../decisions/0006-reliability-and-recovery.md) |
| Related contracts | [Canonical Attendance Event](../contracts/canonical-attendance-event.md) |
| Related proposals | [Ingestion Modularisation](../proposals/ingestion-modularisation.md), [Device Layer Abstraction](../proposals/device-layer-abstraction.md), [Transformation Layer](../proposals/transformation-layer.md), [Messaging Abstraction Implementation Proposal](../proposals/messaging-abstraction.md) |
| Related workflows | [Issue Lifecycle](../workflows/issue-lifecycle.md), [Branch Strategy](../workflows/branch-strategy.md), [Commit Conventions](../workflows/commit-conventions.md), [Validation Gates](../workflows/validation-gates.md), [Local Development](../workflows/local-development.md) |
| GitHub verification | Verified with `gh issue view 103`, `gh issue list`, and `gh api repos/:owner/:repo/milestones --paginate` during ETLP-103. |

## Purpose

This document converts the [Architecture Readiness Audit](architecture-readiness-audit.md) into
implementation planning work for Attendance Automation v2.

It is action-oriented, but it does not redesign the architecture, create new roadmap work, mutate GitHub
issues, or change milestones. It identifies the remaining gaps between the current repository and the agreed
v2 production-ready target, then maps those gaps to the existing roadmap.

## Methodology

Gaps are identified by comparing:

- the current tracked repository;
- the [Production Readiness Specification](../specifications/production-readiness.md);
- the [Architecture Capability Matrix](../architecture-capability-matrix.md);
- the [Architecture Readiness Audit](architecture-readiness-audit.md); and
- the live GitHub roadmap verified with `gh`.

The status values in this document reuse the production-readiness classification from the capability matrix:
`Ready`, `Partially Ready`, `Planned`, `Missing`, and `Deferred`.

## Overall Repository Status

Current repository maturity: **architecture-led implementation baseline**. Foundation, device abstraction,
configuration, transformation, canonical attendance event ownership, documentation governance, and validation
workflow are established.

Overall implementation readiness: **incomplete**. Mandatory v2 execution surfaces remain planned or partial,
especially messaging, storage, FSM isolation, executable test coverage, CI test execution, runtime
bootstrap expectations, and deployment/provisioning documentation.

Overall documentation readiness: **mostly complete but not closed**. The ownership model, architecture,
ADRs, core specifications, and canonical attendance event contract are present, but missing active owners
remain for storage abstraction, backend response payloads, operational architecture, and deployment
documentation.

Overall production-readiness maturity: **partially ready**. The architecture is stable enough to guide
implementation, but the repository is not production-ready until the remaining roadmap-backed implementation,
testing, CI, documentation, and operational gaps are closed.

## Gap Analysis By Capability

### Device abstraction

- Status: Ready
- Current evidence: Biometric device abstraction is owned by
  [BiometricDevice](../specifications/biometric-device.md), with ZKTeco implementation and construction
  boundaries covered by supporting specifications and ADR-0003.
- Remaining gaps: No remaining v2 capability gap identified by the capability matrix.
- Existing GitHub coverage: ETLP-25 through ETLP-28 are closed.
- Implementation readiness: Ready for downstream implementation to consume without redesigning the device
  boundary.

### Biometric device configuration

- Status: Ready
- Current evidence: [Biometric Device Configuration](../specifications/biometric-device-configuration.md)
  owns configuration shape, vendor option ownership, startup validation, and fail-fast behaviour.
- Remaining gaps: Broader runtime bootstrap documentation is still tracked separately under runtime
  seed/bootstrap and operational readiness.
- Existing GitHub coverage: ETLP-22 and ETLP-25 through ETLP-28 are closed.
- Implementation readiness: Ready for runtime composition and validation use.

### Transformation strategy

- Status: Ready
- Current evidence: [TransformationStrategy](../specifications/transformation-strategy.md) owns
  transformation behaviour, normalisation, validation, canonicalisation, and filtering.
- Remaining gaps: No remaining v2 transformation capability gap identified by the capability matrix.
- Existing GitHub coverage: ETLP-29 through ETLP-32 are closed.
- Implementation readiness: Ready, with future work limited to test expansion already covered by the roadmap.

### Canonical attendance event contract

- Status: Ready
- Current evidence: [Canonical Attendance Event](../contracts/canonical-attendance-event.md) owns the
  internal attendance event model, backend-compatible Pub/Sub payload, serialisation, and schema evolution
  notes.
- Remaining gaps: Runtime enforcement of the canonical event shape is still an architecture limitation, but
  the contract owner exists.
- Existing GitHub coverage: ETLP-14, ETLP-19, and ETLP-30 are closed.
- Implementation readiness: Ready as the data contract for transformation and backend payload alignment.

### Runtime state

- Status: Partially Ready
- Current evidence: [Architecture](../architecture.md) owns runtime placement and
  [Reliability Model](../specifications/reliability-model.md) owns recovery behaviour. Runtime state model
  evidence exists in implementation and tests.
- Remaining gaps: Snapshot persistence and final FSM isolation remain incomplete.
- Existing GitHub coverage: ETLP-24 is closed; ETLP-45 is open.
- Implementation readiness: Partially ready until runtime state persistence and FSM-related recovery work are
  completed.

### Checkpoint and recovery

- Status: Partially Ready
- Current evidence: [Reliability Model](../specifications/reliability-model.md) owns checkpoint and recovery
  behaviour, with ADR-0004 and ADR-0006 providing rationale.
- Remaining gaps: FSM isolation, transition ownership, and snapshot persistence are still roadmap work.
- Existing GitHub coverage: ETLP-42 through ETLP-45 are open.
- Implementation readiness: Partially ready; implementation can proceed against the reliability contract, but
  the v2 checkpoint/recovery path is not complete.

### FSM isolation

- Status: Planned
- Current evidence: ADR-0004 owns FSM, workflow, runtime, and execution-boundary rationale.
- Remaining gaps: FSM module extraction, state enum, transitions, and snapshot persistence remain open.
- Existing GitHub coverage: ETLP-42 through ETLP-45 are open.
- Implementation readiness: Planned; this is a mandatory v2 implementation gap.

### Messaging abstraction

- Status: Planned
- Current evidence: [Messaging Model](../specifications/messaging-model.md) owns `MessageTopic`,
  `TopicResolver`, `Messenger`, receive timeout, send failure, routing, ACK policy, and provider boundary
  expectations.
- Remaining gaps: Messaging interface, Google Pub/Sub adapter, migration of messaging logic, and retry
  handling remain open.
- Existing GitHub coverage: ETLP-33 through ETLP-36 are open.
- Implementation readiness: Planned; this is the next major implementation gap in the existing roadmap.

### Messaging routing

- Status: Planned
- Current evidence: [Messaging Model](../specifications/messaging-model.md) owns routing policy and separates
  messaging transport from payload schemas, workflow retry, FSM transition policy, checkpointing, and business
  idempotency.
- Remaining gaps: Routing enforcement through the messaging abstraction is not yet implemented.
- Existing GitHub coverage: ETLP-33 through ETLP-36 are open.
- Implementation readiness: Planned; routing readiness depends on the messaging milestone.

### Backend ingestion

- Status: Partially Ready
- Current evidence: [Canonical Attendance Event](../contracts/canonical-attendance-event.md),
  [Reliability Model](../specifications/reliability-model.md), and
  [Messaging Model](../specifications/messaging-model.md) define the relevant payload, reliability, and
  transport boundaries. [Architecture](../architecture.md) identifies backend wrappers and implementation
  modules.
- Remaining gaps: Backend response payload ownership is incomplete, and backend readiness still depends on
  messaging and storage completion.
- Existing GitHub coverage: ETLP-14, ETLP-16, ETLP-17, ETLP-19, and ETLP-30 are closed; ETLP-104 is open.
- Implementation readiness: Partially ready; implementation work must preserve existing backend boundaries and
  close response-payload documentation gaps through existing coverage.

### Backend response payloads

- Status: Missing
- Current evidence: The architecture artefact inventory records no tracked standalone contract for
  review-period, review-sheet response, or last-stored timestamp response schemas.
- Remaining gaps: Active response payload ownership is missing.
- Existing GitHub coverage: No direct standalone issue was found; ETLP-104 covers remaining architecture
  documentation at milestone level.
- Implementation readiness: Missing; v2 readiness needs documented validation expectations for backend
  responses consumed by the Pi4 workflow.

### Storage abstraction

- Status: Planned
- Current evidence: [Architecture](../architecture.md) records Google Sheets as current persistence and
  PostgreSQL as future storage; the artefact inventory records no active storage abstraction specification or
  contract.
- Remaining gaps: Storage interface, adapters, schema, and migration work remain open. The storage abstraction
  owner is not yet present.
- Existing GitHub coverage: ETLP-37 through ETLP-41 are open.
- Implementation readiness: Planned; implementation must preserve the v2 Google Sheets boundary and the
  documented future PostgreSQL boundary.

### Google Sheets review output

- Status: Partially Ready
- Current evidence: [Architecture](../architecture.md) owns placement for Google Sheets review output, and
  [Data Pipeline](../data-pipeline.md) describes the current flow.
- Remaining gaps: Google Sheets adapter work remains open under the storage abstraction milestone, and no
  standalone storage contract exists.
- Existing GitHub coverage: ETLP-16 is closed; ETLP-38 is open.
- Implementation readiness: Partially ready; the current surface exists, but the abstraction and adapter work
  are incomplete.

### Configuration management

- Status: Partially Ready
- Current evidence: Device configuration is specified and runtime placement is documented by the architecture.
- Remaining gaps: Runtime identity, messaging configuration, backend configuration, storage/review-output
  configuration, and seed/bootstrap expectations need final documentation alignment.
- Existing GitHub coverage: ETLP-22 and ETLP-25 through ETLP-28 are closed.
- Implementation readiness: Partially ready; device configuration is ready, while broader runtime
  configuration remains partial.

### Controlled failure handling

- Status: Partially Ready
- Current evidence: [Reliability Model](../specifications/reliability-model.md) owns visible failures and
  bounded operations, with related validation ownership in active specifications and contracts.
- Remaining gaps: Bounded messaging retry and test evidence for controlled failure paths remain open.
- Existing GitHub coverage: ETLP-36 and ETLP-46 through ETLP-49 are open.
- Implementation readiness: Partially ready; owner documents exist, but implementation and tests are not
  complete.

### Validation

- Status: Partially Ready
- Current evidence: [Validation Gates](../workflows/validation-gates.md) owns repository validation. The
  current validation runner covers Ruff, Black, MyPy, and `git diff --check`.
- Remaining gaps: Executable tests are still pending in the validation workflow and CI.
- Existing GitHub coverage: ETLP-7 through ETLP-9 and ETLP-78 are closed; ETLP-46 through ETLP-49 are open.
- Implementation readiness: Partially ready; quality gates are active, but test execution is not yet part of
  the required validation path.

### Logging and diagnostics

- Status: Partially Ready
- Current evidence: [Architecture](../architecture.md) identifies `attendance_etl.logging_utils` as the
  shared logging setup, and implementation/test evidence exists for logging utilities.
- Remaining gaps: Broader diagnostics across runtime, messaging, backend, configuration, and storage failure
  paths remain partially evidenced.
- Existing GitHub coverage: ETLP-23 is closed.
- Implementation readiness: Partially ready; current logging utility support exists, but diagnostics coverage
  is not complete across all v2 paths.

### Testing and verification

- Status: Planned
- Current evidence: Executable unit tests exist under `tests/unit/ingestion/`. CI currently checks that the
  `tests/` scaffold exists but does not execute pytest.
- Remaining gaps: Transformation, FSM, messaging/storage, integration, backend/function, and core logic test
  coverage remain open. CI test execution and coverage reporting remain open.
- Existing GitHub coverage: ETLP-46 through ETLP-49 are open; ETLP-50 is open.
- Implementation readiness: Planned; production-readiness claims need executable test evidence.

### CI/CD and quality gates

- Status: Partially Ready
- Current evidence: [Validation Gates](../workflows/validation-gates.md) and `.github/workflows/ci.yml`
  enforce Ruff, Black, and MyPy. Local validation also runs `git diff --check`.
- Remaining gaps: CI does not execute tests, coverage reporting remains open, and dependency audit remains
  open.
- Existing GitHub coverage: ETLP-8, ETLP-9, ETLP-76, and ETLP-78 are closed; ETLP-50 and ETLP-51 are open.
- Implementation readiness: Partially ready; quality gates exist, but v2 CI readiness is incomplete.

### Operational architecture

- Status: Missing
- Current evidence: [Architecture](../architecture.md) records deployment-wrapper placement, but the artefact
  inventory records no active operational architecture or deployment specification.
- Remaining gaps: Active operational ownership is missing for the v2 path.
- Existing GitHub coverage: ETLP-104 is open.
- Implementation readiness: Missing; v2 needs sufficient operational documentation without promoting
  enterprise operations into scope.

### Deployment and provisioning

- Status: Missing
- Current evidence: [Local Development](../workflows/local-development.md) and
  [Validation Gates](../workflows/validation-gates.md) cover local workflow and validation, but no active
  deployment architecture specification was found.
- Remaining gaps: Manual but repeatable deployment/provisioning guidance is missing for the v2 path.
- Existing GitHub coverage: ETLP-53 and ETLP-104 are open.
- Implementation readiness: Missing; deployment/provisioning documentation is mandatory for v2 readiness.

### Runtime seed/bootstrap files

- Status: Partially Ready
- Current evidence: Device configuration ownership exists, and reliability/storage documents identify runtime
  persistence surfaces such as snapshot and review-period files.
- Remaining gaps: Required seed/bootstrap expectations are not fully defined across runtime, messaging,
  backend, storage, and recovery paths.
- Existing GitHub coverage: ETLP-21, ETLP-22, and ETLP-24 are closed; ETLP-45 and ETLP-104 are open.
- Implementation readiness: Partially ready; bootstrap expectations need final documentation and runtime
  alignment.

### Documentation governance

- Status: Ready
- Current evidence: [Documentation Governance](../governance/documentation.md) owns documentation ownership,
  lifecycle, taxonomy, and precedence rules.
- Remaining gaps: Governance exists, but remaining documentation alignment issues are still open.
- Existing GitHub coverage: ETLP-97 is closed; ETLP-57 through ETLP-60 and ETLP-84 are open.
- Implementation readiness: Ready as governance; alignment work remains roadmap-covered.

### Issue lifecycle and validation workflow

- Status: Ready
- Current evidence: [Issue Lifecycle](../workflows/issue-lifecycle.md) and
  [Validation Gates](../workflows/validation-gates.md) own issue flow, validation commands, and completion
  reporting.
- Remaining gaps: No remaining v2 workflow ownership gap identified by the capability matrix.
- Existing GitHub coverage: ETLP-7 through ETLP-12 and ETLP-78 are closed.
- Implementation readiness: Ready for issue-scoped implementation work.

### GitHub roadmap traceability

- Status: Partially Ready
- Current evidence: Live `gh` verification shows completed architecture-consolidation work through ETLP-102
  and open closeout issues ETLP-103 through ETLP-105.
- Remaining gaps: Repository gap analysis, remaining architecture documentation, and roadmap finalisation are
  still open in milestone `13 – Architecture Consolidation & Production Readiness`.
- Existing GitHub coverage: ETLP-99 through ETLP-102 are closed; ETLP-103 through ETLP-105 are open.
- Implementation readiness: Partially ready; traceability exists, but roadmap closeout is not complete.

## Documentation Gaps

Mandatory for v2:

- Storage abstraction owner: no active storage abstraction specification or contract exists.
- Backend response payload contracts: no tracked standalone owner exists for review-period, review-sheet
  response, or last-stored timestamp response schemas.
- Operational and deployment documentation: no active operational architecture or deployment specification was
  found.
- Runtime seed/bootstrap expectations: required files, generated files, invalid inputs, and recovery-related
  bootstrap expectations are not fully documented across the v2 path.
- Documentation alignment: ETLP-57 through ETLP-60, ETLP-84, and ETLP-104 remain open.

Future work, not a v2 blocker unless later promoted by accepted repository authority:

- enterprise operations manuals;
- commercial support runbooks;
- enterprise monitoring dashboards;
- full infrastructure-as-code; and
- automated release promotion.

## Implementation Gaps

Implementation work remaining by existing milestone:

| Milestone | Theme | Remaining implementation gap |
| --- | --- | --- |
| `05 - Messaging Abstraction` | Messaging | Create the messaging interface, implement the Pub/Sub adapter, move messaging logic, and implement bounded retry handling through ETLP-33 through ETLP-36. |
| `06 - Storage Abstraction` | Storage | Create storage abstraction work through ETLP-37 through ETLP-41 while preserving Google Sheets as the v2 review-output surface and PostgreSQL-first persistence as future scope. |
| `07 - FSM Isolation` | FSM | Extract FSM ownership, define states and transitions, and extract snapshot persistence through ETLP-42 through ETLP-45. |
| `08 - Testing` | Testing | Add transformation, FSM, messaging/storage, and core logic coverage through ETLP-46 through ETLP-49. |
| `09 - CI Pipeline` | CI | Add coverage reporting and dependency audit through ETLP-50 and ETLP-51. |
| `10 - Portfolio Enhancements` | Portfolio | Complete system diagrams, local development guide, example payloads, badges, and engineering concepts through ETLP-52 through ETLP-56. |
| `11 - Documentation Alignment` | Documentation | Complete final documentation alignment, terminology consistency, diagram alignment, README clarity, and ownership navigation through ETLP-57 through ETLP-60 and ETLP-84. |
| `12 - Refactoring & Improvements` | Refactoring | Refactor biometric device lifecycle and extraction contracts through ETLP-95. |
| `13 – Architecture Consolidation & Production Readiness` | Architecture closeout | Complete architecture documentation and roadmap finalisation through ETLP-104 and ETLP-105. |

This grouping summarises implementation themes only. It does not replace issue bodies, change milestones, or
create new roadmap items.

## Testing Gaps

Current coverage:

- Executable unit tests exist under `tests/unit/ingestion/`.
- Existing tests cover current ingestion, configuration, device, transformation, runtime, storage persistence,
  logging, and model surfaces.
- CI checks that the `tests/` scaffold exists.

Remaining coverage:

- Transformation test expansion remains open under ETLP-46.
- FSM tests remain open under ETLP-47.
- Messaging and storage tests remain open under ETLP-48.
- Core logic coverage remains open under ETLP-49.
- Integration and backend/function test evidence remains incomplete for v2 production-readiness claims.

CI execution and coverage reporting:

- CI currently runs Ruff, Black, MyPy, and test scaffold validation.
- CI does not execute pytest.
- Coverage reporting remains open under ETLP-50.
- Dependency audit remains open under ETLP-51.

## Operational Gaps

Remaining operational work is documentation and runtime-readiness work within the existing v2 scope:

- deployment/provisioning documentation for the v2 path;
- required runtime seed/bootstrap file expectations;
- manual deployment guidance for the Pi4 path, backend wrappers, Pub/Sub, and Google Sheets dependencies;
- restart behaviour and recovery expectations aligned with the Reliability Model;
- service supervision or runtime execution guidance sufficient for a competent developer/operator; and
- validation checks that must be run before claiming readiness.

This document does not introduce monitoring, observability, commercial runbooks, full infrastructure-as-code,
or automated deployment promotion as v2 requirements.

## Production Readiness Summary

| Category | Current status | Blocking? | GitHub coverage | Ready? |
| --- | --- | --- | --- | --- |
| Architecture readiness | Partially Ready | Yes | ETLP-104 and ETLP-105 open | No |
| Documentation readiness | Partially Ready | Yes | ETLP-57 through ETLP-60, ETLP-84, and ETLP-104 open | No |
| Implementation readiness | Partially Ready | Yes | ETLP-33 through ETLP-45 open | No |
| Testing readiness | Planned | Yes | ETLP-46 through ETLP-49 open | No |
| CI/CD readiness | Partially Ready | Yes | ETLP-50 and ETLP-51 open | No |
| Configuration readiness | Partially Ready | Yes | ETLP-45 and ETLP-104 open for remaining runtime/bootstrap coverage | No |
| Messaging readiness | Planned | Yes | ETLP-33 through ETLP-36 open | No |
| Storage readiness | Planned | Yes | ETLP-37 through ETLP-41 open | No |
| Runtime state, checkpoint, and recovery readiness | Partially Ready | Yes | ETLP-42 through ETLP-45 open | No |
| Backend ingestion readiness | Partially Ready | Yes | ETLP-104 open for remaining documentation coverage | No |
| Controlled failure and validation readiness | Partially Ready | Yes | ETLP-36 and ETLP-46 through ETLP-49 open | No |
| Logging and diagnostics readiness | Partially Ready | No | ETLP-23 closed; no standalone logging specification required by v2 | Partial |
| Operational deployment readiness | Missing | Yes | ETLP-53 and ETLP-104 open | No |
| GitHub roadmap readiness | Partially Ready | Yes | ETLP-103 through ETLP-105 open | No |

## Implementation Sequence

The recommended execution order is based on architectural dependencies, not strictly on GitHub milestone
numbering. This sequence does not change milestones, create issues, remove issues, or alter scope. It only
records the order in which the existing roadmap should be executed to avoid rework across dependent
capabilities.

```text
Architecture Documentation Completion
  ->
Roadmap Finalisation
  ->
FSM
  ->
Messaging
  ->
Storage
  ->
Testing
  ->
CI
  ->
Portfolio
  ->
Documentation Alignment
  ->
Refactoring
```

FSM isolation precedes messaging because the FSM boundary owns transition policy, waiting-state
classification, checkpoint eligibility, and recovery semantics. Messaging receive, timeout, retry, and routing
behaviour must integrate with those FSM and workflow boundaries rather than forcing them to be rediscovered
during the messaging milestone.

Messaging precedes storage because backend publication, response handling, and request/response transport
boundaries should be isolated before storage adapters and persistence behaviour are finalised.

Storage precedes broad test and CI expansion because storage interfaces, adapters, duplicate-handling
expectations, and backend persistence behaviour need stable seams before messaging/storage integration tests
and coverage gates can be completed.

The remaining milestones then follow the existing roadmap sequence: testing validates the implemented
architecture, CI enforces the validation path, portfolio work improves presentation, documentation alignment
normalises repository-wide consistency, and refactoring resolves non-functional improvements after the core
architecture has been implemented.

## Repository Completion Assessment

Repository currently is:

- architecture complete;
- documentation mostly complete; and
- implementation incomplete.

The architecture is complete because the accepted architecture, ADRs, production-readiness criteria,
capability matrix, artefact inventory, readiness audit, messaging model, reliability model, transformation
specification, device specifications, and canonical attendance event contract establish the v2 direction.

Documentation is mostly complete because governance, ownership rules, architecture, ADRs, core specifications,
contracts, proposals, workflows, and audits exist. It is not fully complete because storage abstraction,
backend response payloads, operational deployment, runtime bootstrap expectations, and final documentation
alignment still have missing or open coverage.

Implementation is incomplete because mandatory v2 implementation work remains open for messaging, storage,
FSM isolation, tests, CI additions, operational documentation, and roadmap closeout.

## Conclusion

The architecture is now stable. Remaining work is primarily implementation and roadmap closeout.

The repository can move through the remaining roadmap without architecture redesign. Messaging, storage, FSM,
testing, CI, portfolio, documentation alignment, refactoring, architecture documentation completion, and
roadmap finalisation should proceed through the existing milestones and issues exactly as they currently
exist.
