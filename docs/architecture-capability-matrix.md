# Architecture Capability Matrix

## Metadata

| Field | Value |
| --- | --- |
| Status | Accepted |
| Lifecycle | Active |
| Owner | Architecture capability inventory and production-readiness traceability for Attendance Automation v2 |
| Related audits | [Architecture Artefact Inventory](audits/architecture-artefact-inventory.md) |
| Related specifications | [Production Readiness](specifications/production-readiness.md), [Biometric Device Configuration](specifications/biometric-device-configuration.md), [BiometricDevice](specifications/biometric-device.md), [BiometricDeviceFactory](specifications/device-factory.md), [Messaging Model](specifications/messaging-model.md), [Reliability Model](specifications/reliability-model.md), [TransformationStrategy](specifications/transformation-strategy.md), [ZKTecoDevice](specifications/zkteco-device.md) |
| Related ADRs | [ADR-0001](decisions/0001-repo-modernisation-design.md), [ADR-0002](decisions/0002-model-adoption-principles.md), [ADR-0003](decisions/0003-device-and-transformation-layer-boundaries.md), [ADR-0004](decisions/0004-fsm-isolation-and-execution-boundaries.md), [ADR-0005](decisions/0005-messaging-abstraction-and-routing-boundary.md), [ADR-0006](decisions/0006-reliability-and-recovery.md) |
| Related contracts | [Canonical Attendance Event](contracts/canonical-attendance-event.md) |
| Related proposals | [Ingestion Modularisation](proposals/ingestion-modularisation.md), [Device Layer Abstraction](proposals/device-layer-abstraction.md), [Transformation Layer](proposals/transformation-layer.md), [Messaging Abstraction Implementation Proposal](proposals/messaging-abstraction.md) |
| Related workflows | [Issue Lifecycle](workflows/issue-lifecycle.md), [Branch Strategy](workflows/branch-strategy.md), [Commit Conventions](workflows/commit-conventions.md), [Validation Gates](workflows/validation-gates.md) |
| GitHub verification | Verified with `gh issue view`, `gh issue list`, and `gh api` on 2026-06-25. |

## Purpose

This matrix maps architectural capabilities to canonical documentation,
production-readiness categories, GitHub roadmap coverage, and v2/v3
classification for Attendance Automation v2.

It consolidates the architecture artefact inventory and the production-readiness
criteria into a single traceability view. It is an inventory and traceability
document only.

## Scope

This matrix covers:

- production-readiness capabilities;
- architectural ownership;
- GitHub issue and milestone traceability;
- implementation status classification; and
- v2/v3 boundary classification.

This matrix does not:

- assess implementation correctness in detail;
- redefine canonical ownership;
- create roadmap recommendations;
- create implementation tasks;
- modify GitHub issues or milestones; or
- replace specifications, contracts, proposals, ADRs, workflows, or architecture
  documents.

Status values use the readiness classification from
[Production Readiness](specifications/production-readiness.md): `Ready`,
`Partially Ready`, `Planned`, `Missing`, and `Deferred`.

## Capability Inventory

| Capability | Production-readiness category | Canonical owner | Supporting artefacts | GitHub coverage | Status | v2/v3 classification |
| --- | --- | --- | --- | --- | --- | --- |
| Device abstraction | Implementation Readiness | [BiometricDevice](specifications/biometric-device.md) | [ZKTecoDevice](specifications/zkteco-device.md), [BiometricDeviceFactory](specifications/device-factory.md), [ADR-0003](decisions/0003-device-and-transformation-layer-boundaries.md), [Device Layer Abstraction](proposals/device-layer-abstraction.md) | Milestone `03 - Device Layer Abstraction`: ETLP-25, ETLP-26, ETLP-27, ETLP-28 all closed. | Ready | V2 mandatory capability |
| Biometric device configuration | Configuration Readiness | [Biometric Device Configuration](specifications/biometric-device-configuration.md) | [Architecture](architecture.md), [Reliability Model](specifications/reliability-model.md), [Device Layer Abstraction](proposals/device-layer-abstraction.md) | Milestone `03 - Device Layer Abstraction`: ETLP-25 through ETLP-28 closed; Milestone `02 - Repository Structure Modernisation`: ETLP-22 closed. | Ready | V2 mandatory capability |
| Transformation strategy | Implementation Readiness | [TransformationStrategy](specifications/transformation-strategy.md) | [Canonical Attendance Event](contracts/canonical-attendance-event.md), [ADR-0003](decisions/0003-device-and-transformation-layer-boundaries.md), [Transformation Layer](proposals/transformation-layer.md) | Milestone `04 - Transformation Layer`: ETLP-29, ETLP-30, ETLP-31, ETLP-32 all closed. | Ready | V2 mandatory capability |
| Canonical attendance event contract | Backend Ingestion Readiness | [Canonical Attendance Event](contracts/canonical-attendance-event.md) | [Architecture](architecture.md), [TransformationStrategy](specifications/transformation-strategy.md), [ADR-0002](decisions/0002-model-adoption-principles.md) | Milestone `01 - Documentation & Repo Polish`: ETLP-14 and ETLP-19 closed; Milestone `04 - Transformation Layer`: ETLP-30 closed. | Ready | V2 mandatory capability |
| Runtime state | Runtime State, Checkpoint, And Recovery Readiness | [Architecture](architecture.md) for runtime placement; [Reliability Model](specifications/reliability-model.md) for recovery behaviour | [ADR-0004](decisions/0004-fsm-isolation-and-execution-boundaries.md), [Ingestion Modularisation](proposals/ingestion-modularisation.md), `src/attendance_etl/models/runtime_state.py` | Milestone `02 - Repository Structure Modernisation`: ETLP-24 closed; Milestone `07 - FSM Isolation`: ETLP-45 open. | Partially Ready | V2 mandatory support capability |
| Checkpoint and recovery | Runtime State, Checkpoint, And Recovery Readiness | [Reliability Model](specifications/reliability-model.md) | [ADR-0004](decisions/0004-fsm-isolation-and-execution-boundaries.md), [ADR-0006](decisions/0006-reliability-and-recovery.md), [Architecture](architecture.md) | Milestone `07 - FSM Isolation`: ETLP-42, ETLP-43, ETLP-44, ETLP-45 open. | Partially Ready | V2 mandatory capability |
| FSM isolation | Runtime State, Checkpoint, And Recovery Readiness | [ADR-0004](decisions/0004-fsm-isolation-and-execution-boundaries.md) for rationale; [Reliability Model](specifications/reliability-model.md) for checkpoint/recovery behaviour | [Architecture](architecture.md), [Ingestion Modularisation](proposals/ingestion-modularisation.md) | Milestone `07 - FSM Isolation`: ETLP-42, ETLP-43, ETLP-44, ETLP-45 open. | Planned | V2 mandatory capability |
| Messaging abstraction | Messaging Readiness | [Messaging Model](specifications/messaging-model.md) | [ADR-0005](decisions/0005-messaging-abstraction-and-routing-boundary.md), [ADR-0006](decisions/0006-reliability-and-recovery.md), [Messaging Abstraction Implementation Proposal](proposals/messaging-abstraction.md) | Milestone `05 - Messaging Abstraction`: ETLP-33, ETLP-34, ETLP-35, ETLP-36 open. | Planned | V2 mandatory capability |
| Messaging routing | Messaging Readiness | [Messaging Model](specifications/messaging-model.md) | [ADR-0005](decisions/0005-messaging-abstraction-and-routing-boundary.md), [Reliability Model](specifications/reliability-model.md), [Messaging Abstraction Implementation Proposal](proposals/messaging-abstraction.md) | Milestone `05 - Messaging Abstraction`: ETLP-33 through ETLP-36 open. | Planned | V2 mandatory capability |
| Backend ingestion | Backend Ingestion Readiness | [Canonical Attendance Event](contracts/canonical-attendance-event.md), [Reliability Model](specifications/reliability-model.md), and [Messaging Model](specifications/messaging-model.md) | [Architecture](architecture.md), [Data Pipeline](data-pipeline.md), `src/backend/*/main.py`, `src/attendance_etl/functions/` | Milestone `01 - Documentation & Repo Polish`: ETLP-16, ETLP-17, ETLP-19 closed; Milestone `13 – Architecture Consolidation & Production Readiness`: ETLP-104 open. | Partially Ready | V2 mandatory capability |
| Backend response payloads | Backend Ingestion Readiness | Not found: no tracked contract document owns review-period, review-sheet response, or last-stored timestamp response schemas. | [Messaging Model](specifications/messaging-model.md) references transport boundaries; [Reliability Model](specifications/reliability-model.md) references business validation; [Architecture Artefact Inventory](audits/architecture-artefact-inventory.md) records missing response contracts. | No direct GitHub coverage found for standalone backend response payload contracts. Milestone `13 – Architecture Consolidation & Production Readiness`: ETLP-104 open for remaining architecture documentation. | Missing | V2 required by production-readiness criteria |
| Storage abstraction | Storage Readiness | Not found: no active storage abstraction specification or contract exists. | [Architecture](architecture.md), [Reliability Model](specifications/reliability-model.md), [Data Pipeline](data-pipeline.md), [Architecture Artefact Inventory](audits/architecture-artefact-inventory.md) | Milestone `06 - Storage Abstraction`: ETLP-37, ETLP-38, ETLP-39, ETLP-40, ETLP-41 open. | Planned | V2 mandatory capability once owner exists; PostgreSQL-first persistence is v3/future |
| Google Sheets review output | Storage Readiness | [Architecture](architecture.md) owns placement; no standalone storage contract found. | [Data Pipeline](data-pipeline.md), [Reliability Model](specifications/reliability-model.md), `src/attendance_etl/functions/store_attend_records.py` | Milestone `06 - Storage Abstraction`: ETLP-38 open; Milestone `01 - Documentation & Repo Polish`: ETLP-16 closed. | Partially Ready | V2 current persistence and review-output surface |
| Configuration management | Configuration Readiness | [Biometric Device Configuration](specifications/biometric-device-configuration.md) for device configuration; [Architecture](architecture.md) for runtime configuration placement | [ADR-0001](decisions/0001-repo-modernisation-design.md), [ADR-0003](decisions/0003-device-and-transformation-layer-boundaries.md), [Reliability Model](specifications/reliability-model.md) | Milestone `02 - Repository Structure Modernisation`: ETLP-22 closed; Milestone `03 - Device Layer Abstraction`: ETLP-25 through ETLP-28 closed. | Partially Ready | V2 mandatory capability |
| Controlled failure handling | Controlled Failure And Validation Readiness | [Reliability Model](specifications/reliability-model.md) | [Biometric Device Configuration](specifications/biometric-device-configuration.md), [Messaging Model](specifications/messaging-model.md), [TransformationStrategy](specifications/transformation-strategy.md), [Canonical Attendance Event](contracts/canonical-attendance-event.md) | Milestone `05 - Messaging Abstraction`: ETLP-36 open; Milestone `08 - Testing`: ETLP-46 through ETLP-49 open. | Partially Ready | V2 mandatory capability |
| Validation | Controlled Failure And Validation Readiness | [Validation Gates](workflows/validation-gates.md) for repository validation; active specifications and contracts for behaviour-specific validation | [Local Development](workflows/local-development.md), [Production Readiness](specifications/production-readiness.md), [Reliability Model](specifications/reliability-model.md) | Milestone `00 - Repository Execution Foundation`: ETLP-7, ETLP-8, ETLP-9 closed; Milestone `02 - Repository Structure Modernisation`: ETLP-78 closed; Milestone `08 - Testing`: ETLP-46 through ETLP-49 open. | Partially Ready | V2 mandatory capability |
| Logging and diagnostics | Logging And Diagnostics Readiness | [Architecture](architecture.md) identifies `attendance_etl.logging_utils`; no separate logging specification is created by [Production Readiness](specifications/production-readiness.md). | [ADR-0001](decisions/0001-repo-modernisation-design.md), `src/attendance_etl/logging_utils.py`, `tests/unit/ingestion/test_logging_utils.py` | Milestone `02 - Repository Structure Modernisation`: ETLP-23 closed. | Partially Ready | V2 support capability |
| Testing and verification | Testing Readiness | [Validation Gates](workflows/validation-gates.md) and active specifications | [Production Readiness](specifications/production-readiness.md), tests under `tests/unit/ingestion/`, `.github/workflows/ci.yml` | Milestone `08 - Testing`: ETLP-46, ETLP-47, ETLP-48, ETLP-49 open; Milestone `09 - CI Pipeline`: ETLP-50 open. | Planned | V2 mandatory capability |
| CI/CD and quality gates | CI/CD Readiness | [Validation Gates](workflows/validation-gates.md) | [.github CI workflow](../.github/workflows/ci.yml), [.pre-commit-config.yaml](../.pre-commit-config.yaml), [Local Development](workflows/local-development.md) | Milestone `00 - Repository Execution Foundation`: ETLP-8 and ETLP-9 closed; Milestone `02 - Repository Structure Modernisation`: ETLP-76 and ETLP-78 closed; Milestone `09 - CI Pipeline`: ETLP-50 and ETLP-51 open. | Partially Ready | V2 mandatory capability; release automation is v3/future |
| Operational architecture | Operational Deployment Readiness | Not found: no active operational architecture or deployment specification exists. | [Architecture](architecture.md), [Production Readiness](specifications/production-readiness.md), [Architecture Artefact Inventory](audits/architecture-artefact-inventory.md) | Milestone `13 – Architecture Consolidation & Production Readiness`: ETLP-104 open. | Missing | V2 requires sufficient operational deployment documentation; enterprise operations are v3/future |
| Deployment and provisioning | Operational Deployment Readiness | Not found: no active deployment architecture specification exists. | [Architecture](architecture.md), [Local Development](workflows/local-development.md), [Validation Gates](workflows/validation-gates.md) | Milestone `10 - Portfolio Enhancements`: ETLP-53 open; Milestone `13 – Architecture Consolidation & Production Readiness`: ETLP-104 open. | Missing | V2 requires manual but repeatable deployment/provisioning documentation |
| Runtime seed/bootstrap files | Configuration Readiness | [Biometric Device Configuration](specifications/biometric-device-configuration.md) for `biometric_device_config.json`; [Reliability Model](specifications/reliability-model.md) for snapshot/review-period recovery interaction | [Ingestion Modularisation](proposals/ingestion-modularisation.md), [Architecture](architecture.md), `src/attendance_etl/storage/snapshot.py`, `src/attendance_etl/storage/review_period.py` | Milestone `02 - Repository Structure Modernisation`: ETLP-21, ETLP-22, ETLP-24 closed; Milestone `07 - FSM Isolation`: ETLP-45 open; Milestone `13 – Architecture Consolidation & Production Readiness`: ETLP-104 open. | Partially Ready | V2 mandatory capability |
| Documentation governance | Documentation Readiness | [Documentation Governance](governance/documentation.md) | [Documentation Index](README.md), [AGENTS.md](../AGENTS.md), [Architecture Artefact Inventory](audits/architecture-artefact-inventory.md) | Milestone `11 - Documentation Alignment`: ETLP-97 closed; ETLP-57, ETLP-58, ETLP-59, ETLP-60, ETLP-84 open. | Ready | V2 governance capability |
| Issue lifecycle and validation workflow | GitHub Roadmap Readiness | [Issue Lifecycle](workflows/issue-lifecycle.md) and [Validation Gates](workflows/validation-gates.md) | [Branch Strategy](workflows/branch-strategy.md), [Commit Conventions](workflows/commit-conventions.md), [Local Development](workflows/local-development.md) | Milestone `00 - Repository Execution Foundation`: ETLP-7 through ETLP-12 closed; Milestone `02 - Repository Structure Modernisation`: ETLP-78 closed. | Ready | V2 workflow capability |
| GitHub roadmap traceability | GitHub Roadmap Readiness | [Issue Lifecycle](workflows/issue-lifecycle.md) | [Architecture Artefact Inventory](audits/architecture-artefact-inventory.md), this matrix, [Validation Gates](workflows/validation-gates.md) | Milestone `13 – Architecture Consolidation & Production Readiness`: ETLP-99 and ETLP-100 closed; ETLP-101, ETLP-102, ETLP-103, ETLP-104, ETLP-105 open. | Partially Ready | V2 traceability capability |

## Capability Dependency Overview

- Device abstraction precedes transformation strategy because transformation
  consumes extracted biometric data after device-specific access is isolated.
- Biometric device configuration precedes runtime device construction because
  runtime composition depends on validated `BiometricDeviceConfig` before a
  concrete `BiometricDevice` can be created.
- Transformation strategy precedes full canonical attendance event runtime
  adoption because it owns normalisation, validation, canonicalisation, and
  filtering before downstream publication.
- Messaging abstraction and messaging routing precede workflow migration away
  from raw Google Pub/Sub helpers.
- Storage abstraction precedes backend storage replacement and PostgreSQL-first
  persistence.
- FSM isolation preserves checkpoint/recovery semantics by keeping waiting
  states, non-waiting states, retry, and checkpoint eligibility aligned with the
  Reliability Model.
- Backend ingestion depends on the Canonical Attendance Event contract,
  Messaging Model, Reliability Model, and storage/review-output boundaries.
- Testing and CI/CD verify implementation readiness and roadmap completion
  claims through the repository validation workflow.
- Operational deployment depends on configuration, runtime state, messaging,
  storage, reliability, validation, and documentation readiness.

## Canonical Ownership Summary

| Architectural Concern | Canonical Owner | Coverage Status |
| --- | --- | --- |
| System topology and responsibility placement | [Architecture](architecture.md) | Ready |
| Production-readiness criteria | [Production Readiness](specifications/production-readiness.md) | Ready |
| Documentation ownership and lifecycle | [Documentation Governance](governance/documentation.md) | Ready |
| Device abstraction | [BiometricDevice](specifications/biometric-device.md) | Ready |
| Concrete ZKTeco implementation | [ZKTecoDevice](specifications/zkteco-device.md) | Ready |
| Device construction | [BiometricDeviceFactory](specifications/device-factory.md) | Ready |
| Biometric device configuration | [Biometric Device Configuration](specifications/biometric-device-configuration.md) | Ready |
| Transformation behaviour | [TransformationStrategy](specifications/transformation-strategy.md) | Ready |
| Attendance event data contract | [Canonical Attendance Event](contracts/canonical-attendance-event.md) | Ready |
| Messaging transport contract | [Messaging Model](specifications/messaging-model.md) | Planned |
| Reliability, retry, checkpoint, and recovery behaviour | [Reliability Model](specifications/reliability-model.md) | Partially Ready |
| FSM rationale and execution-boundary decision | [ADR-0004](decisions/0004-fsm-isolation-and-execution-boundaries.md) | Planned |
| Messaging rationale and routing decision | [ADR-0005](decisions/0005-messaging-abstraction-and-routing-boundary.md) | Planned |
| Reliability rationale | [ADR-0006](decisions/0006-reliability-and-recovery.md) | Partially Ready |
| Storage abstraction | Not found | Planned |
| Backend response payload schemas | Not found | Missing |
| Operational architecture | Not found | Missing |
| Deployment architecture | Not found | Missing |
| Validation workflow | [Validation Gates](workflows/validation-gates.md) | Partially Ready |
| Issue lifecycle | [Issue Lifecycle](workflows/issue-lifecycle.md) | Ready |
| GitHub roadmap traceability | [Issue Lifecycle](workflows/issue-lifecycle.md), supported by [Architecture Artefact Inventory](audits/architecture-artefact-inventory.md) | Partially Ready |

## Roadmap Coverage Summary

GitHub coverage was verified with live `gh` CLI reads. Issue states below are
GitHub issue states at verification time.

| Milestone | Capability areas covered | Issue coverage | State summary |
| --- | --- | --- | --- |
| `05 - Messaging Abstraction` | Messaging abstraction, routing, Pub/Sub adapter, bounded send retry | ETLP-33, ETLP-34, ETLP-35, ETLP-36 | All four issues open. |
| `06 - Storage Abstraction` | Storage interface, Google Sheets adapter, PostgreSQL adapter, database schema, migration script | ETLP-37, ETLP-38, ETLP-39, ETLP-40, ETLP-41 | All five issues open. |
| `07 - FSM Isolation` | FSM module, FSM state enum, FSM transitions, snapshot persistence | ETLP-42, ETLP-43, ETLP-44, ETLP-45 | All four issues open. |
| `08 - Testing` | Transformation tests, FSM tests, messaging/storage tests, core logic coverage | ETLP-46, ETLP-47, ETLP-48, ETLP-49 | All four issues open. |
| `09 - CI Pipeline` | Coverage reporting and dependency audit | ETLP-50, ETLP-51 | Both issues open. |
| `13 – Architecture Consolidation & Production Readiness` | Architecture artefact inventory, production readiness criteria, capability matrix, readiness audit, gap analysis, documentation completion, roadmap finalisation | ETLP-99, ETLP-100, ETLP-101, ETLP-102, ETLP-103, ETLP-104, ETLP-105 | ETLP-99 and ETLP-100 closed; ETLP-101 through ETLP-105 open. |

Other relevant milestones:

| Milestone | Capability areas covered | Issue coverage | State summary |
| --- | --- | --- | --- |
| `00 - Repository Execution Foundation` | Package layout foundation, test scaffold, CI workflow, formatting/linting/typing checks, agent instructions, local development workflow | ETLP-6 through ETLP-12 | All listed issues closed. |
| `01 - Documentation & Repo Polish` | README, canonical event schema, architecture diagram, data pipeline, reliability documentation, architecture document, data contract, future roadmap | ETLP-13 through ETLP-20 | All listed issues closed. |
| `02 - Repository Structure Modernisation` | Pi client split, configuration module, logging module, data models, validation runner, pre-commit, version tooling | ETLP-21 through ETLP-24 and ETLP-76 through ETLP-78 | All listed issues closed. |
| `03 - Device Layer Abstraction` | Device interface, ZKTeco adapter, device factory, extraction movement | ETLP-25, ETLP-26, ETLP-27, ETLP-28 | All listed issues closed. |
| `04 - Transformation Layer` | Transformation module, attendance domain models, template method, ZKTeco strategy | ETLP-29, ETLP-30, ETLP-31, ETLP-32 | All listed issues closed. |
| `10 - Portfolio Enhancements` | System diagrams, local development guide, example payloads, portfolio-facing concepts | ETLP-52, ETLP-53, ETLP-54, ETLP-55, ETLP-56 | All listed issues open. |
| `11 - Documentation Alignment` | Final documentation alignment, terminology, diagram alignment, README clarity, ownership navigation, governance baseline | ETLP-57, ETLP-58, ETLP-59, ETLP-60, ETLP-84, ETLP-97 | ETLP-97 closed; the other listed issues open. |

## Missing Or Partial Capability Ownership

Capabilities with missing canonical owners:

- Backend response payloads: no tracked contract document owns review-period,
  review-sheet response, or last-stored timestamp response schemas.
- Storage abstraction: no active storage abstraction specification or contract
  exists.
- Operational architecture: no active operational architecture or deployment
  specification exists.
- Deployment and provisioning: no active deployment architecture specification
  exists.

Capabilities with no direct GitHub coverage found:

- Backend response payload contracts: no standalone GitHub issue was found for
  review-period, review-sheet response, or last-stored timestamp response
  contracts. ETLP-104 covers remaining architecture documentation at milestone
  level.

Capabilities classified as `Partially Ready`:

- Runtime state.
- Checkpoint and recovery.
- Backend ingestion.
- Google Sheets review output.
- Configuration management.
- Controlled failure handling.
- Validation.
- Logging and diagnostics.
- CI/CD and quality gates.
- Runtime seed/bootstrap files.
- GitHub roadmap traceability.

Capabilities classified as `Missing`:

- Backend response payloads.
- Operational architecture.
- Deployment and provisioning.

Capabilities classified as `Planned`:

- FSM isolation.
- Messaging abstraction.
- Messaging routing.
- Storage abstraction.
- Testing and verification.

## V3 / Future Capability Boundary

The following capabilities are classified as `Deferred` for Attendance
Automation v2 because [Production Readiness](specifications/production-readiness.md)
places them in the v3/future boundary or a deferred scope.

| Capability | Current canonical owner | Status |
| --- | --- | --- |
| Enterprise monitoring dashboards | [Production Readiness](specifications/production-readiness.md) defines this as outside v2 readiness. | Deferred |
| DLQ / poison-message handling | [Messaging Model](specifications/messaging-model.md) and [Reliability Model](specifications/reliability-model.md) defer DLQ and poison-message handling. | Deferred |
| Event/correlation IDs | [Canonical Attendance Event](contracts/canonical-attendance-event.md), [Reliability Model](specifications/reliability-model.md), and [Production Readiness](specifications/production-readiness.md) identify these as future/deferred. | Deferred |
| Advanced replay tooling | [Production Readiness](specifications/production-readiness.md) and [Reliability Model](specifications/reliability-model.md) classify advanced replay/recovery tooling as future. | Deferred |
| PostgreSQL-first production storage | [Architecture](architecture.md) places PostgreSQL under future storage; [Production Readiness](specifications/production-readiness.md) defers PostgreSQL-first persistence. | Deferred |
| Full infrastructure-as-code | [Production Readiness](specifications/production-readiness.md) classifies full infrastructure-as-code as future. | Deferred |
| Multi-site operational dashboards | [Production Readiness](specifications/production-readiness.md) classifies multi-site operational dashboards as future. | Deferred |
| Stronger backend idempotency beyond the documented v2 model | [Reliability Model](specifications/reliability-model.md) and [Production Readiness](specifications/production-readiness.md) defer stronger backend idempotency beyond v2. | Deferred |
| Google Sheets deduplication strategy beyond v2 | [Reliability Model](specifications/reliability-model.md) and [Production Readiness](specifications/production-readiness.md) defer this beyond the v2 model. | Deferred |
| Automated release promotion | [Production Readiness](specifications/production-readiness.md) classifies automated release promotion as future. | Deferred |

## Validation

Before reporting completion for this matrix:

- every readiness status must be one of `Ready`, `Partially Ready`, `Planned`,
  `Missing`, or `Deferred`;
- every canonical owner must reference an existing document or explicitly state
  `Not found`;
- GitHub coverage must be verified using `gh` CLI;
- this document must not create recommendations or roadmap tasks;
- only `docs/architecture-capability-matrix.md` must be modified;
- `./scripts/validate.sh` must be run;
- `git diff --check` must be run; and
- local Markdown links added by this document must be validated.
