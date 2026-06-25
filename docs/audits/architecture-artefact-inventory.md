# Architecture Artefact Inventory

## Purpose

This inventory records the architecture-related artefacts currently present in the tracked repository and the
GitHub milestone, issue, and label metadata verified for architecture traceability.

It is a factual inventory only. It does not redesign the architecture, create new ownership, rewrite existing
specifications, or recommend future changes. Where an artefact was not found, the inventory states that fact.

---

## Repository Overview

Inventory basis:

- Tracked repository files from `git ls-files`.
- GitHub milestones, issues, and labels verified with `gh issue list` and `gh api`.
- Generated caches, build artefacts, and untracked files are excluded.

| Artefact Type | Count | Inventory Result |
| --- | ---: | --- |
| Architecture decision records | 6 | Six accepted ADR files exist under `docs/decisions/`. |
| Specifications | 7 | Seven accepted active specification files exist under `docs/specifications/`. |
| Proposals | 4 | Four proposal files exist under `docs/proposals/`: one active and three implemented/evolved. |
| Contracts | 1 | One accepted active contract exists under `docs/contracts/`. |
| Architecture documents | 4 | `README.md`, `docs/architecture.md`, `docs/data-pipeline.md`, and `docs/future-work.md` contain architecture or roadmap-facing architecture content. |
| Workflow documents | 5 | Five accepted active workflow documents exist under `docs/workflows/`, plus the workflow index. |
| Governance documents | 2 | `docs/governance/documentation.md` owns documentation governance; `AGENTS.md` owns AI-agent operating instructions and points to that governance owner. |
| Diagram inventory | 8 | Six file-backed diagrams and two embedded Mermaid diagrams were found. |
| Audit reports | 0 | No tracked `docs/audits/` files or C05-C15 audit report files existed before this inventory file. |

Implementation and verification evidence surfaces inspected:

| Area | Tracked Evidence |
| --- | --- |
| Runtime and implementation modules | `src/attendance_etl/`, `src/pi4/`, and `src/backend/`. These consume architecture artefacts but do not replace documentation owners. |
| Tests | `tests/unit/ingestion/` contains executable unit tests; `tests/integration/`, `tests/unit/functions/`, and `tests/fixtures/` contain `.gitkeep` scaffolds. |

---

## Architecture Decision Records

| ADR | Status | Purpose | Primary Ownership | Consumed By |
| --- | --- | --- | --- | --- |
| [ADR-0001: Repository Modernisation Design Decisions](../decisions/0001-repo-modernisation-design.md) | Accepted | Records repository modernisation decisions for modularisation, abstraction discipline, naming, configuration, logging, and compatibility entry points. | Repository modernisation rationale and architectural principles. | `README.md`, architecture documentation, modular package layout under `src/attendance_etl/`, and ETLP-21 through ETLP-24 traceability. |
| [ADR-0002: Domain Model Adoption Principles](../decisions/0002-model-adoption-principles.md) | Accepted | Records domain-model adoption principles for `AttendanceEvent`, `Employee`, `ReviewPeriod`, and `RuntimeState`. | Domain model adoption rationale. | Model modules under `src/attendance_etl/models/`, storage modules, transformation modules, and tests under `tests/unit/ingestion/`. |
| [ADR-0003: Device And Transformation Layer Boundaries](../decisions/0003-device-and-transformation-layer-boundaries.md) | Accepted | Records device, transformation, runtime, and domain-model boundary decisions. | Device and transformation boundary rationale. | Device specifications, transformation specification, device and transformation proposals, `src/attendance_etl/devices/`, and `src/attendance_etl/transform/`. |
| [ADR-0004: FSM, Workflow, Runtime, And Messaging Boundary](../decisions/0004-fsm-isolation-and-execution-boundaries.md) | Accepted | Records FSM, workflow, runtime, messaging, persistence, and execution-boundary decisions. | FSM, workflow, runtime, and execution-boundary rationale. | Reliability model, messaging model, `src/attendance_etl/pi4/`, local persistence modules, and FSM-related GitHub issues. |
| [ADR-0005: Messaging Abstraction And Routing Boundary](../decisions/0005-messaging-abstraction-and-routing-boundary.md) | Accepted | Records messaging abstraction, topic vocabulary, routing, ACK, and provider-boundary decisions. | Messaging abstraction and routing rationale. | Messaging model, messaging proposal, current `src/attendance_etl/messaging/`, and ETLP-33 through ETLP-36. |
| [ADR-0006: Reliability And Recovery](../decisions/0006-reliability-and-recovery.md) | Accepted | Records current reliability baseline, target transport/business reliability model, ACK boundary, and retry/recovery scope. | Reliability and recovery rationale. | Reliability model, messaging model, Pi4 workflow/runtime modules, storage modules, backend functions, and ETLP-36. |

ADR navigation artefact:

| Artefact | Purpose |
| --- | --- |
| [Architecture Decision Records Index](../decisions/README.md) | Lists ADR purpose, lifecycle, relationship to other documentation, index, and naming convention. |

---

## Specifications

| Specification | Status | Purpose | Primary Ownership | Consumed By |
| --- | --- | --- | --- | --- |
| [Biometric Device Configuration Specification](../specifications/biometric-device-configuration.md) | Accepted; Lifecycle: Active | Defines biometric device configuration shape, required fields, vendor option ownership, startup validation, and fail-fast rules. | Biometric device configuration contract. | `src/attendance_etl/devices/biometric_device_config.py`, `src/attendance_etl/pi4/runtime.py`, device factory, and configuration tests. |
| [BiometricDevice Specification](../specifications/biometric-device.md) | Accepted; Lifecycle: Active | Defines the runtime-facing `BiometricDevice` abstraction. | Biometric device abstraction contract. | `src/attendance_etl/devices/biometric_device.py`, `ZKTecoDevice`, `BiometricDeviceFactory`, Pi4 workflow/runtime, and device tests. |
| [BiometricDeviceFactory Specification](../specifications/device-factory.md) | Accepted; Lifecycle: Active | Defines the biometric device factory as the composition boundary for concrete biometric device construction. | Biometric device construction contract. | `src/attendance_etl/devices/biometric_device_factory.py`, Pi4 runtime, and factory tests. |
| [Messaging Model Specification](../specifications/messaging-model.md) | Accepted; Lifecycle: Active | Defines messaging transport contracts, topic vocabulary, routing policy, resolver and messenger contracts, timeout/failure types, ACK policy, and Google Pub/Sub adapter expectations. | Messaging transport contract. | Current messaging helper surface, Pi4 workflow/runtime, messaging proposal, reliability model, and ETLP-33 through ETLP-36. |
| [Reliability Model Specification](../specifications/reliability-model.md) | Accepted; Lifecycle: Active | Defines reliability behaviour across ingestion, messaging, workflow/FSM, persistence, transformation, device access, backend processing, and storage side effects. | Reliability behaviour contract. | Pi4 workflow/runtime, messaging model, storage modules, backend functions, tests, and reliability-related issues. |
| [TransformationStrategy Specification](../specifications/transformation-strategy.md) | Accepted; Lifecycle: Active | Defines transformation strategy ownership, template method behaviour, transformation request structure, normalised attendance, validation, canonicalisation, and filtering. | Transformation behaviour contract. | `src/attendance_etl/transform/`, `src/attendance_etl/models/attendance_event.py`, transformation tests, and ETLP-29 through ETLP-32. |
| [ZKTecoDevice Specification](../specifications/zkteco-device.md) | Accepted; Lifecycle: Active | Defines the concrete ZKTeco `BiometricDevice` implementation, SDK ownership, connection option ownership, extraction, clearing, dependencies, and testing expectations. | ZKTeco device implementation contract. | `src/attendance_etl/devices/zkteco_device.py`, Pi4 runtime, device factory, and ZKTeco device tests. |

---

## Proposals

| Proposal | Status | Purpose | Planned GitHub Issues |
| --- | --- | --- | --- |
| [ETLP-21 Ingestion Modularisation Plan](../proposals/ingestion-modularisation.md) | Accepted; Lifecycle: Implemented / Evolved | Records the ETLP-21/ETLP-24 modularisation and model-introduction implementation plan, with later ownership moved to ADRs, specifications, and contracts where stated. | ETLP-21, ETLP-22, ETLP-23, ETLP-24. |
| [Device Layer Abstraction](../proposals/device-layer-abstraction.md) | Accepted; Lifecycle: Implemented / Evolved | Records the Milestone 3 device-layer implementation strategy and evolution context. | ETLP-25, ETLP-26, ETLP-27, ETLP-28. |
| [Transformation Layer](../proposals/transformation-layer.md) | Accepted; Lifecycle: Implemented / Evolved | Records the Milestone 4 transformation-layer implementation strategy and evolution context. | ETLP-29, ETLP-30, ETLP-31, ETLP-32. |
| [Messaging Abstraction Implementation Proposal](../proposals/messaging-abstraction.md) | Accepted; Lifecycle: Active | Records the implementation strategy for the accepted messaging model and ETLP-33 through ETLP-36. | ETLP-33, ETLP-34, ETLP-35, ETLP-36. |

Proposal navigation artefact:

| Artefact | Purpose |
| --- | --- |
| [Design Proposals Index](../proposals/README.md) | Lists proposal purpose, relationship to other documentation, status/lifecycle taxonomy, navigation, and naming convention. |

---

## Contracts

| Contract | Purpose | Owner |
| --- | --- | --- |
| [Canonical Attendance Event](../contracts/canonical-attendance-event.md) | Defines the current internal `AttendanceEvent` model, current backend-compatible Pub/Sub payload, timestamp and event-type serialisation, event semantics, and deferred schema evolution notes. | `docs/contracts/canonical-attendance-event.md` |

Contract navigation artefact:

| Artefact | Purpose |
| --- | --- |
| [Contracts Index](../contracts/README.md) | Lists contract directory purpose, contract documents, contract areas, usage, and scope. |

Contract areas not found as standalone active contract documents:

| Area | Repository Evidence |
| --- | --- |
| Review period payload contract | Not found as a standalone active contract document. `ReviewPeriod` exists in `src/attendance_etl/models/review_period.py`, with workflow and backend usage, but no tracked contract file owns a review-period payload schema. |
| Review sheet response contract | Not found as a standalone active contract document. Backend and workflow code consume review-sheet response shapes, but no tracked contract file owns the response schema. |
| Last-stored timestamp response contract | Not found as a standalone active contract document. Messaging and reliability documents reference the response, and backend code publishes it, but no tracked contract file owns the response schema. |
| Storage abstraction contract | Not found as a standalone active contract document. GitHub milestone `06 - Storage Abstraction` and ETLP-37 through ETLP-41 exist, but no tracked storage abstraction specification or contract exists. |

---

## Architecture Documents

| Document | Status | Purpose | Primary Ownership |
| --- | --- | --- | --- |
| [Architecture](../architecture.md) | Accepted; Lifecycle: Active | Describes current system topology, component placement, sequence view, codebase structure, messaging/backend/storage placement, and documented limitations. | System topology and responsibility placement. |
| [Data Pipeline](../data-pipeline.md) | Status not found; Lifecycle not found | Describes the attendance pipeline stages from device polling through Google Sheets review output. | Pipeline sequencing and stage inventory. |
| [Future Work](../future-work.md) | Status not found; Lifecycle not found | Records directional near-term, mid-term, and long-term evolution paths and deferred ideas. | Directional roadmap context. |
| [Repository README](../../README.md) | Status not found; Lifecycle not found | Provides repository overview, architecture diagram entry point, reliability summary, validation summary, and navigation to architecture, workflow, and contract documents. | Repository overview and navigation. |

Architecture-related navigation document:

| Document | Purpose |
| --- | --- |
| [Documentation Index](../README.md) | Lists the documentation hierarchy, ownership model summary, ADRs, specifications, contracts, proposals, architecture documents, archive, diagrams, governance, and workflows. |

Archived architecture and documentation material:

| Document | Status | Lifecycle | Purpose |
| --- | --- | --- | --- |
| [Documentation Consolidation Plan](../archive/doc-consolidation-plan.md) | Archived | Historical | Archived documentation consolidation plan retained for historical traceability. |
| [Reliability Model v1](../archive/reliability-v1.md) | Archived | Historical | Archived reliability model retained for historical traceability after replacement by the active reliability specification. |

---

## Workflow Documentation

| Workflow Document | Status | Purpose |
| --- | --- | --- |
| [Workflows Index](../workflows/README.md) | Accepted; Lifecycle: Active | Lists workflow documents and their process scope. |
| [Issue Lifecycle](../workflows/issue-lifecycle.md) | Accepted; Lifecycle: Active | Defines GitHub issue lifecycle and status expectations. |
| [Branch Strategy](../workflows/branch-strategy.md) | Accepted; Lifecycle: Active | Defines branch naming and branch flow. |
| [Commit Conventions](../workflows/commit-conventions.md) | Accepted; Lifecycle: Active | Defines commit message format and commit hygiene. |
| [Local Development](../workflows/local-development.md) | Accepted; Lifecycle: Active | Defines fresh-clone setup and local validation workflow. |
| [Validation Gates](../workflows/validation-gates.md) | Accepted; Lifecycle: Active | Defines required validation commands and reporting expectations. |

GitHub workflow artefacts:

| Artefact | Purpose |
| --- | --- |
| [CI workflow](../../.github/workflows/ci.yml) | Runs Ruff, Black, MyPy, and a `tests/` scaffold check on push and pull request events. |
| [Prefix issue title workflow](../../.github/workflows/prefix-issue-title.yml) | Prefixes newly opened GitHub issue titles with `ETLP-<number>`. |
| [Task issue template](../../.github/ISSUE_TEMPLATE/task.md) | Defines the default task issue structure. |

Validation and tooling artefacts:

| Artefact | Purpose |
| --- | --- |
| [`scripts/validate.sh`](../../scripts/validate.sh) | Canonical local validation runner invoked by the validation workflow. |
| [`pyproject.toml`](../../pyproject.toml) | Python package and tool configuration for build metadata, Ruff, Black, MyPy, and version tooling. |
| [`.pre-commit-config.yaml`](../../.pre-commit-config.yaml) | Pre-commit hook configuration for local quality checks and repository hygiene. |

---

## Governance Documentation

| Governance Document | Status | Purpose | Primary Ownership |
| --- | --- | --- | --- |
| [Documentation Governance](../governance/documentation.md) | Accepted; Lifecycle: Active | Defines documentation ownership, lifecycle rules, audit rules, status taxonomy, precedence, archive policy, and navigation philosophy. | Documentation governance. |
| [AI Agent Operating Instructions](../../AGENTS.md) | Status not found; Lifecycle not found | Defines AI-agent operating rules, context loading, validation, GitHub safety, implementation discipline, and branch/commit discipline. | Agent operating instructions. |

---

## Diagrams

File-backed diagrams:

| Diagram | Type | Purpose |
| --- | --- | --- |
| [System Context](../diagrams/c4/system-context.puml) | C4 PlantUML | System-context diagram. |
| [Container View](../diagrams/c4/container-view.puml) | C4 PlantUML | Container-level diagram. |
| [Pi4 Component View](../diagrams/c4/pi4-component-view.puml) | C4 PlantUML | Pi4 component-level diagram. |
| [Transformation Strategy](../diagrams/transformation-strategy.puml) | PlantUML | Transformation strategy diagram. |
| [High-level Architecture](../diagrams/high-level-architecture.png) | PNG | High-level architecture diagram referenced by the repository README and documentation index. |
| [Pi4 Client FSM](../diagrams/pi4-client-fsm.png) | PNG | Raspberry Pi client finite-state-machine diagram referenced by the repository README and documentation index. |

Embedded diagrams:

| Location | Type | Purpose |
| --- | --- | --- |
| [Repository README](../../README.md) | Mermaid flowchart | Inline ETL flow from extraction through storage/review output. |
| [Architecture](../architecture.md) | Mermaid flowchart and Mermaid sequence diagram | Current system flow and simplified sequence view. |

---

## Audit Reports

No tracked audit report files existed before this inventory file. `git ls-files 'docs/audits/**'` returned no
paths before `docs/audits/architecture-artefact-inventory.md` was created.

| Audit | Repository File | Status |
| --- | --- | --- |
| C05 | Not found | No tracked C05 audit report found. |
| C06 | Not found | No tracked C06 audit report found. |
| C07 | Not found | No tracked C07 audit report found. |
| C08 | Not found | No tracked C08 audit report found. |
| C09 | Not found | No tracked C09 audit report found. |
| C10 | Not found | No tracked C10 audit report found. |
| C11 | Not found | No tracked C11 audit report found. |
| C12 | Not found | No tracked C12 audit report found. |
| C13 | Not found | No tracked C13 audit report found. |
| C14 | Not found | No tracked C14 audit report found. |
| C15 | Not found | No tracked C15 audit report found. |

GitHub references to consolidated C05-C15 work:

| GitHub Issue | State | Milestone | Factual Mapping |
| --- | --- | --- | --- |
| ETLP-101: Consolidate Architecture Capability Matrix | Open | `13 – Architecture Consolidation & Production Readiness` | Issue body references consolidation of C05-C14 capability findings. |
| ETLP-102: Produce Architecture Readiness Audit | Open | `13 – Architecture Consolidation & Production Readiness` | Issue body references consolidation of C05-C15 audit findings. |

---

## Canonical Sources

| Concern | Canonical Source |
| --- | --- |
| Runtime | [Architecture](../architecture.md) |
| FSM | [ADR-0004: FSM, Workflow, Runtime, And Messaging Boundary](../decisions/0004-fsm-isolation-and-execution-boundaries.md) |
| Messaging | [Messaging Model Specification](../specifications/messaging-model.md) |
| Reliability | [Reliability Model Specification](../specifications/reliability-model.md) |
| Configuration | [Biometric Device Configuration Specification](../specifications/biometric-device-configuration.md) |
| Storage | Not found: no single active storage specification or storage contract exists in tracked repository documentation. |
| Transformation | [TransformationStrategy Specification](../specifications/transformation-strategy.md) |
| Device Layer | [BiometricDevice Specification](../specifications/biometric-device.md) |
| ZKTeco Device | [ZKTecoDevice Specification](../specifications/zkteco-device.md) |
| Device Construction | [BiometricDeviceFactory Specification](../specifications/device-factory.md) |
| Canonical Attendance Event | [Canonical Attendance Event](../contracts/canonical-attendance-event.md) |
| Testing | [Validation Gates](../workflows/validation-gates.md) |
| CI | [CI workflow](../../.github/workflows/ci.yml) |
| Operational Architecture | Not found: no active operational architecture or deployment specification exists in tracked repository documentation. |
| Deployment | Not found: no active deployment architecture specification exists in tracked repository documentation. |
| Documentation Governance | [Documentation Governance](../governance/documentation.md) |
| Workflow | [Workflows Index](../workflows/README.md) |
| Portfolio | [Future Work](../future-work.md) |

---

## GitHub Traceability

GitHub metadata was verified with `gh issue list --state all --limit 200 --json number,title,state,labels,milestone,url`,
`gh api repos/:owner/:repo/milestones --paginate`, and `gh api repos/:owner/:repo/labels --paginate`.

Architecture consolidation milestone:

| Milestone | State | Due Date | Open Issues | Closed Issues |
| --- | --- | --- | ---: | ---: |
| `13 – Architecture Consolidation & Production Readiness` | Open | 2026-06-26 | 7 | 0 |

Architecture consolidation issues:

| Issue | State | Labels | Artefact Mapping |
| --- | --- | --- | --- |
| ETLP-99: Consolidate Architecture Artefacts | Open | documentation, architecture, roadmap, Highest | This inventory maps to the artefact-consolidation scope. |
| ETLP-100: Define Production Readiness Criteria | Open | documentation, roadmap | No tracked production-readiness specification exists yet. |
| ETLP-101: Consolidate Architecture Capability Matrix | Open | documentation, architecture, roadmap | No tracked capability matrix exists yet. |
| ETLP-102: Produce Architecture Readiness Audit | Open | documentation, architecture, roadmap | No tracked architecture readiness audit exists yet. |
| ETLP-103: Produce Repository Gap Analysis | Open | documentation, architecture, roadmap | No tracked repository gap analysis exists yet. |
| ETLP-104: Complete Architecture Documentation | Open | documentation, architecture, roadmap | Issue metadata references remaining architecture documentation, including storage abstraction and operational deployment specifications. |
| ETLP-105: Finalise Implementation Roadmap | Open | documentation, architecture, roadmap | Issue metadata references roadmap finalisation and architecture freeze. |

Existing architecture artefact issue mappings:

| Artefact Area | GitHub Issue Evidence | Milestone |
| --- | --- | --- |
| Repository package layout | ETLP-6: Create Python Package Layout | `00 - Repository Execution Foundation` |
| AI-agent operating instructions | ETLP-11: Add AI Agent Operating Instructions | `00 - Repository Execution Foundation` |
| Local development workflow | ETLP-12: Document Local Development Workflow | `00 - Repository Execution Foundation` |
| README restructuring | ETLP-13: Restructure README | `01 - Documentation & Repo Polish` |
| Canonical event schema | ETLP-14: Add Canonical Event Schema | `01 - Documentation & Repo Polish` |
| High-level architecture diagram | ETLP-15: Add Architecture Diagram | `01 - Documentation & Repo Polish` |
| Data pipeline document | ETLP-16: Document Data Pipeline Flow | `01 - Documentation & Repo Polish` |
| Reliability documentation | ETLP-17: Document Reliability Mechanisms | `01 - Documentation & Repo Polish` |
| Architecture document | ETLP-18: Create Architecture Document | `01 - Documentation & Repo Polish` |
| Data contract document | ETLP-19: Create Data Contract Document | `01 - Documentation & Repo Polish` |
| Future roadmap document | ETLP-20: Create Future Roadmap Document | `01 - Documentation & Repo Polish` |
| Ingestion modularisation | ETLP-21: Split Raspberry Pi Client | `02 - Repository Structure Modernisation` |
| Configuration module | ETLP-22: Create Config Module | `02 - Repository Structure Modernisation` |
| Logging module | ETLP-23: Introduce Logging Module | `02 - Repository Structure Modernisation` |
| Domain models | ETLP-24: Introduce Data Models | `02 - Repository Structure Modernisation` |
| Device interface | ETLP-25: Create Device Interface | `03 - Device Layer Abstraction` |
| ZKTeco adapter | ETLP-26: Implement ZKTeco Adapter | `03 - Device Layer Abstraction` |
| Device factory | ETLP-27: Implement Device Factory | `03 - Device Layer Abstraction` |
| Device extraction move | ETLP-28: Move Extraction Logic | `03 - Device Layer Abstraction` |
| Transformation module | ETLP-29: Create Transformation Module | `04 - Transformation Layer` |
| Attendance domain models | ETLP-30: Replace Attendance Domain Models | `04 - Transformation Layer` |
| Transformation strategy template method | ETLP-31: Implement TransformationStrategy Template Method | `04 - Transformation Layer` |
| ZKTeco transformation strategy | ETLP-32: Implement ZKTeco Transformation Strategy | `04 - Transformation Layer` |
| Messaging interface | ETLP-33: Create Messaging Interface | `05 - Messaging Abstraction` |
| Pub/Sub adapter | ETLP-34: Implement Pub/Sub Adapter | `05 - Messaging Abstraction` |
| Messaging logic migration | ETLP-35: Move Messaging Logic | `05 - Messaging Abstraction` |
| Retry handling | ETLP-36: Implement Retry Handling | `05 - Messaging Abstraction` |
| Storage interface | ETLP-37: Create Storage Interface | `06 - Storage Abstraction` |
| Google Sheets adapter | ETLP-38: Implement Google Sheets Adapter | `06 - Storage Abstraction` |
| PostgreSQL adapter | ETLP-39: Implement PostgreSQL Adapter | `06 - Storage Abstraction` |
| Database schema | ETLP-40: Create Database Schema | `06 - Storage Abstraction` |
| Database migration script | ETLP-41: Add DB Migration Script | `06 - Storage Abstraction` |
| FSM module | ETLP-42: Extract FSM Module | `07 - FSM Isolation` |
| FSM state enum | ETLP-43: Define FSM State Enum | `07 - FSM Isolation` |
| FSM transitions | ETLP-44: Implement FSM Transitions | `07 - FSM Isolation` |
| Snapshot persistence | ETLP-45: Extract Snapshot Persistence | `07 - FSM Isolation` |
| Transformation tests | ETLP-46: Add Transformation Tests | `08 - Testing` |
| FSM tests | ETLP-47: Add FSM Tests | `08 - Testing` |
| Messaging and storage tests | ETLP-48: Add Messaging & Storage Tests | `08 - Testing` |
| Core logic tests | ETLP-49: Extend Core Logic Test Coverage | `08 - Testing` |
| Coverage reporting | ETLP-50: Add Coverage Reporting | `09 - CI Pipeline` |
| Dependency audit | ETLP-51: Add Dependency Audit | `09 - CI Pipeline` |
| System diagrams | ETLP-52: Add System Diagrams | `10 - Portfolio Enhancements` |
| Local development guide | ETLP-53: Add Local Development Guide | `10 - Portfolio Enhancements` |
| Example event payloads | ETLP-54: Add Example Event Payloads | `10 - Portfolio Enhancements` |
| Engineering concepts section | ETLP-56: Add Engineering Concepts Section | `10 - Portfolio Enhancements` |
| Documentation governance baseline | ETLP-97: Establish architecture and documentation governance baseline | `11 - Documentation Alignment` |

GitHub labels relevant to architecture inventory:

| Label | Description |
| --- | --- |
| architecture | Functional area |
| documentation | Functional area |
| diagram | Functional area |
| data-pipeline | Functional area |
| device | Functional area |
| messaging | Functional area |
| reliability | Functional area |
| testing | Functional area |
| ci | Functional area |
| roadmap | Roadmap work |
| configuration | Configuration work |
| database | Functional area |
| automation | Automation work |
| release-engineering | Description not found. |

---

## Missing Canonical Artefacts

Factual gaps found in tracked repository artefacts:

| Missing Artefact | Repository Evidence |
| --- | --- |
| Storage abstraction specification or contract | No tracked `docs/specifications/` or `docs/contracts/` file owns storage abstraction. GitHub milestone `06 - Storage Abstraction` and ETLP-37 through ETLP-41 exist. |
| Operational architecture specification | No tracked architecture, specification, or contract file owns operational architecture as a standalone concern. |
| Deployment architecture specification | No tracked architecture, specification, or contract file owns deployment architecture as a standalone concern. |
| Production readiness specification | No tracked production-readiness specification exists. ETLP-100 exists and is open. |
| Architecture capability matrix | No tracked capability matrix exists. ETLP-101 exists and is open. |
| Architecture readiness audit | No tracked architecture readiness audit exists. ETLP-102 exists and is open. |
| Repository gap analysis | No tracked repository gap analysis exists. ETLP-103 exists and is open. |
| Review-period payload contract | No tracked contract document owns review-period payload schema. |
| Review-sheet response contract | No tracked contract document owns review-sheet response schema. |
| Last-stored timestamp response contract | No tracked contract document owns last-stored timestamp response schema. |
| C05-C15 audit reports | No tracked C05-C15 audit report files were found. |

---

## Architecture Inventory Status

| Area | Complete | Partial | Missing |
| --- | --- | --- | --- |
| ADRs | Six accepted ADRs are present and indexed. | Not applicable. | Not found. |
| Specifications | Seven accepted active specifications are present. | Storage and deployment specifications are not present. | Storage abstraction specification; operational deployment specification. |
| Proposals | Four proposal files are present and indexed. | Messaging proposal remains active; three proposals are implemented/evolved. | No proposal found for storage abstraction or operational deployment. |
| Contracts | Canonical attendance event contract is present. | Other payload or storage contracts are not present. | Review-period, review-sheet response, last-stored timestamp, and storage abstraction contracts. |
| Architecture Documents | Architecture, data pipeline, future work, README, and documentation index are present. | `data-pipeline.md`, `future-work.md`, and `README.md` do not declare status/lifecycle fields. | Operational architecture document not found. |
| Workflow Documentation | Workflow index and five workflow documents are present. | Not applicable. | Not found. |
| Governance Documentation | Documentation governance and AI-agent operating instructions are present. | `AGENTS.md` does not declare status/lifecycle fields. | Not found. |
| Diagrams | Six file-backed diagrams and two embedded Mermaid diagrams are present. | Diagram alignment work remains represented by open GitHub issues ETLP-52 and ETLP-59. | No additional tracked diagram audit report found. |
| Audit Reports | Not applicable. | Not applicable. | No tracked C05-C15 audit report files found. |
| GitHub Traceability | Milestones, issues, and labels were verified with GitHub CLI. | Some open issues reference artefacts not yet present in tracked files. | Tracked documents for ETLP-100 through ETLP-103 outputs were not found. |
