# Operational Architecture

## Metadata

| Field | Value |
| --- | --- |
| Status | Accepted |
| Lifecycle | Active |
| Owner | Operational runtime topology and execution-boundary requirements for Attendance Automation v2 |
| Related ADRs | [ADR-0004](../decisions/0004-fsm-isolation-and-execution-boundaries.md), [ADR-0005](../decisions/0005-messaging-abstraction-and-routing-boundary.md), [ADR-0006](../decisions/0006-reliability-and-recovery.md) |
| Related specifications | [Production Readiness](production-readiness.md), [Deployment And Provisioning](deployment-and-provisioning.md), [Runtime Bootstrap](runtime-bootstrap.md), [Reliability Model](reliability-model.md), [Messaging Model](messaging-model.md), [Biometric Device Configuration](biometric-device-configuration.md) |
| Related contracts | [Canonical Attendance Event](../contracts/canonical-attendance-event.md), [Backend Response Payloads](../contracts/backend-response-payloads.md) |
| Related proposals, if any | None for this canonical owner |
| Related audits | [Architecture Artefact Inventory](../audits/architecture-artefact-inventory.md), [Architecture Readiness Audit](../audits/architecture-readiness-audit.md), [Repository Gap Analysis](../audits/repository-gap-analysis.md) |

## Purpose

This specification defines the canonical operational architecture for
Attendance Automation v2.

It owns the required runtime roles, execution boundaries, operational
responsibilities, and v2/future boundary for operating the current Pi4,
Google Pub/Sub, Google Cloud Functions, Google Sheets, Google Drive, local
runtime-file, and biometric-device topology.

This document records what the v2 operational architecture requires. It does
not redesign the architecture or replace the system placement described in
[Architecture](../architecture.md) and [Data Pipeline](../data-pipeline.md).
It defines the logical operational architecture. Physical deployment mechanics
remain the responsibility of
[Deployment And Provisioning](deployment-and-provisioning.md).

## Scope

This specification covers:

- runtime topology;
- Pi4 client runtime role;
- backend Cloud Function runtime role;
- Google Pub/Sub runtime role;
- Google Sheets and Google Drive runtime role;
- local runtime-state file role;
- external biometric device role;
- runtime lifecycle;
- execution boundaries;
- operational ownership;
- restart and recovery relationship to the [Reliability Model](reliability-model.md);
- v2 mandatory operational requirements; and
- v3/future operational boundaries.

This specification does not cover:

- deployment step-by-step instructions;
- infrastructure-as-code;
- enterprise monitoring dashboards;
- code implementation details;
- payload field definitions owned by contracts; or
- transport mechanics owned by the [Messaging Model](messaging-model.md).

## Ownership

This specification owns logical operational architecture requirements only.
Physical deployment mechanics remain owned by
[Deployment And Provisioning](deployment-and-provisioning.md).

Adjacent ownership remains with the existing canonical documents:

| Concern | Canonical owner |
| --- | --- |
| System placement and topology overview | [Architecture](../architecture.md) |
| Pipeline sequencing | [Data Pipeline](../data-pipeline.md) |
| Deployment and provisioning requirements | [Deployment And Provisioning](deployment-and-provisioning.md) |
| Pi4 bootstrap and seed-file behaviour | [Runtime Bootstrap](runtime-bootstrap.md) |
| Restart, checkpoint, recovery, retry, idempotency, and delivery guarantees | [Reliability Model](reliability-model.md) |
| Messaging topics, routing, ACK policy, timeout semantics, and adapter responsibilities | [Messaging Model](messaging-model.md) |
| Attendance event payload shape | [Canonical Attendance Event](../contracts/canonical-attendance-event.md) |
| Backend-to-Pi4 response payload shape | [Backend Response Payloads](../contracts/backend-response-payloads.md) |
| Biometric device configuration shape and fail-fast rules | [Biometric Device Configuration](biometric-device-configuration.md) |

When an operational concern depends on one of these owners, this specification
references that owner instead of repeating its detailed rules.

## v2 Requirements

Attendance Automation v2 has the following mandatory operational runtime
topology:

```text
External biometric device
    -> Pi4 ingestion runtime
    -> Google Pub/Sub
    -> Google Cloud Function wrappers
    -> attendance_etl.functions backend modules
    -> Google Sheets / Google Drive review output
    -> Google Pub/Sub response topics
    -> Pi4 ingestion runtime
```

The Pi4 runtime is the commissioned ingestion runtime for v2. It must:

- load validated biometric device configuration before constructing the
  concrete device;
- compose the biometric device, transformation strategy, messaging boundary,
  runtime state, review-period state, and workflow;
- poll the external biometric device through the device abstraction;
- coordinate business side effects through the workflow;
- use Google Pub/Sub as the v2 messaging transport through the project-owned
  messaging boundary;
- persist and load local runtime files through the runtime bootstrap and
  persistence owners; and
- resume after interruption according to the [Reliability Model](reliability-model.md).

The backend Cloud Function runtime is the v2 serverless processing runtime. It
must:

- use deployable wrappers under `src/backend/*/main.py`;
- delegate reusable behaviour to `attendance_etl.functions`;
- process review-period lookup, review-sheet creation, and attendance-record
  storage workflows;
- interact with Google Sheets and Google Drive for current review-output
  persistence; and
- publish backend-to-Pi4 response payloads compatible with
  [Backend Response Payloads](../contracts/backend-response-payloads.md).

Google Pub/Sub is the v2 runtime broker between the Pi4 runtime and backend
Cloud Functions. It must carry request and response messages according to the
[Messaging Model](messaging-model.md). Transport routing, ACK policy, timeout
semantics, provider metadata, and adapter responsibilities are not redefined by
this specification.

Google Sheets and Google Drive are the v2 review-output and document-storage
surfaces. They provide the current persistence surface for attendance review
outputs, review-period lookup, review-sheet creation, raw attendance rows,
daily attendance rows, weekly summaries, and last-stored timestamp feedback.
PostgreSQL is future storage only unless a later accepted owner changes that
boundary.

Local runtime files are v2 operational inputs and recovery surfaces. The Pi4
runtime uses:

- `biometric_device_config.json` for commissioned device configuration;
- `review_period.json` for persisted review-period metadata; and
- `snapshot.json` for persisted runtime checkpoint state.

The detailed bootstrap expectations for these files are owned by
[Runtime Bootstrap](runtime-bootstrap.md).

The external biometric device is the source attendance system. In v2, the
current concrete source is a ZKTeco biometric device behind the project-owned
device abstraction. Device configuration shape, vendor options, and fail-fast
validation are owned by
[Biometric Device Configuration](biometric-device-configuration.md).

The v2 runtime lifecycle is split into four operational phases.

### Provisioning

Provisioning prepares the runtime environment before workflow execution. It
covers required configuration, credentials, Pub/Sub resources, backend wrappers,
Google Sheets/Drive resources, and local runtime files. Detailed provisioning
requirements are owned by
[Deployment And Provisioning](deployment-and-provisioning.md).

### Bootstrap

Bootstrap constructs the Pi4 runtime from validated configuration and local
state. It loads required local files, composes runtime dependencies, and
creates the workflow before execution starts. Detailed bootstrap order and
seed-file expectations are owned by [Runtime Bootstrap](runtime-bootstrap.md).

### Runtime

Runtime execution polls the biometric device, transforms attendance data,
publishes workflow requests and attendance payloads through Google Pub/Sub,
processes backend work in Cloud Functions, updates Google Sheets/Drive, returns
backend responses to the Pi4 runtime, validates and applies those responses in
workflow/action-service logic, and persists eligible non-waiting runtime
checkpoints.

### Recovery

Recovery resumes runtime execution after interruption. On restart, the runtime
recovers from the last checkpointed non-waiting state according to the
[Reliability Model](reliability-model.md). Recovery must preserve the existing
checkpoint and waiting-state boundaries.

Execution boundaries are mandatory:

- runtime composition belongs to the Pi4 runtime;
- business side effects belong to workflow/action-service logic;
- FSM transition policy, waiting-state classification, and checkpoint
  eligibility follow ADR-0004 and the [Reliability Model](reliability-model.md);
- transport mechanics belong to the messaging boundary;
- response payload fields belong to contracts;
- backend durable effects belong to backend/storage integrations; and
- deployment/provisioning requirements belong to
  [Deployment And Provisioning](deployment-and-provisioning.md).

Operational ownership is split by runtime surface:

| Runtime surface | v2 operational ownership |
| --- | --- |
| Pi4 runtime | Bootstrap, workflow composition, device polling, local files, and restart entry point |
| Backend Cloud Functions | Serverless entry points and backend workflow processing |
| Google Pub/Sub | Runtime transport broker and topic/subscription resources |
| Google Sheets / Google Drive | Current review-output persistence and backing document access |
| Local runtime files | Pi4 configuration, review-period metadata, and checkpoint state |
| External biometric device | Source attendance records and device-clearing boundary |

Restart and recovery must preserve the reliability model. Waiting states must
not be persisted as restart targets, and recovery must resume from the last
checkpointed non-waiting state. Duplicate sends or repeated backend attempts
after restart must be tolerated within the v2 reliability model without
claiming exactly-once transport or complete business idempotency.

## v3 / Future Boundary

The following concerns are outside v2 operational architecture unless promoted
by a later accepted repository authority:

- full infrastructure-as-code;
- automated release promotion;
- blue/green deployment;
- enterprise monitoring dashboards;
- multi-site operational dashboards;
- commercial incident-management runbooks;
- PostgreSQL-first persistence;
- dead-letter queues and poison-message handling;
- event or correlation identifiers;
- stronger backend idempotency beyond the documented v2 model;
- advanced replay or recovery tooling; and
- backend publisher migration to depend on `Messenger`.

Future implementation details not specified by current accepted owners are
implementation-defined and belong to future proposal or implementation work.

## Non-Goals

This specification does not:

- introduce a new architecture;
- create ADRs or proposals;
- define deployment commands;
- define infrastructure-as-code;
- define commercial production operations;
- define payload schemas;
- define transport mechanics;
- define retry or idempotency policy;
- define storage abstraction interfaces; or
- update README files, diagrams, GitHub issues, or milestones.

## Validation

Changes to this specification must validate that:

- operational requirements preserve the topology in [Architecture](../architecture.md);
- no transport, payload, retry, checkpoint, or configuration rules are
  duplicated from their canonical owners;
- v2 requirements do not promote v3/future concerns into mandatory scope;
- local Markdown links resolve; and
- repository validation follows [Validation Gates](../workflows/validation-gates.md).
