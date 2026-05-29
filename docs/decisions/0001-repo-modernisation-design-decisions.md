# ADR-0001 — Repository Modernisation Design Decisions

## Status

Accepted

## Context

During Milestone 2 (Repository Structure Modernisation), several architectural decisions were made while refactoring the ingestion client into a modular structure.

These decisions are intended to guide future refactoring work and prevent unnecessary architectural drift.

---

## Decision 1: Modularisation Before Abstraction

The project prioritises separation of responsibilities before introducing extensibility abstractions.

The primary goal is to improve maintainability and testability of the existing system.

As a result:

- ETLP-21 owns modularisation.
- ETLP-22 owns configuration centralisation.
- ETLP-23 owns logging centralisation.
- ETLP-24 owns domain model introduction.

The project intentionally avoids introducing abstractions that are not yet required by a concrete use case.

### Consequences

The system may contain fewer interfaces than a typical enterprise architecture.

This is intentional.

Abstractions should be introduced only when they solve an existing problem.

---

## Decision 2: No Device Interface At Present

The ingestion system currently supports a single biometric device implementation.

An AttendanceDevice interface or protocol was considered during ETLP-21.

The interface was rejected because:

- only one device vendor is supported;
- no alternate implementation exists;
- no runtime device selection exists;
- no test seam currently requires it.

### Consequences

The system uses a concrete ZKTecoDevice implementation.

A future multi-vendor requirement may justify introducing a device interface.

---

## Decision 3: Responsibility-Oriented Naming

Component names should represent architectural responsibilities rather than implementation technologies.

Names should remain stable even when underlying technologies change.

### Examples

Preferred:

- DeviceClient
- MessagingClient
- RecordTransformer
- RuntimeState
- ReviewPeriodStore
- Pi4Workflow
- Pi4Runtime
- IngestionClient

Avoid:

- PubSubClient
- GoogleMessaging
- ZKTecoLogger
- RabbitMqPublisher

### Consequences

Operational tooling, dashboards, and log analysis remain stable across technology migrations.

---

## Decision 4: Logging Names Follow Responsibilities

Logger identities should represent the responsibility of the component rather than the implementation technology.

Example:

text [MessagingClient] [RuntimeState] [Pi4Workflow]

rather than:

text [PubSub] [ZKTeco] [GoogleCloud]

### Consequences

Future migrations between cloud providers, messaging systems, or hardware vendors should not require logger renaming.

---

## Decision 5: RuntimeState Is A Domain Concept

The persisted ingestion checkpoint represents the complete runtime state required to resume processing after interruption.

The term RuntimeState was selected because it accurately describes the responsibility without exposing implementation details such as snapshots or checkpoints.

### Current Scope

Within ETLP-23 and earlier milestones, RuntimeState refers to the responsibility represented by the persisted state.

### Future Scope

ETLP-24 is expected to introduce a dedicated RuntimeState domain model.

Possible future direction:

- RuntimeState (domain model)
- RuntimeStateStore (persistence mechanism)

This preserves a clear separation between state representation and state persistence.

---

## Decision 6: Canonical Event Adoption Remains Deferred

The canonical attendance event contract is documented.

However, ETLP-21 through ETLP-23 do not adopt the canonical contract in runtime processing.

The ingestion pipeline continues to use existing transitional payloads.

### Consequences

Refactoring and modularisation can proceed without introducing behavioural risk.

Canonical contract adoption will be addressed by a dedicated future change.
