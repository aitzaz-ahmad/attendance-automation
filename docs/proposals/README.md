# Design Proposals

This directory contains design proposals, implementation plans, and technical design documents for the Attendance Automation project.

## Purpose

Design proposals capture the intended design of a change before implementation begins.

A proposal typically documents:

- the problem being addressed
- the current state of the system
- the proposed architecture or design
- implementation strategy
- migration approach
- risks and constraints
- acceptance criteria

The goal is to preserve engineering intent and design rationale during significant refactoring or feature work.

## Relationship To Other Documentation

The documentation hierarchy is:

1. Architecture Decision Records (ADRs) explain why decisions were made.
2. Specifications explain what behaviour and technical contracts are required.
3. Contracts define repository-level data contracts.
4. Design Proposals explain how a change is intended to be implemented.
5. Architecture documentation explains where system responsibilities live.
6. Archive content preserves superseded or non-active material.

### Example

A modularisation effort may produce:

text docs/ ├── decisions/ │   └── 0001-repository-modernisation-design-decisions.md │ ├── proposals/ │   └── etlp-21-ingestion-modularisation.md │ └── architecture.md

Where:

- the ADR explains why modularisation was chosen;
- the proposal explains how modularisation should be performed;
- the architecture document describes the resulting system after implementation.

## When To Create A Proposal

Create a proposal when a change:

- significantly modifies architecture or module boundaries;
- introduces a new subsystem;
- affects deployment or operational behavior;
- requires a staged migration strategy;
- contains non-trivial implementation risk;
- benefits from review before implementation.

Examples include:

- large-scale refactoring
- persistence-layer migration
- messaging-system migration
- API introduction
- domain-model redesign
- cloud-platform migration

## Proposal Status And Lifecycle

Proposal status records approval:

| Status | Meaning |
|----------|----------|
| Draft | Proposal is being shaped and is not yet approved as implementation guidance. |
| Accepted | Proposal is approved as an implementation strategy. |
| Superseded | Proposal has been replaced by a newer proposal or decision and is no longer implementation guidance. |
| Archived | Proposal has been moved to the archive because it is superseded or retained only as non-active record. |

Proposal lifecycle records implementation state:

| Lifecycle | Meaning |
|----------|----------|
| Planned | Proposal records approved implementation intent that has not started. |
| Active | Proposal remains the current HOW document for in-progress or pending implementation work. |
| Implemented | Proposal records the implementation strategy that has been completed and remains useful implementation context. |
| Implemented / Evolved | Proposal records completed implementation work, but later ADRs, specifications, contracts, or proposals have changed the current authoritative design. |

A proposal should not be deleted after implementation. It preserves the implementation strategy that was executed and records how the system evolved.

Proposals with lifecycle `Implemented / Evolved` must defer to the current owning documents:

- ADRs explain why.
- Specifications define what.
- Contracts define data.
- Proposals describe how.
- Architecture describes where responsibilities live.
- Archive content preserves superseded or non-active material.

## Naming Convention

Proposal files should use descriptive names:

text etlp-21-ingestion-modularisation.md etlp-24-domain-model-introduction.md postgresql-migration.md api-layer-introduction.md

Where a proposal is directly associated with a GitHub issue, the issue identifier should be included in the filename.

## Navigation

### Active Implementation Strategy

| Proposal | Status | Lifecycle |
|----------|----------|----------|
| [Messaging Abstraction](messaging-abstraction.md) | Accepted | Active implementation strategy for ETLP-33 through ETLP-36 |

### Implemented / Evolved Proposal Records

| Proposal | Status | Lifecycle |
|----------|----------|----------|
| [ETLP-21 — Ingestion Modularisation](ingestion-modularisation.md) | Accepted | Implemented / Evolved; retained for ETLP-21/ETLP-24 migration context and later boundary evolution |
| [Device Layer Abstraction](device-layer-abstraction.md) | Accepted | Implemented / Evolved; current device behaviour is traceable through [ADR-0003](../decisions/0003-device-and-transformation-layer-boundaries.md) and device specifications |
| [Transformation Layer](transformation-layer.md) | Accepted | Implemented / Evolved; current transformation behaviour is traceable through [ADR-0003](../decisions/0003-device-and-transformation-layer-boundaries.md), the [transformation specification](../specifications/transformation-strategy.md), and the [canonical attendance event contract](../contracts/canonical-attendance-event.md) |

## Notes

Design proposals are intentionally more detailed than ADRs.

An ADR should remain concise and capture decisions.

A proposal may contain package structures, dependency diagrams, migration strategies, implementation phases, risk analysis, and acceptance criteria.

When both exist, the ADR should be consulted for the architectural rationale, while the proposal should be consulted for implementation intent and evolution context.
