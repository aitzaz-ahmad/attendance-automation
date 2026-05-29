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
2. Design Proposals explain how a change is intended to be implemented.
3. Architecture documentation explains what the system currently looks like.
4. Implementation documentation explains how the current implementation works.

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

## Proposal Lifecycle

| Status | Meaning |
|----------|----------|
| Draft | Early work-in-progress proposal |
| Proposed | Ready for review and discussion |
| Accepted | Approved implementation plan |
| Implemented | Implementation completed |
| Superseded | Replaced by a newer proposal |
| Rejected | Considered but intentionally not pursued |

A proposal should not be deleted after implementation. It provides historical context explaining how the system evolved.

## Naming Convention

Proposal files should use descriptive names:

text etlp-21-ingestion-modularisation.md etlp-24-domain-model-introduction.md postgresql-migration.md api-layer-introduction.md

Where a proposal is directly associated with a GitHub issue, the issue identifier should be included in the filename.

## Index

| Proposal | Status |
|----------|----------|
| ETLP-21 — Ingestion Modularisation | Implemented |

## Notes

Design proposals are intentionally more detailed than ADRs.

An ADR should remain concise and capture decisions.

A proposal may contain package structures, dependency diagrams, migration strategies, implementation phases, risk analysis, and acceptance criteria.

When both exist, the ADR should be consulted for the architectural rationale, while the proposal should be consulted for implementation intent and historical context.
