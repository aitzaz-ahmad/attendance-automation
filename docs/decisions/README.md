# Architecture Decision Records (ADRs)

This directory contains Architecture Decision Records (ADRs) for the Attendance Automation project.

## Purpose

Architecture documents describe the current structure of the system.

Architecture Decision Records describe the reasoning behind important design decisions.

An ADR captures:

- the problem or context
- the decision that was made
- alternatives that were considered
- the consequences of the decision

The goal is to preserve engineering rationale so future contributors can understand why the system was designed a certain way.

## When To Create An ADR

Create an ADR when a decision:

- significantly affects the architecture
- influences future implementation work
- introduces or rejects an abstraction
- changes system boundaries or ownership
- is likely to be revisited in the future

Examples include:

- introducing a new persistence layer
- selecting a messaging technology
- defining modularisation boundaries
- introducing domain models
- establishing repository-wide conventions

## ADR Lifecycle

Each ADR should contain a status:

| Status | Meaning |
|----------|----------|
| Draft | Under discussion and not yet adopted |
| Accepted | Approved and considered part of the architecture |
| Superseded | Replaced by a newer ADR |
| Archived | Moved to the archive and retained only as non-active record |

When an ADR is superseded or archived, it should remain in the repository for historical context.

## Relationship To Other Documentation

The documentation hierarchy is:

1. ADRs explain why decisions were made.
2. Specifications define what behaviour and technical contracts are required.
3. Contracts define data and repository-level behavioural contracts.
4. Proposals preserve how changes were intended to be implemented.
5. Architecture documentation describes where system responsibilities live.
6. Archive content preserves superseded or non-active material.

When introducing significant architectural changes, consult the relevant ADRs before modifying the design.

## Index

| ADR | Title | Status |
|----------|----------|----------|
| [ADR-0001](0001-repo-modernisation-design.md) | Repository Modernisation Design Decisions | Accepted |
| [ADR-0002](0002-model-adoption-principles.md) | Domain Model Adoption Principles | Accepted |
| [ADR-0003](0003-device-and-transformation-layer-boundaries.md) | Device And Transformation Layer Boundaries | Accepted |
| [ADR-0004](0004-fsm-isolation-and-execution-boundaries.md) | FSM, Workflow, Runtime, And Messaging Boundary | Accepted |
| [ADR-0005](0005-messaging-abstraction-and-routing-boundary.md) | Messaging Abstraction And Routing Boundary | Accepted |
| [ADR-0006](0006-reliability-and-recovery.md) | Reliability And Recovery | Accepted |

## Naming Convention

ADR files use the following naming scheme:

text 0001-short-decision-title.md 0002-another-decision.md 0003-example-decision.md

Numbers should be sequential and never reused.

## Notes

ADRs are intentionally lightweight.

The objective is not exhaustive documentation. The objective is to capture important engineering decisions while the reasoning is still fresh.
