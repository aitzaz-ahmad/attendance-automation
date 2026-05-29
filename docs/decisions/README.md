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
| Proposed | Under discussion and not yet adopted |
| Accepted | Approved and considered part of the architecture |
| Superseded | Replaced by a newer ADR |
| Rejected | Considered but intentionally not adopted |

When an ADR is superseded, it should remain in the repository for historical context.

## Relationship To Other Documentation

The documentation hierarchy is:

1. ADRs explain why decisions were made.
2. Architecture documentation explains what the system looks like.
3. Implementation documentation explains how the system works.

When introducing significant architectural changes, consult the relevant ADRs before modifying the design.

## Index

| ADR | Title | Status |
|----------|----------|----------|
| ADR-0001 | Repository Modernisation Design Decisions | Accepted |

## Naming Convention

ADR files use the following naming scheme:

text 0001-short-decision-title.md 0002-another-decision.md 0003-example-decision.md

Numbers should be sequential and never reused.

## Notes

ADRs are intentionally lightweight.

The objective is not exhaustive documentation. The objective is to capture important engineering decisions while the reasoning is still fresh.
