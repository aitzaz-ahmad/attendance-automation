# Documentation Index

This directory contains the primary project documentation.

## Documentation Hierarchy

The repository documentation is organised according to the following hierarchy:

| Documentation Type | Purpose |
|----------|----------|
| Architecture Decision Records (ADRs) | Explain **why** important architectural decisions were made |
| Design Proposals | Explain **how** significant changes were intended to be implemented |
| Architecture Documentation | Describe **what** the system currently looks like |
| Workflows | Describe **how** contributors develop, validate, and maintain the project |
| Contracts | Define data contracts and interface boundaries |
| Future Work | Capture potential future evolution paths |

A useful rule of thumb is:

    ADRs         -> Why
    Proposals    -> How we planned it
    Architecture -> What exists today
    Workflows    -> How we work
    Contracts    -> Data/interface definitions
    Future Work  -> Where we may go

When introducing significant architectural changes:

1. Consult relevant ADRs to understand the rationale.
2. Review any associated design proposals for implementation intent.
3. Update architecture documentation to reflect the final system state.

## Architecture

- [Architecture](architecture.md)
- [Data pipeline](data-pipeline.md)
- [Reliability model](reliability.md)
- [Future work](future-work.md)

## Decisions

- [Decision records index](decisions/README.md)
- [ADR-0001 — Repository Modernisation Design Decisions](decisions/0001-repository-modernisation-design-decisions.md)

## Proposals

- [Proposals index](proposals/README.md)
- [ETLP-21 — Ingestion Modularisation](proposals/ingestion-modularisation.md)

## Contracts

- [Canonical attendance event](contracts/canonical-attendance-event.md)

## Diagrams

- [High-level architecture diagram](diagrams/high-level-architecture.png)
- [Raspberry Pi client finite state machine](diagrams/pi4-client-fsm.png)

## Workflows

- [Workflows index](workflows/README.md)
- [Local development](workflows/local-development.md)
- [Validation gates](workflows/validation-gates.md)
- [Branch strategy](workflows/branch-strategy.md)
- [Commit conventions](workflows/commit-conventions.md)
- [Issue lifecycle](workflows/issue-lifecycle.md)
