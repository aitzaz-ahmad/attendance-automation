# Documentation Index

This directory contains the primary project documentation.

## Documentation Hierarchy

The repository documentation is organised according to the following hierarchy:

| Documentation Type | Purpose |
|----------|----------|
| Architecture Decision Records (ADRs) | Explain **why** important architectural decisions were made |
| Specifications | Define **what** behaviour and technical contracts are required |
| Contracts | Define **data** and repository-level behavioural contracts |
| Design Proposals | Preserve **how** significant changes were intended to be implemented |
| Architecture Documentation | Describe **where** system responsibilities live |
| Archive | Preserve **superseded** or non-active material |
| Workflows | Describe **how** contributors develop, validate, and maintain the project |
| Future Work | Capture potential future evolution paths |
| Governance | Define **how** documentation ownership, lifecycle, and audits work |

A useful rule of thumb is:

    ADRs           -> Why
    Specifications -> What
    Contracts      -> Data
    Proposals      -> How
    Architecture   -> Where
    Archive        -> Superseded material
    Workflows      -> How we work
    Future Work    -> Where we may go
    Governance     -> How documentation is governed

When introducing significant architectural changes:

1. Consult relevant ADRs to understand the rationale.
2. Review specifications and contracts for the required behaviour and data shape.
3. Review any associated design proposals for implementation intent.
4. Update architecture documentation to reflect where responsibilities live.

## Engineering Principles

The following principles guide architectural evolution, design decisions,
and implementation work throughout the project.

### Requirement-Driven Architecture

Do not introduce new architectural abstractions unless:

1. They already exist in the codebase.
2. They are explicitly required by a ticket or specification.
3. They are explicitly proposed and approved during design review.

Architectural evolution should be driven by requirements rather than by architectural patterns or perceived symmetry.

The project prefers extending existing abstractions over introducing speculative layers, managers, repositories, registries, adapters, factories, stores, or other constructs without a demonstrated need.

## Decisions

- [Decision records index](decisions/README.md)
- [ADR-0001 — Repository Modernisation Design](decisions/0001-repo-modernisation-design.md)
- [ADR-0002 — Model Adoption Principles](decisions/0002-model-adoption-principles.md)
- [ADR-0003 — Device And Transformation Layer Boundaries](decisions/0003-device-and-transformation-layer-boundaries.md)
- [ADR-0004 — FSM, Workflow, Runtime, And Messaging Boundary](decisions/0004-fsm-isolation-and-execution-boundaries.md)
- [ADR-0005 — Messaging Abstraction And Routing Boundary](decisions/0005-messaging-abstraction-and-routing-boundary.md)
- [ADR-0006 — Reliability And Recovery](decisions/0006-reliability-and-recovery.md)

## Specifications

- [Biometric Device Configuration specification](specifications/biometric-device-configuration.md)
- [BiometricDevice specification](specifications/biometric-device.md)
- [BiometricDeviceFactory specification](specifications/device-factory.md)
- [Messaging model specification](specifications/messaging-model.md)
- [Reliability model specification](specifications/reliability-model.md)
- [TransformationStrategy specification](specifications/transformation-strategy.md)
- [ZKTecoDevice specification](specifications/zkteco-device.md)

## Contracts

- [Contracts index](contracts/README.md)
- [Canonical attendance event](contracts/canonical-attendance-event.md)

## Governance

- [Documentation governance](governance/documentation.md)

## Proposals

- [Proposals index](proposals/README.md)
- [ETLP-21 — Ingestion Modularisation](proposals/ingestion-modularisation.md)
- [Device Layer Abstraction](proposals/device-layer-abstraction.md)
- [Transformation Layer](proposals/transformation-layer.md)
- [Messaging Abstraction](proposals/messaging-abstraction.md)

## Architecture

- [Architecture](architecture.md)
- [Data pipeline](data-pipeline.md)
- [Reliability model](specifications/reliability-model.md)
- [Future work](future-work.md)

## Archive

- [Documentation consolidation plan](archive/doc-consolidation-plan.md)
- [Reliability model v1](archive/reliability-v1.md)

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
