# Domain Model Adoption Principles

## Context

ETLP-24 introduced the following domain models:

- `AttendanceEvent`
- `Employee`
- `ReviewPeriod`
- `RuntimeState`

The initial implementation created model classes but did not fully adopt them as internal contracts. Existing dictionaries and raw field collections remained the primary mechanism for exchanging data between modules.

This increased complexity without significantly improving the architecture.

## Decision

Domain models shall become the primary internal contracts between modules.

Models are not introduced merely to wrap existing dictionaries.

A model migration is considered complete only when internal module boundaries exchange model instances rather than dictionaries.

## Internal Boundary Rule

Dictionaries shall exist only at:

- serialization boundaries
- deserialization boundaries
- persistence boundaries
- external integration boundaries

Internal module boundaries shall exchange domain models.

## AttendanceEvent

AttendanceEvent is the canonical internal representation of an attendance event.

The ingestion workflow shall operate on AttendanceEvent objects rather than attendance dictionaries.

Device-specific transformations shall convert proprietary attendance records into AttendanceEvent objects as early as possible.

Future device integrations shall integrate through AttendanceEvent rather than introducing new workflow-specific record formats.

## Employee

Employee is the canonical internal representation of a device user.

Employee collections shall store Employee objects rather than names or ad-hoc dictionaries.

Preferred representation:

    Dict[user_id, Employee]

Employee instances shall be reused throughout the ingestion pipeline.

## RuntimeState

RuntimeState is the canonical representation of persisted ingestion state.

Snapshot persistence shall operate on RuntimeState objects.

RuntimeState owns serialization and deserialization.

Snapshot persistence components own file I/O.

## ReviewPeriod

ReviewPeriod is the canonical representation of review period information.

Review-period persistence shall operate on ReviewPeriod objects.

ReviewPeriod owns serialization and deserialization.

Persistence helpers own file I/O.

## Serialization Interfaces

Domain models shall implement:

- `ISerializable`
- `IDeserializable`

`ISerializable`:

    to_dict() -> Dict[str, Any]

`IDeserializable`:

    from_dict(payload: Dict[str, Any])

The exact serialized shape depends on the model's declared contract.

## Architectural Principle


Creating a model without replacing the dictionary-based internal contract is not considered successful model adoption.

## Scope Boundaries

### Context

During ETLP-24 review, it became clear that some model-adoption work overlapped with future roadmap items.

### Decision

ETLP-24 owns:

- `AttendanceEvent` definition
- `Employee` adoption
- `ReviewPeriod` adoption
- `RuntimeState` adoption
- `ISerializable`
- `IDeserializable`

ETLP-24 does not own:

- device abstraction
- canonical transformation
- messaging abstraction

### Deferred Roadmap Items

Device abstraction remains owned by:

    Milestone 3 — Device Layer Abstraction

Canonical attendance-event adoption remains owned by:

    Milestone 4 — Transformation Layer

Messaging abstraction remains owned by:

    Milestone 5 — Messaging Abstraction

### Consequences

Model adoption may prepare future roadmap work but must not consume the scope of those milestones prematurely.
