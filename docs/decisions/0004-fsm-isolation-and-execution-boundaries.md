# ADR-0004: FSM, Workflow, Runtime, And Messaging Boundary

## Status

Accepted

## Context

Milestone 4 is complete.

The next implementation sequence needs to decide finite-state-machine isolation before Messaging Abstraction work proceeds.

Messaging abstraction decisions depend on the FSM boundary, especially around:

- receive waits
- timeout semantics
- retry behavior
- waiting states
- checkpoint eligibility
- restart recovery

The current Pi4 runtime path still mixes FSM policy, workflow side effects, Pub/Sub receive loops, sleeps, and snapshot decisions across the runtime and workflow modules.

This ADR records the boundary decisions needed before implementing the messaging abstraction.

## Decision 1: Raise The Minimum Python Runtime To 3.10

The project shall raise its minimum supported Python runtime to Python 3.10.

This is a deliberate project-baseline change.

Rationale:

- Python 3.8 compatibility is no longer desirable for this portfolio project.
- Modern Python libraries and tooling increasingly assume Python 3.10 or newer.
- Current `python-statemachine` releases require Python >=3.10.

### Consequences

Implementation work must update repository Python version metadata and tool targets consistently.

This includes, at minimum:

- `project.requires-python`
- Ruff target version
- Black target version
- MyPy Python version
- CI/runtime documentation where applicable

## Decision 2: Adopt `python-statemachine` For Pi4 FSM Isolation

The project shall adopt `python-statemachine` for Pi4 FSM isolation.

This decision is conditional on raising the project runtime baseline to Python >=3.10.

The runtime-baseline decision and FSM-library decision are therefore ordered decisions, not independent decisions.

The library shall be treated as an implementation mechanism rather than an architectural owner of business logic.

Rationale:

- The project should prefer a community-supported FSM library over a proprietary or custom FSM implementation.
- The project goal is to standardise, modernise, decouple, and avoid reinventing the wheel.
- Portfolio value is improved by using recognised libraries with clear architectural boundaries.
- `python-statemachine` supports explicit states, event-based transitions, actions, guards, callbacks, validation, and diagrams.

## Decision 3: FSM Isolation Is A Redesign

FSM isolation shall be treated as a deliberate redesign, not a thin wrapper around the current integer constants, handler table, and transition helper.

The current implementation serves as behavioural and migration input.

It does not define the target architecture.

The FSM boundary shall own:

- state definitions
- event definitions
- transition rules
- transition validation
- guard conditions
- waiting-state classification
- checkpoint eligibility policy

Retry ownership remains deferred pending the Messaging Reliability ADR.

The FSM boundary shall not own:

- transport calls
- device extraction
- transformation
- record publication
- snapshot file I/O
- sleep execution
- backend payload construction

## Decision 4: Workflow Owns Business Side Effects

`Pi4Workflow` or future workflow action services shall remain responsible for coordinating business actions across project-owned abstractions.

Workflow actions include:

- requesting review periods
- requesting review sheets
- extracting biometric data
- invoking transformation
- publishing attendance records
- receiving backend responses through the messaging abstraction
- clearing device records when safe

The workflow layer shall convert action outcomes into FSM events.

The FSM shall decide the next state from those events.

## Decision 5: Runtime Owns Composition

`Pi4Runtime` shall remain the composition root.

Runtime owns:

- logging setup
- configuration loading
- device construction
- transformation strategy construction
- messaging adapter construction
- persisted state loading
- workflow construction
- workflow startup

Runtime shall not own transition policy.

Runtime shall not embed messaging retry policy.

## Decision 6: Messaging Must Not Hide Workflow Retry Loops

Messaging Abstraction receive semantics shall be bounded and explicit.

The exact representation of timeout or no-message outcomes remains deferred to the messaging ADR.

Messaging implementations must not hide indefinite retry loops inside provider adapters.

Retry policy that affects workflow progress belongs at the workflow/FSM boundary, not inside transport adapters.

### Consequences

Milestone 5 Messaging Abstraction must be designed after this FSM boundary is accepted.

The messaging ADR may decide whether timeout/no-message outcomes are represented through return objects, exceptions, result enums, callbacks, or another explicit contract.

This ADR only requires that the semantics are bounded and visible to workflow/FSM policy.

## Decision 7: Preserve Checkpoint Recovery Semantics

The current checkpoint recovery rule remains architecturally important:

- non-waiting states may be checkpointed;
- waiting states must not be persisted as restart targets.

This preserves the existing recovery intent: after interruption, the Pi4 client resumes from the last checkpointed non-waiting state rather than restarting inside a receive wait whose request or response may no longer be valid.

The FSM boundary shall own checkpoint eligibility policy.

Snapshot persistence shall own serialization and file I/O only.

## Deferred Decisions

- Exact FSM state and event naming.
- Representation of action outcomes.
- Messaging timeout semantics.
- Messaging retry semantics.
- Workflow retry semantics.
- State persistence representation, including numeric IDs versus symbolic state names.
- Preservation or removal of the final-alarm legacy quirk.
- Exact checkpoint implementation mechanism.

## Consequences

### Positive

- FSM behavior becomes independently testable.
- Workflow business actions become clearer.
- Messaging abstraction can avoid owning business retry loops.
- Runtime composition remains separate from transition policy.
- Checkpoint policy becomes explicit instead of incidental.
- The project can use a recognised FSM library rather than expanding custom transition machinery.

### Negative

- The project drops Python 3.8 compatibility.
- Tooling, CI, packaging metadata, and documentation need coordinated runtime-version updates.
- FSM migration is larger than a wrapper refactor.
- Existing tests around legacy quirks may need to be rewritten around intended state-machine behavior.
- Messaging abstraction work depends on the adoption of the FSM boundary defined by this ADR.

## Rejected Alternatives

| Alternative | Reason Rejected |
|------------|-----------------|
| Keep Python 3.8 support | Blocks current `python-statemachine` adoption and preserves an outdated compatibility target for a portfolio project. |
| Keep the custom integer-state FSM | Continues project-specific FSM mechanics when a maintained library is available. |
| Introduce a custom FSM abstraction first and evaluate libraries later | Adds project-specific infrastructure without a demonstrated need and delays adoption of a maintained community solution. |
| Wrap the current enum/table design | Preserves legacy structure instead of isolating FSM policy cleanly. |
| Implement Messaging Abstraction before FSM isolation | Risks baking workflow retry and waiting-state behavior into the messaging contract prematurely. |
| Put transport side effects inside FSM callbacks | Couples transition policy to transport mechanics and makes the FSM harder to test. |
| Let messaging own indefinite receive retries | Hides workflow progress decisions inside transport adapters and prevents explicit FSM timeout/retry behavior. |
| Persist waiting states as restart targets | Reintroduces the recovery risk already avoided by the current checkpoint policy. |

## References

- [ADR-0001: Repository Modernisation](0001-repo-modernisation-design.md)
- [ADR-0002: Model Adoption Principles](0002-model-adoption-principles.md)
- [ADR-0003: Device And Transformation Layer Boundaries](0003-device-and-transformation-layer-boundaries.md)
- [Messaging Model](../specifications/messaging-model.md)
- [Reliability Model](../specifications/reliability-model.md)
- [Data Pipeline](../data-pipeline.md)
- [`python-statemachine` on PyPI](https://pypi.org/project/python-statemachine/)
