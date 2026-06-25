# Documentation Governance

## Status

Accepted

## Lifecycle

Active

## Purpose And Scope

This document is the canonical owner of documentation governance for the
repository. It defines how contributors and AI agents decide where information
belongs, how documents evolve, and how documentation audits should distinguish
active authority from historical context.

This guide governs documentation structure and ownership. It does not restate
the contents of architecture decision records, specifications, contracts,
proposals, architecture documents, workflow documents, or archived material.
Those documents remain authoritative for their own subject matter.

## Documentation Ownership Model

Repository documentation follows a single-owner model:

| Documentation Type | Owns |
| --- | --- |
| Architecture decision records | Why a material architectural decision was made |
| Specifications | What behaviour and technical contracts are required |
| Contracts | Data shapes, schemas, and repository-level data contracts |
| Proposals | How a significant change is intended to be implemented |
| Architecture | Where responsibilities live in the system topology |
| Workflows | How contributors develop, validate, and maintain the repository |
| Archive | Superseded or non-active material retained for history |
| Governance | How documentation ownership, lifecycle, and audits work |

When a topic fits more than one document type, choose the document that owns
the information being added. Other documents should cross-reference that owner
instead of repeating its content.

## Normative And Informative Documents

Repository documents are either normative or informative. Normative documents
define repository authority. Informative documents explain, plan, visualise,
navigate, or preserve historical context.

| Documentation Type | Classification |
| --- | --- |
| ADR | Normative |
| Specification | Normative |
| Contract | Normative |
| Governance | Normative |
| Proposal | Informative |
| Architecture | Informative |
| Workflow | Informative |
| Archive | Historical |

When documentation appears to conflict, first determine whether both documents
are intended to be authoritative. Informative or historical documents should
normally reference the appropriate normative owner rather than redefine it.

## Canonical Ownership Rules

- Each durable concept should have one active canonical owner.
- Documents outside the owner should summarise only enough context to orient
  readers, then link to the owner.
- A document may record historical implementation intent even when the active
  contract has moved elsewhere, but it must label that relationship clearly.
- Do not update a proposal to silently match later implementation if the
  proposal is meant to preserve the plan that was reviewed.
- Do not place payload schemas, data fields, or serialisation rules in ADRs,
  proposals, or architecture documents when a contract owns them.
- Do not place retry, recovery, or behavioural guarantees in proposals when a
  specification owns the active behaviour.
- Do not duplicate workflow commands outside workflow documents unless the
  duplicate is only a pointer to the workflow owner.

## Cross-Reference Policy

Cross-references preserve traceability between related documents without
creating competing authority.

Use cross-references when:

- an ADR explains the reason behind a specification;
- a specification depends on a contract;
- a proposal records the implementation strategy for an accepted decision;
- an architecture document places behaviour defined by a specification;
- an archived document has a current replacement.

Cross-references should name the target document and its ownership role. They
should avoid copying tables, schemas, command lists, or detailed behavioural
rules from the target owner.

## Documentation Lifecycle

Documentation evolves by adding traceability, not by erasing history. Earlier
documents remain part of the engineering record. Later evolution is recorded
through new ADRs, specifications, proposals, contracts, and appendices.
Documents should not be silently rewritten to hide previous design decisions
or implementation history.

Documentation changes should follow this pattern:

1. A need is identified through an issue, review, audit, or implementation
   change.
2. The current owner is identified using the ownership model.
3. The owner is updated, or a new owner is created if no suitable owner exists.
4. Related documents are updated with references, status notes, or archive
   pointers when needed.
5. Validation is run according to the repository workflow.

Lifecycle changes should preserve review history. If a historical document no
longer represents active guidance, mark its status and cross-reference the
current owner instead of rewriting it into a different kind of document.

## Status And Lifecycle Taxonomy

Documentation classification uses two separate fields:

- `Status` records approval state.
- `Lifecycle` records implementation, navigation, or historical state where
  that state applies to the document type.

Do not combine approval and lifecycle values into artificial compound statuses.

## Status Taxonomy

Use these statuses consistently when classifying document approval:

| Status | Meaning |
| --- | --- |
| Draft | Under development; not yet authoritative. |
| Accepted | Approved and authoritative for its document type. |
| Superseded | Replaced by a newer document while retained for traceability. |
| Archived | Removed from active navigation and retained only for historical reference. |

Valid statuses depend on document type:

| Document Type | Valid Statuses |
| --- | --- |
| ADR | Draft, Accepted, Superseded, Archived |
| Specification | Draft, Accepted, Superseded, Archived |
| Contract | Draft, Accepted, Superseded, Archived |
| Proposal | Draft, Accepted, Superseded, Archived |
| Architecture | Draft, Accepted, Superseded, Archived |
| Workflow | Draft, Accepted, Superseded, Archived |
| Archive | Archived |
| Governance | Draft, Accepted, Superseded, Archived |

## Lifecycle Taxonomy

Use lifecycle only where implementation, navigation, or historical state needs
to be represented separately from approval status.

| Lifecycle | Meaning |
| --- | --- |
| Planned | Approved direction that is not yet the current implemented baseline. |
| Active | Current implementation strategy, behavioural contract, data contract, governance guide, workflow, or architecture guidance. |
| Implemented | Proposal or specification has been implemented and remains aligned with the current baseline. |
| Implemented / Evolved | Proposal has been implemented and subsequently evolved. The proposal remains the implementation record. |
| Historical | Archived material retained for traceability rather than active authority. |

Valid lifecycle values depend on document type:

| Document Type | Valid Lifecycle Values |
| --- | --- |
| ADR | None required |
| Specification | Planned, Active, Implemented |
| Contract | Active |
| Proposal | Planned, Active, Implemented, Implemented / Evolved |
| Architecture | Active |
| Workflow | Active |
| Archive | Historical |
| Governance | Active |

Document authority depends on document type, status, and lifecycle:

- Accepted ADRs record architectural decisions and rationale.
- Accepted specifications with lifecycle `Active` define current behavioural
  and technical contracts.
- Accepted contracts with lifecycle `Active` define canonical data.
- Accepted proposals describe implementation strategy rather than behavioural
  authority; their lifecycle explains whether the strategy is planned, active,
  implemented, or implemented and evolved.
- Proposals with lifecycle `Implemented / Evolved` remain implementation
  records rather than current behavioural specifications.

A document may be both useful and non-authoritative. Audits must classify that
relationship instead of treating age or implementation drift as an automatic
defect.

## Document Precedence

When two active documents appear to disagree, resolve the conflict using the
following precedence order.

| Precedence | Documentation Type |
| ---: | --- |
| 1 | Accepted Contracts with lifecycle `Active` |
| 2 | Accepted Specifications with lifecycle `Active` |
| 3 | Governance with lifecycle `Active` |
| 4 | Accepted ADRs |
| 5 | Accepted Proposals with lifecycle `Active` |
| 6 | Architecture with lifecycle `Active` |
| 7 | Workflows with lifecycle `Active` |
| 8 | README and navigation documents |
| 9 | Archived material |

Document precedence is only used to resolve apparent conflicts. It does not
replace the canonical ownership model, which determines where new information
should be added.

## Proposal Evolution

Proposals own implementation strategy and migration intent. After
implementation, a proposal remains a record of the reviewed plan.

When implementation finishes:

- keep the proposal unless there is an explicit reason to archive it;
- mark its lifecycle if it is not already clear;
- add references to current ADRs, specifications, contracts, or architecture
  documents when those documents own the active design;
- preserve migration history and risk notes that remain useful context.

When later evolution changes the active design, do not rewrite the proposal as
though the later design was the original plan. Add a status note, appendix, or
cross-reference to the newer owner.

Proposals with lifecycle `Implemented / Evolved` shall preserve the originally
reviewed implementation strategy. Subsequent implementation changes should be
recorded through appendices, status notes, and cross-references to newer ADRs,
specifications, contracts, or proposals rather than silently rewriting the
original proposal.

## Specification Evolution

Specifications own active behavioural and technical contracts. They should
describe required behaviour, boundaries, guarantees, and validation
expectations without repeating data schemas or implementation plans owned
elsewhere.

When a specification changes:

- update the specification as the active behavioural owner;
- reference any ADR that explains the decision;
- reference contracts for canonical data shapes;
- reference proposals only for implementation history or migration strategy;
- update architecture documents only when responsibility placement changes.

## ADR Evolution

ADRs own architectural rationale and decision history. They should stay concise
and explain why the repository chose a direction, including relevant context
and consequences.

When later decisions refine or replace an ADR:

- create a new ADR when the rationale materially changes;
- add cross-references between the older and newer ADRs;
- mark superseded or refined status where appropriate;
- leave detailed behavioural contracts to specifications and data contracts to
  contract documents.

ADRs should not become implementation plans, schemas, or validation runbooks.

## Archive Policy

The archive preserves superseded or non-active material that still has
historical value.

Archive a document when:

- it is no longer active guidance;
- it has been replaced by a clearer canonical owner;
- keeping it in the active tree would confuse readers;
- its historical context remains useful for traceability.

Archived documents should state that they are archived and point to the current
canonical replacement when one exists. Do not rely on archive content as active
authority unless an active document explicitly says to do so.

## Documentation Audit Rules

Documentation audits must evaluate documents against their status, lifecycle,
and ownership role.

Audits should:

- evaluate documents according to status and lifecycle;
- identify the canonical owner before reporting duplication;
- distinguish historical records from active guidance;
- recommend references instead of copied content;
- preserve traceability between ADRs, specifications, contracts, proposals,
  architecture, workflows, archive, and implementation;
- report ambiguity when no clear owner exists;
- avoid treating proposals with lifecycle `Implemented` or
  `Implemented / Evolved` as
  documentation drift solely because the implementation has changed.
- avoid proposing new governance rules, lifecycle states, or repository
  conventions unless supported by repository evidence or explicitly requested
  by contributors.

Audits should classify findings by ownership impact: conflicting authority,
missing owner, stale active guidance, unclear status or lifecycle, broken
reference, or acceptable historical record.

## AI-Agent Auditing Guidance

AI agents must load context incrementally and stay issue-scoped. Start with the
assigned issue, `AGENTS.md`, and directly referenced files. Expand to this
governance guide when documentation ownership, lifecycle, audit classification,
or cross-reference policy matters.

When auditing documentation, agents should:

- state the scope being audited;
- list the authoritative documents used for comparison;
- separate active documentation from historical records;
- avoid broad repository reads unless uncertainty requires them;
- avoid GitHub mutation unless explicitly requested;
- report validation and acceptance status explicitly.

Before reporting documentation drift, determine whether the document is
intended to be normative, informative, historical, or archived. Only active
normative documents should normally be treated as behavioural or contractual
authority.

## Repository Navigation Philosophy

Navigation should help readers find the owner, not encourage every document to
repeat the same content.

Indexes should:

- point readers to canonical owners;
- show lifecycle where that prevents confusion;
- keep historical records discoverable without presenting them as current
  authority;
- avoid long summaries that duplicate the target documents.

The documentation tree should remain navigable by role: decisions, behaviour,
data, implementation strategy, topology, workflow, archive, and governance.

## Context-Loading Philosophy

Contributors and agents should avoid loading all documentation before every
task. Documentation context should be gathered in the smallest sufficient set:

1. Load the task, issue, or directly referenced file.
2. Load `AGENTS.md` for operating rules.
3. Load this governance guide when documentation ownership or lifecycle matters.
4. Load the relevant canonical owner for the topic being changed.
5. Load related documents only when cross-reference or ambiguity checks require
   them.

This keeps work deterministic, reduces unrelated architectural drift, and
prevents obsolete documents from overriding active owners.

## Documentation Validation Expectations

Documentation changes must follow the repository validation workflow. The
validation workflow remains the canonical owner of required validation
commands.

For documentation changes:

- run the required validation commands from the validation workflow;
- run `git diff --check`;
- validate internal Markdown links when documentation links are added or
  changed;
- report any validation failure explicitly;
- include an acceptance-criteria audit before reporting completion.

Do not claim documentation completion when required validation has not been run
or has failed.
