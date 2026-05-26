# Contracts

This directory is reserved for repository-level behavioural and data contracts.

Contracts define semantics that implementation agents must preserve unless a
scoped issue explicitly changes them.

## Contract Areas

Future contract documents may cover:

- metadata semantics
- canonical event schema rules
- transformation contracts
- validation behaviour
- scheduling semantics
- sync/reporting semantics

## Usage

Agents should load contract documents only when the current task depends on
the relevant contract.

Do not infer contracts from unrelated files. If a contract is missing or
ambiguous, report the ambiguity instead of inventing behaviour.

## Scope

Contracts should be precise, authoritative, and implementation-relevant.

Avoid using this directory for broad design notes or speculative future plans.
