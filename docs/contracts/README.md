# Contracts

This directory contains repository-level behavioural and data contracts, including payload contracts shared
across architecture, pipeline, and implementation work.

Contracts define semantics that implementation agents must preserve unless a
scoped issue explicitly changes them.

## Contract Documents

- [Canonical attendance event](canonical-attendance-event.md): target internal payload shape for one
  normalised attendance event extracted from a source biometric device.

## Contract Areas

Contract documents may cover:

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
