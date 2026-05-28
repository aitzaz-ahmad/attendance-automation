# Future Work

This roadmap describes likely post-MVP evolution paths for the attendance ETL system. It is directional, not
a committed delivery plan. Current implementation focus remains ETL correctness, maintainability, and clear
repository boundaries.

For current architecture, pipeline sequencing, reliability behavior, and event shape, see
[Architecture](architecture.md), [Data Pipeline](data-pipeline.md), [Reliability Model](reliability.md), and
[Canonical Attendance Event](contracts/canonical-attendance-event.md).

## Near-Term

- Adopt the canonical attendance event contract end to end across transformation, publication, backend
  processing, and persistence boundaries.
- Isolate the transformation layer so device-specific decoding and canonical event construction are explicit
  and testable.
- Add focused ingestion and function tests around current ZKTeco extraction, Pub/Sub message handling, and
  Google Sheets update behavior.
- Introduce a small device abstraction around biometric attendance extraction while preserving the existing
  ZKTeco path.
- Improve validation and runtime schema checks at publication and backend processing boundaries.
- Separate reliability and finite state machine behavior from the ingestion client into dedicated modules.
- Improve dependency isolation for Cloud Function deployment wrappers and shared package code.
- Improve local development and validation ergonomics without changing production behavior.

## Mid-Term

- Evaluate PostgreSQL as a future persistence backend for attendance events and review state. PostgreSQL is
  not currently implemented and could either complement or replace parts of the Google Sheets-backed review
  persistence after the storage contract is better understood.
- Add a storage abstraction so Google Sheets behavior and any future database-backed persistence can share
  clearer interfaces and validation expectations.
- Introduce an API boundary for querying attendance events, review state, and administrative or reporting
  workflows. This API layer is not currently implemented and should remain high level until concrete caller
  needs are validated.
- Extend ingestion through an abstraction that can support additional biometric vendors without forcing a
  rewrite of canonicalisation, publication, or backend processing.
- Mature reporting and analytics capabilities around aggregated attendance reporting, historical summaries,
  operational reporting, and trend analysis using the canonical event model.
- Replace or complement the Google Sheets review surface where a more structured review workflow proves
  necessary.
- Enforce contract and runtime validation consistently across ingestion, publication, function processing, and
  persistence.

## Long-Term

- Provide historical attendance analytics built from stored canonical events and review outcomes.
- Add operational dashboards for ingestion health, pending review state, and storage outcomes.
- Build event replay or recovery tooling for reprocessing attendance events after operational failures.
- Support richer reporting exports for HR review and audit workflows.
- Expand multi-site ingestion support when multiple devices or locations become a validated requirement.
- Define storage partitioning, retention, and archival strategies after the expected data volume and review
  history requirements are known.

## Non-Goals / Deferred Ideas

- The current roadmap does not redesign the system into a distributed platform or commit to a production
  migration plan.
- PostgreSQL, an API layer, and a reporting and analytics layer are future directions, not currently implemented
  components.
- Production hardening and architecture evolution should follow validated operational needs, not speculative
  scale assumptions.
- Current work should continue to prioritize ETL correctness, deterministic behavior, maintainable module
  boundaries, and explicit contracts.
