# Deployment And Provisioning

## Metadata

| Field | Value |
| --- | --- |
| Status | Accepted |
| Lifecycle | Active |
| Owner | Deployment and provisioning requirements for Attendance Automation v2 |
| Related ADRs | [ADR-0004](../decisions/0004-fsm-isolation-and-execution-boundaries.md), [ADR-0005](../decisions/0005-messaging-abstraction-and-routing-boundary.md), [ADR-0006](../decisions/0006-reliability-and-recovery.md) |
| Related specifications | [Production Readiness](production-readiness.md), [Operational Architecture](operational-architecture.md), [Runtime Bootstrap](runtime-bootstrap.md), [Reliability Model](reliability-model.md), [Messaging Model](messaging-model.md), [Biometric Device Configuration](biometric-device-configuration.md) |
| Related contracts | [Canonical Attendance Event](../contracts/canonical-attendance-event.md), [Backend Response Payloads](../contracts/backend-response-payloads.md) |
| Related proposals, if any | None for this canonical owner |
| Related audits | [Architecture Artefact Inventory](../audits/architecture-artefact-inventory.md), [Architecture Readiness Audit](../audits/architecture-readiness-audit.md), [Repository Gap Analysis](../audits/repository-gap-analysis.md) |

## Purpose

This specification defines the canonical deployment and provisioning
requirements for Attendance Automation v2.

It establishes what must be provisioned before the v2 runtime can operate
without requiring a fully automated deployment pipeline, Terraform, commercial
release management, or new implementation scripts.

## Scope

This specification covers:

- runtime prerequisites;
- infrastructure prerequisites;
- external service prerequisites;
- backend Cloud Function deployment prerequisites;
- Google Pub/Sub topic and subscription provisioning expectations;
- Google Sheets and Google Drive prerequisites;
- credential provisioning expectations;
- required runtime configuration files;
- required local runtime files;
- manual deployment assumptions;
- validation before deployment;
- v2 mandatory requirements; and
- v3/future deployment boundaries.

This specification does not cover:

- fully automated deployment pipelines;
- Terraform or infrastructure-as-code;
- commercial production release management;
- implementation scripts unless already present;
- runtime bootstrap ordering owned by [Runtime Bootstrap](runtime-bootstrap.md);
- transport mechanics owned by the [Messaging Model](messaging-model.md); or
- payload fields owned by contracts.

## Ownership

This specification owns deployment and provisioning requirements. It does not
own runtime topology, payload contracts, retry policy, or implementation
strategy.

Adjacent ownership remains:

| Concern | Canonical owner |
| --- | --- |
| Runtime topology and operational roles | [Operational Architecture](operational-architecture.md) |
| Pi4 bootstrap order and seed-file behaviour | [Runtime Bootstrap](runtime-bootstrap.md) |
| Repository validation commands | [Validation Gates](../workflows/validation-gates.md) |
| Messaging topics, routing, ACK policy, and timeout behaviour | [Messaging Model](messaging-model.md) |
| Restart, recovery, retry, and idempotency | [Reliability Model](reliability-model.md) |
| Biometric device configuration shape | [Biometric Device Configuration](biometric-device-configuration.md) |
| Attendance event payloads | [Canonical Attendance Event](../contracts/canonical-attendance-event.md) |
| Backend response payloads | [Backend Response Payloads](../contracts/backend-response-payloads.md) |

Deployment commands, packaging mechanics, service-account creation steps,
Google Cloud project policy, and environment-specific naming are
implementation-defined unless already owned by an accepted repository document.

## v2 Requirements

V2 deployment may be manual, but it must be repeatable. A competent
developer/operator must be able to identify the required runtime inputs,
provision the external resources, deploy the current runtime surfaces, and run
repository validation before claiming deployment readiness.

### Runtime Prerequisites

Runtime prerequisites apply to the commissioned ingestion runtime. The current
v2 runtime target is the Raspberry Pi / Pi4 deployment, but the prerequisite
category is intentionally runtime-oriented so that later runtime targets can
reuse the structure without changing current Pi4 requirements.

The current Pi4 runtime requires:

- a supported Python runtime and installed project dependencies suitable for
  the current repository baseline;
- access to the commissioned biometric device network;
- a valid `biometric_device_config.json` file;
- a valid `review_period.json` seed file;
- a valid `snapshot.json` seed or recovery file;
- Google Pub/Sub credentials or environment access sufficient for the Pi4
  runtime to publish and receive v2 workflow messages;
- local filesystem permissions to read required runtime files and write runtime
  state; and
- operator knowledge of the configured `site_id` as the commissioned runtime
  identity.

### Infrastructure Prerequisites

Infrastructure prerequisites cover backend runtime surfaces and broker
resources required by the current v2 architecture.

Backend Cloud Function prerequisites are:

- deployable wrappers under `src/backend/*/main.py`;
- backend dependencies available per function according to the existing
  function-specific dependency layout;
- Google Cloud project access for deploying the function wrappers;
- Pub/Sub trigger configuration for the request topics consumed by each
  backend function;
- service-account access for Google Sheets and Google Drive operations; and
- runtime configuration compatible with the current backend module constants or
  later accepted configuration owners.

Google Pub/Sub provisioning expectations are:

- request topics and response topics must exist or be created before runtime
  use;
- topic names and routing policy must remain compatible with the
  [Messaging Model](messaging-model.md);
- Pi4 inbound subscriptions must exist or be created according to the
  messaging adapter contract;
- targeted last-stored timestamp responses must preserve the documented
  routing model; and
- provisioning must not introduce alternate topic names without a contract
  migration.

### External Services

Google Sheets and Google Drive prerequisites are:

- the review-period source sheet must be accessible to backend code;
- attendance review sheets must be creatable or openable by backend code;
- service-account credentials must have the required Drive and Sheets access;
- current Google Sheets review output remains the v2 persistence and review
  surface; and
- PostgreSQL resources are not required for v2 deployment.

### Credentials

Credential provisioning expectations are:

- credentials must be provided out of band through the deployment environment
  or required local credential files;
- credentials must not be embedded into repository documentation as secret
  values;
- missing or invalid credentials are deployment/startup failures, not hidden
  fallback behaviour;
- exact credential-creation steps are implementation-defined unless a workflow
  owner later documents them; and
- centralised secrets management is future scope.

### Runtime Files

Required runtime configuration files are:

- `biometric_device_config.json`, owned by
  [Biometric Device Configuration](biometric-device-configuration.md);
- `review_period.json`, whose bootstrap behaviour is owned by
  [Runtime Bootstrap](runtime-bootstrap.md); and
- `snapshot.json`, whose checkpoint/recovery interaction is owned by
  [Runtime Bootstrap](runtime-bootstrap.md) and the
  [Reliability Model](reliability-model.md).

Required local runtime files are deployment inputs for the Pi4 runtime. They
may be manually provisioned for v2. Missing, malformed, or corrupt required
files must fail explicitly unless an accepted owner defines a generated-file
path.

### Manual Deployment Assumptions

Manual deployment assumptions for v2 are:

- deployment may rely on manual Google Cloud, Pub/Sub, Sheets, Drive, and Pi4
  setup;
- manual steps must preserve accepted topic names, payload contracts, runtime
  files, and reliability boundaries;
- backend functions may continue direct Google Pub/Sub publication where
  accepted by the [Messaging Model](messaging-model.md); and
- release promotion and rollback automation are not v2 requirements.

### Validation

Validation before deployment must include:

- repository validation through `./scripts/validate.sh`;
- `git diff --check` when local changes exist;
- local Markdown link validation for documentation changes;
- confirmation that required runtime files and credentials are present in the
  target environment;
- confirmation that Pub/Sub topics/subscriptions match the messaging contract;
- confirmation that backend functions can access required Google Sheets and
  Drive resources; and
- confirmation that the Pi4 runtime can reach the configured biometric device.

## v3 / Future Boundary

The following concerns are outside v2 deployment and provisioning unless a
later accepted repository authority promotes them:

- fully automated deployment pipelines;
- Terraform or other infrastructure-as-code;
- automated environment promotion;
- blue/green deployment;
- commercial rollback guarantees;
- centralised production secrets platforms;
- enterprise monitoring dashboards;
- commercial support runbooks;
- PostgreSQL-first deployment;
- automated credential rotation; and
- backend publisher migration to the project-owned `Messenger`.

Where deployment mechanics are not specified by existing repository owners,
they are implementation-defined and belong to future proposal or implementation
work.

## Non-Goals

This specification does not:

- redesign the runtime architecture;
- create implementation scripts;
- require Terraform;
- require a CI/CD release pipeline;
- define Google Cloud console steps;
- define credential secret values;
- define payload schemas;
- define transport internals;
- define storage abstraction interfaces; or
- update README files, diagrams, GitHub issues, or milestones.

## Validation

Changes to this specification must validate that:

- v2 deployment remains manual but repeatable;
- required files, credentials, Pub/Sub resources, backend functions, and Google
  Sheets/Drive prerequisites are documented without exposing secrets;
- future automation is not promoted into v2 scope;
- local Markdown links resolve; and
- repository validation follows [Validation Gates](../workflows/validation-gates.md).
