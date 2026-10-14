# Igor: Integrated General-purpose Orchestrator for Research

## 1. Vision

Igor is a resource-aware experiment runner and recovery orchestrator for Linux.
It executes arbitrary commands, coordinates the compute resources of one
machine, persists execution state, extracts structured results, reports them in
Markdown, and can request diagnosis or code repair from external coding agents.

Igor is independent of programming language and scientific framework. Python,
Julia, R, Rust, Java, native executables, and containers are all represented as
commands with an argument vector, a working directory, and an execution
contract.

The initial deployment target is a trusted single-user Linux workstation using
`systemd --user`. Multi-user and distributed scheduling are explicitly outside
the first version.

## 2. Design Principles

- SQLite is the operational source of truth.
- A job describes intent; an attempt records one concrete execution.
- Every attempt has an immutable execution specification.
- Experiment families define which runs must remain scientifically comparable.
- Process execution, result processing, notifications, reports, and recovery
  are independent persistent actions.
- The worker never waits for Telegram, report generation, or a coding agent.
- Commands are stored as a program and argument vector. A shell is opt-in.
- Experiments receive all available resources by default; Igor coordinates
  exclusivity instead of imposing CPU or memory limits.
- There is no execution timeout by default. Manual cancellation and optional
  project-specific timeouts remain available.
- Large failed or obsolete artifacts are disposable. Their small operational
  record remains in SQLite.
- Generated reports are projections of structured results, not the source of
  truth.
- Coding-agent sessions are ephemeral by default and must not pollute the
  user's conversation history.

## 3. Initial Scope

### 3.1 Included

- One Rust binary named `igor`.
- CLI, worker, and supervisor modes.
- One SQLite database per user and machine.
- Direct-process and Docker executors.
- Arbitrary command execution for any language.
- Persistent jobs, attempts, events, actions, and deliveries.
- Priority scheduling and exclusive GPU allocation.
- Whole-host exclusive execution when resource requirements are unknown.
- Logs, cancellation, retries, leases, heartbeat, and restart recovery.
- Git revision capture and immutable configuration hashes.
- Experiment families, seeds, generations, and comparability rules.
- Telegram notifications through a persistent outbox.
- Project-specific metric extractors returning versioned JSON.
- Markdown report generation with deterministic or agent-assisted rendering.
- OpenCode and Pi integrations.
- Isolated Git worktrees for code repair.
- Automatic cleanup and storage inspection.
- Installation and management of `systemd --user` services.
- Human-readable and JSON CLI output.

### 3.2 Deferred

- Multi-user security and authorization.
- Scheduling across multiple machines.
- Automatic GPU sharing based on utilization.
- Kubernetes and cluster schedulers.
- Podman support.
- Dynamic Rust plugins.
- Web interface.
- Automatic resource estimation.
- Fully automatic promotion of repaired code to a project's main branch.
- Direct Obsidian integration.

## 4. Repository Layout

Start with a small Rust workspace and split further only when a boundary proves
useful.

```text
Igor-Jobqueue-experiment/
|-- Cargo.toml
|-- Cargo.lock
|-- rust-toolchain.toml
|-- README.md
|-- DESIGN.md
|-- LICENSE-MIT
|-- LICENSE-APACHE
|-- crates/
|   |-- igor-core/
|   |-- igor-daemon/
|   `-- igor-cli/
|-- config/
|   |-- example.toml
|   `-- report-result.schema.json
|-- packaging/
|   `-- systemd/
|-- docs/
|-- examples/
`-- tests/
```

Responsibilities:

- `igor-core`: domain model, state machine, SQLite migrations, scheduler,
  executors, resources, actions, retention, and integrations.
- `igor-daemon`: worker and supervisor loops plus the Unix-socket protocol.
- `igor-cli`: command parsing, presentation, service management, and daemon
  client.

The domain model must not depend on Clap or human-readable terminal output.

## 5. Runtime Architecture

```text
                         +----------------------+
                         |      igor CLI        |
                         +----------+-----------+
                                    |
                              Unix socket
                                    |
               +--------------------+--------------------+
               |                                         |
     +---------v----------+                    +----------v---------+
     |    igor worker     |                    |  igor supervisor   |
     | reserve resources  |                    | actions and outbox |
     | execute attempts   |                    | metrics and agents |
     | capture lifecycle  |                    | reports and cleanup|
     +---------+----------+                    +----------+---------+
               |                                          |
               +--------------------+---------------------+
                                    |
                         +----------v-----------+
                         |       SQLite         |
                         | jobs and attempts    |
                         | events and actions   |
                         | metrics and reports  |
                         +----------------------+
```

The worker and supervisor are separate long-running processes built from the
same binary. Both use transactional claims and leases. SQLite runs in WAL mode
with foreign keys enabled and numbered migrations.

Each daemon role owns a separate Unix socket under the shared runtime directory.
This lets both processes run independently without introducing a third broker;
the CLI contacts the role responsible for each request.

Long agent calls, network delivery, report generation, and artifact cleanup
must never block the worker from starting the next compatible experiment.

## 6. Linux And XDG Paths

User-mode defaults:

```text
$XDG_CONFIG_HOME/igor/config.toml
$XDG_STATE_HOME/igor/igor.sqlite3
$XDG_STATE_HOME/igor/logs/
$XDG_STATE_HOME/igor/worktrees/
$XDG_STATE_HOME/igor/reports/
$XDG_RUNTIME_DIR/igor/worker.sock
$XDG_RUNTIME_DIR/igor/supervisor.sock
$XDG_RUNTIME_DIR/igor/resources/
```

Fallbacks follow the XDG base-directory specification, normally:

```text
~/.config/igor/
~/.local/state/igor/
/run/user/$UID/igor/
```

Large project artifacts remain in project-configured locations rather than
being copied into the global Igor state directory.

Portable project configuration lives with the project and may be committed:

```text
PROJECT_ROOT/.igor/project.toml
PROJECT_ROOT/.igor/report-prompt.md
```

`project.toml` declares workload defaults and requirements. Machine-specific
capacity and credentials remain in `$XDG_CONFIG_HOME/igor/config.toml`; GPU
identities and other host details must not leak into portable project config.
When no project path is explicit, Igor searches from the current directory
toward its parents for `.igor/project.toml`.

## 7. Command Execution Contract

A direct command is represented structurally:

```toml
[execution]
program = "python"
args = ["-m", "experiments.runner", "--config", "configs/seed_1.json"]
cwd = "/home/user/projects/example"
executor = "process"
shell = false

[environment]
set = { OMP_NUM_THREADS = "8" }
remove = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
inherit = "minimal"
```

Julia is no different:

```toml
[execution]
program = "julia"
args = ["--project=.", "run.jl", "--config", "configs/seed_1.json"]
cwd = "/home/user/projects/example"
executor = "process"
```

Shell parsing is only enabled explicitly:

```bash
igor submit --shell 'make test | tee report.txt'
```

Each attempt stores a frozen copy of its effective command, arguments,
environment policy, working directory, Git revision, configuration hashes,
executor settings, resources, and result contract.

Submission creates the first pending attempt atomically with its job. This is
when the effective execution specification is frozen; the worker later starts
that existing attempt. Direct submissions always retain their argument vector.
Explicit shell submissions are frozen as `/bin/sh -c COMMAND` and marked as
shell execution.

## 8. Executors

### 8.1 Direct Process

- Launch in a dedicated process group or transient user service.
- Capture stdout and stderr without assuming UTF-8 at the process boundary.
- Persist PID plus process start identity to avoid PID-reuse errors.
- Cancel with `SIGTERM`, wait for a grace period, then use `SIGKILL`.
- Record exit code or terminating signal.
- Recover or classify an attempt as lost after daemon restart.

### 8.2 Docker

- Invoke the Docker CLI directly with argument vectors. Docker commands never
  pass through a shell, and v1 does not add a Docker client dependency.
- Use only images already present in the local daemon. Igor inspects the image
  before creation, never pulls implicitly, and verifies a configured canonical
  `sha256` digest by resolving the exact `image@digest` reference.
- Keep the host working directory separate from the container working directory.
  A Docker executor may declare an absolute `workdir`; otherwise the image
  default is retained.
- Require absolute bind-mount sources and targets, reject duplicate targets and
  traversal components, and preserve explicit read-only or read-write access.
  Igor is a trusted-user tool, so an explicitly declared writable source is the
  v1 authorization boundary; Docker is not treated as a sandbox.
- Preserve environment defaults declared by the image and add only the frozen
  command's explicit `environment.set` entries. Ambient host variables are never
  copied into Docker jobs, regardless of the direct-process inheritance policy.
- Select only the concrete GPU identities assigned by the scheduler instead of
  using an all-GPU shortcut.
- Name containers `igor-ATTEMPT_ID` and label them with `igor.project_id`,
  `igor.job_id`, `igor.attempt_id`, and optional `igor.generation_id`.
- Persist container ID, name, resolved image identity, and lifecycle state before
  declaring an attempt running. Recover by container ID, with exact Igor labels
  as a fallback that must produce one unambiguous match.
- Keep Docker optional. Capability checks run only for Docker work or explicit
  diagnostics; process execution must work when the CLI or daemon is absent.

Container identity is stored one-to-one with an attempt. The durable record
contains the Docker ID and deterministic name, resolved image reference and ID,
log paths, lifecycle state, last observed Docker status, outcome fields, and
created, started, finished, and removed timestamps. Its states are:

- `created`: `docker create` returned an ID and Igor persisted it, but successful
  start has not yet been recorded.
- `running`: Docker start succeeded and the attempt entered `running` in the same
  database transaction as the attempt-state event.
- `exited`: Docker reported a terminal status and its outcome is durable.
- `removed`: cleanup completed or the container was already absent.
- `lost`: the persisted identity can no longer be reconciled safely.

External Docker operations cannot share a transaction with SQLite. Igor handles
the boundaries deliberately: creation uses deterministic labels, then persists
the returned ID before start; persistence failure triggers best-effort removal;
and a crash after start but before the `running` transaction is recovered by
inspecting the record still marked `created`. Container identity transitions
require the live job claim and its complete M7 resource-lease set.

Cancellation and recovery preserve that ownership boundary. Cancellation polls
the durable request, runs `docker stop` against the remaining persisted grace
deadline, accepts a shorter repeated request, and escalates to `docker kill` only
after inspection still reports a live container. Logs and the authoritative
inspect result are persisted before disposable cleanup. Worker shutdown is not
cancellation: Docker CLI helper processes are dropped, the container is left
untouched, and the next owner reconciles it by durable ID.

Recovery never launches a replacement. A running container retains its original
complete resource leases while logs and wait supervision reattach. A terminal
container is finalized from inspection; a narrowly confirmed missing container
is classified as lost; daemon, permission, timeout, malformed-output, and
ambiguous-label failures preserve ownership for another reconciliation attempt.
Terminal containers whose first removal was interrupted are discovered and
removed idempotently without repeating attempt finalization.

### 8.3 systemd Transient Units

Where available, direct attempts should support transient user units such as:

```text
igor-job-123-attempt-2.service
```

This provides cgroup identity, reliable signalling, optional enforced limits,
and process survival independent of the Igor worker. A plain process-group
backend remains available for portability and testing.

## 9. Resources And Scheduling

### 9.1 Default Policy

Experiments are unrestricted by default:

- No `CPUQuota`.
- No `MemoryMax`.
- No wall-clock timeout.
- The active experiment can use all available CPU and RAM.
- GPU requests are exclusive by default.
- Jobs with unknown requirements reserve the whole compute host, preventing a
  second experiment from causing oversubscription during dataset preparation.

The supervisor may run lightweight work concurrently using low CPU priority
and idle I/O scheduling. This includes Telegram delivery, small metric
extraction, and report bookkeeping. Heavy analysis should declare resources
and be scheduled as a job or stage.

### 9.2 Optional Requests And Limits

Projects can later provide scheduling hints or enforced limits:

```toml
[resources]
mode = "exclusive-host"
gpu = "any"
gpu_count = 1
gpu_exclusive = true
cpu_threads = "unlimited"
memory = "unlimited"
timeout = "none"
```

Resource requests guide scheduling. Enforced cgroup limits are a separate,
explicit option. Igor must never silently convert a scheduling estimate into a
hard limit.

The global host configuration may cap the CPU threads and memory considered by
the scheduler, restrict enabled GPU identities, and limit concurrent jobs. The
worker defaults to one concurrent experiment to avoid accidental multi-job
oversubscription. These are admission controls, not hard process limits; strict
enforcement requires the later cgroup and `systemd` integration.

### 9.3 Scheduling Policy

- Effective priority first. While queued, a job gains one priority point for
  every complete hour since submission. Aging is uncapped and computed at claim
  time; it never changes the submitted priority stored with the job.
- Global submission order within equal effective priority.
- Resource fit.
- Atomic reservation of all requested resources.
- Host-global GPU and named-resource leases.

The scheduler considers candidates in that order and selects the first complete
resource request that currently fits. An older incompatible job therefore does
not block compatible work, while uncapped aging ensures an old job eventually
outranks newly submitted work with any fixed finite priority.

Future staged jobs may declare CPU preparation, GPU execution, CPU analysis,
and publication separately. Until then, one command holds its declared
resources for its full lifetime.

## 10. Jobs, Attempts, Families, And Generations

### 10.1 Jobs And Attempts

A job describes the intended experiment. An attempt records one execution:

```text
Job 123
|-- Attempt 1 -> failed
|-- Repair 1  -> validated code change
`-- Attempt 2 -> succeeded
```

Retries never erase attempts. Logs, errors, code revisions, and repair
relationships remain traceable even after large artifacts are removed.

### 10.2 Experiment Families

A family groups runs that must be compared under the same implementation and
scientific protocol:

```text
Family: gnn-frozen-unweighted
Generation 1, revision A
|-- seed 1 -> succeeded
|-- seed 2 -> succeeded
`-- seed 3 -> code failure

Generation 2, repaired revision B
|-- seed 1 -> queued again
|-- seed 2 -> queued again
|-- seed 3 -> queued again
|-- seed 4 -> queued again
`-- seed 5 -> queued again
```

A validated code correction can create a new family generation. All related
seeds are rerun so the aggregate never mixes code revisions. Successful results
from the previous generation become `superseded`, not comparable.

A transient infrastructure failure that does not modify code can be retried by
the operator with `igor retry JOB_ID` inside the same generation. The retry
transaction compares the job, prior attempt, frozen Git identity and generation
record (including protocol digest and required seed) before creating the next
attempt; it copies the previous source, configuration and result contract.
The prior attempt and all other members remain unchanged. While a retry is
queued or running the generation is incomplete, and it becomes comparable only
if every required member eventually succeeds. Automatic classification of
transient versus code failures belongs to M16.01–M16.02; this manual retry does
not authorize replacing code or silently creating a new generation.
Superseded jobs cannot be retried.

The versioned `family.toml` declares every required seed explicitly. Shared
`execution.program` and `execution.args` are a direct command vector; each
`[[members]]` entry appends seed-specific `args` and/or scientific configuration
paths, never a shell template. Shared and member configuration paths are
frozen with the attempt. A family file cannot omit required members, repeat a
seed, or mix direct and shell execution. The family generation freezes the
revision and protocol identity before any member becomes runnable.
Preparation reads the Git identity before and after freezing every member and
rejects a changed worktree. The protocol digest includes execution and
scientific inputs, project configuration, declared content hashes for every
seed, and the Git dirty-worktree digest when explicitly allowed. Scheduling
priority and display name do not change scientific comparability. Every job
and attempt carries the same frozen generation and source identity.

```toml
schema_version = 1
name = "gnn-frozen-unweighted"
scientific_configurations = ["configs/shared.toml"]

[execution]
program = "/usr/bin/python3"
args = ["train.py", "--frozen"]

[[members]]
seed = 1
args = ["--seed", "1"]
scientific_configurations = ["configs/seed1.toml"]

[[members]]
seed = 2
args = ["--seed", "2"]
scientific_configurations = ["configs/seed2.toml"]
```

`igor family show FAMILY_ID [--json]` reads the family, generation identities,
and current member states in one SQLite read transaction. It returns members
ordered by submission and generations ordered by number. State counts include
every job state, with zero-valued entries for absent states, both per
generation and across the family. These are current **job** counts, not the
number of attempts. In protocol v18, `family_show`, `family_supersede` and their
responses require matching CLI/daemon versions.

Each generation exposes a status assessed from the immutable required seed
list in its stored family specification and the **current job state** of every
member. `incomplete` means a required seed is missing or any member is queued
or running (including during a retry). `complete` means every required seed
has exactly one terminal job but at least one failed, was cancelled, was lost,
or was superseded. `comparable` means exactly one job per required seed,
all succeeded, and every member carries the same family ID and complete
generation identity (ID, number, Git revision, protocol digest). An extra or
duplicate seed or inconsistent membership is `invalid`, never comparable;
an unreadable frozen family specification or protocol-digest mismatch is an
integrity error rather than an assumed complete generation. A later successful
retry can move a generation from `complete` through `incomplete` to
`comparable`, without treating old attempts as additional seeds. `comparable`
qualifies results **within one generation**. The current generation is the
highest generation number in the family, exposed as `current_generation_id`.

The M11.10 read boundary for future metric/report aggregation is
`FamilyGenerationRepository::comparable_generation(family_id, generation_id)`.
It requires both IDs, reads only that generation and its jobs in one SQLite
snapshot, verifies the frozen protocol and exact seed membership, and returns
an immutable `ComparableGeneration` only when every member has succeeded.
Incomplete, failed, invalid, or mixed-ID/revision/protocol selections fail
closed; a successful member from another generation cannot fill a missing
seed. Historic generations remain readable but cannot be selected as
comparable after supersession. Future M12/M14 consumers must use this selection
rather than family-wide counts or `jobs.list`,
and revalidate before publishing if the state can change after selection.
Current aggregates must select `current_generation_id` and exclude superseded
historical results; their final publication remains part of M12/M14.

`igor family supersede FAMILY_ID --file family.toml` requires an existing
registered project and an unchanged scientific protocol digest, while freezing
a fresh Git source revision for all seeds. It rejects incomplete or invalid
old generations: queued jobs must be cancelled explicitly, and running jobs
must finish or be cancelled with the normal job command first. The repository
verifies the expected previous generation, project scope and consecutive
generation number, then atomically marks old terminal jobs `superseded`, appends
their state-change events and inserts every new job/attempt/event. Old attempt
states and history remain untouched. A stale or concurrent supersession cannot
publish a second successor; failure at any insertion rolls the whole change
back. This command does not apply an agent repair or implicitly cancel work;
the approval-controlled repair workflow remains in M16.
The SQLite transaction reserves a write on the family row before inspecting
the current generation so competing worker writes cannot invalidate a deferred
read snapshot before publication.

M11.13 exercises a five-seed family against a disposable Git repository and
real local worker. A held first seed demonstrates `incomplete` while the other
four remain queued; releasing the barrier produces one comparable generation.
A separate deterministic failure remains non-comparable through a retry, then
repaired code is committed and all five seeds are rerun in a successor. The
superseded attempts remain readable but cannot satisfy the new generation's
selection; changing a required seed instead of repairing code changes the
scientific protocol and is rejected. This validates the M11/A11 selection
boundary; A11.03 requires the actual current-generation aggregate from M12/M14.

### 10.3 Running Old Generations

When a repair invalidates a generation and one of its attempts is still
running, Igor asks for approval before cancellation. After approval it performs
a graceful cancellation and frees the GPU. It never terminates the run solely
because a coding agent proposed an unvalidated patch.

## 11. Git And Code Repair

Every attempt is pinned to a Git revision or immutable source snapshot. A
repair must not modify the shared project checkout used by other attempts.

Initial submission requires a Git worktree until immutable snapshots for
non-Git projects are implemented. Dirty worktrees are rejected unless the user
passes an explicit allow-dirty policy. An allowed dirty submission records the
revision, branch, repository identity, dirty state, and a digest of the observed
worktree changes; this is an explicit reproducibility waiver rather than a claim
that the clean revision alone identifies those bytes.

Repair flow:

1. Classify the failure.
2. Create an isolated Git worktree at the failed revision.
3. Invoke the configured coding agent in that worktree.
4. Ensure protected paths and scientific configuration files did not change.
5. Run project-configured validation commands.
6. Record the patch, changed paths, validation result, and candidate revision.
7. Request approval for cancellation of obsolete running attempts.
8. Create a new family generation containing every required seed.
9. Run all members against the same repaired revision.

Default propagation policy:

```toml
[recovery]
propagation = "family-generation"
approval = "manual"
max_repair_cycles = 1
timeout = "none"
protected_paths = ["configs/**", "splits/**", "data/**"]
```

The repair revision can live under an Igor-specific Git reference without
automatically changing the project's main branch. Promotion or cherry-picking
into the main branch remains an explicit user action.

## 12. Persistent Data Model

Igor presents this as automatic database schema updating. Internally, each
schema update remains a numbered SQL migration, following SQLx and database
tooling conventions.

The initial schema should include:

| Table | Responsibility |
|---|---|
| `_sqlx_migrations` | SQLx-managed versions and checksums for numbered migrations |
| `projects` | Project identity and configuration path |
| `families` | Comparable experiment families |
| `generations` | Revision and protocol shared by family members |
| `jobs` | Desired execution and retry policy |
| `attempts` | Concrete immutable executions |
| `events` | Append-only lifecycle history |
| `resources` | Machine capacities and named resources |
| `resource_leases` | Atomic reservations with heartbeat |
| `actions` | Extraction, reporting, recovery, cleanup tasks |
| `metrics` | Normalized scalar and structured results |
| `artifacts` | Published or removed artifact metadata |
| `deliveries` | Notification outbox and retries |
| `recoveries` | Diagnoses, patches, validations, and decisions |
| `report_runs` | Prompt, model, context, output, and cleanup state |
| `agent_sessions` | Ephemeral session IDs pending deletion |

Required SQLite behavior:

- `PRAGMA journal_mode=WAL`.
- `PRAGMA foreign_keys=ON` on every connection.
- Bounded busy timeout.
- Check constraints for finite state sets.
- Transactional state transition plus event/action insertion.
- Indexed scheduling and action-outbox queries.
- UTC timestamps.
- Consistent backup through the SQLite backup API.

SQLite may create `-wal` and `-shm` companion files. This remains one logical
database.

Project roots are registered canonically and idempotently. Submission order is
global to the database so priority ties have deterministic FIFO behavior across
projects. Project registration, job creation, the first pending attempt, and
their initial events use atomic transactions where applicable.

## 13. Actions And Supervisor

Attempt completion creates independent actions:

```text
attempt_succeeded
|-- extract_metrics
|-- publish_artifacts
|-- generate_report, when the family is complete
`-- send_notification

attempt_failed
|-- classify_failure
|-- diagnose_or_repair
|-- cleanup_failed_artifacts
`-- send_notification
```

Each action has its own status, lease, retry policy, and error. Repeating an
action must be idempotent. A failed Telegram delivery must not regenerate a
report, and a report failure must never rerun an experiment.

Delivery is at least once. External services may receive a duplicate if the
machine fails after delivery but before acknowledgement is persisted. Event and
idempotency keys should be included where supported.

## 14. Metric Extraction Contract

Igor does not assume that every project has accuracy, F1, epochs, or a test
split. A project-specific command translates artifacts into versioned JSON:

```toml
[results]
extractor = ["./scripts/export_igor_metrics.py"]
working_directory = "attempt"
```

`[results]` is optional in project configuration v1. Its extractor is a
non-empty direct program-and-arguments vector; no shell or argument rewriting
is implicit. Bare program names use `PATH`, absolute programs are explicit,
and a relative program containing `/` is resolved within the registered
project root before submission. The arguments remain literal; relative
argument paths are resolved from the future attempt working directory, so a
project script should normally be the executable itself with a shebang.
Submission freezes the resolved command in every attempt's result contract
with no inherited environment, and all members of one family must have the
same frozen extractor. On successful execution, finalizing an attempt also
enqueues a uniquely keyed `extract_metrics` action in the same SQLite
transaction. The supervisor processes extraction independently from Telegram
and worker scheduling: it runs the frozen direct command in the private attempt
log directory with a cleared environment (only `PATH` is provided). Standard
input is one JSON line with `schema_version`, project/job/attempt IDs, optional
family/generation/seed identity, and only the persisted `stdout.log` and
`stderr.log` paths for that attempt. These paths are checked against the
attempt directory before invocation; this context is not a process sandbox.
The extractor has no default timeout, heartbeats its fenced action lease, and
limits captured stdout to 1 MiB and stderr to 64 KiB. Shutdown terminates the
owned child, leaving the durable action to be reclaimed on restart.

The supervisor parses and validates result JSON against the v1 structural
contract. Schema errors and child failures retry the action without changing
the successful job/attempt outcome. After the third claim fails, only the
independent `extract_metrics` action becomes `failed`; the job and attempt
remain `succeeded`. End-to-end tests cover nonzero exit, malformed JSON and a
missing declared artifact at the terminal retry boundary. Migration `0010`
stores the original structured result in `metric_documents` keyed by attempt
ID. In one fenced transaction Igor stores the validated document, marks the
action succeeded, and appends its event; an existing different document cannot
be overwritten.
The completion transaction reserves the claimed action row before reading or
indexing results, avoiding a read-to-write upgrade against a stale WAL snapshot.
M12.07 indexes arbitrary top-level scalar names and finite values in `metrics`;
per-class structures remain in the original document. M12.08 records the
optional selection split, criterion, value, epoch counters and checkpoint
declaration in `metric_selections` (migration `0011`), retaining the original
selection JSON for exact replay comparisons. Both indexes publish in the same
fenced transaction as the original document and action outcome. A conflicting
existing scalar or selection rolls back the whole operation; equal values can
be replayed without duplicate rows. Selection checkpoint paths are only
declarations until M12.10–M12.11 stage and publish artifacts. M12.15 parses
extractor stdout with a recursive JSON visitor before v1 validation: duplicate
object keys, including escaped aliases at any depth, are rejected rather than
silently replaced by the last occurrence. Malformed JSON and non-finite number
tokens fail parsing; missing fields and unsupported schema versions fail v1
validation. These failures retry only `extract_metrics`, without publishing a
document or changing the successful experiment outcome.

M12.09 registers each declared artifact before completing its extraction
action. The declaration is an attempt-relative path and role (with optional
claimed SHA-256). Igor accepts only a regular file inside the attempt's
persisted log directory: parent symlinks and `..`/absolute paths are rejected,
the file is opened without following a final symlink, and size and SHA-256 are
computed by streaming its bytes. A claimed digest must match Igor's hash.
Registration is fenced by the active extractor claim, records an
`artifact_registered` event and is idempotent for the same path/role/bytes;
a changed file or role conflicts. The result document cannot complete its
action until all declared paths are registered. Migration `0012` adds
`size_bytes` and distinct `registered`, `staged`, `published`, `removed`
states while preserving legacy published rows with unknown size. Registration
does **not** mean publication. M12.10 copies each registered output to
`STATE_DIR/artifact-staging/ATTEMPT_ID/ARTIFACT_ID` before completing the
extractor action. The copy is created privately through a temporary file,
stream-verified against the registered size/digest, synced and renamed in its
own attempt directory before the artifact state becomes `staged`. A restart
between rename and state update adopts only a matching staged file; a changed
source, mismatched copy, symlinked staging root or expired action claim is
rejected without marking it published. `staged_path` is kept in artifact
metadata for recovery. M12.11 rechecks the complete staged set before reports
or retention rely on it.

M12.11 freezes the configured project `artifact_root` into the successful
attempt's result contract (protocol v10; absent on older attempts). The
supervisor copies staged bytes to a private sibling directory beneath this
root, verifies every declared artifact by size and digest, removes only
Igor-named partial copy temps, and atomically renames the **whole attempt
directory** to `ARTIFACT_ROOT/ATTEMPT_ID`. It then commits the original metric
document, indexes and all `published` artifact states/paths in one SQLite
transaction with the successful action event. A crash after the directory
rename but before SQLite commit leaves the completed files visible but their
database state `staged`; recovery adopts only a complete matching directory.
No partial directory appears at the final path. Only a document with
`status = "success"` publishes artifacts; partial/failed extractor documents
keep their outputs staged. Filesystem rename and SQLite commit cannot form one
kernel transaction: consumers use the durable database publication state, not
directory presence, as the publication authority.

Repeating a successful extraction rechecks staged and already published files
against their registered size, digest, frozen root and recorded paths. An
identical replay publishes no second document, scalar, selection or artifact;
a changed file or mismatched publication path remains a conflict. The action
claim still fences the repeated operation.

Telegram deliveries select up to ten indexed scalars for their own attempt at
send time. If extraction completes before the delivery is claimed, the
notification includes the available project-defined metrics before the job ID.
Extraction and delivery run independently, so an earlier delivery may omit
metrics; it does not wait for extraction.

M12.13 adds `igor metrics JOB_ID [--json]` via worker protocol v11. The
response lists every attempt separately, preserving each extraction action's
state and last error, original validated document, selection and uncapped
indexed scalar names/values. Each attempt's rows use one read transaction;
`null` action state means extraction was never queued, while `null` document
means no result was published yet. The response frame limit is 32 MiB (requests
remain limited to 1 MiB); oversized responses return a protocol error.

M12.14 exercises three source fixtures in disposable registered projects:
Python reads the declared `stdout_path`, a POSIX shell emits Julia-compatible
JSON for a checked sample log without requiring Julia, and a compiled Rust
binary reads `stdout.log` from the attempt directory. Each extracts
project-defined scalar names and declares an artifact; integration tests check
the indexed values, durable document and published file size/digest.

Example output:

```json
{
  "schema_version": 1,
  "status": "success",
  "metrics": {
    "accuracy": 0.867020,
    "kappa": 0.814708,
    "macro_f1": 0.793544
  },
  "selection": {
    "split": "validation",
    "metric": "macro_f1",
    "value": 0.790778,
    "epoch": 31,
    "trained_epochs": 41
  },
  "classes": {
    "Wake": {"f1": 0.924787},
    "N1": {"f1": 0.455164},
    "N2": {"f1": 0.878496},
    "N3": {"f1": 0.843338},
    "REM": {"f1": 0.865937}
  },
  "artifacts": [
    {"path": "metrics.json", "role": "metrics"}
  ]
}
```

The published Draft 2020-12 contract is
[`schemas/metric-result-v1.schema.json`](schemas/metric-result-v1.schema.json),
with a complete example in
[`config/example-metric-result-v1.json`](config/example-metric-result-v1.json).
`schema_version = 1` is required; unknown top-level fields and unsupported
versions are rejected. Metric and class names are project-defined, `selection`
and `artifacts` are optional, and no accuracy metric, split or checkpoint is
mandatory. `selection.value` may differ from the final metric value when the
selected checkpoint was evaluated at an earlier epoch. The result's `status`
describes extraction, independently of the job's execution outcome. Paths are
attempt-relative declarations: schema validation alone does not establish
filesystem containment, file existence or a trustworthy checksum. Igor
checks these properties before publication; a supplied digest never replaces
Igor's own file hash.

Igor stores the original document and indexes selected scalar values without
changing their scientific meaning.

## 15. Markdown Reports

Obsidian is not required by the first version. SQLite stores operational and
structured result data. Report generation is optional and disabled by default.
When explicitly enabled, Igor generates a project Markdown report after a
complete comparable family generation finishes. Execution does not require a
metric extractor, report generator, coding agent or notification provider.

Default configuration:

```toml
[report]
enabled = false
trigger = "family-completed"
output = "RESULTS.md"
mode = "replace-generated-file"
generator = "deterministic"
prompt_file = ".igor/report-prompt.md"
```

Enable reports with `enabled = true`. The deterministic generator requires no
coding agent; `opencode` and `pi` are explicit optional integrations. Existing
project files with an explicit enabled flag or generator retain those choices.

The report includes:

- Family and generation.
- Git revision and configuration hashes.
- Status and metrics for every seed.
- Aggregates supplied by the project extractor.
- Validation-based selection criteria.
- Per-class metrics where available.
- Failures, repairs, and repeated generations.
- Links to retained artifacts and logs.
- Igor, extractor, prompt, provider, and model versions.

Only the newest complete and comparable generation appears as the primary
result. Superseded generations may be summarized as history but are never mixed
into the aggregate.

An optional future `managed-section` mode can update a marked region in a
handwritten Markdown document. The safe default is a fully generated
`RESULTS.md`.

## 16. Agent-Assisted Report Generation

Each project may provide a versioned prompt describing the desired analysis:

```markdown
# .igor/report-prompt.md

Generate a concise scientific report for this experiment family.

- Compare the GNN against the configured baseline.
- Treat test metrics as descriptive.
- Never select a model using test data.
- Highlight macro-F1, kappa, and per-class behavior.
- Do not mix superseded generations.
- State clearly whether the result is provisional or final.
```

Igor prepares an isolated context directory:

```text
report-context/
|-- prompt.md
|-- family.json
|-- metrics.json
|-- attempts.json
|-- artifacts.json
`-- previous-report.md
```

The agent receives normalized context rather than unrestricted database access.
It returns Markdown to Igor; Igor validates and publishes it atomically. The
agent does not write SQLite or the final report directly.

`report_runs` records provider, model, prompt hash, context hash, output hash,
status, timestamps, and error. Report generation has no timeout by default but
can be cancelled or configured with a project-specific limit.

## 17. Ephemeral Agent Sessions

Agent sessions must not be confused with the user's interactive conversations.

### 17.1 Pi

Invoke Pi in print mode with no session persistence:

```bash
pi --no-session -p "..."
```

### 17.2 OpenCode

OpenCode currently creates a session for `opencode run`. Igor should use the
local OpenCode server API where practical:

1. Create a session titled `igor:<action>:<id>`.
2. Submit the prompt and capture structured output.
3. Delete it with `DELETE /session/:id` in a cleanup block.
4. Persist unfinished cleanup in `agent_sessions`.
5. Remove stale `igor:*` sessions after supervisor restart.

CLI-based fallback may parse the session ID from JSON output and run:

```bash
opencode session delete SESSION_ID
```

The session can exist temporarily while the agent runs. After a power loss it
may remain until the supervisor restarts and performs cleanup. Igor should not
claim stronger guarantees than this.

Default policy:

```toml
[agents]
session_policy = "ephemeral"
keep_session_on_failure = false
timeout = "none"
```

Igor keeps its own concise action log and result even when the external session
is deleted. Telegram credentials and unrelated secrets are never inherited by
agent processes.

## 18. Retention And Garbage Collection

Failed rows are useful history; failed files usually are not. Igor retains the
small database record while cleaning large outputs.

Lifecycle:

1. A running attempt writes to an isolated temporary directory.
2. On success, only declared artifacts are published.
3. On failure, full logs and temporary artifacts remain until diagnosis.
4. After diagnosis, failed checkpoints, partial outputs, repair worktrees, and
   full logs are removed by default.
5. A concise log tail, error, hashes, diagnosis, and repair relation remain in
   SQLite.
6. Superseded artifacts remain until the replacement generation succeeds, then
   are removed by default.

Default policy:

```toml
[retention]
failed_artifacts = "delete-after-diagnosis"
failed_full_logs = "delete-after-diagnosis"
failed_log_tail_kb = 512
compress_success_logs = true
delete_repair_worktrees = true
superseded_artifacts = "delete-after-replacement"
```

Project exceptions are explicit:

```toml
[artifacts]
keep_on_success = ["metrics.json", "model.weights", "figures/**"]
keep_on_failure = ["crash-dump.json"]
```

M13.01 reads optional `[retention]` and `[artifacts]` sections from the
version-one project configuration. Missing retention fields inherit the
defaults above, and writing those defaults explicitly preserves the same
serialized configuration identity as omitting them. `keep_on_success` and
`keep_on_failure` accept distinct, nonempty attempt-relative patterns such as
`figures/**`; absolute paths, dot/parent or empty components, and NUL bytes
are rejected at configuration load time. Later cleanup operations must also
check the actual filesystem boundary before removing any file.

M13.02 adds `artifacts.lifecycle_state` in migration `0013`, distinct from the
physical publication `state`. New registrations start `running`; successful
publication marks `published` in the same transaction as document/action
completion. Partial/failed extractor documents and terminal extraction failures
mark staged or registered outputs `failed` without deleting their bytes.
Migration backfills prior published/removed rows and classifies non-published
rows from attempt, metric-document and extraction-action outcomes. The schema
rejects unknown values and inconsistent physical/lifecycle pairs. `retained`
and `superseded` remain durable lifecycle labels for subsequent policy actions.

M13.03 adds migration `0014` and a durable `diagnosis_required` marker on
failed artifact rows. A diagnosis counts as complete only when its recovery
stores `diagnosis_json` and has left `pending`/`diagnosing`, with no other
unfinished recovery for the attempt. SQLite rejects removing or deleting a
marked artifact until that evidence exists; retaining it cannot clear the
marker or bypass the guard. The same read-only diagnosis check is available
for later cleanup of failed logs. This barrier does not itself remove files.

M13.04 freezes a nondefault `failed_log_tail_kb` per attempt (protocol v12;
older attempts retain the 512 KiB default). Migration `0015` stores bounded
raw-byte tails of both stdout and stderr for failed, lost or cancelled attempts
in SQLite, alongside the terminal transition. The configured limit is per
stream, supports zero, and is capped at 16384 KiB per stream. Tail capture
seeks from the end without loading full logs; failed reads retain any available
stream but mark the row `complete = false`. Cleanup must require a complete row
before removing full logs, as well as the durable diagnosis required above.

M13.05 freezes `failed_artifact_retention` into new attempt result contracts
(protocol v13): failed artifact policy and `keep_on_failure` glob patterns.
Legacy attempts without this snapshot are preserved. An independent supervisor
task discovers diagnosed failed artifacts in bounded pages and rechecks their
eligibility under SQLite's writer lock before removing any bytes. It excludes
active extraction and requires complete log tails for failed/lost/cancelled
attempts; unsuccessful extraction on a succeeded experiment leaves full logs
untouched. Policy exceptions mark artifact lifecycle `retained`.

Cleanup preflights the complete batch (up to 64 artifact rows) against the
persisted attempt log paths, private staging paths and registered size/digest.
It rejects traversal, symlinked components, mismatched bytes and log-stream
paths. Hidden or final publication copies defer cleanup to publication recovery.
Only individual source/staged files are removed, then parent directories are
synced before publishing `removed` tombstones and cleanup events. An interruption
after file removal but before database commit can be retried: absent verified
paths are accepted and existing copies are rechecked. Unregistered outputs,
full logs and directories are not removed in this step. The writer lock stays
owned by the blocking worker during filesystem work, including caller abort;
hashing uses bounded memory but lock duration depends on artifact size.

M13.06 adds migration `0016` and a cleanup consumer for explicitly registered
repair worktrees; creating them and running repair agents remain in M16. The
registration links a recovery to exactly `WORKTREE_DIR/RECOVERY_ID`, stores
Git common/admin paths and filesystem identities, and freezes the deletion
policy. It rejects symlinked, foreign or main checkouts. Explicit release stores
patch bytes and SHA-256 plus versioned diagnosis, validation, changed-path and
optional candidate records in SQLite. Release requires a terminal recovery
(`succeeded`, `failed`, `rejected`, `cancelled`) and no pending/running repair
action or active agent session. Those records cannot then be reactivated or
overwritten while the worktree is released.

A candidate revision must resolve to a commit and is preserved before release
as a direct `refs/igor/repairs/RECOVERY_ID` ref without replacing a different
existing ref. Cleanup revalidates this reference and the Git/filesystem identity
before targeted `git worktree remove --force`; it never broadly prunes worktrees.
A committed `cleaning` intent permits retry after removal and an event/database
rollback. Parent directories are synced before the removed tombstone and event
are committed. If Git leaves a partial removal whose identity cannot be verified,
cleanup remains deferred in `cleaning`. A false frozen deletion flag publishes
`retained` instead. Existing unregistered worktrees are never candidates.

M13.07 adds migration `0017`: physically published artifacts of superseded jobs
are marked lifecycle `superseded`, with retained/removed rows preserved. The
family supersession transaction performs the same marking; extraction completion
checks the owning job under its writer lock so late publication cannot lose the
superseded label. Identical published replay preserves superseded/retained state.

The replacement barrier is live, not a latched success: only the highest-number
current generation in the same family can qualify. A transaction-local domain
assessment requires its exact seed membership and frozen protocol to be
`Comparable`. SQL guards also reject removal or row deletion unless the current
generation has successful matching members, regardless of artifact lifecycle;
the domain helper additionally validates the protocol digest before physical
cleanup. Failed-artifact cleanup invokes this helper under its writer lock before
any file operation for superseded jobs. G2 success followed by pending/failed G3
therefore preserves G1 artifacts again. M13.07 does not itself remove superseded
published files; policy-driven retirement follows in M13.08.

M13.08 freezes `superseded_artifact_retention` in new attempts (protocol v14),
including `superseded_artifacts` policy and `keep_on_success` patterns. No
snapshot on a legacy attempt means preservation. Keep/pattern exceptions mark
`retained` without unlinking; the independent supervisor cleanup processes only
published/superseded artifacts from succeeded attempts with superseded jobs.
Deletion requires the current replacement's domain assessment to be comparable
and no pending/running extraction. Source paths must match persisted execution
stdout/stderr roots under both writer reservations, not a changed runtime root.

Migration `0018` stores immutable per-artifact cleanup identities and the
authorized replacement generation before any unlink. The whole batch preflights
source, staged and final UUID publication files by size/digest and rejects unsafe
paths or hidden publication-preparation copies. A second writer transaction
rechecks live replacement eligibility, project/root identity, artifact rows and
persisted log paths before deletion and directory fsync. Interrupted removals
can replay missing copies only with prepared intent. If a newer generation is
now incomplete, cleanup does not unlink remaining copies; it may complete a
tombstone for an artifact whose copies are already all absent, using a
transaction-local completion-only marker. Intents and artifact metadata remain
as evidence; unrelated files, directories and stdout/stderr streams are kept.

M13.09 freezes optional `compress_success_logs` per attempt (protocol v15;
legacy absence preserves raw logs). Migration `0019` records exact temporary
reservations and immutable gzip identities per attempt/stream. Compression
uses a Rust streaming codec, private 0600 temporaries, synced output and atomic
no-replace publication; only the exact reserved partial temporary is reclaimed
after interruption. Prepared identity is durable before rename, and published
metadata before raw retirement. The second phase verifies archive compressed
size/digest, decompressed size/digest, and raw identity, then unlinks and syncs
its parent before committing retirement/event.

Every phase rechecks succeeded outcome, frozen policy, persisted raw paths,
terminal extraction and diagnosis/repair consumers under a writer reservation.
For process-group attempts it also checks for live group members in `/proc`:
the leader's success alone does not imply its children's log descriptors are
closed. A non-quiescent group defers compression; a changed raw log never matches
and cannot be retired using stale archive identity. Blocking workers retain
the transaction while doing filesystem work. Corrupt/missing archives preserve
raw files and cause archive reads to fail rather than silently lose bytes.

Executor metadata and extractor context keep the literal raw paths.
`logs_for_job` exposes published archive descriptors for process, container and
unit executions. CLI reads prefer raw files, refresh descriptors if raw files
disappear, and decode archives using uncompressed follow offsets. It selects
archives through SQLite metadata, not filename suffix. Raw and archive names
are reserved from artifact registration/cleanup. Failed/lost/cancelled attempts
continue to use M13.04's tail records and are not compressed here.

M13.10 audits deletion ownership across cleanup and publication housekeeping.
Checks walk the full absolute pathname from `/` without canonicalizing away
links, including ancestors above configured roots. Artifact registration and
cleanup reject raw empty/dot/parent components and repeated/trailing separators
before matching retention patterns, so aliases cannot bypass an exact keep rule.
Failed-artifact cleanup verifies the registered project, strict publication
subtree and extractor/project association; unregistered or missing-root cases
preserve bytes. Nested extractor working directories inside the project remain
valid. Missing-parent replay syncs the nearest surviving validated directory.

| Destructive operation | Registered boundary and ownership check |
| --- | --- |
| Failed source/staged artifact removal | Persisted attempt log paths, UUID staging paths, registered project/publication subtree, full no-symlink ancestor walk, size/digest preflight. |
| Superseded publication/source/staged removal | Frozen publication root within the project and exact published metadata, persisted logs in both transactions, immutable intent and live replacement barrier. |
| Successful raw-log retirement and partial temporary recovery | Persisted per-stream paths and immutable archive/reservation identity, full ancestor walk, verified gzip/raw bytes and consumer/writer barriers. |
| Repair worktree removal | Exact recovery-derived worktree and stored Git/admin/directory identity, targeted Git removal, durable release evidence and candidate reference. |
| Artifact staging/publication temporary removal | Full source/staging/project/candidate directory-chain validation before file operations; only the invocation's temporary or a registered-artifact UUID temporary is eligible. |

These are ownership checks for a trusted same-user local installation, not a
filesystem sandbox against concurrent adversarial replacement. Race/interruption
coverage and the broader GC command remain in M13.12–M13.16.

M13.11 adds migration `0020` with failed-artifact cleanup intents and shared
version-one tombstones. Failed cleanup commits exact policy/project/attempt/copy
identities before unlink, reacquires the writer and rechecks all barriers.
Required files missing on first preparation are conflicts; missing files after
prepared intent may be replayed, while remaining bytes must still match. A new
incomplete replacement generation never authorizes further removal through an
old intent.

Failed-artifact discovery idempotently enqueues the existing
`cleanup_failed_artifacts` action kind, with real job/attempt associations and
versioned spec. Its executor validates the lease and association under both
writer reservations. Completion, barrier deferral and remaining batches are
distinct outcomes: deferral and continuation postpone durably without spending
operational retries; actual failures retain retry/error history. A long blocking
preflight or deletion cannot renew through another connection while holding the
writer, so the owner renews its validated claim inside that transaction before
commit. Stale claims at phase entry remain rejected.

Failed/superseded artifact cleanup, repair-worktree retirement and successful
raw-log retirement write immutable tombstones in the transaction publishing
their terminal state and cleanup event. Each tombstone records operation,
subject, schema version, reason/outcome, identity and decision/completion times;
failed-action execution also records its action ID. Replay cannot reset times or
duplicate terminal events. Existing removed rows are not backfilled with invented
verified identities or completion times. Superseded/worktree/archive consumers
retain their existing durable state machines; only failed cleanup is wired into
the literal failed-artifact action outbox.

Cancellation and job retry also reserve the jobs writer before reading their
state. This prevents read-to-write WAL snapshot upgrades from failing when
worker or supervisor writes commit concurrently; the no-op reservation changes
neither lifecycle semantics nor event history.
Notification enqueueing likewise reserves its fenced action before reading,
so delivery insertion and action completion cannot upgrade a stale read snapshot.

M13.12 adds worker protocol v16 `storage_status` and CLI
`igor storage status [--json]`. Project selection follows `igor list`: a selected
registered project is scoped; outside a project the inventory is global and
includes unregistered historical projects. Every section is aggregated through
independent queries in one deferred read transaction, without reserving a writer
or running filesystem/cleanup operations.

Artifact logical sizes count each row once and expose unknown-size counts;
removed history is separate from active records. Archive raw/compressed identity
bytes are distinct from physical measurements. Unmatched durable log preparation
reservations are reported separately with unknown temporary size; matching
archive records prevent double counting their preparation. Failed-tail and
repair-patch BLOB lengths are payload bytes, not SQLite allocation. Cleanup
actions/intents and operation-specific tombstone associations show progress and
history without adding their identities to live storage totals. Actual disk
footprint and worktree disk sizes remain unmeasured (`null` in JSON).

M13.13 adds worker protocol v17 `gc_plan` and `igor gc --dry-run [--json]`.
It reads one deferred database snapshot and observes filesystem/process/Git
identities through read-only preflight helpers shared with retention executors.
No writer reservation, cleanup intent, action claim, temporary creation, archive
publication or Git mutation is part of preview. M13.14 adds confirmed execution
as a separate request, without turning preview into mutation.

The typed plan covers failed/superseded artifacts, repair worktrees and successful
log compression, with operation/subject IDs, readiness and reason codes, effects
and exact verified target paths. Unsafe whole-batch preflight suppresses its
removals; legacy and active-consumer/replacement barriers remain visible. Existing
owned compression temporaries are described as recompression/reclamation targets,
not preserved bytes. Unregistered projects defer worktree and raw-log retirement
in both preview and execution, including a registration change between phases.
Missing-copy metadata replay retains the existing intent-specific conditions.

Preview is bounded to 32 subjects per operation and 64 artifacts per attempt,
with truncation and continuation markers rather than silently presenting a
partial plan as complete. Byte attribution uses individual verified files and
deduplicates paths; gzip savings and worktree totals stay unknown. The plan is
advisory and requires writer-fenced revalidation at execution. Tests compare
stable-fixture effects against existing executors and verify SQL/filesystem
nonmutation using a read-only connection while another writer holds a transaction.

M13.14 adds protocol v18 `gc_execute` and CLI
`igor gc [--dry-run] [--failed] [--yes] [--json]`. Interactive execution shows the
selected work and accepts only an affirmative response; decline/EOF sends no
execution request. Dry-run never prompts and uses the same failed-only filter.
JSON stdout is a single plan or execution result; prompts and summaries use
stderr. Unsafe or error results exit nonzero after preserving partial JSON.

Execution uses one fixed typed worklist selected from initially actionable plan
entries, deduplicated by attempt/recovery. Artifact IDs and log streams constrain
both executor phases; newly ready or newly inserted records cannot expand the
confirmed pass. Per-entry consent fingerprints bind persisted context and
identity, while observed target paths are compared and never used as unlink
instructions. Every operation uses its existing writer-fenced executor and
rechecks policy, consumers, identity, paths and live generation barriers. Counts
distinguish completed/replayed, deferred, unsafe and error/partial outcomes.
The command does not claim global outbox work or rediscover/drain candidates.

The bound remains 32 subjects per operation and 64 artifacts per attempt. GC
execution request frames allow 32 MiB to carry bounded selections, while other
requests retain the 1 MiB limit. `--failed` includes registered failed-artifact
retention, failed full-log retirement and explicitly owned private failed outputs,
with no force deletion or recursive sweep.

A13.01 failed logs adds SQLite v21 and protocol v19. New attempts freeze
`result.failed_full_logs`; absent legacy snapshots and `keep` preserve full logs,
and retries clone the original contract. Each stdout/stderr retirement records an
immutable prepared identity (attempt, stream, exact raw path, original size/hash,
tail start/end/hash). Both writer-first phases require durable `diagnosed_attempts`,
a complete bounded tail matching the quiescent raw suffix, a failed/cancelled/lost
attempt, registered project, no active extractor or repair/diagnostic consumer,
backend registration and process-group quiescence. Superseded jobs also require
the current replacement generation to be comparable, including zero-artifact jobs.
Absolute directory chains, exact persisted paths, regular files and single links
are verified before removal. Missing raw bytes authorize metadata-only replay
only with the matching prepared intent. Surviving-parent fsync precedes atomic
completion, tombstone and one cleanup event per stream. A bounded supervisor
maintenance pass rediscovers deferred work; explicit GC uses the same executor,
fixed stream selections and immutable consent identity in both phases.

A13.01 private outputs adds SQLite v22 and protocol v20. Submission freezes
`result.owned_namespace = Some(true)` alongside failed-artifact retention and
keep patterns; absent legacy snapshots (including retries) preserve outputs.
Every process, Docker and systemd log preparation goes through one repository
helper. A newly created, empty `RuntimePaths.log_dir/ATTEMPT_UUID` directory is
fsynced and recorded before child launch with exact root, device/inode, attempt
ID, creation proof and stdout/stderr bindings. Existing directories are never
adopted; checkout, publication and staging overlap cannot grant ownership.

Discovery happens only after durable diagnosis, complete tails, terminal failed
outcome, registered project, no extractor/repair consumers, quiescent process
group and current comparable replacement. It reads at most 4097 directory entries,
without recursion; namespaces with more than 4096 children explicitly defer.
Sorted actionable pages contain at most 64 files, skipping protected entries
without hashing them or spending an actionable slot. Registered sources, exact
stdout/stderr log/archive names, durable compression temporary paths and `.git`
boundaries are excluded; ordinary dotfiles, `.tmp`/`.gz`, `config` and `index`
remain eligible. Directories are preserved. Nonregular, symlinked or hardlinked
leaves are unsafe; keep patterns apply to relative leaf names. More than 4096 registered exclusions
defer the namespace rather than dropping exclusions. Truncation is explicit.
Preview/storage status do not insert manifests, intents or actions.

`owned_output_manifest` records stable attempt/name-hash subjects and immutable
file/root identities in prepared/completed states. Consent binds the complete
identity and frozen context; selected execution never adds new files. Both
writer-first phases recheck eligibility, full absolute directory chains,
root device/inode, persisted stdout/stderr paths and file digest/device/inode.
Unlink uses a verified directory descriptor; parent fsync precedes immutable
completion, `failed_owned_output_retention` tombstone and one subject event.
Missing leaves are replayable only with matching prepared evidence; substituted
roots/leaves preserve bytes. The root itself is never deleted. Bounded periodic
supervisor maintenance and worker-only GC share this executor, including
zero-registered-artifact candidates. Storage exposes manifest logical bytes,
pending/completed intent states and tombstone references, not a fabricated
physical footprint. The real-process acceptance writes binary logs longer than
their tails, FD-discovered private siblings and checkout/external sentinels;
diagnosis and supervisor retire only full logs and the disposable sibling.

`owned_output_discovery` records namespace completion separately from immutable
ownership. A drained or keep-only namespace produces one metadata-only subject
with consent binding its root identity and sorted child metadata snapshot. New
files, unsafe roots, consumers or pending manifests prevent this authorization
from advancing completion. Finishing discovery never expands a confirmed file
worklist: it is authorized in a subsequent request after the selected outputs
are retired. Candidate queries exclude completed discoveries while retaining
prepared manifests, so drained history cannot starve the 33rd namespace.
Completed namespaces are not rescanned for future unknown outputs. Storage
reports pending/completed discovery counts without filesystem writes.

`JobLogs` returns descriptors only after completed retirement. `RetiredLogChunk`
reads one SQLite BLOB `substr` capped at 64 KiB, validates the frozen descriptor,
and returns original byte coordinates. Tail BLOBs are immutable once an intent
exists. `igor logs` falls back only after raw NotFound and a completed descriptor,
pins refreshes to the chosen attempt, preserves binary bytes, reports omitted
prefixes on stderr and retains original offsets across raw-to-tail follow.
Neither the two 16 MiB tails nor their JSON byte expansion enter a metadata frame.
Storage inventory reports intent states and logical identity sizes; physical raw
footprint stays unknown rather than counting prepared or retired files as present.

A13.02 checks saved read-only plans against independently observed execution
effects for all six GC operations. Oracles derive target paths and byte lengths
from fixture files, compare exact file deltas (subtree roots for worktrees),
verify preserved sentinels and candidate refs, and inspect per-subject outcomes
and durable evidence. Compression checks the deterministic gzip destination,
binary roundtrip and absence of residual temporaries across fresh, reserved,
prepared, published and metadata-only phases; it does not predict fresh private
temporary names or compressed sizes. Namespace discovery completion changes
metadata only, never implicitly authorizing a new file-removal worklist.

Under unchanged eligibility and consent, the selected effects and outcomes
match the plan. Changed consent or barriers defer the saved selection without
expansion. A successful retain decision with no persisted lifecycle marker is
reported completed only after strict post-step preview/consent revalidation;
legacy snapshots and disabled compression do not create fictitious records.
Persisted terminal subjects retain their already-done reporting semantics.

CLI acceptance compares project/global scope with two real worker attempts,
human/JSON truncation, fixed failed-only worklists and read-only SQL/filesystem
snapshots. Initial preview-unsafe subjects remain outside execution consent but
appear as unsafe outcomes with exit code 1 alongside selected partial results.
An unsafe-only plan needs no confirmation or execution request and reports the
rejection even without `--yes`. Decline/EOF of a nonempty worklist still sends no
request and returns an empty unconfirmed result. No schema or protocol change is
needed for these outcome-reporting corrections.

A13.03 extends interruption acceptance with fresh database handles after exact
fault seams for all six operations. Failed/superseded artifacts, full failed
logs and private outputs retain committed prepared identities across a failed
completion transaction after real unlinks; restart preserves decision identity,
stored binary tails and the original selected subjects. Replay checks complete
SQL/filesystem snapshots and one tombstone/event per subject. Diagnosis,
consumers, replacement generations and protected/unselected bytes remain
revalidated through the existing phase-boundary tests.

Compression restart tests cover reservations before any bytes, interrupted
partial temporaries, rename before publication metadata, raw retirement before
completion, and event rollback. Only the exact durably reserved temporary is
recovered; foreign temporaries stay intact and the published gzip still
reproduces the original binary log. Worktree recovery preserves candidate refs
and registered Git boundaries; an absent tree without a prior cleaning intent
remains unsafe, while matching durable cleaning evidence permits targeted replay.
An interrupted metadata-only namespace closure rolls back without deleting bytes
and completes once on the same saved selection after reopening SQLite.

Maintenance acceptance expires a running failed-cleanup claim after unlink and
completion rollback, reopens SQLite, rejects the stale lease and reclaims the
same idempotent action with a fresh lease. Completion retains the original
decision identity/time and action attribution without duplicate evidence.

The CLI fixture additionally kills the worker with verified SIGKILL after a
faulted GC request has left a durable prepared intent, then reopens SQLite and
starts a new worker on the same paths. It completes the missing-copy metadata
once and observes an empty read-only preview afterwards. Exact in-operation
failure locations use SQL trigger faults; this process-kill case occurs after
the failed request, not inside an active filesystem or SQL phase. Broader
crash-injection hardening remains in M17.10.

M13.15 complements phase-boundary fault triggers with independent-connection
concurrency tests. External writers and barrier/oneshot handoffs overlap real
repository operations without using sleeps to select ordering. Diagnosis commit
blocks cleanup while rollback permits it; read-only preview sees committed
diagnosis state even while a substantive writer transaction remains open.
Late publication racing cleanup preserves files until current replacement
success. Real G3 supersession racing a confirmed G1 retirement accepts either
legal serialized outcome and checks that subsequent replay under pending G3
cannot begin another unlink.

Concurrent worktree cleaners and log compressors preserve unrelated bytes and
protected candidate references, with exactly one durable tombstone/event per
removed subject or retired stream. Oracles inspect event identities and stable
replay records, not merely aggregate counts. Worker futures are structured under
deadlock watchdogs so test error paths do not detach cleanup tasks. These tests
exercise writer-first overlap and both legal outcomes; existing triggers remain
the exact preparation/removal phase seams.

M13.16 extends path negatives to interrupted cleanup with already-absent leaves.
Prepared intents do not bypass validation of linked source/staging/publication
ancestry, and an unsafe missing copy blocks retirement of other valid copies
in the batch. Tests verify external sentinel bytes and link metadata remain
unchanged, intents remain prepared and no removal tombstone is accepted.

Reserved log temporaries reject symlinks, shared hardlinks, foreign parents and
nonregular FIFO leaves without reclaiming or publishing bytes. FIFO tests run
the operation in a killable child test process with a watchdog; a blocking-read
regression cannot leave the parent suite stuck. Worktree tests cover `.git`,
admin `gitdir` and `commondir` links/foreign backreferences at registration,
candidate release and cleanup, plus missing-tree replay behind linked parents.
They also establish exact lexical path/root matching: normalized-away dot,
separator or trailing spellings are rejected. The ordinary Git `../..`
commondir literal and exact absolute common-dir paths remain valid. These
lexical fixes prevent alias acceptance; the probes did not demonstrate a new
foreign-file deletion in the worktree code.

Cleanup is an idempotent supervisor action. It must never delete files outside
registered attempt directories or declared artifact roots.

Commands:

```bash
igor storage status
igor gc --dry-run
igor gc
igor gc --failed
```

## 19. Notifications

The MVP includes Telegram through a transactional outbox. Future providers may
include webhooks, desktop notifications, and external commands.

Notifications should contain useful results rather than primarily file paths:

- Job, attempt, family, generation, and seed.
- Exit status and duration.
- Main extracted metrics.
- Validation selection criterion.
- Per-class metrics when configured.
- Family progress.
- Failure classification and repair status.
- Technical log reference last.

Credentials live in protected user configuration and are never persisted in
job specifications, passed to experiments, passed to agents, or included in
stored exception URLs.
`igor notify setup` accepts the token on standard input. The terminal attempt
transaction enqueues an idempotent notification action; the supervisor claims
that action, atomically creates a delivery and marks the action succeeded, then
claims and sends deliveries separately. Failed HTTP calls record fixed error
codes rather than exception strings or token-bearing URLs. A failed send is
rescheduled with bounded backoff; an expired delivery lease may cause an
at-least-once duplicate after restart.

## 20. CLI Surface

### 20.1 Projects And Configuration

```bash
igor init [PATH]
igor project add PATH
igor project list
igor project remove [PATH]
igor config path
igor config show
igor config check
igor doctor
```

`igor init` creates `.igor/project.toml` with unrestricted CPU and memory, no
timeout, and exclusive-host scheduling defaults. It creates only configuration
files needed by enabled features and never overwrites an existing file unless
the user passes `--force`. Runtime state, logs, databases, and secrets are not
created inside the project.

`igor project remove` defaults to the current directory and removes only the
active registration. It preserves the project directory and all Igor history,
can be reversed with `igor project add`, and refuses to deregister a project
while it has queued or running jobs.

### 20.2 Jobs And Families

```bash
igor submit -- COMMAND ARG...
igor submit --file job.toml
igor family submit --file family.toml
igor list
igor show JOB_ID
igor family show FAMILY_ID
igor wait JOB_ID
igor cancel JOB_ID
igor retry JOB_ID
igor remove JOB_ID
igor events JOB_ID
```

### 20.3 Logs And Results

```bash
igor logs JOB_ID
igor logs --follow JOB_ID
igor metrics JOB_ID
igor report generate FAMILY_ID
igor report generate FAMILY_ID --generator opencode
igor report generate FAMILY_ID --generator pi
igor report retry FAMILY_ID
```

### 20.4 Recovery And Agents

```bash
igor recovery show JOB_ID
igor recovery approve RECOVERY_ID
igor recovery reject RECOVERY_ID
igor agents sessions
igor agents sessions cleanup
```

### 20.5 Resources And Storage

```bash
igor resources
igor storage status
igor gc --dry-run
igor gc
```

### 20.6 Daemons

```bash
igor worker
igor supervisor
igor daemon status
igor daemon health
```

All read commands should support `--json` and stable documented exit codes.

## 21. systemd Management

One installed binary provides both services:

```text
igor-worker.service
igor-supervisor.service
```

Management commands:

```bash
igor service install --user
igor service install --user --enable --start
igor service enable
igor service enable --now
igor service disable
igor service disable --now
igor service start
igor service stop
igor service restart
igor service status
igor service logs
igor service uninstall --user
igor uninstall
```

Installation generates units using the detected stable binary and XDG paths,
runs `systemctl --user daemon-reload`, and never invokes `sudo` implicitly.
Installing units, enabling them, and starting them remain distinguishable
operations.

Both uninstall commands require confirmation unless `--yes` is supplied.
`igor uninstall` is intentionally conservative: it removes the same managed
user units as `igor service uninstall` but preserves the Igor binary,
configuration, database, logs, and job history.

The initial systemd units should use restart-on-failure, deliberate shutdown
semantics, and resource priority appropriate to each service. They must not
hardcode a project path.

The worker and supervisor are two user units backed by the same absolute,
installed `igor` executable. Generate their contents from one deterministic
renderer and treat binary paths and any configured XDG values as untrusted unit
input: escape systemd argument syntax and specifiers, rather than interpolating
raw paths into `ExecStart` or `Environment`. Do not bind units to the caller's
current project or working directory. Keep install separate from enable and
start; reinstalling identical units is idempotent. A failed install or reload
must not silently destroy previously installed working units.

Use `Restart=on-failure` for daemon crashes and a bounded stop interval long
enough for the worker's cooperative shutdown and lease handoff. Do not apply
experiment CPU, memory, or GPU limits to the worker unit. Give only the
supervisor low CPU and I/O scheduling priority. Stopping a user service must
leave recoverable active attempts available for reconciliation after restart;
validate that behavior with an opt-in live user-manager test, while ordinary
tests validate rendering and CLI behavior without a running systemd manager.
Until direct attempts use independent transient units, the worker service must
not use systemd's default whole-cgroup kill behavior: Process children must
survive worker stop/restart for the persisted-identity recovery path to work.
An opt-in live user-manager test verifies this survival across `systemctl --user`
restart. Process-group reattachment cannot recover the final exit code after
the original parent exits, so a subsequently completed attempt may be
classified as `lost` even though its process finished. Transient per-attempt
units should be evaluated for durable identity and authoritative completion;
retain process groups for environments without a user manager.
An initial `systemd-run --user` probe showed that completed successful units
are unloaded immediately unless `--remain-after-exit` is set. Retained units
expose `ExecMainStatus` and `InvocationID`, but require explicit cleanup after
the terminal outcome has been committed; launching them safely also requires
persisting their names before external creation and reconciling both sides of
that boundary on worker restart.
The optional `systemd_user_unit` process backend must reserve a unique,
attempt-derived unit name durably before invoking `systemd-run --user` with
`--remain-after-exit`. On recovery, inspect that exact name and its invocation
identity; never start a replacement for an attempt that might already be
running. Capture logs in Igor's attempt paths, clear the manager's ambient
environment before passing explicitly allowed job values, and use systemd's
reported result, exit code, or termination signal as the authoritative terminal
outcome. Disable systemd-run argument environment expansion so literal dollar
signs in job arguments are preserved. Stop or
kill only the owned unit for cancellation and timeout. Persist the outcome and
release leases before stopping/resetting the retained unit. If systemd is
unavailable or a result is ambiguous, preserve ownership for reconciliation.
The existing process-group backend stays the default and portable fallback.

## 22. Diagnostics And Maintenance

`igor doctor` checks:

- Configuration validity.
- SQLite access and schema version.
- Unix-socket permissions.
- User systemd availability and unit state.
- Cgroup capabilities.
- Docker availability when configured.
- GPU discovery and device visibility.
- Coding-agent availability and versions.
- Telegram configuration without revealing secrets.
- Available state and artifact disk space.
- Stale leases, transient units, containers, worktrees, and agent sessions.

Database operations:

```bash
igor db check
igor db migrate
igor db backup DESTINATION
```

User-facing service uninstall supports `--dry-run` and requires explicit
confirmation unless `--yes` is passed. `igor db migrate --dry-run` validates
and previews a pending schema upgrade without applying it. Automatic cleanup
of Igor-owned attempts does not prompt the user.

### 22.1 Compatibility Boundaries

The local CLI/worker/supervisor wire protocol requires an exact version match
(currently `9`); incompatible messages fail before operational dispatch. Global
and project TOML, job and family TOML, and event payloads each have their own
explicit version (`1` in the current implementation). Readers reject an
unsupported version rather than silently interpreting it as the current schema.
SQLite uses numbered, checksummed SQLx migrations (currently through `0020`).
Writable worker startup applies available migrations; `igor db check` only
reads, and `igor db migrate` explicitly upgrades an existing database after
integrity and foreign-key checks. Back up before upgrades. Database migration
does not imply TOML conversion, downgrade support, or compatibility between
different protocol versions. Upgrade the binary and both user services together.

### 22.2 Trusted-User Security Boundary

Igor assumes a trusted single-user Linux account. Worker and supervisor sockets
are private to that user and reject peers of another UID, but processes running
as the same user are not mutually isolated. Direct jobs, transient user units,
Docker jobs, explicit shell commands, and future hooks or coding agents must be
treated as code with access to that account. No general hook or coding-agent
executor is currently available; planned context limits and session cleanup
must be implemented and audited before relying on them.

Docker uses only explicitly configured mount sources; writable mounts can
modify host paths beyond the project root and are not a security sandbox.
Declared scientific files are checked within the project and symlinks are
rejected, while Docker mounts deliberately use their separately declared
absolute-path boundary. The user configuration holds Telegram credentials,
is written with mode `0600`, and is redacted in effective-config output.
Direct experiment process environments start cleared and apply an inheritance
policy; names recognized as sensitive are filtered or rejected, but arbitrary
names and job output are not guaranteed secret-free. Use minimal inheritance
for untrusted environment contents and protect user config, logs, SQLite files
and backups.

### 22.3 Subprocess Environment Audit

Current production subprocess entry points and environment boundaries:

| Child | Call sites | Environment and boundary |
|---|---|---|
| Direct experiment | `worker.rs` process-group execution | Starts with `env_clear`; the default inherits only basic locale, user, home and path values. Explicit variables with recognized sensitive names are rejected. `inherit = "all"` is opt-in and filters recognized names, not arbitrary secret values. |
| Transient experiment unit | `worker.rs` and `systemd.rs` `systemd-run` | Builds the same filtered process environment, then launches the attempt through `/usr/bin/env -i` inside the user unit. The manager command itself is a trusted administrative child. |
| Docker experiment | `executor.rs` planner and `worker.rs` Docker lifecycle | The trusted host `docker` CLI inherits the worker environment for local daemon and user configuration. Container creation passes only explicit, name-validated `--env` values and assigned GPUs; it does not copy the worker's ambient environment into the container. Image defaults remain in force. Explicit values appear in Docker CLI arguments. |
| Local metadata helpers | `submission.rs` Git, `inventory.rs` `nvidia-smi`, `executor.rs` Docker probes | Inherit the worker's environment as trusted local tools. Git's configured helpers/filters and executable resolution therefore remain within the trusted-user boundary; their environment is not used as a job's environment. |
| Service administration | CLI `systemctl` and `journalctl`; worker `systemctl`/`systemd-run` | Inherit the invoking CLI/worker environment to contact the user manager and display service state. They do not receive Telegram credentials from project job specifications or pass the inherited environment as the experiment unit environment. |
| Metric extractor | `extract.rs` supervisor action | Starts with `env_clear`, passes only `PATH`, runs the frozen direct command from the private attempt directory, and supplies verified attempt log paths and IDs on stdin. Its stdout/stderr are bounded; extractor code remains trusted same-user code, not a sandbox. |
| Repair-worktree maintenance | `repair_worktrees.rs` registration/release/cleanup | Uses `/usr/bin/git` with a cleared environment and fixed `PATH`, disables hooks, bounds output and command duration, and operates only on validated repository/worktree identities. Patch evidence is in SQLite and candidate refs are preserved before targeted removal. |

Telegram delivery uses an in-process HTTP client, not a shell child. Hooks,
coding-agent subprocesses are not implemented yet and must receive their own
environment review when introduced. This audit does not
claim that trusted administrative helpers are isolated from the invoking
account or that a non-sensitive variable name cannot hold a secret. Existing
process, Docker and systemd tests exercise the experiment/container boundary;
re-audit when new subprocess entry points are added.

### 22.4 Destructive Operation Audit

| Operation | Scope and recovery contract |
|---|---|
| `igor init --force` and user configuration updates | `--force` explicitly authorizes replacement of the two project templates. The `.igor` directory and both target paths are checked against symlinks/nonregular entries before writing; forced replacement stages each file and preserves existing permissions. Global user settings use an atomic private-file writer. Project initialization is repeatable, but its two file replacements are not one filesystem transaction. |
| Service installation/management/removal | Only the fixed Igor user-unit names are targeted. Preexisting units must be regular files with the expected Igor identity; symlinks and unrelated units are rejected even on uninstall. Installation skips identical units and attempts file rollback on reload failure. Uninstall offers `--dry-run`, defaults to a confirmation prompt, stages owned files for restoration if reload fails, and is a no-op if no units exist. `disable --now` can already have changed service state when a later file/reload step fails; restoration of unit bytes does not restart a stopped service. |
| `igor db migrate` and `igor db backup` | Migration is explicit, supports `--dry-run`, refuses to create a missing source, and checks integrity/foreign keys before and after applying checksummed migrations. Backup targets an exclusive new path; the SQLite backup API verifies the copy and refuses overwrite/source alias. Neither offers a downgrade or an implicit restore. |
| Project deregistration, job cancellation and retry | Project removal changes registration only after checking that no queued/running jobs remain; history and files are retained and registration can be restored. Cancellation targets a specific job ID and attempts a graceful stop before escalation. Retry preserves previous attempts. These explicit state transitions do not delete research history. |
| Worker cleanup and internal database pruning | Docker commands address a persisted container ID, systemd commands a persisted attempt-derived unit name with invocation checks, and process groups are signalled only while the recovered leader identity still matches. Missing Docker objects are treated idempotently. Resource-leases and stale Igor-managed inventory rows are pruned within scoped transactions; job, attempt and event history is not pruned. Stale socket removal checks UID/type and rechecks inode under the role lock. |

Remaining limits: a process-group identity check and subsequent signal cannot
form one atomic kernel operation; use the transient user-unit backend when
authoritative restart outcomes matter. A systemd unit name can also be reused
between inspection and a manager operation by another process of the same
trusted user. Crash-injection coverage for these boundaries remains in M17.10.

## 23. Rust Stack

Initial dependencies:

| Need | Crate or approach |
|---|---|
| CLI | `clap` |
| Async processes and signals | `tokio` |
| SQLite and migrations | `sqlx` |
| Serialization | `serde`, `serde_json`, `toml` |
| HTTP and Telegram | `reqwest` |
| Diagnostics | `tracing`, `tracing-subscriber` |
| Library errors | `thiserror` |
| Application context | `anyhow` |
| XDG paths | `directories` |
| IDs | `uuid` |
| Timestamps | `time` |
| CLI integration tests | `assert_cmd`, `predicates` |
| Temporary fixtures | `tempfile` |

Avoid adding an abstraction or dependency until its first use is implemented.
Commit `Cargo.lock` because Igor ships executable applications.

## 24. Testing Strategy

### 24.1 Unit Tests

- Valid and invalid state transitions.
- Retry and delivery backoff.
- Priority, resource fit, and aging.
- Family-generation comparability.
- Repair invalidation of every seed.
- Retention decisions.
- Prompt and context hashing.
- Structured command serialization.

### 24.2 SQLite Integration Tests

- Every migration from an empty database.
- Upgrade from prior schema fixtures.
- Foreign-key enforcement.
- Concurrent claims.
- Atomic resource reservations.
- Leases and heartbeat expiry.
- Transactional events and actions.
- Idempotent notification and report actions.
- Consistent backup.

### 24.3 Process Tests

- Success and nonzero exit.
- Signals and cancellation escalation.
- No-timeout execution.
- stdout and stderr capture.
- Arguments and paths containing spaces.
- Worker restart during an active process.
- Lost-process classification.

### 24.4 Docker And systemd Tests

- Disposable container success, failure, cancellation, and recovery.
- Image digest verification.
- GPU argument construction without requiring a GPU in ordinary CI.
- Ordinary Docker tests use a stateful fake CLI and require no Docker runtime.
  Real-daemon tests are opt-in through `IGOR_RUN_DOCKER_TESTS=1` and
  `IGOR_TEST_DOCKER_IMAGE`; they use only an already-local CPU image and print an
  explicit skip reason when prerequisites are absent.
- Unit generation and validation.
- User-service install, enable, disable, and uninstall where systemd is
  available.

### 24.5 Agent And Report Tests

- Pi invocation includes `--no-session`.
- OpenCode session deletion after success and failure.
- Cleanup of an orphaned `igor:*` session.
- Agent context excludes secrets.
- Invalid report output is not published.
- Atomic report replacement.
- Superseded generations are excluded from aggregates.

Quality gates:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --no-deps
```

## 25. Implementation Roadmap

### Phase 1: Foundation

- Initialize Git and the Cargo workspace.
- Add dual MIT/Apache-2.0 licensing.
- Implement typed configuration and XDG paths.
- Add SQLite migrations and core state types.
- Establish formatting, linting, tests, and CI.

### Phase 2: Execution Engine

- Implement CLI submission and inspection.
- Implement jobs, attempts, events, and leases.
- Implement the direct-process executor.
- Add logs, cancellation, recovery, and exclusive-host scheduling.
- Add resource discovery and exclusive GPU allocation.

### Phase 3: Daily Operation

- Implement the worker and supervisor services.
- Add Unix-socket communication.
- Add service install, enable, disable, status, logs, and uninstall commands.
- Add Docker execution.
- Add Telegram outbox and useful result notifications.
- Add `igor doctor`, database backup, and garbage collection.

### Phase 4: Scientific Results

- Define and validate the metric-extractor JSON schema.
- Implement families, generations, seeds, and comparability checks.
- Implement deterministic Markdown reports.
- Add prompted report generation through OpenCode and Pi.
- Implement ephemeral session cleanup.

### Phase 5: Assisted Recovery

- Classify transient and code failures.
- Create isolated repair worktrees.
- Enforce protected paths.
- Invoke OpenCode or Pi and validate repairs.
- Request cancellation approval for obsolete active attempts.
- Create a new generation and rerun every family member.
- Clean failed and superseded artifacts according to policy.

### Phase 5b: Release Hardening And Measured Optimization (Milestone 17)

- Finish diagnostic, security, compatibility, and release-packaging work.
- Before changing performance-sensitive code, establish reproducible baselines
  for representative queue, scheduling, recovery, persistence, CLI, and
  supervisor workloads. Record environment, fixture, p50/p95 latency,
  throughput, memory, allocations, and database query costs where relevant.
- Review module boundaries and simplify duplicated or tightly coupled code
  when it improves cohesion and maintainability. Optimize measured bottlenecks
  rather than making speculative rewrites; compare before and after against
  predeclared goals and watch for regressions in other workloads.
- Preserve public contracts, durable history, transaction boundaries, lease
  fencing, restart recovery, and security properties with focused tests.
- Rerun all quality gates, opt-in integration suites, and release checks on a
  clean environment after the last optimization, before accepting Hito 17.

### Phase 6: Sleep_CNN Migration

- Wait for Sleep_CNN jobs `#61-#63` and their curator actions to finish.
- Freeze submissions to the legacy queue.
- Back up both legacy SQLite databases with the SQLite backup API.
- Export jobs, attempts, notifications, and curator history to a versioned
  interchange format.
- Import legacy history into a new Igor database without altering old files.
- Create the Sleep_CNN project, family, extractor, report prompt, Docker, and
  artifact configuration.
- Validate disposable commands and a smoke family.
- Activate Igor only for new experiments.
- Preserve legacy databases and snapshots read-only until migration is fully
  verified.

## 26. MVP Acceptance Criteria

The first useful release is complete when it can:

1. Install and operate worker and supervisor user services from the CLI.
2. Execute arbitrary process and Docker commands without language assumptions.
3. Give an experiment unrestricted CPU and RAM with exclusive GPU scheduling.
4. Persist jobs, attempts, events, logs, and actions across daemon restarts.
5. Cancel and retry without losing attempt history.
6. Run every seed of a family against one immutable generation.
7. Invalidate and rerun a complete family after an approved code repair.
8. Extract normalized metrics through a project command.
9. Send useful persistent Telegram notifications.
10. Generate `RESULTS.md` from a versioned project prompt.
11. Use Pi without session persistence and remove OpenCode sessions after use.
12. Remove failed large artifacts after diagnosis while preserving concise
    provenance and failure history in SQLite.
13. Pass formatting, linting, unit, SQLite, process, and CLI integration tests.

## 27. Fixed Decisions

- Product and binary name: Igor / `igor`.
- Meaning: Integrated General-purpose Orchestrator for Research.
- Implementation language: Rust.
- License: MIT OR Apache-2.0.
- Deployment: trusted single-user Linux workstation first.
- Service mode: `systemd --user` first.
- Services: worker and supervisor from one binary.
- Database: one logical SQLite database per installation.
- Executors in MVP: direct process and Docker.
- Default compute policy: exclusive and unrestricted.
- Default timeout: none.
- Default GPU policy: exclusive.
- Code repair scope: complete experiment family generation.
- Active obsolete attempt: cancel only after approval.
- Scientific configurations: immutable and protected from coding agents.
- Reports: project `RESULTS.md`, generated when a comparable family completes.
- Report prompt: versioned project file.
- Report agents: OpenCode and Pi.
- Agent history: ephemeral by default.
- Failed artifacts: delete after diagnosis unless explicitly retained.
- Superseded artifacts: delete after a successful replacement generation.
- Sleep_CNN migration: only after legacy jobs through `#63` finish.
