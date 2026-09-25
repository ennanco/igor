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

A validated code correction creates a new family generation. All related seeds
are rerun so the aggregate never mixes code revisions. Successful results from
the previous generation become `superseded`, not comparable.

A transient infrastructure failure that does not modify code only retries the
failed job within the same generation.

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
extractor = ["python", "scripts/export_igor_metrics.py"]
working_directory = "attempt"
```

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
    {"path": "metrics.json", "role": "metrics", "sha256": "..."}
  ]
}
```

Igor stores the original document and indexes selected scalar values without
changing their scientific meaning.

## 15. Markdown Reports

Obsidian is not required by the first version. SQLite stores operational and
structured result data. When a complete comparable family generation finishes,
Igor generates a project Markdown report.

Default configuration:

```toml
[report]
enabled = true
trigger = "family-completed"
output = "RESULTS.md"
mode = "replace-generated-file"
generator = "opencode"
prompt_file = ".igor/report-prompt.md"
```

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
igor db backup
```

Destructive cleanup commands provide `--dry-run` and require explicit
confirmation unless `--yes` is passed.

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
