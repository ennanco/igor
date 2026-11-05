# Igor

[![Status: in development](https://img.shields.io/badge/status-in%20development-orange)](TASKS.md)
[![CI](https://github.com/ennanco/igor/actions/workflows/ci.yml/badge.svg)](https://github.com/ennanco/igor/actions/workflows/ci.yml)
[![Rust 1.97.1](https://img.shields.io/badge/rust-1.97.1-black?logo=rust)](rust-toolchain.toml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)

**Integrated General-purpose Orchestrator for Research**

Igor is a resource-aware experiment runner and recovery orchestrator for Linux.
It executes process and Docker workloads, coordinates exclusive compute
resources, persists execution history, extracts structured results, and supports
opt-in deterministic project reports.

The `v0.2` roadmap scope (milestones 0–10) and non-agent `v0.3` scope (11–14)
have passed acceptance. Families, generation-scoped metrics, recoverable retention
and deterministic Markdown reports work together in a real five-seed workflow.
Reports use a normalized read-only context, two-phase atomic publication and
guarded generated-file replacement. The supervisor triggers durable actions for
completed comparable families; manual generation and retries are available through
`igor report generate` and `igor report retry`. Partial or failed replacements
keep results provisional instead of selecting an older generation's aggregate.
No packaged release
has been published, and the current Cargo/binary version is
still `0.1.0`. Igor remains under active development and is not ready for
production experiment management. See
[`DESIGN.md`](DESIGN.md) for the agreed architecture and [`TASKS.md`](TASKS.md)
for the ordered implementation backlog.

## Requirements

- Linux
- Rust `1.97.1`
- Git

Docker is optional. Direct-process installations do not need the Docker CLI,
daemon, socket, or Rust Docker client dependency. Docker jobs require a locally
reachable Docker daemon and permission for the worker user to use it.

## Optional Results And Notifications

Jobs can run with the worker alone. Metric extraction, Markdown reports,
coding-agent integrations and Telegram are optional and configured independently:

- Omit `[results]` to run without a metric extractor.
- Reports are disabled by default (`[report] enabled = false`). The available
  default generator is deterministic. Optional OpenCode/Pi integrations are
  planned for Milestone 15; explicit unsupported generators do not silently fall
  back to deterministic output.
- Omit Telegram credentials to run without Telegram delivery.

The core report context selects the current family generation in one SQLite
snapshot and separates historical generations and retries from primary metric
series. Pending extraction makes the context provisional; projects without an
extractor have an explicit empty metric result. Git and configuration identities,
artifacts and repair evidence come from persisted records. The pure
`render_report_markdown` API renders that context with stable GFM tables,
provisional/history labels, structured extractor evidence and encoded links to
published artifacts. `stage_report` prepares a private sibling file, and
`StagedReport::publish` revalidates its bytes and destination before an atomic
rename. Competing publishers are serialized; failures before rename preserve the
old output, while a later directory-sync failure reports the already-published
file identity explicitly.

The default `replace-generated-file` mode uses `stage_generated_report` and
requires reports to be explicitly enabled. It creates a missing `RESULTS.md` (or
the configured output), and replaces an existing file only when its generated-file
claim matches the project/path and its body checksum is intact. Handwritten
files and generated reports containing manual edits or appended notes are
preserved. Explicit OpenCode/Pi choices are not silently replaced by the
deterministic generator.

With the supervisor running, the `family-completed` trigger discovers the current
comparable generation and queues one durable report action with a frozen report
policy. Generations completed with the worker alone are discovered when the
supervisor starts; repeated discovery or restart does not duplicate requests.
The current report policy controls opt-in at discovery time. Extraction may
still be pending when the intent is queued. The report executor defers until the
current generation is ready, then revalidates the claim, project, report policy
and exact context before publishing. Superseded requests are cancelled; report
failures do not rerun experiments or change successful job outcomes. Busy outputs
and readiness waits preserve the retry budget. A shutdown/client cancellation
does not abandon an already-started publication transaction. Report executions
also retain durable context and output-hash history in SQLite.

## Request A Report

In `.igor/project.toml`, enable reporting explicitly:

```toml
[report]
enabled = true
generator = "deterministic"
```

Request a report for a family's current comparable generation:

```bash
igor report generate FAMILY_ID
igor report generate FAMILY_ID --json
igor --project /path/to/project report generate FAMILY_ID --json
```

The command returns a durable action ID, generation, state and planned output
(default `RESULTS.md`). Success means the request was accepted; the supervisor
publishes asynchronously. Requests can be queued with the worker alone and will
be processed when `igor supervisor` or its user service is running. No extractor
or coding agent is required for deterministic reports.

The current/selected project scopes the family; outside a project the family ID
resolves its registered owner. New requests for disabled reports or a
non-comparable generation return conflict (exit 9); missing/scoped-out families
return not-found (exit 8). Repeating the CLI command creates a fresh report
request without rerunning jobs or resetting an earlier action's retry budget.
Protocol-level retries with the same request UUID acknowledge the original
request without re-enqueueing, even if its generation/policy subsequently changed.
Publication still checks the live policy and protects handwritten/edited files.
The report RPC uses protocol v22; use matching CLI, worker and supervisor builds.

### Retry A Failed Report

```bash
igor report retry FAMILY_ID
igor report retry FAMILY_ID --json
```

Retry reuses the latest report action for the current comparable generation,
only when it is failed. It keeps its ID and frozen configuration, resets the
report retry budget and queues it for the supervisor. The previous error and
attempt count remain in immutable history. A newer active, successful or cancelled
request blocks fallback to an older failure (conflict, exit 9); a generation with
no report request returns not-found (exit 8). Disabled or changed report policy,
changed project metadata or a stale/incomplete generation also reject new retries.

`--project` and ambient-project scope work as for generation. JSON includes both
the retry request UUID and the existing action ID. Protocol replay of that same
UUID only acknowledges the original retry; it does not reset an active or failed
action again. Jobs and experiment attempts are unchanged, and publication still
protects handwritten/edited files. Use `report generate` for a fresh request with
the current configuration rather than reusing an incompatible frozen action.

### Report Provenance And Recovery

SQLite v23 records each report run's exact context JSON/SHA-256, frozen policy
and generation, claim provenance, destination, and full expected output hash/size.
The `context` phase is committed before staging; `prepared` records publication
intent before rename; `published` acknowledges a synced output together with the
action and its events. A prepared hash alone does not assert that a file exists.

After interruption, a reclaimed action revalidates the live generation/policy
and compares the destination with the recorded intent/baseline. An exact matching
output can be synced and acknowledged without rewriting it. Different newer
content is preserved rather than overwritten by recovery. Failed/retried and
superseded runs keep their original evidence; legacy runs with no recorded context
retain unknown provenance. Core read-only APIs `report_run` and `report_runs`
expose this history and verify context checksums.

The wire protocol remains v22; use updated matching CLI/worker/supervisor builds
with the v23 database. Old migration checksums are unchanged.

A minimal project configuration is simply `schema_version = 1`. `igor init`
includes optional configuration examples, with reports disabled. The supervisor
is used for enabled background services; it is not required to execute jobs.

Acceptance coverage includes a five-seed workflow across successful, failed and
manually repaired generations, preservation of handwritten files, full Markdown
snapshots and publication recovery after SIGKILL. The final report contains only
the current comparable generation's observations. See the
[acceptance evidence](crates/igor-cli/tests/report/README.md).

## Install From Source

Igor does not have packaged releases yet. Clone the repository and install the
`igor` binary with the pinned Rust toolchain:

```bash
git clone https://github.com/ennanco/igor.git
cd igor
rustup toolchain install 1.97.1
cargo install --path crates/igor-cli --locked
igor --version
```

Ensure Cargo's binary directory, normally `$HOME/.cargo/bin`, is in `PATH`.
During development, commands can instead be run without installing:

```bash
cargo run -p igor-cli -- --version
```

## Quick Start

Igor projects are Git repositories. Initialize the portable project
configuration and commit it before submitting work:

```bash
cd /path/to/project
igor init .
git add .igor
git commit -m "chore: configure Igor"
```

`igor init` creates `.igor/project.toml` and the default report prompt. Machine
state, logs, credentials, and host-specific settings remain outside the project
in the configured XDG directories.

Start the worker from the project in one terminal:

```bash
igor worker
```

The worker currently runs in the foreground. Leave it running and use a second
terminal in the same project to register the project and submit a command:

```bash
igor project add .
igor submit --name smoke-test -- /bin/sh -c 'echo started; sleep 2; echo finished'
```

The submit command prints the job ID. Use it in the remaining commands:

```bash
igor list
igor show JOB_ID
igor events JOB_ID
igor logs --follow JOB_ID
igor wait JOB_ID
```

After `igor family submit --file family.toml`, inspect the returned family ID:

```bash
igor family show FAMILY_ID
igor family show FAMILY_ID --json
```

The output lists every generation in order, its frozen revision and protocol
digest, member jobs, current states and status (`incomplete`, `complete`,
`comparable`, or `invalid`). A generation is comparable only when exactly its
required seeds have succeeded under the same generation identity; terminal
failures make it complete but not comparable. Counts are reported per generation and
across the family for all seven job states; `queued` is the pending-job count.
Counts describe current jobs, not accumulated attempts, and include superseded
members rather than silently combining their results with the current
generation. Large histories remain subject to the local protocol frame limit
until M17.18 adds pagination.

Running jobs can be cancelled, and failed, cancelled, or lost jobs can be
retried without deleting their previous attempts:

```bash
igor cancel JOB_ID
igor retry JOB_ID
```

A project with no queued or running jobs can be deregistered without deleting
its files or history, then registered again later:

```bash
igor project remove .
igor project add .
```

Igor's managed user services can be stopped and removed independently of user
data. Both forms preserve the binary, configuration, database, logs, and job
history, and require confirmation unless `--yes` is supplied:

```bash
igor service uninstall --user
igor service uninstall --user --dry-run
igor uninstall
igor uninstall --dry-run
```

Most inspection commands support machine-readable output through `--json`, for
example:

```bash
igor list --json
igor show JOB_ID --json
igor wait JOB_ID --json
```

Use `igor --help` or `igor COMMAND --help` for the complete command reference.

## Docker Jobs

Igor invokes the `docker` CLI directly and never pulls an image implicitly. The
configured image must already be local. When `digest` is set, Igor resolves the
exact `IMAGE@sha256:...` reference before creating a container and rejects a
mismatch. A digest for a pulled image can be obtained with:

```bash
docker image inspect --format '{{index .RepoDigests 0}}' alpine:3.20
```

Copy and edit [`config/example-docker-job.toml`](config/example-docker-job.toml),
then submit it like any other versioned job:

```bash
igor submit --file config/example-docker-job.toml
```

Docker execution follows these boundaries:

- Bind-mount sources and container targets must be absolute. Mounts are allowed
  only when explicitly declared, and writable mounts grant the container access
  to modify that host path.
- Image environment defaults are preserved. Only values in
  `environment.set` are added; ambient host variables, including Telegram,
  coding-agent, cloud, SSH, and Git credentials, are not copied.
- GPU jobs receive only the concrete GPU identities assigned by Igor's
  scheduler. Igor never uses Docker's all-GPU shortcut.
- Containers are labelled with Igor project, job, attempt, and optional
  generation identities. The worker persists the container ID before start.
- Cancellation uses bounded `docker stop`, escalates to `docker kill`, records
  the inspected result, and only then removes disposable containers.
- After worker restart, Igor reconciles the persisted ID first and can reattach
  logs and completion supervision. Exact labels are used only when a crash
  happened before the ID was persisted.

Common failure prefixes in `igor events JOB_ID` distinguish
`docker_cli_unavailable`, `docker_daemon_unavailable`,
`docker_image_missing_or_invalid`, `docker_create_failed`,
`docker_start_failed`, `docker_oom_killed`, `docker_timeout`, and
`docker_container_lost`. Use `igor show`, `igor events`, and `igor logs` together
with `docker version` when diagnosing a job.

## Host Scheduling Limits

The worker discovers host CPU, memory, and NVIDIA GPUs when it starts. Inspect
the effective configuration and registered resources with:

```bash
igor config show
igor resources
igor resources --json
```

Global scheduling capacities can be changed from the CLI. Restart the worker
after updating them:

```bash
igor config set --max-concurrent-jobs 1 --memory-bytes 32GB
igor config set --gpu GPU-uuid-1 --gpu GPU-uuid-2
igor config set --disable-gpus
igor config set --auto-memory --auto-cpu --auto-gpus
```

These values define the capacities that the resource-aware scheduler will use.
Compatible experiments can run concurrently up to `max_concurrent_jobs`; jobs
using the default exclusive-host mode remain isolated. The scheduler orders
work by submitted priority, adds one priority point per complete hour in the
queue to prevent starvation, uses global submission order to break effective
priority ties, and skips requests that do not currently fit. Aging never changes
the submitted priority shown by `igor list` or `igor show`.

Scheduling capacities do not impose hard cgroup limits on an individual
process; strict process limits will arrive with the planned `systemd`
integration. CPU and memory remain unrestricted unless a smaller scheduling
capacity is configured.

## User Services

Install the worker and supervisor units from an installed Igor binary:

```bash
igor service install --user
igor service install --user --enable --start
igor service status --json
igor service logs --follow
```

Installation alone neither enables nor starts the services. `igor service
enable [--now]`, `disable [--now]`, `start`, `stop`, and `restart` manage both
units. Repeating an unchanged installation does not reload systemd. The worker
unit leaves active Process jobs alive on stop so a restarted worker can
reconcile them; the supervisor runs at low CPU and I/O priority. Commands use
`systemd --user` and require a running user manager; no `sudo` is used.
`igor service uninstall --user` removes the managed units after confirmation
while preserving Igor's binary, configuration, database, logs, and history.
On restart, Process jobs remain alive and the worker reattaches to them. If
such a job finishes after its original worker exits, the process-group backend
cannot read its exit status and may record it as `lost`. For attempts requiring
authoritative exit status after a restart, set `executor.kind = "process"` and
`executor.settings.isolation = "systemd_user_unit"` in the job file. This
optional backend requires a running user manager, reserves each unit before
launch, retains its status through recovery, and cleans it after finalization.
The default `process_group` backend remains available without systemd.

## Supervisor And Notifications

The supervisor records a notification action in the same database transaction
that finishes each attempt. It then creates a Telegram delivery in the durable
outbox and sends it independently of the worker. Install and run both services
with `igor service install --user --enable --start` to enable this flow.

Configure Telegram in the **user** configuration, not `.igor/project.toml`:

```bash
igor notify setup --chat-id CHAT_ID < /path/to/private-bot-token
igor service restart
igor notify test
```

The token is read from standard input rather than a command argument and stored
in the user configuration with mode `0600`. `config show` redacts it. A delivery
failure is retried with bounded backoff; an unavailable endpoint never waits on
the worker's next job. Pending deliveries survive supervisor restart. Delivery
is **at least once**: if the supervisor crashes after Telegram accepts a message
but before Igor records acknowledgement, a duplicate can be sent. Messages
include status, duration, available metrics and family progress before the
technical job identifier. Metrics appear when an extractor has published them.

## Experiment Families

Declare a versioned `family.toml` with shared direct execution arguments and
one `[[members]]` entry per required seed. Member arguments and scientific
configuration files are explicit; see the complete format in `DESIGN.md`.
Commit project-local family and configuration files to Git (or set
`allow_dirty = true` explicitly). After registering the project and starting
the worker, submit its first generation:

```bash
igor family submit --file family.toml --json
```

Igor freezes one Git source and protocol identity for all members before
inserting the family, generation, jobs, attempts and initial events in one
database transaction. A changed worktree or failed insert leaves no partial
family submission. Each family name is unique per project; this command creates
its first generation. Inspect it with `igor family show FAMILY_ID [--json]`.
For an operator-confirmed transient failure, `igor retry JOB_ID` creates a new
attempt for the affected seed in the **same** generation, preserving its frozen
Git identity, configuration, protocol and earlier attempt history. A successful
retry makes the generation comparable only when every other required seed has
succeeded too. Automatic failure classification and agent-assisted repairs
remain on the backlog.

To replace a terminal generation after a committed code repair, keep the same
scientific `family.toml` and run:

```bash
igor family supersede FAMILY_ID --file family.toml --json
igor family show FAMILY_ID --json
```

Supersession requires every job in the previous generation to be terminal and
the new generation to have the same scientific protocol. Cancel any queued or
running jobs explicitly first; Igor will not stop them during supersession.
The operation adds a complete fresh seed set and marks the older jobs
`superseded` in one transaction, retaining their attempts and events. Family
inspection identifies the highest numbered generation as
`current_generation_id`; historical jobs cannot be retried once superseded.
Five-seed partial, failed, retried and repaired generations are covered by
disposable-repository tests. The current-generation aggregate and its exclusion
of old attempts will be verified when metrics and reports are available.

## Metric Result Contract

Project metric extractors will emit one JSON document per attempt, following the
versioned [metric-result v1 JSON Schema](schemas/metric-result-v1.schema.json).
See the [example output](config/example-metric-result-v1.json). Metric names and
class names are project-defined; selection metadata and declared artifacts are
optional. A project can freeze an extractor in `.igor/project.toml`:

```toml
[results]
extractor = ["./scripts/export_igor_metrics.py"]
working_directory = "attempt"
```

The program runs directly, without a shell; a project-relative executable is
resolved inside the registered project. Arguments remain literal and will be
interpreted relative to the attempt directory when executed. A successful
attempt enqueues an independent supervisor action. The extractor receives one
JSON line on stdin with only that attempt's IDs and verified stdout/stderr log
paths, and runs with a cleared environment apart from `PATH`. Its JSON output
is validated as v1 and stored durably before the action succeeds; if extraction
exhausts its retries, its action fails while the job and attempt remain
successful. Duplicate JSON keys (even when escaped), malformed or non-finite
numbers, missing required fields and unsupported versions are rejected before
publication. Igor indexes the top-level scalar metrics by their project-defined
names and stores the optional selection split, metric, value, epochs and
checkpoint declaration alongside the original document. Igor registers declared
attempt files with their role, actual byte size and verified SHA-256, then copies
them to a private per-attempt staging directory with state `staged`. For a
successful extractor result, Igor verifies the complete set, atomically renames
its attempt directory under the frozen project `artifact_root`, and marks every
artifact `published` with the metric document in one database transaction. A
partial or failed extractor result keeps declared outputs staged. After an
interruption, consult the database state rather than directory presence to
determine what has been published.

Repeating an identical extraction verifies existing published bytes without
duplicating its document, scalar metrics, selection or registered artifact.
When Telegram delivery occurs after extraction, its message can include up to
ten indexed metrics for that attempt; earlier deliveries do not wait for
extraction.

Run `igor metrics JOB_ID` to see the extraction state, indexed scalar values
and selection for each attempt. `igor metrics JOB_ID --json` returns `job_id`
and an `attempts` array in attempt order. Every entry includes `attempt_id`,
`action_state`, `last_error`, the original `document`, `selection`, and all
indexed scalar `metrics` as `[name, value]` pairs sorted by name. A missing
extraction action has `action_state: null`; a pending or failed action can have
`document: null`. Results remain associated with their own attempt, including
after a job is retried. A nonexistent job returns exit code `8`.

The [extractor fixtures](crates/igor-cli/tests/fixtures/metrics) demonstrate
Python standard-library parsing of the attempt's stdout, a POSIX shell
emitting Julia-compatible JSON without a Julia installation, and a native Rust
binary reading the attempt log. Integration tests compile the native source,
copy the scripts as executables, run each in an isolated attempt and verify
its scalar metrics and published artifact. The shell fixture uses a fixed
sample result for the `training-output` test log.

## Retention Policy Configuration

Project TOML can specify the retention defaults from
[`DESIGN.md`](DESIGN.md#18-retention-and-garbage-collection) in an optional
`[retention]` section and attempt-relative exceptions in `[artifacts]`. For
example:

```toml
[retention]
failed_artifacts = "keep"
failed_log_tail_kb = 256

[artifacts]
keep_on_success = ["metrics.json", "figures/**"]
keep_on_failure = ["crash-dump.json"]
```

Missing fields use the documented defaults. New submissions freeze
`failed_artifacts` and `keep_on_failure` per attempt. After a durable diagnosis,
the supervisor removes registered failed artifact copies from the attempt and
private staging directories by default. `keep` or matching `keep_on_failure`
patterns retain them. Older attempts without a frozen cleanup policy are
preserved.
Artifact records track physical publication (`registered`, `staged`,
`published`, `removed`) separately from their retention lifecycle (`running`,
`failed`, `published`, `retained`, `superseded`, `removed`). The lifecycle is
durable; failed-artifact and supersession cleanup both use it.
Failed artifacts carry a durable diagnosis requirement: Igor refuses to mark
them removed until the attempt has a completed, stored diagnosis. A pending
diagnosis keeps the files protected, even if a retention rule selects them.
For a failed, lost or cancelled attempt, Igor stores only the last
`failed_log_tail_kb` KiB of each stdout/stderr file as binary data in SQLite.
The default is 512 KiB per stream, zero disables capture, and the maximum is
16384 KiB. Missing or unreadable files yield an incomplete record, so
cleanup preserves full logs while capture remains incomplete. The
per-attempt limit is frozen when the job is submitted.

Failed-artifact cleanup checks paths, size and SHA-256 before removing any file
in a batch and preserves a database tombstone with a cleanup event. It waits
for active extraction to finish and for complete log tails on failed, lost or
cancelled attempts. Hidden or final publication copies defer this artifact
cleanup. Full logs and owned private outputs use their separate verified
retirement operations described below.

Registered repair worktrees have a separate cleanup lifecycle. The supervisor
can remove an explicitly released worktree only after its recovery is terminal,
its patch and versioned diagnosis/validation records are durable in SQLite,
and no repair action or agent session still needs it. Candidate commits are
preserved under `refs/igor/repairs/RECOVERY_ID` before removal. The frozen
`delete_repair_worktrees = false` policy keeps the registered worktree instead.
Worktree creation and repair-agent execution remain planned for Milestone 16.

Supersession preserves existing published bytes and marks their lifecycle
`superseded`. Extraction finishing after supersession uses that same marker.
Retirement of artifacts from superseded jobs is blocked while the family's
current replacement generation is incomplete, failed or non-comparable; an
older successful replacement cannot bypass a newer pending generation. Failed
artifact cleanup applies this barrier before touching files. New submissions
also freeze `superseded_artifacts` and `keep_on_success`. After the current
replacement is comparable, the supervisor removes superseded published files
and their verified source/staging copies by default. `keep` or matching
`keep_on_success` patterns retain them; legacy attempts without this snapshot
are preserved. Cleanup records durable intent before removing files and keeps
the artifact identity and history in a tombstone. If a newer replacement becomes
incomplete during recovery, remaining copies are preserved; an already completed
physical removal can still finish its tombstone without further deletion.

Successful logs are compressed to gzip when the frozen per-attempt
`compress_success_logs` policy is true. The supervisor waits for extraction
and active diagnosis/repair consumers, and for live process-group writers to
finish. It publishes a verified archive and durable metadata before removing
the raw file. `igor logs` and `igor logs --follow` continue to emit the original
bytes, transparently reading gzip when needed. Explicit `false` and older
attempts without the policy snapshot preserve raw logs. Failed logs are not
compressed by this operation.

Cleanup checks the complete absolute directory chain, including ancestors above
configured roots, and refuses symlinks or traversal spellings. Artifact paths
must be unambiguous relative paths; interior `.` and repeated/trailing
separators are rejected before retention matching. Failed-artifact cleanup also
requires a registered project and a publication root inside it. These checks
apply to publication-temporary housekeeping as well as artifact and log cleanup.

Failed-artifact cleanup runs through a durable action outbox with leases,
retries and a stable per-attempt idempotency key. Waiting for diagnosis,
extraction or replacement does not consume the operational retry budget.
Removal first commits immutable copy identities; missing files are accepted on
replay only with that prior intent. Cleanup operations publish versioned,
immutable tombstones alongside their terminal state and event, preserving
identity, outcome and completion time across restart or acknowledgement retries.

## Storage Inventory

```bash
igor storage status
igor storage status --json
```

This read-only command needs only the worker. Inside a registered project it
reports that project's records; outside a project it reports globally, including
history from unregistered projects. `--project` selects a project explicitly.

The inventory groups artifacts by publication/lifecycle state, with known
logical bytes and unknown-size counts; removed artifacts are separate history.
It also reports log archive identities, pending compression reservations,
failed-log BLOB payloads, repair-worktree states/patch payloads, failed-cleanup
actions, intents and tombstones. Each artifact is counted once regardless of
the number of copy paths in metadata. It neither claims cleanup work nor scans
directories: actual disk footprint and unfinished temporary sizes are not
measured and appear as `null` in JSON.

## Garbage Collection

```bash
igor gc --dry-run
igor gc --dry-run --json
igor gc
igor gc --failed
igor gc --failed --yes --json
```

The worker-only preview follows the same project/global scope as storage status.
It shows verified target paths and effects for failed/superseded artifacts,
released repair worktrees, failed-log retirement, owned private failed outputs
and successful-log compression. Entries distinguish
ready, retained, deferred, unsafe and metadata-only operations with reasons.

Dry-run uses a read-only database snapshot and filesystem observations. It
does not enqueue actions, claim leases, prepare intents, create/reclaim temporary
files, compress logs or remove anything. The bounded preview covers up to
32 subjects per operation and 64 artifact rows or actionable private output
files per attempt, explicitly reporting
truncation and continuation markers. Known copy bytes are deduplicated by path;
gzip savings and worktree size remain unknown. The result is advisory: execution
must revalidate eligibility and bytes.

When eligibility and file identities remain unchanged, executing the saved
worklist produces the advertised effects. Metadata-only completion does not
remove bytes. Successful-log compression publishes the verified `.log.gz`
counterpart and retires the raw log; private temporary names and final gzip
size are determined during execution. Known copy bytes describe observed or
recorded targets, not predicted space savings.

Without `--dry-run`, Igor displays the bounded selected work and asks for
confirmation, defaulting to no. `--yes` approves noninteractively. Decline or
EOF sends no execution request, and dry-run never prompts. Execution processes
one fixed worklist: newly discovered or newly ready items are not added while
waiting for confirmation, and every selected item is revalidated under the
executor's writer reservation. `--failed` selects registered failed-artifact
retention, failed full-log retirement and owned private failed outputs.

Full failed logs use the retention policy frozen at submission. With
`failed_full_logs = "delete-after-diagnosis"`, retirement waits for durable
diagnosis, complete verified binary tails, quiescent writers and all extraction,
repair and replacement-generation barriers. `keep` and legacy attempts without
a frozen policy retain their full logs. The supervisor periodically retries
eligible retirement; worker-only installations can invoke `igor gc --failed`.
After retirement, `igor logs` reads the stored tail in bounded chunks, reports
the omitted prefix on stderr and preserves original offsets with `--follow`.

Private disposable outputs use frozen `failed_artifacts` and `keep_on_failure`
policies. Only a fresh, empty worker-created `logs/ATTEMPT_UUID` directory with
durably recorded ownership before launch is eligible. Discovery considers only
immediate regular-file children, excluding registered artifacts, exact log/archive
names, durably reserved compression temporary paths and `.git` boundaries.
Dotfiles and general `.tmp`/`.gz` outputs remain eligible. It never scans the checkout or external data and
never removes the directory itself. Preexisting directories and legacy attempts
without frozen ownership remain preserved. Diagnosis, complete tails, consumers,
writer quiescence and current comparable replacement barriers apply to both
executor phases. Exact root/file device, inode, path and digest identities bind
consent and crash replay; symlinks, hardlinks and special files are unsafe.
The supervisor retries bounded passes; explicit GC keeps a fixed per-file
worklist. Storage status reports immutable manifest bytes and intent/history
states, with physical file sizes left unmeasured.
Directory inventory reads at most 4097 entries; more than 4096 children defer the
namespace explicitly. Protected files and preserved directories do not consume
the 64-file actionable page. Once only protected entries remain, a separate
metadata-only GC selection binds the exact namespace snapshot and durably closes
discovery. Completed discovery is excluded from future candidate pages, including
namespaces containing only kept files; new files are not swept after closure.

With `--json`, stdout contains only the structured result; prompts and summaries
go to stderr. Completed, already-done, deferred, unsafe and error counts describe
partial outcomes. Errors or unsafe entries return exit code `1` after emitting
the result. Verified retain decisions can count as completed without deleting
files or creating a lifecycle record. Entries rejected as unsafe in the preview
are reported but never selected for execution; an unsafe-only plan reports them
without prompting or sending an execution request. Explicitly declining a
nonempty worklist leaves an empty, unconfirmed result. Truncated work is not
drained automatically; later invocations can
preview another bounded pass.

Interrupted cleanup resumes from durable intents. Recovery revalidates the
selected identities and current barriers, completes metadata for already-retired
copies only when prior evidence matches, and preserves unselected files and
protected Git refs. Retirement keeps one tombstone and cleanup event per subject;
metadata-only namespace closure creates no removal evidence. Expired maintenance
claims are recovered with a new lease; stale claims
cannot resume the work.

## Database Integrity

Check an existing Igor database without starting a daemon or applying migrations:

```bash
igor db check
igor db check --json
igor --database /path/to/igor.sqlite3 db check
igor db backup /path/to/backup.sqlite3
igor db migrate --dry-run
igor db migrate
```

`igor db check` opens the database read-only and verifies the full SQLite integrity
check, foreign keys, and the expected schema version. It exits unsuccessfully
when the database is missing or a check fails; it never creates or upgrades a
database. After an outdated-schema result, starting the worker applies available
migrations through the existing startup path. `igor db migrate` applies available
migrations explicitly to an existing database after checking its integrity and
foreign keys; it also rechecks both after migration. Neither command creates a
missing source database. Back up before migrating: `igor db backup` uses SQLite's
online backup API, verifies the copy's integrity, and refuses to overwrite an
existing destination or the source. `--dry-run` previews migration after
validating the existing database; migration and backup accept `--json`.

## Exit Status

| Code | Meaning |
| --- | --- |
| `0` | Command completed successfully. |
| `1` | Other command failure, including failed local database checks or `igor wait` on a job that did not succeed. |
| `2` | Invalid CLI syntax or arguments (Clap). |
| `3` | Worker/supervisor unavailable or request timed out. |
| `4` | Daemon protocol version mismatch. |
| `5` | Daemon rejected an invalid request. |
| `6` | Daemon database unavailable. |
| `7` | Daemon internal error. |
| `8` | Requested entity not found. |
| `9` | Request conflicts with existing state. |

Codes `3`–`9` apply to structured daemon-client errors; locally detected
failures currently use `1`. `--json` commands produce JSON on success and
report failures to stderr with a nonzero exit status.

## Compatibility And Upgrades

| Contract | Version in this build | Compatibility behavior |
| --- | --- | --- |
| Worker/supervisor Unix-socket protocol | `19` | CLI and daemon must use the same version; mismatches are rejected (exit `4`). |
| Global and project TOML | `1` each | The reader rejects unsupported `schema_version` values; it does not rewrite these files on read. |
| Job and family TOML | `1` each | Submission rejects unsupported versions before creating jobs. |
| Event payloads | `1` | Unsupported versions are rejected when decoded. |
| SQLite schema | `21` | Numbered, checksummed SQLx migrations run on writable worker startup or explicit `igor db migrate`. |

Upgrade the CLI and both user services together; restart the services so they
run the new binary (`igor service restart` if installed). Before a database
upgrade, make an online backup with `igor db backup DESTINATION`; inspect an
existing database with `igor db check`. `igor db migrate` applies available
SQLite migrations to an existing database, but does not migrate TOML files or
provide a downgrade path. If a configuration or payload reports an unsupported
version, use a compatible Igor version or explicitly convert that document;
do not change `schema_version` alone. Keep the original database backup when
testing a new release. The protocol version changes when its wire contract
changes; persisted schema versions are independent of the protocol version.

## Security Model

Igor targets a **trusted single-user Linux account**. The CLI, worker,
supervisor, experiments and any explicitly invoked external tools run with that
account's access. User sockets have mode `0600` and check the connecting UID;
this is not authorization between processes of the same user. Do not share the
account or its sockets with untrusted workloads. Neither direct processes,
`systemd --user` units nor Docker containers are treated as a sandbox.

- Direct commands use argument vectors. Shell execution requires an explicit
  shell submission; its contents run with the user's privileges. Docker uses
  the local daemon and explicit bind mounts: a writable mount authorizes writes
  to that host path, including paths outside the Igor project. Images are
  inspected locally and never pulled implicitly; pin a digest when exact image
  identity matters.
- Telegram credentials belong in the user config, not the project.
  `igor notify setup` reads the token from stdin and saves the file with mode `0600`;
  `igor config show` redacts it. Treat the private config and database backups as
  sensitive files. Job environment variables with recognized secret-like names
  are rejected; inherited process variables are filtered by name, so a secret
  under an innocuous name can still pass through `inherit = "all"`. Prefer the
  default minimal environment and review explicitly declared values.
- Declared project input identities are hashed at submission and checked against
  the project root; symlinked declared files are rejected. Docker mount sources
  are an explicit exception to that root boundary. Validate file and mount
  paths before running third-party experiments; their stdout, stderr, and
  artifacts can contain sensitive data accessible to the same account.
- General-purpose hooks and coding-agent execution are planned, not part of the
  current runner. When introduced, hooks and agents will be external code under
  the user's privileges; their configuration, context, filesystem access and
  cleanup must be reviewed separately rather than assumed isolated.

See `DESIGN.md` for the corresponding executor, session, and path contracts.

## Development

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --no-deps
```

Ordinary tests use a fake Docker CLI and require no Docker installation. The
real-Docker lifecycle test is opt-in, uses an already-local disposable CPU
image, and never pulls:

```bash
docker pull alpine:3.20
IGOR_RUN_DOCKER_TESTS=1 \
IGOR_TEST_DOCKER_IMAGE=alpine:3.20 \
cargo test -p igor-cli --test job_cli \
  opt_in_real_docker_jobs_cover_worker_lifecycle -- --nocapture
```

The test prints an explicit skip reason when opt-in is disabled, the CLI or
daemon is unavailable, the image is absent, or the image has no `/bin/sh` or
canonical repository digest.

The live user-systemd test is also opt-in. It requires a running user manager,
skips if Igor units or active sockets already exist, and removes the disposable
units after testing install, enable, restart with an active Process job, disable,
and uninstall:

```bash
IGOR_RUN_SYSTEMD_TESTS=1 cargo test -p igor-cli --test service_cli \
  live_user_systemd_lifecycle_is_opt_in -- --nocapture
```

Inspect the CLI:

```bash
cargo run -p igor-cli -- --version
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your
option.
