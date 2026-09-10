# Igor Implementation Tasks

This checklist turns [`DESIGN.md`](DESIGN.md) into ordered, verifiable work.
Tasks are arranged by dependency rather than by document section. Each milestone
must finish its acceptance checks before work starts on a dependent milestone.

## Working Rules

- Keep Sleep_CNN's current queue and services untouched until legacy job `#63`
  and its curator actions have finished.
- Build and test Igor entirely in this repository with disposable databases,
  processes, containers, units, and project fixtures.
- Prefer the smallest implementation that satisfies the current milestone.
- Do not add deferred features from `DESIGN.md` to the MVP.
- Add every SQLite change through a numbered migration.
- Add tests with each behavior rather than postponing tests to the end.
- Run formatting, Clippy, and relevant tests before completing a milestone.
- Record material design changes in `DESIGN.md` before implementing them.
- Never use a real research dataset or active GPU experiment in automated tests.

## Delivery Map

| Release | Milestones | Usable outcome |
|---|---|---|
| `v0.1` | 0-6 | Persistent generic process queue with CLI and recovery |
| `v0.2` | 7-10 | Docker, GPU scheduling, systemd, supervisor, Telegram |
| `v0.3` | 11-14 | Families, metrics, retention, and Markdown reports |
| `v0.4` | 15-16 | Ephemeral coding agents and assisted family repair |
| `v1.0` | 17-18 | Hardening, release packaging, and Sleep_CNN migration |

## Milestone 0: Repository Foundation

### Tasks

- [x] `M0.01` Initialize a Git repository using `main` as the initial branch.
- [x] `M0.02` Create the root Cargo workspace.
- [x] `M0.03` Create `igor-core` as a library crate.
- [x] `M0.04` Create `igor-daemon` as a library and binary crate.
- [x] `M0.05` Create `igor-cli` with the `igor` binary.
- [x] `M0.06` Add a pinned stable toolchain compatible with Rust `1.97.1`.
- [x] `M0.07` Add workspace-wide package metadata and dependency versions.
- [x] `M0.08` Add `LICENSE-MIT` and `LICENSE-APACHE`.
- [x] `M0.09` Add a concise `README.md` with project status and scope.
- [x] `M0.10` Add `.gitignore` entries for Cargo, local state, fixtures, logs,
  temporary worktrees, generated reports, and editor artifacts.
- [x] `M0.11` Configure workspace Rust lints without suppressing warnings.
- [x] `M0.12` Add a basic tracing initialization shared by both binaries.
- [x] `M0.13` Add a CI workflow for format, Clippy, tests, and documentation.
- [x] `M0.14` Add a contribution command documenting the local quality gates.

### Acceptance

- [x] `A0.01` `cargo fmt --check` succeeds.
- [x] `A0.02` `cargo clippy --workspace --all-targets -- -D warnings` succeeds.
- [x] `A0.03` `cargo test --workspace` succeeds.
- [x] `A0.04` `cargo run -p igor-cli -- --version` prints the workspace version.
- [ ] `A0.05` CI runs the same checks on a clean checkout.

## Milestone 1: Core Contracts And State Machines

Dependencies: milestone 0.

### Tasks

- [ ] `M1.01` Define strongly typed identifiers for projects, families,
  generations, jobs, attempts, events, actions, deliveries, recoveries,
  reports, resources, and agent sessions.
- [ ] `M1.02` Define the project model and versioned project configuration.
- [ ] `M1.03` Define a command specification containing program, argument vector,
  working directory, shell policy, and environment policy.
- [ ] `M1.04` Define direct-process and Docker executor specifications without
  implementing either executor.
- [ ] `M1.05` Define resource requests with unrestricted CPU and memory defaults.
- [ ] `M1.06` Define `exclusive-host`, exclusive GPU, and named-resource modes.
- [ ] `M1.07` Define job and immutable attempt specifications.
- [ ] `M1.08` Define family membership, seeds, and generation identity.
- [ ] `M1.09` Define job, attempt, action, delivery, recovery, report, and cleanup
  state enums.
- [ ] `M1.10` Define valid state transitions as domain operations.
- [ ] `M1.11` Define event kinds and versioned event payloads.
- [ ] `M1.12` Define retry policies for attempts, actions, and deliveries.
- [ ] `M1.13` Define the no-timeout default and optional explicit timeout.
- [ ] `M1.14` Define artifact roles and retention decisions.
- [ ] `M1.15` Define structured error categories and stable public error codes.
- [ ] `M1.16` Add Serde round-trip tests for every persisted contract.
- [ ] `M1.17` Add transition-table tests covering every legal and illegal state
  change.
- [ ] `M1.18` Add compatibility tests that reject unsupported config and payload
  versions with actionable errors.

### Acceptance

- [ ] `A1.01` Domain types do not depend on Clap, SQLx, systemd, or terminal
  presentation.
- [ ] `A1.02` A job and attempt specification can round-trip through JSON without
  changing argument boundaries or defaults.
- [ ] `A1.03` CPU, memory, and timeout are unrestricted when omitted.
- [ ] `A1.04` Invalid state transitions fail before reaching persistence.

## Milestone 2: Configuration And XDG Paths

Dependencies: milestone 1.

### Tasks

- [ ] `M2.01` Implement XDG config, state, runtime, log, worktree, and report
  path discovery.
- [ ] `M2.02` Implement deterministic fallback paths when XDG variables are
  absent.
- [ ] `M2.03` Define the global `config.toml` schema and defaults.
- [ ] `M2.04` Define the project `igor.toml` schema and defaults.
- [ ] `M2.05` Implement precedence for CLI options, `IGOR_*` variables, selected
  config file, and built-in defaults.
- [ ] `M2.06` Resolve relative project paths against the configuration file that
  declares them.
- [ ] `M2.07` Validate that artifact and cleanup roots cannot escape declared
  project locations.
- [ ] `M2.08` Implement `igor config path`.
- [ ] `M2.09` Implement `igor config show` with secret redaction.
- [ ] `M2.10` Implement `igor config check`.
- [ ] `M2.11` Add fixtures for valid, minimal, complete, and invalid configs.
- [ ] `M2.12` Generate an example global config and example project config.

### Acceptance

- [ ] `A2.01` Tests run under disposable XDG directories without touching the
  user's real configuration.
- [ ] `A2.02` A minimal config produces unrestricted compute and no timeout.
- [ ] `A2.03` Invalid paths, versions, and enum values report their exact config
  location.
- [ ] `A2.04` Secret values never appear in `config show` or validation errors.

## Milestone 3: SQLite Foundation And Repositories

Dependencies: milestones 1-2.

### Tasks

- [ ] `M3.01` Add SQLx with SQLite and compile-time migration support.
- [ ] `M3.02` Create migration `0001` for `schema_migrations`, `projects`,
  `families`, `generations`, `jobs`, and `attempts`.
- [ ] `M3.03` Create migration `0002` for `events`, `resources`, and
  `resource_leases`.
- [ ] `M3.04` Create migration `0003` for `actions`, `metrics`, `artifacts`,
  `deliveries`, `recoveries`, `report_runs`, and `agent_sessions`.
- [ ] `M3.05` Add state constraints and foreign keys to all tables.
- [ ] `M3.06` Add indexes for queue ordering, attempt lookup, event streams,
  leases, pending actions, and pending deliveries.
- [ ] `M3.07` Enable WAL, foreign keys, and a bounded busy timeout on every
  writable connection.
- [ ] `M3.08` Implement transactional migration startup.
- [ ] `M3.09` Implement project repository operations.
- [ ] `M3.10` Implement family and generation repository operations.
- [ ] `M3.11` Implement job and attempt repository operations.
- [ ] `M3.12` Implement append-only event repository operations.
- [ ] `M3.13` Implement action and delivery transactional claims.
- [ ] `M3.14` Implement resource and lease repository operations.
- [ ] `M3.15` Ensure each lifecycle transition and its event are committed in one
  transaction.
- [ ] `M3.16` Implement database integrity checking.
- [ ] `M3.17` Implement consistent backup through SQLite's backup API.
- [ ] `M3.18` Add concurrent-claim and busy-database integration tests.
- [ ] `M3.19` Add migration tests from an empty database and versioned fixtures.
- [ ] `M3.20` Add tests proving foreign-key enforcement on every connection.

### Acceptance

- [ ] `A3.01` Two claimers cannot acquire the same job, action, delivery, or
  resource lease.
- [ ] `A3.02` A failed transaction leaves neither a state change nor a partial
  event.
- [ ] `A3.03` A backup taken while writes occur passes integrity checks.
- [ ] `A3.04` Every schema change is represented by a migration.

## Milestone 4: Local Protocol And Daemon Skeleton

Dependencies: milestone 3.

### Tasks

- [ ] `M4.01` Define a versioned request and response protocol for the Unix
  socket.
- [ ] `M4.02` Define stable protocol errors and client exit-code mapping.
- [ ] `M4.03` Implement socket creation under `$XDG_RUNTIME_DIR/igor`.
- [ ] `M4.04` Restrict the socket to the current user.
- [ ] `M4.05` Reject incompatible protocol versions.
- [ ] `M4.06` Implement worker startup, shutdown, and signal handling.
- [ ] `M4.07` Implement supervisor startup, shutdown, and signal handling.
- [ ] `M4.08` Implement health, version, and database-status requests.
- [ ] `M4.09` Implement a reusable CLI client.
- [ ] `M4.10` Implement `igor daemon health`.
- [ ] `M4.11` Implement `igor daemon status`.
- [ ] `M4.12` Add graceful cleanup of stale socket files after verified dead
  owners.
- [ ] `M4.13` Add integration tests with disposable worker and supervisor
  processes.

### Acceptance

- [ ] `A4.01` An unprivileged second user cannot use the socket.
- [ ] `A4.02` Abrupt daemon termination does not prevent a clean restart.
- [ ] `A4.03` CLI errors distinguish unavailable daemon, protocol mismatch, and
  invalid request.

## Milestone 5: Submission And Inspection CLI

Dependencies: milestone 4.

### Tasks

- [ ] `M5.01` Implement `igor project add PATH`.
- [ ] `M5.02` Implement `igor project list`.
- [ ] `M5.03` Implement generic `igor submit -- PROGRAM ARG...`.
- [ ] `M5.04` Implement explicit `igor submit --shell COMMAND`.
- [ ] `M5.05` Implement `igor submit --file job.toml`.
- [ ] `M5.06` Resolve and freeze the effective job specification at submission.
- [ ] `M5.07` Capture Git repository identity, branch, revision, and dirty state.
- [ ] `M5.08` Hash declared scientific configurations and immutable inputs.
- [ ] `M5.09` Require explicit policy when submitting a dirty Git worktree.
- [ ] `M5.10` Implement priority and human-readable job names.
- [ ] `M5.11` Implement `igor list`.
- [ ] `M5.12` Implement `igor show JOB_ID`.
- [ ] `M5.13` Implement `igor events JOB_ID`.
- [ ] `M5.14` Implement `igor wait JOB_ID`.
- [ ] `M5.15` Add `--json` to every read command.
- [ ] `M5.16` Add CLI tests for spaces, empty arguments, Unicode, and arguments
  beginning with `-`.
- [ ] `M5.17` Add tests proving direct submission never invokes a shell.

### Acceptance

- [ ] `A5.01` Python, Julia, Rust, and shell examples serialize to the same
  language-neutral job contract.
- [ ] `A5.02` Attempt specifications remain unchanged when project config or Git
  HEAD changes after submission.
- [ ] `A5.03` Human and JSON output expose the same state and identifiers.

## Milestone 6: Direct Process Worker

Dependencies: milestone 5.

### Tasks

- [ ] `M6.01` Implement atomic priority and FIFO job selection.
- [ ] `M6.02` Create an immutable attempt before launching a process.
- [ ] `M6.03` Implement the direct-process executor with argument vectors.
- [ ] `M6.04` Launch attempts in dedicated process groups.
- [ ] `M6.05` Implement minimal environment inheritance and explicit additions
  and removals.
- [ ] `M6.06` Strip Igor, Telegram, agent, and unrelated secret variables from
  child environments.
- [ ] `M6.07` Capture stdout and stderr into attempt-specific logs.
- [ ] `M6.08` Stream logs without blocking process supervision.
- [ ] `M6.09` Persist PID, process start identity, start time, and heartbeat.
- [ ] `M6.10` Record normal exit, nonzero exit, and terminating signal.
- [ ] `M6.11` Implement manual cancellation with `SIGTERM` and configurable
  grace period before `SIGKILL`.
- [ ] `M6.12` Implement `igor cancel JOB_ID`.
- [ ] `M6.13` Implement `igor retry JOB_ID` without deleting prior attempts.
- [ ] `M6.14` Implement `igor logs JOB_ID`.
- [ ] `M6.15` Implement `igor logs --follow JOB_ID`.
- [ ] `M6.16` Reconcile running attempts after worker restart.
- [ ] `M6.17` Detect PID reuse before reattaching to a process.
- [ ] `M6.18` Classify unrecoverable missing processes as `lost`.
- [ ] `M6.19` Ensure no timeout is applied unless explicitly configured.
- [ ] `M6.20` Add process fixtures for success, failure, sleep, signal handling,
  large output, binary output, and child processes.
- [ ] `M6.21` Add restart and cancellation integration tests.

### Acceptance

- [ ] `A6.01` A command can run longer than a test-configured observation period
  without being killed when timeout is absent.
- [ ] `A6.02` Worker restart preserves or correctly classifies the active
  attempt.
- [ ] `A6.03` Cancellation terminates the complete process group.
- [ ] `A6.04` A retry creates a new attempt and preserves the failed one.
- [ ] `A6.05` This milestone forms the first usable `v0.1` process queue.

## Milestone 7: Resource Scheduling

Dependencies: milestone 6.

### Tasks

- [ ] `M7.01` Discover host CPU count and total memory for diagnostics only.
- [ ] `M7.02` Discover NVIDIA GPUs when available without making NVIDIA a core
  requirement.
- [ ] `M7.03` Register host, GPU, and named resources in SQLite.
- [ ] `M7.04` Implement `exclusive-host` as the default for unknown jobs.
- [ ] `M7.05` Implement exclusive allocation of a specific GPU.
- [ ] `M7.06` Set the assigned device in `CUDA_VISIBLE_DEVICES` for direct jobs.
- [ ] `M7.07` Implement named exclusive resources.
- [ ] `M7.08` Implement lease heartbeats and expiry reconciliation.
- [ ] `M7.09` Reserve all required resources atomically before creating a running
  attempt.
- [ ] `M7.10` Release leases on every terminal transition and failed launch.
- [ ] `M7.11` Implement priority aging without reordering equal effective
  priorities unpredictably.
- [ ] `M7.12` Implement `igor resources` and JSON output.
- [ ] `M7.13` Add simulated multi-GPU and named-resource tests.
- [ ] `M7.14` Add tests proving CPU and memory discovery does not impose limits.

### Acceptance

- [ ] `A7.01` Two exclusive-host jobs never run concurrently.
- [ ] `A7.02` Two jobs never receive the same exclusive GPU lease.
- [ ] `A7.03` A job can use all host CPU and memory by default.
- [ ] `A7.04` Stale leases recover without allowing two live owners.

## Milestone 8: Docker Executor

Dependencies: milestones 6-7.

### Tasks

- [ ] `M8.01` Implement Docker capability detection.
- [ ] `M8.02` Validate and resolve configured image references and digests.
- [ ] `M8.03` Build `docker create` arguments structurally without shell parsing.
- [ ] `M8.04` Implement configured read-only and read-write mounts.
- [ ] `M8.05` Pass only the minimal configured environment.
- [ ] `M8.06` Attach the assigned GPU using a concrete device selection.
- [ ] `M8.07` Label containers with project, job, attempt, and generation IDs.
- [ ] `M8.08` Persist container ID before declaring the attempt running.
- [ ] `M8.09` Capture Docker logs and exit status.
- [ ] `M8.10` Implement graceful Docker cancellation.
- [ ] `M8.11` Recover by container ID and use labels only as a fallback.
- [ ] `M8.12` Distinguish missing daemon, missing image, OOM, cancelled, and
  application failures.
- [ ] `M8.13` Remove disposable containers idempotently after finalization.
- [ ] `M8.14` Add tests for command construction without requiring Docker.
- [ ] `M8.15` Add opt-in integration tests using disposable CPU containers.

### Acceptance

- [ ] `A8.01` Docker remains optional for process-only installations.
- [ ] `A8.02` A daemon restart can recover a live container.
- [ ] `A8.03` Docker jobs cannot inherit Telegram or coding-agent credentials.
- [ ] `A8.04` A configured image digest is checked before launch.

## Milestone 9: systemd Installation And Runtime Integration

Dependencies: milestones 6-8.

### Tasks

- [ ] `M9.01` Design worker and supervisor user units without project paths.
- [ ] `M9.02` Generate units using the installed binary and XDG locations.
- [ ] `M9.03` Configure deliberate restart and shutdown behavior.
- [ ] `M9.04` Give supervisor background work low CPU and I/O priority without
  limiting experiment resources.
- [ ] `M9.05` Implement `igor service install --user`.
- [ ] `M9.06` Implement install flags `--enable` and `--start`.
- [ ] `M9.07` Implement `igor service enable [--now]`.
- [ ] `M9.08` Implement `igor service disable [--now]`.
- [ ] `M9.09` Implement service start, stop, and restart.
- [ ] `M9.10` Implement `igor service status`.
- [ ] `M9.11` Implement `igor service logs`.
- [ ] `M9.12` Implement `igor service uninstall --user` with explicit
  confirmation.
- [ ] `M9.13` Ensure service commands never invoke `sudo` implicitly.
- [ ] `M9.14` Validate generated units with `systemd-analyze --user verify`.
- [ ] `M9.15` Add unit snapshot tests and opt-in live user-systemd tests.
- [ ] `M9.16` Evaluate transient user units for direct attempts and implement
  them if recovery is more reliable than process groups.
- [ ] `M9.17` Preserve the process-group backend as a portable fallback.

### Acceptance

- [ ] `A9.01` A fresh user can install, enable, start, inspect, disable, and
  uninstall both services using only `igor` commands.
- [ ] `A9.02` Unit installation is idempotent.
- [ ] `A9.03` Restarting Igor services does not cancel a recoverable active
  attempt.

## Milestone 10: Supervisor Actions And Telegram

Dependencies: milestones 3-4 and 9.

### Tasks

- [ ] `M10.01` Implement supervisor action discovery and transactional claims.
- [ ] `M10.02` Implement action leases, heartbeat, retries, and recovery.
- [ ] `M10.03` Implement bounded exponential backoff utilities.
- [ ] `M10.04` Create terminal job notifications transactionally with attempt
  finalization.
- [ ] `M10.05` Implement Telegram configuration outside project files.
- [ ] `M10.06` Implement Telegram setup and credential validation.
- [ ] `M10.07` Implement `igor notify test`.
- [ ] `M10.08` Implement persistent delivery claims and retry scheduling.
- [ ] `M10.09` Redact bot tokens from URLs, errors, logs, and diagnostics.
- [ ] `M10.10` Include useful status, duration, family progress, and available
  metrics before technical paths.
- [ ] `M10.11` Document at-least-once delivery and possible duplicates.
- [ ] `M10.12` Ensure supervisor failure cannot block worker scheduling.
- [ ] `M10.13` Add mocked network, rate-limit, timeout, duplicate, and restart
  tests.

### Acceptance

- [ ] `A10.01` An unavailable Telegram endpoint does not delay the next job.
- [ ] `A10.02` Delivery resumes after supervisor restart.
- [ ] `A10.03` No stored error or child environment contains the Telegram token.
- [ ] `A10.04` This milestone completes the daily-operation `v0.2` baseline.

## Milestone 11: Families, Generations, And Comparable Seeds

Dependencies: milestones 3, 5, and 7.

### Tasks

- [ ] `M11.01` Define the versioned `family.toml` format.
- [ ] `M11.02` Represent required members and seed-specific argument or config
  substitutions without shell templates.
- [ ] `M11.03` Freeze one Git revision and protocol identity per generation.
- [ ] `M11.04` Validate that every member uses the same generation invariants.
- [ ] `M11.05` Implement `igor family submit --file family.toml`.
- [ ] `M11.06` Insert the family, generation, jobs, and initial events in one
  transaction.
- [ ] `M11.07` Implement `igor family show FAMILY_ID`.
- [ ] `M11.08` Report pending, running, failed, succeeded, and superseded member
  counts.
- [ ] `M11.09` Define when a generation is complete and comparable.
- [ ] `M11.10` Prevent aggregation across revisions or generation IDs.
- [ ] `M11.11` Permit transient retries within the same generation.
- [ ] `M11.12` Implement superseding a generation without deleting its database
  history.
- [ ] `M11.13` Add tests for complete, partial, failed, repaired, and mixed
  generation scenarios.

### Acceptance

- [ ] `A11.01` Five configured seeds run against one immutable generation.
- [ ] `A11.02` A mixed-revision family cannot be marked comparable.
- [ ] `A11.03` Superseded attempts are never selected for the current aggregate.

## Milestone 12: Metrics And Artifact Publication

Dependencies: milestones 10-11.

### Tasks

- [ ] `M12.01` Publish the versioned metric-result JSON Schema.
- [ ] `M12.02` Define project extractor configuration as program and arguments.
- [ ] `M12.03` Run the extractor as an independent supervisor action.
- [ ] `M12.04` Provide only declared attempt paths and metadata to the extractor.
- [ ] `M12.05` Validate extractor JSON and schema version.
- [ ] `M12.06` Store the original structured result.
- [ ] `M12.07` Index scalar metrics without assuming scientific names.
- [ ] `M12.08` Store selection split, metric, value, and checkpoint metadata.
- [ ] `M12.09` Register artifact path, role, size, digest, and publication state.
- [ ] `M12.10` Stage outputs in attempt-specific directories.
- [ ] `M12.11` Publish declared successful artifacts atomically.
- [ ] `M12.12` Ensure extractor failure does not change experiment success.
- [ ] `M12.13` Implement `igor metrics JOB_ID` and JSON output.
- [ ] `M12.14` Add fixture extractors in Python, Julia-compatible shell output,
  and native test binaries.
- [ ] `M12.15` Add malformed, missing, duplicate, non-finite, and unsupported
  schema tests.

### Acceptance

- [ ] `A12.01` Igor processes project metrics without knowing their names.
- [ ] `A12.02` Repeating extraction is idempotent.
- [ ] `A12.03` Partial outputs cannot be mistaken for published artifacts.
- [ ] `A12.04` Telegram can include indexed metrics after extraction succeeds.

## Milestone 13: Retention, Storage, And Garbage Collection

Dependencies: milestone 12.

### Tasks

- [ ] `M13.01` Implement retention policy parsing and effective defaults.
- [ ] `M13.02` Track running, failed, published, retained, superseded, and
  removed artifact states.
- [ ] `M13.03` Preserve failed files until required diagnosis finishes.
- [ ] `M13.04` Preserve only the configured failed-log tail in SQLite.
- [ ] `M13.05` Delete failed large artifacts after diagnosis by default.
- [ ] `M13.06` Delete repair worktrees after their required records and patches
  are durable.
- [ ] `M13.07` Keep superseded artifacts until a replacement generation
  succeeds.
- [ ] `M13.08` Delete superseded artifacts after successful replacement by
  default.
- [ ] `M13.09` Compress successful logs according to policy.
- [ ] `M13.10` Never delete outside registered attempt or artifact roots.
- [ ] `M13.11` Implement idempotent cleanup actions and tombstone metadata.
- [ ] `M13.12` Implement `igor storage status`.
- [ ] `M13.13` Implement `igor gc --dry-run`.
- [ ] `M13.14` Implement confirmed `igor gc` and `igor gc --failed`.
- [ ] `M13.15` Add race tests for diagnosis, publication, replacement, and
  cleanup.
- [ ] `M13.16` Add path traversal and symlink escape tests.

### Acceptance

- [ ] `A13.01` A failed attempt leaves concise history but no large disposable
  files after diagnosis.
- [ ] `A13.02` Dry-run output exactly predicts cleanup effects.
- [ ] `A13.03` Cleanup interruption can be retried safely.

## Milestone 14: Deterministic Markdown Reports

Dependencies: milestones 11-13.

### Tasks

- [ ] `M14.01` Build a normalized report context for one family generation.
- [ ] `M14.02` Include jobs, attempts, metrics, Git identity, configurations,
  repairs, artifacts, and tool versions.
- [ ] `M14.03` Exclude superseded generations from primary aggregates.
- [ ] `M14.04` Implement a deterministic Markdown renderer as the reliable
  baseline.
- [ ] `M14.05` Write reports to a staging file and publish with atomic rename.
- [ ] `M14.06` Implement the default generated `RESULTS.md` mode.
- [ ] `M14.07` Implement the `family-completed` report action trigger.
- [ ] `M14.08` Do not generate a final report for an incomplete generation.
- [ ] `M14.09` Implement `igor report generate FAMILY_ID`.
- [ ] `M14.10` Implement `igor report retry FAMILY_ID`.
- [ ] `M14.11` Persist report context and output hashes.
- [ ] `M14.12` Add golden report tests for complete, failed, repaired, and
  superseded families.
- [ ] `M14.13` Add atomic-publication failure tests.

### Acceptance

- [ ] `A14.01` The same normalized context produces stable Markdown.
- [ ] `A14.02` Handwritten project files are not modified by the default mode.
- [ ] `A14.03` A failed report action cannot damage the previous report.
- [ ] `A14.04` Milestones 11-14 complete the non-agent `v0.3` workflow.

## Milestone 15: OpenCode And Pi Report Generators

Dependencies: milestone 14.

### Tasks

- [ ] `M15.01` Define a coding-agent adapter interface around external
  processes and structured results.
- [ ] `M15.02` Load a versioned project report prompt.
- [ ] `M15.03` Build an isolated report-context directory.
- [ ] `M15.04` Ensure the context excludes secrets and unrelated project files.
- [ ] `M15.05` Implement Pi print-mode invocation with `--no-session`.
- [ ] `M15.06` Implement OpenCode server/API session creation and prompting.
- [ ] `M15.07` Title OpenCode sessions with stable `igor:<action>:<id>` IDs.
- [ ] `M15.08` Delete OpenCode sessions after success and failure.
- [ ] `M15.09` Persist unfinished session cleanup before invoking OpenCode.
- [ ] `M15.10` Clean stale Igor OpenCode sessions after supervisor restart.
- [ ] `M15.11` Implement the OpenCode CLI fallback with JSON session-ID parsing.
- [ ] `M15.12` Capture agent output without exposing the final report path for
  direct modification.
- [ ] `M15.13` Validate agent-produced Markdown before atomic publication.
- [ ] `M15.14` Persist provider, model, prompt, context, and output hashes.
- [ ] `M15.15` Implement generator selection in project configuration.
- [ ] `M15.16` Implement `--generator deterministic|opencode|pi`.
- [ ] `M15.17` Add `igor agents sessions` and cleanup commands.
- [ ] `M15.18` Add fake-agent contract tests and opt-in real-agent smoke tests.

### Acceptance

- [ ] `A15.01` Pi leaves no saved session when invoked by Igor.
- [ ] `A15.02` OpenCode sessions disappear after Igor finishes.
- [ ] `A15.03` A power-loss fixture leaves a cleanup record that succeeds after
  restart.
- [ ] `A15.04` Agent failure preserves the deterministic report and experiment
  state.

## Milestone 16: Assisted Diagnosis And Family Repair

Dependencies: milestones 11, 13, and 15.

### Tasks

- [ ] `M16.01` Define transient infrastructure, deterministic code, resource,
  cancellation, and unknown failure classifications.
- [ ] `M16.02` Implement deterministic classification rules before consulting an
  agent.
- [ ] `M16.03` Define diagnosis and repair prompts separately from report
  prompts.
- [ ] `M16.04` Create repair worktrees at the failed attempt revision.
- [ ] `M16.05` Record the worktree and cleanup action before invoking an agent.
- [ ] `M16.06` Invoke OpenCode or Pi with the failed attempt context and log
  excerpts.
- [ ] `M16.07` Enforce protected scientific configuration and data paths.
- [ ] `M16.08` Reject changes outside declared repairable code paths.
- [ ] `M16.09` Capture and hash the repair diff.
- [ ] `M16.10` Run project-configured validation commands without a default
  timeout.
- [ ] `M16.11` Record validation commands, outputs, and exit statuses.
- [ ] `M16.12` Create an Igor-specific candidate Git revision or immutable
  repaired snapshot.
- [ ] `M16.13` Require approval before applying a code repair.
- [ ] `M16.14` Identify all jobs in the affected family generation.
- [ ] `M16.15` Request approval before cancelling an obsolete active attempt.
- [ ] `M16.16` Gracefully cancel the obsolete attempt after approval.
- [ ] `M16.17` Mark the old generation superseded.
- [ ] `M16.18` Create a new generation containing every required seed.
- [ ] `M16.19` Pin every new member to the same repaired revision and unchanged
  scientific protocol.
- [ ] `M16.20` Implement `igor recovery show JOB_ID`.
- [ ] `M16.21` Implement recovery approve and reject commands.
- [ ] `M16.22` Ensure a transient retry does not create a new generation.
- [ ] `M16.23` Add end-to-end repair tests using a disposable Git repository and
  fake agent.
- [ ] `M16.24` Add tests proving scientific JSON and pending jobs cannot be
  silently modified.

### Acceptance

- [ ] `A16.01` A validated code repair reruns every seed in a new generation.
- [ ] `A16.02` No aggregate mixes old and repaired code.
- [ ] `A16.03` A running obsolete attempt is never cancelled without approval.
- [ ] `A16.04` Repair artifacts are cleaned after durable capture according to
  policy.
- [ ] `A16.05` Milestones 15-16 complete the assisted `v0.4` workflow.

## Milestone 17: Doctor, Hardening, And Release Packaging

Dependencies: milestones 0-16.

### Tasks

- [ ] `M17.01` Implement `igor doctor` checks for config, SQLite, socket,
  systemd, cgroups, Docker, GPUs, agents, Telegram, disk, leases, units,
  containers, worktrees, and sessions.
- [ ] `M17.02` Implement `igor db check`.
- [ ] `M17.03` Implement `igor db migrate`.
- [ ] `M17.04` Implement `igor db backup`.
- [ ] `M17.05` Add stable exit-code documentation.
- [ ] `M17.06` Add protocol and config compatibility documentation.
- [ ] `M17.07` Add security documentation for trusted-user operation, Docker,
  hooks, agents, secrets, and path boundaries.
- [ ] `M17.08` Audit every subprocess environment for secret leakage.
- [ ] `M17.09` Audit every destructive operation for scope validation,
  dry-run, confirmation, and idempotency.
- [ ] `M17.10` Add crash-injection tests around state transitions and filesystem
  publication.
- [ ] `M17.11` Add database corruption and recovery documentation.
- [ ] `M17.12` Add storage-pressure warnings without automatic deletion of
  successful current-generation artifacts.
- [ ] `M17.13` Add shell completions and man-page generation.
- [ ] `M17.14` Produce reproducible release binaries for supported Linux targets.
- [ ] `M17.15` Document installation through release binaries and Cargo.
- [ ] `M17.16` Add release checks for licenses and dependency advisories.
- [ ] `M17.17` Run all quality gates and opt-in integration suites on a clean
  machine or VM.

### Acceptance

- [ ] `A17.01` `igor doctor` distinguishes required failures from optional
  unavailable integrations.
- [ ] `A17.02` A fresh Linux user can install and operate Igor from documented
  steps.
- [ ] `A17.03` No test or diagnostic uses active Sleep_CNN state.
- [ ] `A17.04` Release artifacts include licenses, checksums, and version info.

## Milestone 18: Sleep_CNN Adapter And Migration

Dependencies: milestone 17 and completion of legacy Sleep_CNN job `#63` plus
all terminal curator actions.

### Safety Gate

- [ ] `G18.01` Confirm jobs `#61`, `#62`, and `#63` are terminal.
- [ ] `G18.02` Confirm no Sleep_CNN container is active.
- [ ] `G18.03` Confirm queue and curator outboxes have no pending actions.
- [ ] `G18.04` Freeze new submissions to the legacy queue.
- [ ] `G18.05` Obtain explicit approval before stopping legacy services.

### Tasks

- [ ] `M18.01` Back up both legacy SQLite databases through the SQLite backup
  API rather than copying WAL-mode main files.
- [ ] `M18.02` Back up legacy snapshots, logs, manifests, and service units.
- [ ] `M18.03` Run integrity checks against every database backup.
- [ ] `M18.04` Define and document a versioned legacy export format.
- [ ] `M18.05` Export jobs, attempts, notification state, and curator history.
- [ ] `M18.06` Import legacy records with preserved legacy IDs and source labels.
- [ ] `M18.07` Keep imported legacy history terminal and immutable.
- [ ] `M18.08` Create a Sleep_CNN `igor.toml`.
- [ ] `M18.09` Define Sleep_CNN direct and Docker execution profiles.
- [ ] `M18.10` Define the GNN family and seed-generation contract.
- [ ] `M18.11` Define immutable scientific config and dataset paths.
- [ ] `M18.12` Implement the Sleep_CNN metric extractor.
- [ ] `M18.13` Add the versioned Sleep_CNN report prompt.
- [ ] `M18.14` Configure successful, failed, and superseded artifact retention.
- [ ] `M18.15` Validate a disposable process job.
- [ ] `M18.16` Validate a disposable Docker job without GPU.
- [ ] `M18.17` Validate a small GPU smoke family outside the legacy queue.
- [ ] `M18.18` Validate Telegram and report generation.
- [ ] `M18.19` Enable Igor only for new Sleep_CNN experiments.
- [ ] `M18.20` Preserve old databases and snapshots read-only until imported
  history and new operation have been verified.
- [ ] `M18.21` Remove legacy services only after an explicit final approval.

### Acceptance

- [ ] `A18.01` Imported history matches legacy job and attempt counts.
- [ ] `A18.02` Imported history cannot be accidentally scheduled.
- [ ] `A18.03` A new Sleep_CNN family runs, extracts metrics, notifies, and
  generates a report through Igor.
- [ ] `A18.04` The old queue remains recoverable until migration sign-off.
- [ ] `A18.05` The full `v1.0` acceptance criteria in `DESIGN.md` are satisfied.

## Final Release Checklist

- [ ] `R1` Review every fixed decision in `DESIGN.md` against implementation.
- [ ] `R2` Run `cargo fmt --check`.
- [ ] `R3` Run Clippy for all targets and features with warnings denied.
- [ ] `R4` Run all workspace tests.
- [ ] `R5` Run documentation tests.
- [ ] `R6` Run process, Docker, systemd, agent, crash, and migration suites.
- [ ] `R7` Run a clean-machine installation test.
- [ ] `R8` Verify that defaults impose no CPU, memory, or execution timeout.
- [ ] `R9` Verify exclusive GPU and unknown-job host reservation.
- [ ] `R10` Verify family-wide rerun after approved code repair.
- [ ] `R11` Verify cleanup of failed and superseded large artifacts.
- [ ] `R12` Verify ephemeral Pi and OpenCode session behavior.
- [ ] `R13` Verify useful Telegram result messages.
- [ ] `R14` Verify deterministic and prompted `RESULTS.md` generation.
- [ ] `R15` Back up the release database and restore it into a clean state root.
- [ ] `R16` Tag the release only after all checks and migration sign-off pass.
