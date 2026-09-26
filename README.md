# Igor

[![Status: in development](https://img.shields.io/badge/status-in%20development-orange)](TASKS.md)
[![CI](https://github.com/ennanco/igor/actions/workflows/ci.yml/badge.svg)](https://github.com/ennanco/igor/actions/workflows/ci.yml)
[![Rust 1.97.1](https://img.shields.io/badge/rust-1.97.1-black?logo=rust)](rust-toolchain.toml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)

**Integrated General-purpose Orchestrator for Research**

Igor is a resource-aware experiment runner and recovery orchestrator for Linux.
It will execute arbitrary process and Docker workloads, coordinate exclusive
compute resources, persist execution history, extract structured results, and
generate project reports.

The project includes its first usable local process queue but remains under
active development and is not ready for production experiment management. See
[`DESIGN.md`](DESIGN.md) for the agreed architecture and [`TASKS.md`](TASKS.md)
for the ordered implementation backlog.

## Requirements

- Linux
- Rust `1.97.1`
- Git

Docker is optional. Direct-process installations do not need the Docker CLI,
daemon, socket, or Rust Docker client dependency. Docker jobs require a locally
reachable Docker daemon and permission for the worker user to use it.

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
igor uninstall
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
