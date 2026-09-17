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

Docker and `systemd --user` integration are planned but are not required by the
current direct-process queue.

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

Most inspection commands support machine-readable output through `--json`, for
example:

```bash
igor list --json
igor show JOB_ID --json
igor wait JOB_ID --json
```

Use `igor --help` or `igor COMMAND --help` for the complete command reference.

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
The current worker remains limited to one experiment while M7 scheduling is
completed. They do not impose hard cgroup limits on an individual process;
strict process limits will arrive with the planned `systemd` integration. CPU
and memory remain unrestricted unless a smaller scheduling capacity is
configured.

## Development

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --no-deps
```

Inspect the CLI:

```bash
cargo run -p igor-cli -- --version
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your
option.
