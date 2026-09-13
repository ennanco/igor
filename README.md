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

## Project Setup

Initialize portable configuration in an existing project:

```bash
igor init /path/to/project
```

This creates `.igor/project.toml` and the default report prompt. Machine state,
logs, credentials, and host-specific resources remain outside the project in
the configured XDG directories.

## Requirements

- Linux
- Rust `1.97.1`
- `systemd --user` for the initial service integration
- SQLite and Docker capabilities will be provided through Rust libraries and
  optional runtime integrations

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
