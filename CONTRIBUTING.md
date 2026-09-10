# Contributing

Igor is currently an experimental single-user Linux project. Keep changes
focused on the active milestone in [`TASKS.md`](TASKS.md), and update
[`DESIGN.md`](DESIGN.md) before changing a fixed architectural decision.

Run the complete local quality gate before requesting review:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --no-deps
```

Tests must use disposable XDG directories and must not access live research
databases, datasets, GPU jobs, containers, or user services.
