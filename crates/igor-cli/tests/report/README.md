# Non-agent report acceptance

The M14/A11 acceptance evidence combines focused invariant tests with the actual
CLI/worker/supervisor pipeline. All state belongs to disposable fixture projects.

| Criterion | Evidence |
| --- | --- |
| A14.01: stable Markdown from the same normalized context | `igor-core/tests/report.rs` and `tests/report/golden.rs`: full snapshots, JSON round trips, collection permutations and context non-mutation. |
| A14.02: preserve handwritten files in the default mode | `igor-core/tests/generated_report.rs`: manuscripts, unknown/foreign claims, appended notes and edits. `igor-daemon/src/report_triggers.rs` and CLI `report_cli.rs` verify report-only failures/retries preserve files and job outcomes. |
| A14.03: publication failures cannot damage a previous report | `igor-core/src/report_execution_tests.rs`: rename/sync faults and isolated SIGKILL before/after rename, old bytes/inode, exact-file reconciliation, one ACK and orphan preservation. Persistence `report_execution.rs`/`report_ledger.rs` cover SQL/event/COMMIT faults, stale leases and preservation of different newer output. |
| A14.04/A11.03: complete non-agent workflow and current-only aggregate | `acceptance.rs::five_seed_non_agent_workflow_publishes_only_current_generation`: three five-seed generations, real metrics/artifacts, worker-only execution, supervisor restart, manual/automatic reports, partial/failed replacements, frozen retry, current-only primary results and superseded-copy cleanup. |

## Integrated scenario

1. Five jobs succeed with scores 101–105 while only the worker is running. Metrics
   and report remain pending. Starting the supervisor runs extraction, publishes
   model artifacts and completes one manual report plus the automatic trigger.
2. A committed replacement has scores in the 200 range and fails seed 3. Seed 5
   is held while earlier seeds finish and some metrics are indexed. Both partial
   and terminal-failed contexts remain provisional. The previous report and model
   copies survive; retrying the same failed job creates one failed attempt rather
   than a new generation or a final report.
3. A manually repaired revision reruns all five seeds. The scientific input, seed
   membership and protocol digest are unchanged. The worker again completes jobs
   independently; restarting the supervisor extracts and automatically publishes
   scores 301–305. Stored context, actual report body/hash, current attempt IDs,
   indexed selection/classes and model bytes agree. Old attempt IDs and scores
   never enter primary results.
4. Superseded copies are retired with historical metadata and report provenance
   retained. `storage status`, GC preview and logs work. Scientific input, manual
   notes, prompt and project configuration are byte-identical. Fifteen jobs plus
   one explicit failed retry account for exactly sixteen attempts.

The scenario requires neither coding agents nor Telegram/Docker/systemd services.
No extractor and disabled reports are separate supported cases, covered by the
configuration, context, generated-file and daemon suites. A provisional renderer
fixture is diagnostic text; it is not permission to publish an incomplete report.
Post-rename uncertainty may expose the complete new file before acknowledgement;
recovery confirms it rather than attempting to restore an obsolete baseline.

## Focused verification

```sh
cargo test -p igor-cli --test report_cli -- --test-threads=1
cargo test -p igor-core --test report --test generated_report --test report_publication
cargo test -p igor-core --lib persistence::report_execution::tests -- --test-threads=1
cargo test -p igor-core --test persistence report_ -- --test-threads=1
```

The SIGKILL parent launches its ignored child entry explicitly. The golden
regeneration helper stays ignored during normal verification. Full milestone
validation uses the workspace all-features serial suite, strict Clippy, format,
docs and `git diff --check`. Roadmap v0.3 acceptance does not bump Cargo's 0.1.0
version or publish release artifacts.
