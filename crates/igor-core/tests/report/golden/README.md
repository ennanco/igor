# Report format v1 goldens

These full Markdown snapshots exercise the pure renderer with fixed context-v1
identities and paths. They do not authorize publication of provisional reports.
The context-builder and publication integration suites cover database selection,
ownership and live gates separately.

- `complete.md`: comparable two-seed family, selection/per-class evidence,
  escaped text, retained artifact link and removed artifact without a link.
- `failed.md`: terminal but noncomparable family, failed job/extraction and no
  primary series, even though another seed succeeded.
- `repaired.md`: successful retry, failed attempt retained as history, current
  repair evidence, patch/validation/decision and safe JSON fencing.
- `superseded.md`: only current-generation primary observations, two earlier
  generations and a historical failed repair kept in separate sections.

Normal tests compare every byte and repeat with round-tripped, reordered
collections; they never update snapshots. To intentionally regenerate:

```sh
cargo test -p igor-core --test report golden::regenerate_report_goldens -- --ignored --exact
cargo test -p igor-core --test report
```

Review the full Markdown changes and the independent semantic assertions before
accepting a format change. Do not regenerate snapshots just to silence a failure.
