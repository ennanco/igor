// Disposable migration fixtures only: reconstruct pre-ledger databases for upgrade tests.
pub const REMOVE_REPORT_LEDGER: &str = "
DROP TRIGGER IF EXISTS report_action_success_guard;
DROP TRIGGER IF EXISTS report_runs_no_delete;
DROP TRIGGER IF EXISTS report_runs_update_guard;
DROP TRIGGER IF EXISTS report_runs_identity_guard;
DROP TRIGGER IF EXISTS report_runs_insert_guard;
DROP INDEX IF EXISTS report_runs_prepared_idx;
DROP INDEX IF EXISTS report_runs_output_idx;
DROP INDEX IF EXISTS report_runs_action_idx;
ALTER TABLE report_runs DROP COLUMN action_id;
ALTER TABLE report_runs DROP COLUMN project_id;
ALTER TABLE report_runs DROP COLUMN family_id;
ALTER TABLE report_runs DROP COLUMN context_json;
ALTER TABLE report_runs DROP COLUMN spec_json;
ALTER TABLE report_runs DROP COLUMN output_path;
ALTER TABLE report_runs DROP COLUMN output_size_bytes;
ALTER TABLE report_runs DROP COLUMN previous_output_hash;
ALTER TABLE report_runs DROP COLUMN renderer_version;
ALTER TABLE report_runs DROP COLUMN initial_lease_id;
ALTER TABLE report_runs DROP COLUMN publication_phase;
ALTER TABLE report_runs DROP COLUMN prepared_at;
ALTER TABLE report_runs DROP COLUMN published_at;
DELETE FROM _sqlx_migrations WHERE version = 23;
";
