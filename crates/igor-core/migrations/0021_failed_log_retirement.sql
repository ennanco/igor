CREATE TABLE failed_log_retirements (
    attempt_id TEXT NOT NULL REFERENCES attempts(id),
    stream TEXT NOT NULL CHECK(stream IN ('stdout', 'stderr')),
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json)
        AND COALESCE(json_extract(identity_json, '$.attempt_id') = attempt_id, 0)
        AND COALESCE(json_extract(identity_json, '$.stream') = stream, 0)
        AND COALESCE(json_type(identity_json, '$.original_path') = 'text', 0)
        AND COALESCE(json_type(identity_json, '$.original_size') = 'integer'
            AND json_extract(identity_json, '$.original_size') >= 0, 0)
        AND COALESCE(json_type(identity_json, '$.start') = 'integer'
            AND json_extract(identity_json, '$.start') >= 0, 0)
        AND COALESCE(json_extract(identity_json, '$.end') = json_extract(identity_json, '$.original_size'), 0)
        AND COALESCE(json_extract(identity_json, '$.end') - json_extract(identity_json, '$.start') BETWEEN 0 AND 16777216, 0)
        AND COALESCE(length(json_extract(identity_json, '$.original_sha256')) = 64, 0)
        AND COALESCE(length(json_extract(identity_json, '$.tail_sha256')) = 64, 0)),
    state TEXT NOT NULL CHECK(state IN ('prepared', 'completed')),
    decided_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY(attempt_id, stream)
);
CREATE TRIGGER failed_log_retirement_identity_immutable
BEFORE UPDATE OF identity_json, attempt_id, stream, decided_at ON failed_log_retirements
BEGIN SELECT RAISE(ABORT, 'immutable failed log retirement identity'); END;
CREATE TRIGGER failed_log_retirement_state_monotonic
BEFORE UPDATE OF state ON failed_log_retirements
WHEN OLD.state = 'completed' AND NEW.state != 'completed'
BEGIN SELECT RAISE(ABORT, 'completed failed log retirement'); END;
CREATE TRIGGER failed_log_retirement_preserved
BEFORE DELETE ON failed_log_retirements
BEGIN SELECT RAISE(ABORT, 'preserved failed log retirement'); END;
CREATE TRIGGER retired_failed_tails_immutable
BEFORE UPDATE ON failed_log_tails
WHEN EXISTS(SELECT 1 FROM failed_log_retirements WHERE attempt_id = OLD.attempt_id)
BEGIN SELECT RAISE(ABORT, 'immutable retired failed tail'); END;
CREATE TRIGGER retired_failed_tails_preserved
BEFORE DELETE ON failed_log_tails
WHEN EXISTS(SELECT 1 FROM failed_log_retirements WHERE attempt_id = OLD.attempt_id)
BEGIN SELECT RAISE(ABORT, 'preserved retired failed tail'); END;
CREATE TRIGGER retired_failed_tails_replacement_guard
BEFORE INSERT ON failed_log_tails
WHEN EXISTS(SELECT 1 FROM failed_log_retirements WHERE attempt_id = NEW.attempt_id)
BEGIN SELECT RAISE(ABORT, 'preserved retired failed tail'); END;
