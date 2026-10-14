CREATE TABLE success_log_archive_preparations (
    attempt_id TEXT NOT NULL REFERENCES attempts(id),
    stream TEXT NOT NULL CHECK (stream IN ('stdout', 'stderr')),
    raw_path TEXT NOT NULL,
    temp_path TEXT NOT NULL UNIQUE,
    PRIMARY KEY (attempt_id, stream)
);
CREATE TRIGGER success_log_preparation_update BEFORE UPDATE ON success_log_archive_preparations
BEGIN SELECT RAISE(ABORT, 'immutable log preparation'); END;
CREATE TRIGGER success_log_preparation_delete BEFORE DELETE ON success_log_archive_preparations
BEGIN SELECT RAISE(ABORT, 'immutable log preparation'); END;

CREATE TABLE success_log_archives (
    attempt_id TEXT NOT NULL REFERENCES attempts(id),
    stream TEXT NOT NULL CHECK (stream IN ('stdout', 'stderr')),
    identity_json TEXT NOT NULL CHECK (json_valid(identity_json)
        AND COALESCE(json_type(identity_json, '$.path') = 'text', 0)
        AND COALESCE(json_type(identity_json, '$.temporary_path') = 'text', 0)
        AND COALESCE(json_type(identity_json, '$.original_path') = 'text', 0)
        AND COALESCE(json_type(identity_json, '$.original_size') = 'integer' AND json_extract(identity_json, '$.original_size') >= 0, 0)
        AND COALESCE(json_type(identity_json, '$.archive_size') = 'integer' AND json_extract(identity_json, '$.archive_size') > 0, 0)
        AND COALESCE(length(json_extract(identity_json, '$.original_sha256')) = 64, 0)
        AND COALESCE(length(json_extract(identity_json, '$.archive_sha256')) = 64, 0)
        AND COALESCE(json_extract(identity_json, '$.codec_version') = 1, 0)),
    state TEXT NOT NULL CHECK (state IN ('prepared', 'published')),
    retired INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0, 1) AND (retired = 0 OR state = 'published')),
    PRIMARY KEY (attempt_id, stream)
);
CREATE TRIGGER success_log_archive_identity_update BEFORE UPDATE ON success_log_archives
WHEN NEW.attempt_id != OLD.attempt_id OR NEW.stream != OLD.stream
 OR NEW.identity_json != OLD.identity_json OR (OLD.state = 'published' AND NEW.state != 'published') OR NEW.retired < OLD.retired
BEGIN SELECT RAISE(ABORT, 'immutable log archive identity'); END;
CREATE TRIGGER success_log_archive_identity_delete BEFORE DELETE ON success_log_archives
BEGIN SELECT RAISE(ABORT, 'immutable log archive identity'); END;
