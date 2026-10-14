ALTER TABLE artifacts ADD COLUMN diagnosis_required INTEGER NOT NULL DEFAULT 0
    CHECK (diagnosis_required IN (0, 1));

UPDATE artifacts SET diagnosis_required = 1 WHERE lifecycle_state = 'failed';

CREATE VIEW diagnosed_attempts AS
SELECT attempt_id FROM recoveries GROUP BY attempt_id
HAVING SUM(CASE WHEN diagnosis_json IS NOT NULL AND state NOT IN ('pending', 'diagnosing')
                THEN 1 ELSE 0 END) > 0
   AND SUM(CASE WHEN diagnosis_json IS NULL OR state IN ('pending', 'diagnosing')
                THEN 1 ELSE 0 END) = 0;

CREATE TRIGGER artifacts_diagnosis_insert BEFORE INSERT ON artifacts
WHEN NEW.lifecycle_state = 'failed' AND NEW.diagnosis_required != 1
BEGIN
    SELECT RAISE(ABORT, 'failed artifact requires diagnosis evidence');
END;

CREATE TRIGGER artifacts_diagnosis_update BEFORE UPDATE OF state, lifecycle_state, diagnosis_required ON artifacts
WHEN (OLD.diagnosis_required = 1 AND NEW.diagnosis_required != 1)
    OR (NEW.lifecycle_state = 'failed' AND NEW.diagnosis_required != 1)
    OR (OLD.diagnosis_required = 1 AND OLD.state != 'removed' AND NEW.state = 'removed'
        AND NOT EXISTS (SELECT 1 FROM diagnosed_attempts WHERE attempt_id = OLD.attempt_id))
BEGIN
    SELECT RAISE(ABORT, 'failed artifact requires completed diagnosis');
END;

CREATE TRIGGER artifacts_diagnosis_delete BEFORE DELETE ON artifacts
WHEN OLD.diagnosis_required = 1
    AND NOT EXISTS (SELECT 1 FROM diagnosed_attempts WHERE attempt_id = OLD.attempt_id)
BEGIN
    SELECT RAISE(ABORT, 'failed artifact requires completed diagnosis');
END;
