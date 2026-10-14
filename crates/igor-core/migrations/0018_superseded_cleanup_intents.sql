CREATE TABLE superseded_cleanup_intents (
    artifact_id TEXT PRIMARY KEY NOT NULL REFERENCES artifacts(id),
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json)),
    replacement_generation_id TEXT NOT NULL REFERENCES generations(id),
    state TEXT NOT NULL DEFAULT 'prepared' CHECK(state IN ('prepared', 'removed')),
    completion_only INTEGER NOT NULL DEFAULT 0 CHECK(completion_only IN (0, 1))
);

CREATE TRIGGER superseded_cleanup_insert_guard BEFORE INSERT ON superseded_cleanup_intents
WHEN NEW.state != 'prepared' OR NEW.completion_only != 0 OR NOT EXISTS (
    SELECT 1 FROM artifacts r JOIN attempts a ON a.id = r.attempt_id
    JOIN jobs j ON j.id = a.job_id JOIN generations old ON old.id = j.generation_id
    JOIN successful_current_generations g ON g.family_id = j.family_id
    WHERE r.id = NEW.artifact_id AND r.state = 'published' AND r.lifecycle_state = 'superseded'
      AND a.state = 'succeeded' AND j.state = 'superseded'
      AND g.id = NEW.replacement_generation_id AND g.generation_number > old.generation_number)
BEGIN
    SELECT RAISE(ABORT, 'superseded cleanup intent requires published successful attempt and replacement');
END;

CREATE TRIGGER superseded_cleanup_identity_update BEFORE UPDATE ON superseded_cleanup_intents
WHEN NEW.artifact_id IS NOT OLD.artifact_id
 OR NEW.identity_json IS NOT OLD.identity_json
 OR NEW.replacement_generation_id IS NOT OLD.replacement_generation_id
 OR (OLD.state = 'removed' AND NEW.state != 'removed')
BEGIN
    SELECT RAISE(ABORT, 'immutable superseded cleanup evidence');
END;

CREATE TRIGGER superseded_cleanup_identity_delete BEFORE DELETE ON superseded_cleanup_intents
BEGIN
    SELECT RAISE(ABORT, 'immutable superseded cleanup evidence');
END;

DROP TRIGGER artifacts_replacement_update;
CREATE TRIGGER artifacts_replacement_update BEFORE UPDATE OF state ON artifacts
WHEN NEW.state = 'removed' AND OLD.state != 'removed'
 AND NOT (OLD.state = 'published' AND OLD.lifecycle_state = 'superseded'
          AND EXISTS (SELECT 1 FROM superseded_cleanup_intents i
                      WHERE i.artifact_id = OLD.id AND i.state = 'prepared' AND i.completion_only = 1))
 AND EXISTS (SELECT 1 FROM attempts a JOIN jobs j ON j.id = a.job_id
             WHERE a.id = OLD.attempt_id AND j.state = 'superseded'
               AND NOT EXISTS (SELECT 1 FROM successful_current_generations g
                   JOIN generations old ON old.id = j.generation_id
                   WHERE g.family_id = j.family_id AND g.id != old.id
                     AND g.generation_number > old.generation_number))
BEGIN
    SELECT RAISE(ABORT, 'superseded artifact requires successful current replacement');
END;
