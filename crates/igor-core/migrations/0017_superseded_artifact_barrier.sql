UPDATE artifacts SET lifecycle_state = 'superseded'
WHERE state = 'published' AND lifecycle_state = 'published'
  AND EXISTS (SELECT 1 FROM attempts a JOIN jobs j ON j.id = a.job_id
              WHERE a.id = artifacts.attempt_id AND j.state = 'superseded');

-- Live conservative SQL prerequisite, not a cached success latch. The domain
-- assessor must additionally validate the frozen protocol digest before cleanup.
CREATE VIEW successful_current_generations AS
SELECT g.id, g.family_id, g.generation_number FROM generations g
WHERE NOT EXISTS (SELECT 1 FROM generations newer
                  WHERE newer.family_id = g.family_id
                    AND newer.generation_number > g.generation_number)
  AND json_type(g.spec_json, '$.family.members') = 'array'
  AND json_array_length(g.spec_json, '$.family.members') > 0
  AND (SELECT count(*) FROM jobs j WHERE j.generation_id = g.id)
      = json_array_length(g.spec_json, '$.family.members')
  AND NOT EXISTS (
      SELECT 1 FROM json_each(g.spec_json, '$.family.members') seed
      WHERE json_type(seed.value, '$.seed') != 'integer'
         OR (SELECT count(*) FROM jobs j WHERE j.generation_id = g.id
             AND json_extract(j.spec_json, '$.family.seed') = json_extract(seed.value, '$.seed')) != 1)
  AND (SELECT count(DISTINCT json_extract(seed.value, '$.seed'))
       FROM json_each(g.spec_json, '$.family.members') seed)
      = json_array_length(g.spec_json, '$.family.members')
  AND NOT EXISTS (
      SELECT 1 FROM jobs j WHERE j.generation_id = g.id AND (
          j.state != 'succeeded' OR j.family_id IS NOT g.family_id
          OR json_extract(j.spec_json, '$.id') IS NOT j.id
          OR json_extract(j.spec_json, '$.family.family_id') IS NOT g.family_id
          OR json_extract(j.spec_json, '$.family.generation.id') IS NOT g.id
          OR json_extract(j.spec_json, '$.family.generation.number') IS NOT g.generation_number
          OR json_extract(j.spec_json, '$.family.generation.source_revision') IS NOT g.source_revision
          OR json_extract(j.spec_json, '$.family.generation.protocol_digest') IS NOT g.protocol_digest));

CREATE TRIGGER artifacts_replacement_update BEFORE UPDATE OF state ON artifacts
WHEN NEW.state = 'removed' AND OLD.state != 'removed'
 AND EXISTS (SELECT 1 FROM attempts a JOIN jobs j ON j.id = a.job_id
             WHERE a.id = OLD.attempt_id AND j.state = 'superseded'
               AND NOT EXISTS (SELECT 1 FROM successful_current_generations g
                   JOIN generations old ON old.id = j.generation_id
                   WHERE g.family_id = j.family_id AND g.id != old.id
                     AND g.generation_number > old.generation_number))
BEGIN
    SELECT RAISE(ABORT, 'superseded artifact requires successful current replacement');
END;

CREATE TRIGGER artifacts_replacement_delete BEFORE DELETE ON artifacts
WHEN EXISTS (SELECT 1 FROM attempts a JOIN jobs j ON j.id = a.job_id
             WHERE a.id = OLD.attempt_id AND j.state = 'superseded'
               AND NOT EXISTS (SELECT 1 FROM successful_current_generations g
                   JOIN generations old ON old.id = j.generation_id
                   WHERE g.family_id = j.family_id AND g.id != old.id
                     AND g.generation_number > old.generation_number))
BEGIN
    SELECT RAISE(ABORT, 'superseded artifact requires successful current replacement');
END;
