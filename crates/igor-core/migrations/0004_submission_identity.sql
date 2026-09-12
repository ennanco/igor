ALTER TABLE projects ADD COLUMN is_alias INTEGER NOT NULL DEFAULT 0 CHECK (is_alias IN (0, 1));

CREATE TABLE project_aliases (
    project_id TEXT PRIMARY KEY NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    canonical_project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE
);

INSERT INTO project_aliases (project_id, canonical_project_id)
SELECT id, canonical_id
FROM (
    SELECT id,
           first_value(id) OVER (PARTITION BY root_path ORDER BY created_at, id) AS canonical_id
    FROM projects
)
WHERE id != canonical_id;

UPDATE projects
SET is_alias = 1
WHERE id IN (SELECT project_id FROM project_aliases);

CREATE UNIQUE INDEX projects_root_path_unique_idx ON projects (root_path) WHERE is_alias = 0;

CREATE TEMP TABLE legacy_job_order AS
SELECT id, project_id, submission_order FROM jobs;

UPDATE jobs SET submission_order = 'igor-migration:' || id;
WITH ordered AS (
    SELECT id,
           row_number() OVER (ORDER BY project_id, submission_order, id) AS position
    FROM legacy_job_order
)
UPDATE jobs
SET submission_order = (SELECT position FROM ordered WHERE ordered.id = jobs.id);

DROP TABLE legacy_job_order;

CREATE UNIQUE INDEX jobs_global_submission_order_idx ON jobs (submission_order);
CREATE INDEX jobs_project_history_idx ON jobs (project_id, submission_order DESC);

CREATE TRIGGER attempts_spec_immutable
BEFORE UPDATE OF spec_json ON attempts
BEGIN
    SELECT RAISE(ABORT, 'attempt specifications are immutable');
END;
