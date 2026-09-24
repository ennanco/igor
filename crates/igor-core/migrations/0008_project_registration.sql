ALTER TABLE projects ADD COLUMN registered INTEGER NOT NULL DEFAULT 1
    CHECK (registered IN (0, 1));

CREATE INDEX projects_registered_idx ON projects (registered, name, id)
    WHERE is_alias = 0;

CREATE TRIGGER jobs_require_registered_project
BEFORE INSERT ON jobs
WHEN NOT EXISTS (
    SELECT 1 FROM projects
    WHERE projects.id = NEW.project_id AND projects.registered = 1
)
BEGIN
    SELECT RAISE(ABORT, 'jobs require a registered project');
END;
