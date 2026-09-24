CREATE TABLE attempt_containers (
    attempt_id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    container_id TEXT NOT NULL UNIQUE CHECK (length(trim(container_id)) > 0),
    container_name TEXT NOT NULL UNIQUE CHECK (length(trim(container_name)) > 0),
    image_reference TEXT NOT NULL CHECK (length(trim(image_reference)) > 0),
    image_id TEXT NOT NULL CHECK (length(trim(image_id)) > 0),
    state TEXT NOT NULL CHECK (state IN ('created', 'running', 'exited', 'removed', 'lost')),
    docker_status TEXT,
    stdout_path TEXT NOT NULL,
    stderr_path TEXT NOT NULL,
    exit_code INTEGER,
    oom_killed INTEGER CHECK (oom_killed IN (0, 1)),
    error TEXT,
    started_at TEXT,
    finished_at TEXT,
    removed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (container_name = 'igor-' || attempt_id),
    CHECK (state != 'running' OR started_at IS NOT NULL),
    CHECK (state NOT IN ('exited', 'lost') OR finished_at IS NOT NULL),
    CHECK (state != 'removed' OR removed_at IS NOT NULL),
    FOREIGN KEY (attempt_id, job_id, project_id)
        REFERENCES attempts(id, job_id, project_id) ON DELETE CASCADE
);

CREATE INDEX attempt_containers_state_idx ON attempt_containers (state, updated_at);
