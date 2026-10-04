CREATE TABLE metric_selections (
    attempt_id TEXT PRIMARY KEY NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    split TEXT NOT NULL CHECK (length(trim(split)) > 0),
    metric TEXT NOT NULL CHECK (length(trim(metric)) > 0),
    value REAL NOT NULL CHECK (value >= -1.7976931348623157e308 AND value <= 1.7976931348623157e308),
    epoch INTEGER CHECK (epoch IS NULL OR epoch >= 0),
    trained_epochs INTEGER CHECK (trained_epochs IS NULL OR trained_epochs >= 0),
    checkpoint_path TEXT,
    selection_json TEXT NOT NULL CHECK (json_valid(selection_json) AND json_type(selection_json) = 'object'),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX metric_selections_split_metric_idx ON metric_selections (split, metric);
