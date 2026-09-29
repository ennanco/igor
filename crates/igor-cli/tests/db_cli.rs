use std::{error::Error, fs, path::Path, process::Command};

use igor_core::{Database, DatabaseOptions, IntegrityCheck};
use serde_json::Value;
use sqlx::Connection;
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

fn command(home: &Path, cwd: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_igor"));
    command
        .env_clear()
        .current_dir(cwd)
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_STATE_HOME", home.join("state"))
        .env("XDG_RUNTIME_DIR", home.join("runtime"));
    command
}

fn text(bytes: Vec<u8>) -> Result<String, Box<dyn Error>> {
    Ok(String::from_utf8(bytes)?)
}

fn database_path(temporary: &TempDir) -> std::path::PathBuf {
    temporary.path().join("state/igor/igor.sqlite3")
}

async fn initialized_database(path: &Path) -> Result<i64, Box<dyn Error>> {
    let database = Database::open(path).await?;
    database.integrity_check(IntegrityCheck::Full).await?;
    let schema_version = database.schema_version().await?;
    database.pool().close().await;
    Ok(schema_version)
}

#[test]
fn db_check_on_missing_database_fails_without_creating_it() -> TestResult {
    const SENTINEL: &str = "DB-SENTINEL-SECRET";
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    let config = home.join("config/igor/config.toml");
    fs::create_dir_all(config.parent().ok_or("missing config parent")?)?;
    fs::write(
        &config,
        format!("schema_version = 1\n[telegram]\nbot_token = '{SENTINEL}'\n"),
    )?;
    let database = database_path(&temporary);
    assert!(!database.exists());
    let output = command(&home, temporary.path())
        .args([
            "--database",
            database.to_str().ok_or("non-UTF-8 database path")?,
            "db",
            "check",
            "--json",
        ])
        .output()?;
    assert!(!output.status.success());
    let stderr = text(output.stderr)?;
    assert!(stderr.contains("database file does not exist"), "{stderr}");
    assert!(!stderr.contains(SENTINEL));
    assert!(!database.exists());
    Ok(())
}

#[tokio::test]
async fn db_check_accepts_initialized_database_and_preserves_schema() -> TestResult {
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    fs::create_dir_all(&home)?;
    let database = database_path(&temporary);
    let before = initialized_database(&database).await?;
    assert_eq!(before, Database::latest_schema_version());

    let output = command(&home, temporary.path())
        .args([
            "--database",
            database.to_str().ok_or("non-UTF-8 database path")?,
            "db",
            "check",
            "--json",
        ])
        .output()?;
    assert!(output.status.success(), "{}", text(output.stderr)?);
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["status"], "ok");
    assert_eq!(report["schema_version"], before);
    assert_eq!(report["latest_schema_version"], before);

    let output = command(&home, temporary.path())
        .args([
            "--database",
            database.to_str().ok_or("non-UTF-8 database path")?,
            "db",
            "check",
        ])
        .output()?;
    assert!(output.status.success(), "{}", text(output.stderr)?);
    let stdout = text(output.stdout)?;
    assert!(stdout.contains("database check passed"), "{stdout}");
    assert!(
        stdout.contains(&format!("schema version {before}")),
        "{stdout}"
    );

    let reopened = Database::open_with_options(
        &database,
        DatabaseOptions {
            writable: false,
            ..DatabaseOptions::default()
        },
    )
    .await?;
    assert_eq!(reopened.schema_version().await?, before);
    assert_eq!(Database::latest_schema_version(), before);
    Ok(())
}

#[tokio::test]
async fn db_check_fails_on_foreign_key_violation() -> TestResult {
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    fs::create_dir_all(&home)?;
    let database = database_path(&temporary);
    initialized_database(&database).await?;

    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&database)
        .create_if_missing(false)
        .foreign_keys(false);
    let mut connection = sqlx::SqliteConnection::connect_with(&options).await?;
    sqlx::query(
        "INSERT INTO events (id, project_id, kind, payload_json) \
         VALUES ('evt-broken', 'missing-project', 'job_submitted', '{}')",
    )
    .execute(&mut connection)
    .await?;
    connection.close().await?;

    let output = command(&home, temporary.path())
        .args([
            "--database",
            database.to_str().ok_or("non-UTF-8 database path")?,
            "db",
            "check",
        ])
        .output()?;
    assert!(!output.status.success());
    let stderr = text(output.stderr)?;
    assert!(stderr.contains("foreign key"), "{stderr}");
    Ok(())
}

#[tokio::test]
async fn db_check_fails_on_outdated_schema_version() -> TestResult {
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    fs::create_dir_all(&home)?;
    let database = database_path(&temporary);
    let before = initialized_database(&database).await?;

    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&database)
        .create_if_missing(false);
    let mut connection = sqlx::SqliteConnection::connect_with(&options).await?;
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = ?")
        .bind(before)
        .execute(&mut connection)
        .await?;
    connection.close().await?;
    let read_only = Database::open_with_options(
        &database,
        DatabaseOptions {
            writable: false,
            ..DatabaseOptions::default()
        },
    )
    .await?;
    let outdated = read_only.schema_version().await?;
    assert_ne!(outdated, before);
    read_only.pool().close().await;

    let output = command(&home, temporary.path())
        .args([
            "--database",
            database.to_str().ok_or("non-UTF-8 database path")?,
            "db",
            "check",
            "--json",
        ])
        .output()?;
    assert!(!output.status.success());
    let stderr = text(output.stderr)?;
    assert!(
        stderr.contains(&format!(
            "schema version {outdated} does not match latest {before}"
        )),
        "{stderr}"
    );
    Ok(())
}

#[tokio::test]
async fn db_migrate_existing_older_and_current_databases() -> TestResult {
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    fs::create_dir_all(&home)?;
    let database = database_path(&temporary);
    let latest = initialized_database(&database).await?;
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&database)
        .create_if_missing(false);
    let mut connection = sqlx::SqliteConnection::connect_with(&options).await?;
    sqlx::query("DROP TABLE attempt_units")
        .execute(&mut connection)
        .await?;
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 9")
        .execute(&mut connection)
        .await?;
    connection.close().await?;

    let preview = command(&home, temporary.path())
        .args([
            "--database",
            database.to_str().ok_or("non-UTF-8 path")?,
            "db",
            "migrate",
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(preview.status.success(), "{}", text(preview.stderr)?);
    let preview: Value = serde_json::from_slice(&preview.stdout)?;
    assert_eq!(preview["status"], "dry_run");
    assert_eq!(preview["schema_version_before"], 8);
    assert_eq!(preview["would_migrate"], true);
    let unchanged = Database::open_with_options(
        &database,
        DatabaseOptions {
            writable: false,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(unchanged.schema_version().await?, 8);
    let units: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='attempt_units'",
    )
    .fetch_one(unchanged.pool())
    .await?;
    assert_eq!(units, 0);
    unchanged.pool().close().await;

    let output = command(&home, temporary.path())
        .args([
            "--database",
            database.to_str().ok_or("non-UTF-8 path")?,
            "db",
            "migrate",
            "--json",
        ])
        .output()?;
    assert!(output.status.success(), "{}", text(output.stderr)?);
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["schema_version_before"], 8);
    assert_eq!(report["schema_version_after"], latest);
    let read_only = Database::open_with_options(
        &database,
        DatabaseOptions {
            writable: false,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(read_only.schema_version().await?, latest);
    let pool = read_only.pool();
    let exists: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='attempt_units'",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(exists, 1);
    read_only.pool().close().await;

    let output = command(&home, temporary.path())
        .args([
            "--database",
            database.to_str().ok_or("non-UTF-8 path")?,
            "db",
            "migrate",
            "--json",
        ])
        .output()?;
    assert!(output.status.success(), "{}", text(output.stderr)?);
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["status"], "unchanged");
    assert_eq!(report["schema_version_before"], latest);
    Ok(())
}

#[test]
fn db_migrate_missing_does_not_create_database() -> TestResult {
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    fs::create_dir_all(&home)?;
    let database = database_path(&temporary);
    let output = command(&home, temporary.path())
        .args([
            "--database",
            database.to_str().ok_or("non-UTF-8 path")?,
            "db",
            "migrate",
        ])
        .output()?;
    assert!(!output.status.success());
    assert!(text(output.stderr)?.contains("refusing to create"));
    assert!(!database.exists());
    Ok(())
}

#[tokio::test]
async fn db_backup_is_restorable_and_refuses_overwrite_or_alias() -> TestResult {
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    fs::create_dir_all(&home)?;
    let source = database_path(&temporary);
    let version = initialized_database(&source).await?;
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&source)
        .create_if_missing(false);
    let mut connection = sqlx::SqliteConnection::connect_with(&options).await?;
    sqlx::query("INSERT INTO projects (id, name, root_path, config_path) VALUES ('backup-project', 'test', '/tmp/test', '/tmp/test/config.toml')")
        .execute(&mut connection)
        .await?;
    connection.close().await?;
    let destination = temporary.path().join("backup.sqlite3");
    let output = command(&home, temporary.path())
        .args([
            "--database",
            source.to_str().ok_or("non-UTF-8 path")?,
            "db",
            "backup",
            destination.to_str().ok_or("non-UTF-8 path")?,
            "--json",
        ])
        .output()?;
    assert!(output.status.success(), "{}", text(output.stderr)?);
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["status"], "ok");
    assert_eq!(report["source_schema_version"], version);
    let restored = Database::open_with_options(
        &destination,
        DatabaseOptions {
            writable: false,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(restored.schema_version().await?, version);
    restored.integrity_check(IntegrityCheck::Full).await?;
    let restored_project: String =
        sqlx::query_scalar("SELECT name FROM projects WHERE id = 'backup-project'")
            .fetch_one(restored.pool())
            .await?;
    assert_eq!(restored_project, "test");
    restored.pool().close().await;
    let original_backup = fs::read(&destination)?;

    let output = command(&home, temporary.path())
        .args([
            "--database",
            source.to_str().ok_or("non-UTF-8 path")?,
            "db",
            "backup",
            destination.to_str().ok_or("non-UTF-8 path")?,
        ])
        .output()?;
    assert!(!output.status.success());
    assert!(text(output.stderr)?.contains("already exists"));
    assert_eq!(fs::read(&destination)?, original_backup);
    let output = command(&home, temporary.path())
        .args([
            "--database",
            source.to_str().ok_or("non-UTF-8 path")?,
            "db",
            "backup",
            source.to_str().ok_or("non-UTF-8 path")?,
        ])
        .output()?;
    assert!(!output.status.success());
    assert!(text(output.stderr)?.contains("must differ from the source"));
    assert_eq!(initialized_database(&source).await?, version);
    Ok(())
}
