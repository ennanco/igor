use igor_core::{Database, DatabaseOptions, GcPlan, ProjectId, RuntimePaths};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

pub async fn sql_snapshot(db: &Database) -> Result<Vec<(String, Vec<String>)>, Box<dyn Error>> {
    let tables: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetch_all(db.pool()).await?;
    let mut result = vec![];
    for table in tables {
        let columns: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT name FROM pragma_table_info('{table}') ORDER BY cid"
        ))
        .fetch_all(db.pool())
        .await?;
        let expression = columns
            .iter()
            .map(|c| format!("quote(\"{c}\")"))
            .collect::<Vec<_>>()
            .join(" || ',' || ");
        let rows = sqlx::query_scalar(&format!("SELECT {expression} FROM \"{table}\" ORDER BY 1"))
            .fetch_all(db.pool())
            .await?;
        result.push((table, rows));
    }
    Ok(result)
}

type FileSnapshot = Vec<(PathBuf, u32, String)>;

pub fn files(root: &Path, database: &Path) -> Result<FileSnapshot, Box<dyn Error>> {
    fn visit(
        path: &Path,
        database: &Path,
        output: &mut FileSnapshot,
    ) -> Result<(), Box<dyn Error>> {
        if path == database
            || path.as_os_str() == format!("{}-wal", database.display()).as_str()
            || path.as_os_str() == format!("{}-shm", database.display()).as_str()
        {
            return Ok(());
        }
        let m = fs::symlink_metadata(path)?;
        let identity = if m.file_type().is_symlink() {
            format!("symlink:{}", fs::read_link(path)?.display())
        } else if m.is_dir() {
            "directory".into()
        } else if !m.is_file() {
            "special".into()
        } else {
            let mut hash = Sha256::new();
            let mut input = fs::File::open(path)?;
            let mut buffer = [0; 65536];
            loop {
                let n = input.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                hash.update(&buffer[..n]);
            }
            format!("{:x}", hash.finalize())
        };
        output.push((path.to_owned(), m.permissions().mode(), identity));
        if m.is_dir() {
            for entry in fs::read_dir(path)? {
                visit(&entry?.path(), database, output)?;
            }
        }
        Ok(())
    }
    let mut output = vec![];
    visit(root, database, &mut output)?;
    output.sort();
    Ok(output)
}

pub async fn readonly_plan(
    db: &Database,
    paths: &RuntimePaths,
    root: &Path,
    scope: Option<ProjectId>,
) -> Result<GcPlan, Box<dyn Error>> {
    let readonly = Database::open_with_options(
        &paths.database,
        DatabaseOptions {
            writable: false,
            create_if_missing: false,
            ..Default::default()
        },
    )
    .await?;
    let before = sql_snapshot(db).await?;
    let disk = files(root, &paths.database)?;
    let project_roots: Vec<String> =
        sqlx::query_scalar("SELECT root_path FROM projects ORDER BY id")
            .fetch_all(db.pool())
            .await?;
    let mut external = vec![];
    for project in project_roots {
        let project = PathBuf::from(project);
        if project.exists() && !project.starts_with(root) {
            external.push((project.clone(), files(&project, &paths.database)?));
        }
    }
    // A concurrent writer must not prevent a SELECT-only preview, even during hashing/Git checks.
    let mut writer = db.pool().begin().await?;
    sqlx::query("UPDATE projects SET name = name")
        .execute(&mut *writer)
        .await?;
    let plan = readonly.gc_plan(scope, paths).await?;
    assert_eq!(readonly.gc_plan(scope, paths).await?, plan);
    writer.rollback().await?;
    assert_eq!(sql_snapshot(db).await?, before);
    assert_eq!(files(root, &paths.database)?, disk);
    for (project, before) in external {
        assert_eq!(files(&project, &paths.database)?, before);
    }
    assert!(plan.advisory && plan.requires_revalidation);
    assert_eq!(plan.estimated_gzip_savings, None);
    assert_eq!(plan.worktree_bytes, None);
    readonly.pool().close().await;
    Ok(plan)
}
