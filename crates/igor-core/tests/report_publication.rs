use std::{
    error::Error,
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::Path,
    sync::Barrier,
    thread,
};

use igor_core::{ReportPublicationError, stage_report};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn staging_is_private_and_publication_replaces_exact_bytes_in_one_new_inode() -> TestResult {
    let root = TempDir::new()?;
    let output = root.path().join("RESULTS.md");
    fs::write(&output, "previous report\n")?;
    let previous = fs::metadata(&output)?.ino();
    let markdown = "<!-- igor:generated-report:v1 -->\n# Métricas\n\nExact result: αβγ\n";
    let staged = stage_report(root.path(), Path::new("RESULTS.md"), markdown)?;
    let temporary = staged.staging_path().to_path_buf();
    assert_eq!(temporary.parent(), output.parent());
    assert_eq!(fs::read(&temporary)?, markdown.as_bytes());
    assert_eq!(
        fs::metadata(&temporary)?.permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(fs::metadata(&temporary)?.nlink(), 1);
    assert_eq!(fs::read(&output)?, b"previous report\n");
    let expected = staged.report().clone();
    assert_eq!(expected.size_bytes, markdown.len() as u64);
    assert_eq!(
        expected.sha256,
        format!("sha256:{:x}", Sha256::digest(markdown.as_bytes()))
    );
    assert_eq!(staged.publish()?, expected);
    assert_eq!(fs::read(&output)?, markdown.as_bytes());
    assert_ne!(fs::metadata(&output)?.ino(), previous);
    assert_eq!(fs::metadata(&output)?.permissions().mode() & 0o777, 0o600);
    assert!(!temporary.exists());
    stage_report(root.path(), &output, "absolute scoped output\n")?.publish()?;
    assert_eq!(fs::read(&output)?, b"absolute scoped output\n");
    Ok(())
}

#[test]
fn new_output_is_absent_until_publish_and_drop_cleans_only_its_own_temporary() -> TestResult {
    let root = TempDir::new()?;
    let output = root.path().join("RESULTS.md");
    let foreign = root.path().join(".igor-report-unrelated.tmp");
    fs::write(&foreign, "not owned by this invocation")?;
    let staged = stage_report(root.path(), &output, "new report")?;
    let temporary = staged.staging_path().to_path_buf();
    assert!(!output.exists());
    drop(staged);
    assert!(!temporary.exists());
    assert!(!output.exists());
    assert_eq!(fs::read(&foreign)?, b"not owned by this invocation");
    stage_report(root.path(), &output, "")?.publish()?;
    assert_eq!(fs::read(&output)?, b"");
    Ok(())
}

#[test]
fn stale_staged_writers_cannot_overwrite_another_publication() -> TestResult {
    for initially_present in [false, true] {
        let root = TempDir::new()?;
        let output = root.path().join("RESULTS.md");
        if initially_present {
            fs::write(&output, "previous report")?;
        }
        let first = stage_report(root.path(), &output, "first complete report")?;
        let second = stage_report(root.path(), &output, "stale second report")?;
        let stale_temporary = second.staging_path().to_path_buf();
        first.publish()?;
        assert!(matches!(
            second.publish(),
            Err(ReportPublicationError::Changed { .. })
        ));
        assert_eq!(fs::read(&output)?, b"first complete report");
        assert!(!stale_temporary.exists());
    }
    Ok(())
}

#[test]
fn simultaneous_publishers_revalidate_under_the_directory_lock() -> TestResult {
    for initially_present in [false, true] {
        let root = TempDir::new()?;
        let output = root.path().join("RESULTS.md");
        if initially_present {
            fs::write(&output, "previous report")?;
        }
        let first = stage_report(root.path(), &output, "first complete report")?;
        let second = stage_report(root.path(), &output, "second complete report")?;
        let gate = Barrier::new(2);
        let (first, second) = thread::scope(|scope| -> Result<_, std::io::Error> {
            let gate = &gate;
            let first = scope.spawn(move || {
                gate.wait();
                first.publish()
            });
            let second = scope.spawn(move || {
                gate.wait();
                second.publish()
            });
            Ok((
                first
                    .join()
                    .map_err(|_| std::io::Error::other("first publisher panicked"))?,
                second
                    .join()
                    .map_err(|_| std::io::Error::other("second publisher panicked"))?,
            ))
        })?;
        let expected = match (first, second) {
            (Ok(_), Err(ReportPublicationError::Changed { .. })) => {
                b"first complete report".as_slice()
            }
            (Err(ReportPublicationError::Changed { .. }), Ok(_)) => {
                b"second complete report".as_slice()
            }
            result => {
                return Err(format!(
                    "simultaneous publication did not fence one writer: {result:?}"
                )
                .into());
            }
        };
        assert_eq!(fs::read(&output)?, expected);
        assert_eq!(fs::read_dir(root.path())?.count(), 1);
    }
    Ok(())
}

#[test]
fn external_edits_and_removal_are_preserved() -> TestResult {
    for initial in [false, true] {
        let root = TempDir::new()?;
        let output = root.path().join("RESULTS.md");
        if initial {
            fs::write(&output, "alpha")?;
        }
        let staged = stage_report(root.path(), &output, "new report")?;
        fs::write(&output, "bravo")?; // Same length as the initial contents, with the same inode if present.
        assert!(matches!(
            staged.publish(),
            Err(ReportPublicationError::Changed { .. })
        ));
        assert_eq!(fs::read(&output)?, b"bravo");
        let staged = stage_report(root.path(), &output, "new report")?;
        fs::remove_file(&output)?;
        assert!(matches!(
            staged.publish(),
            Err(ReportPublicationError::Changed { .. })
        ));
        assert!(!output.exists());
    }
    Ok(())
}

#[test]
fn modified_or_replaced_staging_is_not_published_or_blindly_removed() -> TestResult {
    for alteration in ["bytes", "replacement", "permissions"] {
        let root = TempDir::new()?;
        let output = root.path().join("RESULTS.md");
        fs::write(&output, "previous report")?;
        let staged = stage_report(root.path(), &output, "new-report")?;
        let temporary = staged.staging_path().to_path_buf();
        if alteration == "replacement" {
            fs::remove_file(&temporary)?;
        }
        if alteration == "permissions" {
            fs::set_permissions(&temporary, fs::Permissions::from_mode(0o644))?;
        } else {
            fs::write(&temporary, "bad-report")?; // Same length; bytes/identity must be checked.
        }
        assert!(staged.publish().is_err());
        assert_eq!(fs::read(&output)?, b"previous report");
        if alteration == "replacement" {
            assert_eq!(fs::read(&temporary)?, b"bad-report");
        } else {
            assert!(!temporary.exists());
        }
    }
    Ok(())
}

#[test]
fn linked_staging_cannot_publish_and_foreign_entries_are_preserved() -> TestResult {
    for hardlink in [false, true] {
        let root = TempDir::new()?;
        let output = root.path().join("RESULTS.md");
        fs::write(&output, "previous report")?;
        let staged = stage_report(root.path(), &output, "new report")?;
        let temporary = staged.staging_path().to_path_buf();
        let other = root.path().join("other");
        if hardlink {
            fs::hard_link(&temporary, &other)?;
        } else {
            fs::write(&other, "external bytes")?;
            fs::remove_file(&temporary)?;
            symlink(&other, &temporary)?;
        }
        assert!(staged.publish().is_err());
        assert_eq!(fs::read(&output)?, b"previous report");
        assert_eq!(
            fs::read(&other)?,
            if hardlink {
                b"new report".as_slice()
            } else {
                b"external bytes".as_slice()
            }
        );
        assert!(fs::symlink_metadata(&temporary).is_ok());
    }
    Ok(())
}

#[test]
fn path_aliases_and_outputs_outside_the_project_are_rejected_before_staging() -> TestResult {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("project");
    fs::create_dir(&root)?;
    for output in [
        "",
        "../escape.md",
        "./RESULTS.md",
        "nested/../RESULTS.md",
        "nested//RESULTS.md",
        "RESULTS.md/",
    ] {
        assert!(matches!(
            stage_report(&root, Path::new(output), "new report"),
            Err(ReportPublicationError::UnsafePath { .. })
        ));
    }
    assert!(stage_report(&root, &temporary.path().join("outside.md"), "new report").is_err());
    assert!(stage_report(&root.join("."), Path::new("RESULTS.md"), "new report").is_err());
    assert!(stage_report(&root, Path::new("missing/RESULTS.md"), "new report").is_err());
    assert_eq!(fs::read_dir(&root)?.count(), 0);
    assert!(!temporary.path().join("outside.md").exists());
    Ok(())
}

#[test]
fn symlink_ancestors_and_nonregular_or_linked_destinations_are_preserved() -> TestResult {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("project");
    let outside = temporary.path().join("outside");
    fs::create_dir(&root)?;
    fs::create_dir(&outside)?;
    fs::write(outside.join("RESULTS.md"), "outside report")?;
    symlink(&outside, root.join("linked"))?;
    symlink(temporary.path(), temporary.path().join("alias"))?;
    for (project, output) in [
        (root.clone(), Path::new("linked/RESULTS.md")),
        (
            temporary.path().join("alias/project"),
            Path::new("RESULTS.md"),
        ),
    ] {
        assert!(stage_report(&project, output, "new report").is_err());
    }
    let output = root.join("RESULTS.md");
    symlink(outside.join("RESULTS.md"), &output)?;
    assert!(stage_report(&root, &output, "new report").is_err());
    fs::remove_file(&output)?;
    fs::hard_link(outside.join("RESULTS.md"), &output)?;
    assert!(stage_report(&root, &output, "new report").is_err());
    fs::remove_file(&output)?;
    fs::create_dir(&output)?;
    assert!(stage_report(&root, &output, "new report").is_err());
    fs::remove_dir(&output)?;
    nix::unistd::mkfifo(&output, nix::sys::stat::Mode::from_bits_truncate(0o600))?;
    assert!(stage_report(&root, &output, "new report").is_err());
    assert_eq!(fs::read(outside.join("RESULTS.md"))?, b"outside report");
    Ok(())
}

#[test]
fn replaced_parent_does_not_redirect_publication_or_cleanup() -> TestResult {
    let root = TempDir::new()?;
    let parent = root.path().join("reports");
    let moved = root.path().join("moved");
    fs::create_dir(&parent)?;
    let output = parent.join("RESULTS.md");
    fs::write(&output, "previous report")?;
    let staged = stage_report(root.path(), &output, "new report")?;
    let name = staged
        .staging_path()
        .file_name()
        .ok_or("temporary filename")?
        .to_owned();
    fs::rename(&parent, &moved)?;
    fs::create_dir(&parent)?;
    fs::write(&output, "different directory report")?;
    assert!(staged.publish().is_err());
    assert_eq!(fs::read(&output)?, b"different directory report");
    assert_eq!(fs::read(moved.join("RESULTS.md"))?, b"previous report");
    assert_eq!(fs::read(moved.join(name))?, b"new report");
    Ok(())
}

#[test]
fn concurrent_readers_observe_only_complete_old_or_new_reports() -> TestResult {
    let root = TempDir::new()?;
    let output = root.path().join("RESULTS.md");
    let old = "old report\n".repeat(64 * 1024);
    let new = "new report with a different length\n".repeat(64 * 1024);
    fs::write(&output, &old)?;
    let gate = Barrier::new(2);
    thread::scope(|scope| -> TestResult {
        let reader = scope.spawn(|| -> std::io::Result<()> {
            gate.wait();
            for _ in 0..64 {
                let bytes = fs::read(&output)?;
                if bytes != old.as_bytes() && bytes != new.as_bytes() {
                    return Err(std::io::Error::other("reader observed a partial report"));
                }
            }
            Ok(())
        });
        gate.wait();
        let result = (|| -> TestResult {
            for bytes in [&new, &old, &new] {
                stage_report(root.path(), &output, bytes)?.publish()?;
            }
            Ok(())
        })();
        reader
            .join()
            .map_err(|_| std::io::Error::other("report reader panicked"))??;
        result
    })?;
    assert_eq!(fs::read(&output)?, new.as_bytes());
    Ok(())
}
