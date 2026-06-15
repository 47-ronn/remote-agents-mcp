//! Filesystem operations: directory listing and rotating backups.

use anyhow::Result;
use remote_agents_shared::DirEntry;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

/// Backup file naming: `<original-name>.<unix-millis>.bak`.
fn backup_suffix() -> &'static str {
    ".bak"
}

/// Create a timestamped backup of `path` inside `backup_dir`, then prune old
/// backups for the same file so at most `max_versions` are kept (0 = keep all).
pub async fn create_backup(path: &str, backup_dir: &str, max_versions: usize) -> Result<PathBuf> {
    let backup_dir = Path::new(backup_dir);
    tokio::fs::create_dir_all(backup_dir).await?;

    let filename = Path::new(path)
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);

    let backup_path = backup_dir.join(format!("{}.{}{}", filename, timestamp, backup_suffix()));
    tokio::fs::copy(path, &backup_path).await?;
    debug!("Created backup: {:?}", backup_path);

    prune_backups(backup_dir, &filename, max_versions).await?;

    Ok(backup_path)
}

/// Remove the oldest backups for `filename` beyond `max_versions`.
async fn prune_backups(backup_dir: &Path, filename: &str, max_versions: usize) -> Result<()> {
    if max_versions == 0 {
        return Ok(());
    }

    let prefix = format!("{}.", filename);
    let suffix = backup_suffix();

    let mut backups: Vec<PathBuf> = Vec::new();
    let mut dir = tokio::fs::read_dir(backup_dir).await?;
    while let Some(entry) = dir.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&prefix) && name.ends_with(suffix) {
            backups.push(entry.path());
        }
    }

    if backups.len() <= max_versions {
        return Ok(());
    }

    // Names sort lexically the same as chronologically (fixed prefix + numeric
    // millis + fixed suffix), so ascending order is oldest-first.
    backups.sort();
    let remove_count = backups.len() - max_versions;
    for old in backups.into_iter().take(remove_count) {
        if let Err(e) = tokio::fs::remove_file(&old).await {
            debug!("Failed to prune backup {:?}: {}", old, e);
        } else {
            debug!("Pruned old backup: {:?}", old);
        }
    }

    Ok(())
}

/// List a directory, optionally filtering entries whose name contains `pattern`.
/// Directories are sorted first, then alphabetically.
pub async fn list_directory(path: &str, pattern: Option<&str>) -> Result<Vec<DirEntry>> {
    let mut entries = Vec::new();
    let mut dir = tokio::fs::read_dir(path).await?;

    while let Some(entry) = dir.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();

        if let Some(pattern) = pattern {
            if !name.contains(pattern) {
                continue;
            }
        }

        let metadata = entry.metadata().await?;
        let modified = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs());

        entries.push(DirEntry {
            name,
            is_dir: metadata.is_dir(),
            size: metadata.len(),
            modified,
        });
    }

    entries.sort_by(|a, b| match (a.is_dir, b.is_dir) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.name.cmp(&b.name),
    });

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_list_directory() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        // Create test files
        fs::write(dir_path.join("file_a.txt"), "content a").unwrap();
        fs::write(dir_path.join("file_b.txt"), "content b").unwrap();
        fs::create_dir(dir_path.join("subdir")).unwrap();

        let entries = list_directory(dir_path.to_str().unwrap(), None)
            .await
            .unwrap();

        assert_eq!(entries.len(), 3);
        // Directories first
        assert!(entries[0].is_dir);
        assert_eq!(entries[0].name, "subdir");
        // Then files alphabetically
        assert_eq!(entries[1].name, "file_a.txt");
        assert_eq!(entries[2].name, "file_b.txt");
    }

    #[tokio::test]
    async fn test_list_directory_with_pattern() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        fs::write(dir_path.join("test_a.txt"), "a").unwrap();
        fs::write(dir_path.join("test_b.txt"), "b").unwrap();
        fs::write(dir_path.join("other.log"), "c").unwrap();

        let entries = list_directory(dir_path.to_str().unwrap(), Some("test"))
            .await
            .unwrap();

        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.name.contains("test")));
    }

    #[tokio::test]
    async fn test_create_backup() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        let backup_dir = dir_path.join("backups");

        // Create original file
        let original = dir_path.join("data.txt");
        fs::write(&original, "original content").unwrap();

        let backup_path = create_backup(
            original.to_str().unwrap(),
            backup_dir.to_str().unwrap(),
            3,
        )
        .await
        .unwrap();

        assert!(backup_path.exists());
        assert!(backup_path.to_str().unwrap().contains("data.txt."));
        assert!(backup_path.to_str().unwrap().ends_with(".bak"));

        let backup_content = fs::read_to_string(&backup_path).unwrap();
        assert_eq!(backup_content, "original content");
    }

    #[tokio::test]
    async fn test_backup_pruning() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        let backup_dir = dir_path.join("backups");
        fs::create_dir_all(&backup_dir).unwrap();

        let original = dir_path.join("data.txt");

        // Create 5 backups
        for i in 0..5 {
            fs::write(&original, format!("content {}", i)).unwrap();
            create_backup(
                original.to_str().unwrap(),
                backup_dir.to_str().unwrap(),
                3, // max 3 versions
            )
            .await
            .unwrap();
            // Small delay to ensure different timestamps
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        // Count remaining backups
        let count = fs::read_dir(&backup_dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_str()
                    .unwrap()
                    .ends_with(".bak")
            })
            .count();

        assert_eq!(count, 3, "Should have pruned to max 3 backups");
    }
}
