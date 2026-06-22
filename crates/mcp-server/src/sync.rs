//! Folder sync (`SyncDirTo`): diff a source directory tree against a
//! destination's manifest and decide what to transfer / delete. This module is
//! the pure, transport-free decision logic (unit-tested here); the async
//! orchestration that walks the tree, queries the peer, and streams the changed
//! files lives in `relay_controller` (it needs the connection's send
//! primitives), reusing [`crate::transfer::stream_file`].

use remote_agents_shared::ManifestEntry;

/// How to compare source vs. destination files.
#[derive(Debug, Clone, Copy)]
pub struct SyncOpts {
    /// Delete destination files absent from the source.
    pub delete: bool,
    /// Compare by SHA-256 instead of the size+mtime quick check.
    pub checksum: bool,
}

/// What a sync will do: the source files to (re)send and the destination
/// relative paths to delete.
#[derive(Debug, Default)]
pub struct SyncPlan {
    pub to_transfer: Vec<ManifestEntry>,
    pub to_delete: Vec<String>,
}

/// Decide the sync actions from the two manifests.
///
/// Quick check (default): a source file is sent when it's missing on the
/// destination, its size differs, or its mtime is **newer** than the
/// destination's. Comparing "newer than" (rather than "not equal") keeps repeat
/// syncs stable without preserving mtimes: after a copy the destination's
/// mtime is its write time, which is ≥ the source's mtime, so an unchanged file
/// is not resent on the next run. (Caveat, as with rsync's quick check: a
/// same-size edit backdated below the last sync time is missed — use `checksum`
/// for content-exact comparison. Large reverse clock skew between hosts can also
/// force a resend.)
///
/// `checksum` mode compares SHA-256 instead (manifests must have been built with
/// hashes); a missing hash on either side is treated as "changed".
pub fn diff_manifests(
    src: &[ManifestEntry],
    dest: &[ManifestEntry],
    dest_root_exists: bool,
    opts: &SyncOpts,
) -> SyncPlan {
    use std::collections::{HashMap, HashSet};

    let dest_by: HashMap<&str, &ManifestEntry> =
        dest.iter().map(|e| (e.rel_path.as_str(), e)).collect();

    let mut plan = SyncPlan::default();
    for s in src {
        let needs = match dest_by.get(s.rel_path.as_str()) {
            None => true,
            Some(d) => {
                if opts.checksum {
                    match (&s.sha256, &d.sha256) {
                        (Some(a), Some(b)) => !a.eq_ignore_ascii_case(b),
                        _ => true, // no hash to compare → resend to be safe
                    }
                } else {
                    s.size != d.size || s.mtime_ms > d.mtime_ms
                }
            }
        };
        if needs {
            plan.to_transfer.push(s.clone());
        }
    }

    if opts.delete && dest_root_exists {
        let src_set: HashSet<&str> = src.iter().map(|e| e.rel_path.as_str()).collect();
        for d in dest {
            if !src_set.contains(d.rel_path.as_str()) {
                plan.to_delete.push(d.rel_path.clone());
            }
        }
    }

    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(rel: &str, size: u64, mtime: u64) -> ManifestEntry {
        ManifestEntry { rel_path: rel.into(), size, mtime_ms: mtime, sha256: None }
    }
    fn eh(rel: &str, sha: &str) -> ManifestEntry {
        ManifestEntry { rel_path: rel.into(), size: 1, mtime_ms: 1, sha256: Some(sha.into()) }
    }
    const QUICK: SyncOpts = SyncOpts { delete: false, checksum: false };
    const QUICK_DEL: SyncOpts = SyncOpts { delete: true, checksum: false };
    const SUM: SyncOpts = SyncOpts { delete: false, checksum: true };

    #[test]
    fn missing_dir_transfers_everything() {
        let src = vec![e("a", 1, 1), e("b/c", 2, 2)];
        let plan = diff_manifests(&src, &[], false, &QUICK);
        assert_eq!(plan.to_transfer.len(), 2);
        assert!(plan.to_delete.is_empty());
    }

    #[test]
    fn unchanged_files_are_skipped() {
        // Source mtime is older than the destination's (its last write) — the
        // stable repeat-sync case.
        let src = vec![e("a", 10, 100)];
        let dest = vec![e("a", 10, 500)];
        let plan = diff_manifests(&src, &dest, true, &QUICK);
        assert!(plan.to_transfer.is_empty());
    }

    #[test]
    fn size_or_newer_mtime_triggers_transfer() {
        let src = vec![e("size", 11, 100), e("time", 10, 999), e("same", 10, 100)];
        let dest = vec![e("size", 10, 100), e("time", 10, 500), e("same", 10, 500)];
        let plan = diff_manifests(&src, &dest, true, &QUICK);
        let names: Vec<&str> = plan.to_transfer.iter().map(|e| e.rel_path.as_str()).collect();
        assert_eq!(names, vec!["size", "time"]);
    }

    #[test]
    fn delete_only_with_flag_and_existing_dest() {
        let src = vec![e("keep", 1, 1)];
        let dest = vec![e("keep", 1, 1), e("stale", 1, 1)];
        assert!(diff_manifests(&src, &dest, true, &QUICK).to_delete.is_empty());
        assert_eq!(diff_manifests(&src, &dest, true, &QUICK_DEL).to_delete, vec!["stale"]);
        // A non-existent dest has nothing to delete.
        assert!(diff_manifests(&src, &[], false, &QUICK_DEL).to_delete.is_empty());
    }

    #[test]
    fn checksum_compares_hashes_not_mtime() {
        let src = vec![eh("a", "AABB"), eh("b", "CCDD")];
        let dest = vec![eh("a", "aabb"), eh("b", "ffff")]; // a matches (case-insensitive)
        let plan = diff_manifests(&src, &dest, true, &SUM);
        let names: Vec<&str> = plan.to_transfer.iter().map(|e| e.rel_path.as_str()).collect();
        assert_eq!(names, vec!["b"]);
    }

    #[test]
    fn checksum_missing_hash_resends() {
        let src = vec![e("a", 1, 1)]; // no sha
        let dest = vec![eh("a", "aabb")];
        assert_eq!(diff_manifests(&src, &dest, true, &SUM).to_transfer.len(), 1);
    }
}
