//! File primitives for the transfer/search feature: binary-safe metadata,
//! chunked reads, image thumbnails, and name/content search.
//!
//! These are blocking (std::fs / image decode / spawning `find`/`grep`); the
//! executor calls them from `spawn_blocking`. Path access is gated by
//! [`safety::check_path_allowed`] exactly like the in-band file ops.

use crate::config::SecurityConfig;
use crate::safety;
use anyhow::{bail, Context, Result};
use base64::Engine;
use remote_agents_shared::{FileMeta, ManifestEntry, SearchKind};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant, UNIX_EPOCH};

/// Best-effort MIME type from a path's extension.
pub fn mime_of(path: &str) -> String {
    mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string()
}

fn is_image_mime(mime: &str) -> bool {
    mime.starts_with("image/")
}

fn modified_ms(meta: &std::fs::Metadata) -> Option<u64> {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
}

/// Build a [`FileMeta`] for a path (no body read). Caller must have checked path
/// safety; `meta_for` re-checks so it's safe to call on raw search hits too.
fn meta_for(path: &str, sec: &SecurityConfig) -> Result<FileMeta> {
    safety::check_path_allowed(path, sec)?;
    let md = std::fs::metadata(path).with_context(|| format!("stat {path}"))?;
    let mime = mime_of(path);
    Ok(FileMeta {
        path: path.to_string(),
        size: md.len(),
        modified: modified_ms(&md),
        is_image: is_image_mime(&mime),
        mime,
    })
}

/// Metadata for one file (response to `FileStat`).
pub fn stat(path: &str, sec: &SecurityConfig) -> Result<FileMeta> {
    meta_for(path, sec)
}

/// Read a binary-safe slice `[offset, offset+len)` of a file as raw bytes.
/// Returns `(bytes, eof)`. This raw building block is transport-agnostic and
/// applies NO whole-file size cap: `max_transfer_size` bounds only the WS/relay
/// path, which is enforced by the base64 wrapper [`read_chunk`]. The direct
/// host↔host UDP transfer streams via this function ([`crate::transfer::stream_file`])
/// and is therefore uncapped — big files ride the direct channel unbounded.
pub(crate) fn read_chunk_raw(
    path: &str,
    offset: u64,
    len: u64,
    sec: &SecurityConfig,
) -> Result<(Vec<u8>, bool)> {
    safety::check_path_allowed(path, sec)?;
    let md = std::fs::metadata(path).with_context(|| format!("stat {path}"))?;
    let size = md.len();
    if offset > size {
        bail!("offset {} past end of file ({} bytes)", offset, size);
    }
    let remaining = size - offset;
    let to_read = len.min(remaining) as usize;

    let mut f = std::fs::File::open(path).with_context(|| format!("open {path}"))?;
    f.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; to_read];
    f.read_exact(&mut buf)?;

    let eof = offset + to_read as u64 >= size;
    Ok((buf, eof))
}

/// Like [`read_chunk_raw`] but base64-encodes the slice for the JSON/WS path
/// (the browser pull-download `FileChunk`). This is the WS/relay path, so it is
/// the one bounded by `max_transfer_size` (0 = unlimited) — the whole-file cap
/// the raw building block deliberately omits.
pub fn read_chunk(
    path: &str,
    offset: u64,
    len: u64,
    sec: &SecurityConfig,
) -> Result<(String, bool)> {
    if sec.max_transfer_size > 0 {
        let size = std::fs::metadata(path)
            .with_context(|| format!("stat {path}"))?
            .len();
        if size > sec.max_transfer_size {
            bail!(
                "File size {} bytes exceeds transfer limit of {} bytes",
                size,
                sec.max_transfer_size
            );
        }
    }
    let (buf, eof) = read_chunk_raw(path, offset, len, sec)?;
    let data = base64::engine::general_purpose::STANDARD.encode(&buf);
    Ok((data, eof))
}

/// Recursively list every regular file under `root`, relative to `root`, for a
/// directory manifest (folder sync). Symlinks are not followed (avoids directory
/// cycles and copying link targets as files). When `with_hash` is set each entry
/// carries its SHA-256. `exclude` holds gitignore-flavored patterns (see
/// [`crate::exclude`]): matching files are skipped and matching directories are
/// pruned before descending, so their contents are never read or hashed. The
/// root must be an existing directory; path access is gated like every file op.
/// Blocking — call from `spawn_blocking`.
pub fn walk_dir(
    root: &str,
    with_hash: bool,
    exclude: &[String],
    sec: &SecurityConfig,
) -> Result<Vec<ManifestEntry>> {
    safety::check_path_allowed(root, sec)?;
    let root_path = std::path::Path::new(root);
    if !root_path.is_dir() {
        bail!("{root} is not a directory");
    }
    let mut out = Vec::new();
    let mut stack = vec![root_path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rd = std::fs::read_dir(&dir).with_context(|| format!("read dir {}", dir.display()))?;
        for entry in rd {
            let path = entry?.path();
            // `symlink_metadata` so a symlink is classified as a link (skipped),
            // not as whatever it points at.
            let md = std::fs::symlink_metadata(&path)
                .with_context(|| format!("stat {}", path.display()))?;
            if md.file_type().is_symlink() {
                continue;
            }
            let rel = path
                .strip_prefix(root_path)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            if md.is_dir() {
                // Prune an excluded directory: don't descend, so nothing under it
                // is listed (or hashed in `checksum` mode).
                if !crate::exclude::dir_excluded(&rel, exclude) {
                    stack.push(path);
                }
            } else if md.is_file() {
                if crate::exclude::is_excluded(&rel, exclude) {
                    continue;
                }
                let sha256 = if with_hash {
                    Some(crate::transfer::sha256_file(&path.to_string_lossy())?)
                } else {
                    None
                };
                out.push(ManifestEntry {
                    rel_path: rel,
                    size: md.len(),
                    mtime_ms: modified_ms(&md).unwrap_or(0),
                    sha256,
                });
            }
        }
    }
    Ok(out)
}

/// Delete the given paths — the destination side of a folder-sync `--delete`.
/// Each path is access-gated; a missing path is ignored (idempotent). Only files
/// (and symlinks) are removed — directories are skipped, since the manifest
/// tracks files. Returns how many were actually removed. Blocking.
pub fn delete_paths(paths: &[String], sec: &SecurityConfig) -> Result<usize> {
    let mut removed = 0;
    for p in paths {
        safety::check_path_allowed(p, sec)?;
        match std::fs::symlink_metadata(p) {
            Ok(md) if md.is_file() || md.file_type().is_symlink() => {
                std::fs::remove_file(p).with_context(|| format!("remove {p}"))?;
                removed += 1;
            }
            // A directory isn't a manifest entry — leave it alone.
            Ok(_) => {}
            // Already gone — nothing to do.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(anyhow::Error::new(e).context(format!("stat {p}"))),
        }
    }
    Ok(removed)
}

/// Produce a downscaled JPEG preview of an image (longest side ≤ `max_px`),
/// returning `(base64_jpeg, width, height)`. Errors if the file is not a
/// decodable image (the web UI then shows a generic file icon).
pub fn thumbnail(path: &str, max_px: u32, sec: &SecurityConfig) -> Result<(String, u32, u32)> {
    safety::check_path_allowed(path, sec)?;
    let max_px = max_px.clamp(16, 1024);
    let img = image::open(path).with_context(|| format!("decode image {path}"))?;
    // `thumbnail` preserves aspect ratio, fitting within the max_px box.
    let thumb = img.thumbnail(max_px, max_px);
    let (w, h) = (thumb.width(), thumb.height());
    let mut buf = std::io::Cursor::new(Vec::new());
    thumb
        .write_to(&mut buf, image::ImageFormat::Jpeg)
        .context("encode thumbnail jpeg")?;
    let data = base64::engine::general_purpose::STANDARD.encode(buf.into_inner());
    Ok((data, w, h))
}

/// Default search roots: configured `search_roots`, else the whole home dir.
/// (Searching only Pictures/Documents/… missed everything else — e.g. a project
/// folder like `~/web_biz` — so a plain "find this file" came up empty. Heavy
/// dirs are pruned in `search` to keep `find` within the timeout.)
fn default_roots(sec: &SecurityConfig) -> Vec<String> {
    if !sec.search_roots.is_empty() {
        return sec.search_roots.clone();
    }
    dirs::home_dir()
        .map(|h| vec![h.to_string_lossy().to_string()])
        .unwrap_or_default()
}

/// Directory names pruned from `find` traversal: huge and rarely the target, so
/// skipping them keeps a home-wide search fast (esp. macOS `~/Library`).
const PRUNE_DIRS: &[&str] = &[
    "node_modules", ".git", "Library", ".cache", "Caches", ".cargo", ".rustup",
    ".npm", ".pnpm-store", "venv", ".venv", "__pycache__", ".Trash", "target",
];

/// Run a search program, collecting up to `max` newline-separated paths from its
/// stdout. Stops on any of: enough results, the program finishing, or `timeout`
/// elapsing — then kills the child so a huge tree (slow to traverse, few matches)
/// can neither hang nor run to completion. `timeout` of 0 disables the deadline.
///
/// stdout is drained on a helper thread so the deadline is enforced even while a
/// blocking read would otherwise stall between lines.
fn run_collecting(
    program: &str,
    args: &[String],
    max: usize,
    timeout: Duration,
) -> Result<Vec<String>> {
    let mut child = Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("spawn {program}"))?;

    let mut out = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        let (tx, rx) = mpsc::channel::<String>();
        // Reader thread: forwards lines until the pipe closes (child killed/done).
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });

        let deadline = (!timeout.is_zero()).then(|| Instant::now() + timeout);
        loop {
            if out.len() >= max {
                break;
            }
            let recv = match deadline {
                Some(d) => {
                    let remaining = d.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    rx.recv_timeout(remaining)
                }
                None => rx.recv().map_err(|_| mpsc::RecvTimeoutError::Disconnected),
            };
            match recv {
                Ok(line) => {
                    if !line.is_empty() {
                        out.push(line);
                    }
                }
                // Channel closed (reader finished) or deadline elapsed.
                Err(_) => break,
            }
        }
    }
    // Enough / done / timed out: stop the search and reap it.
    let _ = child.kill();
    let _ = child.wait();
    Ok(out)
}

/// Search for files under `roots` matching `query`. Uses POSIX `find`/`grep`
/// (present on Linux & macOS); args are passed as a vector (no shell), so the
/// query is not interpreted by a shell.
pub fn search(
    roots: &[String],
    query: &str,
    kind: SearchKind,
    sec: &SecurityConfig,
) -> Result<Vec<FileMeta>> {
    if query.trim().is_empty() {
        bail!("empty search query");
    }
    let roots: Vec<String> = if roots.is_empty() {
        default_roots(sec)
    } else {
        roots.to_vec()
    };
    // Only search roots the policy allows.
    let roots: Vec<String> = roots
        .into_iter()
        .filter(|r| safety::check_path_allowed(r, sec).is_ok())
        .collect();
    if roots.is_empty() {
        bail!("no allowed search roots");
    }

    let max = sec.search_max_results.max(1);
    let timeout = Duration::from_secs(sec.search_timeout_secs);
    let paths = match kind {
        SearchKind::Name | SearchKind::Image => {
            // find <roots...> ( -name node_modules -o … ) -prune -o -type f
            //   -iname '*query*' -print
            // The prune group skips heavy dirs; the trailing -print is required
            // once -prune/-o are in play (else pruned dirs would be printed too).
            let mut args: Vec<String> = roots.clone();
            args.push("(".into());
            for (i, d) in PRUNE_DIRS.iter().enumerate() {
                if i > 0 {
                    args.push("-o".into());
                }
                args.push("-name".into());
                args.push((*d).to_string());
            }
            args.push(")".into());
            args.push("-prune".into());
            args.push("-o".into());
            args.push("-type".into());
            args.push("f".into());
            args.push("-iname".into());
            args.push(format!("*{query}*"));
            args.push("-print".into());
            run_collecting("find", &args, max * 2, timeout)?
        }
        SearchKind::Content => {
            // grep -rIl --exclude-dir=<heavy> -e query <roots...>
            // (-I skips binary, -l lists files). Excluding the heavy dirs keeps a
            // home-wide content scan from drowning in Library/node_modules/.git;
            // both GNU and BSD grep support --exclude-dir.
            let mut args: Vec<String> = vec!["-rIl".into()];
            for d in PRUNE_DIRS {
                args.push(format!("--exclude-dir={d}"));
            }
            args.push("-e".into());
            args.push(query.into());
            args.extend(roots.clone());
            run_collecting("grep", &args, max, timeout)?
        }
    };

    let mut hits = Vec::new();
    for p in paths {
        let Ok(meta) = meta_for(&p, sec) else { continue };
        if kind == SearchKind::Image && !meta.is_image {
            continue;
        }
        hits.push(meta);
        if hits.len() >= max {
            break;
        }
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn sec() -> SecurityConfig {
        SecurityConfig::default()
    }

    #[test]
    fn walk_dir_lists_files_relative_with_sizes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("sub/deep")).unwrap();
        std::fs::write(dir.path().join("a.txt"), b"hello").unwrap();
        std::fs::write(dir.path().join("sub/b.txt"), b"hi").unwrap();
        std::fs::write(dir.path().join("sub/deep/c.bin"), b"xyz!").unwrap();

        let mut entries = walk_dir(&dir.path().to_string_lossy(), false, &[], &sec()).unwrap();
        entries.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        let rels: Vec<&str> = entries.iter().map(|e| e.rel_path.as_str()).collect();
        assert_eq!(rels, vec!["a.txt", "sub/b.txt", "sub/deep/c.bin"]);
        assert_eq!(entries[0].size, 5);
        assert!(entries.iter().all(|e| e.sha256.is_none())); // no hashing requested
    }

    #[test]
    fn walk_dir_prunes_excluded_dirs_and_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("node_modules/pkg")).unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/main.rs"), b"fn main(){}").unwrap();
        std::fs::write(dir.path().join("src/debug.log"), b"noise").unwrap();
        std::fs::write(dir.path().join("node_modules/pkg/index.js"), b"x").unwrap();
        std::fs::write(dir.path().join("keep.txt"), b"y").unwrap();

        let exclude = vec!["node_modules".to_string(), "*.log".to_string()];
        let mut entries =
            walk_dir(&dir.path().to_string_lossy(), false, &exclude, &sec()).unwrap();
        entries.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        let rels: Vec<&str> = entries.iter().map(|e| e.rel_path.as_str()).collect();
        assert_eq!(rels, vec!["keep.txt", "src/main.rs"]);
    }

    #[test]
    fn walk_dir_with_hash_sets_sha256() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"hello").unwrap();
        let entries = walk_dir(&dir.path().to_string_lossy(), true, &[], &sec()).unwrap();
        // SHA-256("hello")
        assert_eq!(
            entries[0].sha256.as_deref(),
            Some("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
        );
    }

    #[test]
    fn walk_dir_errors_on_missing_or_nondir() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope");
        assert!(walk_dir(&missing.to_string_lossy(), false, &[], &sec()).is_err());
    }

    #[test]
    fn delete_paths_removes_files_and_ignores_missing() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.txt");
        let b = dir.path().join("b.txt");
        std::fs::write(&a, b"x").unwrap();
        std::fs::write(&b, b"y").unwrap();
        let gone = dir.path().join("already-gone.txt");

        let removed = delete_paths(
            &[
                a.to_string_lossy().into_owned(),
                gone.to_string_lossy().into_owned(),
            ],
            &sec(),
        )
        .unwrap();
        assert_eq!(removed, 1); // only `a` existed
        assert!(!a.exists());
        assert!(b.exists()); // untouched
    }

    #[test]
    fn read_chunk_is_binary_safe_and_marks_eof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob.bin");
        // Bytes that are NOT valid UTF-8 (would break read_to_string).
        let data: Vec<u8> = (0u8..=255).cycle().take(1000).collect();
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&data)
            .unwrap();
        let p = path.to_string_lossy().to_string();

        // First 400 bytes, not eof.
        let (b64, eof) = read_chunk(&p, 0, 400, &sec()).unwrap();
        let got = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        assert_eq!(got, &data[0..400]);
        assert!(!eof);

        // Remaining bytes, eof. A len past the end is clamped.
        let (b64, eof) = read_chunk(&p, 400, 10_000, &sec()).unwrap();
        let got = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        assert_eq!(got, &data[400..1000]);
        assert!(eof);
    }

    #[test]
    fn read_chunk_rejects_offset_past_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x");
        std::fs::write(&path, b"abc").unwrap();
        let p = path.to_string_lossy().to_string();
        assert!(read_chunk(&p, 99, 10, &sec()).is_err());
    }

    // The WS/relay base64 path (`read_chunk`) still enforces `max_transfer_size`;
    // the transport-agnostic raw building block (`read_chunk_raw`) does not — so
    // a host↔host transfer of a file over the cap reads fine over the raw path.
    #[test]
    fn read_chunk_caps_ws_path_but_raw_is_uncapped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big");
        std::fs::write(&path, vec![7u8; 5000]).unwrap();
        let p = path.to_string_lossy().to_string();

        let mut cfg = sec();
        cfg.max_transfer_size = 1000; // file (5000) is 5× the cap

        assert!(
            read_chunk(&p, 0, 512, &cfg).is_err(),
            "base64/WS path must reject a file over the cap"
        );
        let (buf, _eof) = read_chunk_raw(&p, 0, 512, &cfg)
            .expect("raw path must ignore the cap for host↔host transfers");
        assert_eq!(buf.len(), 512);
    }

    #[test]
    fn stat_reports_mime_and_is_image() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("pic.JPG");
        std::fs::write(&img, b"not really a jpeg but ext drives mime").unwrap();
        let m = stat(&img.to_string_lossy(), &sec()).unwrap();
        assert_eq!(m.mime, "image/jpeg");
        assert!(m.is_image);

        let doc = dir.path().join("notes.txt");
        std::fs::write(&doc, b"hello").unwrap();
        let m = stat(&doc.to_string_lossy(), &sec()).unwrap();
        assert!(!m.is_image);
        assert_eq!(m.size, 5);
    }

    #[test]
    #[cfg(unix)]
    fn search_by_name_finds_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("vacation-photo.jpg"), b"x").unwrap();
        std::fs::write(dir.path().join("taxes.pdf"), b"y").unwrap();
        let roots = vec![dir.path().to_string_lossy().to_string()];

        let hits = search(&roots, "vacation", SearchKind::Name, &sec()).unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].path.ends_with("vacation-photo.jpg"));

        // Image kind filters out non-images even on a name match.
        std::fs::write(dir.path().join("vacation-notes.txt"), b"z").unwrap();
        let imgs = search(&roots, "vacation", SearchKind::Image, &sec()).unwrap();
        assert!(imgs.iter().all(|h| h.is_image));
        assert!(imgs.iter().any(|h| h.path.ends_with("vacation-photo.jpg")));
    }

    #[test]
    #[cfg(unix)]
    fn search_finds_nested_files_but_prunes_heavy_dirs() {
        let dir = tempfile::tempdir().unwrap();
        // A file in a normal project subdir must be found (the case that the
        // old Pictures/Documents-only default roots missed).
        std::fs::create_dir_all(dir.path().join("web_biz")).unwrap();
        std::fs::write(dir.path().join("web_biz/founders-playbook-ru.md"), b"x").unwrap();
        // A same-named file inside a pruned dir must NOT be returned.
        std::fs::create_dir_all(dir.path().join("node_modules/pkg")).unwrap();
        std::fs::write(dir.path().join("node_modules/pkg/founders-playbook-ru.md"), b"y").unwrap();
        let roots = vec![dir.path().to_string_lossy().to_string()];

        let hits = search(&roots, "founders-playbook", SearchKind::Name, &sec()).unwrap();
        assert_eq!(hits.len(), 1, "exactly the non-pruned match: {hits:?}");
        assert!(hits[0].path.ends_with("web_biz/founders-playbook-ru.md"));
        assert!(!hits[0].path.contains("node_modules"));
    }

    #[test]
    #[cfg(unix)]
    fn search_by_content_finds_matching_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"the needle is here\n").unwrap();
        std::fs::write(dir.path().join("b.txt"), b"nothing relevant\n").unwrap();
        let roots = vec![dir.path().to_string_lossy().to_string()];

        let hits = search(&roots, "needle", SearchKind::Content, &sec()).unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].path.ends_with("a.txt"));
    }

    #[test]
    #[cfg(unix)]
    fn search_by_content_excludes_heavy_dirs() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("src.txt"), b"the needle is here\n").unwrap();
        // Same content inside a pruned dir must be skipped (else a home-wide
        // content search drowns in node_modules/etc).
        std::fs::create_dir_all(dir.path().join("node_modules/pkg")).unwrap();
        std::fs::write(dir.path().join("node_modules/pkg/x.txt"), b"the needle is here\n").unwrap();
        let roots = vec![dir.path().to_string_lossy().to_string()];

        let hits = search(&roots, "needle", SearchKind::Content, &sec()).unwrap();
        assert_eq!(hits.len(), 1, "only the non-pruned match: {hits:?}");
        assert!(hits[0].path.ends_with("src.txt"));
    }

    #[test]
    #[cfg(unix)]
    fn run_collecting_honors_result_cap() {
        let out = run_collecting(
            "sh",
            &["-c".into(), "seq 100".into()],
            3,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(out.len(), 3, "should stop at the result cap");
        assert_eq!(out, vec!["1", "2", "3"]);
    }

    #[test]
    #[cfg(unix)]
    fn run_collecting_returns_all_lines_before_timeout() {
        let out = run_collecting(
            "sh",
            &["-c".into(), "printf 'a\\nb\\nc\\n'".into()],
            100,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(out, vec!["a", "b", "c"]);
    }

    #[test]
    #[cfg(unix)]
    fn run_collecting_aborts_on_timeout() {
        let start = Instant::now();
        // The match is printed only after a 5s sleep; a 300ms deadline must win.
        let out = run_collecting(
            "sh",
            &["-c".into(), "sleep 5; echo late".into()],
            10,
            Duration::from_millis(300),
        )
        .unwrap();
        assert!(out.is_empty(), "no output should arrive before the deadline");
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "must return at the deadline, not wait for the child ({}ms)",
            start.elapsed().as_millis()
        );
    }

    #[test]
    fn thumbnail_downscales_an_image() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.png");
        // Generate a real 200x200 PNG so the decoder accepts it.
        let img = image::RgbImage::from_fn(200, 200, |x, _| {
            image::Rgb([(x % 256) as u8, 0, 0])
        });
        image::DynamicImage::ImageRgb8(img)
            .save(&path)
            .unwrap();

        let (b64, w, h) = thumbnail(&path.to_string_lossy(), 64, &sec()).unwrap();
        assert!(w <= 64 && h <= 64);
        assert!(!b64.is_empty());
        // Output is a decodable JPEG.
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        assert!(image::load_from_memory(&bytes).is_ok());
    }
}
