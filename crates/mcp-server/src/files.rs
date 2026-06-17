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
use remote_agents_shared::{FileMeta, SearchKind};
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
/// Returns `(bytes, eof)`. The whole-file size is bounded by `max_transfer_size`
/// (0 = unlimited). Backs [`read_chunk`], which base64's the result.
fn read_chunk_raw(
    path: &str,
    offset: u64,
    len: u64,
    sec: &SecurityConfig,
) -> Result<(Vec<u8>, bool)> {
    safety::check_path_allowed(path, sec)?;
    let md = std::fs::metadata(path).with_context(|| format!("stat {path}"))?;
    let size = md.len();
    if sec.max_transfer_size > 0 && size > sec.max_transfer_size {
        bail!(
            "File size {} bytes exceeds transfer limit of {} bytes",
            size,
            sec.max_transfer_size
        );
    }
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

/// Like [`read_chunk_raw`] but base64-encodes the slice for the JSON/WS path.
pub fn read_chunk(
    path: &str,
    offset: u64,
    len: u64,
    sec: &SecurityConfig,
) -> Result<(String, bool)> {
    let (buf, eof) = read_chunk_raw(path, offset, len, sec)?;
    let data = base64::engine::general_purpose::STANDARD.encode(&buf);
    Ok((data, eof))
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

/// Default search roots: configured `search_roots`, else home + common dirs.
fn default_roots(sec: &SecurityConfig) -> Vec<String> {
    if !sec.search_roots.is_empty() {
        return sec.search_roots.clone();
    }
    let mut roots = Vec::new();
    if let Some(home) = dirs::home_dir() {
        for sub in ["Pictures", "Documents", "Downloads", "Desktop"] {
            let p = home.join(sub);
            if p.is_dir() {
                roots.push(p.to_string_lossy().to_string());
            }
        }
        // Fall back to the whole home dir if none of the common subdirs exist.
        if roots.is_empty() {
            roots.push(home.to_string_lossy().to_string());
        }
    }
    roots
}

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
            // find <roots...> -type f -iname '*query*'
            let mut args: Vec<String> = roots.clone();
            args.push("-type".into());
            args.push("f".into());
            args.push("-iname".into());
            args.push(format!("*{query}*"));
            run_collecting("find", &args, max * 2, timeout)?
        }
        SearchKind::Content => {
            // grep -rIl -e query <roots...>   (-I skips binary, -l lists files)
            let mut args: Vec<String> = vec!["-rIl".into(), "-e".into(), query.into()];
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
