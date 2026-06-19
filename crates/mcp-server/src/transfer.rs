//! Host↔host file transfer: the progress registry and the receiver-side write.
//!
//! The bytes ride the ordinary peer command path (`relay_controller`), which
//! prefers the direct UDP channel and falls back to the relay — so there is no
//! bespoke UDP file protocol here. The *sender* orchestration lives in
//! `relay_controller` (it needs the connection's send primitives); this module
//! holds the shared progress store, the receiver write, and the checksum.

use crate::config::SecurityConfig;
use crate::safety;
use anyhow::{bail, Context, Result};
use base64::Engine;
use remote_agents_shared::{Command, CommandResult, TransferState, TransferStatus};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::RwLock;

/// In-memory registry of host↔host transfers this node initiated, polled via
/// `TransferGet`. Cheap to clone (shared `Arc` in `AgentState`).
#[derive(Default)]
pub struct TransferStore {
    inner: RwLock<HashMap<String, TransferStatus>>,
}

impl TransferStore {
    /// Register a new transfer of `total` bytes in the `Queued` state.
    pub fn start(&self, id: &str, total: u64) {
        self.inner.write().unwrap().insert(
            id.to_string(),
            TransferStatus {
                id: id.to_string(),
                state: TransferState::Queued,
                bytes: 0,
                total,
                error: None,
            },
        );
    }

    /// Record progress (and flip to `Running`).
    pub fn progress(&self, id: &str, bytes: u64) {
        if let Some(t) = self.inner.write().unwrap().get_mut(id) {
            t.state = TransferState::Running;
            t.bytes = bytes;
        }
    }

    /// Mark the transfer complete.
    pub fn done(&self, id: &str) {
        if let Some(t) = self.inner.write().unwrap().get_mut(id) {
            t.state = TransferState::Done;
            t.bytes = t.total;
        }
    }

    /// Mark the transfer failed with a reason.
    pub fn fail(&self, id: &str, error: impl Into<String>) {
        if let Some(t) = self.inner.write().unwrap().get_mut(id) {
            t.state = TransferState::Failed;
            t.error = Some(error.into());
        }
    }

    /// Look up a transfer's status.
    pub fn get(&self, id: &str) -> Option<TransferStatus> {
        self.inner.read().unwrap().get(id).cloned()
    }
}

/// Write one received slice to `dest_path` at `offset` (the destination side of
/// `SendFileTo`). At `offset == 0` the file is created/truncated; later slices
/// seek and overwrite in place. On `eof`, the whole file is hashed and compared
/// to `expected_sha256`. Path access is policy-gated; the caller enforces write
/// mode. Blocking — call from `spawn_blocking`.
pub fn receive_chunk(
    dest_path: &str,
    offset: u64,
    bytes_b64: &str,
    eof: bool,
    expected_sha256: Option<&str>,
    sec: &SecurityConfig,
) -> Result<()> {
    safety::check_path_allowed(dest_path, sec)?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(bytes_b64)
        .context("decode transfer chunk")?;

    // `offset` is peer-supplied; compute the end with checked arithmetic so a
    // near-`u64::MAX` offset can't wrap PAST the size guard and then `seek` to a
    // colossal position (which would balloon a sparse file and make the eof
    // sha256 read run effectively forever). Overflow ⇒ treat as over-limit.
    let end = offset
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| anyhow::anyhow!("transfer chunk offset overflow ({offset} + {})", bytes.len()))?;
    if sec.max_transfer_size > 0 && end > sec.max_transfer_size {
        bail!("transfer exceeds limit of {} bytes", sec.max_transfer_size);
    }

    let mut f = if offset == 0 {
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(dest_path)
            .with_context(|| format!("create {dest_path}"))?
    } else {
        std::fs::OpenOptions::new()
            .write(true)
            .open(dest_path)
            .with_context(|| format!("open {dest_path}"))?
    };
    f.seek(SeekFrom::Start(offset))?;
    f.write_all(&bytes)?;

    if eof {
        f.flush()?;
        drop(f);
        if let Some(expected) = expected_sha256 {
            let actual = sha256_file(dest_path)?;
            if !actual.eq_ignore_ascii_case(expected) {
                bail!("sha256 mismatch (got {actual}, expected {expected})");
            }
        }
    }
    Ok(())
}

/// Stream a local file to a peer as a sequence of `FileRecv` commands, updating
/// `store` as it goes. Transport-agnostic: `send_chunk` delivers one `FileRecv`
/// to the destination and returns its reply (the caller supplies the WS/UDP
/// peer-send for its connection type). Slice lengths come from the known `size`
/// (so the base64'd chunk needn't report its raw length); the whole-file SHA-256
/// rides the final slice for end-to-end verification.
#[allow(clippy::too_many_arguments)]
pub async fn stream_file<F, Fut>(
    store: &TransferStore,
    src_path: &str,
    dest_path: &str,
    transfer_id: &str,
    sec: &SecurityConfig,
    chunk: u64,
    size: u64,
    send_chunk: F,
) -> Result<()>
where
    F: Fn(Command) -> Fut,
    Fut: Future<Output = Result<CommandResult>>,
{
    let sha = {
        let sp = src_path.to_string();
        tokio::task::spawn_blocking(move || sha256_file(&sp))
            .await
            .map_err(|e| anyhow::anyhow!("hash failed: {e}"))??
    };
    let chunk = chunk.max(1);
    let mut offset = 0u64;
    loop {
        let want = chunk.min(size - offset);
        let (data, _) = {
            let (sp, sec2) = (src_path.to_string(), sec.clone());
            tokio::task::spawn_blocking(move || crate::files::read_chunk(&sp, offset, want, &sec2))
                .await
                .map_err(|e| anyhow::anyhow!("read chunk failed: {e}"))??
        };
        let eof = offset + want >= size;
        let cmd = Command::FileRecv {
            transfer_id: transfer_id.to_string(),
            dest_path: dest_path.to_string(),
            offset,
            bytes: data,
            eof,
            sha256: if eof { Some(sha.clone()) } else { None },
        };
        match send_chunk(cmd).await? {
            CommandResult::Ok => {}
            other => bail!("destination rejected chunk: {other:?}"),
        }
        offset += want;
        store.progress(transfer_id, offset);
        if eof {
            break;
        }
    }
    Ok(())
}

/// Lowercase-hex SHA-256 of a whole file (streamed, constant memory).
pub fn sha256_file(path: &str) -> Result<String> {
    let mut f = std::fs::File::open(path).with_context(|| format!("open {path}"))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex(&hasher.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sec() -> SecurityConfig {
        SecurityConfig::default()
    }

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    #[test]
    fn store_tracks_lifecycle() {
        let s = TransferStore::default();
        s.start("t1", 100);
        assert_eq!(s.get("t1").unwrap().state, TransferState::Queued);
        s.progress("t1", 40);
        let p = s.get("t1").unwrap();
        assert_eq!(p.state, TransferState::Running);
        assert_eq!(p.bytes, 40);
        s.done("t1");
        let d = s.get("t1").unwrap();
        assert_eq!(d.state, TransferState::Done);
        assert_eq!(d.bytes, 100);
        s.fail("t1", "boom");
        assert_eq!(s.get("t1").unwrap().state, TransferState::Failed);
    }

    #[test]
    fn receive_reassembles_chunks_and_verifies_sha() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");
        let dp = dest.to_string_lossy().to_string();

        // Binary payload across two chunks.
        let data: Vec<u8> = (0u8..=255).cycle().take(700).collect();
        let sha = {
            let mut h = Sha256::new();
            h.update(&data);
            hex(&h.finalize())
        };

        receive_chunk(&dp, 0, &b64(&data[0..400]), false, None, &sec()).unwrap();
        receive_chunk(&dp, 400, &b64(&data[400..700]), true, Some(&sha), &sec()).unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), data);
    }

    #[test]
    fn receive_rejects_sha_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");
        let dp = dest.to_string_lossy().to_string();
        let r = receive_chunk(&dp, 0, &b64(b"hello"), true, Some("deadbeef"), &sec());
        assert!(r.is_err());
    }

    // A peer-supplied `offset` near u64::MAX must not wrap past the size guard.
    // Without checked arithmetic this would seek to a colossal position and
    // balloon a sparse file; here it must be rejected before any file is touched.
    #[test]
    fn receive_rejects_overflowing_offset() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");
        let dp = dest.to_string_lossy().to_string();

        let r = receive_chunk(&dp, u64::MAX, &b64(b"x"), false, None, &sec());
        assert!(r.is_err(), "overflowing offset must be rejected");
        // The guard runs before any open/seek, so no file is created.
        assert!(!dest.exists(), "no file should be created on a rejected chunk");
    }

    // An in-range offset whose end exceeds the configured limit is rejected by
    // the size guard (the non-overflowing sibling of the case above).
    #[test]
    fn receive_rejects_over_limit_offset() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");
        let dp = dest.to_string_lossy().to_string();

        let mut cfg = sec();
        cfg.max_transfer_size = 1024;
        let r = receive_chunk(&dp, 1024, &b64(b"x"), false, None, &cfg);
        assert!(r.is_err(), "offset past the limit must be rejected");
        assert!(!dest.exists());
    }

    // Pipe stream_file's FileRecv chunks straight into the receiver (the closure
    // stands in for the network), so the whole sender→receiver core is exercised
    // without a relay.
    async fn round_trip(data: &[u8], chunk: u64) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.bin");
        let dst = dir.path().join("dst.bin");
        std::fs::write(&src, data).unwrap();

        let mut cfg = sec();
        cfg.transfer_chunk_size = chunk;
        let store = TransferStore::default();
        store.start("t", data.len() as u64);

        // The closure applies each FileRecv to its own dest_path (set by
        // stream_file), so it only needs the security config.
        let cfg_recv = cfg.clone();
        let send = move |cmd: Command| {
            let cfg = cfg_recv.clone();
            async move {
                match cmd {
                    Command::FileRecv { dest_path, offset, bytes, eof, sha256, .. } => {
                        receive_chunk(&dest_path, offset, &bytes, eof, sha256.as_deref(), &cfg)?;
                        Ok(CommandResult::Ok)
                    }
                    other => anyhow::bail!("unexpected command {other:?}"),
                }
            }
        };

        let dst_s = dst.to_string_lossy().to_string();
        stream_file(
            &store,
            &src.to_string_lossy(),
            &dst_s,
            "t",
            &cfg,
            chunk,
            data.len() as u64,
            send,
        )
        .await
        .unwrap();

        assert_eq!(
            store.get("t").unwrap().bytes,
            data.len() as u64,
            "progress should reach total"
        );
        std::fs::read(&dst).unwrap()
    }

    #[tokio::test]
    async fn stream_file_round_trips_binary_across_many_chunks() {
        // 5000 bytes of non-UTF8 data, 1 KiB chunks → 5 chunks.
        let data: Vec<u8> = (0u8..=255).cycle().take(5000).collect();
        assert_eq!(round_trip(&data, 1024).await, data);
    }

    #[tokio::test]
    async fn stream_file_round_trips_exact_multiple_and_empty() {
        // Size an exact multiple of the chunk (no short final slice).
        let data: Vec<u8> = (0u8..=255).cycle().take(2048).collect();
        assert_eq!(round_trip(&data, 1024).await, data);
        // Empty file → a single eof slice; receiver writes an empty file.
        assert_eq!(round_trip(b"", 1024).await, Vec::<u8>::new());
    }
}
