//! Transparent zstd compression for end-to-end payloads (provider history,
//! file-transfer chunks, fleet results, …).
//!
//! Compression happens INSIDE the encrypted envelope — compress *then* encrypt,
//! since ciphertext is incompressible — and is gated by a size threshold so
//! small commands aren't touched. The result is **self-describing**: a payload
//! is either a raw JSON byte string or a zstd frame, distinguished on read by
//! the zstd magic bytes. JSON never starts with those bytes, so detection has no
//! false positives, and a reader transparently accepts uncompressed payloads
//! (small messages, or peers that don't compress). Pure-Rust via `ruzstd`.

use ruzstd::decoding::StreamingDecoder;
use ruzstd::encoding::{compress_to_vec, CompressionLevel};
use std::io::Read;

/// zstd frame magic, little-endian `0xFD2FB528`.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Payloads below this size aren't worth compressing (frame overhead + CPU).
const COMPRESS_THRESHOLD: usize = 512;

/// Hard ceiling on the *decompressed* size of an inbound zstd frame.
///
/// `maybe_decompress` runs on untrusted wire data (peers/relay can deliver any
/// frame that decrypts under the shared key). zstd is an amplification vector: a
/// frame within the relay's ~900 KB payload cap can expand to many gigabytes,
/// and a streaming `read_to_end` would allocate all of it — a memory-exhaustion
/// DoS. We bound the read instead. 128 MiB sits comfortably above the largest
/// legitimate single message (UDP's `MAX_FRAGMENTED_BODY` ≈ 78 MiB; the relay
/// TCP frame is far smaller) while turning the bomb into a clean error.
const MAX_DECOMPRESSED: usize = 128 * 1024 * 1024;

/// Whether `data` looks like a zstd frame (i.e. was produced by
/// [`maybe_compress`]).
fn is_zstd(data: &[u8]) -> bool {
    data.len() >= 4 && data[..4] == ZSTD_MAGIC
}

/// Compress `data` when it's large enough AND compression actually shrinks it;
/// otherwise return it unchanged. Output is self-describing (zstd frame or raw).
pub fn maybe_compress(data: &[u8]) -> Vec<u8> {
    if data.len() < COMPRESS_THRESHOLD {
        return data.to_vec();
    }
    let compressed = compress_to_vec(data, CompressionLevel::Fastest);
    if compressed.len() < data.len() {
        compressed
    } else {
        data.to_vec()
    }
}

/// Inverse of [`maybe_compress`]: decompress a zstd frame, or pass raw bytes
/// through unchanged.
pub fn maybe_decompress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    decompress_capped(data, MAX_DECOMPRESSED)
}

/// [`maybe_decompress`] with an explicit decompressed-size ceiling. Bounding the
/// read lets us prove the cap rejects bombs without allocating 128 MiB per test.
fn decompress_capped(data: &[u8], limit: usize) -> std::io::Result<Vec<u8>> {
    if !is_zstd(data) {
        return Ok(data.to_vec());
    }
    let decoder = StreamingDecoder::new(data)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    // Bound the read so a malicious frame can't expand without limit. `take`
    // reads at most `limit + 1` bytes, capping the allocation; if the extra byte
    // materialises the frame was over budget → reject it.
    let mut out = Vec::new();
    decoder.take(limit as u64 + 1).read_to_end(&mut out)?;
    if out.len() > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "decompressed payload exceeds maximum size",
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_payload_is_left_raw() {
        let data = b"{\"cmd\":\"get_info\"}";
        let out = maybe_compress(data);
        assert_eq!(out, data, "below threshold → unchanged");
        assert!(!is_zstd(&out));
        // And it round-trips through decompress untouched.
        assert_eq!(maybe_decompress(&out).unwrap(), data);
    }

    #[test]
    fn large_compressible_payload_round_trips_and_shrinks() {
        // Highly compressible (repetitive JSON-ish text), well over threshold.
        let data = "{\"role\":\"assistant\",\"text\":\"hello fleet \"}"
            .repeat(500)
            .into_bytes();
        let compressed = maybe_compress(&data);
        assert!(is_zstd(&compressed), "should be a zstd frame");
        assert!(compressed.len() < data.len() / 2, "should shrink substantially");
        assert_eq!(maybe_decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn incompressible_payload_stays_raw() {
        // High-entropy bytes (xorshift64) over the threshold: zstd can't shrink
        // them, so we keep the raw form (no wasted frame overhead).
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let data: Vec<u8> = (0..4096)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect();
        let out = maybe_compress(&data);
        assert!(!is_zstd(&out), "incompressible → kept raw");
        assert_eq!(out, data);
        assert_eq!(maybe_decompress(&out).unwrap(), data);
    }

    #[test]
    fn raw_json_passes_through_decompress() {
        // A reader must accept an uncompressed payload (legacy / small peer).
        let json = b"[1,2,3]";
        assert_eq!(maybe_decompress(json).unwrap(), json);
    }

    #[test]
    fn decompression_bomb_is_rejected_at_the_cap() {
        // A zstd "bomb": a tiny frame that expands far beyond its own size. zeros
        // compress to a handful of bytes, so this models an attacker frame well
        // within the relay's payload cap that would otherwise allocate ~1 MiB
        // (and, at the real 128 MiB ceiling, gigabytes) on decode.
        let bomb = maybe_compress(&vec![0u8; 1024 * 1024]);
        assert!(is_zstd(&bomb), "test premise: the frame is compressed");
        assert!(
            bomb.len() < 4096,
            "test premise: the frame is far smaller than its expansion"
        );

        // With a small limit the bomb is rejected (not OOM'd) ...
        let err = decompress_capped(&bomb, 64 * 1024).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        // ... and a frame whose output fits the limit still round-trips intact,
        // including exactly at the boundary (limit == output len).
        let exact = decompress_capped(&bomb, 1024 * 1024).unwrap();
        assert_eq!(exact.len(), 1024 * 1024);
        assert!(exact.iter().all(|&b| b == 0));
    }
}
