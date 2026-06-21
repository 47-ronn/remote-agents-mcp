//! UDP peer-to-peer data channel for direct host communication.
//!
//! The WebSocket relay is used only for signaling (exchanging public endpoints);
//! actual data flows directly between peers over UDP. If UDP hole-punching fails
//! (e.g. symmetric NAT), the system falls back to WebSocket transport.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐   WS (signaling)   ┌─────────────┐
//! │  MCP Server │ ◄──────────────────►│    Relay    │
//! └──────┬──────┘                     └──────┬──────┘
//!        │                                   │
//!        │ UDP (data)                        │ WS
//!        │                                   │
//!        ▼                                   ▼
//! ┌─────────────┐   UDP (data)       ┌─────────────┐
//! │   Agent 1   │ ◄──────────────────►│   Agent 2   │
//! └─────────────┘                     └─────────────┘
//! ```
//!
//! # Hole Punching Protocol
//!
//! 1. Both peers bind a local UDP socket and determine their public endpoint
//!    via STUN or by having the relay echo back their source address.
//! 2. Peers exchange `UdpOffer` messages through the WS relay (signaling).
//! 3. Both peers simultaneously send UDP probe packets to each other's
//!    public endpoint to punch holes in NATs.
//! 4. Once bidirectional connectivity is confirmed, the channel is established.
//! 5. If probing fails after timeout, fall back to WS transport.
//!
//! # Reliable Transport
//!
//! For command/result payloads that need reliability, we implement a simple
//! ACK-based protocol on top of UDP with retransmission.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// A network endpoint (IP + port) for UDP connectivity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Endpoint {
    /// IPv4 or IPv6 address as string
    pub addr: std::net::IpAddr,
    /// UDP port
    pub port: u16,
}

impl Endpoint {
    pub fn new(addr: std::net::IpAddr, port: u16) -> Self {
        Self { addr, port }
    }

    pub fn to_socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.addr, self.port)
    }
}

/// Build our reflexive (public) UDP endpoint by combining the IP the relay
/// observed for our connection (`reflected`, from a `YourEndpoint` message —
/// only its IP is meaningful; the relay sees our TCP port, not the UDP one)
/// with `local`, the actual port our UDP socket bound. Returns `None` when the
/// relay never reflected an address, so offers fall back to local-only.
///
/// Correct for hosts with a public IP or behind a full-cone NAT — the common
/// case for the remote servers this project manages.
pub fn reflexive_endpoint(reflected: Option<Endpoint>, local: Endpoint) -> Option<Endpoint> {
    reflected.map(|r| Endpoint::new(r.addr, local.port))
}

impl From<SocketAddr> for Endpoint {
    fn from(addr: SocketAddr) -> Self {
        Self {
            addr: addr.ip(),
            port: addr.port(),
        }
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.addr, self.port)
    }
}

/// UDP channel offer sent through WebSocket signaling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpOffer {
    /// Unique channel ID (both peers must use the same ID)
    pub channel_id: String,
    /// Session ID of the peer making the offer
    pub from_session: String,
    /// Target agent or MCP session ID
    pub to_session: String,
    /// Local endpoint (behind NAT, may not be directly reachable)
    pub local_endpoint: Endpoint,
    /// Public endpoint as seen by the relay/STUN (reflexive address)
    pub public_endpoint: Option<Endpoint>,
    /// Random nonce for this offer (used in hole-punching probes)
    pub nonce: [u8; 16],
}

/// UDP channel answer sent in response to an offer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpAnswer {
    /// Channel ID (must match the offer)
    pub channel_id: String,
    /// Session ID of the answering peer
    pub from_session: String,
    /// Local endpoint
    pub local_endpoint: Endpoint,
    /// Public endpoint
    pub public_endpoint: Option<Endpoint>,
    /// Nonce for this answer
    pub nonce: [u8; 16],
    /// Whether the peer accepts UDP (false = force WS fallback)
    pub accepted: bool,
}

/// Result of UDP channel establishment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpChannelResult {
    /// Channel ID
    pub channel_id: String,
    /// Whether UDP channel was successfully established
    pub success: bool,
    /// Reason for failure (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// UDP packet types for the data channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UdpPacketType {
    /// Hole-punching probe (contains nonce)
    Probe = 0x01,
    /// Probe acknowledgment
    ProbeAck = 0x02,
    /// Keepalive ping
    Ping = 0x03,
    /// Keepalive pong
    Pong = 0x04,
    /// Reliable data packet (needs ACK)
    Data = 0x10,
    /// Acknowledgment for data packet
    DataAck = 0x11,
    /// Unreliable data packet (no ACK needed)
    DataUnreliable = 0x12,
    /// One fragment of a reliable message larger than `UDP_MAX_PAYLOAD`
    /// (needs ACK, like `Data`). Payload is prefixed with a [`FragmentHeader`].
    DataFragment = 0x13,
    /// Channel close
    Close = 0xFF,
}

impl TryFrom<u8> for UdpPacketType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Probe),
            0x02 => Ok(Self::ProbeAck),
            0x03 => Ok(Self::Ping),
            0x04 => Ok(Self::Pong),
            0x10 => Ok(Self::Data),
            0x11 => Ok(Self::DataAck),
            0x12 => Ok(Self::DataUnreliable),
            0x13 => Ok(Self::DataFragment),
            0xFF => Ok(Self::Close),
            _ => Err(()),
        }
    }
}

/// Wire format for UDP packets.
///
/// ```text
/// ┌───────────┬───────────┬──────────────┬─────────────────┐
/// │  Type(1)  │  Seq(4)   │  Length(2)   │  Payload(var)   │
/// └───────────┴───────────┴──────────────┴─────────────────┘
/// ```
///
/// All multi-byte integers are big-endian.
pub const UDP_HEADER_SIZE: usize = 7; // type(1) + seq(4) + len(2)
pub const UDP_MAX_PAYLOAD: usize = 1200; // Safe for most MTUs
pub const UDP_MAX_PACKET: usize = UDP_HEADER_SIZE + UDP_MAX_PAYLOAD;

/// Parsed UDP packet header.
#[derive(Debug, Clone, Copy)]
pub struct UdpPacketHeader {
    pub packet_type: UdpPacketType,
    pub sequence: u32,
    pub payload_len: u16,
}

impl UdpPacketHeader {
    /// Parse header from bytes. Returns None if invalid.
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < UDP_HEADER_SIZE {
            return None;
        }
        let packet_type = UdpPacketType::try_from(buf[0]).ok()?;
        let sequence = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        let payload_len = u16::from_be_bytes([buf[5], buf[6]]);
        Some(Self {
            packet_type,
            sequence,
            payload_len,
        })
    }

    /// Write header to buffer. Buffer must be at least UDP_HEADER_SIZE bytes.
    pub fn write(&self, buf: &mut [u8]) {
        buf[0] = self.packet_type as u8;
        buf[1..5].copy_from_slice(&self.sequence.to_be_bytes());
        buf[5..7].copy_from_slice(&self.payload_len.to_be_bytes());
    }
}

/// State of a UDP channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChannelState {
    /// Initial state, no connectivity yet
    #[default]
    Disconnected,
    /// Sent offer, waiting for answer
    Offering,
    /// Received offer, sent answer, probing
    Answering,
    /// Both peers probing for connectivity
    Probing,
    /// UDP channel established
    Connected,
    /// Hole-punching failed, using WS fallback
    Fallback,
    /// Channel closed
    Closed,
}

/// Configuration for UDP channel.
#[derive(Debug, Clone)]
pub struct UdpConfig {
    /// Timeout for hole-punching probes (ms)
    pub probe_timeout_ms: u64,
    /// Interval between probe packets (ms)
    pub probe_interval_ms: u64,
    /// Maximum probe attempts before giving up
    pub max_probe_attempts: u32,
    /// Keepalive interval (ms)
    pub keepalive_interval_ms: u64,
    /// Retransmission timeout for reliable packets (ms)
    pub rto_ms: u64,
    /// Maximum retransmissions before failure
    pub max_retransmissions: u32,
}

impl Default for UdpConfig {
    fn default() -> Self {
        Self {
            probe_timeout_ms: 5000,
            probe_interval_ms: 100,
            max_probe_attempts: 50,
            keepalive_interval_ms: 10000,
            rto_ms: 200,
            max_retransmissions: 5,
        }
    }
}

// ============================================================================
// Fragmentation
//
// Reliable messages whose (encrypted) body exceeds `UDP_MAX_PAYLOAD` are split
// into several `DataFragment` packets. Each fragment's UDP payload begins with
// a fixed [`FragmentHeader`] so the receiver can reassemble them by message id,
// independent of arrival order or duplicate retransmissions.
// ============================================================================

/// Size of the per-fragment sub-header: msg_id(4) + index(2) + count(2).
pub const FRAGMENT_HEADER_SIZE: usize = 8;
/// Max bytes of message body carried by a single fragment.
pub const FRAGMENT_CHUNK_SIZE: usize = UDP_MAX_PAYLOAD - FRAGMENT_HEADER_SIZE;

/// Per-fragment sub-header, prepended to each `DataFragment` payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentHeader {
    /// Identifies the logical message all fragments belong to.
    pub msg_id: u32,
    /// 0-based index of this fragment.
    pub index: u16,
    /// Total number of fragments in the message.
    pub count: u16,
}

impl FragmentHeader {
    /// Parse the sub-header from the start of a fragment payload, returning the
    /// header and the remaining chunk bytes. `None` if the buffer is too short.
    pub fn parse(buf: &[u8]) -> Option<(Self, &[u8])> {
        if buf.len() < FRAGMENT_HEADER_SIZE {
            return None;
        }
        let msg_id = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let index = u16::from_be_bytes([buf[4], buf[5]]);
        let count = u16::from_be_bytes([buf[6], buf[7]]);
        Some((Self { msg_id, index, count }, &buf[FRAGMENT_HEADER_SIZE..]))
    }

    fn write(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.msg_id.to_be_bytes());
        buf[4..6].copy_from_slice(&self.index.to_be_bytes());
        buf[6..8].copy_from_slice(&self.count.to_be_bytes());
    }
}

/// Application-level frame carried over an established UDP data channel.
///
/// Lets a command (with its bulk partition `data`) travel directly to an agent
/// over UDP instead of through the WS relay, and a result travel back. The
/// `payload`/`result` strings are the SAME AES-GCM E2E envelopes used on the WS
/// path ([`Command::encrypt`]/[`CommandResult::encrypt`]), so confidentiality is
/// identical regardless of transport. The UDP path is an optimization; WS is the
/// fallback when no channel is established.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UdpFrame {
    /// A command for the agent. `payload` is an encrypted [`crate::Command`].
    Command {
        request_id: String,
        from_session: String,
        payload: String,
    },
    /// A command result. `result` is an encrypted [`crate::CommandResult`].
    Result { request_id: String, result: String },
    /// A command error (decrypt/exec failure) for `request_id`.
    Error { request_id: String, error: String },
}

impl UdpFrame {
    /// Serialize to JSON bytes for transmission over a UDP channel.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Parse from bytes received over a UDP channel; `None` if malformed.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }
}

/// Largest message body that can be fragmented: the header's `count`/`index` are
/// `u16`, so a message is capped at `u16::MAX` fragments of `FRAGMENT_CHUNK_SIZE`
/// body bytes each. Bodies this large don't occur on current paths (the relay's
/// ~1 MiB frame limit is ≈ 880 fragments) — the cap exists so a future caller
/// can't silently overflow the count via an `as u16` wrap, which would corrupt
/// every fragment's `count` and break reassembly.
pub const MAX_FRAGMENTED_BODY: usize = (u16::MAX as usize) * FRAGMENT_CHUNK_SIZE;

/// Number of fragments [`split_into_fragments`] yields for a `len`-byte body —
/// always ≥ 1, since an empty body still travels as one (empty) fragment.
fn fragment_count(len: usize) -> usize {
    if len == 0 {
        1
    } else {
        len.div_ceil(FRAGMENT_CHUNK_SIZE)
    }
}

/// Split `body` into fragment payloads (each = [`FragmentHeader`] + chunk),
/// chunking at `FRAGMENT_CHUNK_SIZE`. Always yields at least one fragment.
///
/// Returns `None` when `body` would need more than `u16::MAX` fragments (i.e.
/// `body.len() > MAX_FRAGMENTED_BODY`): the count wouldn't fit the header's
/// `u16`, so rather than wrap silently we refuse to fragment it.
pub fn split_into_fragments(body: &[u8], msg_id: u32) -> Option<Vec<Vec<u8>>> {
    if fragment_count(body.len()) > u16::MAX as usize {
        return None;
    }
    let chunks: Vec<&[u8]> = if body.is_empty() {
        vec![&body[0..0]]
    } else {
        body.chunks(FRAGMENT_CHUNK_SIZE).collect()
    };
    // Provably fits `u16` now (guarded above), so no truncating cast.
    let count = chunks.len() as u16;
    let fragments = chunks
        .into_iter()
        .enumerate()
        .map(|(i, chunk)| {
            let header = FragmentHeader { msg_id, index: i as u16, count };
            let mut out = vec![0u8; FRAGMENT_HEADER_SIZE + chunk.len()];
            header.write(&mut out);
            out[FRAGMENT_HEADER_SIZE..].copy_from_slice(chunk);
            out
        })
        .collect();
    Some(fragments)
}

/// Default cap on the number of messages reassembled concurrently. A message
/// whose final fragment never arrives (the sender exhausts retransmissions, or a
/// peer floods distinct `msg_id`s with a single fragment each) would otherwise
/// linger forever — an unbounded leak / DoS vector. Past the cap we evict the
/// least-recently-touched partial message, which is the most likely abandoned.
/// 256 concurrent in-flight fragmented messages per channel is already far
/// beyond any real workload (the transfer layer chunks sequentially).
pub const MAX_PARTIAL_MESSAGES: usize = 256;

/// One message being reassembled.
#[derive(Debug)]
struct Partial {
    /// Expected fragment count (from the fragment sub-header).
    count: u16,
    /// index -> chunk bytes for the fragments seen so far.
    parts: std::collections::HashMap<u16, Vec<u8>>,
    /// Monotonic tick of the most recent fragment for this message; used to
    /// evict the least-recently-touched message when over capacity.
    last_touched: u64,
}

/// Reassembles `DataFragment` payloads into complete messages. Tolerates
/// out-of-order arrival and duplicate fragments (retransmissions), and bounds
/// memory by evicting stale, never-completed messages past a capacity.
#[derive(Debug)]
pub struct FragmentReassembler {
    partial: std::collections::HashMap<u32, Partial>,
    /// Monotonic counter stamped onto each touched message (eviction ordering).
    tick: u64,
    /// Maximum number of in-flight partial messages before eviction kicks in.
    max_messages: usize,
}

impl Default for FragmentReassembler {
    fn default() -> Self {
        Self::with_capacity(MAX_PARTIAL_MESSAGES)
    }
}

impl FragmentReassembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct with an explicit cap on concurrent partial messages.
    pub fn with_capacity(max_messages: usize) -> Self {
        Self {
            partial: std::collections::HashMap::new(),
            tick: 0,
            max_messages: max_messages.max(1),
        }
    }

    /// Number of messages currently being reassembled (for bookkeeping/tests).
    pub fn pending_len(&self) -> usize {
        self.partial.len()
    }

    /// Drop the least-recently-touched partial message. Called when inserting a
    /// brand-new message id would exceed `max_messages`.
    fn evict_oldest(&mut self) {
        if let Some((&oldest_id, _)) = self
            .partial
            .iter()
            .min_by_key(|(_, p)| p.last_touched)
        {
            self.partial.remove(&oldest_id);
        }
    }

    /// Feed one fragment payload (sub-header + chunk). Returns the fully
    /// reassembled message body once the final missing fragment arrives,
    /// otherwise `None`. Malformed or inconsistent fragments are ignored.
    pub fn insert(&mut self, payload: &[u8]) -> Option<Vec<u8>> {
        let (header, chunk) = FragmentHeader::parse(payload)?;
        if header.count == 0 || header.index >= header.count {
            return None; // nonsensical framing
        }

        self.tick = self.tick.wrapping_add(1);
        let tick = self.tick;

        // Bound memory: if this fragment opens a *new* message and we are already
        // at capacity, evict the stalest one first so the map can't grow without
        // limit on never-completed messages.
        if !self.partial.contains_key(&header.msg_id) && self.partial.len() >= self.max_messages {
            self.evict_oldest();
        }

        let entry = self.partial.entry(header.msg_id).or_insert_with(|| Partial {
            count: header.count,
            parts: std::collections::HashMap::new(),
            last_touched: tick,
        });
        // A mismatched count for the same id means corruption — reset it.
        if entry.count != header.count {
            entry.count = header.count;
            entry.parts.clear();
        }
        entry.parts.insert(header.index, chunk.to_vec());
        entry.last_touched = tick;

        if entry.parts.len() as u16 != header.count {
            return None;
        }

        // All present — concatenate in index order and drop the buffer.
        let partial = self.partial.remove(&header.msg_id)?;
        let mut body = Vec::new();
        for i in 0..partial.count {
            body.extend_from_slice(partial.parts.get(&i)?);
        }
        Some(body)
    }
}

// ============================================================================
// STUN Discovery
// ============================================================================

/// Default public STUN servers for NAT traversal.
pub const STUN_SERVERS: &[&str] = &[
    "stun.l.google.com:19302",
    "stun1.l.google.com:19302",
    "stun.cloudflare.com:3478",
];

/// Discover our public UDP endpoint via STUN.
///
/// Sends a STUN Binding Request to a public STUN server and parses the
/// XOR-MAPPED-ADDRESS from the response to determine our public IP and port.
///
/// This is essential for UDP hole-punching when behind NAT.
#[cfg(feature = "udp")]
pub async fn stun_discover(
    socket: &tokio::net::UdpSocket,
    servers: &[&str],
    timeout: std::time::Duration,
) -> Option<Endpoint> {
    use tokio::time::timeout as tokio_timeout;

    for server in servers {
        // Resolve server address
        let addr: std::net::SocketAddr = match tokio::net::lookup_host(server).await {
            Ok(mut addrs) => match addrs.next() {
                Some(a) => a,
                None => continue,
            },
            Err(_) => continue,
        };

        // Build STUN Binding Request (RFC 5389)
        // Message Type: 0x0001 (Binding Request)
        // Message Length: 0 (no attributes)
        // Magic Cookie: 0x2112A442
        // Transaction ID: random 12 bytes
        let mut request = [0u8; 20];
        request[0..2].copy_from_slice(&0x0001u16.to_be_bytes()); // Type
        request[2..4].copy_from_slice(&0x0000u16.to_be_bytes()); // Length
        request[4..8].copy_from_slice(&0x2112A442u32.to_be_bytes()); // Magic Cookie
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut request[8..20]); // Transaction ID

        // Send request
        if socket.send_to(&request, addr).await.is_err() {
            continue;
        }

        // Wait for response
        let mut response = [0u8; 256];
        let recv_result = tokio_timeout(timeout, socket.recv_from(&mut response)).await;
        let (len, _from) = match recv_result {
            Ok(Ok(r)) => r,
            _ => continue,
        };

        // Parse STUN response
        if len < 20 {
            continue;
        }

        // Check message type (0x0101 = Binding Success Response)
        let msg_type = u16::from_be_bytes([response[0], response[1]]);
        if msg_type != 0x0101 {
            continue;
        }

        // Check magic cookie
        let magic = u32::from_be_bytes([response[4], response[5], response[6], response[7]]);
        if magic != 0x2112A442 {
            continue;
        }

        // Parse attributes to find XOR-MAPPED-ADDRESS (0x0020)
        let msg_len = u16::from_be_bytes([response[2], response[3]]) as usize;
        let mut offset = 20;
        while offset + 4 <= 20 + msg_len && offset + 4 <= len {
            let attr_type = u16::from_be_bytes([response[offset], response[offset + 1]]);
            let attr_len = u16::from_be_bytes([response[offset + 2], response[offset + 3]]) as usize;
            offset += 4;

            if offset + attr_len > len {
                break;
            }

            // XOR-MAPPED-ADDRESS (0x0020) or MAPPED-ADDRESS (0x0001)
            if (attr_type == 0x0020 || attr_type == 0x0001) && attr_len >= 8 {
                let family = response[offset + 1];
                if family == 0x01 {
                    // IPv4
                    let xor_port = u16::from_be_bytes([response[offset + 2], response[offset + 3]]);
                    let xor_ip = u32::from_be_bytes([
                        response[offset + 4],
                        response[offset + 5],
                        response[offset + 6],
                        response[offset + 7],
                    ]);

                    let (port, ip) = if attr_type == 0x0020 {
                        // XOR with magic cookie
                        (xor_port ^ 0x2112, xor_ip ^ 0x2112A442)
                    } else {
                        (xor_port, xor_ip)
                    };

                    let ip_addr = std::net::IpAddr::V4(std::net::Ipv4Addr::from(ip));
                    return Some(Endpoint::new(ip_addr, port));
                }
            }

            // Align to 4-byte boundary
            offset += (attr_len + 3) & !3;
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn endpoint_roundtrip() {
        let ep = Endpoint::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 12345);
        let json = serde_json::to_string(&ep).unwrap();
        let parsed: Endpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(ep, parsed);
    }

    #[test]
    fn packet_header_roundtrip() {
        let header = UdpPacketHeader {
            packet_type: UdpPacketType::Data,
            sequence: 0x12345678,
            payload_len: 100,
        };
        let mut buf = [0u8; UDP_HEADER_SIZE];
        header.write(&mut buf);
        let parsed = UdpPacketHeader::parse(&buf).unwrap();
        assert_eq!(parsed.packet_type, UdpPacketType::Data);
        assert_eq!(parsed.sequence, 0x12345678);
        assert_eq!(parsed.payload_len, 100);
    }

    #[test]
    fn packet_type_conversion() {
        assert_eq!(UdpPacketType::try_from(0x01), Ok(UdpPacketType::Probe));
        assert_eq!(UdpPacketType::try_from(0x10), Ok(UdpPacketType::Data));
        assert_eq!(
            UdpPacketType::try_from(0x13),
            Ok(UdpPacketType::DataFragment)
        );
        assert!(UdpPacketType::try_from(0x99).is_err());
    }

    #[test]
    fn fragment_header_roundtrip() {
        let h = FragmentHeader { msg_id: 0xDEADBEEF, index: 3, count: 9 };
        let mut buf = vec![0u8; FRAGMENT_HEADER_SIZE + 2];
        h.write(&mut buf);
        buf[FRAGMENT_HEADER_SIZE..].copy_from_slice(b"hi");
        let (parsed, chunk) = FragmentHeader::parse(&buf).unwrap();
        assert_eq!(parsed, h);
        assert_eq!(chunk, b"hi");
    }

    #[test]
    fn fragment_header_parse_rejects_short_buffer() {
        assert!(FragmentHeader::parse(&[0u8; FRAGMENT_HEADER_SIZE - 1]).is_none());
    }

    #[test]
    fn fragment_count_and_u16_boundary() {
        // Empty body still travels as one fragment; chunking is div_ceil.
        assert_eq!(fragment_count(0), 1);
        assert_eq!(fragment_count(1), 1);
        assert_eq!(fragment_count(FRAGMENT_CHUNK_SIZE), 1);
        assert_eq!(fragment_count(FRAGMENT_CHUNK_SIZE + 1), 2);
        // The largest fragmentable body is exactly u16::MAX fragments; one byte
        // more needs 65536, which can't fit the header's u16 count — so it's the
        // split/no-split boundary. (Asserted on the pure length math; allocating
        // an ~78 MiB body just to drive the branch isn't worth it.)
        assert_eq!(fragment_count(MAX_FRAGMENTED_BODY), u16::MAX as usize);
        assert!(fragment_count(MAX_FRAGMENTED_BODY + 1) > u16::MAX as usize);
    }

    #[test]
    fn single_fragment_for_small_body() {
        let frags = split_into_fragments(b"small", 1).expect("fits u16");
        assert_eq!(frags.len(), 1);
        let (h, chunk) = FragmentHeader::parse(&frags[0]).unwrap();
        assert_eq!(h.count, 1);
        assert_eq!(h.index, 0);
        assert_eq!(chunk, b"small");
    }

    #[test]
    fn split_then_reassemble_roundtrip() {
        // ~3.5 chunks worth of data.
        let body: Vec<u8> = (0..FRAGMENT_CHUNK_SIZE * 3 + 100)
            .map(|i| (i % 251) as u8)
            .collect();
        let frags = split_into_fragments(&body, 42).expect("fits u16");
        assert_eq!(frags.len(), 4);

        let mut r = FragmentReassembler::new();
        // Feed all but the last → still incomplete.
        for f in &frags[..frags.len() - 1] {
            assert!(r.insert(f).is_none());
        }
        let done = r.insert(&frags[frags.len() - 1]).expect("completes");
        assert_eq!(done, body);
        assert_eq!(r.pending_len(), 0);
    }

    #[test]
    fn reassemble_out_of_order_and_duplicates() {
        let body: Vec<u8> = (0..FRAGMENT_CHUNK_SIZE * 2 + 5).map(|i| i as u8).collect();
        let frags = split_into_fragments(&body, 7).expect("fits u16");
        assert_eq!(frags.len(), 3);

        let mut r = FragmentReassembler::new();
        // Reverse order, with a duplicate thrown in (simulating a retransmit).
        assert!(r.insert(&frags[2]).is_none());
        assert!(r.insert(&frags[2]).is_none()); // duplicate, harmless
        assert!(r.insert(&frags[0]).is_none());
        let done = r.insert(&frags[1]).expect("completes after last unique");
        assert_eq!(done, body);
    }

    #[test]
    fn udp_frame_roundtrips_each_variant() {
        let cases = vec![
            UdpFrame::Command {
                request_id: "r1".into(),
                from_session: "mcp-1".into(),
                payload: "ENC".into(),
            },
            UdpFrame::Result { request_id: "r1".into(), result: "ENCRES".into() },
            UdpFrame::Error { request_id: "r1".into(), error: "boom".into() },
        ];
        for f in cases {
            let bytes = f.to_bytes();
            assert!(!bytes.is_empty());
            assert_eq!(UdpFrame::from_bytes(&bytes), Some(f));
        }
    }

    #[test]
    fn reflexive_endpoint_combines_reflected_ip_with_local_port() {
        let reflected = Endpoint::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)), 55000); // TCP port
        let local = Endpoint::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 41234); // bound UDP port
        let public = reflexive_endpoint(Some(reflected), local).unwrap();
        assert_eq!(public.addr, IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7))); // reflected IP
        assert_eq!(public.port, 41234); // our UDP port, NOT the reflected TCP port
        // No reflection → None (offer falls back to local endpoint only).
        assert!(reflexive_endpoint(None, local).is_none());
    }

    #[test]
    fn udp_frame_from_bytes_rejects_garbage() {
        assert!(UdpFrame::from_bytes(b"not json").is_none());
        assert!(UdpFrame::from_bytes(b"{\"kind\":\"bogus\"}").is_none());
    }

    /// Build a single fragment (index 0 of `count`) for `msg_id` carrying one
    /// byte — enough to open a partial message without completing it.
    fn lone_fragment(msg_id: u32, count: u16) -> Vec<u8> {
        let mut b = vec![0u8; FRAGMENT_HEADER_SIZE + 1];
        FragmentHeader { msg_id, index: 0, count }.write(&mut b);
        b[FRAGMENT_HEADER_SIZE] = 0xAB;
        b
    }

    #[test]
    fn reassembler_bounds_partial_messages_by_evicting_oldest() {
        // A small cap makes the eviction observable. Each id contributes one
        // fragment of a 2-fragment message, so none ever completes.
        let mut r = FragmentReassembler::with_capacity(4);
        for id in 0..100u32 {
            assert!(r.insert(&lone_fragment(id, 2)).is_none());
            // Never exceeds the cap — the leak is bounded.
            assert!(r.pending_len() <= 4, "exceeded cap at id {id}");
        }
        assert_eq!(r.pending_len(), 4);
    }

    #[test]
    fn reassembler_eviction_keeps_recently_touched_message_alive() {
        // Capacity 2. Open msg 1, then keep feeding it while a churn of other
        // ids streams past; msg 1 stays "warm" and must survive to completion.
        let mut r = FragmentReassembler::with_capacity(2);
        let body: Vec<u8> = (0..FRAGMENT_CHUNK_SIZE + 10).map(|i| i as u8).collect();
        let frags = split_into_fragments(&body, 1).expect("fits u16");
        assert_eq!(frags.len(), 2);

        // Feed the first fragment of msg 1.
        assert!(r.insert(&frags[0]).is_none());

        // Churn other single-fragment messages through, re-touching msg 1 each
        // round so it is never the least-recently-touched candidate.
        for id in 10..40u32 {
            assert!(r.insert(&lone_fragment(id, 2)).is_none());
            // Re-touch msg 1 (duplicate of its first fragment — harmless).
            assert!(r.insert(&frags[0]).is_none());
        }

        // The final fragment of msg 1 must still complete it.
        let done = r.insert(&frags[1]).expect("warm message survived eviction");
        assert_eq!(done, body);
    }

    #[test]
    fn reassembler_ignores_nonsensical_framing() {
        let mut r = FragmentReassembler::new();
        // count = 0 is invalid.
        let bad = {
            let mut b = vec![0u8; FRAGMENT_HEADER_SIZE];
            FragmentHeader { msg_id: 1, index: 0, count: 0 }.write(&mut b);
            b
        };
        assert!(r.insert(&bad).is_none());
        assert_eq!(r.pending_len(), 0);
        // index >= count is invalid.
        let bad2 = {
            let mut b = vec![0u8; FRAGMENT_HEADER_SIZE];
            FragmentHeader { msg_id: 1, index: 5, count: 3 }.write(&mut b);
            b
        };
        assert!(r.insert(&bad2).is_none());
        assert_eq!(r.pending_len(), 0);
    }

    #[cfg(feature = "udp")]
    #[tokio::test]
    async fn stun_discovery_returns_public_endpoint() {
        use std::time::Duration;
        
        let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let local = socket.local_addr().unwrap();
        println!("Local socket: {}", local);
        
        let result = super::stun_discover(&socket, super::STUN_SERVERS, Duration::from_secs(3)).await;
        
        // Should succeed unless behind very restrictive firewall
        if let Some(ep) = result {
            println!("STUN discovered: {}:{}", ep.addr, ep.port);
            // Port should be non-zero
            assert!(ep.port > 0, "STUN should return non-zero port");
            // Address should be non-local (unless in special network config)
            assert!(!ep.addr.is_loopback(), "Should not be loopback");
        } else {
            println!("STUN discovery failed (may be blocked by firewall)");
        }
    }
}
