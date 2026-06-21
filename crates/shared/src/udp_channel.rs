//! UDP channel implementation with hole-punching and reliable transport.
//!
//! This module is only available with the `udp` feature.

#[cfg(feature = "udp")]
mod channel_impl {
    use crate::crypto::Cipher;
    use crate::udp::*;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::net::UdpSocket;
    use tokio::sync::{mpsc, Mutex, Notify, RwLock};

    // Congestion control / pacing bounds. The in-flight (unacked) packet count is
    // held to `cwnd`, which grows additively per ack and halves on loss (AIMD) —
    // so the sender never bursts more than the path + (small, non-root) receive
    // buffer can absorb, which is the main throughput limiter without big buffers.
    const MIN_CWND: usize = 4;
    const INIT_CWND: usize = 16;
    const MAX_CWND: usize = 256;
    /// Acks this far past an unacked packet imply it was lost → fast-retransmit.
    const DUPACK_THRESHOLD: u32 = 3;
    const RTO_FLOOR_MS: f64 = 50.0;
    const RTO_CEIL_MS: f64 = 4000.0;

    /// Error type for UDP channel operations.
    #[derive(Debug, thiserror::Error)]
    pub enum UdpChannelError {
        #[error("IO error: {0}")]
        Io(#[from] std::io::Error),
        #[error("Channel closed")]
        Closed,
        #[error("Hole-punching failed: {0}")]
        HolePunchFailed(String),
        #[error("Send timeout")]
        SendTimeout,
        #[error("Encryption error")]
        Crypto,
        #[error("Invalid packet")]
        InvalidPacket,
        #[error("Message too large to fragment")]
        MessageTooLarge,
    }

    /// A pending reliable packet awaiting acknowledgment.
    #[allow(dead_code)]
    struct PendingPacket {
        data: Vec<u8>,
        sequence: u32,
        sent_at: Instant,
        retries: u32,
        /// Set once we fast-retransmit this packet, so a burst of acks for later
        /// packets doesn't trigger repeated fast-retransmits of the same one.
        fast_rtx: bool,
    }

    /// A bidirectional UDP channel to a remote peer.
    pub struct UdpChannel {
        /// Unique channel ID
        pub channel_id: String,
        /// Local UDP socket
        socket: Arc<UdpSocket>,
        /// Remote peer's CONFIRMED endpoint (the candidate that answered the
        /// punch). `None` until hole-punching locks onto a working candidate.
        peer_endpoint: RwLock<Option<SocketAddr>>,
        /// Candidate peer endpoints to probe during hole-punching (local + public,
        /// ICE-style). Same-host/LAN peers connect via the local candidate; peers
        /// behind different NATs via the public one.
        peer_candidates: RwLock<Vec<SocketAddr>>,
        /// Channel state
        state: RwLock<ChannelState>,
        /// E2E encryption cipher
        cipher: Cipher,
        /// Outbound sequence number
        send_seq: Mutex<u32>,
        /// Pending reliable packets (seq -> packet)
        pending: Mutex<HashMap<u32, PendingPacket>>,
        /// Received data queue
        recv_tx: mpsc::Sender<Vec<u8>>,
        /// Configuration
        config: UdpConfig,
        /// Nonces for this channel (local, remote)
        local_nonce: [u8; 16],
        remote_nonce: RwLock<Option<[u8; 16]>>,
        /// Monotonic id assigned to each fragmented message.
        frag_msg_id: Mutex<u32>,
        /// Reassembly buffer for inbound `DataFragment` packets.
        reassembler: Mutex<FragmentReassembler>,
        /// Congestion window: max unacked reliable packets in flight (AIMD).
        cwnd: Mutex<usize>,
        /// Woken whenever a DataAck frees window space, so a paced sender resumes.
        acked: Notify,
        /// Smoothed RTT / its variation (ms), for an adaptive RTO (RFC 6298).
        srtt_ms: Mutex<Option<f64>>,
        rttvar_ms: Mutex<f64>,
    }

    impl UdpChannel {
        /// Create a new UDP channel.
        pub async fn new(
            channel_id: String,
            cipher: Cipher,
            config: UdpConfig,
        ) -> Result<(Self, mpsc::Receiver<Vec<u8>>), UdpChannelError> {
            // Bind to any available port, with large socket buffers so a burst of
            // reliable fragments (a 256 KiB slice is ~220 packets, and pipelined
            // slices burst several at once) isn't dropped by an undersized kernel
            // queue — the main cause of retransmit stalls. The kernel clamps to
            // net.core.{r,w}mem_max; we request generously and ignore failures.
            let socket = {
                use socket2::{Domain, Socket, Type};
                let s = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
                let _ = s.set_recv_buffer_size(8 * 1024 * 1024);
                let _ = s.set_send_buffer_size(8 * 1024 * 1024);
                // Windows honors the request above directly (no global cap); macOS
                // clamps to kern.ipc.maxsockbuf. Linux clamps SO_RCVBUF to
                // net.core.rmem_max (~208 KiB default) — SO_*BUFFORCE bypasses that
                // cap when we have CAP_NET_ADMIN (e.g. running as root via systemd),
                // so we get the large buffer without requiring a sysctl change.
                // Best-effort: silently ignored (EPERM) when unprivileged.
                #[cfg(target_os = "linux")]
                {
                    use std::os::unix::io::AsRawFd;
                    let fd = s.as_raw_fd();
                    let sz: libc::c_int = 8 * 1024 * 1024;
                    let p = &sz as *const libc::c_int as *const libc::c_void;
                    let len = std::mem::size_of_val(&sz) as libc::socklen_t;
                    unsafe {
                        libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUFFORCE, p, len);
                        libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUFFORCE, p, len);
                    }
                }
                s.set_nonblocking(true)?;
                s.bind(&"0.0.0.0:0".parse::<SocketAddr>().unwrap().into())?;
                UdpSocket::from_std(s.into())?
            };
            let socket = Arc::new(socket);

            let (recv_tx, recv_rx) = mpsc::channel(64);

            // Generate random nonce
            let mut local_nonce = [0u8; 16];
            rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut local_nonce);

            let channel = Self {
                channel_id,
                socket,
                peer_endpoint: RwLock::new(None),
                peer_candidates: RwLock::new(Vec::new()),
                state: RwLock::new(ChannelState::Disconnected),
                cipher,
                send_seq: Mutex::new(0),
                pending: Mutex::new(HashMap::new()),
                recv_tx,
                config,
                local_nonce,
                remote_nonce: RwLock::new(None),
                frag_msg_id: Mutex::new(0),
                reassembler: Mutex::new(FragmentReassembler::new()),
                cwnd: Mutex::new(INIT_CWND),
                acked: Notify::new(),
                srtt_ms: Mutex::new(None),
                rttvar_ms: Mutex::new(0.0),
            };

            Ok((channel, recv_rx))
        }

        /// Get the local endpoint (the raw bound address — `0.0.0.0:port` for a
        /// wildcard bind).
        pub fn local_endpoint(&self) -> Result<Endpoint, UdpChannelError> {
            Ok(self.socket.local_addr()?.into())
        }

        /// Local endpoint to ADVERTISE in an offer/answer for hole-punching. The
        /// socket binds `0.0.0.0`, so substitute the host's real egress IP when
        /// available — otherwise the peer gets `0.0.0.0` as a candidate and can't
        /// reach us on the LAN. Keeps the bound port.
        pub fn advertised_endpoint(&self) -> Result<Endpoint, UdpChannelError> {
            let ep = self.local_endpoint()?;
            if ep.addr.is_unspecified() {
                if let Some(ip) = crate::udp::local_egress_ip() {
                    return Ok(Endpoint::new(ip, ep.port));
                }
            }
            Ok(ep)
        }

        /// Discover our public endpoint via STUN.
        ///
        /// This should be called before sending an offer to determine our
        /// public IP and port for NAT traversal.
        pub async fn discover_public_endpoint(&self) -> Option<Endpoint> {
            crate::udp::stun_discover(
                &self.socket,
                crate::udp::STUN_SERVERS,
                Duration::from_secs(2),
            )
            .await
        }

        /// Get the local nonce for this channel.
        pub fn local_nonce(&self) -> [u8; 16] {
            self.local_nonce
        }

        /// Get current channel state.
        pub async fn state(&self) -> ChannelState {
            *self.state.read().await
        }

        /// Set a single known remote endpoint and nonce (from signaling). The
        /// endpoint is both the confirmed peer and the sole punch candidate.
        pub async fn set_peer(&self, endpoint: SocketAddr, nonce: [u8; 16]) {
            *self.peer_endpoint.write().await = Some(endpoint);
            *self.peer_candidates.write().await = vec![endpoint];
            *self.remote_nonce.write().await = Some(nonce);
        }

        /// Set multiple candidate endpoints (local + public) and the peer nonce.
        /// `peer_endpoint` stays `None` until `punch_hole` confirms which
        /// candidate is reachable. Empty/duplicate candidates are ignored.
        pub async fn set_peer_candidates(&self, candidates: Vec<SocketAddr>, nonce: [u8; 16]) {
            let mut deduped: Vec<SocketAddr> = Vec::with_capacity(candidates.len());
            for c in candidates {
                if !deduped.contains(&c) {
                    deduped.push(c);
                }
            }
            *self.peer_candidates.write().await = deduped;
            *self.remote_nonce.write().await = Some(nonce);
        }

        /// Adopt `from` as the confirmed peer endpoint when a payload authenticated
        /// it (successful E2E decrypt) and it differs from the current one. Lets the
        /// channel follow the peer's real source address across asymmetric
        /// multi-candidate punches and NAT/bridge rebinds.
        async fn adopt_peer(&self, from: SocketAddr) {
            if *self.peer_endpoint.read().await != Some(from) {
                *self.peer_endpoint.write().await = Some(from);
            }
        }

        /// Start hole-punching to establish connectivity. Probes EVERY candidate
        /// endpoint (local + public) each round and locks onto the first one that
        /// answers with a nonce-matching ProbeAck — so same-host/LAN peers connect
        /// via their local address and cross-NAT peers via the public one, with no
        /// a-priori guess about which is reachable.
        pub async fn punch_hole(&self) -> Result<(), UdpChannelError> {
            let candidates: Vec<SocketAddr> = {
                let cands = self.peer_candidates.read().await.clone();
                if !cands.is_empty() {
                    cands
                } else {
                    // Back-compat: fall back to a single confirmed endpoint.
                    match *self.peer_endpoint.read().await {
                        Some(p) => vec![p],
                        None => {
                            return Err(UdpChannelError::HolePunchFailed(
                                "peer endpoint not set".to_string(),
                            ))
                        }
                    }
                }
            };

            *self.state.write().await = ChannelState::Probing;

            let start = Instant::now();
            let timeout = Duration::from_millis(self.config.probe_timeout_ms);
            let interval = Duration::from_millis(self.config.probe_interval_ms);

            let mut attempts = 0u32;
            let mut probe_buf = [0u8; UDP_HEADER_SIZE + 16]; // header + nonce

            loop {
                if start.elapsed() > timeout {
                    *self.state.write().await = ChannelState::Fallback;
                    return Err(UdpChannelError::HolePunchFailed("timeout".to_string()));
                }

                if attempts >= self.config.max_probe_attempts {
                    *self.state.write().await = ChannelState::Fallback;
                    return Err(UdpChannelError::HolePunchFailed(
                        "max attempts exceeded".to_string(),
                    ));
                }

                // Send a probe to every candidate this round.
                let header = UdpPacketHeader {
                    packet_type: UdpPacketType::Probe,
                    sequence: attempts,
                    payload_len: 16,
                };
                header.write(&mut probe_buf);
                probe_buf[UDP_HEADER_SIZE..].copy_from_slice(&self.local_nonce);
                for cand in &candidates {
                    let _ = self.socket.send_to(&probe_buf, *cand).await;
                }
                attempts += 1;

                // Try to receive a probe / probe-ack. We accept packets from any
                // known candidate, AND from any source whose embedded nonce
                // authenticates it as our peer (peer-reflexive candidate) — this
                // is how we learn the peer's real source address when it differs
                // from what was signaled (NAT/bridge translation).
                let mut recv_buf = [0u8; UDP_MAX_PACKET];
                match tokio::time::timeout(interval, self.socket.recv_from(&mut recv_buf)).await {
                    Ok(Ok((len, from))) => {
                        if len >= UDP_HEADER_SIZE {
                            if let Some(h) = UdpPacketHeader::parse(&recv_buf[..len]) {
                                let expected = *self.remote_nonce.read().await;
                                // The nonce a Probe/ProbeAck carries, if present.
                                let carried: Option<[u8; 16]> = if len >= UDP_HEADER_SIZE + 16 {
                                    Some(
                                        recv_buf[UDP_HEADER_SIZE..UDP_HEADER_SIZE + 16]
                                            .try_into()
                                            .unwrap(),
                                    )
                                } else {
                                    None
                                };
                                let nonce_ok = carried.is_some() && carried == expected;
                                let known = candidates.contains(&from);
                                match h.packet_type {
                                    UdpPacketType::Probe if known || nonce_ok => {
                                        // Ack whoever probed us (on its source addr).
                                        let ack_header = UdpPacketHeader {
                                            packet_type: UdpPacketType::ProbeAck,
                                            sequence: h.sequence,
                                            payload_len: 16,
                                        };
                                        let mut ack_buf = [0u8; UDP_HEADER_SIZE + 16];
                                        ack_header.write(&mut ack_buf);
                                        ack_buf[UDP_HEADER_SIZE..].copy_from_slice(&self.local_nonce);
                                        let _ = self.socket.send_to(&ack_buf, from).await;
                                        // An authenticated probe means `from` is a
                                        // working return path — lock onto it.
                                        if nonce_ok {
                                            *self.peer_endpoint.write().await = Some(from);
                                            *self.state.write().await = ChannelState::Connected;
                                            return Ok(());
                                        }
                                    }
                                    UdpPacketType::ProbeAck if nonce_ok => {
                                        // Connected once the peer echoes our expected
                                        // nonce — lock onto the address that answered.
                                        *self.peer_endpoint.write().await = Some(from);
                                        *self.state.write().await = ChannelState::Connected;
                                        return Ok(());
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => return Err(e.into()),
                    Err(_) => {} // Timeout, continue probing
                }
            }
        }

        /// Send reliable data (will be retransmitted until ACKed).
        pub async fn send_reliable(&self, data: &[u8]) -> Result<(), UdpChannelError> {
            if *self.state.read().await != ChannelState::Connected {
                return Err(UdpChannelError::Closed);
            }

            let peer = self
                .peer_endpoint
                .read()
                .await
                .ok_or(UdpChannelError::Closed)?;

            // Encrypt payload
            let encrypted = self
                .cipher
                .encrypt(data)
                .map_err(|_| UdpChannelError::Crypto)?;
            let encrypted_bytes = encrypted.as_bytes();

            // Small payloads go as a single Data packet (the common case).
            if encrypted_bytes.len() <= UDP_MAX_PAYLOAD {
                self.send_reliable_packet(UdpPacketType::Data, encrypted_bytes, peer)
                    .await?;
                return Ok(());
            }

            // Larger payloads are split into DataFragment packets, each
            // independently ACKed/retransmitted. The receiver reassembles by
            // the shared message id before decrypting.
            let msg_id = {
                let mut id = self.frag_msg_id.lock().await;
                let v = *id;
                *id = id.wrapping_add(1);
                v
            };
            let fragments = split_into_fragments(encrypted_bytes, msg_id)
                .ok_or(UdpChannelError::MessageTooLarge)?;
            for fragment in fragments {
                self.send_reliable_packet(UdpPacketType::DataFragment, &fragment, peer)
                    .await?;
            }
            Ok(())
        }

        /// Build a reliable packet of `packet_type`, register it for
        /// retransmission, and send it. Caller supplies the full UDP payload.
        /// Fold a fresh RTT sample into SRTT/RTTVAR (RFC 6298).
        async fn update_rtt(&self, sample: std::time::Duration) {
            let r = sample.as_secs_f64() * 1000.0;
            let mut srtt = self.srtt_ms.lock().await;
            let mut var = self.rttvar_ms.lock().await;
            match *srtt {
                None => {
                    *srtt = Some(r);
                    *var = r / 2.0;
                }
                Some(s) => {
                    *var = 0.75 * *var + 0.25 * (s - r).abs();
                    *srtt = Some(0.875 * s + 0.125 * r);
                }
            }
        }

        /// Current retransmission timeout from the RTT estimate, clamped. Falls
        /// back to the static config value until we have a first sample.
        async fn current_rto(&self) -> Duration {
            match *self.srtt_ms.lock().await {
                Some(s) => {
                    let v = *self.rttvar_ms.lock().await;
                    Duration::from_millis((s + 4.0 * v).clamp(RTO_FLOOR_MS, RTO_CEIL_MS) as u64)
                }
                None => Duration::from_millis(self.config.rto_ms),
            }
        }

        async fn send_reliable_packet(
            &self,
            packet_type: UdpPacketType,
            payload: &[u8],
            peer: SocketAddr,
        ) -> Result<(), UdpChannelError> {
            // Flow control: don't put more than `cwnd` packets in flight. Wait for
            // a DataAck to free a slot (or a short timer, in case acks were lost).
            loop {
                let inflight = self.pending.lock().await.len();
                let cwnd = *self.cwnd.lock().await;
                if inflight < cwnd {
                    break;
                }
                if *self.state.read().await == ChannelState::Closed {
                    return Err(UdpChannelError::Closed);
                }
                tokio::select! {
                    _ = self.acked.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                }
            }

            let seq = {
                let mut s = self.send_seq.lock().await;
                let seq = *s;
                *s = s.wrapping_add(1);
                seq
            };

            let mut packet = vec![0u8; UDP_HEADER_SIZE + payload.len()];
            let header = UdpPacketHeader {
                packet_type,
                sequence: seq,
                payload_len: payload.len() as u16,
            };
            header.write(&mut packet);
            packet[UDP_HEADER_SIZE..].copy_from_slice(payload);

            {
                let mut pending = self.pending.lock().await;
                pending.insert(
                    seq,
                    PendingPacket {
                        data: packet.clone(),
                        sequence: seq,
                        sent_at: Instant::now(),
                        retries: 0,
                        fast_rtx: false,
                    },
                );
            }

            self.socket.send_to(&packet, peer).await?;
            Ok(())
        }

        /// Send unreliable data (fire and forget).
        pub async fn send_unreliable(&self, data: &[u8]) -> Result<(), UdpChannelError> {
            if *self.state.read().await != ChannelState::Connected {
                return Err(UdpChannelError::Closed);
            }

            let peer = self
                .peer_endpoint
                .read()
                .await
                .ok_or(UdpChannelError::Closed)?;

            // Encrypt payload
            let encrypted = self
                .cipher
                .encrypt(data)
                .map_err(|_| UdpChannelError::Crypto)?;
            let encrypted_bytes = encrypted.as_bytes();

            if encrypted_bytes.len() > UDP_MAX_PAYLOAD {
                return Err(UdpChannelError::InvalidPacket);
            }

            // Build packet (sequence not meaningful for unreliable)
            let mut packet = vec![0u8; UDP_HEADER_SIZE + encrypted_bytes.len()];
            let header = UdpPacketHeader {
                packet_type: UdpPacketType::DataUnreliable,
                sequence: 0,
                payload_len: encrypted_bytes.len() as u16,
            };
            header.write(&mut packet);
            packet[UDP_HEADER_SIZE..].copy_from_slice(encrypted_bytes);

            self.socket.send_to(&packet, peer).await?;
            Ok(())
        }

        /// Process incoming packets. Call this in a loop.
        pub async fn recv_loop(&self) -> Result<(), UdpChannelError> {
            let mut buf = [0u8; UDP_MAX_PACKET];

            loop {
                let state = *self.state.read().await;
                if state == ChannelState::Closed {
                    return Ok(());
                }

                let (len, from) = self.socket.recv_from(&mut buf).await?;

                // We deliberately do NOT drop packets whose source differs from
                // the confirmed peer endpoint. With multi-candidate punching the
                // two ends can lock onto asymmetric addresses, and NAT/bridge
                // rebinding can move the source mid-stream. Application payloads
                // are E2E-encrypted (AES-GCM), so a successful decrypt — not the
                // source address — authenticates the peer; on a valid decrypt we
                // ADOPT `from` as the peer endpoint (below) so our sends follow
                // the peer's real address. Control frames are answered to `from`.
                if len < UDP_HEADER_SIZE {
                    continue;
                }

                let header = match UdpPacketHeader::parse(&buf[..len]) {
                    Some(h) => h,
                    None => continue,
                };

                match header.packet_type {
                    UdpPacketType::Data => {
                        // Send ACK
                        let ack = UdpPacketHeader {
                            packet_type: UdpPacketType::DataAck,
                            sequence: header.sequence,
                            payload_len: 0,
                        };
                        let mut ack_buf = [0u8; UDP_HEADER_SIZE];
                        ack.write(&mut ack_buf);
                        let _ = self.socket.send_to(&ack_buf, from).await;

                        // Decrypt and deliver
                        if len > UDP_HEADER_SIZE {
                            let encrypted =
                                String::from_utf8_lossy(&buf[UDP_HEADER_SIZE..len]).to_string();
                            if let Ok(decrypted) = self.cipher.decrypt(&encrypted) {
                                self.adopt_peer(from).await;
                                let _ = self.recv_tx.send(decrypted).await;
                            }
                        }
                    }

                    UdpPacketType::DataFragment => {
                        // ACK each fragment individually (same as Data).
                        let ack = UdpPacketHeader {
                            packet_type: UdpPacketType::DataAck,
                            sequence: header.sequence,
                            payload_len: 0,
                        };
                        let mut ack_buf = [0u8; UDP_HEADER_SIZE];
                        ack.write(&mut ack_buf);
                        let _ = self.socket.send_to(&ack_buf, from).await;

                        // Buffer; once the whole message is present, decrypt it.
                        if len > UDP_HEADER_SIZE {
                            let completed = {
                                let mut r = self.reassembler.lock().await;
                                r.insert(&buf[UDP_HEADER_SIZE..len])
                            };
                            if let Some(body) = completed {
                                let encrypted = String::from_utf8_lossy(&body).to_string();
                                if let Ok(decrypted) = self.cipher.decrypt(&encrypted) {
                                    self.adopt_peer(from).await;
                                    let _ = self.recv_tx.send(decrypted).await;
                                }
                            }
                        }
                    }

                    UdpPacketType::DataUnreliable => {
                        // Decrypt and deliver
                        if len > UDP_HEADER_SIZE {
                            let encrypted =
                                String::from_utf8_lossy(&buf[UDP_HEADER_SIZE..len]).to_string();
                            if let Ok(decrypted) = self.cipher.decrypt(&encrypted) {
                                self.adopt_peer(from).await;
                                let _ = self.recv_tx.send(decrypted).await;
                            }
                        }
                    }

                    UdpPacketType::DataAck => {
                        let acked_seq = header.sequence;
                        let mut rtt_sample = None;
                        let mut fast: Vec<Vec<u8>> = Vec::new();
                        {
                            let mut pending = self.pending.lock().await;
                            if let Some(pkt) = pending.remove(&acked_seq) {
                                // Karn's algorithm: only sample RTT from packets we
                                // never retransmitted (else the ack is ambiguous).
                                if pkt.retries == 0 {
                                    rtt_sample = Some(pkt.sent_at.elapsed());
                                }
                            }
                            // Fast-retransmit: any still-pending packet whose seq is
                            // well behind the one just acked has almost certainly
                            // been lost — resend it now instead of waiting an RTO.
                            for pkt in pending.values_mut() {
                                if !pkt.fast_rtx
                                    && pkt.sequence < acked_seq
                                    && acked_seq - pkt.sequence > DUPACK_THRESHOLD
                                {
                                    pkt.fast_rtx = true;
                                    pkt.retries += 1;
                                    pkt.sent_at = Instant::now();
                                    fast.push(pkt.data.clone());
                                }
                            }
                        }
                        if let Some(s) = rtt_sample {
                            self.update_rtt(s).await;
                        }
                        // Additive increase on a good ack.
                        {
                            let mut c = self.cwnd.lock().await;
                            *c = (*c + 1).min(MAX_CWND);
                        }
                        if !fast.is_empty() {
                            for data in &fast {
                                let _ = self.socket.send_to(data, from).await;
                            }
                            // Loss signal → multiplicative decrease (once).
                            let mut c = self.cwnd.lock().await;
                            *c = (*c / 2).max(MIN_CWND);
                        }
                        self.acked.notify_waiters();
                    }

                    UdpPacketType::Ping => {
                        let pong = UdpPacketHeader {
                            packet_type: UdpPacketType::Pong,
                            sequence: header.sequence,
                            payload_len: 0,
                        };
                        let mut pong_buf = [0u8; UDP_HEADER_SIZE];
                        pong.write(&mut pong_buf);
                        let _ = self.socket.send_to(&pong_buf, from).await;
                    }

                    UdpPacketType::Close => {
                        *self.state.write().await = ChannelState::Closed;
                        return Ok(());
                    }

                    _ => {}
                }
            }
        }

        /// Retransmission loop for reliable packets.
        pub async fn retransmit_loop(&self) -> Result<(), UdpChannelError> {
            loop {
                // Fine-grained tick; each packet is judged against the current
                // adaptive RTO rather than one fixed interval.
                tokio::time::sleep(Duration::from_millis(25)).await;

                let state = *self.state.read().await;
                if state == ChannelState::Closed {
                    return Ok(());
                }

                let peer = match *self.peer_endpoint.read().await {
                    Some(p) => p,
                    None => continue,
                };

                let rto = self.current_rto().await;
                let mut had_loss = false;
                {
                    let mut pending = self.pending.lock().await;
                    let mut to_remove = Vec::new();
                    for (seq, pkt) in pending.iter_mut() {
                        if pkt.sent_at.elapsed() > rto {
                            if pkt.retries >= self.config.max_retransmissions {
                                to_remove.push(*seq);
                            } else {
                                let _ = self.socket.send_to(&pkt.data, peer).await;
                                pkt.retries += 1;
                                pkt.sent_at = Instant::now();
                                pkt.fast_rtx = false;
                                had_loss = true;
                            }
                        }
                    }
                    for seq in to_remove {
                        pending.remove(&seq);
                    }
                }
                if had_loss {
                    // Timeout is a strong congestion signal: back off (once/tick).
                    let mut c = self.cwnd.lock().await;
                    *c = (*c / 2).max(MIN_CWND);
                }
                // A retransmit may have abandoned packets (freeing window) — wake
                // any paced sender so it re-evaluates.
                self.acked.notify_waiters();
            }
        }

        /// Close the channel.
        pub async fn close(&self) -> Result<(), UdpChannelError> {
            if let Some(peer) = *self.peer_endpoint.read().await {
                let header = UdpPacketHeader {
                    packet_type: UdpPacketType::Close,
                    sequence: 0,
                    payload_len: 0,
                };
                let mut buf = [0u8; UDP_HEADER_SIZE];
                header.write(&mut buf);
                let _ = self.socket.send_to(&buf, peer).await;
            }
            *self.state.write().await = ChannelState::Closed;
            Ok(())
        }

        /// Test-only: force the channel straight to `Connected` with `peer`,
        /// bypassing the hole-punching handshake. Lets unit tests exercise the
        /// data path over loopback without real NAT traversal.
        #[cfg(test)]
        pub async fn force_connected_to(&self, peer: SocketAddr) {
            *self.peer_endpoint.write().await = Some(peer);
            *self.state.write().await = ChannelState::Connected;
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::net::{IpAddr, Ipv4Addr};

        /// Build the loopback peer address from a channel's bound port.
        fn loopback_peer(ch: &UdpChannel) -> SocketAddr {
            let port = ch.local_endpoint().unwrap().port;
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
        }

        /// Wire two channels to each other over loopback, both Connected, with
        /// their recv/retransmit loops running. Shared cipher so each can
        /// decrypt the other's payloads.
        async fn connected_pair() -> (Arc<UdpChannel>, Arc<UdpChannel>, mpsc::Receiver<Vec<u8>>) {
            let cipher = Cipher::from_passphrase("udp-test-key");
            let (a, _a_rx) = UdpChannel::new("ch".into(), cipher.clone(), UdpConfig::default())
                .await
                .unwrap();
            let (b, b_rx) = UdpChannel::new("ch".into(), cipher, UdpConfig::default())
                .await
                .unwrap();
            let (a, b) = (Arc::new(a), Arc::new(b));

            let (a_peer, b_peer) = (loopback_peer(&a), loopback_peer(&b));
            a.force_connected_to(b_peer).await;
            b.force_connected_to(a_peer).await;

            for ch in [a.clone(), b.clone()] {
                let recv = ch.clone();
                tokio::spawn(async move { recv.recv_loop().await });
                tokio::spawn(async move { ch.retransmit_loop().await });
            }
            (a, b, b_rx)
        }

        #[tokio::test]
        async fn small_payload_delivered_over_loopback() {
            let (a, _b, mut b_rx) = connected_pair().await;
            a.send_reliable(b"hello world").await.unwrap();
            let got = tokio::time::timeout(Duration::from_secs(3), b_rx.recv())
                .await
                .expect("delivery timed out")
                .expect("channel closed");
            assert_eq!(got, b"hello world");
        }

        #[tokio::test]
        async fn large_payload_is_fragmented_and_reassembled() {
            let (a, _b, mut b_rx) = connected_pair().await;
            // 16 KiB — far exceeds UDP_MAX_PAYLOAD even after base64 expansion,
            // so this MUST traverse the DataFragment path and be reassembled.
            let payload: Vec<u8> = (0..16_384).map(|i| (i % 256) as u8).collect();
            a.send_reliable(&payload).await.unwrap();
            let got = tokio::time::timeout(Duration::from_secs(5), b_rx.recv())
                .await
                .expect("delivery timed out")
                .expect("channel closed");
            assert_eq!(got.len(), payload.len());
            assert_eq!(got, payload);
        }

        #[tokio::test]
        async fn punch_hole_handshake_connects_both_peers() {
            // Two fresh channels that know each other's loopback endpoint and
            // nonce (as they would after exchanging Offer/Answer via the relay).
            let cipher = Cipher::from_passphrase("punch-test-key");
            let (a, _arx) = UdpChannel::new("ch".into(), cipher.clone(), UdpConfig::default())
                .await
                .unwrap();
            let (b, _brx) = UdpChannel::new("ch".into(), cipher, UdpConfig::default())
                .await
                .unwrap();
            let (a, b) = (Arc::new(a), Arc::new(b));

            let (a_peer, b_peer) = (loopback_peer(&a), loopback_peer(&b));
            a.set_peer(b_peer, b.local_nonce()).await;
            b.set_peer(a_peer, a.local_nonce()).await;

            // Both run the probe/ack exchange concurrently; each answers the
            // other's probes and confirms on a nonce-matching ProbeAck.
            let (ra, rb) = (a.clone(), b.clone());
            let handshake = async move {
                let ha = tokio::spawn(async move { ra.punch_hole().await });
                let hb = tokio::spawn(async move { rb.punch_hole().await });
                let (resa, resb) = tokio::join!(ha, hb);
                resa.unwrap().expect("A hole-punch failed");
                resb.unwrap().expect("B hole-punch failed");
            };
            tokio::time::timeout(Duration::from_secs(10), handshake)
                .await
                .expect("hole-punching did not converge");

            assert_eq!(a.state().await, ChannelState::Connected);
            assert_eq!(b.state().await, ChannelState::Connected);
        }

        /// Local-candidate punching: when the FIRST candidate is unreachable (a
        /// dead address, e.g. a public IP that hairpins) the punch must still
        /// connect via a later reachable candidate (the loopback/LAN one) and
        /// lock `peer_endpoint` onto it.
        #[tokio::test]
        async fn punch_connects_via_non_first_candidate() {
            let cipher = Cipher::from_passphrase("multi-cand-key");
            let (a, _arx) = UdpChannel::new("ch".into(), cipher.clone(), UdpConfig::default())
                .await
                .unwrap();
            let (b, _brx) = UdpChannel::new("ch".into(), cipher, UdpConfig::default())
                .await
                .unwrap();
            let (a, b) = (Arc::new(a), Arc::new(b));

            let (a_peer, b_peer) = (loopback_peer(&a), loopback_peer(&b));
            // A dead candidate (TEST-NET-1, never answers) placed FIRST, the real
            // loopback peer SECOND — mirrors "public unreachable, local works".
            let dead = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 9);
            a.set_peer_candidates(vec![dead, b_peer], b.local_nonce()).await;
            b.set_peer_candidates(vec![dead, a_peer], a.local_nonce()).await;

            let (ra, rb) = (a.clone(), b.clone());
            let handshake = async move {
                let ha = tokio::spawn(async move { ra.punch_hole().await });
                let hb = tokio::spawn(async move { rb.punch_hole().await });
                let (resa, resb) = tokio::join!(ha, hb);
                resa.unwrap().expect("A hole-punch failed");
                resb.unwrap().expect("B hole-punch failed");
            };
            tokio::time::timeout(Duration::from_secs(10), handshake)
                .await
                .expect("hole-punching did not converge via fallback candidate");

            assert_eq!(a.state().await, ChannelState::Connected);
            assert_eq!(b.state().await, ChannelState::Connected);
            // Each side locked onto the reachable loopback candidate, not the dead one.
            assert_eq!(*a.peer_endpoint.read().await, Some(b_peer));
            assert_eq!(*b.peer_endpoint.read().await, Some(a_peer));
        }
    }
}

#[cfg(feature = "udp")]
pub use channel_impl::*;
