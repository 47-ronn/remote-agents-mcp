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
    use tokio::sync::{mpsc, Mutex, RwLock};

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
    }

    /// A pending reliable packet awaiting acknowledgment.
    #[allow(dead_code)]
    struct PendingPacket {
        data: Vec<u8>,
        sequence: u32,
        sent_at: Instant,
        retries: u32,
    }

    /// A bidirectional UDP channel to a remote peer.
    pub struct UdpChannel {
        /// Unique channel ID
        pub channel_id: String,
        /// Local UDP socket
        socket: Arc<UdpSocket>,
        /// Remote peer's endpoint (after hole-punching)
        peer_endpoint: RwLock<Option<SocketAddr>>,
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
    }

    impl UdpChannel {
        /// Create a new UDP channel.
        pub async fn new(
            channel_id: String,
            cipher: Cipher,
            config: UdpConfig,
        ) -> Result<(Self, mpsc::Receiver<Vec<u8>>), UdpChannelError> {
            // Bind to any available port
            let socket = UdpSocket::bind("0.0.0.0:0").await?;
            let socket = Arc::new(socket);

            let (recv_tx, recv_rx) = mpsc::channel(64);

            // Generate random nonce
            let mut local_nonce = [0u8; 16];
            rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut local_nonce);

            let channel = Self {
                channel_id,
                socket,
                peer_endpoint: RwLock::new(None),
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
            };

            Ok((channel, recv_rx))
        }

        /// Get the local endpoint.
        pub fn local_endpoint(&self) -> Result<Endpoint, UdpChannelError> {
            Ok(self.socket.local_addr()?.into())
        }

        /// Get the local nonce for this channel.
        pub fn local_nonce(&self) -> [u8; 16] {
            self.local_nonce
        }

        /// Get current channel state.
        pub async fn state(&self) -> ChannelState {
            *self.state.read().await
        }

        /// Set the remote peer's endpoint and nonce (from signaling).
        pub async fn set_peer(&self, endpoint: SocketAddr, nonce: [u8; 16]) {
            *self.peer_endpoint.write().await = Some(endpoint);
            *self.remote_nonce.write().await = Some(nonce);
        }

        /// Start hole-punching to establish connectivity.
        pub async fn punch_hole(&self) -> Result<(), UdpChannelError> {
            let peer = self.peer_endpoint.read().await.ok_or_else(|| {
                UdpChannelError::HolePunchFailed("peer endpoint not set".to_string())
            })?;

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

                // Send probe packet
                let header = UdpPacketHeader {
                    packet_type: UdpPacketType::Probe,
                    sequence: attempts,
                    payload_len: 16,
                };
                header.write(&mut probe_buf);
                probe_buf[UDP_HEADER_SIZE..].copy_from_slice(&self.local_nonce);

                self.socket.send_to(&probe_buf, peer).await?;
                attempts += 1;

                // Try to receive probe ack
                let mut recv_buf = [0u8; UDP_MAX_PACKET];
                match tokio::time::timeout(interval, self.socket.recv_from(&mut recv_buf)).await {
                    Ok(Ok((len, from))) => {
                        if from == peer && len >= UDP_HEADER_SIZE {
                            if let Some(h) = UdpPacketHeader::parse(&recv_buf[..len]) {
                                match h.packet_type {
                                    UdpPacketType::Probe => {
                                        // Send probe ack
                                        let ack_header = UdpPacketHeader {
                                            packet_type: UdpPacketType::ProbeAck,
                                            sequence: h.sequence,
                                            payload_len: 16,
                                        };
                                        let mut ack_buf = [0u8; UDP_HEADER_SIZE + 16];
                                        ack_header.write(&mut ack_buf);
                                        ack_buf[UDP_HEADER_SIZE..].copy_from_slice(&self.local_nonce);
                                        let _ = self.socket.send_to(&ack_buf, peer).await;
                                    }
                                    UdpPacketType::ProbeAck if len >= UDP_HEADER_SIZE + 16 => {
                                        // Connected once the peer echoes our expected nonce.
                                        let expected = *self.remote_nonce.read().await;
                                        let received: [u8; 16] = recv_buf
                                            [UDP_HEADER_SIZE..UDP_HEADER_SIZE + 16]
                                            .try_into()
                                            .unwrap();
                                        if expected == Some(received) {
                                            *self.state.write().await = ChannelState::Connected;
                                            return Ok(());
                                        }
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
            for fragment in split_into_fragments(encrypted_bytes, msg_id) {
                self.send_reliable_packet(UdpPacketType::DataFragment, &fragment, peer)
                    .await?;
            }
            Ok(())
        }

        /// Build a reliable packet of `packet_type`, register it for
        /// retransmission, and send it. Caller supplies the full UDP payload.
        async fn send_reliable_packet(
            &self,
            packet_type: UdpPacketType,
            payload: &[u8],
            peer: SocketAddr,
        ) -> Result<(), UdpChannelError> {
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

                // Verify sender
                let expected_peer = *self.peer_endpoint.read().await;
                if let Some(peer) = expected_peer {
                    if from != peer {
                        continue; // Ignore packets from unknown sources
                    }
                }

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
                                let _ = self.recv_tx.send(decrypted).await;
                            }
                        }
                    }

                    UdpPacketType::DataAck => {
                        // Remove from pending
                        let mut pending = self.pending.lock().await;
                        pending.remove(&header.sequence);
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
            let rto = Duration::from_millis(self.config.rto_ms);

            loop {
                tokio::time::sleep(rto).await;

                let state = *self.state.read().await;
                if state == ChannelState::Closed {
                    return Ok(());
                }

                let peer = match *self.peer_endpoint.read().await {
                    Some(p) => p,
                    None => continue,
                };

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
                        }
                    }
                }

                for seq in to_remove {
                    pending.remove(&seq);
                }
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
    }
}

#[cfg(feature = "udp")]
pub use channel_impl::*;
