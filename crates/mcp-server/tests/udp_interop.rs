//! Cross-implementation UDP interop: the MCP-side transport (`relay_udp`) dials
//! the agent-side transport (`udp_transport`), they hole-punch over loopback,
//! and the MCP sends a command frame directly over UDP which the agent receives
//! on its inbound queue. This is the real production path for "partition data
//! over UDP" and verifies the two parallel transports interoperate (and that the
//! iter44 punch/recv-race fix holds in both).

use remote_agent::{relay_udp, udp_transport};
use remote_agents_shared::{Cipher, Endpoint, UdpFrame};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::test]
async fn mcp_dials_agent_and_sends_command_over_udp() {
    let cipher = Cipher::from_passphrase("interop-key");
    // Simulate YourEndpoint reflecting loopback so offers carry a reachable addr.
    let lo = Endpoint::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    // MCP-side transport (dials).
    let (mcp_sig_tx, mut mcp_sig_rx) = mpsc::channel(16);
    let mcp = Arc::new(relay_udp::UdpTransport::new(cipher.clone(), mcp_sig_tx));
    mcp.set_session_id("MCP".into()).await;
    mcp.set_public_endpoint(lo).await;

    // Agent-side transport (answers + receives application data).
    let (ag_sig_tx, mut ag_sig_rx) = mpsc::channel(16);
    let (ag_in_tx, mut ag_in_rx) = mpsc::channel(16);
    let agent = Arc::new(udp_transport::UdpTransport::new(
        cipher,
        "AGENT".into(),
        ag_sig_tx,
        ag_in_tx,
    ));
    agent.set_public_endpoint(lo).await;

    // Signaling handshake (normally shuttled through the relay).
    mcp.offer_channel("AGENT".into()).await.unwrap();
    let offer = loop {
        match mcp_sig_rx.recv().await.unwrap() {
            relay_udp::SignalMessage::Offer(o) => break o,
            _ => continue,
        }
    };
    agent.handle_offer(offer).await.unwrap();
    let answer = loop {
        match ag_sig_rx.recv().await.unwrap() {
            udp_transport::SignalMessage::Answer(a) => break a,
            _ => continue,
        }
    };
    mcp.handle_answer(answer).await.unwrap();

    // Wait for the hole-punch to connect both ends.
    let mut connected = false;
    for _ in 0..60 {
        if mcp.has_udp_channel("AGENT").await && agent.has_udp_channel("MCP").await {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(connected, "cross-transport UDP channel did not connect");

    // MCP sends a command frame over UDP; the agent receives it inbound.
    let frame = UdpFrame::Command {
        request_id: "rq".into(),
        from_session: "MCP".into(),
        payload: "ENCRYPTED".into(),
    };
    assert!(mcp.send_via_udp("AGENT", &frame.to_bytes()).await.unwrap());

    let (peer, data) = tokio::time::timeout(Duration::from_secs(3), ag_in_rx.recv())
        .await
        .expect("inbound timed out")
        .expect("inbound channel closed");
    assert_eq!(peer, "MCP");
    assert_eq!(UdpFrame::from_bytes(&data), Some(frame));
}
