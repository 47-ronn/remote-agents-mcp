//! End-to-end task-chain lifecycle over a real in-process relay plus two agent
//! loops: arm a chain on the OWNER host (replication to the second host with a
//! ChainAck), finish the source task, and assert the owner's tick fires the
//! follow-up on the destination host — all without any panel connected.

use remote_agent::config::Config;
use remote_agent::{connection, relay_api::McpServer};
use remote_agents_relay::{router, state::RelayState};
use remote_agents_shared::{Command, CommandResult, Target};
use std::sync::Arc;
use std::time::Duration;

async fn start_relay() -> u16 {
    let state = Arc::new(RelayState::new(None));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(
            listener,
            router(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    port
}

fn agent_config(id: &str, port: u16, runner: Vec<&str>) -> Config {
    Config {
        id: id.to_string(),
        name: id.to_string(),
        room: "dev".to_string(),
        token: "secret".to_string(),
        relay_url: format!("ws://127.0.0.1:{port}"),
        autonomous: remote_agent::config::AutonomousConfig {
            enabled: Some(true),
            runner: runner.into_iter().map(String::from).collect(),
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn chain_arms_replicates_and_fires_follow_up_on_completion() {
    std::env::set_var("REMOTE_AGENTS_NO_STUN", "1");
    let port = start_relay().await;
    let relay_url = format!("ws://127.0.0.1:{port}");

    // Owner (also the source-task host) and destination, both autonomous with
    // a trivial `echo` runner so tasks complete immediately.
    let owner_cfg = agent_config("owner-host", port, vec!["echo"]);
    let dst_cfg = agent_config("dst-host", port, vec!["echo"]);

    tokio::spawn(async move {
        let _ = connection::run(&owner_cfg).await;
    });
    tokio::spawn(async move {
        let _ = connection::run(&dst_cfg).await;
    });

    // Anonymous controller: dispatches the source task and arms the chain.
    let ctrl = McpServer::new();
    ctrl.join_room(&relay_url, "dev", "secret", None, None, None)
        .await
        .expect("controller joins room");

    let mut ready = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(agents) = ctrl.list_agents("dev").await {
            let has = |id: &str| agents.iter().any(|a| a.id == id);
            if has("owner-host") && has("dst-host") {
                ready = true;
                break;
            }
        }
    }
    assert!(ready, "both agents should register with the relay");

    // 1) Source task on the owner host.
    let res = ctrl
        .send_command(
            "dev",
            Target::Agent { id: "owner-host".into() },
            Command::TaskDispatch {
                prompt: "source work".into(),
                initiator: None,
            },
        )
        .await
        .expect("dispatch source task");
    let src_task = match res.into_iter().next() {
        Some((_, CommandResult::TaskQueued { id })) => id,
        other => panic!("expected TaskQueued, got {other:?}"),
    };

    // 2) Arm the chain ON the owner (it becomes the chain owner). The replica
    //    copy goes to dst-host — the only autonomous peer — via ChainArm.
    let chain = remote_agents_shared::TaskChain {
        chain_id: format!("e2e-{src_task}"),
        src_agent: "owner-host".into(),
        src_task: src_task.clone(),
        dst_agent: "dst-host".into(),
        prompt: "follow up on the source result".into(),
        owner: "owner-host".into(),
        state: "armed".into(),
        fired_task: None,
        note: None,
        created_at: 1,
    };
    let res = ctrl
        .send_command(
            "dev",
            Target::Agent { id: "owner-host".into() },
            Command::ChainArm { chain: Box::new(chain.clone()) },
        )
        .await
        .expect("arm chain on owner");
    match res.into_iter().next() {
        Some((_, CommandResult::ChainAck { ok: true, .. })) => {}
        other => panic!("expected ChainAck ok, got {other:?}"),
    }

    // The replica (dst-host) must hold the chain copy too.
    let mut replicated = false;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let r = ctrl
            .send_command("dev", Target::Agent { id: "dst-host".into() }, Command::ChainList)
            .await
            .expect("chain list on dst");
        if let Some((_, CommandResult::ChainList { chains })) = r.into_iter().next() {
            if chains.iter().any(|c| c.chain_id == chain.chain_id) {
                replicated = true;
                break;
            }
        }
    }
    assert!(replicated, "dst-host should hold the replica copy");

    // 3) Wait for the owner's tick to fire: the source task (echo) finishes
    //    immediately, so within ~10s the follow-up must appear on dst-host.
    let mut fired = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let r = ctrl
            .send_command("dev", Target::Agent { id: "owner-host".into() }, Command::ChainList)
            .await
            .expect("chain list on owner");
        if let Some((_, CommandResult::ChainList { chains })) = r.into_iter().next() {
            if let Some(c) = chains.iter().find(|c| c.chain_id == chain.chain_id) {
                if c.state == "fired" && c.fired_task.is_some() {
                    fired = true;
                    break;
                }
            }
        }
    }
    assert!(fired, "owner should have fired the chain after the source finished");

    // 4) The fired follow-up must exist as a real task on dst-host.
    let r = ctrl
        .send_command("dev", Target::Agent { id: "owner-host".into() }, Command::ChainList)
        .await
        .expect("chain list on owner");
    let chains = match r.into_iter().next() {
        Some((_, CommandResult::ChainList { chains })) => chains,
        other => panic!("expected ChainList, got {other:?}"),
    };
    let fired_task = chains
        .iter()
        .find(|c| c.chain_id == chain.chain_id)
        .and_then(|c| c.fired_task.clone())
        .expect("fired_task id recorded");
    let r = ctrl
        .send_command(
            "dev",
            Target::Agent { id: "dst-host".into() },
            Command::TaskGet { id: fired_task },
        )
        .await
        .expect("task get on dst");
    match r.into_iter().next() {
        Some((_, CommandResult::Task { task })) => {
            assert!(
                task.prompt.contains("[chain "),
                "follow-up prompt should carry the chain marker: {}",
                task.prompt
            );
            assert_eq!(task.initiator.as_deref(), Some("owner-host"));
        }
        other => panic!("expected Task on dst-host, got {other:?}"),
    }
}
