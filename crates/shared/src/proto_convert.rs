//! Conversions between the generated protobuf wire types ([`crate::proto`]) and
//! the idiomatic Rust domain types ([`crate::protocol`]/[`crate::types`]).
//!
//! Per the protocol-unification plan the proto schema is the single source of
//! truth for the wire, but business logic keeps clean `match` on the domain
//! enums — so all encode/decode happens here at the boundary, fully roundtrip
//! tested. Encrypted command/result envelopes cross as base64 `String` on the
//! domain side and opaque `bytes` on the wire; file slices likewise.
//!
//! Direction convention: `proto -> domain` is always `TryFrom` (the wire is
//! untrusted — a malformed IP, out-of-range port, bad nonce length, missing
//! oneof, or invalid base64 is a typed error, never a panic). `domain -> proto`
//! is `From` where it cannot fail, and `TryFrom` for the few types that must
//! base64-decode a payload (`Command`, `CommandResult`, `ClientMessage`,
//! `ServerMessage`).

use crate::proto;
use base64::Engine as _;
use prost::Message as _;

/// Failure converting a wire message to/from its domain type.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConvertError {
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("empty oneof: {0}")]
    EmptyOneof(&'static str),
    #[error("invalid IP address: {0}")]
    BadIp(String),
    #[error("port out of u16 range: {0}")]
    BadPort(u32),
    #[error("nonce must be 16 bytes, got {0}")]
    BadNonce(usize),
    #[error("invalid base64 payload")]
    BadBase64,
}

fn b64_decode(s: &str) -> Result<Vec<u8>, ConvertError> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|_| ConvertError::BadBase64)
}

fn b64_encode(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

/// Failure decoding a wire frame into a domain message: either the protobuf
/// bytes were malformed, or they decoded but carried an invalid value.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("protobuf decode failed: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error(transparent)]
    Convert(#[from] ConvertError),
}

impl crate::ClientMessage {
    /// Encode to protobuf bytes for a binary WS/UDP frame.
    pub fn to_proto_bytes(&self) -> Result<Vec<u8>, ConvertError> {
        let p: proto::ClientMessage = self.clone().try_into()?;
        Ok(p.encode_to_vec())
    }
    /// Decode a binary WS/UDP frame into the domain message.
    pub fn from_proto_bytes(bytes: &[u8]) -> Result<Self, WireError> {
        Ok(proto::ClientMessage::decode(bytes)?.try_into()?)
    }
}

impl crate::ServerMessage {
    /// Encode to protobuf bytes for a binary WS frame.
    pub fn to_proto_bytes(&self) -> Result<Vec<u8>, ConvertError> {
        let p: proto::ServerMessage = self.clone().try_into()?;
        Ok(p.encode_to_vec())
    }
    /// Decode a binary WS frame into the domain message.
    pub fn from_proto_bytes(bytes: &[u8]) -> Result<Self, WireError> {
        Ok(proto::ServerMessage::decode(bytes)?.try_into()?)
    }
}

// ============================================================================
// Enums
// ============================================================================

impl From<crate::AgentMode> for proto::AgentMode {
    fn from(m: crate::AgentMode) -> Self {
        use crate::AgentMode as D;
        match m {
            D::Plan => Self::Plan,
            D::Edit => Self::Edit,
            D::Bypass => Self::Bypass,
            D::Disabled => Self::Disabled,
        }
    }
}
impl From<proto::AgentMode> for crate::AgentMode {
    fn from(m: proto::AgentMode) -> Self {
        use proto::AgentMode as P;
        match m {
            P::Plan => Self::Plan,
            P::Edit => Self::Edit,
            P::Bypass => Self::Bypass,
            P::Disabled => Self::Disabled,
        }
    }
}
fn mode_to_i32(m: crate::AgentMode) -> i32 {
    proto::AgentMode::from(m) as i32
}
fn mode_from_i32(i: i32) -> crate::AgentMode {
    proto::AgentMode::try_from(i).unwrap_or_default().into()
}

impl From<crate::SearchKind> for proto::SearchKind {
    fn from(k: crate::SearchKind) -> Self {
        use crate::SearchKind as D;
        match k {
            D::Name => Self::Name,
            D::Content => Self::Content,
            D::Image => Self::Image,
        }
    }
}
impl From<proto::SearchKind> for crate::SearchKind {
    fn from(k: proto::SearchKind) -> Self {
        use proto::SearchKind as P;
        match k {
            P::Name => Self::Name,
            P::Content => Self::Content,
            P::Image => Self::Image,
        }
    }
}

impl From<crate::TransferState> for proto::TransferState {
    fn from(s: crate::TransferState) -> Self {
        use crate::TransferState as D;
        match s {
            D::Queued => Self::Queued,
            D::Running => Self::Running,
            D::Done => Self::Done,
            D::Failed => Self::Failed,
        }
    }
}
impl From<proto::TransferState> for crate::TransferState {
    fn from(s: proto::TransferState) -> Self {
        use proto::TransferState as P;
        match s {
            P::Queued => Self::Queued,
            P::Running => Self::Running,
            P::Done => Self::Done,
            P::Failed => Self::Failed,
        }
    }
}

impl From<crate::TaskStatus> for proto::TaskStatus {
    fn from(s: crate::TaskStatus) -> Self {
        use crate::TaskStatus as D;
        match s {
            D::Queued => Self::Queued,
            D::Running => Self::Running,
            D::Done => Self::Done,
            D::Failed => Self::Failed,
        }
    }
}
impl From<proto::TaskStatus> for crate::TaskStatus {
    fn from(s: proto::TaskStatus) -> Self {
        use proto::TaskStatus as P;
        match s {
            P::Queued => Self::Queued,
            P::Running => Self::Running,
            P::Done => Self::Done,
            P::Failed => Self::Failed,
        }
    }
}
fn task_status_to_i32(s: crate::TaskStatus) -> i32 {
    proto::TaskStatus::from(s) as i32
}
fn task_status_from_i32(i: i32) -> crate::TaskStatus {
    proto::TaskStatus::try_from(i).unwrap_or_default().into()
}

// ============================================================================
// Endpoint + helpers
// ============================================================================

impl From<crate::Endpoint> for proto::Endpoint {
    fn from(e: crate::Endpoint) -> Self {
        proto::Endpoint {
            addr: e.addr.to_string(),
            port: e.port as u32,
        }
    }
}
impl TryFrom<proto::Endpoint> for crate::Endpoint {
    type Error = ConvertError;
    fn try_from(e: proto::Endpoint) -> Result<Self, ConvertError> {
        let addr = e
            .addr
            .parse()
            .map_err(|_| ConvertError::BadIp(e.addr.clone()))?;
        let port = u16::try_from(e.port).map_err(|_| ConvertError::BadPort(e.port))?;
        Ok(crate::Endpoint { addr, port })
    }
}
fn req_ep(e: Option<proto::Endpoint>, f: &'static str) -> Result<crate::Endpoint, ConvertError> {
    e.ok_or(ConvertError::MissingField(f))?.try_into()
}
fn opt_ep(e: Option<proto::Endpoint>) -> Result<Option<crate::Endpoint>, ConvertError> {
    e.map(TryInto::try_into).transpose()
}
fn nonce_from(v: Vec<u8>) -> Result<[u8; 16], ConvertError> {
    let len = v.len();
    v.try_into().map_err(|_| ConvertError::BadNonce(len))
}

// ============================================================================
// UDP signaling
// ============================================================================

impl From<crate::UdpOffer> for proto::UdpOffer {
    fn from(o: crate::UdpOffer) -> Self {
        proto::UdpOffer {
            channel_id: o.channel_id,
            from_session: o.from_session,
            to_session: o.to_session,
            local_endpoint: Some(o.local_endpoint.into()),
            local_candidates: o.local_candidates.into_iter().map(Into::into).collect(),
            public_endpoint: o.public_endpoint.map(Into::into),
            nonce: o.nonce.to_vec(),
        }
    }
}
impl TryFrom<proto::UdpOffer> for crate::UdpOffer {
    type Error = ConvertError;
    fn try_from(o: proto::UdpOffer) -> Result<Self, ConvertError> {
        Ok(crate::UdpOffer {
            channel_id: o.channel_id,
            from_session: o.from_session,
            to_session: o.to_session,
            local_endpoint: req_ep(o.local_endpoint, "UdpOffer.local_endpoint")?,
            local_candidates: o
                .local_candidates
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            public_endpoint: opt_ep(o.public_endpoint)?,
            nonce: nonce_from(o.nonce)?,
        })
    }
}

impl From<crate::UdpAnswer> for proto::UdpAnswer {
    fn from(a: crate::UdpAnswer) -> Self {
        proto::UdpAnswer {
            channel_id: a.channel_id,
            from_session: a.from_session,
            local_endpoint: Some(a.local_endpoint.into()),
            local_candidates: a.local_candidates.into_iter().map(Into::into).collect(),
            public_endpoint: a.public_endpoint.map(Into::into),
            nonce: a.nonce.to_vec(),
            accepted: a.accepted,
        }
    }
}
impl TryFrom<proto::UdpAnswer> for crate::UdpAnswer {
    type Error = ConvertError;
    fn try_from(a: proto::UdpAnswer) -> Result<Self, ConvertError> {
        Ok(crate::UdpAnswer {
            channel_id: a.channel_id,
            from_session: a.from_session,
            local_endpoint: req_ep(a.local_endpoint, "UdpAnswer.local_endpoint")?,
            local_candidates: a
                .local_candidates
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            public_endpoint: opt_ep(a.public_endpoint)?,
            nonce: nonce_from(a.nonce)?,
            accepted: a.accepted,
        })
    }
}

impl From<crate::UdpChannelResult> for proto::UdpChannelResult {
    fn from(r: crate::UdpChannelResult) -> Self {
        proto::UdpChannelResult {
            channel_id: r.channel_id,
            success: r.success,
            error: r.error,
        }
    }
}
impl TryFrom<proto::UdpChannelResult> for crate::UdpChannelResult {
    type Error = ConvertError;
    fn try_from(r: proto::UdpChannelResult) -> Result<Self, ConvertError> {
        Ok(crate::UdpChannelResult {
            channel_id: r.channel_id,
            success: r.success,
            error: r.error,
        })
    }
}

// ============================================================================
// Supporting structs
// ============================================================================

impl From<crate::PlatformInfo> for proto::PlatformInfo {
    fn from(p: crate::PlatformInfo) -> Self {
        proto::PlatformInfo {
            family: p.family,
            arch: p.arch,
            distro: p.distro,
            kernel: p.kernel,
            shell: p.shell,
        }
    }
}
impl From<proto::PlatformInfo> for crate::PlatformInfo {
    fn from(p: proto::PlatformInfo) -> Self {
        crate::PlatformInfo {
            family: p.family,
            arch: p.arch,
            distro: p.distro,
            kernel: p.kernel,
            shell: p.shell,
        }
    }
}

impl From<crate::AgentInfo> for proto::AgentInfo {
    fn from(a: crate::AgentInfo) -> Self {
        proto::AgentInfo {
            id: a.id,
            name: a.name,
            mode: mode_to_i32(a.mode),
            os: a.os,
            arch: a.arch,
            hostname: a.hostname,
            tags: a.tags,
            platform: Some(a.platform.into()),
            autonomous: a.autonomous,
            accepts_commands: a.accepts_commands,
            connected_at: a.connected_at,
            version: a.version,
            session_id: a.session_id,
            update_available: a.update_available,
            connections: a.connections,
        }
    }
}
impl From<proto::AgentInfo> for crate::AgentInfo {
    fn from(a: proto::AgentInfo) -> Self {
        crate::AgentInfo {
            id: a.id,
            name: a.name,
            mode: mode_from_i32(a.mode),
            os: a.os,
            arch: a.arch,
            hostname: a.hostname,
            tags: a.tags,
            platform: a.platform.map(Into::into).unwrap_or_default(),
            autonomous: a.autonomous,
            accepts_commands: a.accepts_commands,
            connected_at: a.connected_at,
            version: a.version,
            session_id: a.session_id,
            update_available: a.update_available,
            connections: a.connections,
        }
    }
}

impl From<crate::SessionMeta> for proto::SessionMeta {
    fn from(s: crate::SessionMeta) -> Self {
        proto::SessionMeta {
            provider: s.provider,
            id: s.id,
            title: s.title,
            updated: s.updated,
            cwd: s.cwd,
            resumable: s.resumable,
        }
    }
}
impl From<proto::SessionMeta> for crate::SessionMeta {
    fn from(s: proto::SessionMeta) -> Self {
        crate::SessionMeta {
            provider: s.provider,
            id: s.id,
            title: s.title,
            updated: s.updated,
            cwd: s.cwd,
            resumable: s.resumable,
        }
    }
}

impl From<crate::SessionMessage> for proto::SessionMessage {
    fn from(m: crate::SessionMessage) -> Self {
        proto::SessionMessage {
            role: m.role,
            text: m.text,
            ts: m.ts,
        }
    }
}
impl From<proto::SessionMessage> for crate::SessionMessage {
    fn from(m: proto::SessionMessage) -> Self {
        crate::SessionMessage {
            role: m.role,
            text: m.text,
            ts: m.ts,
        }
    }
}

impl From<crate::FileMeta> for proto::FileMeta {
    fn from(m: crate::FileMeta) -> Self {
        proto::FileMeta {
            path: m.path,
            size: m.size,
            modified: m.modified,
            mime: m.mime,
            is_image: m.is_image,
        }
    }
}
impl From<proto::FileMeta> for crate::FileMeta {
    fn from(m: proto::FileMeta) -> Self {
        crate::FileMeta {
            path: m.path,
            size: m.size,
            modified: m.modified,
            mime: m.mime,
            is_image: m.is_image,
        }
    }
}

impl From<crate::TransferStatus> for proto::TransferStatus {
    fn from(t: crate::TransferStatus) -> Self {
        proto::TransferStatus {
            id: t.id,
            state: proto::TransferState::from(t.state) as i32,
            bytes: t.bytes,
            total: t.total,
            error: t.error,
            files_done: t.files_done,
            files_total: t.files_total,
        }
    }
}
impl From<proto::TransferStatus> for crate::TransferStatus {
    fn from(t: proto::TransferStatus) -> Self {
        crate::TransferStatus {
            id: t.id,
            state: proto::TransferState::try_from(t.state)
                .unwrap_or_default()
                .into(),
            bytes: t.bytes,
            total: t.total,
            files_done: t.files_done,
            files_total: t.files_total,
            error: t.error,
        }
    }
}

impl From<crate::ManifestEntry> for proto::ManifestEntry {
    fn from(e: crate::ManifestEntry) -> Self {
        proto::ManifestEntry {
            rel_path: e.rel_path,
            size: e.size,
            mtime_ms: e.mtime_ms,
            sha256: e.sha256,
        }
    }
}
impl From<proto::ManifestEntry> for crate::ManifestEntry {
    fn from(e: proto::ManifestEntry) -> Self {
        crate::ManifestEntry {
            rel_path: e.rel_path,
            size: e.size,
            mtime_ms: e.mtime_ms,
            sha256: e.sha256,
        }
    }
}

impl From<crate::TunnelInfo> for proto::TunnelInfo {
    fn from(t: crate::TunnelInfo) -> Self {
        proto::TunnelInfo {
            id: t.id,
            target: t.target,
            public_url: t.public_url,
            status: t.status,
        }
    }
}
impl From<proto::TunnelInfo> for crate::TunnelInfo {
    fn from(t: proto::TunnelInfo) -> Self {
        crate::TunnelInfo {
            id: t.id,
            target: t.target,
            public_url: t.public_url,
            status: t.status,
        }
    }
}

impl From<crate::AutonomousTask> for proto::AutonomousTask {
    fn from(t: crate::AutonomousTask) -> Self {
        proto::AutonomousTask {
            id: t.id,
            initiator: t.initiator,
            prompt: t.prompt,
            status: task_status_to_i32(t.status),
            result: t.result,
            error: t.error,
            created_at: t.created_at,
            started_at: t.started_at,
            finished_at: t.finished_at,
            exit_code: t.exit_code,
        }
    }
}
impl From<proto::AutonomousTask> for crate::AutonomousTask {
    fn from(t: proto::AutonomousTask) -> Self {
        crate::AutonomousTask {
            id: t.id,
            initiator: t.initiator,
            prompt: t.prompt,
            status: task_status_from_i32(t.status),
            result: t.result,
            error: t.error,
            created_at: t.created_at,
            started_at: t.started_at,
            finished_at: t.finished_at,
            exit_code: t.exit_code,
        }
    }
}

impl From<crate::DirEntry> for proto::DirEntry {
    fn from(d: crate::DirEntry) -> Self {
        proto::DirEntry {
            name: d.name,
            is_dir: d.is_dir,
            size: d.size,
            modified: d.modified,
        }
    }
}
impl From<proto::DirEntry> for crate::DirEntry {
    fn from(d: proto::DirEntry) -> Self {
        crate::DirEntry {
            name: d.name,
            is_dir: d.is_dir,
            size: d.size,
            modified: d.modified,
        }
    }
}

impl From<crate::ScheduledTask> for proto::ScheduledTask {
    fn from(s: crate::ScheduledTask) -> Self {
        proto::ScheduledTask {
            name: s.name,
            cron: s.cron,
            command: s.command,
            last_run: s.last_run,
            run_count: s.run_count,
        }
    }
}
impl From<proto::ScheduledTask> for crate::ScheduledTask {
    fn from(s: proto::ScheduledTask) -> Self {
        crate::ScheduledTask {
            name: s.name,
            cron: s.cron,
            command: s.command,
            last_run: s.last_run,
            run_count: s.run_count,
        }
    }
}

impl From<crate::GitStatus> for proto::GitStatus {
    fn from(g: crate::GitStatus) -> Self {
        proto::GitStatus {
            branch: g.branch,
            clean: g.clean,
            ahead: g.ahead,
            behind: g.behind,
            staged: g.staged,
            modified: g.modified,
            untracked: g.untracked,
        }
    }
}
impl From<proto::GitStatus> for crate::GitStatus {
    fn from(g: proto::GitStatus) -> Self {
        crate::GitStatus {
            branch: g.branch,
            clean: g.clean,
            ahead: g.ahead,
            behind: g.behind,
            staged: g.staged,
            modified: g.modified,
            untracked: g.untracked,
        }
    }
}

// ---- Target ----------------------------------------------------------------

impl From<crate::Target> for proto::Target {
    fn from(t: crate::Target) -> Self {
        use crate::Target as D;
        use proto::target::Kind;
        let kind = match t {
            D::Agent { id } => Kind::Agent(proto::target::Agent { id }),
            D::All => Kind::All(proto::target::All {}),
            D::Tagged { tags } => Kind::Tagged(proto::target::Tagged { tags }),
            D::Platform { family } => Kind::Platform(proto::target::Platform { family }),
        };
        proto::Target { kind: Some(kind) }
    }
}
impl TryFrom<proto::Target> for crate::Target {
    type Error = ConvertError;
    fn try_from(t: proto::Target) -> Result<Self, ConvertError> {
        use proto::target::Kind;
        Ok(match t.kind.ok_or(ConvertError::EmptyOneof("Target"))? {
            Kind::Agent(a) => crate::Target::Agent { id: a.id },
            Kind::All(_) => crate::Target::All,
            Kind::Tagged(t) => crate::Target::Tagged { tags: t.tags },
            Kind::Platform(p) => crate::Target::Platform { family: p.family },
        })
    }
}

// ---- AgentEvent ------------------------------------------------------------

impl From<crate::AgentEvent> for proto::AgentEvent {
    fn from(e: crate::AgentEvent) -> Self {
        use crate::AgentEvent as D;
        use proto::agent_event::Kind;
        let kind = match e {
            D::TaskCompleted { task_id, status } => {
                Kind::TaskCompleted(proto::agent_event::TaskCompleted {
                    task_id,
                    status: task_status_to_i32(status),
                })
            }
        };
        proto::AgentEvent { kind: Some(kind) }
    }
}
impl TryFrom<proto::AgentEvent> for crate::AgentEvent {
    type Error = ConvertError;
    fn try_from(e: proto::AgentEvent) -> Result<Self, ConvertError> {
        use proto::agent_event::Kind;
        Ok(match e.kind.ok_or(ConvertError::EmptyOneof("AgentEvent"))? {
            Kind::TaskCompleted(t) => crate::AgentEvent::TaskCompleted {
                task_id: t.task_id,
                status: task_status_from_i32(t.status),
            },
        })
    }
}

// ============================================================================
// Command
// ============================================================================

impl TryFrom<crate::Command> for proto::Command {
    type Error = ConvertError;
    fn try_from(c: crate::Command) -> Result<Self, ConvertError> {
        use crate::Command as D;
        use proto::command as p;
        use proto::command::Kind as K;
        let kind = match c {
            D::Exec { command, timeout_ms, cwd } => K::Exec(p::Exec { command, timeout_ms, cwd }),
            D::ReadFile { path } => K::ReadFile(p::ReadFile { path }),
            D::WriteFile { path, content, create_backup } => {
                K::WriteFile(p::WriteFile { path, content, create_backup })
            }
            D::ListDir { path, pattern } => K::ListDir(p::ListDir { path, pattern }),
            D::GitStatus { repo } => K::GitStatus(p::GitStatusCmd { repo }),
            D::GitPull { repo, remote, branch } => K::GitPull(p::GitPull { repo, remote, branch }),
            D::GitCommit { repo, message, files } => {
                K::GitCommit(p::GitCommit { repo, message, files })
            }
            D::GitPush { repo, remote, branch } => K::GitPush(p::GitPush { repo, remote, branch }),
            D::ScheduleAdd { name, cron, command } => {
                K::ScheduleAdd(p::ScheduleAdd { name, cron, command })
            }
            D::ScheduleRemove { name } => K::ScheduleRemove(p::ScheduleRemove { name }),
            D::ScheduleList => K::ScheduleList(p::ScheduleList {}),
            D::TaskDispatch { prompt, initiator } => {
                K::TaskDispatch(p::TaskDispatch { prompt, initiator })
            }
            D::TaskGet { id } => K::TaskGet(p::TaskGet { id }),
            D::TaskList => K::TaskList(p::TaskList {}),
            D::SessionList => K::SessionList(p::SessionList {}),
            D::SessionGet { provider, id } => K::SessionGet(p::SessionGet { provider, id }),
            D::SessionResume { provider, id, prompt } => {
                K::SessionResume(p::SessionResume { provider, id, prompt })
            }
            D::SessionTerminate { id } => K::SessionTerminate(p::SessionTerminate { id }),
            D::FileStat { path } => K::FileStat(p::FileStat { path }),
            D::FileChunk { path, offset, len } => K::FileChunk(p::FileChunk { path, offset, len }),
            D::FileThumb { path, max_px } => K::FileThumb(p::FileThumb { path, max_px }),
            D::FileSearch { roots, query, kind } => K::FileSearch(p::FileSearch {
                roots,
                query,
                kind: proto::SearchKind::from(kind) as i32,
            }),
            D::SendFileTo { src_path, dest_id, dest_path } => {
                K::SendFileTo(p::SendFileTo { src_path, dest_id, dest_path })
            }
            D::FileRecv { transfer_id, dest_path, offset, bytes, eof, sha256 } => {
                K::FileRecv(p::FileRecv {
                    transfer_id,
                    dest_path,
                    offset,
                    bytes: b64_decode(&bytes)?,
                    eof,
                    sha256,
                })
            }
            D::TransferGet { id } => K::TransferGet(p::TransferGet { id }),
            D::DirManifest { path, with_hash, exclude } => {
                K::DirManifest(p::DirManifest { path, with_hash, exclude })
            }
            D::SyncDirTo { src_path, dest_id, dest_path, delete, checksum, dry_run, exclude } => {
                K::SyncDirTo(p::SyncDirTo {
                    src_path,
                    dest_id,
                    dest_path,
                    delete,
                    checksum,
                    dry_run,
                    exclude,
                })
            }
            D::DeletePaths { paths } => K::DeletePaths(p::DeletePaths { paths }),
            D::TunnelStart { target } => K::TunnelStart(p::TunnelStart { target }),
            D::TunnelList => K::TunnelList(p::TunnelList {}),
            D::TunnelStop { id } => K::TunnelStop(p::TunnelStop { id }),
            D::SetMode { mode } => K::SetMode(p::SetMode { mode: mode_to_i32(mode) }),
            D::GetInfo => K::GetInfo(p::GetInfo {}),
            D::MapTask { job_id, partition_id, map_fn, data } => {
                K::MapTask(p::MapTask { job_id, partition_id, map_fn, data })
            }
            D::ReduceTask { job_id, reduce_fn, inputs } => {
                K::ReduceTask(p::ReduceTask { job_id, reduce_fn, inputs })
            }
        };
        Ok(proto::Command { kind: Some(kind) })
    }
}
impl TryFrom<proto::Command> for crate::Command {
    type Error = ConvertError;
    fn try_from(c: proto::Command) -> Result<Self, ConvertError> {
        use crate::Command as D;
        use proto::command::Kind as K;
        Ok(match c.kind.ok_or(ConvertError::EmptyOneof("Command"))? {
            K::Exec(e) => D::Exec { command: e.command, timeout_ms: e.timeout_ms, cwd: e.cwd },
            K::ReadFile(e) => D::ReadFile { path: e.path },
            K::WriteFile(e) => D::WriteFile {
                path: e.path,
                content: e.content,
                create_backup: e.create_backup,
            },
            K::ListDir(e) => D::ListDir { path: e.path, pattern: e.pattern },
            K::GitStatus(e) => D::GitStatus { repo: e.repo },
            K::GitPull(e) => D::GitPull { repo: e.repo, remote: e.remote, branch: e.branch },
            K::GitCommit(e) => D::GitCommit { repo: e.repo, message: e.message, files: e.files },
            K::GitPush(e) => D::GitPush { repo: e.repo, remote: e.remote, branch: e.branch },
            K::ScheduleAdd(e) => D::ScheduleAdd { name: e.name, cron: e.cron, command: e.command },
            K::ScheduleRemove(e) => D::ScheduleRemove { name: e.name },
            K::ScheduleList(_) => D::ScheduleList,
            K::TaskDispatch(e) => D::TaskDispatch { prompt: e.prompt, initiator: e.initiator },
            K::TaskGet(e) => D::TaskGet { id: e.id },
            K::TaskList(_) => D::TaskList,
            K::SessionList(_) => D::SessionList,
            K::SessionGet(e) => D::SessionGet { provider: e.provider, id: e.id },
            K::SessionResume(e) => {
                D::SessionResume { provider: e.provider, id: e.id, prompt: e.prompt }
            }
            K::SessionTerminate(e) => D::SessionTerminate { id: e.id },
            K::FileStat(e) => D::FileStat { path: e.path },
            K::FileChunk(e) => D::FileChunk { path: e.path, offset: e.offset, len: e.len },
            K::FileThumb(e) => D::FileThumb { path: e.path, max_px: e.max_px },
            K::FileSearch(e) => D::FileSearch {
                roots: e.roots,
                query: e.query,
                kind: proto::SearchKind::try_from(e.kind).unwrap_or_default().into(),
            },
            K::SendFileTo(e) => {
                D::SendFileTo { src_path: e.src_path, dest_id: e.dest_id, dest_path: e.dest_path }
            }
            K::FileRecv(e) => D::FileRecv {
                transfer_id: e.transfer_id,
                dest_path: e.dest_path,
                offset: e.offset,
                bytes: b64_encode(&e.bytes),
                eof: e.eof,
                sha256: e.sha256,
            },
            K::TransferGet(e) => D::TransferGet { id: e.id },
            K::DirManifest(e) => {
                D::DirManifest { path: e.path, with_hash: e.with_hash, exclude: e.exclude }
            }
            K::SyncDirTo(e) => D::SyncDirTo {
                src_path: e.src_path,
                dest_id: e.dest_id,
                dest_path: e.dest_path,
                delete: e.delete,
                checksum: e.checksum,
                dry_run: e.dry_run,
                exclude: e.exclude,
            },
            K::DeletePaths(e) => D::DeletePaths { paths: e.paths },
            K::TunnelStart(e) => D::TunnelStart { target: e.target },
            K::TunnelList(_) => D::TunnelList,
            K::TunnelStop(e) => D::TunnelStop { id: e.id },
            K::SetMode(e) => D::SetMode { mode: mode_from_i32(e.mode) },
            K::GetInfo(_) => D::GetInfo,
            K::MapTask(e) => D::MapTask {
                job_id: e.job_id,
                partition_id: e.partition_id,
                map_fn: e.map_fn,
                data: e.data,
            },
            K::ReduceTask(e) => {
                D::ReduceTask { job_id: e.job_id, reduce_fn: e.reduce_fn, inputs: e.inputs }
            }
        })
    }
}

// ============================================================================
// CommandResult
// ============================================================================

impl TryFrom<crate::CommandResult> for proto::CommandResult {
    type Error = ConvertError;
    fn try_from(r: crate::CommandResult) -> Result<Self, ConvertError> {
        use crate::CommandResult as D;
        use proto::command_result as p;
        use proto::command_result::Kind as K;
        let kind = match r {
            D::Exec { stdout, stderr, exit_code, duration_ms, timed_out } => {
                K::Exec(p::Exec { stdout, stderr, exit_code, duration_ms, timed_out })
            }
            D::File { content, size } => K::File(p::File { content, size }),
            D::Dir { entries } => {
                K::Dir(p::Dir { entries: entries.into_iter().map(Into::into).collect() })
            }
            D::GitStatus { status } => {
                K::GitStatus(p::GitStatusResult { status: Some(status.into()) })
            }
            D::Git { output, success } => K::Git(p::Git { output, success }),
            D::Info { info } => K::Info(p::Info { info: Some(info.into()) }),
            D::Mode { mode } => K::Mode(p::Mode { mode: mode_to_i32(mode) }),
            D::Schedule { tasks } => {
                K::Schedule(p::Schedule { tasks: tasks.into_iter().map(Into::into).collect() })
            }
            D::TaskQueued { id } => K::TaskQueued(p::TaskQueued { id }),
            D::Task { task } => K::Task(p::Task { task: Some(task.into()) }),
            D::TaskList { tasks } => K::TaskList(p::TaskListResult {
                tasks: tasks.into_iter().map(Into::into).collect(),
            }),
            D::FileMeta { meta } => K::FileMeta(p::FileMetaResult { meta: Some(meta.into()) }),
            D::FileChunk { data, eof } => {
                K::FileChunk(p::FileChunk { data: b64_decode(&data)?, eof })
            }
            D::FileThumb { data, w, h } => {
                K::FileThumb(p::FileThumb { data: b64_decode(&data)?, w, h })
            }
            D::FileSearch { hits } => K::FileSearch(p::FileSearchResult {
                hits: hits.into_iter().map(Into::into).collect(),
            }),
            D::TransferQueued { id } => K::TransferQueued(p::TransferQueued { id }),
            D::Transfer { status } => K::Transfer(p::Transfer { status: Some(status.into()) }),
            D::DirManifest { entries, root_exists } => {
                K::DirManifest(p::DirManifestResult {
                    entries: entries.into_iter().map(Into::into).collect(),
                    root_exists,
                })
            }
            D::TunnelStarted { tunnel } => {
                K::TunnelStarted(p::TunnelStarted { tunnel: Some(tunnel.into()) })
            }
            D::TunnelList { tunnels } => K::TunnelList(p::TunnelListResult {
                tunnels: tunnels.into_iter().map(Into::into).collect(),
            }),
            D::SessionList { sessions, active } => K::SessionList(p::SessionListResult {
                sessions: sessions.into_iter().map(Into::into).collect(),
                active,
            }),
            D::SessionTranscript { messages } => K::SessionTranscript(p::SessionTranscript {
                messages: messages.into_iter().map(Into::into).collect(),
            }),
            D::MapResult { job_id, partition_id, output, success, error } => {
                K::MapResult(p::MapResult { job_id, partition_id, output, success, error })
            }
            D::ReduceResult { job_id, output, success, error } => {
                K::ReduceResult(p::ReduceResult { job_id, output, success, error })
            }
            D::Ok => K::Ok(p::Ok {}),
        };
        Ok(proto::CommandResult { kind: Some(kind) })
    }
}
impl TryFrom<proto::CommandResult> for crate::CommandResult {
    type Error = ConvertError;
    fn try_from(r: proto::CommandResult) -> Result<Self, ConvertError> {
        use crate::CommandResult as D;
        use proto::command_result::Kind as K;
        Ok(match r.kind.ok_or(ConvertError::EmptyOneof("CommandResult"))? {
            K::Exec(e) => D::Exec {
                stdout: e.stdout,
                stderr: e.stderr,
                exit_code: e.exit_code,
                duration_ms: e.duration_ms,
                timed_out: e.timed_out,
            },
            K::File(e) => D::File { content: e.content, size: e.size },
            K::Dir(e) => D::Dir { entries: e.entries.into_iter().map(Into::into).collect() },
            K::GitStatus(e) => D::GitStatus {
                status: e
                    .status
                    .ok_or(ConvertError::MissingField("CommandResult.GitStatus.status"))?
                    .into(),
            },
            K::Git(e) => D::Git { output: e.output, success: e.success },
            K::Info(e) => D::Info {
                info: e
                    .info
                    .ok_or(ConvertError::MissingField("CommandResult.Info.info"))?
                    .into(),
            },
            K::Mode(e) => D::Mode { mode: mode_from_i32(e.mode) },
            K::Schedule(e) => {
                D::Schedule { tasks: e.tasks.into_iter().map(Into::into).collect() }
            }
            K::TaskQueued(e) => D::TaskQueued { id: e.id },
            K::Task(e) => D::Task {
                task: e
                    .task
                    .ok_or(ConvertError::MissingField("CommandResult.Task.task"))?
                    .into(),
            },
            K::TaskList(e) => D::TaskList { tasks: e.tasks.into_iter().map(Into::into).collect() },
            K::FileMeta(e) => D::FileMeta {
                meta: e
                    .meta
                    .ok_or(ConvertError::MissingField("CommandResult.FileMeta.meta"))?
                    .into(),
            },
            K::FileChunk(e) => D::FileChunk { data: b64_encode(&e.data), eof: e.eof },
            K::FileThumb(e) => D::FileThumb { data: b64_encode(&e.data), w: e.w, h: e.h },
            K::FileSearch(e) => {
                D::FileSearch { hits: e.hits.into_iter().map(Into::into).collect() }
            }
            K::TransferQueued(e) => D::TransferQueued { id: e.id },
            K::Transfer(e) => D::Transfer {
                status: e
                    .status
                    .ok_or(ConvertError::MissingField("CommandResult.Transfer.status"))?
                    .into(),
            },
            K::DirManifest(e) => D::DirManifest {
                entries: e.entries.into_iter().map(Into::into).collect(),
                root_exists: e.root_exists,
            },
            K::TunnelStarted(e) => D::TunnelStarted {
                tunnel: e
                    .tunnel
                    .ok_or(ConvertError::MissingField("CommandResult.TunnelStarted.tunnel"))?
                    .into(),
            },
            K::TunnelList(e) => {
                D::TunnelList { tunnels: e.tunnels.into_iter().map(Into::into).collect() }
            }
            K::SessionList(e) => D::SessionList {
                sessions: e.sessions.into_iter().map(Into::into).collect(),
                active: e.active,
            },
            K::SessionTranscript(e) => D::SessionTranscript {
                messages: e.messages.into_iter().map(Into::into).collect(),
            },
            K::MapResult(e) => D::MapResult {
                job_id: e.job_id,
                partition_id: e.partition_id,
                output: e.output,
                success: e.success,
                error: e.error,
            },
            K::ReduceResult(e) => D::ReduceResult {
                job_id: e.job_id,
                output: e.output,
                success: e.success,
                error: e.error,
            },
            K::Ok(_) => D::Ok,
        })
    }
}

// ============================================================================
// ClientMessage envelope
// ============================================================================

impl TryFrom<crate::ClientMessage> for proto::ClientMessage {
    type Error = ConvertError;
    fn try_from(m: crate::ClientMessage) -> Result<Self, ConvertError> {
        use crate::ClientMessage as D;
        use proto::client_message as p;
        use proto::client_message::Kind as K;
        let kind = match m {
            D::Auth { room, token, agent_info } => K::Auth(p::Auth {
                room,
                token,
                agent_info: agent_info.map(|b| (*b).into()),
            }),
            D::ListAgents => K::ListAgents(p::ListAgents {}),
            D::Command { request_id, target, payload } => K::Command(p::Command {
                request_id,
                target: Some(target.into()),
                payload,
            }),
            D::CommandResult { request_id, result } => K::CommandResult(p::CommandResult {
                request_id,
                result,
            }),
            D::CommandError { request_id, error } => {
                K::CommandError(p::CommandError { request_id, error })
            }
            D::Notify { event } => K::Notify(p::Notify { event: Some(event.into()) }),
            D::UdpOffer(o) => K::UdpOffer(o.into()),
            D::UdpAnswer(a) => K::UdpAnswer(a.into()),
            D::UdpResult(r) => K::UdpResult(r.into()),
            D::Ping => K::Ping(p::Ping {}),
            D::Close => K::Close(p::Close {}),
            D::UpdateAgent { agent_info } => K::UpdateAgent(p::UpdateAgent {
                agent_info: Some((*agent_info).into()),
            }),
        };
        Ok(proto::ClientMessage { kind: Some(kind) })
    }
}
impl TryFrom<proto::ClientMessage> for crate::ClientMessage {
    type Error = ConvertError;
    fn try_from(m: proto::ClientMessage) -> Result<Self, ConvertError> {
        use crate::ClientMessage as D;
        use proto::client_message::Kind as K;
        Ok(match m.kind.ok_or(ConvertError::EmptyOneof("ClientMessage"))? {
            K::Auth(a) => D::Auth {
                room: a.room,
                token: a.token,
                agent_info: a.agent_info.map(|i| Box::new(i.into())),
            },
            K::ListAgents(_) => D::ListAgents,
            K::Command(c) => D::Command {
                request_id: c.request_id,
                target: c
                    .target
                    .ok_or(ConvertError::MissingField("ClientMessage.Command.target"))?
                    .try_into()?,
                payload: c.payload,
            },
            K::CommandResult(c) => D::CommandResult {
                request_id: c.request_id,
                result: c.result,
            },
            K::CommandError(c) => D::CommandError { request_id: c.request_id, error: c.error },
            K::Notify(n) => D::Notify {
                event: n
                    .event
                    .ok_or(ConvertError::MissingField("ClientMessage.Notify.event"))?
                    .try_into()?,
            },
            K::UdpOffer(o) => D::UdpOffer(o.try_into()?),
            K::UdpAnswer(a) => D::UdpAnswer(a.try_into()?),
            K::UdpResult(r) => D::UdpResult(r.try_into()?),
            K::Ping(_) => D::Ping,
            K::Close(_) => D::Close,
            K::UpdateAgent(u) => D::UpdateAgent {
                agent_info: Box::new(
                    u.agent_info
                        .ok_or(ConvertError::MissingField("ClientMessage.UpdateAgent.agent_info"))?
                        .into(),
                ),
            },
        })
    }
}

// ============================================================================
// ServerMessage envelope
// ============================================================================

impl TryFrom<crate::ServerMessage> for proto::ServerMessage {
    type Error = ConvertError;
    fn try_from(m: crate::ServerMessage) -> Result<Self, ConvertError> {
        use crate::ServerMessage as D;
        use proto::server_message as p;
        use proto::server_message::Kind as K;
        let kind = match m {
            D::AuthOk { session_id } => K::AuthOk(p::AuthOk { session_id }),
            D::AuthFailed { reason } => K::AuthFailed(p::AuthFailed { reason }),
            D::AgentList { agents } => K::AgentList(p::AgentList {
                agents: agents.into_iter().map(Into::into).collect(),
            }),
            D::AgentJoined { agent } => {
                K::AgentJoined(p::AgentJoined { agent: Some((*agent).into()) })
            }
            D::AgentLeft { agent_id } => K::AgentLeft(p::AgentLeft { agent_id }),
            D::AgentModeChanged { agent_id, mode } => K::AgentModeChanged(p::AgentModeChanged {
                agent_id,
                mode: mode_to_i32(mode),
            }),
            D::Command { request_id, from_session, payload } => K::Command(p::Command {
                request_id,
                from_session,
                payload,
            }),
            D::CommandResult { request_id, agent_id, result } => {
                K::CommandResult(p::CommandResult {
                    request_id,
                    agent_id,
                    result,
                })
            }
            D::CommandError { request_id, agent_id, error } => {
                K::CommandError(p::CommandError { request_id, agent_id, error })
            }
            D::Event { agent_id, event } => {
                K::Event(p::Event { agent_id, event: Some(event.into()) })
            }
            D::UdpOffer { from_session, offer } => {
                K::UdpOffer(p::UdpOffer { from_session, offer: Some(offer.into()) })
            }
            D::UdpAnswer { from_session, answer } => {
                K::UdpAnswer(p::UdpAnswer { from_session, answer: Some(answer.into()) })
            }
            D::UdpResult { from_session, result } => {
                K::UdpResult(p::UdpResult { from_session, result: Some(result.into()) })
            }
            D::YourEndpoint { endpoint } => {
                K::YourEndpoint(p::YourEndpoint { endpoint: Some(endpoint.into()) })
            }
            D::Pong => K::Pong(p::Pong {}),
            D::Error { message } => K::Error(p::Error { message }),
        };
        Ok(proto::ServerMessage { kind: Some(kind) })
    }
}
impl TryFrom<proto::ServerMessage> for crate::ServerMessage {
    type Error = ConvertError;
    fn try_from(m: proto::ServerMessage) -> Result<Self, ConvertError> {
        use crate::ServerMessage as D;
        use proto::server_message::Kind as K;
        Ok(match m.kind.ok_or(ConvertError::EmptyOneof("ServerMessage"))? {
            K::AuthOk(e) => D::AuthOk { session_id: e.session_id },
            K::AuthFailed(e) => D::AuthFailed { reason: e.reason },
            K::AgentList(e) => {
                D::AgentList { agents: e.agents.into_iter().map(Into::into).collect() }
            }
            K::AgentJoined(e) => D::AgentJoined {
                agent: Box::new(
                    e.agent
                        .ok_or(ConvertError::MissingField("ServerMessage.AgentJoined.agent"))?
                        .into(),
                ),
            },
            K::AgentLeft(e) => D::AgentLeft { agent_id: e.agent_id },
            K::AgentModeChanged(e) => {
                D::AgentModeChanged { agent_id: e.agent_id, mode: mode_from_i32(e.mode) }
            }
            K::Command(e) => D::Command {
                request_id: e.request_id,
                from_session: e.from_session,
                payload: e.payload,
            },
            K::CommandResult(e) => D::CommandResult {
                request_id: e.request_id,
                agent_id: e.agent_id,
                result: e.result,
            },
            K::CommandError(e) => {
                D::CommandError { request_id: e.request_id, agent_id: e.agent_id, error: e.error }
            }
            K::Event(e) => D::Event {
                agent_id: e.agent_id,
                event: e
                    .event
                    .ok_or(ConvertError::MissingField("ServerMessage.Event.event"))?
                    .try_into()?,
            },
            K::UdpOffer(e) => D::UdpOffer {
                from_session: e.from_session,
                offer: e
                    .offer
                    .ok_or(ConvertError::MissingField("ServerMessage.UdpOffer.offer"))?
                    .try_into()?,
            },
            K::UdpAnswer(e) => D::UdpAnswer {
                from_session: e.from_session,
                answer: e
                    .answer
                    .ok_or(ConvertError::MissingField("ServerMessage.UdpAnswer.answer"))?
                    .try_into()?,
            },
            K::UdpResult(e) => D::UdpResult {
                from_session: e.from_session,
                result: e
                    .result
                    .ok_or(ConvertError::MissingField("ServerMessage.UdpResult.result"))?
                    .try_into()?,
            },
            K::YourEndpoint(e) => D::YourEndpoint {
                endpoint: req_ep(e.endpoint, "ServerMessage.YourEndpoint.endpoint")?,
            },
            K::Pong(_) => D::Pong,
            K::Error(e) => D::Error { message: e.message },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use std::net::{IpAddr, Ipv4Addr};

    // Compare two domain values structurally without needing PartialEq on every
    // type: serde_json::to_value is deterministic for a given type.
    fn jeq<T: serde::Serialize>(a: &T, b: &T) -> bool {
        serde_json::to_value(a).unwrap() == serde_json::to_value(b).unwrap()
    }

    fn ep() -> Endpoint {
        Endpoint::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 9)), 5000)
    }

    fn agent() -> AgentInfo {
        AgentInfo {
            id: "a-1".into(),
            name: "host".into(),
            mode: AgentMode::Bypass,
            os: "linux".into(),
            arch: "x86_64".into(),
            hostname: "h".into(),
            tags: vec!["gpu".into(), "vm".into()],
            platform: PlatformInfo {
                family: "linux".into(),
                arch: "x86_64".into(),
                distro: Some("Ubuntu 22.04".into()),
                kernel: None,
                shell: Some("/bin/bash".into()),
            },
            autonomous: true,
            accepts_commands: false,
            connected_at: 1234,
            version: "0.1.15".into(),
            session_id: Some("sess".into()),
            update_available: None,
            connections: Some(2),
        }
    }

    // Round-trip a domain value through proto -> prost bytes -> proto -> domain
    // and assert structural equality.
    macro_rules! rt {
        ($domain:expr, $proto_ty:ty) => {{
            let orig = $domain;
            let p: $proto_ty = orig.clone().try_into().expect("domain -> proto");
            let bytes = p.encode_to_vec();
            let p2 = <$proto_ty>::decode(&bytes[..]).expect("decode prost bytes");
            let back = TryInto::try_into(p2).expect("proto -> domain");
            assert!(jeq(&orig, &back), "roundtrip mismatch:\n{:?}\n!=\n{:?}", orig, back);
        }};
    }

    #[test]
    fn command_all_variants_roundtrip() {
        let cmds = vec![
            Command::Exec { command: "ls".into(), timeout_ms: Some(5000), cwd: Some("/tmp".into()) },
            Command::Exec { command: "ls".into(), timeout_ms: None, cwd: None },
            Command::ReadFile { path: "/a".into() },
            Command::WriteFile { path: "/a".into(), content: "x".into(), create_backup: true },
            Command::ListDir { path: "/".into(), pattern: Some("*.rs".into()) },
            Command::GitStatus { repo: "/r".into() },
            Command::GitPull { repo: "/r".into(), remote: "origin".into(), branch: Some("main".into()) },
            Command::GitCommit { repo: "/r".into(), message: "m".into(), files: vec!["a".into()] },
            Command::GitPush { repo: "/r".into(), remote: "origin".into(), branch: None },
            Command::ScheduleAdd { name: "n".into(), cron: "* * * * * *".into(), command: "c".into() },
            Command::ScheduleRemove { name: "n".into() },
            Command::ScheduleList,
            Command::TaskDispatch { prompt: "p".into(), initiator: Some("i".into()) },
            Command::TaskGet { id: "t".into() },
            Command::TaskList,
            Command::SessionList,
            Command::SessionGet { provider: "claude".into(), id: "s".into() },
            Command::SessionResume { provider: "claude".into(), id: "s".into(), prompt: "go".into() },
            Command::SessionTerminate { id: "s".into() },
            Command::FileStat { path: "/f".into() },
            Command::FileChunk { path: "/f".into(), offset: 10, len: 100 },
            Command::FileThumb { path: "/f".into(), max_px: 256 },
            Command::FileSearch { roots: vec!["/".into()], query: "q".into(), kind: SearchKind::Content },
            Command::SendFileTo { src_path: "/s".into(), dest_id: "d".into(), dest_path: "/d".into() },
            Command::FileRecv {
                transfer_id: "t".into(),
                dest_path: "/d".into(),
                offset: 0,
                bytes: b64_encode(&[1u8, 2, 3, 255, 0]),
                eof: true,
                sha256: Some("abc".into()),
            },
            Command::TransferGet { id: "t".into() },
            Command::DirManifest {
                path: "/d".into(),
                with_hash: true,
                exclude: vec!["*.log".into()],
            },
            Command::SyncDirTo {
                src_path: "/s".into(),
                dest_id: "d".into(),
                dest_path: "/d".into(),
                delete: true,
                checksum: true,
                dry_run: false,
                exclude: vec!["node_modules".into(), "/build".into()],
            },
            Command::DeletePaths { paths: vec!["/d/a".into(), "/d/b".into()] },
            Command::TunnelStart { target: "http://localhost:3000".into() },
            Command::TunnelList,
            Command::TunnelStop { id: "t".into() },
            Command::SetMode { mode: AgentMode::Edit },
            Command::GetInfo,
            Command::MapTask { job_id: "j".into(), partition_id: 3, map_fn: "f".into(), data: "[1]".into() },
            Command::ReduceTask { job_id: "j".into(), reduce_fn: "r".into(), inputs: vec!["1".into(), "2".into()] },
        ];
        for c in cmds {
            rt!(c, proto::Command);
        }
    }

    #[test]
    fn command_result_all_variants_roundtrip() {
        let results = vec![
            CommandResult::Exec { stdout: "o".into(), stderr: "e".into(), exit_code: 1, duration_ms: Some(7), timed_out: Some(false) },
            CommandResult::File { content: "c".into(), size: 5 },
            CommandResult::Dir { entries: vec![DirEntry { name: "f".into(), is_dir: false, size: 3, modified: Some(9) }] },
            CommandResult::GitStatus { status: GitStatus { branch: "main".into(), clean: true, ahead: 0, behind: 1, staged: vec![], modified: vec!["a".into()], untracked: vec![] } },
            CommandResult::Git { output: "ok".into(), success: true },
            CommandResult::Info { info: agent() },
            CommandResult::Mode { mode: AgentMode::Plan },
            CommandResult::Schedule { tasks: vec![ScheduledTask { name: "n".into(), cron: "c".into(), command: "cmd".into(), last_run: Some(1), run_count: 4 }] },
            CommandResult::TaskQueued { id: "t".into() },
            CommandResult::Task { task: AutonomousTask { id: "t".into(), initiator: Some("i".into()), prompt: "p".into(), status: TaskStatus::Running, result: None, error: None, created_at: 1, started_at: Some(2), finished_at: None, exit_code: None } },
            CommandResult::TaskList { tasks: vec![] },
            CommandResult::FileMeta { meta: FileMeta { path: "/f".into(), size: 1, modified: None, mime: "image/png".into(), is_image: true } },
            CommandResult::FileChunk { data: b64_encode(&[9u8, 8, 7]), eof: false },
            CommandResult::FileThumb { data: b64_encode(&[1u8, 2]), w: 64, h: 48 },
            CommandResult::FileSearch { hits: vec![] },
            CommandResult::TransferQueued { id: "t".into() },
            CommandResult::Transfer { status: TransferStatus { id: "t".into(), state: TransferState::Running, bytes: 10, total: 100, files_done: 2, files_total: 5, error: None } },
            CommandResult::DirManifest { entries: vec![ManifestEntry { rel_path: "a/b.txt".into(), size: 9, mtime_ms: 123, sha256: Some("deadbeef".into()) }], root_exists: true },
            CommandResult::TunnelStarted { tunnel: TunnelInfo { id: "t".into(), target: "x".into(), public_url: "u".into(), status: "running".into() } },
            CommandResult::TunnelList { tunnels: vec![] },
            CommandResult::SessionList { sessions: vec![SessionMeta { provider: "claude".into(), id: "s".into(), title: "t".into(), updated: 1, cwd: None, resumable: true }], active: vec!["s".into()] },
            CommandResult::SessionTranscript { messages: vec![SessionMessage { role: "user".into(), text: "hi".into(), ts: Some(1) }] },
            CommandResult::MapResult { job_id: "j".into(), partition_id: 0, output: "o".into(), success: true, error: None },
            CommandResult::MapResult { job_id: "j".into(), partition_id: 1, output: "".into(), success: false, error: Some("boom".into()) },
            CommandResult::ReduceResult { job_id: "j".into(), output: "o".into(), success: true, error: None },
            CommandResult::Ok,
        ];
        for r in results {
            rt!(r, proto::CommandResult);
        }
    }

    #[test]
    fn client_message_all_variants_roundtrip() {
        let payload = vec![10u8, 20, 30];
        let offer = UdpOffer {
            channel_id: "c".into(),
            from_session: "a".into(),
            to_session: "b".into(),
            local_endpoint: ep(),
            local_candidates: vec![ep()],
            public_endpoint: Some(ep()),
            nonce: [7u8; 16],
        };
        let answer = UdpAnswer {
            channel_id: "c".into(),
            from_session: "b".into(),
            local_endpoint: ep(),
            local_candidates: vec![],
            public_endpoint: None,
            nonce: [3u8; 16],
            accepted: true,
        };
        let result = UdpChannelResult { channel_id: "c".into(), success: false, error: Some("nat".into()) };
        let msgs = vec![
            ClientMessage::Auth { room: "r".into(), token: "t".into(), agent_info: Some(Box::new(agent())) },
            ClientMessage::Auth { room: "r".into(), token: "t".into(), agent_info: None },
            ClientMessage::ListAgents,
            ClientMessage::Command { request_id: "r".into(), target: Target::Agent { id: "a".into() }, payload: payload.clone() },
            ClientMessage::Command { request_id: "r".into(), target: Target::All, payload: payload.clone() },
            ClientMessage::Command { request_id: "r".into(), target: Target::Tagged { tags: vec!["g".into()] }, payload: payload.clone() },
            ClientMessage::Command { request_id: "r".into(), target: Target::Platform { family: "linux".into() }, payload: payload.clone() },
            ClientMessage::CommandResult { request_id: "r".into(), result: payload.clone() },
            ClientMessage::CommandError { request_id: "r".into(), error: "e".into() },
            ClientMessage::Notify { event: AgentEvent::TaskCompleted { task_id: "t".into(), status: TaskStatus::Done } },
            ClientMessage::UdpOffer(offer),
            ClientMessage::UdpAnswer(answer),
            ClientMessage::UdpResult(result),
            ClientMessage::Ping,
            ClientMessage::Close,
            ClientMessage::UpdateAgent { agent_info: Box::new(agent()) },
        ];
        for m in msgs {
            rt!(m, proto::ClientMessage);
        }
    }

    #[test]
    fn server_message_all_variants_roundtrip() {
        let payload = vec![1u8, 2, 3];
        let offer = UdpOffer {
            channel_id: "c".into(),
            from_session: "a".into(),
            to_session: "b".into(),
            local_endpoint: ep(),
            local_candidates: vec![ep(), ep()],
            public_endpoint: Some(ep()),
            nonce: [1u8; 16],
        };
        let answer = UdpAnswer {
            channel_id: "c".into(),
            from_session: "b".into(),
            local_endpoint: ep(),
            local_candidates: vec![],
            public_endpoint: Some(ep()),
            nonce: [2u8; 16],
            accepted: false,
        };
        let result = UdpChannelResult { channel_id: "c".into(), success: true, error: None };
        let msgs = vec![
            ServerMessage::AuthOk { session_id: "s".into() },
            ServerMessage::AuthFailed { reason: "no".into() },
            ServerMessage::AgentList { agents: vec![agent(), agent()] },
            ServerMessage::AgentJoined { agent: Box::new(agent()) },
            ServerMessage::AgentLeft { agent_id: "a".into() },
            ServerMessage::AgentModeChanged { agent_id: "a".into(), mode: AgentMode::Disabled },
            ServerMessage::Command { request_id: "r".into(), from_session: "s".into(), payload: payload.clone() },
            ServerMessage::CommandResult { request_id: "r".into(), agent_id: "a".into(), result: payload.clone() },
            ServerMessage::CommandError { request_id: "r".into(), agent_id: "a".into(), error: "e".into() },
            ServerMessage::Event { agent_id: "a".into(), event: AgentEvent::TaskCompleted { task_id: "t".into(), status: TaskStatus::Failed } },
            ServerMessage::UdpOffer { from_session: "s".into(), offer },
            ServerMessage::UdpAnswer { from_session: "s".into(), answer },
            ServerMessage::UdpResult { from_session: "s".into(), result },
            ServerMessage::YourEndpoint { endpoint: ep() },
            ServerMessage::Pong,
            ServerMessage::Error { message: "boom".into() },
        ];
        for m in msgs {
            rt!(m, proto::ServerMessage);
        }
    }

    #[test]
    fn ipv6_endpoint_roundtrips() {
        let e = Endpoint::new("2001:db8::1".parse().unwrap(), 9000);
        let p: proto::Endpoint = e.into();
        let back: Endpoint = p.try_into().unwrap();
        assert_eq!(back, e);
    }

    #[test]
    fn error_paths_are_typed_never_panic() {
        // empty oneof
        assert_eq!(
            crate::Command::try_from(proto::Command { kind: None }).unwrap_err(),
            ConvertError::EmptyOneof("Command")
        );
        // bad IP
        assert!(matches!(
            crate::Endpoint::try_from(proto::Endpoint { addr: "not-an-ip".into(), port: 1 }),
            Err(ConvertError::BadIp(_))
        ));
        // port out of u16 range
        assert_eq!(
            crate::Endpoint::try_from(proto::Endpoint { addr: "127.0.0.1".into(), port: 70000 }).unwrap_err(),
            ConvertError::BadPort(70000)
        );
        // bad nonce length (offer with 4-byte nonce)
        let bad = proto::UdpOffer {
            channel_id: "c".into(),
            from_session: "a".into(),
            to_session: "b".into(),
            local_endpoint: Some(proto::Endpoint { addr: "127.0.0.1".into(), port: 1 }),
            local_candidates: vec![],
            public_endpoint: None,
            nonce: vec![0u8; 4],
        };
        assert_eq!(crate::UdpOffer::try_from(bad).unwrap_err(), ConvertError::BadNonce(4));
        // bad base64 in a FileRecv slice (still base64 in the domain; the
        // command/result ENVELOPE is now raw bytes, no base64 there).
        let cmd = Command::FileRecv {
            transfer_id: "t".into(),
            dest_path: "/d".into(),
            offset: 0,
            bytes: "!!!not base64!!!".into(),
            eof: false,
            sha256: None,
        };
        assert_eq!(proto::Command::try_from(cmd).unwrap_err(), ConvertError::BadBase64);
    }
}
