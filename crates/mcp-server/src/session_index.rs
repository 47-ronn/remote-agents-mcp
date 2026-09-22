//! Local full-text index over the host's AI-chat history (ctx-style).
//!
//! The session importers in [`crate::sessions`] parse provider transcripts on
//! demand; this module turns them into a persistent
//! [Tantivy](https://github.com/quickwit-oss/tantivy) index so a query
//! ("failed migration") returns *ranked, cited snippets* instead of forcing
//! the caller to pull whole transcripts. Each message is a document; a hit
//! carries `session_id` + `seq` so the UI can jump straight to the context
//! window (`SessionGet { around_seq }`).
//!
//! Design (borrowed from ctx):
//! - **Incremental**: a sidecar manifest maps `provider␟session_id` → the
//!   session's `updated` fingerprint; only changed sessions are re-parsed and
//!   re-indexed (delete by composite key + re-add), removed ones deleted.
//! - **Atomic**: a refresh is one writer commit — searchers never observe a
//!   partially built generation.
//! - **Budgeted**: a refresh has a soft time budget; sessions it didn't reach
//!   stay unindexed and are picked up by the next refresh (converges).
//! - **Token-efficient**: results are per-session best snippets with a match
//!   count, capped by `limit` — tiny payloads vs. a 700 KB transcript.
//!
//! All entry points are blocking (fs + writer) → callers keep them inside
//! `spawn_blocking`, like the rest of [`crate::sessions`].

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _, Result};
use remote_agents_shared::{SessionMeta, SessionMessage, SessionSearchHit};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, EmptyQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Value, STORED, STRING, TEXT};
use tantivy::snippet::SnippetGenerator;
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument, Term};

/// Default max hits returned by [`search`].
pub const DEFAULT_LIMIT: usize = 20;
/// Snippet fragment length cap (characters).
const SNIPPET_MAX_CHARS: usize = 240;
/// Per-provider cap for the indexer's session scan (list + transcripts). The
/// panel shows 200; the index wants the deep history. Overridable with
/// `REMOTE_AGENTS_SESSION_INDEX_MAX`.
const DEFAULT_INDEX_MAX_PER_PROVIDER: usize = 2_000;
/// Soft wall-clock budget for one refresh pass; sessions not reached are
/// deferred to the next refresh (first build on a big history converges).
const REFRESH_BUDGET: Duration = Duration::from_secs(90);
/// Re-scan even without explicit staleness no more often than this.
const REFRESH_TTL: Duration = Duration::from_secs(60);

/// Set when the underlying history changed (autonomous task finished, …) so
/// the next search refreshes before answering.
static STALE: AtomicBool = AtomicBool::new(true);

// --- schema -------------------------------------------------------------------

/// Field set of the index. One document = one session message.
struct Fields {
    /// Full message text — the searched BM25 field (stored for snippets).
    text: Field,
    /// `provider␟session_id` composite key — the delete handle (not stored).
    sid: Field,
    provider: Field,
    session_id: Field,
    title: Field,
    role: Field,
    cwd: Field,
    /// 0-based message position in the transcript.
    seq: Field,
    /// Message ts (Unix ms; 0 = unknown).
    ts: Field,
    /// Session `updated` (Unix ms) — denormalized for display.
    updated: Field,
}

fn build_schema() -> (tantivy::schema::Schema, Fields) {
    let mut b = tantivy::schema::Schema::builder();
    let f = Fields {
        text: b.add_text_field("text", TEXT | STORED),
        sid: b.add_text_field("sid", STRING),
        provider: b.add_text_field("provider", STRING | STORED),
        session_id: b.add_text_field("session_id", STRING | STORED),
        title: b.add_text_field("title", STRING | STORED),
        role: b.add_text_field("role", STRING | STORED),
        cwd: b.add_text_field("cwd", STRING | STORED),
        seq: b.add_i64_field("seq", STORED),
        ts: b.add_i64_field("ts", STORED),
        updated: b.add_i64_field("updated", STORED),
    };
    (b.build(), f)
}

fn sid_key(provider: &str, id: &str) -> String {
    format!("{provider}\u{1f}{id}")
}

// --- manifest -------------------------------------------------------------------

/// Sidecar mapping session key → last-indexed fingerprint (the session's
/// `updated` value). Missing/changed fingerprint → re-index that session.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct Manifest {
    fingerprints: HashMap<String, u64>,
}

impl Manifest {
    fn load(dir: &Path) -> Self {
        fs::read_to_string(dir.join("manifest.json"))
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save(&self, dir: &Path) {
        let tmp = dir.join("manifest.json.tmp");
        let final_path = dir.join("manifest.json");
        if let Ok(json) = serde_json::to_string(&self) {
            if fs::write(&tmp, json).and_then(|_| fs::rename(&tmp, &final_path)).is_err() {
                tracing::warn!("session index: failed to persist manifest");
            }
        }
    }
}

// --- cached index handle ---------------------------------------------------------

struct Handle {
    reader: IndexReader,
    /// The writer lives for the process lifetime (tantivy allows one per
    /// index; committing publishes a new generation to the reader).
    writer: Mutex<IndexWriter>,
    fields: Fields,
}

impl Handle {
    fn open(dir: &Path) -> Result<Self> {
        let (schema, fields) = build_schema();
        let index = if dir.join("meta.json").exists() {
            Index::open_in_dir(dir)?
        } else {
            fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
            Index::create_in_dir(dir, schema)?
        };
        let reader = index.reader()?;
        // Single writer thread: deterministic, no worker lifecycle to manage
        // inside spawn_blocking; parsing (not indexing) is the bottleneck.
        let writer = index.writer_with_num_threads(1, 32 * 1024 * 1024)?;
        Ok(Self { reader, writer: Mutex::new(writer), fields })
    }
}

fn index_dir() -> PathBuf {
    dirs::data_dir()
        .map(|p| p.join("remote-agents").join("sessions-index"))
        .unwrap_or_else(|| PathBuf::from("sessions-index"))
}

/// Open (or create) the index at the default location; `None` when it can't
/// be opened (first search then just answers empty rather than erroring).
/// The error is cached so a broken directory doesn't spam retries.
fn open_handle() -> Option<&'static Handle> {
    static HANDLE: OnceLock<Result<Handle, String>> = OnceLock::new();
    HANDLE
        .get_or_init(|| Handle::open(&index_dir()).map_err(|e| format!("{e:#}")))
        .as_ref()
        .ok()
}

fn index_max_per_provider() -> usize {
    std::env::var("REMOTE_AGENTS_SESSION_INDEX_MAX")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_INDEX_MAX_PER_PROVIDER)
}

// --- public API -------------------------------------------------------------------

/// Mark the index out-of-date (called when the history just changed, e.g. an
/// autonomous task finished). The next search refreshes before answering.
pub fn mark_stale() {
    STALE.store(true, Ordering::Release);
}

/// Full-text search over the local chat history: BM25-ranked, per-session
/// best snippet + match count. Refreshes the index first when stale.
pub fn search(query: &str, providers: &[String], limit: usize) -> Result<Vec<SessionSearchHit>> {
    let Some(h) = open_handle() else {
        // No index at all (dir unavailable): answer empty rather than error.
        return Ok(vec![]);
    };
    refresh_if_stale();
    search_reader(&h.reader, &h.fields, query, providers, limit)
}

/// Refresh now (blocking; bounded by the refresh budget).
/// Returns the number of re-indexed sessions. When the budget ran out, the
/// stale flag stays set so the *next* search continues the build (a huge
/// first-time history converges over a few searches).
pub fn refresh() -> Result<usize> {
    let Some(h) = open_handle() else {
        bail!("session index unavailable");
    };
    // The panel's 30s cache may be cold; the indexer wants the fresh, deep list.
    crate::sessions::invalidate_cache();
    let scanned = crate::sessions::list_sessions_with(index_max_per_provider());
    let (sessions, failed, budget_exhausted) = collect_changed(&scanned, &index_dir());
    let mut writer = h.writer.lock().expect("index writer poisoned");
    let n = apply_sessions(&mut writer, &h.fields, &scanned, sessions, failed, &index_dir())?;
    drop(writer);
    h.reader.reload()?;
    if !budget_exhausted {
        STALE.store(false, Ordering::Release);
    }
    Ok(n)
}

fn refresh_if_stale() {
    static LAST_REFRESH: Mutex<Option<Instant>> = Mutex::new(None);
    let stale = STALE.load(Ordering::Acquire);
    let due = LAST_REFRESH
        .lock()
        .map(|g| g.is_none_or(|at| at.elapsed() > REFRESH_TTL))
        .unwrap_or(true);
    if stale || due {
        if let Err(e) = refresh() {
            tracing::warn!("session index refresh failed: {e:#}");
        }
        if let Ok(mut g) = LAST_REFRESH.lock() {
            *g = Some(Instant::now());
        }
    }
}

// --- indexing pipeline -------------------------------------------------------------

/// One session + its (already fetched) full transcript.
struct IndexedSession {
    meta: SessionMeta,
    messages: Vec<SessionMessage>,
}

/// Of the scanned sessions, fetch full transcripts for the ones whose
/// fingerprint changed, respecting the refresh budget. Returns
/// `(to_index, failed, budget_exhausted)`: failed sessions keep their
/// fingerprint recorded (so a broken transcript doesn't burn the budget on
/// every refresh); a content change bumps `updated` and retries them. When
/// the budget ran out, remaining sessions stay unindexed — the caller keeps
/// the stale flag on so the next search picks up where this pass stopped.
fn collect_changed(
    scanned: &[SessionMeta],
    dir: &Path,
) -> (Vec<IndexedSession>, Vec<SessionMeta>, bool) {
    let manifest = Manifest::load(dir);
    let deadline = Instant::now() + REFRESH_BUDGET;
    let mut out = Vec::new();
    let mut failed = Vec::new();
    for meta in scanned {
        let key = sid_key(&meta.provider, &meta.id);
        if manifest.fingerprints.get(&key) == Some(&meta.updated) {
            continue; // unchanged since last index
        }
        if Instant::now() > deadline {
            tracing::info!(
                "session index: refresh budget reached after {} sessions; continuing on next refresh",
                out.len() + failed.len()
            );
            return (out, failed, true);
        }
        match crate::sessions::full_transcript(&meta.provider, &meta.id) {
            Ok(messages) => out.push(IndexedSession { meta: meta.clone(), messages }),
            Err(e) => {
                tracing::debug!("session index: skip {} {}: {e:#}", meta.provider, meta.id);
                failed.push(meta.clone());
            }
        }
    }
    (out, failed, false)
}

/// Core, testable indexing step:
/// - re-index `sessions` (delete old docs by composite key, add new);
/// - record fingerprints for `failed` (indexed as 0 docs, skipped until
///   their content changes);
/// - delete sessions that vanished from the provider scan;
/// - commit and persist the manifest.
///
/// Returns the number of re-indexed sessions.
fn apply_sessions(
    writer: &mut IndexWriter,
    fields: &Fields,
    scanned: &[SessionMeta],
    sessions: Vec<IndexedSession>,
    failed: Vec<SessionMeta>,
    dir: &Path,
) -> Result<usize> {
    let mut manifest = Manifest::load(dir);

    // Everything the provider scan currently sees (changed or not).
    let scanned_keys: std::collections::HashSet<String> =
        scanned.iter().map(|m| sid_key(&m.provider, &m.id)).collect();

    // Delete sessions that vanished from the provider scans.
    for key in manifest.fingerprints.keys() {
        if !scanned_keys.contains(key) {
            writer.delete_term(Term::from_field_text(fields.sid, key));
        }
    }

    let mut reindexed = 0usize;
    for s in sessions {
        let key = sid_key(&s.meta.provider, &s.meta.id);
        writer.delete_term(Term::from_field_text(fields.sid, &key));
        for (seq, m) in s.messages.iter().enumerate() {
            if m.text.trim().is_empty() {
                continue;
            }
            let mut doc = TantivyDocument::default();
            doc.add_text(fields.text, m.text.as_str());
            doc.add_text(fields.sid, key.as_str());
            doc.add_text(fields.provider, s.meta.provider.as_str());
            doc.add_text(fields.session_id, s.meta.id.as_str());
            doc.add_text(fields.title, s.meta.title.as_str());
            doc.add_text(fields.role, m.role.as_str());
            if let Some(cwd) = s.meta.cwd.as_deref() {
                doc.add_text(fields.cwd, cwd);
            }
            doc.add_i64(fields.seq, seq as i64);
            doc.add_i64(fields.ts, m.ts.unwrap_or(0) as i64);
            doc.add_i64(fields.updated, s.meta.updated as i64);
            writer.add_document(doc)?;
        }
        manifest.fingerprints.insert(key, s.meta.updated);
        reindexed += 1;
    }
    for m in failed {
        manifest
            .fingerprints
            .insert(sid_key(&m.provider, &m.id), m.updated);
    }

    writer.commit()?;
    manifest.save(dir);
    Ok(reindexed)
}

// --- search ------------------------------------------------------------------------

/// Pure search over a reader — also the test entry point.
fn search_reader(
    reader: &IndexReader,
    fields: &Fields,
    query: &str,
    providers: &[String],
    limit: usize,
) -> Result<Vec<SessionSearchHit>> {
    let q = query.trim();
    if q.is_empty() {
        bail!("empty query");
    }
    let limit = limit.clamp(1, 100);

    let text_query = parse_user_query(reader.searcher().index(), fields, q)?;
    // Snippets are generated from a second parse of the same user query —
    // never from the provider filter (its terms must not drive highlighting).
    let snip_query = parse_user_query(reader.searcher().index(), fields, q)?;
    let effective: Box<dyn Query> = if providers.is_empty() {
        text_query
    } else {
        let filter: Vec<(Occur, Box<dyn Query>)> = providers
            .iter()
            .map(|p| {
                (
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(fields.provider, p.as_str()),
                        IndexRecordOption::Basic,
                    )) as Box<dyn Query>,
                )
            })
            .collect();
        Box::new(BooleanQuery::from(vec![
            (Occur::Must, text_query),
            (Occur::Must, Box::new(BooleanQuery::from(filter))),
        ]))
    };

    let searcher = reader.searcher();
    // Over-fetch so per-session aggregation still fills `limit` sessions.
    let top = searcher.search(
        effective.as_ref(),
        &TopDocs::with_limit(limit * 4 + 20).order_by_score(),
    )?;

    // Snippets come from the *text* query only (never the provider filter).
    let mut snipper = SnippetGenerator::create(&searcher, snip_query.as_ref(), fields.text).ok();
    if let Some(sg) = snipper.as_mut() {
        sg.set_max_num_chars(SNIPPET_MAX_CHARS);
    }

    let mut groups: HashMap<(String, String), (SessionSearchHit, u32)> = HashMap::new();
    let mut order: Vec<(String, String)> = Vec::new();

    for (score, addr) in top {
        let Ok(doc) = searcher.doc::<TantivyDocument>(addr) else { continue };
        let Some(text) = doc.get_first(fields.text).and_then(|v| v.as_str()) else {
            continue;
        };
        let get_str = |f: Field| {
            doc.get_first(f)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string()
        };
        let provider = get_str(fields.provider);
        let session_id = get_str(fields.session_id);
        if provider.is_empty() || session_id.is_empty() {
            continue;
        }
        let seq = doc.get_first(fields.seq).and_then(|v| v.as_i64()).unwrap_or(0).max(0) as u32;
        let ts = doc.get_first(fields.ts).and_then(|v| v.as_i64()).unwrap_or(0).max(0) as u64;

        let snippet_text = match snipper.as_ref() {
            Some(sg) => {
                let snippet = sg.snippet_from_doc(&doc);
                let frag = snippet.fragment().trim();
                if frag.is_empty() {
                    head_chars(text, SNIPPET_MAX_CHARS)
                } else {
                    frag.to_string()
                }
            }
            None => head_chars(text, SNIPPET_MAX_CHARS),
        };

        let key = (provider.clone(), session_id.clone());
        let hit = SessionSearchHit {
            provider: provider.clone(),
            session_id: session_id.clone(),
            title: get_str(fields.title),
            role: get_str(fields.role),
            snippet: snippet_text,
            ts: (ts > 0).then_some(ts),
            seq,
            score,
            match_count: 1,
            cwd: doc
                .get_first(fields.cwd)
                .and_then(|v| v.as_str())
                .map(String::from),
        };
        match groups.get_mut(&key) {
            Some((best, count)) => {
                *count += 1;
                if hit.score > best.score {
                    *best = hit;
                }
            }
            None => {
                groups.insert(key.clone(), (hit, 1));
                order.push(key);
            }
        }
    }

    let mut out: Vec<SessionSearchHit> = order
        .into_iter()
        .filter_map(|k| {
            groups.remove(&k).map(|(mut hit, count)| {
                hit.match_count = count;
                hit
            })
        })
        .collect();
    out.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    out.truncate(limit);
    Ok(out)
}

/// Tolerant query parsing: raw user input; on a parse error, retry with the
/// whole string escaped as a single phrase (dots/slashes/colons are common in
/// file names and error strings).
fn parse_user_query(
    index: &Index,
    fields: &Fields,
    q: &str,
) -> Result<Box<dyn Query>> {
    let parser = QueryParser::for_index(index, vec![fields.text]);
    match parser.parse_query(q) {
        Ok(query) => Ok(Box::new(query)),
        Err(_) => {
            let escaped = format!("\"{}\"", q.replace('"', " "));
            let query = parser
                .parse_query(&escaped)
                .unwrap_or_else(|_| Box::new(EmptyQuery));
            Ok(query)
        }
    }
}

fn head_chars(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let cut: String = s.chars().take(n).collect();
        format!("{cut}…")
    }
}

// --- tests ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("ra-sidx-{}-{}", tag, std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn meta(provider: &str, id: &str, updated: u64) -> SessionMeta {
        SessionMeta {
            provider: provider.into(),
            id: id.into(),
            title: format!("{provider} {id}"),
            updated,
            cwd: Some(format!("/repo/{id}")),
            resumable: true,
        }
    }

    fn msg(role: &str, text: &str, ts: u64) -> SessionMessage {
        SessionMessage { role: role.into(), text: text.into(), ts: Some(ts) }
    }

    /// Build an index at `dir` from the given sessions and return a reader.
    fn build(
        dir: &Path,
        sessions: Vec<IndexedSession>,
    ) -> (IndexReader, Fields) {
        let (schema, fields) = build_schema();
        let index = Index::create_in_dir(dir, schema).unwrap();
        let mut writer = index.writer_with_num_threads(1, 16 * 1024 * 1024).unwrap();
        let scanned: Vec<SessionMeta> = sessions.iter().map(|s| s.meta.clone()).collect();
        let n = apply_sessions(&mut writer, &fields, &scanned, sessions, vec![], dir).unwrap();
        assert!(n > 0);
        let reader = index.reader().unwrap();
        reader.reload().unwrap();
        (reader, fields)
    }

    #[test]
    fn search_ranks_and_cites_by_session() {
        let dir = tmp_dir("rank");
        let (reader, fields) = build(
            &dir,
            vec![
                IndexedSession {
                    meta: meta("claude", "s1", 100),
                    messages: vec![
                        msg("user", "fix the failed migration on prod", 1),
                        msg("assistant", "the migration failed because of the old cursor name", 2),
                        msg("assistant", "unrelated small talk about weather", 3),
                    ],
                },
                IndexedSession {
                    meta: meta("opencode", "s2", 200),
                    messages: vec![
                        msg("user", "how do I brew coffee", 4),
                        msg("assistant", "failed migration retry logic explained here", 5),
                    ],
                },
            ],
        );

        let hits = search_reader(&reader, &fields, "failed migration", &[], 20).unwrap();
        assert_eq!(hits.len(), 2, "both sessions match");
        assert!(hits[0].score >= hits[1].score);
        for h in &hits {
            assert!(
                h.snippet.to_lowercase().contains("migration"),
                "snippet cites the match: {}",
                h.snippet
            );
            assert!(h.match_count >= 1);
            assert!(h.cwd.is_some());
        }
        // The strongest hit mentions both terms.
        assert!(hits[0].snippet.to_lowercase().contains("failed"));
    }

    #[test]
    fn provider_filter_excludes_others() {
        let dir = tmp_dir("filter");
        let (reader, fields) = build(
            &dir,
            vec![
                IndexedSession {
                    meta: meta("claude", "s1", 100),
                    messages: vec![msg("user", "failed migration", 1)],
                },
                IndexedSession {
                    meta: meta("opencode", "s2", 100),
                    messages: vec![msg("user", "failed migration", 2)],
                },
            ],
        );
        let hits = search_reader(&reader, &fields, "failed migration", &["opencode".to_string()], 20)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].provider, "opencode");
    }

    #[test]
    fn special_characters_query_does_not_error() {
        let dir = tmp_dir("special");
        let (reader, fields) = build(
            &dir,
            vec![IndexedSession {
                meta: meta("claude", "s1", 100),
                messages: vec![msg("user", "error in /usr/local/bin:42 (timeout)", 1)],
            }],
        );
        // Raw input with parser-hostile characters must not blow up.
        let hits = search_reader(&reader, &fields, "/usr/local/bin:42 (timeout)", &[], 20).unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].snippet.contains("timeout"));
    }

    #[test]
    fn incremental_refresh_deletes_and_updates() {
        let dir = tmp_dir("incr");
        let (schema, fields) = build_schema();
        let index = Index::create_in_dir(&dir, schema).unwrap();
        let mut writer = index.writer_with_num_threads(1, 16 * 1024 * 1024).unwrap();

        let v1 = vec![
            IndexedSession {
                meta: meta("claude", "keep", 100),
                messages: vec![msg("user", "alpha topic", 1)],
            },
            IndexedSession {
                meta: meta("claude", "drop", 100),
                messages: vec![msg("user", "beta topic", 2)],
            },
        ];
        let scanned1: Vec<SessionMeta> = v1.iter().map(|s| s.meta.clone()).collect();
        apply_sessions(&mut writer, &fields, &scanned1, v1, vec![], &dir).unwrap();

        // v2: one session gone, one extended (bumped fingerprint).
        let v2 = vec![IndexedSession {
            meta: meta("claude", "keep", 101),
            messages: vec![
                msg("user", "alpha topic", 1),
                msg("assistant", "gamma topic added", 3),
            ],
        }];
        let scanned2: Vec<SessionMeta> = v2.iter().map(|s| s.meta.clone()).collect();
        apply_sessions(&mut writer, &fields, &scanned2, v2, vec![], &dir).unwrap();

        let reader = index.reader().unwrap();
        reader.reload().unwrap();

        let hits = search_reader(&reader, &fields, "beta", &[], 20).unwrap();
        assert!(hits.is_empty(), "dropped session must be deleted from the index");
        let hits = search_reader(&reader, &fields, "gamma", &[], 20).unwrap();
        assert_eq!(hits.len(), 1, "extended session must be re-indexed");
        assert_eq!(hits[0].match_count, 1);
        let hits = search_reader(&reader, &fields, "alpha", &[], 20).unwrap();
        assert_eq!(hits.len(), 1, "unchanged content still searchable after re-index");
    }

    #[test]
    fn failed_sessions_are_skipped_until_content_changes() {
        let dir = tmp_dir("failed");
        let (schema, fields) = build_schema();
        let index = Index::create_in_dir(&dir, schema).unwrap();
        let mut writer = index.writer_with_num_threads(1, 16 * 1024 * 1024).unwrap();

        // Session "broken" fails to parse (recorded, 0 docs); "ok" indexes.
        let v1 = vec![IndexedSession {
            meta: meta("claude", "ok", 100),
            messages: vec![msg("user", "alpha topic", 1)],
        }];
        let scanned: Vec<SessionMeta> =
            vec![meta("claude", "ok", 100), meta("claude", "broken", 100)];
        apply_sessions(&mut writer, &fields, &scanned, v1, vec![meta("claude", "broken", 100)], &dir)
            .unwrap();

        let reader = index.reader().unwrap();
        reader.reload().unwrap();
        let hits = search_reader(&reader, &fields, "alpha", &[], 20).unwrap();
        assert_eq!(hits.len(), 1);
        // "broken" contributes no docs but is in the manifest (not retried,
        // not treated as vanished).
        let manifest = Manifest::load(&dir);
        assert!(manifest.fingerprints.contains_key(&sid_key("claude", "broken")));
        assert!(manifest.fingerprints.contains_key(&sid_key("claude", "ok")));
    }

    #[test]
    fn empty_query_is_rejected() {
        let dir = tmp_dir("empty");
        let (reader, fields) = build(
            &dir,
            vec![IndexedSession {
                meta: meta("claude", "s", 1),
                messages: vec![msg("user", "x", 1)],
            }],
        );
        assert!(search_reader(&reader, &fields, "   ", &[], 20).is_err());
    }

    #[test]
    fn per_session_aggregation_caps_output() {
        let dir = tmp_dir("limit");
        let (reader, fields) = build(
            &dir,
            vec![IndexedSession {
                meta: meta("claude", "s", 1),
                messages: (0..30)
                    .map(|i| msg("user", &format!("unique{i:02} needle"), i))
                    .collect(),
            }],
        );
        let hits = search_reader(&reader, &fields, "needle", &[], 5).unwrap();
        assert_eq!(hits.len(), 1, "one session → one aggregated hit");
        assert!(hits[0].match_count >= 5, "match_count counts all messages");
    }
}
