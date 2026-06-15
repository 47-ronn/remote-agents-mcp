//! MapReduce job coordination (Phase 13).
//!
//! This module holds the *coordinator-side* bookkeeping for distributed
//! map/reduce jobs: splitting input across the fleet and tracking each
//! partition's map result until the job is ready to reduce. It is deliberately
//! pure (no I/O, no networking) so the dispatch/runtime layers can be built and
//! tested on top of it independently.
//!
//! The protocol pieces it pairs with live in `remote_agents_shared`:
//! [`Command::MapTask`]/[`Command::ReduceTask`] and the matching
//! `CommandResult::MapResult`/`ReduceResult`.

use remote_agents_shared::{Command, CommandResult};
use std::future::Future;

/// Split `items` into at most `workers` balanced partitions.
///
/// Partition sizes differ by at most one. When there are fewer items than
/// workers, only `items.len()` (non-empty) partitions are returned — we never
/// dispatch an empty partition. `workers == 0` or no items yields no work.
pub fn partition<T: Clone>(items: &[T], workers: usize) -> Vec<Vec<T>> {
    if items.is_empty() || workers == 0 {
        return Vec::new();
    }
    let n = workers.min(items.len());
    let base = items.len() / n;
    let rem = items.len() % n;

    let mut out = Vec::with_capacity(n);
    let mut start = 0;
    for i in 0..n {
        // The first `rem` partitions take one extra item to stay balanced.
        let len = base + if i < rem { 1 } else { 0 };
        out.push(items[start..start + len].to_vec());
        start += len;
    }
    out
}

/// Status of a single map partition within a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionStatus {
    /// Dispatched (or awaiting dispatch); no result yet.
    Pending,
    /// Map completed successfully, carrying its output.
    Done(String),
    /// Map failed, carrying the error message (eligible for re-dispatch).
    Failed(String),
}

/// Tracks the map phase of one job: a fixed set of partitions, each moving from
/// `Pending` to `Done`/`Failed` as results arrive. Once every partition is
/// `Done`, the ordered outputs can be handed to the reduce phase.
#[derive(Debug, Clone)]
pub struct JobState {
    pub job_id: String,
    partitions: Vec<PartitionStatus>,
    /// Count of failed map attempts per partition (drives retry capping).
    attempts: Vec<u32>,
}

impl JobState {
    /// Create a job expecting `count` map partitions.
    pub fn new(job_id: impl Into<String>, count: usize) -> Self {
        Self {
            job_id: job_id.into(),
            partitions: vec![PartitionStatus::Pending; count],
            attempts: vec![0; count],
        }
    }

    /// Number of partitions in this job.
    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// Record a map result for `partition_id`. Out-of-range ids are ignored
    /// (returns `false`); a valid record returns `true`. Re-recording a
    /// partition overwrites its status (e.g. a retry's success replaces a
    /// previous failure).
    pub fn record(&mut self, partition_id: u32, result: Result<String, String>) -> bool {
        let idx = partition_id as usize;
        let Some(slot) = self.partitions.get_mut(idx) else {
            return false;
        };
        *slot = match result {
            Ok(output) => PartitionStatus::Done(output),
            Err(error) => {
                self.attempts[idx] += 1;
                PartitionStatus::Failed(error)
            }
        };
        true
    }

    /// Partition ids that have failed (regardless of how many attempts).
    pub fn failed(&self) -> Vec<u32> {
        self.ids_where(|s| matches!(s, PartitionStatus::Failed(_)))
    }

    /// Number of failed attempts recorded for `partition_id` (0 if out of range).
    pub fn attempts(&self, partition_id: u32) -> u32 {
        self.attempts.get(partition_id as usize).copied().unwrap_or(0)
    }

    /// Failed partitions still eligible for re-dispatch — those whose failure
    /// count has not exceeded `max_retries`. With `max_retries = 2`, a partition
    /// may be retried after its 1st and 2nd failures, but not the 3rd.
    pub fn redispatchable(&self, max_retries: u32) -> Vec<u32> {
        self.failed()
            .into_iter()
            .filter(|&id| self.attempts(id) <= max_retries)
            .collect()
    }

    /// Whether any partition has failed beyond `max_retries` — the job can no
    /// longer complete and should be reported as failed (vs. retried forever).
    pub fn is_exhausted(&self, max_retries: u32) -> bool {
        self.failed()
            .into_iter()
            .any(|id| self.attempts(id) > max_retries)
    }

    /// Partition ids still awaiting a result.
    pub fn pending(&self) -> Vec<u32> {
        self.ids_where(|s| matches!(s, PartitionStatus::Pending))
    }

    /// Whether every partition completed successfully.
    pub fn is_complete(&self) -> bool {
        !self.partitions.is_empty()
            && self
                .partitions
                .iter()
                .all(|s| matches!(s, PartitionStatus::Done(_)))
    }

    /// The ordered map outputs, but only once the whole job is complete.
    /// Returns `None` while any partition is still pending or failed, so the
    /// reduce phase can never run on a partial result set.
    pub fn outputs(&self) -> Option<Vec<String>> {
        if !self.is_complete() {
            return None;
        }
        Some(
            self.partitions
                .iter()
                .map(|s| match s {
                    PartitionStatus::Done(o) => o.clone(),
                    _ => unreachable!("is_complete guarantees all Done"),
                })
                .collect(),
        )
    }

    fn ids_where(&self, pred: impl Fn(&PartitionStatus) -> bool) -> Vec<u32> {
        self.partitions
            .iter()
            .enumerate()
            .filter(|(_, s)| pred(s))
            .map(|(i, _)| i as u32)
            .collect()
    }
}

/// Plan the map phase of a job: split `items` across up to `workers` partitions
/// and build one [`Command::MapTask`] per partition (its `data` is the
/// partition's items as a JSON array). Returns the tracking [`JobState`]
/// alongside the tasks ready for fleet dispatch.
pub fn plan_map_tasks(
    job_id: impl Into<String>,
    items: &[String],
    map_fn: &str,
    workers: usize,
) -> (JobState, Vec<Command>) {
    let job_id = job_id.into();
    let parts = partition(items, workers);
    let job = JobState::new(job_id.clone(), parts.len());
    let tasks = parts
        .iter()
        .enumerate()
        .map(|(i, p)| Command::MapTask {
            job_id: job_id.clone(),
            partition_id: i as u32,
            map_fn: map_fn.to_string(),
            // Opaque to the protocol; the map runtime parses this back.
            data: serde_json::to_string(p).unwrap_or_else(|_| "[]".to_string()),
        })
        .collect();
    (job, tasks)
}

/// Build the [`Command::ReduceTask`] for a completed job, folding the ordered
/// map outputs. Returns `None` while the job is still incomplete, so reduce can
/// never be dispatched on a partial result set.
pub fn build_reduce_task(job: &JobState, reduce_fn: &str) -> Option<Command> {
    job.outputs().map(|inputs| Command::ReduceTask {
        job_id: job.job_id.clone(),
        reduce_fn: reduce_fn.to_string(),
        inputs,
    })
}

/// Fold one map/reduce `CommandResult` into a `(output, success, error)` tuple.
fn result_outcome(res: Result<CommandResult, String>) -> Result<String, String> {
    match res {
        Ok(CommandResult::MapResult { output, success, error, .. })
        | Ok(CommandResult::ReduceResult { output, success, error, .. }) => {
            if success {
                Ok(output)
            } else {
                Err(error.unwrap_or_else(|| "map/reduce reported failure".to_string()))
            }
        }
        Ok(other) => Err(format!("unexpected result type: {other:?}")),
        Err(e) => Err(e), // dispatch/transport error
    }
}

/// Run a full MapReduce job: partition `items`, dispatch each `MapTask`, retry
/// failed partitions up to `max_retries`, then dispatch the `ReduceTask` and
/// return its output.
///
/// `dispatch` is the transport seam — it sends one [`Command`] to a worker and
/// yields its [`CommandResult`] (or a transport error). Keeping it injected
/// makes the orchestration testable without a live relay; the real MCP tool
/// passes a closure backed by fleet dispatch.
pub async fn run_job<F, Fut>(
    job_id: impl Into<String>,
    items: &[String],
    map_fn: &str,
    reduce_fn: &str,
    workers: usize,
    max_retries: u32,
    dispatch: F,
) -> Result<String, String>
where
    F: Fn(Command) -> Fut,
    Fut: Future<Output = Result<CommandResult, String>>,
{
    let (mut job, tasks) = plan_map_tasks(job_id, items, map_fn, workers);

    // Dispatch a set of partitions concurrently and fold their results in.
    async fn dispatch_round<F, Fut>(job: &mut JobState, tasks: &[Command], ids: &[u32], dispatch: &F)
    where
        F: Fn(Command) -> Fut,
        Fut: Future<Output = Result<CommandResult, String>>,
    {
        let futures = ids
            .iter()
            .map(|&id| dispatch(tasks[id as usize].clone()));
        let results = futures::future::join_all(futures).await;
        for (&id, res) in ids.iter().zip(results) {
            job.record(id, result_outcome(res));
        }
    }

    if !job.is_empty() {
        // Round 1: every partition.
        let initial = job.pending();
        dispatch_round(&mut job, &tasks, &initial, &dispatch).await;

        // Retry rounds until complete or a partition exhausts its retries.
        while !job.is_complete() {
            if job.is_exhausted(max_retries) {
                return Err(format!(
                    "job '{}' failed: partition(s) {:?} exhausted {} retries",
                    job.job_id,
                    job.failed(),
                    max_retries
                ));
            }
            let retry = job.redispatchable(max_retries);
            // Not complete, nothing retryable, not exhausted → defensive guard.
            if retry.is_empty() {
                return Err(format!("job '{}' stalled with no progress", job.job_id));
            }
            dispatch_round(&mut job, &tasks, &retry, &dispatch).await;
        }
    }

    // Reduce phase. Empty input has no partitions (and is not "complete"), so
    // build the reduce over an empty input set explicitly.
    let reduce_task = if job.is_empty() {
        Command::ReduceTask {
            job_id: job.job_id.clone(),
            reduce_fn: reduce_fn.to_string(),
            inputs: Vec::new(),
        }
    } else {
        build_reduce_task(&job, reduce_fn)
            .ok_or_else(|| format!("job '{}' not complete; cannot reduce", job.job_id))?
    };
    result_outcome(dispatch(reduce_task).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_balances_evenly() {
        let items: Vec<u32> = (0..9).collect();
        let parts = partition(&items, 3);
        assert_eq!(parts.len(), 3);
        assert!(parts.iter().all(|p| p.len() == 3));
        // Concatenation preserves order and covers every item.
        let flat: Vec<u32> = parts.concat();
        assert_eq!(flat, items);
    }

    #[test]
    fn partition_distributes_remainder_to_first_partitions() {
        let items: Vec<u32> = (0..10).collect();
        let parts = partition(&items, 3);
        let sizes: Vec<usize> = parts.iter().map(|p| p.len()).collect();
        assert_eq!(sizes, vec![4, 3, 3]); // differ by at most one
        assert_eq!(parts.concat(), items);
    }

    #[test]
    fn partition_never_creates_empty_partitions() {
        let items: Vec<u32> = (0..2).collect();
        let parts = partition(&items, 5); // more workers than items
        assert_eq!(parts.len(), 2);
        assert!(parts.iter().all(|p| !p.is_empty()));
    }

    #[test]
    fn partition_edge_cases() {
        assert!(partition::<u32>(&[], 4).is_empty());
        assert!(partition(&[1, 2, 3], 0).is_empty());
        let one = partition(&[42], 1);
        assert_eq!(one, vec![vec![42]]);
    }

    #[test]
    fn job_tracks_pending_until_all_done() {
        let mut job = JobState::new("j1", 3);
        assert_eq!(job.len(), 3);
        assert_eq!(job.pending(), vec![0, 1, 2]);
        assert!(!job.is_complete());
        assert!(job.outputs().is_none());

        assert!(job.record(0, Ok("a".into())));
        assert!(job.record(2, Ok("c".into())));
        assert_eq!(job.pending(), vec![1]);
        assert!(!job.is_complete());

        assert!(job.record(1, Ok("b".into())));
        assert!(job.is_complete());
        // Outputs are returned in partition order, not arrival order.
        assert_eq!(job.outputs(), Some(vec!["a".into(), "b".into(), "c".into()]));
    }

    #[test]
    fn job_reports_failed_partitions_for_redispatch() {
        let mut job = JobState::new("j1", 3);
        job.record(0, Ok("a".into()));
        job.record(1, Err("boom".into()));
        job.record(2, Ok("c".into()));

        assert_eq!(job.failed(), vec![1]);
        assert!(!job.is_complete());
        assert!(job.outputs().is_none());

        // A retry succeeds and clears the failure.
        job.record(1, Ok("b".into()));
        assert!(job.failed().is_empty());
        assert!(job.is_complete());
    }

    #[test]
    fn retry_counting_caps_redispatch() {
        let mut job = JobState::new("j", 2);
        // Partition 0 keeps failing; partition 1 is fine.
        job.record(1, Ok("ok".into()));

        job.record(0, Err("e1".into())); // attempt 1
        assert_eq!(job.attempts(0), 1);
        assert_eq!(job.redispatchable(2), vec![0]); // 1 <= 2 → retry
        assert!(!job.is_exhausted(2));

        job.record(0, Err("e2".into())); // attempt 2
        assert_eq!(job.redispatchable(2), vec![0]); // 2 <= 2 → still retry
        assert!(!job.is_exhausted(2));

        job.record(0, Err("e3".into())); // attempt 3
        assert!(job.redispatchable(2).is_empty()); // 3 > 2 → no more retries
        assert!(job.is_exhausted(2)); // job can never complete
        assert!(!job.is_complete());
    }

    #[test]
    fn successful_retry_clears_failure_and_attempts_frozen() {
        let mut job = JobState::new("j", 1);
        job.record(0, Err("boom".into()));
        assert_eq!(job.attempts(0), 1);
        assert_eq!(job.redispatchable(3), vec![0]);

        // The retry succeeds: no longer failed, attempts stay at the failure count.
        job.record(0, Ok("done".into()));
        assert!(job.failed().is_empty());
        assert!(job.redispatchable(3).is_empty());
        assert!(job.is_complete());
        assert_eq!(job.attempts(0), 1);
    }

    #[test]
    fn job_ignores_out_of_range_partition() {
        let mut job = JobState::new("j1", 2);
        assert!(!job.record(5, Ok("x".into())));
        assert_eq!(job.pending(), vec![0, 1]);
    }

    #[test]
    fn empty_job_is_not_complete() {
        let job = JobState::new("j0", 0);
        assert!(job.is_empty());
        assert!(!job.is_complete());
        assert!(job.outputs().is_none());
    }

    fn strings(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn plan_map_tasks_partitions_into_tasks() {
        let items = strings(&["a", "b", "c", "d", "e"]);
        let (job, tasks) = plan_map_tasks("job-1", &items, "m", 2);

        assert_eq!(job.len(), 2);
        assert_eq!(tasks.len(), 2);

        // Reconstruct the data carried by each task and check coverage/order.
        let mut reassembled = Vec::new();
        for (i, t) in tasks.iter().enumerate() {
            match t {
                Command::MapTask { job_id, partition_id, map_fn, data } => {
                    assert_eq!(job_id, "job-1");
                    assert_eq!(*partition_id, i as u32);
                    assert_eq!(map_fn, "m");
                    let part: Vec<String> = serde_json::from_str(data).unwrap();
                    reassembled.extend(part);
                }
                other => panic!("expected MapTask, got {other:?}"),
            }
        }
        assert_eq!(reassembled, items);
    }

    #[test]
    fn plan_map_tasks_empty_input_yields_no_tasks() {
        let (job, tasks) = plan_map_tasks("j", &[], "m", 4);
        assert!(job.is_empty());
        assert!(tasks.is_empty());
    }

    // --- run_job orchestration (fake dispatcher) ---------------------------

    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A fake worker: maps a partition to `"<data>!"`, reduces by joining inputs
    /// with `,`. `fail_first` partition ids fail on their first attempt (to
    /// exercise retry), and `always_fail` ids never succeed.
    struct FakeWorker {
        attempts: Mutex<HashMap<u32, u32>>,
        fail_first: Vec<u32>,
        always_fail: Vec<u32>,
    }

    impl FakeWorker {
        fn new() -> Self {
            Self {
                attempts: Mutex::new(HashMap::new()),
                fail_first: vec![],
                always_fail: vec![],
            }
        }
        async fn handle(&self, cmd: Command) -> Result<CommandResult, String> {
            match cmd {
                Command::MapTask { job_id, partition_id, data, .. } => {
                    let n = {
                        let mut a = self.attempts.lock().unwrap();
                        let e = a.entry(partition_id).or_insert(0);
                        *e += 1;
                        *e
                    };
                    let fail = self.always_fail.contains(&partition_id)
                        || (self.fail_first.contains(&partition_id) && n == 1);
                    if fail {
                        Ok(CommandResult::MapResult {
                            job_id,
                            partition_id,
                            output: String::new(),
                            success: false,
                            error: Some(format!("partition {partition_id} attempt {n} failed")),
                        })
                    } else {
                        // data is a JSON array of the partition's items.
                        let items: Vec<String> = serde_json::from_str(&data).unwrap();
                        Ok(CommandResult::MapResult {
                            job_id,
                            partition_id,
                            output: format!("{}!", items.join("+")),
                            success: true,
                            error: None,
                        })
                    }
                }
                Command::ReduceTask { job_id, inputs, .. } => Ok(CommandResult::ReduceResult {
                    job_id,
                    output: inputs.join(","),
                    success: true,
                    error: None,
                }),
                other => Err(format!("unexpected command: {other:?}")),
            }
        }
    }

    fn strs(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn run_job_happy_path_maps_then_reduces() {
        let worker = FakeWorker::new();
        let items = strs(&["a", "b", "c", "d"]);
        let out = run_job("j", &items, "m", "r", 2, 1, |cmd| worker.handle(cmd))
            .await
            .unwrap();
        // 2 workers → partitions ["a","b"] and ["c","d"] → "a+b!","c+d!" → reduce join.
        assert_eq!(out, "a+b!,c+d!");
    }

    #[tokio::test]
    async fn run_job_retries_transient_failures() {
        let mut worker = FakeWorker::new();
        worker.fail_first = vec![0]; // partition 0 fails once, then succeeds
        let items = strs(&["x", "y", "z", "w"]);
        let out = run_job("j", &items, "m", "r", 2, 2, |cmd| worker.handle(cmd))
            .await
            .unwrap();
        assert_eq!(out, "x+y!,z+w!");
        // Partition 0 was attempted twice (1 fail + 1 success).
        assert_eq!(*worker.attempts.lock().unwrap().get(&0).unwrap(), 2);
    }

    #[tokio::test]
    async fn run_job_fails_when_partition_exhausts_retries() {
        let mut worker = FakeWorker::new();
        worker.always_fail = vec![1];
        let items = strs(&["a", "b", "c", "d"]);
        // 4 workers → 4 partitions, so partition id 1 exists and always fails.
        let err = run_job("jx", &items, "m", "r", 4, 1, |cmd| worker.handle(cmd))
            .await
            .unwrap_err();
        assert!(err.contains("exhausted"), "unexpected error: {err}");
        assert!(err.contains("jx"));
    }

    #[tokio::test]
    async fn run_job_empty_input_reduces_over_nothing() {
        let worker = FakeWorker::new();
        let out = run_job("j0", &[], "m", "r", 4, 1, |cmd| worker.handle(cmd))
            .await
            .unwrap();
        assert_eq!(out, ""); // reduce over [] → empty join
    }

    #[test]
    fn build_reduce_task_waits_for_completion() {
        let items = strings(&["x", "y"]);
        let (mut job, _tasks) = plan_map_tasks("job-2", &items, "m", 2);

        // Not complete yet → no reduce task.
        assert!(build_reduce_task(&job, "r").is_none());

        job.record(0, Ok("1".into()));
        assert!(build_reduce_task(&job, "r").is_none());
        job.record(1, Ok("2".into()));

        match build_reduce_task(&job, "r").expect("reduce task ready") {
            Command::ReduceTask { job_id, reduce_fn, inputs } => {
                assert_eq!(job_id, "job-2");
                assert_eq!(reduce_fn, "r");
                assert_eq!(inputs, strings(&["1", "2"]));
            }
            other => panic!("expected ReduceTask, got {other:?}"),
        }
    }
}
