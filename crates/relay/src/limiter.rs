//! Per-IP abuse gating: a sliding-window connection budget plus an
//! auth-failure ban list.
//!
//! The audit measured ~1.3k unthrottled auth attempts/sec against the relay —
//! token guessing is only bounded by entropy. This module bounds it per
//! client IP:
//! - every WS upgrade / HTTP info call counts against a connection budget
//!   (sliding window); over-budget attempts are denied with `Retry-After`;
//! - every failed authentication is recorded; too many within the failure
//!   window bans the IP for a fixed cooldown.
//!
//! Fail-open by design: if the tracking table hits its hard cap (an address
//! spray), new IPs are simply not throttled rather than the relay OOMing
//! through its own limiter. All state is in-memory (nothing persists across
//! restarts), which is fine — windows are seconds, not hours.

use dashmap::DashMap;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Budgets. Defaults are generous for legitimate use — a fleet of dozens of
/// agents behind one NAT mass-reconnecting after a relay restart stays under
/// the budget, while scripted token guessing (thousands/sec) is cut off.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Max new connections (WS upgrades + HTTP info calls) per IP within
    /// `conn_window`. Denials also count (the window only drains when the IP
    /// goes quiet). `0` disables the connection budget.
    pub max_conns: usize,
    pub conn_window: Duration,
    /// Auth failures within `failure_window` before the IP is banned.
    /// `0` disables failure tracking.
    pub max_failures: usize,
    pub failure_window: Duration,
    /// How long a ban lasts. Fixed (not extended): a banned IP cannot connect,
    /// so it cannot generate further failures; by the time the ban lifts, the
    /// failure window has long drained.
    pub ban_duration: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_conns: 60,
            conn_window: Duration::from_secs(10),
            max_failures: 10,
            failure_window: Duration::from_secs(60),
            ban_duration: Duration::from_secs(300),
        }
    }
}

/// Verdict for one connection attempt.
#[derive(Debug, PartialEq)]
pub enum Verdict {
    Allow,
    Deny {
        /// How long the caller should wait before retrying.
        retry_after: Duration,
    },
}

#[derive(Default)]
struct IpWindows {
    conns: VecDeque<Instant>,
    failures: VecDeque<Instant>,
    banned_until: Option<Instant>,
}

/// Hard ceiling on tracked IPs. Beyond it, previously unseen IPs are not
/// throttled (fail-open); stale entries are swept periodically so the table
/// stays bounded without a background task.
const MAX_TRACKED_IPS: usize = 100_000;
/// Sweep the table every N limiter operations (amortized cleanup).
const SWEEP_EVERY: u64 = 4096;

pub struct RateLimiter {
    limits: Limits,
    entries: DashMap<IpAddr, IpWindows>,
    ops: AtomicU64,
}

impl RateLimiter {
    pub fn new(limits: Limits) -> Self {
        RateLimiter {
            limits,
            entries: DashMap::new(),
            ops: AtomicU64::new(0),
        }
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Check — and count — one connection attempt from `ip`. Denials are also
    /// counted, so a client that keeps hammering stays denied until it pauses
    /// for a full window.
    pub fn gate_connection(&self, ip: IpAddr) -> Verdict {
        let l = &self.limits;
        if l.max_conns == 0 && l.max_failures == 0 {
            return Verdict::Allow; // limiter fully disabled
        }
        let now = Instant::now();
        if self.ops.fetch_add(1, Ordering::Relaxed).is_multiple_of(SWEEP_EVERY) {
            self.sweep(now);
        }
        // Fail-open beyond the tracking cap.
        if !self.entries.contains_key(&ip) && self.entries.len() >= MAX_TRACKED_IPS {
            self.sweep(now);
            if self.entries.len() >= MAX_TRACKED_IPS {
                return Verdict::Allow;
            }
        }
        let mut w = self.entries.entry(ip).or_default();

        // Active ban first.
        if let Some(until) = w.banned_until {
            if now < until {
                return Verdict::Deny {
                    retry_after: until - now,
                };
            }
            w.banned_until = None;
        }
        if l.max_conns == 0 {
            return Verdict::Allow; // budget off, failure-ban-only mode
        }

        while w.conns.front().is_some_and(|t| now.saturating_duration_since(*t) > l.conn_window) {
            w.conns.pop_front();
        }
        w.conns.push_back(now);
        if w.conns.len() > l.max_conns {
            let retry_after = w
                .conns
                .front()
                .map(|t| l.conn_window.saturating_sub(now.saturating_duration_since(*t)))
                .unwrap_or(l.conn_window)
                .max(Duration::from_millis(1));
            return Verdict::Deny { retry_after };
        }
        Verdict::Allow
    }

    /// Record a failed authentication from `ip`. Too many within the failure
    /// window bans the IP for `ban_duration`.
    pub fn record_failure(&self, ip: IpAddr) {
        let l = &self.limits;
        if l.max_failures == 0 {
            return;
        }
        let now = Instant::now();
        if !self.entries.contains_key(&ip) && self.entries.len() >= MAX_TRACKED_IPS {
            self.sweep(now);
            if self.entries.len() >= MAX_TRACKED_IPS {
                return; // fail-open
            }
        }
        let mut w = self.entries.entry(ip).or_default();
        while w.failures.front().is_some_and(|t| now.saturating_duration_since(*t) > l.failure_window)
        {
            w.failures.pop_front();
        }
        w.failures.push_back(now);
        if w.failures.len() > l.max_failures {
            w.banned_until = Some(now + l.ban_duration);
            w.failures.clear();
        }
    }

    /// Clear the failure streak after a successful authentication, so a
    /// long-lived legit client behind NAT never accumulates toward a ban from
    /// occasional blips.
    pub fn record_success(&self, ip: IpAddr) {
        if let Some(mut w) = self.entries.get_mut(&ip) {
            w.failures.clear();
        }
    }

    /// Drop entries with no live window and no active ban.
    fn sweep(&self, now: Instant) {
        let horizon = self
            .limits
            .conn_window
            .max(self.limits.failure_window);
        self.entries.retain(|_, w| {
            w.banned_until.is_some_and(|u| u > now)
                || w.conns.back().is_some_and(|t| now.saturating_duration_since(*t) < horizon)
                || w
                    .failures
                    .back()
                    .is_some_and(|t| now.saturating_duration_since(*t) < horizon)
        });
    }
}

/// Constant-time equality for secrets (auth/server tokens): no early exit on
/// the first differing byte, so a network observer cannot time-sweep the token
/// prefix-by-prefix. Comparing lengths still leaks the length, which is
/// standard and acceptable (padding would complicate every caller for little
/// gain).
pub fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn ip(n: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, n))
    }

    #[test]
    fn ct_eq_has_no_early_exit_and_matches_semantics() {
        assert!(ct_eq("secret", "secret"));
        assert!(!ct_eq("secret", "secreX"));
        assert!(!ct_eq("secret", "secrets")); // length differs
        assert!(ct_eq("", ""));
    }

    #[test]
    fn connection_budget_denies_over_limit_then_recovers() {
        let l = RateLimiter::new(Limits {
            max_conns: 3,
            conn_window: Duration::from_millis(80),
            max_failures: 0,
            ..Default::default()
        });
        for _ in 0..3 {
            assert_eq!(l.gate_connection(ip(1)), Verdict::Allow);
        }
        match l.gate_connection(ip(1)) {
            Verdict::Deny { retry_after } => assert!(retry_after <= Duration::from_millis(80)),
            v => panic!("4th attempt must be denied, got {v:?}"),
        }
        // A different IP has its own budget.
        assert_eq!(l.gate_connection(ip(2)), Verdict::Allow);
        // After the window drains, the original IP is allowed again.
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(l.gate_connection(ip(1)), Verdict::Allow);
    }

    #[test]
    fn denials_count_so_hammering_stays_denied() {
        let l = RateLimiter::new(Limits {
            max_conns: 2,
            conn_window: Duration::from_millis(200),
            max_failures: 0,
            ..Default::default()
        });
        assert_eq!(l.gate_connection(ip(3)), Verdict::Allow);
        assert_eq!(l.gate_connection(ip(3)), Verdict::Allow);
        // Denied attempts keep the window full: still denied at t=~60ms.
        std::thread::sleep(Duration::from_millis(60));
        assert!(matches!(l.gate_connection(ip(3)), Verdict::Deny { .. }));
    }

    #[test]
    fn auth_failures_ban_and_ban_expires() {
        let l = RateLimiter::new(Limits {
            max_conns: 0, // ban-only mode still works without a budget
            max_failures: 2,
            failure_window: Duration::from_secs(60),
            ban_duration: Duration::from_millis(120),
            ..Default::default()
        });
        l.record_failure(ip(4));
        l.record_failure(ip(4));
        assert_eq!(l.gate_connection(ip(4)), Verdict::Allow); // 2 == max, not over
        l.record_failure(ip(4)); // 3rd within window → banned
        match l.gate_connection(ip(4)) {
            Verdict::Deny { retry_after } => assert!(retry_after <= Duration::from_millis(120)),
            v => panic!("banned IP must be denied, got {v:?}"),
        }
        // Success clears a streak before it becomes a ban.
        l.record_failure(ip(5));
        l.record_success(ip(5));
        l.record_failure(ip(5));
        assert_eq!(l.gate_connection(ip(5)), Verdict::Allow);
        // Ban lifts after the cooldown.
        std::thread::sleep(Duration::from_millis(140));
        assert_eq!(l.gate_connection(ip(4)), Verdict::Allow);
    }

    #[test]
    fn failure_window_slides() {
        let l = RateLimiter::new(Limits {
            max_failures: 2,
            failure_window: Duration::from_millis(60),
            ban_duration: Duration::from_millis(200),
            ..Default::default()
        });
        l.record_failure(ip(6));
        std::thread::sleep(Duration::from_millis(70));
        l.record_failure(ip(6)); // first one expired → streak is 1, not 2
        l.record_failure(ip(6)); // now 2 == max, still no ban
        assert_eq!(l.gate_connection(ip(6)), Verdict::Allow);
    }

    #[test]
    fn disabled_limiter_allows_everything() {
        let l = RateLimiter::new(Limits {
            max_conns: 0,
            max_failures: 0,
            ..Default::default()
        });
        for _ in 0..100 {
            assert_eq!(l.gate_connection(ip(7)), Verdict::Allow);
        }
        l.record_failure(ip(7)); // no-op when disabled
        assert_eq!(l.gate_connection(ip(7)), Verdict::Allow);
    }
}
