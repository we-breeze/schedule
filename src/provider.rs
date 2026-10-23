use async_trait::async_trait;

use crate::{
    Claim, Completion, Job, JobRecord, Lease, Result, Schedule, ScheduleRecord, SubmitResult,
    Timestamp,
};

/// Atomic durable operations. Implementations must reject expired/stale owners.
/// Claim also recovers expired reservations; no separate recovery daemon is needed.
/// Queued start deadlines are cancelled before filtering kinds or due times.
/// Delivery is at least once; job identity remains deduplicated while retained.
#[async_trait]
pub trait JobProvider: Send + Sync + 'static {
    async fn submit(&self, job: Job) -> Result<SubmitResult>;
    async fn get(&self, id: &str) -> Result<Option<JobRecord>>;
    async fn claim(
        &self,
        owner: &str,
        kinds: &[String],
        limit: usize,
        lease_ms: u64,
    ) -> Result<Vec<Claim>>;
    async fn renew(&self, id: &str, token: &str, lease_ms: u64) -> Result<bool>;
    async fn finish(&self, id: &str, token: &str, completion: Completion) -> Result<bool>;
    async fn register_schedule(&self, schedule: Schedule) -> Result<()>;
    async fn due_schedules(
        &self,
        kinds: Option<&[String]>,
        limit: usize,
    ) -> Result<Vec<ScheduleRecord>>;
    /// Atomically enqueue the occurrence and advance its cursor. Capacity errors
    /// leave the cursor untouched. Revision conflicts return false.
    async fn advance_schedule(
        &self,
        id: &str,
        revision: u64,
        job: Option<Job>,
        next_run_at_ms: Option<Timestamp>,
    ) -> Result<bool>;
}

/// General distributed mutual exclusion, independent from job storage.
/// Release/renew must compare the token atomically. Errors must propagate.
#[async_trait]
pub trait LockProvider: Send + Sync + 'static {
    async fn try_acquire(&self, key: &str, token: &str, lease_ms: u64) -> Result<Option<Lease>>;
    async fn renew_lock(&self, lease: &Lease, lease_ms: u64) -> Result<bool>;
    async fn release(&self, lease: &Lease) -> Result<bool>;
}
