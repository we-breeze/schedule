//! Explicit single-process providers. They provide no cross-process durability.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::*;
use async_trait::async_trait;

#[derive(Default)]
struct State {
    jobs: BTreeMap<String, JobRecord>,
    schedules: BTreeMap<String, ScheduleRecord>,
    locks: BTreeMap<String, Lease>,
}

pub struct MemoryProvider {
    state: Mutex<State>,
    capacity: usize,
    clock: Arc<dyn Fn() -> Timestamp + Send + Sync>,
}

impl MemoryProvider {
    pub fn new(capacity: usize) -> Result<Self> {
        Self::with_clock(capacity, Arc::new(crate::now_ms))
    }
    pub fn with_clock(
        capacity: usize,
        clock: Arc<dyn Fn() -> Timestamp + Send + Sync>,
    ) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::Invalid("capacity must be positive".into()));
        }
        Ok(Self {
            state: Mutex::new(State::default()),
            capacity,
            clock,
        })
    }

    fn insert(&self, state: &mut State, job: Job) -> Result<SubmitResult> {
        job.validate()?;
        if state.jobs.contains_key(&job.id) {
            return Ok(SubmitResult::Existing);
        }
        if let Some(scope) = job.overlap_key.as_ref()
            && state
                .jobs
                .values()
                .any(|r| !r.status.is_terminal() && r.job.overlap_key.as_ref() == Some(scope))
        {
            return Ok(SubmitResult::OverlapSkipped);
        }
        if state
            .jobs
            .values()
            .filter(|r| !r.status.is_terminal())
            .count()
            >= self.capacity
        {
            return Err(Error::Capacity);
        }
        state.jobs.insert(job.id.clone(), JobRecord::queued(job));
        Ok(SubmitResult::Inserted)
    }

    fn prune(&self, state: &mut State) {
        let mut terminal = state
            .jobs
            .values()
            .filter(|r| {
                r.status.is_terminal()
                    && !state
                        .schedules
                        .values()
                        .any(|s| s.last_job_id.as_deref() == Some(&r.job.id))
            })
            .map(|r| (r.finished_at_ms.unwrap_or_default(), r.job.id.clone()))
            .collect::<Vec<_>>();
        terminal.sort();
        let remove = terminal.len().saturating_sub(self.capacity);
        for (_, id) in terminal.into_iter().take(remove) {
            state.jobs.remove(&id);
        }
    }
}

#[async_trait]
impl JobProvider for MemoryProvider {
    async fn submit(&self, job: Job) -> Result<SubmitResult> {
        self.insert(&mut self.state.lock().unwrap(), job)
    }
    async fn get(&self, id: &str) -> Result<Option<JobRecord>> {
        Ok(self.state.lock().unwrap().jobs.get(id).cloned())
    }
    async fn claim(
        &self,
        owner: &str,
        kinds: &[String],
        limit: usize,
        lease_ms: u64,
    ) -> Result<Vec<Claim>> {
        if owner.is_empty() || lease_ms == 0 || lease_ms > i64::MAX as u64 {
            return Err(Error::Invalid("invalid claim".into()));
        }
        let now = (self.clock)();
        let until = now
            .checked_add(lease_ms as i64)
            .ok_or_else(|| Error::Invalid("lease overflow".into()))?;
        let mut state = self.state.lock().unwrap();
        for record in state.jobs.values_mut() {
            if record.status == JobStatus::Running
                && record.lease_until_ms.is_some_and(|until| until <= now)
            {
                record.status = JobStatus::Queued;
                record.owner_token = None;
                record.lease_until_ms = None;
            }
            if record.status == JobStatus::Queued
                && record.job.expires_at_ms.is_some_and(|at| at <= now)
            {
                record.status = JobStatus::Cancelled;
                record.finished_at_ms = Some(now);
            }
        }
        self.prune(&mut state);
        let mut ids = state
            .jobs
            .values()
            .filter(|r| {
                r.status == JobStatus::Queued
                    && r.job.run_at_ms <= now
                    && kinds.contains(&r.job.kind)
            })
            .map(|r| (r.job.run_at_ms, r.job.id.clone()))
            .collect::<Vec<_>>();
        ids.sort();
        let mut claims = Vec::new();
        for (_, id) in ids.into_iter().take(limit) {
            let record = state.jobs.get_mut(&id).unwrap();
            let token = format!("{owner}:{}", uuid::Uuid::new_v4());
            record.owner_token = Some(token.clone());
            record.lease_until_ms = Some(until);
            record.status = JobStatus::Running;
            record.deliveries += 1;
            claims.push(Claim {
                record: record.clone(),
                owner_token: token,
            });
        }
        Ok(claims)
    }
    async fn renew(&self, id: &str, token: &str, lease_ms: u64) -> Result<bool> {
        if lease_ms == 0 || lease_ms > i64::MAX as u64 {
            return Err(Error::Invalid("invalid lease".into()));
        }
        let now = (self.clock)();
        let mut state = self.state.lock().unwrap();
        let Some(record) = state.jobs.get_mut(id) else {
            return Ok(false);
        };
        if !owns(record, token, now) {
            return Ok(false);
        }
        record.lease_until_ms = Some(now.saturating_add(lease_ms as i64));
        Ok(true)
    }
    async fn finish(&self, id: &str, token: &str, completion: Completion) -> Result<bool> {
        let now = (self.clock)();
        let mut state = self.state.lock().unwrap();
        let Some(record) = state.jobs.get_mut(id) else {
            return Ok(false);
        };
        if !owns(record, token, now) {
            return Ok(false);
        }
        match completion {
            Completion::Success(value) => {
                record.status = JobStatus::Succeeded;
                record.result = Some(value);
            }
            Completion::Cancelled => record.status = JobStatus::Cancelled,
            Completion::ContinueAt { run_at_ms, payload } => {
                record.status = JobStatus::Queued;
                record.job.run_at_ms = run_at_ms;
                if let Some(payload) = payload {
                    record.job.payload = payload;
                }
            }
            Completion::Failure { error, retry_at_ms } => {
                record.failures += 1;
                record.last_error = Some(error);
                if let Some(at) = retry_at_ms.filter(|_| record.failures < record.job.max_failures)
                {
                    record.status = JobStatus::Queued;
                    record.job.run_at_ms = at;
                } else {
                    record.status = JobStatus::Failed;
                }
            }
        }
        record.owner_token = None;
        record.lease_until_ms = None;
        if record.status.is_terminal() {
            record.finished_at_ms = Some(now);
        }
        self.prune(&mut state);
        Ok(true)
    }
    async fn register_schedule(&self, schedule: Schedule) -> Result<()> {
        schedule.validate()?;
        let mut state = self.state.lock().unwrap();
        if let Some(old) = state.schedules.get(&schedule.id) {
            if old.schedule.same_definition(&schedule) {
                return Ok(());
            }
        } else if state.schedules.len() >= self.capacity {
            return Err(Error::Capacity);
        }
        let revision = state
            .schedules
            .get(&schedule.id)
            .map_or(0, |old| old.revision + 1);
        state.schedules.insert(
            schedule.id.clone(),
            ScheduleRecord {
                next_run_at_ms: Some(schedule.first_run_at_ms),
                schedule,
                revision,
                last_job_id: None,
            },
        );
        Ok(())
    }
    async fn due_schedules(
        &self,
        kinds: Option<&[String]>,
        limit: usize,
    ) -> Result<Vec<ScheduleRecord>> {
        let now = (self.clock)();
        let mut records = self
            .state
            .lock()
            .unwrap()
            .schedules
            .values()
            .filter(|s| {
                s.next_run_at_ms.is_some_and(|at| at <= now)
                    && kinds.is_none_or(|kinds| kinds.contains(&s.schedule.job.kind))
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|s| (s.next_run_at_ms, s.schedule.id.clone()));
        records.truncate(limit);
        Ok(records)
    }
    async fn advance_schedule(
        &self,
        id: &str,
        revision: u64,
        job: Option<Job>,
        next_run_at_ms: Option<Timestamp>,
    ) -> Result<bool> {
        let mut state = self.state.lock().unwrap();
        if state
            .schedules
            .get(id)
            .is_none_or(|s| s.revision != revision)
        {
            return Ok(false);
        }
        let mut last_job = None;
        if let Some(job) = job {
            let id = job.id.clone();
            if self.insert(&mut state, job)? != SubmitResult::OverlapSkipped {
                last_job = Some(id);
            }
        }
        let record = state.schedules.get_mut(id).unwrap();
        record.revision += 1;
        record.next_run_at_ms = next_run_at_ms;
        if last_job.is_some() {
            record.last_job_id = last_job;
        }
        Ok(true)
    }
}

fn owns(record: &JobRecord, token: &str, now: Timestamp) -> bool {
    record.status == JobStatus::Running
        && record.owner_token.as_deref() == Some(token)
        && record.lease_until_ms.is_some_and(|until| until > now)
}

#[async_trait]
impl LockProvider for MemoryProvider {
    async fn try_acquire(&self, key: &str, token: &str, lease_ms: u64) -> Result<Option<Lease>> {
        if key.is_empty() || token.is_empty() || lease_ms == 0 || lease_ms > i64::MAX as u64 {
            return Err(Error::Invalid("invalid lock".into()));
        }
        let now = (self.clock)();
        let mut state = self.state.lock().unwrap();
        if state
            .locks
            .get(key)
            .is_some_and(|lease| lease.expires_at_ms > now && lease.token != token)
        {
            return Ok(None);
        }
        let lease = Lease {
            key: key.into(),
            token: token.into(),
            expires_at_ms: now.saturating_add(lease_ms as i64),
        };
        state.locks.insert(key.into(), lease.clone());
        Ok(Some(lease))
    }
    async fn renew_lock(&self, lease: &Lease, lease_ms: u64) -> Result<bool> {
        if lease_ms == 0 || lease_ms > i64::MAX as u64 {
            return Err(Error::Invalid("invalid lease".into()));
        }
        let now = (self.clock)();
        let mut state = self.state.lock().unwrap();
        let Some(current) = state.locks.get_mut(&lease.key) else {
            return Ok(false);
        };
        if current.token != lease.token || current.expires_at_ms <= now {
            return Ok(false);
        }
        current.expires_at_ms = now.saturating_add(lease_ms as i64);
        Ok(true)
    }
    async fn release(&self, lease: &Lease) -> Result<bool> {
        let mut state = self.state.lock().unwrap();
        if state.locks.get(&lease.key).is_some_and(|current| {
            current.token == lease.token && current.expires_at_ms > (self.clock)()
        }) {
            state.locks.remove(&lease.key);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
