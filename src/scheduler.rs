use crate::*;
use std::{collections::BTreeSet, future::Future, sync::Arc, time::Duration};

#[derive(Clone, Debug)]
pub struct SchedulerOptions {
    pub poll_interval: Duration,
    pub batch_size: usize,
    /// None schedules every kind. Some(empty) intentionally disables all kinds.
    pub allowed_kinds: Option<BTreeSet<String>>,
}

impl Default for SchedulerOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(250),
            batch_size: 100,
            allowed_kinds: None,
        }
    }
}

pub struct Scheduler {
    provider: Arc<dyn JobProvider>,
    options: SchedulerOptions,
}

impl Scheduler {
    pub fn new(provider: Arc<dyn JobProvider>, options: SchedulerOptions) -> Result<Self> {
        if options.poll_interval.is_zero() || options.batch_size == 0 {
            return Err(Error::Invalid("invalid scheduler options".into()));
        }
        Ok(Self { provider, options })
    }
    pub async fn register(&self, schedule: Schedule) -> Result<()> {
        self.provider.register_schedule(schedule).await
    }

    /// Plan and persist one bounded tick. All occurrences use revision CAS.
    pub async fn run_once(&self) -> Result<usize> {
        self.run_once_at(crate::now_ms()).await
    }

    pub async fn run_once_at(&self, now: Timestamp) -> Result<usize> {
        let kinds = self
            .options
            .allowed_kinds
            .as_ref()
            .map(|set| set.iter().cloned().collect::<Vec<_>>());
        let records = self
            .provider
            .due_schedules(kinds.as_deref(), self.options.batch_size)
            .await?;
        let mut count = 0;
        for mut record in records {
            let limit = match record.schedule.misfire {
                MisfirePolicy::CatchUp { max_per_tick } => max_per_tick,
                _ => 1,
            };
            for _ in 0..limit.min(self.options.batch_size.saturating_sub(count)) {
                let Some(mut due) = record.next_run_at_ms.filter(|at| *at <= now) else {
                    break;
                };
                if let Trigger::FixedDelay { delay_ms } = record.schedule.trigger
                    && let Some(last) = record.last_job_id.as_deref()
                {
                    let previous = self.provider.get(last).await?.ok_or_else(|| {
                        Error::Provider(
                            "fixed-delay predecessor was removed before scheduling".into(),
                        )
                    })?;
                    if !previous.status.is_terminal() {
                        break;
                    }
                    due = previous
                        .finished_at_ms
                        .ok_or_else(|| {
                            Error::Provider("terminal job has no completion time".into())
                        })?
                        .checked_add(delay_ms as i64)
                        .ok_or_else(|| Error::Invalid("timestamp overflow".into()))?;
                    if due > now {
                        self.provider
                            .advance_schedule(&record.schedule.id, record.revision, None, Some(due))
                            .await?;
                        break;
                    }
                }
                if matches!(
                    record.schedule.misfire,
                    MisfirePolicy::FireOnce | MisfirePolicy::Skip
                ) {
                    due = record.schedule.trigger.latest_due(due, now)?;
                }
                let skip = !matches!(record.schedule.trigger, Trigger::FixedDelay { .. })
                    && record.schedule.misfire == MisfirePolicy::Skip
                    && now.saturating_sub(due) as u64 > record.schedule.misfire_grace_ms;
                let next = record.schedule.trigger.next_after(due)?;
                let mut job = record.schedule.job.clone();
                job.id = format!("schedule:{}:{}", record.schedule.id, due);
                job.run_at_ms = due;
                if let Some(after) = record.schedule.start_deadline_after_ms {
                    let deadline = due
                        .checked_add(after as i64)
                        .ok_or_else(|| Error::Invalid("schedule start deadline overflow".into()))?;
                    job.expires_at_ms = Some(
                        job.expires_at_ms
                            .map_or(deadline, |absolute| absolute.min(deadline)),
                    );
                }
                if record.schedule.overlap == OverlapPolicy::Skip {
                    job.overlap_key = Some(format!("schedule:{}", record.schedule.id));
                }
                let new_job = if skip { None } else { Some(job.clone()) };
                if !self
                    .provider
                    .advance_schedule(&record.schedule.id, record.revision, new_job, next)
                    .await?
                {
                    break;
                }
                record.revision += 1;
                record.next_run_at_ms = next;
                if !skip {
                    record.last_job_id = Some(job.id);
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    pub async fn run_until<F: Future<Output = ()> + Send>(&self, shutdown: F) -> Result<()> {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => return Ok(()),
                result = self.run_once() => match result {
                    Ok(_) | Err(Error::Capacity) => {},
                    Err(error) => return Err(error),
                }
            }
            tokio::select! { () = &mut shutdown => return Ok(()), () = tokio::time::sleep(self.options.poll_interval) => {} }
        }
    }
}
