use chrono::{TimeZone, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use crate::{Error, Job, Result, Timestamp};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DayMatch {
    Or,
    And,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Trigger {
    Once,
    FixedRate {
        interval_ms: u64,
    },
    FixedDelay {
        delay_ms: u64,
    },
    Cron {
        expression: String,
        timezone: String,
        day_match: DayMatch,
    },
}

impl Trigger {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::FixedRate { interval_ms }
            | Self::FixedDelay {
                delay_ms: interval_ms,
            } if *interval_ms == 0 || *interval_ms > i64::MAX as u64 => Err(Error::Invalid(
                "schedule interval must be positive and fit milliseconds".into(),
            )),
            Self::Cron {
                expression,
                timezone,
                ..
            } => {
                timezone
                    .parse::<Tz>()
                    .map_err(|e| Error::Invalid(e.to_string()))?;
                expression
                    .parse::<croner::Cron>()
                    .map_err(|e| Error::Invalid(e.to_string()))?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub fn next_after(&self, at_ms: Timestamp) -> Result<Option<Timestamp>> {
        self.validate()?;
        match self {
            Self::Once => Ok(None),
            Self::FixedRate { interval_ms }
            | Self::FixedDelay {
                delay_ms: interval_ms,
            } => at_ms
                .checked_add(*interval_ms as i64)
                .map(Some)
                .ok_or_else(|| Error::Invalid("schedule timestamp overflow".into())),
            Self::Cron {
                expression,
                timezone,
                day_match,
            } => {
                let tz: Tz = timezone
                    .parse()
                    .map_err(|e: chrono_tz::ParseError| Error::Invalid(e.to_string()))?;
                let instant = Utc
                    .timestamp_millis_opt(at_ms)
                    .single()
                    .ok_or_else(|| Error::Invalid("schedule timestamp out of range".into()))?
                    .with_timezone(&tz);
                let cron = parse_cron(expression, *day_match)?;
                cron.find_next_occurrence(&instant, false)
                    .map(|next| Some(next.timestamp_millis()))
                    .map_err(|e| Error::Invalid(e.to_string()))
            }
        }
    }

    pub(crate) fn latest_due(&self, first: Timestamp, now: Timestamp) -> Result<Timestamp> {
        match self {
            Self::FixedRate { interval_ms } => {
                Ok(first + (now - first) / *interval_ms as i64 * *interval_ms as i64)
            }
            Self::Cron {
                expression,
                timezone,
                day_match,
            } => {
                let tz: Tz = timezone
                    .parse()
                    .map_err(|e: chrono_tz::ParseError| Error::Invalid(e.to_string()))?;
                let now = Utc
                    .timestamp_millis_opt(now)
                    .single()
                    .ok_or_else(|| Error::Invalid("schedule timestamp out of range".into()))?
                    .with_timezone(&tz);
                let cron = parse_cron(expression, *day_match)?;
                cron.find_previous_occurrence(&now, true)
                    .map(|at| at.timestamp_millis().max(first))
                    .map_err(|e| Error::Invalid(e.to_string()))
            }
            _ => Ok(first),
        }
    }
}

fn parse_cron(expression: &str, day_match: DayMatch) -> Result<croner::Cron> {
    croner::parser::CronParser::builder()
        .dom_and_dow(matches!(day_match, DayMatch::And))
        .build()
        .parse(expression)
        .map_err(|e| Error::Invalid(e.to_string()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OverlapPolicy {
    Allow,
    Skip,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MisfirePolicy {
    /// Ignore occurrences older than the configured grace period.
    Skip,
    /// Coalesce missed occurrences into the latest due occurrence.
    FireOnce,
    /// Replay the oldest due occurrences, at most this many per tick.
    CatchUp { max_per_tick: usize },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Schedule {
    pub id: String,
    pub trigger: Trigger,
    pub job: Job,
    pub first_run_at_ms: Timestamp,
    pub overlap: OverlapPolicy,
    pub misfire: MisfirePolicy,
    pub misfire_grace_ms: u64,
    /// Per-occurrence start deadline relative to its scheduled timestamp.
    /// An absolute template deadline, if supplied, remains an upper bound.
    #[serde(default)]
    pub start_deadline_after_ms: Option<u64>,
}

impl Schedule {
    /// Registration start times are initialization hints, not definition
    /// changes. A replica starting later must not reset a durable cursor.
    pub fn same_definition(&self, other: &Self) -> bool {
        self.definition_signature() == other.definition_signature()
    }

    pub fn definition_signature(&self) -> String {
        let mut definition = self.clone();
        definition.first_run_at_ms = 0;
        definition.job.run_at_ms = 0;
        serde_json::to_string(&definition).expect("a Schedule contains only serializable values")
    }

    pub fn new(
        id: impl Into<String>,
        trigger: Trigger,
        job: Job,
        first_run_at_ms: Timestamp,
    ) -> Result<Self> {
        let schedule = Self {
            id: id.into(),
            trigger,
            job,
            first_run_at_ms,
            overlap: OverlapPolicy::Skip,
            misfire: MisfirePolicy::FireOnce,
            misfire_grace_ms: 1000,
            start_deadline_after_ms: None,
        };
        schedule.validate()?;
        Ok(schedule)
    }
    pub fn validate(&self) -> Result<()> {
        self.job.validate()?;
        self.trigger.validate()?;
        if self
            .start_deadline_after_ms
            .is_some_and(|ms| ms > i64::MAX as u64)
        {
            return Err(Error::Invalid("schedule start deadline overflow".into()));
        }
        if self.id.is_empty() || matches!(self.misfire, MisfirePolicy::CatchUp { max_per_tick: 0 })
        {
            return Err(Error::Invalid(
                "schedule id and catch-up limit must be nonempty/positive".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScheduleRecord {
    pub schedule: Schedule,
    pub revision: u64,
    pub next_run_at_ms: Option<Timestamp>,
    pub last_job_id: Option<String>,
}
