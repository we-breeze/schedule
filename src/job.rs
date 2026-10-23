use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{Error, Result, Timestamp};

/// Serialized, versionable input; the library never interprets business payloads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub kind: String,
    pub payload: Value,
    pub run_at_ms: Timestamp,
    /// Start deadline. Queued jobs are cancelled at or after this timestamp;
    /// handlers that already started remain governed by timeout and lease.
    #[serde(default)]
    pub expires_at_ms: Option<Timestamp>,
    pub max_failures: u32,
    /// A nonterminal job with this scope prevents another scoped job being queued.
    pub overlap_key: Option<String>,
}

impl Job {
    pub fn new<T: Serialize>(
        id: impl Into<String>,
        kind: impl Into<String>,
        payload: T,
    ) -> Result<Self> {
        Ok(Self {
            id: id.into(),
            kind: kind.into(),
            payload: serde_json::to_value(payload)?,
            run_at_ms: crate::now_ms(),
            expires_at_ms: None,
            max_failures: 3,
            overlap_key: None,
        })
    }

    pub fn decode<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_value(self.payload.clone())?)
    }

    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.kind.is_empty() || self.max_failures == 0 {
            return Err(Error::Invalid(
                "job id, kind and max_failures must be nonempty/positive".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobRecord {
    pub job: Job,
    pub status: JobStatus,
    pub failures: u32,
    pub deliveries: u32,
    pub owner_token: Option<String>,
    pub lease_until_ms: Option<Timestamp>,
    pub finished_at_ms: Option<Timestamp>,
    pub result: Option<Value>,
    pub last_error: Option<String>,
}

impl JobRecord {
    pub fn queued(job: Job) -> Self {
        Self {
            job,
            status: JobStatus::Queued,
            failures: 0,
            deliveries: 0,
            owner_token: None,
            lease_until_ms: None,
            finished_at_ms: None,
            result: None,
            last_error: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claim {
    pub record: JobRecord,
    pub owner_token: String,
}

/// Continuations do not consume the failure budget. Failed retries do.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Completion {
    Success(Value),
    ContinueAt {
        run_at_ms: Timestamp,
        payload: Option<Value>,
    },
    Failure {
        error: String,
        retry_at_ms: Option<Timestamp>,
    },
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitResult {
    Inserted,
    Existing,
    OverlapSkipped,
}

#[derive(Clone, Debug)]
pub struct Lease {
    pub key: String,
    pub token: String,
    pub expires_at_ms: Timestamp,
}
