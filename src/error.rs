/// A scheduling or provider failure. Storage failures never become empty queues.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    Invalid(String),
    #[error("provider failed: {0}")]
    Provider(String),
    #[error("queue capacity reached")]
    Capacity,
    #[error("job or lock ownership was lost")]
    OwnershipLost,
    #[error("handler is not registered: {0}")]
    UnknownHandler(String),
    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("worker task failed: {0}")]
    Worker(String),
}

pub type Result<T> = std::result::Result<T, Error>;
