//! Durable scheduling with application-owned storage, locks and handlers.
//!
//! Delivery is at least once. Handlers must make external side effects idempotent.
//! Provider ownership tokens fence acknowledgements, not external services.
#![doc = include_str!("../README.md")]

mod error;
mod job;
pub mod memory;
mod provider;
mod runtime;
mod scheduler;
#[cfg(feature = "test-support")]
pub mod testing;
mod trigger;

pub use error::{Error, Result};
pub use job::*;
pub use provider::*;
pub use runtime::*;
pub use scheduler::*;
pub use trigger::*;

/// Milliseconds since the Unix epoch.
pub type Timestamp = i64;

/// Current UTC wall-clock time, used for job due times rather than timer delays.
pub fn now_ms() -> Timestamp {
    chrono::Utc::now().timestamp_millis()
}
