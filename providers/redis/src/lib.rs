//! Redis implementations of the provider-independent scheduling contracts.
//! Connections are constructed as one logical shard. Every state transition is
//! an atomic Lua script on the writer; no blocking consumer uses a shared socket.
#![doc = include_str!("../README.md")]

mod jobs;
mod locks;
pub use locks::{LockKeyPolicy, RedisLockProvider};

use brz_redis::{Redis, RedisService, RedisServiceOptions};
use brz_schedule::{Error, Result};
use serde_json::Value;
use std::time::Duration;

#[derive(Clone)]
pub struct RedisConfig {
    /// Breeze endpoint syntax: host:port:database.
    pub endpoint: String,
    pub password: Option<String>,
    pub namespace: String,
    /// Maximum number of queued and running jobs combined.
    pub capacity: usize,
    /// Terminal retention bounds the idempotency horizon. Fixed-delay
    /// predecessors remain pinned until their next occurrence is persisted.
    pub retained_terminal: usize,
}

impl std::fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConfig")
            .field("endpoint", &self.endpoint)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("namespace", &self.namespace)
            .field("capacity", &self.capacity)
            .field("retained_terminal", &self.retained_terminal)
            .finish()
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            endpoint: "127.0.0.1:6379:0".into(),
            password: None,
            namespace: "brz:schedule:v1".into(),
            capacity: 10_000,
            retained_terminal: 10_000,
        }
    }
}

#[derive(Clone)]
pub struct RedisProvider {
    redis: RedisService,
    config: RedisConfig,
    keys: Vec<String>,
}

impl RedisProvider {
    /// Select the lock-key contract explicitly. Raw keys are passed unchanged.
    pub fn locks(&self, policy: LockKeyPolicy) -> RedisLockProvider {
        RedisLockProvider::new(self.redis.clone(), policy)
    }
    pub async fn connect(config: RedisConfig) -> Result<Self> {
        if config.namespace.is_empty()
            || config.capacity == 0
            || config.retained_terminal == 0
            || config.capacity > 100_000
            || config.retained_terminal > 100_000
        {
            return Err(Error::Invalid(
                "namespace and bounded capacities must be valid (1..=100000)".into(),
            ));
        }
        let options = RedisServiceOptions::default()
            .with_password(config.password.clone())
            .with_resource_name("schedule")
            .with_timeout(Duration::from_secs(5));
        let redis = RedisService::single_with_options(config.endpoint.clone(), options)
            .await
            .map_err(provider_error)?;
        let keys = [
            "jobs",
            "ready",
            "inflight",
            "terminal",
            "scopes",
            "schedules",
            "schedule-due",
            "pins",
            "expiry",
        ]
        .iter()
        .map(|suffix| format!("{}:{suffix}", config.namespace))
        .collect();
        Ok(Self {
            redis,
            config,
            keys,
        })
    }

    async fn operation(&self, operation: &str, payload: Value) -> Result<Value> {
        let keys = self.keys.iter().map(String::as_str).collect::<Vec<_>>();
        let payload = serde_json::to_string(&payload)?;
        let result: String = self
            .redis
            .eval(
                include_str!("state.lua"),
                keys.as_slice(),
                &[operation, payload.as_str()],
            )
            .await
            .map_err(provider_error)?;
        let value: Value = serde_json::from_str(&result)?;
        match value.get("error").and_then(Value::as_str) {
            Some("capacity") => Err(Error::Capacity),
            Some(error) => Err(Error::Provider(error.to_owned())),
            None => Ok(value),
        }
    }
}

fn provider_error(error: brz_redis::RedisError) -> Error {
    Error::Provider(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_password() {
        let config = RedisConfig {
            password: Some("sensitive-test-password".into()),
            ..Default::default()
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("sensitive-test-password"));
    }
}
