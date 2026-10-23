use crate::{RedisProvider, provider_error};
use async_trait::async_trait;
use brz_redis::{Redis, RedisService};
use brz_schedule::{Error, Lease, LockProvider, Result};

#[derive(Clone, Debug)]
pub enum LockKeyPolicy {
    Raw,
    Prefix(String),
}

/// A standalone lock adapter. It may share an application-owned Redis service.
#[derive(Clone)]
pub struct RedisLockProvider {
    redis: RedisService,
    policy: LockKeyPolicy,
}

impl RedisLockProvider {
    pub fn new(redis: RedisService, policy: LockKeyPolicy) -> Self {
        Self { redis, policy }
    }
    async fn operation(&self, op: &str, key: &str, token: &str, ttl: u64) -> Result<i64> {
        let key = match &self.policy {
            LockKeyPolicy::Raw => key.to_owned(),
            LockKeyPolicy::Prefix(prefix) => format!("{prefix}{key}"),
        };
        let ttl = ttl.to_string();
        self.redis
            .eval(
                include_str!("lock.lua"),
                &[key.as_str()],
                &[op, token, ttl.as_str()],
            )
            .await
            .map_err(provider_error)
    }
}

#[async_trait]
impl LockProvider for RedisLockProvider {
    async fn try_acquire(&self, key: &str, token: &str, lease_ms: u64) -> Result<Option<Lease>> {
        crate::jobs::validate_lease(lease_ms)?;
        if key.is_empty() || token.is_empty() {
            return Err(Error::Invalid("lock key/token is empty".into()));
        }
        let expiry = self.operation("acquire", key, token, lease_ms).await?;
        Ok((expiry > 0).then(|| Lease {
            key: key.into(),
            token: token.into(),
            expires_at_ms: expiry,
        }))
    }
    async fn renew_lock(&self, lease: &Lease, lease_ms: u64) -> Result<bool> {
        crate::jobs::validate_lease(lease_ms)?;
        Ok(self
            .operation("renew", &lease.key, &lease.token, lease_ms)
            .await?
            > 0)
    }
    async fn release(&self, lease: &Lease) -> Result<bool> {
        Ok(self
            .operation("release", &lease.key, &lease.token, 0)
            .await?
            > 0)
    }
}

#[async_trait]
impl LockProvider for RedisProvider {
    async fn try_acquire(&self, key: &str, token: &str, lease_ms: u64) -> Result<Option<Lease>> {
        crate::jobs::validate_lease(lease_ms)?;
        if key.is_empty() || token.is_empty() {
            return Err(Error::Invalid("lock key/token is empty".into()));
        }
        let expiry = self.lock_op("acquire", key, token, lease_ms).await?;
        Ok((expiry > 0).then(|| Lease {
            key: key.into(),
            token: token.into(),
            expires_at_ms: expiry,
        }))
    }
    async fn renew_lock(&self, lease: &Lease, lease_ms: u64) -> Result<bool> {
        crate::jobs::validate_lease(lease_ms)?;
        Ok(self
            .lock_op("renew", &lease.key, &lease.token, lease_ms)
            .await?
            > 0)
    }
    async fn release(&self, lease: &Lease) -> Result<bool> {
        Ok(self.lock_op("release", &lease.key, &lease.token, 0).await? > 0)
    }
}

impl RedisProvider {
    async fn lock_op(&self, op: &str, key: &str, token: &str, ttl: u64) -> Result<i64> {
        self.locks(LockKeyPolicy::Prefix(format!(
            "{}:lock:",
            self.config.namespace
        )))
        .operation(op, key, token, ttl)
        .await
    }
}
