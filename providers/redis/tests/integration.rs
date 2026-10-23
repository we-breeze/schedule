#![cfg(feature = "integration-tests")]
use brz_schedule::*;
use brz_schedule_redis::{LockKeyPolicy, RedisConfig, RedisProvider};
use std::sync::Arc;

async fn provider(capacity: usize) -> Arc<RedisProvider> {
    let endpoint = std::env::var("BRZ_SCHEDULE_TEST_REDIS")
        .expect("set BRZ_SCHEDULE_TEST_REDIS for explicitly enabled integration tests");
    Arc::new(
        RedisProvider::connect(RedisConfig {
            endpoint,
            namespace: format!("brz:schedule:test:{}", uuid::Uuid::new_v4()),
            capacity,
            retained_terminal: 32,
            ..Default::default()
        })
        .await
        .unwrap(),
    )
}

#[tokio::test]
async fn real_redis_satisfies_shared_provider_contract() {
    let provider = provider(64).await;
    testing::provider_contract(provider.clone(), provider)
        .await
        .unwrap();
}

#[tokio::test]
async fn real_redis_schedule_cas_capacity_and_payload_roundtrip() {
    let provider = provider(1).await;
    let job = Job::new("occupied", "kind", serde_json::json!([[], {}])).unwrap();
    provider.submit(job.clone()).await.unwrap();
    assert_eq!(
        provider.get("occupied").await.unwrap().unwrap().job.payload,
        job.payload
    );
    let schedule = Schedule::new(
        "periodic",
        Trigger::FixedRate { interval_ms: 1000 },
        Job::new("t", "kind", serde_json::json!([])).unwrap(),
        now_ms() - 1000,
    )
    .unwrap();
    provider.register_schedule(schedule.clone()).await.unwrap();
    provider.register_schedule(schedule).await.unwrap();
    let before = provider.due_schedules(None, 1).await.unwrap().remove(0);
    let scheduler = Scheduler::new(provider.clone(), SchedulerOptions::default()).unwrap();
    assert!(matches!(scheduler.run_once().await, Err(Error::Capacity)));
    assert_eq!(
        provider.due_schedules(None, 1).await.unwrap()[0].revision,
        before.revision
    );
    let claim = provider
        .claim("worker", &["kind".into()], 1, 5000)
        .await
        .unwrap()
        .remove(0);
    provider
        .finish(
            "occupied",
            &claim.owner_token,
            Completion::Success(serde_json::json!([])),
        )
        .await
        .unwrap();
    let (a, b) = tokio::join!(scheduler.run_once(), scheduler.run_once());
    assert_eq!(a.unwrap() + b.unwrap(), 1);
    let claims = provider
        .claim("verify", &["kind".into()], 10, 5000)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].record.job.payload, serde_json::json!([]));
}

#[tokio::test]
async fn raw_lock_keys_and_invalid_namespace_types_are_explicit() {
    use brz_redis::{Redis, RedisService};
    let endpoint = std::env::var("BRZ_SCHEDULE_TEST_REDIS").unwrap();
    let redis = RedisService::single(endpoint.clone()).await.unwrap();
    let namespace = format!("brz:schedule:test:{}", uuid::Uuid::new_v4());
    let provider = RedisProvider::connect(RedisConfig {
        endpoint,
        namespace: namespace.clone(),
        ..Default::default()
    })
    .await
    .unwrap();
    let raw = provider.locks(LockKeyPolicy::Raw);
    let key = format!("application-owned:{}", uuid::Uuid::new_v4());
    let lease = raw.try_acquire(&key, "token", 5000).await.unwrap().unwrap();
    let stored: Option<String> = redis.get(key.as_str()).await.unwrap();
    assert_eq!(stored.as_deref(), Some("token"));
    assert!(raw.release(&lease).await.unwrap());
    // Key validation happens before any mutation, including the first HSET.
    redis
        .set(format!("{namespace}:ready"), "wrong-type")
        .await
        .unwrap();
    assert!(matches!(
        provider.submit(Job::new("safe", "kind", ()).unwrap()).await,
        Err(Error::Provider(_))
    ));
    let count: i64 = redis
        .command({
            let mut c = brz_redis::cmd("HLEN");
            c.arg(format!("{namespace}:jobs"));
            c
        })
        .await
        .unwrap();
    assert_eq!(count, 0);
}
