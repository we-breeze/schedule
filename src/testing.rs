//! Reusable conformance checks for provider authors. Never enabled by default.
use crate::*;
use serde_json::json;
use std::{sync::Arc, time::Duration};

/// Verify durable ownership, filtering, continuation/retry and lock contracts.
/// The supplied provider must be an isolated namespace with capacity >= 32.
pub async fn provider_contract(
    jobs: Arc<dyn JobProvider>,
    locks: Arc<dyn LockProvider>,
) -> Result<()> {
    let make = |id: &str, kind: &str| {
        let mut job = Job::new(id, kind, json!({"nested": [[], {}, null, "text"]})).unwrap();
        job.run_at_ms = now_ms() - 1000;
        job
    };
    let kinds = vec!["process".to_string()];
    let original = make("first", "process");
    assert_eq!(jobs.submit(original.clone()).await?, SubmitResult::Inserted);
    assert_eq!(jobs.submit(original.clone()).await?, SubmitResult::Existing);
    jobs.submit(make("other-kind", "unregistered")).await?;
    let mut future = make("future", "process");
    future.run_at_ms = now_ms() + 60_000;
    jobs.submit(future).await?;
    let first = jobs.claim("a", &kinds, 8, 5000).await?;
    assert_eq!(first.len(), 1);
    let claim = &first[0];
    assert_eq!(claim.record.job.payload, original.payload);
    assert!(
        !jobs
            .finish("first", "wrong", Completion::Success(json!(null)))
            .await?
    );
    assert!(jobs.renew("first", &claim.owner_token, 5000).await?);
    assert!(
        jobs.finish(
            "first",
            &claim.owner_token,
            Completion::ContinueAt {
                run_at_ms: now_ms() - 1,
                payload: Some(json!([])),
            }
        )
        .await?
    );
    assert!(!jobs.renew("first", &claim.owner_token, 5000).await?);
    let next = jobs.claim("b", &kinds, 8, 5000).await?;
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].record.failures, 0);
    assert_eq!(next[0].record.job.payload, json!([]));
    assert_ne!(next[0].owner_token, claim.owner_token);
    assert!(
        jobs.finish(
            "first",
            &next[0].owner_token,
            Completion::ContinueAt {
                run_at_ms: now_ms() - 1,
                payload: Some(json!(null)),
            }
        )
        .await?
    );
    let next = jobs.claim("null-payload", &kinds, 8, 5000).await?;
    assert_eq!(next[0].record.job.payload, json!(null));
    assert_eq!(next[0].record.failures, 0);
    assert!(
        jobs.finish(
            "first",
            &next[0].owner_token,
            Completion::Failure {
                error: "temporary".into(),
                retry_at_ms: Some(now_ms() - 1),
            }
        )
        .await?
    );
    for expected in [1, 2] {
        let next = jobs.claim("retry", &kinds, 8, 5000).await?;
        assert_eq!(next[0].record.failures, expected);
        assert!(
            jobs.finish(
                "first",
                &next[0].owner_token,
                Completion::Failure {
                    error: "temporary".into(),
                    retry_at_ms: Some(now_ms() - 1),
                }
            )
            .await?
        );
    }
    let failed = jobs.get("first").await?.unwrap();
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(failed.failures, 3);
    assert!(jobs.claim("empty", &kinds, 8, 5000).await?.is_empty());

    let mut expired_start = make("start-deadline", "unregistered");
    expired_start.run_at_ms = now_ms() + 60_000;
    expired_start.expires_at_ms = Some(now_ms() - 1);
    expired_start.overlap_key = Some("deadline-scope".into());
    jobs.submit(expired_start.clone()).await?;
    assert!(jobs.claim("cleanup", &[], 0, 5000).await?.is_empty());
    let cancelled = jobs.get("start-deadline").await?.unwrap();
    assert_eq!(cancelled.status, JobStatus::Cancelled);
    assert_eq!(cancelled.deliveries, 0);
    assert_eq!(cancelled.failures, 0);
    assert_eq!(jobs.submit(expired_start).await?, SubmitResult::Existing);
    let mut successor = make("deadline-successor", "unregistered");
    successor.overlap_key = Some("deadline-scope".into());
    assert_eq!(jobs.submit(successor).await?, SubmitResult::Inserted);

    jobs.submit(make("null-result", "process")).await?;
    let null = jobs.claim("null-result", &kinds, 1, 5000).await?.remove(0);
    assert!(
        jobs.finish(
            "null-result",
            &null.owner_token,
            Completion::Success(json!(null))
        )
        .await?
    );
    assert_eq!(
        jobs.get("null-result").await?.unwrap().result,
        Some(json!(null))
    );

    jobs.submit(make("expired", "process")).await?;
    let expired = jobs.claim("crash", &kinds, 1, 50).await?.remove(0);
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert!(!jobs.renew("expired", &expired.owner_token, 5000).await?);
    let recovered = jobs.claim("recovery", &kinds, 1, 5000).await?.remove(0);
    assert_eq!(recovered.record.deliveries, 2);
    assert!(
        !jobs
            .finish("expired", &expired.owner_token, Completion::Cancelled)
            .await?
    );
    assert!(
        jobs.finish(
            "expired",
            &recovered.owner_token,
            Completion::Success(json!({"values":[]}))
        )
        .await?
    );
    assert_eq!(
        jobs.get("expired").await?.unwrap().result,
        Some(json!({"values":[]}))
    );

    for id in 0..12 {
        jobs.submit(make(&format!("parallel-{id}"), "process"))
            .await?;
    }
    let (left, right) = tokio::join!(
        jobs.claim("left", &kinds, 8, 5000),
        jobs.claim("right", &kinds, 8, 5000)
    );
    let claims = left?.into_iter().chain(right?).collect::<Vec<_>>();
    assert_eq!(claims.len(), 12);
    let unique = claims
        .iter()
        .map(|c| &c.record.job.id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique.len(), 12);
    for claim in claims {
        assert!(
            jobs.finish(
                &claim.record.job.id,
                &claim.owner_token,
                Completion::Cancelled
            )
            .await?
        );
    }

    let lease = locks.try_acquire("scope", "owner-a", 5000).await?.unwrap();
    assert!(locks.try_acquire("scope", "owner-b", 5000).await?.is_none());
    assert!(locks.renew_lock(&lease, 5000).await?);
    let impostor = Lease {
        token: "owner-b".into(),
        ..lease.clone()
    };
    assert!(!locks.release(&impostor).await?);
    assert!(locks.release(&lease).await?);
    let short = locks.try_acquire("expiry", "a", 50).await?.unwrap();
    tokio::time::sleep(Duration::from_millis(110)).await;
    let replacement = locks.try_acquire("expiry", "b", 5000).await?.unwrap();
    assert!(!locks.release(&short).await?);
    assert!(locks.release(&replacement).await?);
    Ok(())
}
