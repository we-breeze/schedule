use brz_schedule::{memory::MemoryProvider, *};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

#[cfg(feature = "test-support")]
#[tokio::test]
async fn memory_satisfies_shared_provider_contract() {
    let provider = Arc::new(MemoryProvider::new(64).unwrap());
    testing::provider_contract(provider.clone(), provider)
        .await
        .unwrap();
}

#[tokio::test]
async fn capacity_and_overlap_do_not_remove_existing_work() {
    let provider = MemoryProvider::new(1).unwrap();
    let mut first = Job::new("first", "kind", json!({})).unwrap();
    first.overlap_key = Some("scope".into());
    provider.submit(first).await.unwrap();
    let mut second = Job::new("second", "kind", json!({})).unwrap();
    second.overlap_key = Some("scope".into());
    assert_eq!(
        provider.submit(second).await.unwrap(),
        SubmitResult::OverlapSkipped
    );
    assert!(matches!(
        provider.submit(Job::new("full", "kind", ()).unwrap()).await,
        Err(Error::Capacity)
    ));
    let claims = provider
        .claim("worker", &["kind".into()], 1, 5000)
        .await
        .unwrap();
    provider
        .finish(
            "first",
            &claims[0].owner_token,
            Completion::Success(json!(true)),
        )
        .await
        .unwrap();
    provider
        .submit(Job::new("room", "kind", ()).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn exact_expiry_fences_old_owner_without_spending_failure_budget() {
    let clock = Arc::new(AtomicI64::new(1000));
    let read = clock.clone();
    let provider =
        MemoryProvider::with_clock(4, Arc::new(move || read.load(Ordering::SeqCst))).unwrap();
    let mut job = Job::new("job", "kind", ()).unwrap();
    job.run_at_ms = 0;
    provider.submit(job).await.unwrap();
    let old = provider
        .claim("a", &["kind".into()], 1, 100)
        .await
        .unwrap()
        .remove(0);
    clock.store(1100, Ordering::SeqCst);
    assert!(
        !provider
            .finish("job", &old.owner_token, Completion::Success(json!(null)))
            .await
            .unwrap()
    );
    let new = provider
        .claim("b", &["kind".into()], 1, 100)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(new.record.failures, 0);
    assert_ne!(old.owner_token, new.owner_token);
}

#[tokio::test]
async fn schedule_capacity_failure_preserves_cursor_and_revision() {
    let provider = Arc::new(MemoryProvider::new(1).unwrap());
    provider
        .submit(Job::new("occupant", "kind", ()).unwrap())
        .await
        .unwrap();
    let schedule = Schedule::new(
        "schedule",
        Trigger::Once,
        Job::new("template", "kind", ()).unwrap(),
        0,
    )
    .unwrap();
    provider.register_schedule(schedule).await.unwrap();
    let before = provider.due_schedules(None, 1).await.unwrap().remove(0);
    let scheduler = Scheduler::new(provider.clone(), SchedulerOptions::default()).unwrap();
    assert!(matches!(scheduler.run_once().await, Err(Error::Capacity)));
    let after = provider.due_schedules(None, 1).await.unwrap().remove(0);
    assert_eq!(before.revision, after.revision);
    assert_eq!(after.next_run_at_ms, Some(0));
}

#[tokio::test]
async fn start_deadline_does_not_interrupt_running_work_but_cancels_continuation() {
    let clock = Arc::new(AtomicI64::new(1000));
    let read = clock.clone();
    let provider =
        MemoryProvider::with_clock(1, Arc::new(move || read.load(Ordering::SeqCst))).unwrap();
    let mut job = Job::new("job", "kind", ()).unwrap();
    job.run_at_ms = 0;
    job.expires_at_ms = Some(1050);
    provider.submit(job).await.unwrap();
    let claim = provider
        .claim("a", &["kind".into()], 1, 500)
        .await
        .unwrap()
        .remove(0);
    clock.store(1050, Ordering::SeqCst);
    assert!(
        provider
            .renew("job", &claim.owner_token, 500)
            .await
            .unwrap()
    );
    assert!(
        provider
            .finish(
                "job",
                &claim.owner_token,
                Completion::ContinueAt {
                    run_at_ms: 2000,
                    payload: None,
                }
            )
            .await
            .unwrap()
    );
    assert!(
        provider
            .claim("a", &["kind".into()], 1, 500)
            .await
            .unwrap()
            .is_empty()
    );
    let cancelled = provider.get("job").await.unwrap().unwrap();
    assert_eq!(cancelled.status, JobStatus::Cancelled);
    assert_eq!(cancelled.deliveries, 1);
    assert_eq!(cancelled.failures, 0);
    provider
        .submit(Job::new("room", "kind", ()).unwrap())
        .await
        .unwrap();
}

#[test]
fn old_job_envelopes_default_to_no_start_deadline() {
    let job = Job::new("job", "kind", ()).unwrap();
    let mut value = serde_json::to_value(job).unwrap();
    value.as_object_mut().unwrap().remove("expires_at_ms");
    let restored: Job = serde_json::from_value(value).unwrap();
    assert_eq!(restored.expires_at_ms, None);
}
